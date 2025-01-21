#include "heapfile.h"
#include "error.h"

// routine to create a heapfile
const Status createHeapFile(const string fileName)
{
    File* 		file;
    Status 		status;
    FileHdrPage*	hdrPage;
    int			hdrPageNo;
    int			newPageNo;
    Page*		newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {
		// file doesn't exist. First create it and allocate
		// an empty header page and data page.
        status = db.createFile(fileName);
        if(status != OK) return status;

        status = db.openFile(fileName, file);
        if(status != OK) return status;

        status = bufMgr->allocPage(file, hdrPageNo, newPage);
        cout << "allocated header page " << hdrPageNo <<  " to file " << endl;
        if(status != OK) return status;

        hdrPage = (FileHdrPage*) newPage;
        strncpy(hdrPage->fileName, fileName.c_str(), sizeof(hdrPage->fileName));

        status = bufMgr->allocPage(file, newPageNo, newPage);
        cout << "allocated new page " << newPageNo <<  " to file " << endl;
        if(status != OK) return status;

        newPage->init(newPageNo);
        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;
        hdrPage->pageCnt = 1;
        hdrPage->recCnt = 0;
        
        status = bufMgr->unPinPage(file, newPageNo, true);
        if(status != OK) return status;

        status = bufMgr->unPinPage(file, hdrPageNo, true);
        if(status != OK) return status;

        status = db.closeFile(file);
        if (status != OK) return status;
        return OK;
		
    }
    return (OK);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
	return (db.destroyFile (fileName));
}

// constructor opens the underlying file
HeapFile::HeapFile(const string & fileName, Status& returnStatus)
{
    Status 	status;
    Page*	pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {
        // Get the header page number
        status = filePtr->getFirstPage(headerPageNo);
        if (headerPageNo < 0) {
            cerr << "Error: Invalid first page number!" << endl;
        } else {
            cout << "First page number: " << headerPageNo << endl;
        }

        if(status != OK){
            cerr << "Error: Unable to get the hearder page for file" << fileName << endl;
            returnStatus = status;
            db.closeFile(filePtr);
            return;
        }
		
        // Pin the header page
        Page* rawPage;
        status = bufMgr->readPage(filePtr, headerPageNo, rawPage);
        if(status != OK){
            cerr << "Error: Unable to read header file for file" << fileName << endl;
            returnStatus= status;
            db.closeFile(filePtr);
            return;
        }
        
        // Initialize metadata from header page
        headerPage = (FileHdrPage*)rawPage;
        FileHdrPage* fileHdr = (FileHdrPage*)headerPage;
        int firstPageNo = fileHdr->firstPage;
        
        // Pin the first data page
        status = bufMgr->readPage(filePtr, firstPageNo, curPage);
        if (status != OK) {
            cerr << "Error: Unable to read first data page for file " << fileName << endl;
            returnStatus = status;
            bufMgr->unPinPage(filePtr, headerPageNo, false);
            db.closeFile(filePtr);
            return;
        }
        
        // Initialize class members
        curPageNo = firstPageNo;
        curDirtyFlag = false;
        hdrDirtyFlag = false;
        curRec = NULLRID;
        
        // Successfully initialized
        returnStatus = OK;
    }
    else
    {
    	cerr << "open of heap file failed\n";
		returnStatus = status;
		return;
    }
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it 
    if (curPage != NULL)
    {
    	status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
		curPage = NULL;
		curPageNo = 0;
		curDirtyFlag = false;
		if (status != OK) cerr << "error in unpin of date page\n";
    }
	
	 // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK) cerr << "error in unpin of header page\n";
	
	// status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
	// if (status != OK) cerr << "error in flushFile call\n";
	// before close the file
	status = db.closeFile(filePtr);
    if (status != OK)
    {
		cerr << "error in closefile call\n";
		Error e;
		e.print (status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
  return headerPage->recCnt;
}

// retrieve an arbitrary record from a file.
// if record is not on the currently pinned page, the current page
// is unpinned and the required page is read into the buffer pool
// and pinned.  returns a pointer to the record via the rec parameter

const Status HeapFile::getRecord(const RID & rid, Record & rec)
{
    Status status;

    // Validate RID
    if (rid.pageNo <0 || rid.slotNo <0){
        return BADRID;
    }
    
    // Check if the page is already pinned
    if (curPage == NULL || curPageNo != rid.pageNo){
        // Unpin the current page if it exists
        if (curPage != NULL){
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK){
                cerr << "Error: Unable to unpin current page " << curPageNo << endl;
                return status;
            }
        }
        
        // Pin the new page
        status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
        if (status != OK){
            cerr << "Error: Unable to pin page " << rid.pageNo << endl;
            return status;
        }
        
        curPageNo = rid.pageNo;
        curDirtyFlag = false;
    }
    
    // Retrieve the record from the current page
    status = curPage->getRecord(rid, rec);
    if (status != OK){
        cerr << "Error: Unable to get record from page " << rid.pageNo
                     << ", slot " << rid.slotNo << endl;
        return status;
    }
    
    // Update metadata
    curRec = rid;
    return OK;
    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
}

HeapFileScan::HeapFileScan(const string & name,
			   Status & status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
				     const int length_,
				     const Datatype type_, 
				     const char* filter_,
				     const Operator op_)
{
    if (!filter_) {                        // no filtering requested
        filter = NULL;
        return OK;
    }
    
    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int)
         || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}


const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
		curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo) 
    {
		if (curPage != NULL)
		{
			status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
			if (status != OK) return status;
		}
		// restore curPageNo and curRec values
		curPageNo = markedPageNo;
		curRec = markedRec;
		// then read the page
		status = bufMgr->readPage(filePtr, curPageNo, curPage);
		if (status != OK) return status;
		curDirtyFlag = false; // it will be clean
    }
    else curRec = markedRec;
    return OK;
}


const Status HeapFileScan::scanNext(RID& outRid) {
    Status status;
    RID nextRid;
    int nextPageNo;
    Record rec;

    // If curRec is NULLRID, start with the first record
    if (curRec.pageNo ==-1 && curRec.slotNo == -1) {
        status = curPage->firstRecord(nextRid);
        if (status == ENDOFPAGE || status == NORECORDS) {
            // Current page has no records, transition to the next page
            status = curPage->getNextPage(nextPageNo);
            if (nextPageNo == -1) {
                return FILEEOF; // No more pages
            }

            // Unpin current page and pin the next page
            bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            status = bufMgr->readPage(filePtr, nextPageNo, curPage);
            if (status != OK) return status;

            // Update page tracking and reset for scanning
            curPageNo = nextPageNo;
            curRec = NULLRID; // Start at the first record of the new page
            return scanNext(outRid); // Recursive call to process the next page
        }
    } else {
        // Get the next record on the current page
        status = curPage->nextRecord(curRec, nextRid);
    }

    if (status == ENDOFPAGE) {
        // Current page is exhausted, move to the next page
        status = curPage->getNextPage(nextPageNo);
        if (nextPageNo == -1) {
            return FILEEOF; // No more pages
        }

        bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        status = bufMgr->readPage(filePtr, nextPageNo, curPage);
        if (status != OK) return status;

        curPageNo = nextPageNo;
        curRec = NULLRID; // Reset for the new page
        return scanNext(outRid); // Recursive call to process the next page
    }

    if (status == OK) {
        // Apply filter if present
        status = curPage->getRecord(nextRid, rec);
        if (status != OK) return status;

        if (filter != NULL && !matchRec(rec)) {
            // Skip the record if it doesn't satisfy the filter
            curRec = nextRid; // Update curRec for the next call
            return scanNext(outRid); // Recursive call to find the next valid record
        }

        // Valid record found
        curRec = nextRid;  // Update the current record
        outRid = nextRid;  // Return the RID of the valid record
        return OK;
    }

    // If no more records and no errors, return FILEEOF
    return FILEEOF;
}


// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page 

const Status HeapFileScan::getRecord(Record & rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file.
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true; 
    return status;
}


// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record & rec) const
{
    // no filtering requested
    if (!filter) return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length -1 ) >= rec.length)
	return false;

    float diff = 0;                       // < 0 if attr < fltr
    switch(type) {

    case INTEGER:
        int iattr, ifltr;                 // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr;               // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch(op) {
    case LT:  if (diff < 0.0) return true; break;
    case LTE: if (diff <= 0.0) return true; break;
    case EQ:  if (diff == 0.0) return true; break;
    case GTE: if (diff >= 0.0) return true; break;
    case GT:  if (diff > 0.0) return true; break;
    case NE:  if (diff != 0.0) return true; break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string & name,
                               Status & status) : HeapFile(name, status)
{
  //Do nothing. Heapfile constructor will bread the header page and the first
  // data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK) cerr << "error in unpin of data page\n";
    }
}

// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record & rec, RID& outRid)
{
    Page*	newPage;
    int		newPageNo;
    Status	status, unpinstatus;
    RID		rid;

    // check for very large records
    if ((unsigned int) rec.length > PAGESIZE-DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }
    
    // If no current page is set, load the last page
    if (curPage == NULL){
        if(headerPage->lastPage == -1){
            // File is empty, allocate new page
            status = bufMgr->allocPage(filePtr, newPageNo, curPage);
            if (status != OK) return status;
            
            curPage->init(newPageNo);
            headerPage->firstPage = newPageNo;
            headerPage->lastPage = newPageNo;
            headerPage->pageCnt = 1;
            hdrDirtyFlag = true;
            
        }else{
            // Pin the last page
            status = bufMgr->readPage(filePtr, headerPage->lastPage, curPage);
            if (status != OK) return status;
            curPageNo = headerPage->lastPage;
            curDirtyFlag = false;
        }
    }
    
    // Try to insert the record on the current page
    status = curPage->insertRecord(rec, outRid);
    if (status == NOSPACE){
        // Allocate a new page if the current one is full
        status = bufMgr->allocPage(filePtr, newPageNo, newPage);
        if (status != OK) return status;
        
        newPage->init(newPageNo);
        
        // Link the new page to the file
        status = curPage->setNextPage(newPageNo);
        if (status != OK) return status;
        
        // Update the header page metadata
        headerPage->lastPage = newPageNo;
        headerPage->pageCnt++;
        hdrDirtyFlag = true;
        
        // Unpin the old current page
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK) return status;
        
        // Set the new page as the current page
        curPage = newPage;
        curPageNo = newPageNo;
        curDirtyFlag = false;
        
        // Retry insering the record
        status = curPage->insertRecord(rec, outRid);
        if (status != OK) return status;
        
    }else if (status != OK){
        return status;
    }
    
    // Record was inseted successfully, update metadata
    headerPage->recCnt++;
    //cout << "recCnt = " << headerPage->recCnt << endl;
    curDirtyFlag = true;
    
    return OK;  
}


