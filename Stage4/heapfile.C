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
        if (status != OK)
            return status;
        
		status = db.openFile(fileName, file);
        if (status != OK)
            return status;

        Page* pagePtr;
        status = bufMgr->allocPage(file, hdrPageNo, pagePtr);
        if (status != OK)
        {
            db.closeFile(file);
            return status;
        }
		
		hdrPage = (FileHdrPage*)pagePtr;
        strncpy(hdrPage->fileName, fileName.c_str(), sizeof(hdrPage->fileName));
        hdrPage->recCnt = 0;
        hdrPage->firstPage = -1;

		status = bufMgr->allocPage(file, newPageNo, newPage);
        if (status != OK)
            return status;

		newPage->init(newPageNo);

        hdrPage->firstPage = newPageNo;
        hdrPage->lastPage = newPageNo;
        hdrPage->pageCnt = 1;

        status = bufMgr->unPinPage(file, hdrPageNo, true); // 更新後釋放
        if (status != OK)
            return status;

        status = bufMgr->unPinPage(file, newPageNo, true); // 更新後釋放
        db.closeFile(file);
        if (status != OK)
            return status;
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
        returnStatus = filePtr->getFirstPage(headerPageNo);
        if (returnStatus != OK)
        {
            db.closeFile(filePtr);
            return;
        }

		returnStatus = bufMgr->readPage(filePtr, headerPageNo, pagePtr);
        if (returnStatus != OK)
        {
            db.closeFile(filePtr);
            return;
        }

		headerPage = (FileHdrPage*)pagePtr;
		
        curPageNo = headerPage->firstPage;
        returnStatus = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (returnStatus != OK)
        {
            bufMgr->unPinPage(filePtr, curPageNo, false);
            db.closeFile(filePtr);
            return;
        }
        curDirtyFlag = false; // 資料頁尚未修改
        hdrDirtyFlag = false;

        // 5. 初始化其他屬性
        curRec = NULLRID; // 尚未選定記錄
        return;
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

    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;
    if (rid.pageNo < 0 || rid.slotNo < 0)
        return BADRID;

    // Check if the record is on the currently pinned page
    if (curPage == NULL || curPageNo != rid.pageNo)
    {
        // Unpin the current page
        if (curPage != NULL)
        {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK)
                return status;
        }

        // Read the new page
        status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
        if (status != OK)
            return status;

        curPageNo = rid.pageNo;
        curDirtyFlag = false;
    }
    // Retrieve the record from the current page
    status = curPage->getRecord(rid, rec);
    if (status != OK)
            return status;
    curRec = rid;
    return OK;
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


const Status HeapFileScan::scanNext(RID& outRid)
{
    Status 	status = OK;
    RID		nextRid;
    // RID		tmpRid;
    int 	nextPageNo;
    Record      rec;
	
    // 嘗試獲取下一條記錄
    if (curRec.pageNo == NULLRID.pageNo && curRec.slotNo == NULLRID.slotNo)
    {
        // cout << "f" << endl;
        status = curPage->firstRecord(nextRid);
    }
    else
    {
        // cout << "s" << endl;
        status = curPage->nextRecord(curRec, nextRid);
    }

    // cout << status << endl;
    // 3. 如果當前頁面的記錄結束，切換到下一頁
    if (status == ENDOFPAGE || status == NORECORDS)
    {
        status = curPage->getNextPage(nextPageNo);
        // cout << "status: " << status <<  " nextPageNo: " << nextPageNo << endl;
        if (nextPageNo == -1) {
            bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            // curPage = NULL; // 沒有更多的頁面
            curRec = NULLRID;
            // cout << "END" << endl;
            return FILEEOF; // 完成掃描
        }

        // 切換到下一頁
        bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        status = bufMgr->readPage(filePtr, nextPageNo, curPage);
        if (status != OK)
            return status;
        curPageNo = nextPageNo;
        curDirtyFlag = false;
        curRec = NULLRID;
        // cout << "Next Page" << endl;
        return scanNext(outRid); // 回到外層循環嘗試從新頁面中獲取記錄
    }

    // 4. 驗證記錄是否符合過濾條件
    status = curPage->getRecord(nextRid, rec); // 取回記錄
    // cout << "nextRid: " <<  nextRid.pageNo << ", " << nextRid.slotNo << endl;
    if (status != OK)
    {
        return status;
    }


    // cout << (filter == NULL) << "match: "  << matchRec(rec) << endl;
    if (filter == NULL || matchRec(rec))
    {
        // cout << "match" << endl;
        curRec = nextRid;
        outRid = nextRid;
        return OK;
    }
    else
    {
        // cout << "NM" << endl;
        curRec = nextRid; // 更新當前記錄
        return scanNext(outRid);
    }
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
    //Do nothing. Heapfile constructor will read the header page and the first
    // data page of the file into the buffer pool
    // if the first data page of the file is not the last data page of the file
    // unpin the current page and read the last page
    if ((curPage != NULL) && (curPageNo != headerPage->lastPage))
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK) cerr << "error in unpin of data page\n"; 
        curPageNo = headerPage->lastPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK) cerr << "error in readPage \n"; 
        curDirtyFlag = false;
    }
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

    if (curPage == NULL)
    {
        // 如果當前頁面為空，讀取最後一頁
        if (headerPage->lastPage != -1)
        {
            curPageNo = headerPage->lastPage;
            status = bufMgr->readPage(filePtr, curPageNo, curPage);
            if (status != OK)
                return status;
            curPageNo = headerPage->lastPage;
            curDirtyFlag = false;
        }
        else
        {
            // cout << "creat new" << endl;
            // 沒有資料頁，分配新頁
            status = bufMgr->allocPage(filePtr, newPageNo, curPage);
            if (status != OK)
                return status;

            curPage->init(newPageNo);
            headerPage->firstPage = newPageNo;
            headerPage->lastPage = newPageNo;
            headerPage->pageCnt = 1;
            hdrDirtyFlag = true; // 標記標頭頁為髒頁
        }
    }
    // cout << "insert" << endl;
    // 3. 嘗試插入記錄到當前頁
    status = curPage->insertRecord(rec, outRid);
    if (status == OK)
    {
        // 插入成功，更新記錄數量
        headerPage->recCnt++;
        curDirtyFlag = true;
        hdrDirtyFlag = true;
        return OK;
    }
    // cout << "nonspace" << endl;
    // 4. 如果插入失敗（空間不足），分配新頁面並重試插入
    if (status == NOSPACE)
    {
        // 釋放當前頁
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (status != OK) return status;

        // 分配新頁
        status = bufMgr->allocPage(filePtr, newPageNo, newPage);
        if (status != OK) return status;

        newPage->init(newPageNo); // 初始化新頁面

        // 更新鏈接和標頭頁
        status = curPage->setNextPage(newPageNo);
        if (status != OK)
            return status;

        headerPage->lastPage = newPageNo;
        headerPage->pageCnt++;
        hdrDirtyFlag = true;

        // 更新當前頁為新頁面
        curPage = newPage;
        curPageNo = newPageNo;

        // 重試插入記錄
        status = curPage->insertRecord(rec, outRid);
        if (status != OK)
            return status;

        headerPage->recCnt++;
        curDirtyFlag = true;
        return OK;
    }

    // 其他情況，返回錯誤
    return status;
}