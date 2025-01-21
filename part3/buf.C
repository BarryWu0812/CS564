#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)  { if (!(c)) { \
		       cerr << "At line " << __LINE__ << ":" << endl << "  "; \
                       cerr << "This condition should hold: " #c << endl; \
                       exit(1); \
		     } \
                   }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++) 
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int) (bufs * 1.2))*2)/2)+1;
    hashTable = new BufHashTbl (htsize);  // allocate the buffer hash table

    clockHand = bufs - 1;
}


BufMgr::~BufMgr() {

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++) 
    {
        BufDesc* tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true) {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete [] bufTable;
    delete [] bufPool;
}


const Status BufMgr::allocBuf(int & frame) 
{
    int cnt = 0;
    while(cnt < 2 * numBufs)
    {
        cnt++;
        advanceClock();
        BufDesc &currentFrame = bufTable[clockHand];

        if (currentFrame.refbit)
        {
            // cout << "refbit" << endl;
            currentFrame.refbit = false; // Clear the reference bit
            continue;
        }

        // Skip pinned pages
        if (currentFrame.pinCnt > 0)
        {
            // cout << "pin" << endl;
            continue;
        }

        // Evict current page if dirty
        if (currentFrame.dirty) {
            Status writeStatus = currentFrame.file->writePage(currentFrame.pageNo, &bufPool[clockHand]);
            if (writeStatus != OK) return writeStatus;
        }

        // Clear hash table entry
        hashTable->remove(currentFrame.file, currentFrame.pageNo);

        // Clear the frame
        currentFrame.Clear();
        frame = clockHand;
        return OK;
    }

    return BUFFEREXCEEDED; // No free frame found

}

	
const Status BufMgr::readPage(File* file, const int PageNo, Page*& page)
{
    int frameNo;
    Status status = hashTable->lookup(file, PageNo, frameNo);

    if (status == OK) {
        bufTable[frameNo].refbit = true;
        bufTable[frameNo].pinCnt++;
        page = &bufPool[frameNo];
        return OK;
    }

    // Page is not in buffer pool
    Status allocStatus = allocBuf(frameNo);
    if (allocStatus != OK) return allocStatus;

    Status readStatus = file->readPage(PageNo, &bufPool[frameNo]);
    if (readStatus != OK) return readStatus;

    hashTable->insert(file, PageNo, frameNo);
    bufTable[frameNo].Set(file, PageNo);
    page = &bufPool[frameNo];
    return OK;
}


const Status BufMgr::unPinPage(File* file, const int PageNo, 
			       const bool dirty) 
{
    int frameNo;
    Status status = hashTable->lookup(file, PageNo, frameNo);

    if (status != OK) return PAGENOTPINNED;

    BufDesc &frame = bufTable[frameNo];
    if (frame.pinCnt <= 0) return PAGENOTPINNED;

    frame.pinCnt--;
    if (dirty) frame.dirty = true;

    return OK;

}

const Status BufMgr::allocPage(File* file, int& pageNo, Page*& page) 
{
    Status status = file->allocatePage(pageNo);
    if (status != OK) return status;
    int frameNo;
    Status allocStatus = allocBuf(frameNo);
    if (allocStatus != OK) return allocStatus;

    bufPool[frameNo].init(pageNo);
    hashTable->insert(file, pageNo, frameNo);
    bufTable[frameNo].Set(file, pageNo);
    page = &bufPool[frameNo];

    return OK;
}

const Status BufMgr::disposePage(File* file, const int pageNo) 
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File* file) 
{
  Status status;

  for (int i = 0; i < numBufs; i++) {
    BufDesc* tmpbuf = &(bufTable[i]);
    if (tmpbuf->valid == true && tmpbuf->file == file) {

      if (tmpbuf->pinCnt > 0)
	  return PAGEPINNED;

      if (tmpbuf->dirty == true) {
#ifdef DEBUGBUF
	cout << "flushing page " << tmpbuf->pageNo
             << " from frame " << i << endl;
#endif
	if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
					      &(bufPool[i]))) != OK)
	  return status;

	tmpbuf->dirty = false;
      }

      hashTable->remove(file,tmpbuf->pageNo);

      tmpbuf->file = NULL;
      tmpbuf->pageNo = -1;
      tmpbuf->valid = false;
    }

    else if (tmpbuf->valid == false && tmpbuf->file == file)
      return BADBUFFER;
  }
  
  return OK;
}


void BufMgr::printSelf(void) 
{
    BufDesc* tmpbuf;
  
    cout << endl << "Print buffer...\n";
    for (int i=0; i<numBufs; i++) {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char*)(&bufPool[i]) 
             << "\tpinCnt: " << tmpbuf->pinCnt;
    
        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}


