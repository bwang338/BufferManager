#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

#define ASSERT(c)                                              \
    {                                                          \
        if (!(c))                                              \
        {                                                      \
            cerr << "At line " << __LINE__ << ":" << endl      \
                 << "  ";                                      \
            cerr << "This condition should hold: " #c << endl; \
            exit(1);                                           \
        }                                                      \
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

    int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
    hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

    clockHand = bufs - 1;
}

BufMgr::~BufMgr()
{

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true)
        {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete[] bufTable;
    delete[] bufPool;
    delete hashTable;
}

const Status BufMgr::allocBuf(int &frame)
{
    int numPinned = 0;
    int clockStart = clockHand;
    while (numPinned < numBufs)
    {
        if (clockHand == clockStart)
            numPinned = 0;

        advanceClock();
        bufTable[clockHand];
        if (!bufTable[clockHand].valid)
            break;
        if (bufTable[clockHand].refbit)
        {
            bufTable[clockHand].refbit = false;
            continue;
        }
        if (bufTable[clockHand].pinCnt >= 1)
        {
            numPinned++;
            continue;
        }
        if (bufTable[clockHand].dirty)
        {
            if (flushFile(bufTable[clockHand].file) != OK)
                return UNIXERR;
            break;
        }
    }

    if (numPinned == numBufs)
        return BUFFEREXCEEDED;
    else
    {
        if (bufTable[clockHand].valid)
            hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo);
        frame = bufTable[clockHand].frameNo;
        return OK;
    }
}

const Status BufMgr::readPage(File *file, const int PageNo, Page *&page)
{
    int frameNo;
    if (hashTable->lookup(file, PageNo, frameNo) != HASHNOTFOUND)
    {
        bufTable[frameNo].refbit = true;
        bufTable[frameNo].pinCnt++;
        page = &bufPool[frameNo];
        return OK;
    }

    Status s = allocBuf(frameNo);
    if (s != OK)
        return s;

    Page *pagePtr;
    if (file->readPage(PageNo, pagePtr) != OK)
        return UNIXERR;
    bufPool[frameNo] = *pagePtr;

    if (hashTable->insert(file, PageNo, frameNo) != OK)
        return HASHTBLERROR;

    bufTable[frameNo].Set(file, PageNo);

    page = &bufPool[frameNo];
    return OK;
}

const Status BufMgr::unPinPage(File *file, const int PageNo,
                               const bool dirty)
{
    int frameNo;
    if (hashTable->lookup(file, PageNo, frameNo) == HASHNOTFOUND)
        return HASHNOTFOUND;

    if (bufTable[frameNo].pinCnt == 0)
        return PAGENOTPINNED;
    else
        bufTable[frameNo].pinCnt--;

    if (dirty)
        bufTable[frameNo].dirty = 1;

    return OK;
}

const Status BufMgr::allocPage(File *file, int &pageNo, Page *&page)
{
    if (file->allocatePage(pageNo) == UNIXERR)
    {
        return UNIXERR;
    }
    int frameNum;
    if (allocBuf(frameNum) == BUFFEREXCEEDED)
    {
        return BUFFEREXCEEDED;
    }

    if (hashTable->insert(file, pageNo, frameNum) == HASHTBLERROR)
    {
        return HASHTBLERROR;
    }
    bufTable[frameNum].Set(file, pageNo);

    page = &bufPool[frameNum];
    return OK;
}

const Status BufMgr::disposePage(File *file, const int pageNo)
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

const Status BufMgr::flushFile(const File *file)
{
    Status status;

    for (int i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &(bufTable[i]);
        if (tmpbuf->valid == true && tmpbuf->file == file)
        {

            if (tmpbuf->pinCnt > 0)
                return PAGEPINNED;

            if (tmpbuf->dirty == true)
            {
#ifdef DEBUGBUF
                cout << "flushing page " << tmpbuf->pageNo
                     << " from frame " << i << endl;
#endif
                if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
                                                      &(bufPool[i]))) != OK)
                    return status;

                tmpbuf->dirty = false;
            }

            hashTable->remove(file, tmpbuf->pageNo);

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
    BufDesc *tmpbuf;

    cout << endl
         << "Print buffer...\n";
    for (int i = 0; i < numBufs; i++)
    {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char *)(&bufPool[i])
             << "\tpinCnt: " << tmpbuf->pinCnt;

        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}
