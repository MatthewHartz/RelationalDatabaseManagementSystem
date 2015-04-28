#include "pfm.h"

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}

    
PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
    delete _pf_manager;
}


RC PagedFileManager::createFile(const string &fileName)
{
    // we must first test to see if the file already exists
    ifstream test_file(fileName);
    if (test_file.good()) {
        return -1;
    } else {
        ofstream new_file(fileName, ios::binary);
        if (new_file.is_open()) {
            new_file.close();
            return 0;
        } else {
            return -1;
        }
    }
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    if (!std::remove(fileName.c_str())) {
        return 0;
    } else {
        return -1;
    }
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    // check if the file exists
    struct stat buffer;
    if (stat (fileName.c_str(), &buffer) == 0) {
        if(fileHandle.handle != NULL) {
            // this means the handle is associated with another file
            return 0;
        }
        // link this new file handle to this opened file
        fileHandle.handle = new fstream(fileName, ios::in | ios::out | ios::binary | ios::ate);

        // We need to scan the file and grab the free space available in a list
        // and we need to set the currentPage number and currentPage if the file is not empty
        fileHandle.handle->seekg(0, ios::end);
        int length = fileHandle.handle->tellg();
        // if the file is not empty then we need to scan it
        if (length != 0) {
            int numPages = fileHandle.numPages = length / PAGE_SIZE;
            fileHandle.currentPageNum = fileHandle.numPages - 1;

            void *page = malloc(PAGE_SIZE);
            for (int i = 0; i < numPages; i++) {
                // read the page and extract the free space
                fileHandle.readPage(i, page);

                // we need the number of records and freeSpaceOffset to calculate the freeSpace
                int numRecords;
                memcpy(&numRecords, (char *) page + N_OFFSET, sizeof(int));

                int freeSpaceOffset;
                memcpy(&freeSpaceOffset, (char *) page + F_OFFSET, sizeof(int));

                // Now we can derive the freeSpace
                int freeSpace = PAGE_SIZE - (freeSpaceOffset + (numRecords * SLOT_SIZE) + META_INFO);

                // if the free space isn't in range then the page isn't formated right
                if (freeSpace < 0 || freeSpace > PAGE_SIZE) {
                    free(page);
                    return 0;
                }
                fileHandle.freeSpace.push_back(freeSpace); 
            }
            fileHandle.currentPage = page;
        }            
        return 0;
    } else {
        return -1;
    }
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    if (fileHandle.handle == NULL) {
        // file is not associated with a file (error)
        return -1;
    }
    // check to see if the file is open and close it
    if (fileHandle.handle->is_open()) {
        // the file exists and its open
        if (fileHandle.currentPage != NULL) {
            fileHandle.writePage(fileHandle.currentPageNum, fileHandle.currentPage); 
            fileHandle.currentPage = NULL;
            fileHandle.currentPageNum = -1;
        }
        // clear the free space list
        fileHandle.freeSpace.clear();
        fileHandle.numPages = 0;

        fileHandle.handle->close();
        delete fileHandle.handle;
        fileHandle.handle = NULL;
        return 0;
    }
    return -1;
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    numPages = 0;
    handle = NULL;
    currentPage = NULL;
    currentPageNum = -1;
}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
    /*infile->seekg(0, ios::end);
    int length = infile->tellg();

    // Added length guard to protect reading from outside of bounds
    if ((pageNum * PAGE_SIZE) >= length) {
        return -1;
    }*/

    if (pageNum >= numPages) {
        return -1;
    }

    if (handle != NULL && handle->is_open()) {
        handle->seekg(pageNum * PAGE_SIZE, ios::beg);
        handle->read(((char *) data), PAGE_SIZE);
        readPageCounter++;
        return 0;
    } else {
        return -1;
    }
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    if (handle != NULL && handle->is_open()) {
        handle->seekp(pageNum * PAGE_SIZE, ios::beg);
        handle->write(((char *) data), PAGE_SIZE);
        writePageCounter++;
        return 0;
    } else {
        return -1;
    }
}


RC FileHandle::appendPage(const void *data)
{
    if (handle != NULL && handle->is_open()) {
        handle->seekp(0, ios::end);
        handle->write(((char *) data), PAGE_SIZE);
        appendPageCounter++;
        numPages++;
        return 0;
    } else {
        return -1;
    }
}


unsigned FileHandle::getNumberOfPages()
{
    return numPages;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
    return 0;
}
