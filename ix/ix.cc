
#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
    pfm = PagedFileManager::instance();
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    return pfm->createFile(fileName);
}

RC IndexManager::destroyFile(const string &fileName)
{
    return pfm->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixFileHandle)
{
    FileHandle* handle = new FileHandle;

    // open file
    if (pfm->openFile(fileName, *handle) == -1) return -1;

    // append the root page if the file is empty
    if (handle->numPages == 0) {
        // Initialize the root page
        void *data = malloc(PAGE_SIZE);
        if (ixFileHandle.initializeNewNode(data, TypeNode) == -1) {
            return -1;
        }

        if (handle->appendPage(data) == -1) {
            return -1;
        }

        ixFileHandle.setRoot(data);
    }

    ixFileHandle.setHandle(*handle);

    return 0;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    FileHandle* handle = &ixfileHandle.getHandle();
    if (pfm->closeFile(*handle) == -1) return -1;
    delete handle;

    return 0;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{



    return -1;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
}

bool IndexManager::hasEnoughSpace(void *data, const Attribute &attribute) {
    NodeType type;
    memcpy(&type, (char*)data + NODE_TYPE, sizeof(byte));

    // Node type will be used to determine if they are <Key, ridlist> pairs or
    // <pointer, key, pointer> groups

    int freeSpace;
    memcpy(&freeSpace, (char*)data + NODE_FREE, sizeof(int));

    return true;
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return -1;
}

RC IX_ScanIterator::close()
{
    return -1;
}


IXFileHandle::IXFileHandle()
{
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    return -1;
}

int IXFileHandle::initializeNewNode(void *data, NodeType type) {
    // initially set the page to 4096 0's
    memset(data, 0, PAGE_SIZE);

    // initializes the free space slot with the free space value
    int freeSpace = PAGE_SIZE - 5;
    memcpy((char*)data + NODE_FREE, &freeSpace, sizeof(int)); // node free (int) + node type (byte) = 5

    // sets the node type
    int nodeType;
    switch (type) {
        case TypeNode:
            nodeType = 0;
            break;
        case TypeLeaf:
            nodeType = 1;
            break;
        default:
            return -1;
    }

    // initializes the node type slot with node type
    memcpy((char*)data + NODE_TYPE, &nodeType, sizeof(byte));

    // if the node type is not a leaf, initialize the first pointer
    if (type == TypeNode) {
        int pointer = this->getAvailablePageNumber();
        memcpy(data, &pointer, sizeof(int));
    }
}

int IXFileHandle::getAvailablePageNumber() {
    // If there is a page open in freePages use one of those first to reduce File size
    if (this->freePages.size() != 0) {
        return this->freePages[0];
    }

    // else return numPages
    return this->handle->numPages + 1;
}

void IX_PrintError (RC rc)
{
}
