
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
    ixFileHandle.setHandle(*handle);

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
    return 0;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    FileHandle* handle = &ixfileHandle.getHandle();
    if (pfm->closeFile(*handle) == -1) return -1;
    delete handle;

    return 0;
}

RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    // extract root
    void *child = ixFileHandle.getRoot();
    void *parent = NULL;
    int leftPageNum;
    
    // Loop over traverse and save left and right pointers until leaf page
    while(true) {
        // if node == null then we need to create a leaf page
        if (traverse(child, parent, key, attribute, ixFileHandle, leftPageNum) == -1) return -1;

        // if child is null (first entry into a non-leaf node) 
        if(child == NULL) {
            // insert director into node
            child = malloc(PAGE_SIZE);
            int pageNum = ixFileHandle.initializeNewNode(child, TypeLeaf);

            // We should not need to test for enough space,
            // otherwise we have a larger issue at hand
            insertDirector(parent, key, attribute, pageNum, ixFileHandle);

            // Link children together
            ixFileHandle.setLeftPointer(child, leftPageNum);  
            void *leftChild = malloc(PAGE_SIZE);
            ixFileHandle.getHandle().readPage(leftPageNum, leftChild);
            ixFileHandle.setRightPointer(leftChild, pageNum);

            // we need to write all nodes to file (Parent, leftChild and rightChild);
            ixFileHandle.getHandle().writePage(leftPageNum, leftChild);
            ixFileHandle.getHandle().writePage(pageNum, child);
            ixFileHandle.getHandle().writePage(0, parent);
        }

        // test if leaf node
        if (getNodeType(child) == TypeLeaf) {
            break;
        }
    }

    // If the node does not have enough space, we need to split the node
    if(!hasEnoughSpace(child, attribute)) {
        // if not enough space we need to split
        //splitChild();
    }
    
    // Create the new page
    void *node;
    //initializeNewNode(node, TypeLeaf)
    // Initialize the page with left and right pointers
    
    // Insert record
    
    // Compact/shift for insert 
   
   
    return 0;
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

RC IndexManager::traverse(void * &child, void * &parent
                                       , const void *key
                                       , const Attribute &attribute
                                       , IXFileHandle &ixfileHandle
                                       , int leftPageNum) {
    // next if node is empty
    parent = child;
    child = NULL;
    int offset = 0;
    int counter = 0;

    switch(attribute.type) {
        case TypeInt:
            while(true) {
                // directorPage is the page on the right
                int leftPage, rightPage, directorKey, cKey;
                memcpy(&leftPage, (char *) parent + offset, sizeof(int));
                offset += sizeof(int);
                memcpy(&directorKey, (char *) parent + offset, sizeof(int));
                offset += sizeof(int);
                memcpy(&rightPage, (char *) parent + offset, sizeof(int));
                leftPageNum = offset;
                
                // test if director page is zero AND its the first entry in a non-leaf node
                if (rightPage == 0 && counter == 0) {
                    return 0;
                }
                
                // extract the key to compare to
                memcpy(&cKey, (char *) key, sizeof(int));

                // if this is true go left
                if (cKey < directorKey || rightPage == 0) {
                   return ixfileHandle.getHandle().readPage(leftPage, child);
                } 
                counter++;
            }
            
            break;
        case TypeReal:
            return -1; 
        case TypeVarChar:
            // compare strings
            return -1;
            break;
        default:
            return -1;
    }
}


RC IndexManager::insertDirector(void *node, const void *key, const Attribute &attribute, int nextPageNum, IXFileHandle &ixFileHandle) {
    void *director;
    int size = 0;
    int offset = 0;
    void *extractionKey;

    // create the director and save its length
    switch(attribute.type) {
        case TypeInt:
            director = malloc(2 * sizeof(int));
            memcpy(director, (char*)key, sizeof(int));
            size += sizeof(int);
            memcpy((char *) director + size, &nextPageNum, sizeof(int));
            size += sizeof(int);

            int leftPage, rightPage, directorKey, comparisonKey;
            while(getDirectorAtOffset(offset, node, leftPage, rightPage, extractionKey, attribute) != -1) {
                memcpy(&directorKey, (char*) extractionKey, sizeof(int));

                // test if director page is zero in order to break out
                if (rightPage == 0) {
                    return 0;
                }

                // extract the key to compare to
                memcpy(&comparisonKey, (char *) key, sizeof(int));

                // if this is true go left
                if (directorKey > comparisonKey) {
                   break;
                }
            }
            break;
        case TypeReal:
            director = malloc(2 * sizeof(int));
            memcpy(&director, (char*)key, sizeof(float));
            size += sizeof(float);
            memcpy((char *) director + size, &nextPageNum, sizeof(int));
            size += sizeof(int);
            break;
        case TypeVarChar:
            int length;
            memcpy(&length, (char *) key, sizeof(int));
            //director = malloc(sizeof(int) + length));
            //memcpy(&director)
            size = length + sizeof(int);
            break;
        default:
            return -1;
    } 

    // Calculate the size of the node data that needs to be shifted
    int freeSpace = ixFileHandle.getFreeSpace(node);
    int shiftSize = DEFAULT_FREE - freeSpace - offset;
    void* shiftData;

    // Save data into the temp value
    memcpy((char*)shiftData, (char*)node + offset, shiftSize);
    
    // insert the new director here 
    memcpy((char*)node + offset, (char*)director, size);
    offset += size;

    // reinsert the shifted data
    memcpy((char*)node + offset, shiftData, shiftSize);

    // update Freespace
    freeSpace -= size;
    ixFileHandle.setFreeSpace(node, freeSpace);
}

NodeType IndexManager::getNodeType(void *node) {
    NodeType type;
    memcpy(&type, (char *) node + NODE_TYPE, sizeof(byte));

    return type;
}

RC IndexManager::getDirectorAtOffset(int &offset, void* node, int &leftPointer, int &rightPointer, void* key, const Attribute &attribute) {
    switch(attribute.type) {
        case TypeInt:
            memcpy(&leftPointer, (char*) node + offset, sizeof(int));
            offset += sizeof(int);
            memcpy(key, (char*) node + offset, sizeof(int));
            offset += sizeof(int);
            memcpy(&rightPointer, (char*) node + offset, sizeof(int));
            break;
        case TypeReal:
            memcpy(&leftPointer, (char*) node + offset, sizeof(int));
            offset += sizeof(int);
            memcpy(key, (char*) node + offset, sizeof(float));
            offset += sizeof(float);
            memcpy(&rightPointer, (char*) node + offset, sizeof(int));
            break;
        case TypeVarChar:
            memcpy(&leftPointer, (char*) node + offset, sizeof(int));
            offset += sizeof(int);

            int length;
            memcpy(&length, (char*) node + offset, sizeof(int));
            memcpy(key, (char*) node + offset, sizeof(int));
            offset += sizeof(int);

            memcpy(key, (char*) node + offset, length);
            offset += length;
            memcpy(&rightPointer, (char*) node + offset, sizeof(int));
            break;
        default:
            return -1;
    }

    // If left pointer == 0 return -1, this means there are no more directors
    // in the node.
    if (leftPointer == 0) return -1;

    return 0;
}


void IXFileHandle::setLeftPointer(void *node, int leftPageNum) {
    memcpy((char *) node + NODE_LEFT, &leftPageNum, sizeof(int));    
}

void IXFileHandle::setRightPointer(void *node, int rightPageNum) {
    memcpy((char *) node + NODE_RIGHT, &rightPageNum, sizeof(int));    
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
    return handle->collectCounterValues(readPageCount, writePageCount, appendPageCount);
}

int IXFileHandle::initializeNewNode(void *data, NodeType type) {
    // initially set the page to 4096 0's
    memset(data, 0, PAGE_SIZE);

    // initializes the free space slot with the free space value
    int freeSpace = DEFAULT_FREE - sizeof(int);
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
        int pointer = this->getAvailablePageNumber() + 1;
        memcpy(data, &pointer, sizeof(int));
    }
}

int IXFileHandle::getFreeSpace(void *data) {
    int freeSpace;
    memcpy(&freeSpace, (char*)data + NODE_FREE, sizeof(int));

    return freeSpace;
}

int IXFileHandle::getLeftPointer(void *data) {
    int left;
    memcpy(&left, (char*)data + NODE_LEFT, sizeof(int));

    return left;
}

int IXFileHandle::getRightPointer(void *data) {
    int right;
    memcpy(&right, (char*)data + NODE_RIGHT, sizeof(int));

    return right;
}

int IXFileHandle::getAvailablePageNumber() {
    // If there is a page open in freePages use one of those first to reduce File size
    if (this->freePages.size() != 0) {
       int freePage = freePages.front();
       freePages.erase(freePages.begin());
       return freePage;
    }

    // else return numPages
    return handle->numPages;
}

void IX_PrintError (RC rc)
{
   
}
