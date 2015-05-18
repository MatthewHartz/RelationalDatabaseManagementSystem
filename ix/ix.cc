
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

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    // extract root
    void *child = ixfileHandle.getRoot();
    void *parent = NULL;
    int left, right;
    
    // Loop over traverse and save left and right pointers until leaf page
    while(true) {
        // if node == null then we need to create a leaf page
        if (traverse(child, parent, key, attribute, ixfileHandle) == -1) return -1;

        // if child is null (first entry into a non-leaf node) 
        if(child == NULL) {
            // insert director into node
            child = malloc(PAGE_SIZE);
            int pageNum = ixfileHandle.initializeNewNode(child, TypeLeaf);
            insertDirector(parent, key, attribute, pageNum);
            traverse(child, parent, key, attribute, ixfileHandle);
        }

        // test if leaf node
        /*
        if (getNodeType(child) == TypeLeaf) {
            break;
        }*/
    }
    if(!hasEnoughSpace(child, attribute)) {
        // if not enough space we need to split
        //splitChild();
    }
    
    // Create the new page
    void *node;
    //initializeNewNode(node, TypeLeaf)
    // Initialize the page with left and right pointers

    // Insert recrd
    
    // Compact/shift for insert 
   
   
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

RC IndexManager::traverse(void * &child, void * &parent, const void *key, const Attribute &attribute, IXFileHandle &ixfileHandle) {
    // next if node is empty
    parent = child;
    child = NULL;
    int offset = 0;

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
                
                // test if director page is zero in order to break out
                if (rightPage == 0) {
                    return 0;
                }
                
                // extract the key to compare to
                memcpy(&cKey, (char *) key, sizeof(int));

                // if this is true go left
                if (cKey < directorKey) {
                   return ixfileHandle.getHandle().readPage(leftPage, child);
                }  
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


RC IndexManager::insertDirector(void *node, const void *key, const Attribute &attribute, int nextPageNum) {
    // we added 4 bytes in consideration of the next pointer
    int size = sizeof(int);

    // create the director and save its length
    switch(attribute.type) {
        case TypeInt:
            size += sizeof(int);
            break;
        case TypeReal:
            size = sizeof(float);
            break;
        case TypeVarChar:
            int length;
            memcpy(&length, (char *) key, sizeof(int));
            size = length + sizeof(int);
            break;
        default:
            return -1;
    } 
     
    
    // scan through each key and determine where the new key will be placed
    

    // shift to the right for director length
    

    // insert the new director here 

    // 
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
