
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
    void *data = malloc(PAGE_SIZE);
    // open file
    if (pfm->openFile(fileName, *handle) == -1) return -1;
    ixFileHandle.setHandle(*handle);

    // append the root page if the file is empty
    if (handle->numPages == 0) {
        // Initialize the root page
        if (ixFileHandle.initializeNewNode(data, TypeRoot) == -1) {
            return -1;
        }

        if (handle->appendPage(data) == -1) {
            return -1;
        }
    } else {
        ixFileHandle.getHandle().readPage(0, data);
    }

    ixFileHandle.setRoot(data);
    ixFileHandle.getHandle().currentPageNum = 0; // Fix to compensate for close/open file
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
    int childPageNum;
    
    // Loop over traverse and save left and right pointers until leaf page
    while(true) {
        // Test if node is full (This is what makes top-down, top-down
        if(!hasEnoughSpace(child, attribute)) {
            // if not enough space we need to split
            //splitChild();
        }

        // if node == null then we need to create a leaf page
        if (traverse(child, parent, key, attribute, ixFileHandle, childPageNum) == -1) {
            return -1;
        }

        // if child is null (first entry into the root node, SHOULD NEVER HAPPEN OTHERWISE)
        if(child == NULL) {
            void *leftPointerData = malloc(PAGE_SIZE);
            void *rightPointerData = malloc(PAGE_SIZE);

            // Initialize Left Pointer
            int leftPointerNum = ixFileHandle.getAvailablePageNumber();
            ixFileHandle.initializeNewNode(leftPointerData, TypeLeaf);
            ixFileHandle.getHandle().appendPage(leftPointerData);
            ixFileHandle.getHandle().readPage(leftPointerNum, leftPointerData);  // REMOVE THIS IF WORKS

            // Initialize Right Pointer
            int rightPointerNum = ixFileHandle.getAvailablePageNumber();
            ixFileHandle.initializeNewNode(rightPointerData, TypeLeaf);
            ixFileHandle.getHandle().appendPage(rightPointerData);

            // Link left pointer to right pointer
            ixFileHandle.setRightPointer(leftPointerData, rightPointerNum);

            // Link left page to data (I realize this is an additional (write/append, but it will only happen 1, ever, so who cares ahha)
            ixFileHandle.getHandle().writePage(leftPointerNum, leftPointerData);

            // Write left, key, right data to the parent page
            int offSet = 0;
            int keyLength = getKeyLength(key, attribute);
            memcpy((char*) parent + offSet, &leftPointerNum, sizeof(int));
            offSet += sizeof(int);
            memcpy((char*) parent + offSet, key, keyLength);
            offSet += keyLength;
            memcpy((char*) parent + offSet, &rightPointerNum, sizeof(int));

            // Write page to memory (GOING TO ASSUME ROOT)
            ixFileHandle.setRoot(parent);

            // Re-run insert Entry with the newly added root key and pages
            return insertEntry(ixFileHandle, attribute, key, rid);
        }
                
        // test if leaf node
        if (ixFileHandle.getNodeType(child) == TypeLeaf) {
            // do we maybe need to  check for enough space here? and then split?
            break;
        }
    }
       
    // If the node does not have enough space, we need to split the node
    if(!hasEnoughSpace(child, attribute)) {
        // if not enough space we need to split
        //splitChild();
    }

    // Here we are guaranteed to have a leaf node in child and we can safely insert 
    // the new data into the leaf node;
    if (insertIntoLeaf(ixFileHandle, child, key, attribute, rid) == -1) {
        return -1;
    }
     
    // write the node to file
    if(ixFileHandle.getHandle().writePage(childPageNum, child) == -1) {
        return -1;
    }
    return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    // extract root
    void *child = ixFileHandle.getRoot();
    void *parent = NULL;
    int childPageNum;

    // Loop over traverse and save left and right pointers until leaf page
    while(true) {
        // Test if node is full (This is what makes top-down, top-down
        // TODO NOT SURE IF WE WANT THIS
        if(!hasEnoughSpace(child, attribute)) {
            // if not enough space we need to split
            //splitChild();
        }

        // if node == null then we need to create a leaf page
        if (traverse(child, parent, key, attribute, ixFileHandle, childPageNum) == -1) {
            return -1;
        }

        // if child is null (first entry into the root node, SHOULD NEVER HAPPEN OTHERWISE)
        if(child == NULL) {
            return -1;
        }

        // test if leaf node
        if (ixFileHandle.getNodeType(child) == TypeLeaf) {
            // do we maybe need to  check for enough space here? and then split?
            break;
        }
    }

    // TODO NOT SURE IF THIS IS NEEDED
    if(!hasEnoughSpace(child, attribute)) {
        // if not enough space we need to split
        //splitChild();
    }

    // Remove node from leaf
    deleteFromLeaf(ixFileHandle, child, key, attribute, rid);

    // write the node to file
    if(ixFileHandle.getHandle().writePage(childPageNum, child) == -1) {
        return -1;
    }

    return 0;
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

    switch(type) {
        case TypeRoot:
        case TypeNode:
            break;
        case TypeLeaf:
            break;
        default:
            return false;
    }

    return true;
}

RC IndexManager::traverse(void * &child, void * &parent
                                       , const void *key
                                       , const Attribute &attribute
                                       , IXFileHandle &ixfileHandle
                                       , int &leftPageNum) {
    // next if node is empty
    parent = child;
    child = NULL;
    int offset = 0;
    int counter = 0;

    switch(attribute.type) {
        case TypeInt:
            // extract the key to compare to
            int cKey;
            memcpy(&cKey, (char *) key, sizeof(int));

            while(true) {
                // directorPage is the page on the right
                int leftPage, rightPage, directorKey;
                memcpy(&leftPage, (char *) parent + offset, sizeof(int));
                offset += sizeof(int);
                memcpy(&directorKey, (char *) parent + offset, sizeof(int));
                offset += sizeof(int);
                memcpy(&rightPage, (char *) parent + offset, sizeof(int));
                leftPageNum = leftPage;
                
                // test if director page is zero AND its the first entry in a non-leaf node
                if (rightPage == 0 && counter == 0) {
                    return 0;
                }

                // if this is true go left
                if (cKey < directorKey || rightPage == 0) {
                    child = malloc(PAGE_SIZE);
                    ixfileHandle.getHandle().readPage(leftPage, child);
                    return 0;
                } 
                counter++;
            }
            
            return 0;
        case TypeReal:
            return -1; 
        case TypeVarChar:
            // compare strings
            return -1;
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
    return 0;
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

RC IndexManager::insertIntoLeaf(IXFileHandle &ixFileHandle
                                        , void *child
                                        , const void *key
                                        , const Attribute &attribute
                                        , const RID &rid) {
    // calculate the freeSpaceOffset
    int freeSpace = IXFileHandle::getFreeSpace(child);
    int freeSpaceOffset = IXFileHandle::getFreeSpaceOffset(freeSpace);
    int newOffset = 0;
    int nextKeyOffset = 0;
    int newDataOffset = 0;
    int sizeOfNewData, sizeOfShiftedData;
    void *newData;
    void *shiftedData;

    // first we need to determine what type of attribute we have
    switch(attribute.type) {
        case TypeInt:
            // local variale used only by TypeInt
            int incomingKey;
            int leafKey;

            // we can calculate the size of the new data if the key is not equal to 
            // any key in the leaf [key, # of RIDs, rid]
            sizeOfNewData = (2 * sizeof(int)) + RID_SIZE;
            sizeOfShiftedData = 0;

            // extract the keys that will be compared
            memcpy(&incomingKey, (char *) key, sizeof(int));

            // loop throught he keys until we have reached the end
            while (nextKeyOffset < freeSpaceOffset) {
                // lets first extract the key and compare
                memcpy(&leafKey, (char * ) child + nextKeyOffset, sizeof(int));
                
                // if the incoming key is less than the leafKey then we need to 
                // return the prevKey Offset
                if (incomingKey < leafKey) {
                    // calculate the size of the shifting data and set the newOffset
                    // to the previous 
                    sizeOfShiftedData = freeSpaceOffset - nextKeyOffset;
                    newOffset = nextKeyOffset;
                    
                    // lastly we need to build the new data that will be inserted
                    newData = malloc((2 *sizeof(int)) + RID_SIZE);
                    createNewLeafEntry(newData, key, attribute, rid);
                    break; 
                }
                // if the incoming key and the leafkey are the same then we just
                // append the rid the list
                if (incomingKey == leafKey) {
                    // here we must insert a new RID into the list
                    sizeOfNewData = RID_SIZE;

                    // get the offset of the last RID in the list
                    int numberOfRIDs = getNumberOfRids(child, nextKeyOffset + sizeof(int));

                    // get the offset of the last RID in the list
                    newOffset = nextKeyOffset + (2 * sizeof(int)) + (numberOfRIDs * RID_SIZE);

                    // update the number of RIDs in key
                    numberOfRIDs += 1;
                    memcpy((char*)child + (nextKeyOffset + sizeof(int)), &numberOfRIDs, sizeof(int));

                    // calculate the data that will be shifted to the right
                    sizeOfShiftedData = freeSpaceOffset - newOffset;

                    // create the new data which is only an rid
                    newData = malloc(RID_SIZE);
                    memcpy((char *) newData + newDataOffset, &rid.pageNum, sizeof(int));
                    newDataOffset += sizeof(int);
                    memcpy((char *) newData + newDataOffset, &rid.slotNum, sizeof(int));
                    break;
                }
                // move to the next key
                nextKeyOffset = getNextKeyOffset(nextKeyOffset + sizeof(int), child);
            }
            // if we have reached the end then we need to create the data for insertion
            // at the end of the leaf node
            if (nextKeyOffset == freeSpaceOffset) {
                // just insert at the end
                newOffset = freeSpaceOffset; 
                
                // lastly we need to build the new data that will be inserted
                newData = malloc((2 *sizeof(int)) + RID_SIZE);
                createNewLeafEntry(newData, key, attribute, rid);
            } 

            // we need to shift the data to the right and insert the new data
            shiftedData = malloc(sizeOfShiftedData);
            memcpy((char *) shiftedData, (char *) child + newOffset, sizeOfShiftedData);

            // shift to the right
            memcpy((char *) child + newOffset + sizeOfNewData, (char *) shiftedData, sizeOfShiftedData);

            // enter new data
            memcpy((char *) child + newOffset, (char *) newData, sizeOfNewData);

            // update freespace
            ixFileHandle.setFreeSpace(child, freeSpace - sizeOfNewData);
            return 0;
        case TypeReal:
            // do work for a float
            return 0;
        case TypeVarChar:
            // do work for a varChar
            return 0;
        default:
            // shit is borked!
            return -1;
    }
}

RC IndexManager::deleteFromLeaf(IXFileHandle &ixFileHandle
                                , void *child
                                , const void *key
                                , const Attribute &attribute
                                , const RID &rid) {
    // calculate the freeSpaceOffset
    int freeSpace = IXFileHandle::getFreeSpace(child);
    int freeSpaceOffset = IXFileHandle::getFreeSpaceOffset(freeSpace);
    int nextKeyOffset = 0;

    // Save the number of bits for key length (this will be used to read in length size
    int keyLengthSize;
    switch (attribute.type) {
        case TypeInt:
        case TypeReal:
            keyLengthSize = 0;
            break;
        case TypeVarChar:
            keyLengthSize = 4;
            break;
        default:
            return -1;
    }

    // Iterate over keys
    int keyLength;
    void *comparisonKey;
    while (nextKeyOffset < freeSpaceOffset) {
        memcpy(&keyLength, (char*)child + nextKeyOffset, keyLengthSize);
        // Tease out the key from the node
        int keySize;
        if (keyLengthSize != 0) {
            keySize = sizeof(int) + keyLength;
        } else {
            keySize = sizeof(int);

        }

        // Initialize the comparison key
        comparisonKey = malloc(keySize);
        memcpy(comparisonKey, (char*)child + nextKeyOffset, keySize);

        nextKeyOffset += keySize;

        int comparisonResult = compareKeys(key, comparisonKey, attribute);

        // found the key, now remove either the whole key, or just an RID
        if (comparisonResult == 0) {
            // get the RID count
            int ridCount = getNumberOfRids(child, nextKeyOffset);

            // iterate over all the rids
            bool ridFound = false;
            for (int i = 0; i < ridCount; i++) {
                int pageNum, slotNum;
                nextKeyOffset += sizeof(int);
                memcpy(&pageNum, (char*)child + nextKeyOffset , sizeof(int));
                nextKeyOffset += sizeof(int);
                memcpy(&slotNum, (char*)child + nextKeyOffset , sizeof(int));
                nextKeyOffset += sizeof(int);

                // rid found!
                if (pageNum == rid.pageNum && slotNum == rid.slotNum) {
                    ridFound = true;
                    break;
                }
            }

            if (!ridFound) {
                // rid does not exist in list
                return -1;
            } else {
                // create temp pointer for content to be shifted
                int shiftSize = freeSpaceOffset - nextKeyOffset;
                void *shiftContent = malloc(shiftSize);

                // copy the content that is going to be shifted over
                memcpy(shiftContent, (char*)child + nextKeyOffset, shiftSize);

                // calculate the length of data being deleted
                int deleteLength;
                if (ridCount > 1) {
                    deleteLength = RID_SIZE;
                }
                else {
                    deleteLength = RID_SIZE + keySize + sizeof(int);
                }

                // Zero out the data where key + shift data used to lie
                memset((char*)child + (nextKeyOffset - deleteLength), 0, shiftSize + deleteLength);

                // copy over the previous RID and key and upate the freespace
                memcpy((char*)child + (nextKeyOffset - RID_SIZE), shiftContent, shiftSize);
                ixFileHandle.setFreeSpace(child, freeSpace + deleteLength);

            }
        } else if (comparisonResult == -1) {
            // key does not exist in list
            return -1;
        } else {
            nextKeyOffset = getNextKeyOffset(nextKeyOffset, child);
        }

    }

    return 0;
}

int IndexManager::compareKeys(const void *key1, const void *key2, const Attribute attribute) {
    switch (attribute.type) {
        case TypeInt:
        case TypeReal:
            int keyOne, keyTwo;
            memcpy(&keyOne, (char*)key1, sizeof(int));
            memcpy(&keyTwo, (char*)key2, sizeof(int));

            return keyOne < keyTwo;
        case TypeVarChar:
            int keyOneLength, keyTwoLength;
            memcpy(&keyOneLength, (char*)key1, sizeof(int));
            memcpy(&keyTwoLength, (char*)key2, sizeof(int));

            // get the smaller of the two to do the character comparisons
            int compareLen = (keyOneLength > keyTwoLength) ? keyTwoLength : keyOneLength;

            // iterate over the keys and decide which is longer
            char keyOneVal;
            char keyTwoVal;
            for (int i = 0; i < compareLen; i++) {
                memcpy(&keyOneVal, (char*)key1 + (i * sizeof(char)), sizeof(char));
                memcpy(&keyTwoVal, (char*)key2 + (i * sizeof(char)), sizeof(char));

                if (keyOneVal > keyTwoVal) return 1;
                if (keyOneVal < keyTwoVal) return -1;
            }

            // both match up to the shortest string, therefore, return the value
            // either the longest string or that they are the same length
            if (keyOneLength > keyTwoLength) return 1;
            if (keyOneLength < keyTwoLength) return -1;

            // both equal
            return 0;
    }

    return -1;
}

int IndexManager::getNumberOfRids(void *node, int RIDnumOffset) {
    int numberOfRIDs;
    memcpy(&numberOfRIDs, (char *) node + RIDnumOffset, sizeof(int));
    return numberOfRIDs;
}

void IndexManager::createNewLeafEntry(void *data, const void *key, const Attribute &attribute, const RID &rid) {
    int numRids = 1;
    int offset = 0;

    switch (attribute.type) {
        case TypeInt:
        case TypeReal:
            memcpy((char *) data + offset, key, sizeof(int));
            offset += sizeof(int);
            memcpy((char *) data + offset, &numRids, sizeof(int));
            offset += sizeof(int);
            memcpy((char *) data + offset, &rid.pageNum, sizeof(int));
            offset += sizeof(int);
            memcpy((char *) data + offset, &rid.slotNum, sizeof(int));
            break;
        case TypeVarChar:
            break;
        default:
            break;
    }
}

// this function takes the the offset of a key entry plus the key size, so 
// that we are placed at the # of RID's slot
int IndexManager::getNextKeyOffset(int RIDnumOffset, void *node) {
    // we need to extract the number RID's that exist in the node
    return RIDnumOffset + sizeof(int) + getNumberOfRids(node, RIDnumOffset) * RID_SIZE;
}

int IndexManager::getKeyLength(const void *key, Attribute attr) {
    int length;

    switch(attr.type) {
        case TypeInt:
            length = sizeof(int);
            break;
        case TypeReal:
            length = sizeof(int);
            break;
        case TypeVarChar:
            memcpy(&length, (char*) key, sizeof(int));
            length += 4;
            break;
        default:
            return -1;
    }

    return length;
}

void IXFileHandle::setRightPointer(void *node, int rightPageNum) {
    memcpy((char *) node + NODE_RIGHT, &rightPageNum, sizeof(int));    
}

NodeType IXFileHandle::getNodeType(void *node) {
    NodeType type;
    memcpy(&type, (char *) node + NODE_TYPE, sizeof(byte));

    return type;
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
    int freeSpace = DEFAULT_FREE;
    memcpy((char*)data + NODE_FREE, &freeSpace, sizeof(int)); // node free (int) + node type (byte) = 5

    // sets the node type
    byte nodeType;
    switch (type) {
        case TypeNode:
            nodeType = 0;
            break;
        case TypeLeaf:
            nodeType = 1;
            break;
        case TypeRoot:
            nodeType = 2;
            break;
        default:
            return -1;
    }

    // initializes the node type slot with node type
    memcpy((char*)data + NODE_TYPE, &nodeType, sizeof(byte));

    // if the node type is not a leaf, initialize the first pointer
//    if (type == TypeNode) {
//        int pointer = this->getAvailablePageNumber() + 1;
//        memcpy(data, &pointer, sizeof(int));
//    }
    // what does this need to return?
    return 0;
}

int IXFileHandle::getFreeSpace(void *data) {
    int freeSpace;
    memcpy(&freeSpace, (char*)data + NODE_FREE, sizeof(int));

    return freeSpace;
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
