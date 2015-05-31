
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
    delete _index_manager;
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
    ixFileHandle.setHandle(handle);

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
        ixFileHandle.getHandle()->readPage(0, data);
    }

    ixFileHandle.setRoot(data);
    ixFileHandle.getHandle()->currentPageNum = 0; // Fix to compensate for close/open file
    return 0;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    FileHandle* handle = ixfileHandle.getHandle();
    void *root =ixfileHandle.getRoot();
    handle->writePage(ixfileHandle.getRootPageNum(), root);
    if (pfm->closeFile(*handle) == -1) return -1;
    delete handle;
    free(root);
    return 0;
}

RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    // extract root
    void *child = ixFileHandle.getRoot();
    void *parent = NULL;
    int childPageNum = ixFileHandle.getRootPageNum();
    int parentPageNum = 0;

    // Loop over traverse and save left and right pointers until leaf page
    while(true) {
        // Test if node is full (This is what makes top-down, top-down
        if(!hasEnoughSpace(child, attribute)) {
            // if not enough space we need to split
            splitChild(child, parent, attribute, ixFileHandle, key, childPageNum, parentPageNum);

            // if parent != root, free
            if (parentPageNum != ixFileHandle.getRootPageNum()) {
                if (parent != NULL) {
                    free(parent);
                    parent = NULL;
                }
            }

            if (child != NULL) {
                free(child);
                child = NULL;
            }
            return insertEntry(ixFileHandle, attribute, key, rid);
        }

        // if node == null then we need to create a leaf page
        if (getNextNodeByKey(child, parent, key, attribute, ixFileHandle, childPageNum, parentPageNum) == -1) {
            return -1;
        }

        // if child is null (first entry into the root node, SHOULD NEVER HAPPEN OTHERWISE)
        if(child == NULL) {
            void *leftPointerData = malloc(PAGE_SIZE);
            void *rightPointerData = malloc(PAGE_SIZE);

            // Initialize Left Pointer
            int leftPointerNum = ixFileHandle.getAvailablePageNumber();
            ixFileHandle.initializeNewNode(leftPointerData, TypeLeaf);
            ixFileHandle.getHandle()->appendPage(leftPointerData);

            // Initialize Right Pointer
            int rightPointerNum = ixFileHandle.getAvailablePageNumber();
            ixFileHandle.initializeNewNode(rightPointerData, TypeLeaf);
            ixFileHandle.getHandle()->appendPage(rightPointerData);

            // Link left pointer to right pointer
            ixFileHandle.setRightPointer(leftPointerData, rightPointerNum);

            // Link left page to data (I realize this is an additional (write/append, but it will only happen 1, ever, so who cares ahha)
            ixFileHandle.getHandle()->writePage(leftPointerNum, leftPointerData);

            // Write left, key, right data to the parent page
            int offSet = 0;

            switch (attribute.type) {
                case TypeReal:
                case TypeInt:
                    memcpy((char*) parent + offSet, &leftPointerNum, sizeof(int));
                    offSet += sizeof(int);
                    memcpy((char*) parent + offSet, key, sizeof(int));
                    offSet += sizeof(int);
                    memcpy((char*) parent + offSet, &rightPointerNum, sizeof(int));
                    offSet += sizeof(int);

                    break;
                case TypeVarChar:
                    memcpy((char*) parent + offSet, &leftPointerNum, sizeof(int));
                    offSet += sizeof(int);

                    int length;
                    memcpy(&length, (char*)key, sizeof(int));
                    int keySize = length + sizeof(int);
                    memcpy((char*)parent + offSet, (char*)key, keySize);
                    offSet += keySize;

                    memcpy((char*) parent + offSet, &rightPointerNum, sizeof(int));
                    offSet += sizeof(int);

                    break;
            }

            // update freespace
            int freeSpace = ixFileHandle.getFreeSpace(parent);
            ixFileHandle.setFreeSpace(parent, freeSpace - offSet);

            // Write page to memory (GOING TO ASSUME ROOT)
            ixFileHandle.setRoot(parent);

            // free the new child nodes
            if (leftPointerData != NULL) free(leftPointerData);
            if (rightPointerData != NULL) free(rightPointerData);

            // Re-run insert Entry with the newly added root key and pages
            return insertEntry(ixFileHandle, attribute, key, rid);
        }

        // test if leaf node
        NodeType type = ixFileHandle.getNodeType(child);
        if (type == TypeLeaf) {
            // do we maybe need to  check for enough space here? and then split?
            break;
        }
    }

    // If the node does not have enough space, we need to split the node
    if(!hasEnoughSpace(child, attribute)) {
        // if not enough space we need to split
        splitChild(child, parent, attribute, ixFileHandle, key, childPageNum, parentPageNum);

        // if parent != root, free
        if (parentPageNum != ixFileHandle.getRootPageNum()) {
            if (parent != NULL) {
                free(parent);
                parent = NULL;
            }
        }

        if (child != NULL) {
            free(child);
            child = NULL;
        }

        // Now the parent and children have been created/modified, reinsert into tree
        return insertEntry(ixFileHandle, attribute, key, rid);
    }

    // Here we are guaranteed to have a leaf node in child and we can safely insert
    // the new data into the leaf node;
    if (insertIntoLeaf(ixFileHandle, child, key, attribute, rid) == -1) {
        return -1;
    }

    // write the node to file
    if(ixFileHandle.getHandle()->writePage(childPageNum, child) == -1) {
        return -1;
    }

    // if parent != root, free
    if (parentPageNum != ixFileHandle.getRootPageNum()) {
        if (parent != NULL) {
            free(parent);
            parent = NULL;
        }
    }

    if (child != NULL) {
        free(child);
        child = NULL;
    }
    return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    // extract root
    void *child = ixFileHandle.getRoot();
    void *parent = NULL;
    int childPageNum = ixFileHandle.getRootPageNum();
    int parentPageNum;

    // Loop over traverse and save left and right pointers until leaf page
    while(true) {
        // Test if node is full (This is what makes top-down, top-down
        // TODO NOT SURE IF WE WANT THIS
        if(!hasEnoughSpace(child, attribute)) {
            // if not enough space we need to split
            //splitChild();
        }

        // if node == null then we need to create a leaf page
        if (getNextNodeByKey(child, parent, key, attribute, ixFileHandle, childPageNum, parentPageNum) == -1) {
            return -1;
        }

        // if child is null (first entry into the root node, SHOULD NEVER HAPPEN OTHERWISE)
        if (parent != ixFileHandle.getRoot()) {
            free(parent);
            parent = NULL;
        }
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
    if (deleteFromLeaf(ixFileHandle, child, key, attribute, rid) == -1) {
        return -1;
    }

    // write the node to file
    if(ixFileHandle.getHandle()->writePage(childPageNum, child) == -1) {
        return -1;
    }
    
    if (child != NULL) {
        free(child);
        child = NULL;
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
    // need to test and see if the ixFileHandle is valid
    if (ixfileHandle.getHandle() == NULL) {
        return -1;
    }


    // save the information needed to do a range based scan
    ix_ScanIterator.setHandle(ixfileHandle);
    ix_ScanIterator.setAttribute(attribute);
    ix_ScanIterator.setLowKeyValues(lowKey, lowKeyInclusive, attribute);
    ix_ScanIterator.setHighKeyValues(highKey, highKeyInclusive, attribute);

    // we need to save the first leaf node in the tree to begin a scan
    void *root = ixfileHandle.getRoot();
    void *parentNode = NULL;
    void *searchNode = NULL;
    searchNode = malloc(PAGE_SIZE);
    memcpy((char *) searchNode, (char *) root, PAGE_SIZE);
    int searchPageNum, searchOffset = 0;
    int parentPage;

    // find the first leaf node that contains the lowKey
    if (lowKey == NULL) {
        while(ixfileHandle.getNodeType(searchNode) != TypeLeaf) {
            // test the left node, if its null increase the offset and go right
            // not having a left node will only happen if the left node is dead (deleted)
            if(!ixfileHandle.isLeftNodeNull(searchNode, searchOffset, searchPageNum)) {
                ixfileHandle.getNode(searchPageNum, searchNode);
            } else if (!ixfileHandle.isRightNodeNull(searchNode, attribute, searchOffset, searchPageNum)) {
                ixfileHandle.getNode(searchPageNum, searchNode);
            } else {
                // this should not happen
            }
        }

        // once we have found our left-most leaf we need to save it to the scanner
        ix_ScanIterator.setLeafNode(searchNode);
    } else {
        // find the leaf node where the lowKey is located, we'll let geNextEntry() worry about inclusive
        while(ixfileHandle.getNodeType(searchNode) != TypeLeaf) {
            if(getNextNodeByKey(searchNode, parentNode, lowKey, attribute, ixfileHandle, searchPageNum, parentPage)) {
                if (searchNode != NULL) free(searchNode);
                if (parentNode != NULL) free(parentNode);
                return -1;
            }
        }
        ix_ScanIterator.setLeafNode(searchNode);
    }
    // let's set our type and functions
    switch(attribute.type) {
        case TypeInt:
            ix_ScanIterator.setType(ix_ScanIterator.getIntType);
            ix_ScanIterator.setFunc(ix_ScanIterator.compareInts);
            break;
        case TypeReal:
            ix_ScanIterator.setType(ix_ScanIterator.getRealType);
            ix_ScanIterator.setFunc(ix_ScanIterator.compareReals);
            break;
        case TypeVarChar:
            ix_ScanIterator.setType(ix_ScanIterator.getVarCharType);
            ix_ScanIterator.setFunc(ix_ScanIterator.compareVarChars);
            break;
        default:
            break;

    }
    if (searchNode != NULL) free(searchNode);
    if (parentNode != NULL) free(parentNode);

    return 0;
}

void IndexManager::printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const {
    void *root = ixFileHandle.getRoot();
    ofstream myfile("tree.txt");

    // do the initial call to printNode at the root
    int depth = 0;
    printNode(root, ixFileHandle, attribute, depth, myfile);

    myfile.close();
}

RC IndexManager::printNode(void *node, IXFileHandle &ixFileHandle, const Attribute &attribute, int depth, ofstream &myfile) const {
    vector<string> keys;
    vector<int> pointers;
    string depthString;


    // initialize the depth string, this is used to make the correct justifications
    for (int i = 0; i < depth; i++) {
        depthString += "\t";
    }

    // collect the keys based on what type of node
    NodeType type = ixFileHandle.getNodeType(node);
    int counter = 0;
    switch (type) {
        case TypeLeaf:
            getKeysInLeaf(ixFileHandle, node, attribute, keys);

            // print initial brace
            myfile << endl << depthString << "{";

            // print the keys
            myfile << "\"keys\":[";
            for (auto &key: keys) {
                if (counter > 0) {
                    myfile << ",";
                }
                myfile << "\"" << key << "\"";

                counter++;
            }

            // close the keys
            myfile << "]";

            // print closing brace
            myfile << "}";
            break;
        case TypeNode:
        case TypeRoot:
            getKeysInNonLeaf(ixFileHandle, node, attribute, keys, pointers);

            // print initial brace
            myfile << endl << depthString << "{" << endl;

            // print the keys
            myfile << depthString << "\"keys\":[";
            for (auto &key: keys) {
                if (counter > 0) myfile << ",";
                myfile << "\"" << key << "\"";
                counter++;
            }

            // close the keys
            myfile << "]," << endl;

            // recursively descend the tree printing out the children and their keys
            myfile << depthString << "\"children\":[";
            counter = 0;
            for (auto &pointer: pointers) {
                if (counter > 0) myfile << ",";
                void *nextNode = malloc(PAGE_SIZE);
                ixFileHandle.getHandle()->readPage(pointer, nextNode);
                printNode(nextNode, ixFileHandle, attribute, depth + 1, myfile);
                free(nextNode);
                counter++;
            }

            // wrap children in end bracket
            myfile << "]";

            // print closing brace
            myfile << endl << "}" << endl;
            break;
    }

    return 0;
}

bool IndexManager::hasEnoughSpace(void *data, const Attribute &attribute) {
    int type, freeSpace;
    memset(&type, 0, sizeof(int));
    memcpy(&type, (char*)data + NODE_TYPE, sizeof(int));
    memcpy(&freeSpace, (char*) data + NODE_FREE, sizeof(int));

    int entrySize;

    switch(type) {
        case TypeRoot:
        case TypeNode:
            // Key + pointer
            if (attribute.type == TypeVarChar) {
                int key = sizeof(int) + attribute.length;
                entrySize = key + sizeof(int);
            } else {
                entrySize = sizeof(int) + sizeof(int);
            }
            break;
        case TypeLeaf:
            // Key + Number of Rids + First Entry into rid list
            if (attribute.type == TypeVarChar) {
                int key = sizeof(int) + attribute.length;
                entrySize = key + sizeof(int) + RID_SIZE;
            } else {
                entrySize = sizeof(int) + sizeof(int) + RID_SIZE;
            }
            break;
        default:
            return false;
    }

    return freeSpace >=  entrySize;
}

RC IndexManager::getNextNodeByKey(void * &child, void * &parent
                                       , const void *key
                                       , const Attribute &attribute
                                       , IXFileHandle &ixfileHandle
                                       , int &leftPageNum
                                       , int &parentPageNum) {
    // next if node is empty
    parent = child;
    child = NULL;
    parentPageNum = leftPageNum;
    int offset = 0;

    // extract the key to compare to
    int cKey;
    memcpy(&cKey, (char *) key, sizeof(int));

    void *directorKey = NULL;
    int leftPage, rightPage;

    // iterate through each director key
    int counter = 0;
    directorKey = malloc(attribute.length + sizeof(int)); // int to compensate for varchar's length field
    while (getDirectorAtOffset(offset, parent, leftPage, rightPage, directorKey, attribute) != -1) {
        leftPageNum = leftPage;

        int comparisonResult = compareKeys(key, directorKey, attribute);

        // if key < the director key go left
        if (comparisonResult == -1) {
            if (child == NULL)
                child = malloc(PAGE_SIZE);
            ixfileHandle.getHandle()->readPage(leftPage, child);

            // free the directorKey
            if (directorKey != NULL)
                free(directorKey);

            return 0;
        }

        counter++;
    }

    // key is greater than the last director, therefore enter into the last page, else is an empty page (edge case)
    if (counter != 0) {
        if (child == NULL) child = malloc(PAGE_SIZE);
        ixfileHandle.getHandle()->readPage(rightPage, child);
        leftPageNum = rightPage;
    }

    // free the directorKey
    if (directorKey != NULL)
        free(directorKey);

    return 0;
}

RC IndexManager::splitChild(void* child, void *parent
                                       , const Attribute &attribute
                                       , IXFileHandle &ixFileHandle
                                       , const void *key
                                       , int &childPageNum
                                       , int &parentPageNum) {
    // calculate the freeSpaceOffset
    int freeSpace = IXFileHandle::getFreeSpace(child);
    int freeSpaceOffset = IXFileHandle::getFreeSpaceOffset(freeSpace);
    int currentKeyOffset = 0;
    int nextKeyOffset;
    int splitPosition;

    int keyLengthSize;
    int keyLength; // length that describes how many chars in a varchar for example
    int keySize;
    int rightPageNum;
    void *rightPage = NULL; 
    int shiftedSize;

    // variables for non-leaf nodes
    int prevDirectorOffset = 0;
    int currentDirectorOffset = 0;
    int nextDirectorOffset = 0;
    int oldRootPageNum;
    int rootOffset = 0;
    int directorSize;

    // variables used for director creation
    void *directorKey = NULL;
    int directorKeyLength;

    // used for special emtpy left nodes
    void *shiftData = NULL;
    int shiftSize;

    // Get node type
    NodeType nodeType = IXFileHandle::getNodeType(child);

    switch (nodeType) {
        case TypeRoot:
            //printBtree(ixFileHandle, attribute);
            // create a new root node
            parentPageNum = ixFileHandle.getAvailablePageNumber();
            parent = malloc(PAGE_SIZE);
            ixFileHandle.initializeNewNode(parent, TypeRoot);
            
            // in order for getAvailablePageNumber() to work on the right page 
            // we need to append the new root now
            ixFileHandle.getHandle()->appendPage(parent);

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
            // here a directorSize is assumed to be the left page and the key only
            while (currentDirectorOffset < freeSpaceOffset) {
                // skip the first pointer
                nextDirectorOffset += sizeof(int);

                memcpy(&keyLength, (char*)child + nextDirectorOffset, keyLengthSize);
                // Tease out the key from the node
                if (keyLengthSize != 0) {
                    directorSize = sizeof(int) + keyLength;
                } else {
                    directorSize = sizeof(int);
                }

                nextDirectorOffset += directorSize;

                // FOUND SPLIT POINT
                if (nextDirectorOffset >= SPLIT_THRESHOLD) {
                    splitPosition = currentDirectorOffset + sizeof(int);
                    break;
                }

                // shift to the nextDirector
                prevDirectorOffset = currentDirectorOffset; // TODO don't think previous director has a point
                currentDirectorOffset = nextDirectorOffset;
            }
            // Initialize right page
            rightPageNum = ixFileHandle.getAvailablePageNumber();
            rightPage = malloc(PAGE_SIZE);
            ixFileHandle.initializeNewNode(rightPage, TypeNode);
            ixFileHandle.getHandle()->appendPage(rightPage);

            // Save copy data to right page
            shiftedSize = freeSpaceOffset - splitPosition;
            memcpy((char *) rightPage, (char *) child + splitPosition, shiftedSize);
            ixFileHandle.setFreeSpace(rightPage, ixFileHandle.getFreeSpace(rightPage) - shiftedSize);

            // update the pointers
            ixFileHandle.setRightPointer(rightPage, ixFileHandle.getRightPointer(child));
            ixFileHandle.setRightPointer(child, rightPageNum);

            // clear out the move data from left page
            memset((char *) child + splitPosition, 0, shiftedSize);

            // update freeSpace
            ixFileHandle.setFreeSpace(child, ixFileHandle.getFreeSpace(child) + shiftedSize);

            // update the parent with a new director, insertDirector will automatically update freespace
            oldRootPageNum = ixFileHandle.getRootPageNum();
            memcpy((char *) parent + rootOffset, &oldRootPageNum, sizeof(int));
            rootOffset += sizeof(int);

            // insert the directorKey into the new root
            memcpy((char *) parent + rootOffset, (char *) rightPage, directorSize); 
            rootOffset += directorSize;

            // here we insert the right page num and increment the offset
            memcpy((char *) parent + rootOffset, &rightPageNum, sizeof(int));
            rootOffset += sizeof(int);

            // update the free space
            ixFileHandle.setFreeSpace(parent, DEFAULT_FREE - rootOffset);

            // now lets remove the the root director from the right page
            shiftSize = shiftedSize - directorSize;
            shiftData = malloc(shiftedSize);
            memcpy((char *) shiftData, (char *) rightPage + directorSize, shiftSize);
            memcpy((char *) rightPage, (char *) shiftData, shiftSize);
            memset((char *) rightPage + shiftSize, 0, directorSize);
            ixFileHandle.setFreeSpace(rightPage, ixFileHandle.getFreeSpace(rightPage) + directorSize);
            if (shiftData != NULL) free(shiftData);

            // here we have to append the new root to the file
            ixFileHandle.writeNode(parentPageNum, parent);
            ixFileHandle.setRoot(parent);
            ixFileHandle.setRootPageNum(parentPageNum);

            // before we right the left page we need to change it from roottype to nodetype
            ixFileHandle.setNodeType(child, TypeNode);

            // I think here we need to write to write our pages to file for all pages
            ixFileHandle.getHandle()->writePage(childPageNum, child);
            ixFileHandle.writeNode(rightPageNum, rightPage); 
            // free up the right page
            if (rightPage != NULL) {
                free(rightPage);
                rightPage = NULL;
            }

            //printBtree(ixFileHandle, attribute);
 
            break;
        case TypeNode:
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
            // here a directorSize is assumed to be the left page and the key only
            while (currentDirectorOffset < freeSpaceOffset) {
                // skip the first pointer
                nextDirectorOffset += sizeof(int);

                memcpy(&keyLength, (char*)child + nextDirectorOffset, keyLengthSize);
                // Tease out the key from the node
                if (keyLengthSize != 0) {
                    directorSize = sizeof(int) + keyLength;
                } else {
                    directorSize = sizeof(int);
                }
                nextDirectorOffset += directorSize;

                // FOUND SPLIT POINT
                if (nextDirectorOffset >= SPLIT_THRESHOLD) {
                    splitPosition = currentDirectorOffset + sizeof(int);
                    break;
                }

                // update current director offset
                currentDirectorOffset = nextDirectorOffset;
            }
            // Initialize right page
            rightPageNum = ixFileHandle.getAvailablePageNumber();
            rightPage = malloc(PAGE_SIZE);
            ixFileHandle.initializeNewNode(rightPage, TypeNode);

            // Save copy data to right page
            shiftedSize = freeSpaceOffset - splitPosition;
            memcpy((char *) rightPage, (char *) child + splitPosition, shiftedSize);
            ixFileHandle.setFreeSpace(rightPage, ixFileHandle.getFreeSpace(rightPage) - shiftedSize);

            // update the pointers
            ixFileHandle.setRightPointer(rightPage, ixFileHandle.getRightPointer(child));
            ixFileHandle.setRightPointer(child, rightPageNum);

            // clear out the move data from left page
            memset((char *) child + splitPosition, 0, shiftedSize);

            // update freeSpace
            ixFileHandle.setFreeSpace(child, DEFAULT_FREE - splitPosition);

            // create new director to be inserted into parent
            memcpy(&directorKeyLength, (char*)rightPage + (currentKeyOffset - sizeof(int)), keyLengthSize);
            // Tease out the key from the node
            if (keyLengthSize != 0) {
                keySize = sizeof(int) + keyLength;
            } else {
                keySize = sizeof(int);
            }

            directorKey = malloc(keySize);
            memcpy(directorKey, (char*)rightPage, keySize);

            // update the parent with a new director, insertDirector will automatically update freespace
            if(insertDirector(parent, directorKey, attribute, rightPageNum, ixFileHandle)) {
                return -1;
            }

            // now lets remove the first director from the right page
            shiftSize = shiftedSize - directorSize;
            shiftData = malloc(shiftedSize);
            memcpy((char *) shiftData, (char *) rightPage + directorSize, shiftSize);
            memcpy((char *) rightPage, (char *) shiftData, shiftSize);
            memset((char *) rightPage + shiftSize, 0, directorSize);
            ixFileHandle.setFreeSpace(rightPage, ixFileHandle.getFreeSpace(rightPage) + directorSize);
            if (shiftData != NULL) free(shiftData);

            // here we have to append the new root to the file
            ixFileHandle.writeNode(parentPageNum, parent);

            // I think here we need to write to write our pages to file for all pages
            ixFileHandle.getHandle()->writePage(childPageNum, child);
            ixFileHandle.getHandle()->appendPage(rightPage);

            // free up the right page
            if (rightPage != NULL) {
                free(rightPage);
                rightPage = NULL;
            }
            break;
        case TypeLeaf:
            // Save the number of bits for key length (this will be used to read in length size
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
            while (currentKeyOffset < freeSpaceOffset) {
                memcpy(&keyLength, (char*)child + currentKeyOffset, keyLengthSize);
                // Tease out the key from the node
                if (keyLengthSize != 0) {
                    keySize = sizeof(int) + keyLength;
                } else {
                    keySize = sizeof(int);
                }
                nextKeyOffset = getNextKeyOffset(currentKeyOffset + keySize, child);
                keySize = nextKeyOffset - currentKeyOffset;

                // FOUND SPLIT POINT
                if (nextKeyOffset >= SPLIT_THRESHOLD) {
                    // determine which side to split on key
                    splitPosition = (SPLIT_THRESHOLD - currentKeyOffset)
                            > (nextKeyOffset - SPLIT_THRESHOLD)
                            ? nextKeyOffset : currentKeyOffset;
                    break;
                }
                currentKeyOffset += keySize;
            }

            // Initialize right page
            rightPageNum = ixFileHandle.getAvailablePageNumber();
            rightPage = malloc(PAGE_SIZE);
            ixFileHandle.initializeNewNode(rightPage, TypeLeaf);

            // Save copy data to right page
            shiftedSize = freeSpaceOffset - splitPosition;
            memcpy((char *) rightPage, (char *) child + splitPosition, shiftedSize);
            ixFileHandle.setFreeSpace(rightPage, ixFileHandle.getFreeSpace(rightPage) - shiftedSize);

            // update the pointers
            ixFileHandle.setRightPointer(rightPage, ixFileHandle.getRightPointer(child));
            ixFileHandle.setRightPointer(child, rightPageNum);

            // clear out the move data from left page
            memset((char *) child + splitPosition, 0, shiftedSize);

            // update freeSpace
            ixFileHandle.setFreeSpace(child, DEFAULT_FREE - splitPosition);

            // create key for insert into director using the key at the beginning of the split
            memcpy(&keyLength, (char*)rightPage, keyLengthSize);
            if (keyLengthSize != 0) {
                keySize = sizeof(int) + keyLength;
            } else {
                keySize = sizeof(int);
            }
            directorKey = malloc(keySize);
            memcpy(directorKey, (char*)rightPage, keySize);

            // update the parent with a new director, insertDirector will automatically update freespace
            if(insertDirector(parent, directorKey, attribute, rightPageNum, ixFileHandle)) {
                return -1;
            }
            // here we have write the parent to file
            // test
            //ixFileHandle.getHandle()->writePage(parentPageNum, parent);
            ixFileHandle.writeNode(parentPageNum, parent);

            // I think here we need to write to write our pages to file for all pages
            ixFileHandle.getHandle()->writePage(childPageNum, child);
            ixFileHandle.getHandle()->appendPage(rightPage);

            // free up the right page
            if (directorKey != NULL) free(directorKey);
            if (rightPage != NULL) {
                free(rightPage);
                rightPage = NULL;
            }
            break;
    }

    return 0;
}

RC IndexManager::insertDirector(void *node, const void *key, const Attribute &attribute, int nextPageNum, IXFileHandle &ixFileHandle) {
    void *director;
    int size = 0;
    int offset = 0;
    int currentOffset = 0;
    void *extractionKey = NULL;

    // create the director and save its length
    switch(attribute.type) {
        case TypeInt:
        case TypeReal:
        {
            director = malloc(2 * sizeof(int));
            memcpy(director, (char*)key, sizeof(int));
            size += sizeof(int);
            memcpy((char *) director + size, &nextPageNum, sizeof(int));
            size += sizeof(int);
            
            extractionKey = malloc(sizeof(int));

            currentOffset = offset + sizeof(int); // sizeof(int) to compensate for first page

            int leftPage, rightPage, directorKey;
            while(getDirectorAtOffset(offset, node, leftPage, rightPage, extractionKey, attribute) != -1) {
                // compare the keys
                int comparisonResult = compareKeys(key, extractionKey, attribute);

                // if our key is less than the pointer, we found our target position
                if (comparisonResult == -1) {
                    break;
                }

                currentOffset = offset + sizeof(int); // sizeof(int) to compensate for first page
            }
            break;
        }
        case TypeVarChar: {
            int length;
            memcpy(&length, (char *) key, sizeof(int));
            size += length + sizeof(int);

            // copy over the whole key to director
            director = malloc(size + sizeof(int));
            memcpy(director, (char *)key, size);

            memcpy((char*)director + size, &nextPageNum, sizeof(int));
            size += sizeof(int);
            
            extractionKey = malloc(sizeof(int) + length);

            currentOffset = offset + sizeof(int); // sizeof(int) to compensate for first page

            int leftPage, rightPage, directorKey;
            while(getDirectorAtOffset(offset, node, leftPage, rightPage, extractionKey, attribute) != -1) {
                // compare the keys
                int comparisonResult = compareKeys(key, extractionKey, attribute);

                // if our key is less than the pointer, we found our target position
                if (comparisonResult == -1) {
                    break;
                }

                currentOffset = offset + sizeof(int); // sizeof(int) to compensate for first page
            }

            break;
        }
        default:
            return -1;
    }

    // Calculate the size of the node data that needs to be shifted
    int freeSpace = ixFileHandle.getFreeSpace(node);
    int shiftSize = DEFAULT_FREE - freeSpace - currentOffset;
    void* shiftData = malloc(shiftSize);

    // Save data into the temp value
    memcpy((char*)shiftData, (char*)node + currentOffset, shiftSize);

    // insert the new director here
    memcpy((char*)node + currentOffset, (char*)director, size);
    currentOffset += size;

    // reinsert the shifted data
    memcpy((char*)node + currentOffset, shiftData, shiftSize);

    // update Freespace
    freeSpace -= size;
    ixFileHandle.setFreeSpace(node, freeSpace);

    // free director
    if (director != NULL) free(director);
    if (shiftData != NULL) free(shiftData);
    if (extractionKey != NULL) free(extractionKey);
    return 0;
}

RC IndexManager::getDirectorAtOffset(int &offset, void* node, int &leftPointer, int &rightPointer, void* key, const Attribute &attribute) {
    int freeSpace = IXFileHandle::getFreeSpace(node);
    int freeSpaceOffset = IXFileHandle::getFreeSpaceOffset(freeSpace);

    // page is empty (this is usually due to first insert
    if (freeSpaceOffset == offset) {
        return -1;
    }

    // no more keys left
    if (freeSpaceOffset == offset + sizeof(int)) {
        offset += sizeof(int);
        return -1;
    }

    switch(attribute.type) {
        case TypeInt:
            memcpy(&leftPointer, (char*) node + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char *) key, (char*) node + offset, sizeof(int));
            offset += sizeof(int);
            memcpy(&rightPointer, (char*) node + offset, sizeof(int));
            break;
        case TypeReal:
            memcpy(&leftPointer, (char*) node + offset, sizeof(int));
            offset += sizeof(int);
            memcpy((char*)key, (char*) node + offset, sizeof(float));
            offset += sizeof(float);
            memcpy(&rightPointer, (char*) node + offset, sizeof(int));
            break;
        case TypeVarChar:
            // copy left pointer
            memcpy(&leftPointer, (char*) node + offset, sizeof(int));
            offset += sizeof(int);

            // get length of key
            int length;
            memcpy(&length, (char*) node + offset, sizeof(int));

            // copy key from node into key
            memcpy(key, (char*) node + offset, sizeof(int) + length);
            offset += sizeof(int) + length;

            // copy right pointer
            memcpy(&rightPointer, (char*) node + offset, sizeof(int));
            break;
        default:
            return -1;
    }

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
    int sizeOfNewData = 0;
    int sizeOfShiftedData = 0;
    void *newData = NULL;
    void *shiftedData = NULL;

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

        if (comparisonResult == -1) {
            // calculate the size of the shifting data and set the newOffset
            // to the previous
            int startOfOffset = nextKeyOffset - keySize;
            int sizeOfNewEntry = keySize + (3 * sizeof(int));
            sizeOfShiftedData = freeSpaceOffset - startOfOffset;
            newOffset = startOfOffset;

            // lastly we need to build the new data that will be inserted
            sizeOfNewData = createNewLeafEntry(newData, key, attribute, rid);

            // free the comparison key
            if (comparisonKey != NULL) free(comparisonKey);
            break;
        } else if (comparisonResult == 0) {
            // here we must insert a new RID into the list
            sizeOfNewData = RID_SIZE;

            // get the offset of the last RID in the list
            int numberOfRIDs = getNumberOfRids(child, nextKeyOffset);

            // get the offset of the last RID in the list
            newOffset = nextKeyOffset + sizeof(int) + (numberOfRIDs * RID_SIZE);

            // update the number of RIDs in key
            numberOfRIDs += 1;
            memcpy((char*)child + nextKeyOffset, &numberOfRIDs, sizeof(int));

            // calculate the data that will be shifted to the right
            sizeOfShiftedData = freeSpaceOffset - newOffset;

            // create the new data which is only an rid
            newData = malloc(RID_SIZE);
            memcpy((char *) newData + newDataOffset, &rid.pageNum, sizeof(int));
            newDataOffset += sizeof(int);
            memcpy((char *) newData + newDataOffset, &rid.slotNum, sizeof(int));

            // free the comparison key
            if (comparisonKey != NULL) free(comparisonKey);
            break;
        } else {
            nextKeyOffset = getNextKeyOffset(nextKeyOffset, child);

            // free the comparison key
            if (comparisonKey != NULL) free(comparisonKey);
        }
    }

    // if we have reached the end then we need to create the data for insertion
    // at the end of the leaf node
    if (nextKeyOffset == freeSpaceOffset) {
        // just insert at the end
        newOffset = freeSpaceOffset;
        // lastly we need to build the new data that will be inserted
        sizeOfNewData = createNewLeafEntry(newData, key, attribute, rid);
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

    // free memory
    if (newData != NULL) free(newData);
    if (shiftedData != NULL) free(shiftedData);

    return 0;
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

    // page is empty, therefore return -1
    if (nextKeyOffset == freeSpaceOffset) return -1;

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
                } else {
                    deleteLength = RID_SIZE + keySize + sizeof(int);
                }

                // Zero out the data where key + shift data used to lie
                memset((char*)child + (nextKeyOffset - deleteLength), 0, shiftSize + deleteLength);

                // copy over the previous RID and key and upate the freespace
                memcpy((char*)child + (nextKeyOffset - deleteLength), shiftContent, shiftSize);
                ixFileHandle.setFreeSpace(child, freeSpace + deleteLength);
                free(shiftContent);
                break;
            }
        } else if (comparisonResult == -1) {
            // key does not exist in list
            return -1;
        } else {
            nextKeyOffset = getNextKeyOffset(nextKeyOffset, child);
        }
    }

    // free the comparison key
    if (comparisonKey != NULL) free(comparisonKey);

    return 0;
}

int IndexManager::compareKeys(const void *key1, const void *key2, const Attribute attribute) {
    switch (attribute.type) {
        case TypeInt: {
            int keyOne, keyTwo;
            memcpy(&keyOne, (char*)key1, sizeof(int));
            memcpy(&keyTwo, (char*)key2, sizeof(int));

            if (keyOne > keyTwo) return 1;
            if (keyOne < keyTwo) return -1;

            // both equal
            return 0;
        }
        case TypeReal: {
            float keyOne, keyTwo;
            memcpy(&keyOne, (char*)key1, sizeof(float));
            memcpy(&keyTwo, (char*)key2, sizeof(float));

            if (keyOne > keyTwo) return 1;
            if (keyOne < keyTwo) return -1;

            // both equal
            return 0;
        }
        case TypeVarChar: {
            int keyOneLength, keyTwoLength;
            memcpy(&keyOneLength, (char*)key1, sizeof(int));
            memcpy(&keyTwoLength, (char*)key2, sizeof(int));

            // get the smaller of the two to do the character comparisons
            int compareLen = (keyOneLength > keyTwoLength) ? keyTwoLength : keyOneLength;

            // iterate over the keys and decide which is larger
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
    }

    return -1;
}

int IndexManager::getNumberOfRids(void *node, int RIDnumOffset) {
    int numberOfRIDs;
    memcpy(&numberOfRIDs, (char *) node + RIDnumOffset, sizeof(int));
    return numberOfRIDs;
}

int IndexManager::createNewLeafEntry(void *&data, const void *key, const Attribute &attribute, const RID &rid) {
    int numRids = 1;
    int offset = 0;

    switch (attribute.type) {
        case TypeInt:
        case TypeReal:
            data = malloc((2 *sizeof(int)) + RID_SIZE);

            memcpy((char *) data + offset, key, sizeof(int));
            offset += sizeof(int);
            memcpy((char *) data + offset, &numRids, sizeof(int));
            offset += sizeof(int);
            memcpy((char *) data + offset, &rid.pageNum, sizeof(int));
            offset += sizeof(int);
            memcpy((char *) data + offset, &rid.slotNum, sizeof(int));
            offset += sizeof(int);
            break;
        case TypeVarChar: {
            int length;
            memcpy(&length, (char*) key + offset, sizeof(int));
            int keySize = length + sizeof(int);

            data = malloc(keySize + sizeof(int) + RID_SIZE);
            memcpy((char *) data, (char*)key, keySize);
            offset += keySize;

            memcpy((char *) data + offset, &numRids, sizeof(int));
            offset += sizeof(int);
            memcpy((char *) data + offset, &rid.pageNum, sizeof(int));
            offset += sizeof(int);
            memcpy((char *) data + offset, &rid.slotNum, sizeof(int));
            offset += sizeof(int);
            break;
        }
        default:
            break;
    }

    return offset;
}

// this function takes the the offset of a key entry plus the key size, so
// that we are placed at the # of RID's slot
int IndexManager::getNextKeyOffset(int RIDnumOffset, void *node) {
    // we need to extract the number RID's that exist in the node
    return RIDnumOffset + sizeof(int) + (getNumberOfRids(node, RIDnumOffset) * RID_SIZE);
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
            length += sizeof(int);
            break;
        default:
            return -1;
    }

    return length;
}

RC IndexManager::getKeysInLeaf(IXFileHandle &ixFileHandle, void *node, const Attribute &attribute, vector<string> &keys) const {
    int freeSpace = IXFileHandle::getFreeSpace(node);
    int freeSpaceOffset = IXFileHandle::getFreeSpaceOffset(freeSpace);
    int offset = 0;

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

    // collect every key and every page
    int keyLength;
    while (offset < freeSpaceOffset) {
        // This is the key that will be joined with the key and the rids
        string returnKey = "";
        string key;

        // extract key
        memcpy(&keyLength, (char*)node + offset, keyLengthSize);
        switch (attribute.type) {
            case TypeInt: {
                int intKey;
                memcpy(&intKey, (char*)node + offset, sizeof(int));
                offset += sizeof(int);
                key = std::to_string(intKey);
                break;
            }
            case TypeReal: {
                float floatKey;
                memcpy(&floatKey, (char*)node + offset, sizeof(float));
                offset += sizeof(int);
                key = std::to_string(floatKey);
                break;
            }
            case TypeVarChar: {
                int length;
                memcpy(&length, (char*)node + offset, sizeof(int));
                offset += sizeof(int);

                char* s = new char[length + 1];
                memcpy(s, (char *)node + offset, length);
                char nullCharacter = '\0';
                memcpy(s + length, &nullCharacter, sizeof(char));
                offset += length;

                string stringKey(s);

                keys.push_back(stringKey);

                delete[] s; // TODO Added this, it may/may not be affect free() for test extra_1

                break;
            }
            default:
                return -1;
        }

        returnKey += key;

        // get number of Rids (had to use this because it's a const function -_-
        string ridString = "";
        int numberOfRids;
        memcpy(&numberOfRids, (char*)node + offset, sizeof(int));
        offset += sizeof(int);

        // create and append the rid string to the return key
        int counter = 0;
        returnKey += ":[";

        for (int i = 0; i < numberOfRids; i++) {
            string rid;

            // append a comma between rids
            if (counter > 0) rid = ",";

            int pageNum, slotNum;

            memcpy(&pageNum, (char*)node + offset, sizeof(int));
            offset += sizeof(int);
            memcpy(&slotNum, (char*)node + offset, sizeof(int));
            offset += sizeof(int);

            rid += "(" + std::to_string(pageNum) + "," + std::to_string(slotNum) + ")";

            ridString += rid;
            counter++;
        }

        returnKey += ridString;
        returnKey += "]";

        keys.push_back(returnKey);
    }

    return 0;
}

RC IndexManager::getKeysInNonLeaf(IXFileHandle &ixFileHandle
                                 , void *node
                                 , const Attribute &attribute
                                 , vector<string> &keys
                                 , vector<int> &pages) const {
    int freeSpace = ixFileHandle.getFreeSpace(node);
    int freeSpaceOffset = IXFileHandle::getFreeSpaceOffset(freeSpace);
    int offset = 0;

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

    // collect every key and every page
    int keyLength;
    int pageNumber;
    while (offset < freeSpaceOffset) {
        // extract page number
        memcpy(&pageNumber, (char*)node + offset, sizeof(int));
        offset += sizeof(int);
        pages.push_back(pageNumber);

        // if page number was the last thing in the node
        if (offset >= freeSpaceOffset) break;

        // extract key
        switch (attribute.type) {
            case TypeInt: {
                int intKey;
                memcpy(&intKey, (char*)node + offset, sizeof(int));
                offset += sizeof(int);
                keys.push_back(std::to_string(intKey));
                break;
            }
            case TypeReal: {
                float floatKey;
                memcpy(&floatKey, (char*)node + offset, sizeof(float));
                offset += sizeof(int);
                keys.push_back(std::to_string(floatKey));
                break;
            }
            case TypeVarChar: {
                int length;
                memcpy(&length, (char*)node + offset, sizeof(int));
                offset += sizeof(int);

                char* s = new char[length + 1];
                memcpy(s, (char *)node + offset, length);
                char nullCharacter = '\0';
                memcpy(s + length, &nullCharacter, sizeof(char));
                offset += length;

                string stringKey(s);

                delete[] s; // TODO Added this, it may/may not be affect free() for test extra_1

                keys.push_back(stringKey);

                break;
            }
            default:
                return -1;
        }
    }

    return 0;
}

IX_ScanIterator::IX_ScanIterator()
{
    ixFileHandle = NULL;
    leafNode = malloc(PAGE_SIZE);
    currentLeafOffset = 0;
    keyIndex = 0;
    hasBegan = false;
}

IX_ScanIterator::~IX_ScanIterator()
{
    free(leafNode);
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    // extract freeSpace and calculate the freeSpaceOffset
    int freeSpace = IXFileHandle::getFreeSpace(leafNode);
    int freeSpaceOffset = IXFileHandle::getFreeSpaceOffset(freeSpace);
    int nextPageNum;

    // loop until we no longer satisfy the range
    while(true) {
        // if we are at the end of a page go to the next one
        if (currentLeafOffset >= freeSpaceOffset && keyIndex == keyRids.size()) {
            nextPageNum = ixFileHandle->getRightPointer(leafNode);
            if (nextPageNum == -1) return IX_EOF;
            ixFileHandle->getHandle()->readPage(nextPageNum, leafNode);
            freeSpace = IXFileHandle::getFreeSpace(leafNode);
            freeSpaceOffset = IXFileHandle::getFreeSpaceOffset(freeSpace);
            currentLeafOffset = 0;
        }

        // if there is a RID at position keyIndex, return that rid
        if (keyIndex < keyRids.size()) {
            RID nextRid = getNextRid();
            rid.pageNum = nextRid.pageNum;
            rid.slotNum = nextRid.slotNum;

            keyIndex++;
            return 0;
        }

        // set index to 0 and clear the keyRids
        setKeyIndex(0);
        keyRids.clear();

        // get the type and the compare
        int keyOffset = currentLeafOffset;
        (*getKey)(key, rid, leafNode, currentLeafOffset);

        // compare and and evaluate the return type
        if((*compareTypeFunc)(key, lowKey, highKey, leafNode, keyOffset, lowKeyInclusive, highKeyInclusive)) {
            // has began
            hasBegan = true;

            // build the RID list
            int numRids;
            memcpy(&numRids, (char*)leafNode + currentLeafOffset, sizeof(int));
            currentLeafOffset += sizeof(int);

            for (int i = 0; i < numRids; i++) {
                RID temp;
                memcpy(&temp.pageNum, (char*)leafNode + currentLeafOffset, sizeof(int));
                currentLeafOffset += sizeof(int);
                memcpy(&temp.slotNum, (char*)leafNode + currentLeafOffset, sizeof(int));
                currentLeafOffset += sizeof(int);

                keyRids.push_back(temp);
            }

            rid.pageNum = keyRids[0].pageNum;
            rid.slotNum = keyRids[0].slotNum;

            keyIndex++;
            break;
        } else if (hasBegan) {
            // we have finished collecting keys
            return IX_EOF;
        }
        // we need to advance the offset
        currentLeafOffset = IndexManager::getNextKeyOffset(currentLeafOffset, leafNode);

        // if we don't break then we continue searching until we reach the end of
        // the index
    }
    return 0;
}

void IX_ScanIterator::getIntType(void *&type, RID &rid, void *node, int &offset) {
    if (type == NULL) {
        type = malloc(sizeof(int));
    }
    memcpy((char *) type, (char *) node + offset, sizeof(int));
    offset += sizeof(int);
}

void IX_ScanIterator::getRealType(void *&type, RID &rid, void *node, int &offset) {
    if (type == NULL) {
        type = malloc(sizeof(float));
    }
    memcpy((char *) type, (char *) node + offset, sizeof(float));
    offset += sizeof(float);
}

void IX_ScanIterator::getVarCharType(void *&type, RID &rid, void *node, int &offset) {
    int varCharLength;
    memcpy(&varCharLength, (char *) node + offset, sizeof(int));
    if (type == NULL) {
        type = malloc(sizeof(int) + varCharLength);
    }
    memcpy((char *) type, (char *) node + offset, sizeof(int));
    offset += sizeof(int);
    memcpy((char *) type + offset, (char *) node + offset, varCharLength);
    offset += varCharLength;
}

bool IX_ScanIterator::compareInts(void *incomingKey, const void *low
                                                    , const void *high
                                                    , void *node
                                                    , int offset
                                                    , bool lowInc
                                                    , bool highInc) {

    // let's extact the leaf, low and high keys to compare
    int leafKey, lKey, hKey;
    memcpy(&leafKey, (char *) node + offset, sizeof(int));
    if (low != NULL) memcpy(&lKey, (char *) low, sizeof(int));
    if (high != NULL) memcpy(&hKey, (char *) high, sizeof(int));

    // now we can test our comparisons
    if (low != NULL && high != NULL) {
        if (!lowInc && !highInc) {
            return leafKey > lKey && leafKey < hKey;
        } else if (!lowInc && highInc) {
            return leafKey > lKey && leafKey<= hKey;
        } else if (lowInc && !highInc) {
            return leafKey >= lKey && leafKey < hKey;
        } else {
            return leafKey >= lKey && leafKey <= hKey;
        }
    } else if (low == NULL && high != NULL) {
        if (highInc) {
            return hKey >= leafKey;
        } else {
            return hKey > leafKey;
        }
    } else if (low != NULL && high == NULL) {
        if (lowInc) {
            return lKey <= leafKey;
        } else {
            return lKey < leafKey;
        }
    } else {
        // this just means return everything
        return true;
    }

}

bool IX_ScanIterator::compareReals(void *incomingKey, const void *low
                                                    , const void *high
                                                    , void *node
                                                    , int offset
                                                    , bool lowInc
                                                    , bool highInc) {

    float leafKey, lKey, hKey;
    memcpy(&leafKey, (char *) node + offset, sizeof(float));
    if (low != NULL) memcpy(&lKey, (char *) low, sizeof(float));
    if (high != NULL) memcpy(&hKey, (char *) high, sizeof(float));

    // now we can test our comparisons
    if (low != NULL && high != NULL) {
        if (!lowInc && !highInc) {
            return leafKey > lKey && leafKey < hKey;
        } else if (!lowInc && highInc) {
            return leafKey > lKey && leafKey<= hKey;
        } else if (lowInc && !highInc) {
            return leafKey >= lKey && leafKey < hKey;
        } else {
            return leafKey >= lKey && leafKey <= hKey;
        }
    } else if (low == NULL && high != NULL) {
        if (highInc) {
            return hKey >= leafKey;
        } else {
            return hKey > leafKey;
        }
    } else if (low != NULL && high == NULL) {
        if (lowInc) {
            return lKey <= leafKey;
        } else {
            return lKey < leafKey;
        }
    } else {
        // this just means return everything
        return true;
    }
}

bool IX_ScanIterator::compareVarChars(void *incomingKey, const void *low
                                                       , const void *high
                                                       , void *node, int offset
                                                       , bool lowInc
                                                       , bool highInc) {
    char *leafKey = NULL;
    char *lKey = NULL;
    char *hKey = NULL;
    int leafKeySize, lKeySize, hKeySize;

    // extract the key lengths first
    memcpy(&leafKeySize, (char *) node + offset, sizeof(int));
    offset += sizeof(int);

    leafKey = new char[leafKeySize + 1];
    memcpy(leafKey, (char *) node + offset,leafKeySize);
    leafKey[leafKeySize] = '\0';
    offset += leafKeySize;

    if (low != NULL) {
        memcpy(&lKeySize, (char *) low, sizeof(int));
        lKey = new char[lKeySize + 1];
        memcpy(lKey, (char *) low + sizeof(int), lKeySize);
        lKey[lKeySize] = '\0';
    }
    if (high != NULL) {
         memcpy(&hKeySize, (char *) high, sizeof(int));
         hKey = new char[hKeySize + 1];
         memcpy(hKey, (char *) high + sizeof(int), hKeySize);
         hKey[hKeySize] = '\0';
    }
    // now we can test our comparisons
    string sLeaf(leafKey);
    string sLKey, sHKey;

    if (lKey != NULL) sLKey = lKey;
    if (hKey != NULL) sHKey = hKey;

    // free up the c strings
    delete leafKey;
    delete lKey;
    delete hKey;

    // now we can test our comparisons
    if (low != NULL && high != NULL) {
        if (!lowInc && !highInc) {
            return sLeaf > sLKey && sLeaf < sHKey;
        } else if (!lowInc && highInc) {
            return sLeaf > sLKey && sLeaf <= sHKey;
        } else if (lowInc && !highInc) {
            return sLeaf >= sLKey && sLeaf < sHKey;
        } else {
            return sLeaf >= sLKey && sLeaf <= sHKey;
        }
    } else if (low == NULL && high != NULL) {
        if (highInc) {
            return sHKey >= sLeaf;
        } else {
            return sHKey > sLeaf;
        }
    } else if (low != NULL && high == NULL) {
        if (lowInc) {
            return sLKey <= sLeaf;
        } else {
            return sLKey < sLeaf;
        }
    } else {
        // this just means return everything
        return true;
    }
    }

RID IX_ScanIterator::getNextRid() {
    return keyRids[keyIndex];
}

RC IX_ScanIterator::close()
{
    hasBegan = false;
    return 0;
}


IXFileHandle::IXFileHandle()
{
    FileHandle* handle = NULL;
    setHandle(handle);
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

    // set right pointer to -1
    int rightPointer = -1;
    memcpy((char*)data + NODE_RIGHT, &rightPointer, sizeof(int));

    // sets the node type
    setNodeType(data, type);

    return 0;
}

void IXFileHandle::setRightPointer(void *node, int rightPageNum) {
    memcpy((char *) node + NODE_RIGHT, &rightPageNum, sizeof(int));
}

NodeType IXFileHandle::getNodeType(void *node) {
    NodeType type;
    memcpy(&type, (char *) node + NODE_TYPE, sizeof(int));

    return type;
}

void IXFileHandle::setNodeType(void *node, NodeType type) {
//    byte nodeType;
//    switch (type) {
//        case TypeNode:
//            nodeType = 0;
//            break;
//        case TypeLeaf:
//            nodeType = 1;
//            break;
//        case TypeRoot:
//            nodeType = 2;
//            break;
//    }

    // initializes the node type slot with node type
    memcpy((char*)node + NODE_TYPE, &type, sizeof(int));
}

void IXFileHandle::writeNode(int pageNumber, void* data) {
    // If page number == 0, update root node
    if (pageNumber == getRootPageNum()) {
        setRoot(data);
    }

    // write page
    getHandle()->writePage(pageNumber, data);
}

void IXFileHandle::readNode(int pageNumber, void* data) {
    getHandle()->readPage(pageNumber, data);
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

bool IXFileHandle::isLeftNodeNull(void *node, int offset, int &childPageNum) {
    memcpy(&childPageNum, (char *) node + offset, sizeof(int));
    return childPageNum < 0? true : false;
}

bool IXFileHandle::isRightNodeNull(void *node, const Attribute &attribute, int offset, int &childPageNum) {
    int directorLength;
    if (attribute.type == TypeVarChar) {
        memcpy(&directorLength, (char *) node + offset, sizeof(int));
        offset += sizeof(int) + directorLength;
    } else {
        offset += sizeof(int);
    }
    memcpy(&childPageNum, (char *) node + offset, sizeof(int));
    return childPageNum < 0 ? true : false;
}

void IX_ScanIterator::setLowKeyValues(const void *lowK, bool lowKInc, const Attribute &attribute) {
    int keySize;
    if (attribute.length > 4)
        keySize = attribute.length + sizeof(int);
    else
        keySize = attribute.length;

    if (lowK != NULL) {
        lowKey = malloc(keySize);
        memcpy((char*)lowKey, (char*)lowK, keySize);
    }
    else {
        lowKey = NULL;
    }

    lowKeyInclusive = lowKInc;
}

void IX_ScanIterator::setHighKeyValues(const void *highK, bool highKInc, const Attribute &attribute) {
    int keySize;
    if (attribute.length > 4)
        keySize = attribute.length + sizeof(int);
    else
        keySize = attribute.length;

    if (highK != NULL) {
        highKey = malloc(keySize);
        memcpy((char*)highKey, (char*)highK, keySize);
    } else {
        highKey = NULL;
    }

    highKeyInclusive = highKInc;
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


