
#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();
    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
    pfm = PagedFileManager::instance();
    readingPage = NULL;
}

RecordBasedFileManager::~RecordBasedFileManager()
{

}

RC RecordBasedFileManager::createFile(const string &fileName) {
    return pfm->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return pfm->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    if(pfm->openFile(fileName, fileHandle) == -1) {
        return -1;
    }

    // if the file is not empty then we need to scan it
    if (fileHandle.numPages > 0) {
        fileHandle.currentPageNum = fileHandle.numPages - 1;

        void *page = malloc(PAGE_SIZE);
        for (int i = 0; i <fileHandle.numPages; i++) {
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
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return pfm->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    // lets determine if we need to append a new page or just write to a page
    short numFields = recordDescriptor.size();
    int numNullBytes = ceil((double) numFields / CHAR_BIT);
    int fieldNumBytes = numFields * sizeof(short);
    int metaNumBytes = (sizeof(short) + numNullBytes + fieldNumBytes);

    // allocate enought space for the meta data of each record
    // Meta Data: numFields | NullBytes | field Offsets |
    void *metaData = malloc(metaNumBytes);
    int length = buildMetaData(data, recordDescriptor, metaData);

    // findOpenSlot() will search for an open slot in the slot directory
    // if it finds one it will update the rid and return the new offset for the record
    int newOffset = findOpenSlot(fileHandle, length, rid);
    if (newOffset == -1) {
        // first thing we need to do is write the current page to file and free up the memory
        // only if there is 1 or more pages in the file Handle
        if (fileHandle.getNumberOfPages() != 0 && fileHandle.currentPage != NULL) {
            if (fileHandle.writePage(fileHandle.getNumberOfPages() - 1, fileHandle.currentPage)) {
                // error writing to file
                return -1;
            }
            free(fileHandle.currentPage);
        }

        // we need to append a new page
        fileHandle.currentPage = malloc(PAGE_SIZE);
        void *newPage = fileHandle.currentPage;
        memset(newPage, 0, PAGE_SIZE);

        // update the RID
        fileHandle.currentPageNum = fileHandle.getNumberOfPages();
        updateSlotDirectory(rid, fileHandle.getNumberOfPages(), 0);

        // Now let's add the new record
        setUpNewPage(newPage, data, length, fileHandle, metaData, metaNumBytes, recordDescriptor.size());
        fileHandle.appendPage(newPage);
        return 0;
    } else {

        // Determine if we will use the current page or a previous page
        void *page = determinePageToUse(rid, fileHandle);

        transferRecordToPage(page, data, metaData, newOffset, metaNumBytes, recordDescriptor.size(), length);

        // update the number of records and freeSpaceOffset
        int numRecords = incrementNumRecords(page);
        int freeSpaceOffset = incrementFreeSpaceOffset(page, length);

        // finally update freespace list
        updateFreeSpace(numRecords, freeSpaceOffset, rid.pageNum, fileHandle);

        // now we need to enter in the slot directory entry
        int slotEntryOffset = N_OFFSET - ((rid.slotNum + 1) * SLOT_SIZE);
        memcpy((char *) page + slotEntryOffset, &newOffset, sizeof(int));
        memcpy((char *) page + slotEntryOffset + sizeof(int), &length, sizeof(int));

        fileHandle.writePage(rid.pageNum, page);
        // if we opened a page that was not the header page then free that memory
        if (fileHandle.currentPageNum != (unsigned) rid.pageNum) {
            free(page);
        }
        return 0;
    }
    return -1;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
        // Determine which page to use using the rid
    if (readingPage == NULL) {
        readingPage = determinePageToUse(rid, fileHandle);
        readingRID.pageNum = rid.pageNum;
        readingRID.slotNum = rid.slotNum;
    }

    if (readingRID.pageNum != rid.pageNum) {
        readingPage = determinePageToUse(rid, fileHandle);
        readingRID.pageNum = rid.pageNum;
        readingRID.slotNum = rid.slotNum;
    }
    void *page = readingPage;

    int offset, length;
    getSlotFile(rid.slotNum, page, &offset, &length);

    if (offset == 0 && length == 0) {
        // we have a tombstone here and we need to return an error
        return -1;
    }

    // we need to check for a pointer to another record here
    if (length < 0) {
        RID newRid;
        newRid.pageNum = (offset * -1) - 1;
        newRid.slotNum = (length * -1) - 1;
        return readRecord(fileHandle, recordDescriptor, newRid, data);
    }

    // Now copy the entire contents into data
    void *tempData = malloc(length);
    memcpy((char *) tempData, (char *) page + offset, length);

    // we now need to extract the field data from the record
    extractFieldData(recordDescriptor.size(), length, data, tempData);

    // free up the tempData
    free(tempData);

    return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    // Go through all the attributes and print the data
    std::string s;
    int numNullBytes = ceil((double)recordDescriptor.size() / CHAR_BIT);
    int offset = numNullBytes;
    for (auto it = recordDescriptor.begin(); it != recordDescriptor.end(); ++it) {
        // test to see if the field is NULL
        int i = it - recordDescriptor.begin();
        s += it->name + ": ";
        s += isFieldNull(data, i) ? "NULL" : extractType(data, &offset, it->type, it->length);
        s += '\t';
    }
    cout << s << endl;
    return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid) {
    /****** TODO: we need to consider deleting a pointer *********/

    // Determine if we will use the current page or a previous page
    void *page = determinePageToUse(rid, fileHandle);

    // empty file or something went wrong?
    if (page == NULL) return -1;

    int offset, length;
    getSlotFile(rid.slotNum, page, &offset, &length);

    // Test if the slot id is a pointer, and if so collect the RID and return deleteRecord with the new RID.
    if (length < 0) {
        RID newRid;
        newRid.pageNum = (offset * -1) - 1;
        newRid.slotNum = (length * -1) - 1;
        return deleteRecord(fileHandle, recordDescriptor, newRid);
    }
    // Cannot delete a tombstone, therefore error.
    if (length == 0) {
        return -1;
    }

    // Write uninitialized data into the page where the record currently lies
    //void *newData = malloc(length);
    //memcpy(page, (char*) newData, length);
    // memset?
    memset((char *) page + offset, 0, length);

    // Clear out the slot in the meta data
    int zero = 0;
    int location = PAGE_SIZE - (((rid.slotNum + 1) * SLOT_SIZE) + META_INFO);
    memcpy((char *) page + location, &zero, sizeof(int));
    memcpy((char *) page + location + sizeof(int), &zero, sizeof(int));

    // maybe update number of records and freespace offset here?

    // update freeSpace vector
    fileHandle.freeSpace[rid.pageNum] += length;

    // Shifts the data appropriately
    compactMemory(offset, length, page, fileHandle.freeSpace[rid.pageNum]);
    
    // we must write the page back to file
    fileHandle.writePage(rid.pageNum, page);

    // update the currentPage if it was deleted from there 
    if ((unsigned) rid.pageNum == fileHandle.currentPageNum) {
        fileHandle.readPage(fileHandle.currentPageNum, fileHandle.currentPage);
    }
    // free up memory
    if (fileHandle.currentPageNum != (unsigned) rid.pageNum) {
        free(page);
    }
    return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid) {
    void *page = determinePageToUse(rid, fileHandle);
    RID tempRid;

    // Delete the old record
    if (RecordBasedFileManager::deleteRecord(fileHandle, recordDescriptor, rid) == -1) return -1;

    // Get new offset and (potentially) new RID. RID could be new if the updated record is now too large for page.
    short numFields = recordDescriptor.size();
    int numNullBytes = ceil((double) numFields / CHAR_BIT);
    int fieldNumBytes = numFields * sizeof(short);
    int metaNumBytes = (sizeof(short) + numNullBytes + fieldNumBytes);
    
    void *metaData = malloc(metaNumBytes);
    int length = buildMetaData(data, recordDescriptor, metaData);
    
    // we need to determine if this length fits in the page of rid
    int freeSpace = fileHandle.freeSpace[rid.pageNum];

    // If the new RID slot is on a different page, update the slot record with the negated version of these values
    if (length > freeSpace) {
        // because the first open slot is on a new page, just insert record as usual
        if (RecordBasedFileManager::insertRecord(fileHandle, recordDescriptor, data, tempRid) == -1)
            return -1;

        //update slot directory with negative values to reflect tombstone
        tempRid.pageNum = (tempRid.pageNum + 1) * -1;
        tempRid.slotNum = (tempRid.slotNum + 1) * -1;
        
        int slotEntryOffset = N_OFFSET - ((rid.slotNum + 1) * SLOT_SIZE);
        memcpy((char *)page + slotEntryOffset, &tempRid.pageNum, sizeof(int));
        memcpy((char *)page + slotEntryOffset + sizeof(int), &tempRid.slotNum, sizeof(int));

        fileHandle.writePage(rid.pageNum, page);
        // if we opened a page that was not the header page then free that memory
        if (fileHandle.currentPageNum != (unsigned) rid.pageNum) {
            free(page);
        }

        free(metaData);
        return 0;
    }
    else {
        page = determinePageToUse(rid, fileHandle);
        // get the new offset from the page
        int newOffset = getFreeSpaceOffset(page);
        transferRecordToPage(page, data, metaData, newOffset, metaNumBytes, recordDescriptor.size(), length);

        // update the number of records and freeSpaceOffset
        int numRecords = incrementNumRecords(page);
        int freeSpaceOffset = incrementFreeSpaceOffset(page, length);

        // finally update freespace list
        updateFreeSpace(numRecords, freeSpaceOffset, rid.pageNum, fileHandle);

        // now we need to enter in the slot directory entry
        int slotEntryOffset = N_OFFSET - ((rid.slotNum + 1) * SLOT_SIZE);
        memcpy((char *) page + slotEntryOffset, &newOffset, sizeof(int));
        memcpy((char *) page + slotEntryOffset + sizeof(int), &length, sizeof(int));

        fileHandle.writePage(rid.pageNum, page);
        // if we opened a page that was not the header page then free that memory
        if (fileHandle.currentPageNum != (unsigned) rid.pageNum) {
            free(page);
        }
        free(metaData);
        return 0;
    }

    return -1;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle
        , const vector<Attribute> &recordDescriptor
        , const RID &rid
        , const string &attributeName
        , void *data) {
    // first find the location of the attribute
    int i;
    int fieldPlacement = -1;
    for (auto it = recordDescriptor.begin(); it != recordDescriptor.end(); ++it) {
        i = it - recordDescriptor.begin();
        if (it->name == attributeName) {
            fieldPlacement = i;
            break;
        }
    }
    if (fieldPlacement == -1) {
        // the attribute name was not found
        return -1;
    }
    // get the page, record and number of fields in the record
    void *page = determinePageToUse(rid, fileHandle);
    void *record = extractRecord(rid.slotNum, page);
    int numFields = getNumberOfFields(record);

    // we can now test and see if the field is null
    int numNullBytes = ceil((double)recordDescriptor.size() / CHAR_BIT);
    void *nullBytes = malloc(numNullBytes);
    memcpy((char *) nullBytes, (char *) record + FIELD_OFFSET, numNullBytes);

    // if the field is null just return a nullbyte indicator
    if (isFieldNull(nullBytes, fieldPlacement)) {
        void *newNull = malloc(1);
        memset((char *) newNull, 0, 1);
        memcpy((char *) data,  (char *) newNull, 1);
        free(page);
        free(record);
        free(nullBytes);
        free(newNull);
        return 0;
    }

    // Also not sure if void *data is already allocated or not
    int fieldOffset = getFieldOffset(fieldPlacement, numNullBytes, record);
    int nextFieldOffset;

    // in order to get the next field offset we need to check if this is the last field
    if ((fieldPlacement + 1) == numFields) {
        int offset, length;
        getSlotFile(rid.slotNum, page, &offset, &length);
        nextFieldOffset = length;
    } else {
        nextFieldOffset = getFieldOffset(fieldPlacement + 1, numNullBytes, record);
    }

    // let's get the length of the field
    int fieldLength = nextFieldOffset - fieldOffset;

    // TODO: do I need to add a nullField to this one attribute?
    void *newNullByte = malloc(1);
    memset((char *) newNullByte, 0, 1);
    memcpy((char *) data, (char *) newNullByte, 1);
    // extract the field into data and free up memory used
    memcpy((char *) data + 1, (char *) record + fieldOffset, fieldLength);

    free(page);
    free(record);
    free(nullBytes);
    free(newNullByte);
    return 0;
}

int RecordBasedFileManager::getFieldOffset(int location, int numNullBytes, const void *record) {
    int fieldDataOffset = FIELD_OFFSET + numNullBytes + (location * FIELD_OFFSET);
    short fieldOffset;
    memcpy(&fieldOffset, (char *) record + fieldDataOffset, FIELD_OFFSET);
    return (int)fieldOffset;
}

f_data RecordBasedFileManager::getNumberOfFields(const void *record) {
    f_data numRecords;
    memcpy(&numRecords, (char *) record, FIELD_OFFSET);
    return numRecords;
}


void* RecordBasedFileManager::extractRecord(int slotNum, const void *page) {
    // TODO: make a guard against asking for an invalid slotNum
    int offset, length;
    getSlotFile(slotNum, page, &offset, &length);
    void *record = malloc(length);
    memcpy((char *) record, (char *) page + offset, length);
    return record;
}

int RecordBasedFileManager::incrementFreeSpaceOffset(void *page, int length) {
    int freeSpaceOffset;
    memcpy(&freeSpaceOffset, (char *) page + F_OFFSET, sizeof(int));
    freeSpaceOffset += length;
    memcpy((char *) page + F_OFFSET, &freeSpaceOffset, sizeof(int));
    return freeSpaceOffset;
}

int RecordBasedFileManager::decrementFreeSpaceOffset(void *page, int length) {
    int freeSpaceOffset;
    memcpy(&freeSpaceOffset, (char *) page + F_OFFSET, sizeof(int));
    freeSpaceOffset -= length;
    memcpy((char *) page + F_OFFSET, &freeSpaceOffset, sizeof(int));
    return freeSpaceOffset;
}



int RecordBasedFileManager::incrementNumRecords(void *page) {
    int numRecords = extractNumRecords(page);
    numRecords++;
    memcpy((char *) page + N_OFFSET, &numRecords, sizeof(int));
    return numRecords;
}


int RecordBasedFileManager::decrementNumRecords(void *page) {
    int numRecords = extractNumRecords(page);
    numRecords--;
    memcpy((char *) page + N_OFFSET, &numRecords, sizeof(int));
    return numRecords;
}


void RecordBasedFileManager::updateFreeSpace(int numRecords, int freeSpaceOffset,int pageNum, FileHandle &handle) {
    // we can change this to just extract the current freeSpace and increment by numBytes
    int freeSpace = PAGE_SIZE - (freeSpaceOffset + (numRecords * SLOT_SIZE) + META_INFO);
    handle.freeSpace[pageNum] = freeSpace;
}
void RecordBasedFileManager::transferRecordToPage(void *page
        , const void *data
        , void *metaData
        , int newOffset
        , int metaNumBytes
        , int recSize
        , int length) {
    // get the NUllBytes
    int numNullBytes = ceil((double)recSize / CHAR_BIT);

    // copy meta data
    memcpy((char *) page + newOffset, (char *) metaData, metaNumBytes);

    // finally copy over the rest of the data
    newOffset += metaNumBytes;
    length -= metaNumBytes;
    memcpy((char *) page + newOffset, (char *) data + numNullBytes, length);
}

void* RecordBasedFileManager::determinePageToUse(const RID &rid, FileHandle &handle) {
    void *page = NULL;
    if (handle.currentPageNum == (unsigned) rid.pageNum) {
        page = handle.currentPage;
    } else {
        page = malloc(PAGE_SIZE);
        handle.readPage(rid.pageNum, page);
    }
    return page;
}

void RecordBasedFileManager::extractFieldData(int numFields, int length, void *data, void *tempData) {
    int offset = 0;
    int numNullBytes = ceil((double)numFields / CHAR_BIT);
    memcpy((char *) data, (char *) tempData + sizeof(short), numNullBytes);

    // skip over the field offset data a
    offset += numNullBytes + ((numFields + 1) * sizeof(short));
    length -= offset;
    memcpy((char *) data + numNullBytes, (char *) tempData + offset, length);
}


bool RecordBasedFileManager::isFieldNull(const void *data, int i) {
    // create an bitmask to test if the field is null
    unsigned char *bitmask = (unsigned char*) malloc(1);
    memset(bitmask, 0, 1);
    *bitmask = 1 << 7;
    *bitmask >>= i % CHAR_BIT;

    // extract the NULL fields indicator from the data
    unsigned char *nullField = (unsigned char*) malloc(1);
    memcpy(nullField, (char *) data + (i / CHAR_BIT), 1);
    bool retVal = (*bitmask & *nullField) ? true : false;
    free(bitmask);
    free(nullField);
    return retVal;
}

std::string RecordBasedFileManager::extractType(const void *data, int *offset, AttrType t, AttrLength l) {
    if (t == TypeInt) {
        int value;
        memcpy(&value, (char *) data + *offset, sizeof(int));
        *offset += sizeof(int);
        return std::to_string((long long) value);
    } else if (t == TypeReal) {
        float val;
        memcpy(&val, (char *) data + *offset, sizeof(float));
        *offset += sizeof(float);
        std::stringstream ss;
        ss << std::fixed << std::setprecision(1) << val;
        return ss.str();
    } else if (t == TypeVarChar) {
        // first extract the length of the char
        int varCharLength;
        memcpy(&varCharLength, (char *) data + *offset, sizeof(int));

        // now generate a C string with the same length plus 1
        *offset += sizeof(int);
        char* s = new char[varCharLength + sizeof(char)];
        memcpy(s, (char *) data + *offset, varCharLength);
        char value = '\0';
        memcpy(s + varCharLength, &value, sizeof(char));

        std::string str(s);
        *offset += varCharLength;

        delete s;
        return str;
    } else {
        // this shouldn't happen since we assume all incoming data is correct
        return "ERROR EXTRACTING";
    }
}


int RecordBasedFileManager::buildMetaData(const void *data, const vector<Attribute> &descriptor, void *field) {
    short dataOffset = 0;

    // enter the number of fields as the first param in the field data
    short numFields = descriptor.size();
    memcpy((char *) field + dataOffset, &numFields, sizeof(short));
    dataOffset += sizeof(short);

    // Copy null field
    int numNullBytes = ceil((double)numFields / CHAR_BIT);
    memcpy((char *) field + dataOffset, (char *) data, numNullBytes);
    dataOffset += numNullBytes;

    // update the offset, include the num fields slot
    int fieldSize = numFields * sizeof(short);
    dataOffset += fieldSize;

    int fieldData = sizeof(short) + fieldSize;
    int fieldOffset = sizeof(short) + numNullBytes;

    int i;
    for (auto it = descriptor.begin(); it != descriptor.end(); ++it) {
        i = (it - descriptor.begin());
        int of = i * sizeof(short);
        if (isFieldNull(data, i)) {
            // don't add anything to dataOffset
            memcpy((char *) field + (fieldOffset + of), &dataOffset, sizeof(short));
        // find the type and calculate the field location and enter the dataOffset
        } else if (it->type == TypeInt) {
            memcpy((char *) field + (fieldOffset + of), &dataOffset, sizeof(short));
            dataOffset += sizeof(int);
        } else if (it->type == TypeReal) {
            memcpy((char *) field + (fieldOffset + of), &dataOffset, sizeof(short));
            dataOffset += sizeof(float);
        } else if (it->type == TypeVarChar) {
            memcpy((char *) field + (fieldOffset + (i * sizeof(short))), &dataOffset, sizeof(short));
            int varCharLength;
            memcpy(&varCharLength, (char *) data + (dataOffset - fieldData), sizeof(int));
            dataOffset += sizeof(int) + varCharLength;
        } else {
            // this should not happen since we assume all data coming it is always correct, for now
        }
    } // end of for loop
    return dataOffset;
}

void RecordBasedFileManager::getSlotFile(int slotNum, const void *page, int *offset, int *length) {
    // first lets get the slot offset
    int location = PAGE_SIZE - (((slotNum + 1) * SLOT_SIZE) + META_INFO);
    memcpy(offset, (char *) page + location, sizeof(int));
    memcpy(length, (char *) page + location + sizeof(int), sizeof(int));
}




int RecordBasedFileManager::findOpenSlot(FileHandle &handle, int size, RID &rid) {
    // first we need to check and see if the current page has available space
    int pageNum = handle.getNumberOfPages() - 1;
    if (pageNum < 0) {
        // this means we have no pages in a file and must generate a page
        return -1;
    }
    // if we get here we have a page current page and we need to get its freespace
    void *page = handle.currentPage;

    int freeSpace = handle.freeSpace[pageNum];
    int newSlotNum;
    if (freeSpace > (size + SLOT_SIZE)) {
        // the current page has enough space to fit a new record
        newSlotNum = getSlot(handle.currentPage, freeSpace);
        updateSlotDirectory(rid, pageNum, newSlotNum);
        return getFreeSpaceOffset(page);
    }

    int sizeOfFile = handle.currentPageNum;
    int retVal = -1;

    // we only need to test the pages upto the current one, since we already tested it.
    for (int pageNum = 0; pageNum < sizeOfFile; pageNum++) {
        freeSpace = handle.freeSpace[pageNum];
        // if the free space is big enough to accomodate the new record then stick it in.
        if (freeSpace > (size + SLOT_SIZE)) {
            // open a temp page and scan it for a new offset
            void *_tempPage = malloc(PAGE_SIZE);
            handle.readPage(pageNum, _tempPage);

            // update slot directory and get the freeSpaceOffset
            newSlotNum = getSlot(_tempPage, freeSpace);
            updateSlotDirectory(rid, pageNum, newSlotNum);
            retVal = getFreeSpaceOffset(_tempPage);
            free(_tempPage);
            break;
        }
    }
    // if we get here than no space was available and we need to append
    return retVal;
}

int RecordBasedFileManager::getSlot(const void *page, int freeSpace) {
    int numRecords = extractNumRecords(page);
    int startOfSlotDirectoryOffset = getStartOfDirectoryOffset(numRecords, page);

    // we need to test and see if the number of slots directories is equal to the
    // start of the Slot direcotry offset. If its not then we have tombstones
    int numSlotsOffset = PAGE_SIZE - ((numRecords * SLOT_SIZE) + META_INFO);

    // if start of the Slot Direcotry is smaller than number of Slots offset
    // than we know we have tombstones
    if (startOfSlotDirectoryOffset < numSlotsOffset) {
        // we need to loop through all the slot directories until an empyt slot is found
        int slotCounter = 0;
        int slotsOffset = PAGE_SIZE - (SLOT_SIZE + META_INFO);
        while (slotsOffset >= startOfSlotDirectoryOffset) {
            int offset, length;
            memcpy(&offset, (char *) page + slotsOffset, sizeof(int));
            memcpy(&length, (char *) page + slotsOffset + sizeof(int), sizeof(int));

            // check and see if this slot is empty
            if (length == 0 && offset == 0)
                return slotCounter;

            // keep moving along the slot directory
            slotsOffset -= SLOT_SIZE;
            slotCounter++;
        }

    } else {
        // if they are the same then just return the number of records since
        // that will be the next slot number
        return numRecords;
    }
    // if we get here then something went wrong
    return -1;
}

int RecordBasedFileManager::extractNumRecords(const void *page) {
    int numRecords;
    memcpy(&numRecords, (char *) page + N_OFFSET, sizeof(int));
    return numRecords;
}


void RecordBasedFileManager::updateSlotDirectory(RID &rid, int pageNum, int slotNum) {
    rid.pageNum = pageNum;
    rid.slotNum = slotNum;
}

int RecordBasedFileManager::getFreeSpaceOffset(const void *data) {
    int freeSpaceOffset;
    memcpy(&freeSpaceOffset, (char *) data + F_OFFSET, sizeof(int));
    return freeSpaceOffset;
}


void RecordBasedFileManager::setUpNewPage(void *newPage
        , const void *data
        , int length
        , FileHandle &handle
        , void *metaData
        , int fieldNumBytes
        , int recSize) {

    // for the next part we only want to know the length of the record and then copy all it's contents over
    transferRecordToPage(newPage, data, metaData, 0, fieldNumBytes, recSize, length);

    // we need put 1 page in the slot directory meta data
    int numRecords = 1;
    memcpy((char *) newPage + N_OFFSET, &numRecords, sizeof(int));

    // next we need to add slot 1 meta data, each slot is 2 ints (8 bytes) in length to fit the offset and length
    int slotOneOffset = N_OFFSET - (2 * sizeof(int));;

    // enter the offset first which is zero because its the first record in a page
    int offset = 0;
    memcpy((char *) newPage + slotOneOffset, &offset, sizeof(int));
    slotOneOffset += sizeof(int);
    memcpy((char *) newPage + slotOneOffset, &length, sizeof(int));

    // have the FreeSpaceOffset point to the end of the first record
    int freeSpaceOffset = length;
    memcpy((char *) newPage + F_OFFSET, &freeSpaceOffset, sizeof(int));


    // lets setup the freeSpace list in th fileHandle, we don't need a page number
    // because we are making a new page and we just append the end of the list
    int freeSpace = PAGE_SIZE - (freeSpaceOffset + SLOT_SIZE + META_INFO);
    handle.freeSpace.push_back(freeSpace);
}


int RecordBasedFileManager::extractFreeSpaceOffset(const void *page) {
     int freeSpaceOffset;
     memcpy(&freeSpaceOffset, (char *) page + F_OFFSET, sizeof(int));
     return freeSpaceOffset;
}


void RecordBasedFileManager::compactMemory(int offset, int deletedLength, void *data, int freeSpace) {
    // extract FreeSpaceOffset
    int freeSpaceOffset = extractFreeSpaceOffset(data);
    int startOfCompaction = offset + deletedLength;
    int sizeOfDataBeingCompacted = freeSpaceOffset - startOfCompaction;

    // move the data to a temp buffer
    void *dataBeingShifted = malloc(sizeOfDataBeingCompacted);
    memcpy((char *) dataBeingShifted, (char *) data + startOfCompaction, sizeOfDataBeingCompacted);

    // now shift the data over and fill the left over with zeros
    int newFreeSpaceOffset = decrementFreeSpaceOffset(data, deletedLength);
    memcpy((char *) data + offset, (char *) dataBeingShifted, sizeOfDataBeingCompacted);
    memset((char *) data + newFreeSpaceOffset, 0, deletedLength);

    // reduce the number of records by 1
    int numRecords = decrementNumRecords(data);

    // now we need to update all slots with their new offsets
    signed int recordOffset;
    int startOfSlotDirectoryOffset = getStartOfDirectoryOffset(numRecords, data);
    int endOfSlotDirectoryOffset = PAGE_SIZE - META_INFO;
    while (startOfSlotDirectoryOffset < endOfSlotDirectoryOffset) {
        memcpy(&recordOffset, (char *) data + startOfSlotDirectoryOffset, sizeof(int));
        if (recordOffset >= startOfCompaction) {
            recordOffset -= deletedLength;
            memcpy((char *) data + startOfSlotDirectoryOffset, &recordOffset, sizeof(int));
        }
        startOfSlotDirectoryOffset += SLOT_SIZE;
    }

    // free up space
    free(dataBeingShifted);
}

int RecordBasedFileManager::getStartOfDirectoryOffset(int numRecords, const void* page) {
    int currentOffset = N_OFFSET;
    int slotNum = 0;

    while (numRecords > 0) {
        // Pull out the slot information
        int offset, length;
        RecordBasedFileManager::getSlotFile(slotNum, page, &offset, &length);

        // test if it is a record
        if (length != 0) {
            numRecords--;
        }
        currentOffset -= SLOT_SIZE;
        slotNum++;
    }

    return currentOffset;
}

RBFM_ScanIterator::RBFM_ScanIterator() {
    pageNum = 0;
    slotNum = 0;
    scanPage = NULL;
    value = NULL;
}

RBFM_ScanIterator::~RBFM_ScanIterator() {
    //if (scanPage != NULL)
        //free(scanPage);
}


RC RecordBasedFileManager::scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
                                        const string &conditionAttribute, const CompOp compOp,
                                        const void *value, const vector<string> &attributeNames,
                                        RBFM_ScanIterator &rbfm_ScanIterator) {
    // first lets attach the fileHandle to the scanner iterater
    rbfm_ScanIterator.setHandle(fileHandle);
    rbfm_ScanIterator.setCompOp(compOp);
    rbfm_ScanIterator.setValue(value);

    // add the the first page to scanPage and set pageNum and slotNUm
    if ((int ) fileHandle.currentPageNum == rbfm_ScanIterator.getPageNum()) {
        rbfm_ScanIterator.setScanPage(fileHandle.currentPage);
    } else {
        // need to create a temp page cause scanPage is private
        void *_tempScan = malloc(PAGE_SIZE);
        if (fileHandle.readPage(rbfm_ScanIterator.getPageNum(), _tempScan) == -1) {
            free(_tempScan);
            return RBFM_EOF;
        }
        rbfm_ScanIterator.setScanPage(_tempScan);
    }

    // collect the attribute placements for each record
    int i;
    bool foundCondition = false;
    for (auto itN = attributeNames.begin(); itN != attributeNames.end(); ++itN) {
        for (auto it = recordDescriptor.begin(); it != recordDescriptor.end(); ++it) {
            i = it - recordDescriptor.begin();
            if (!foundCondition && it->name == conditionAttribute) {
                rbfm_ScanIterator.setCondType(it->type);
                rbfm_ScanIterator.setConditionAttr(i);
                foundCondition = true;
            }
            if (strcmp(it->name.c_str(), itN->c_str()) == 0) {
                rbfm_ScanIterator.setAttrTypes(it->type);
                rbfm_ScanIterator.setAttrPlacement(i);
            }
        }
    }
    return 0;
}


bool RBFM_ScanIterator::isEndOfPage(void *page, int numRecords, int slotNum, int pageNum) {
    int startOfSlotDirectoryOffset = RecordBasedFileManager::getStartOfDirectoryOffset(numRecords, page);
    int currentSlotOffset = PAGE_SIZE - (((slotNum + 1) * SLOT_SIZE) + META_INFO);
    if (startOfSlotDirectoryOffset > currentSlotOffset)
       return true;
   return false;
}

// get the next record
RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
    bool condNotMet = true;
    int numRecords = RecordBasedFileManager::extractNumRecords(scanPage);
    int rc = RBFM_EOF;

    // we have to check for empty slots
    while (condNotMet) {
        // if we on on the last page and at the end of the page end this search
        if ((unsigned) pageNum == handle->currentPageNum && isEndOfPage(scanPage, numRecords, slotNum, pageNum)) {
            condNotMet = false;
            rc = RBFM_EOF;
            continue;
        }

        // check for end of the page and load new page if needed
        if (isEndOfPage(scanPage, numRecords, slotNum, pageNum)) {
            handle->readPage(++pageNum, scanPage);
            numRecords = RecordBasedFileManager::extractNumRecords(scanPage);
            slotNum = 0;
        }
        // enter in the rid info
        rid.pageNum = pageNum;
        rid.slotNum = slotNum++;

        // check for NULL Fields and make sure they are null
        int offset, length;
        RecordBasedFileManager::getSlotFile(rid.slotNum, scanPage, &offset, &length);

        if (length <= 0) {
            // this means the slot is a tombstone or its a pointer to another page
            continue;
        }
        // extract the record in the slot
        void *record = RecordBasedFileManager::extractRecord(rid.slotNum, scanPage);

        short numFields = RecordBasedFileManager::getNumberOfFields(record);
        int numNullBytes = ceil((double) numFields / CHAR_BIT);
        void *nullField = malloc(numNullBytes);
        memcpy((char *) nullField, (char *) record + FIELD_OFFSET, numNullBytes);

        if(RecordBasedFileManager::isFieldNull(nullField, conditionAttribute)) {
            // this means the field we are trying to COMP is NULL
            /******* TODO: what if we have a NO-OP? ********/
            free(record);
            free(nullField);
            continue;
        }

        // we need to determine the offset of condition attribute and extract where it starts
        short startOfCondOffset;
        int condFieldOffset;
        condFieldOffset = FIELD_OFFSET + numNullBytes + (conditionAttribute * FIELD_OFFSET);
        memcpy(&startOfCondOffset, (char *) record + condFieldOffset, FIELD_OFFSET);

        // test the condition we need to extract
        bool isCompTrue;

        // here we need to run the comparison functions with the data
        if (condType == TypeInt) {
            isCompTrue = processIntComp(startOfCondOffset, compOp, value, record);
        } else if (condType == TypeReal) {
            isCompTrue = processFloatComp(startOfCondOffset, compOp, value, record);
        } else if (condType == TypeVarChar) {
            isCompTrue = processStringComp(startOfCondOffset, compOp, value, record);
        } else {
            // this is bad
            return -1;
        }
        if (isCompTrue) {
            // extract the attributes
            extractScannedData(record, data, length, numFields, nullField);
            int test;
            memcpy(&test, (char *) data + 1, sizeof(int));

            condNotMet = false;
            rc = 0;
        }
        free(record);
        free(nullField);
    }
    return rc;
}

RC RBFM_ScanIterator::close() {
    handle = NULL;
    attrPlacement.clear();
    attrTypes.clear();

    pageNum = 0;
    slotNum = 0;
    if (scanPage != NULL)
        free(scanPage);

    return 0;
}


bool RBFM_ScanIterator::processIntComp(int condOffset, CompOp compOp, const void *value, const void *record) {
    if (compOp == NO_OP) {
        return true;
    }
    int intVal;
    memcpy(&intVal, (char *) value, sizeof(int));

    int recordVal;
    memcpy(&recordVal, (char *) record + condOffset, sizeof(int));

    switch(compOp) {
        case EQ_OP:     return recordVal == intVal;
        case LT_OP:     return recordVal < intVal;
        case GT_OP:     return recordVal > intVal;
        case LE_OP:     return recordVal <= intVal;
        case GE_OP:     return recordVal >= intVal;
        case NE_OP:     return recordVal != intVal;
        default:        return false;
    }
}


bool RBFM_ScanIterator::processFloatComp(int condOffset, CompOp compOp, const void *value, const void *record) {
    if (compOp == NO_OP) {
        return true;
    }

    float floatVal;
    memcpy(&floatVal, (char *) value, sizeof(float));

    float recordVal;
    memcpy(&recordVal, (char *) record + condOffset, sizeof(float));

    switch(compOp) {
        case EQ_OP:     return recordVal == floatVal;
        case LT_OP:     return recordVal < floatVal;
        case GT_OP:     return recordVal > floatVal;
        case LE_OP:     return recordVal <= floatVal;
        case GE_OP:     return recordVal >= floatVal;
        case NE_OP:     return recordVal != floatVal;
        default:        return false;
    }
}


bool RBFM_ScanIterator::processStringComp(int condOffset, CompOp compOp, const void *value, const void *record) {
    if (compOp == NO_OP) {
        return true;
    }

    int valueLength;
    memcpy(&valueLength, (char *) value, sizeof(int));

    // now generate a C string with the same length plus 1
    char* s = new char[valueLength + 1];
    memcpy(s, (char *) value + sizeof(int), valueLength);
    s[valueLength] = '\0';

    int varCharLength;
    memcpy(&varCharLength, (char *) record + condOffset, sizeof(int));
    condOffset += sizeof(int);

    char* sv = new char[varCharLength + 1];
    memcpy(sv, (char *) record + condOffset, varCharLength);
    sv[varCharLength] = '\0';

    bool returnVal;
    switch(compOp) {
        case 0:     returnVal = true;
                    break;
        case 1:     returnVal = strcmp(s, sv) == 0 ? true : false;
                    break;
        case 2:     returnVal = strcmp(s, sv) < 0 ? true : false;
                    break;
        case 3:     returnVal = strcmp(s, sv) > 0 ? true : false;
                    break;
        case 4:     returnVal = strcmp(s, sv) < 0 || strcmp(s, sv) == 0 ? true : false;
                    break;
        case 5:     returnVal = strcmp(s, sv) > 0 || strcmp(s, sv) == 0 ? true : false;
                    break;
        case 6:     returnVal = strcmp(s, sv) != 0 ? true : false;
                    break;
        default:    returnVal = false;
                    break;
    }
    delete []s;
    delete []sv;
    return returnVal;
}


void RBFM_ScanIterator::extractScannedData(void *record, void *data, int length, int numFields, void *nullField) {
    // go through each placement and extract that data
    int sizeOfReturnAttrs = attrPlacement.size();
    AttrType currentType;
    int attrSpot;

    // lets create our nullFields
    int newNumBytes = ceil((double) sizeOfReturnAttrs / CHAR_BIT);
    char *newNullField = new char[newNumBytes];
    memset(newNullField, 0, newNumBytes);

    // make room for the data, we know the returned value will be at max the size of the record
    void *tempData = malloc(length);
    int tempDataOffset = 0;

    // get the old offsets
    int numNullBytes = ceil((double) numFields / CHAR_BIT);
    short startOfFieldOffset = FIELD_OFFSET + numNullBytes;

    for (int i = 0; i < sizeOfReturnAttrs; ++i) {
        // lets extract the first
        currentType = attrTypes[i];
        attrSpot = attrPlacement[i];
        short fieldOffset = startOfFieldOffset + (attrSpot * FIELD_OFFSET);
        short dataOffset;
        memset(&dataOffset, 0, sizeof(short));
        memcpy(&dataOffset, (char *) record + fieldOffset, sizeof(short));

        // lets extract the data we need to get the next attribute
        if (RecordBasedFileManager::isFieldNull(nullField, attrSpot)) {
            newNullField[0] = (1 << 7);
            newNullField[0] >>= attrSpot;
            continue;
        }


        // Here we begin putting the field data into a temp holder
        if (currentType == TypeInt) {
            memcpy((char *) tempData + tempDataOffset, (char *) record + dataOffset, sizeof(int));
            tempDataOffset += sizeof(int);
        } else if (currentType == TypeReal) {
            memcpy((char *) tempData + tempDataOffset, (char *) record + dataOffset, sizeof(float));
            tempDataOffset += sizeof(float);
        } else if (currentType == TypeVarChar) {
            int varCharLength;
            memcpy(&varCharLength, (char *) record + dataOffset, sizeof(int));
            memcpy((char *) tempData + tempDataOffset, &varCharLength, sizeof(int));
            memcpy((char *) tempData + tempDataOffset + sizeof(int)
                                        , (char *) record + dataOffset + sizeof(int), varCharLength);

            tempDataOffset += sizeof(int) + varCharLength;
        } else {
            // should not get here
        }
    } // end of for loop
    // now we can piece together all the data, this will only work if RM layer
    // initializes returnData with NULL
    /********* NOT SURE IF I NEED TO ALLOCATE HERE *************/

    memcpy((char *) data, newNullField, newNumBytes);
    memcpy((char *) data + newNumBytes, (char *) tempData, tempDataOffset);

    delete []newNullField;
    free(tempData);
}


