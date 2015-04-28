#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <climits>
#include <iostream>
#include <cmath>
#include <cstring>
#include <vector>
#include <iterator>
#include <stdio.h>
#include <sstream>
#include <iomanip>
#include <cstring>

#include "../rbf/pfm.h"

using namespace std;

// Stuff
# define RM_EOF (-1)

// Constants
const int RECORD_ATTR_OFFSET_SIZE = 4;


// Record ID
typedef struct
{
  unsigned pageNum;	// page number
  unsigned slotNum; // slot number in the page
} RID;


// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;

typedef unsigned AttrLength;

struct Attribute {
	string   name;     // attribute name
	AttrType type;     // attribute type
	AttrLength length; // attribute length
};


// Comparison Operator (NOT needed for part 1 of the project)
typedef enum { NO_OP = 0,  // no condition
		   EQ_OP = 1,      // =
		   LT_OP = 2,      // <
		   GT_OP = 3,      // >
		   LE_OP = 4,      // <=
		   GE_OP = 5,      // >=
		   NE_OP = 6,      // !=
} CompOp;



/****************************************************************************
The scan iterator is NOT required to be implemented for part 1 of the project 
*****************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iterator to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

// function helpers for scan Iterator


class RBFM_ScanIterator {
public:
        
    RBFM_ScanIterator();
    ~RBFM_ScanIterator();

    // "data" follows the same format as RecordBasedFileManager::insertRecord()
    RC getNextRecord(RID &rid, void *data);
    RC close() { return -1; };

    // Getters and Setters
    void setHandle(FileHandle &fileHandle) { handle = &fileHandle; };
    void setCompOp(const CompOp op) { compOp = op; };
    void setValue(const void *val) { value = val; };
    void setConditionAttr(int i) { conditionAttribute = i; };
    void setCondType(AttrType type) { condType = type; };
    void setAttrPlacement(int i) { attrPlacement.push_back(i); };
    void setAttrTypes(AttrType type) { attrTypes.push_back(type); };
    void setScanPage(void *p) { scanPage = p; };

    int getPageNum() { return pageNum; };
    void* getScanPage() { return scanPage; };
    int getNumFields(void *page);

private:
    FileHandle *handle;
    vector<int> attrPlacement; 
    vector<AttrType> attrTypes;
    const void *value;
    void *scanPage;
    CompOp compOp;
    AttrType condType;
    int conditionAttribute;
    int pageNum;
    int slotNum;

    int getCompOp(CompOp compOp);
    bool processIntComp(int condOffset, CompOp compOp, const void *value, const void *record);
    bool processFloatComp(int condOffset, CompOp compOp, const void *value, const void *record);
    bool processStringComp(int condOffset, CompOp compOp, const void *value, const void *record);
    void extractScannedData(void *record, void *data, int length, int numRecords, void *nullField);
    bool isEndOfPage(void *page, int slotNum, int pageNum);
};



class RecordBasedFileManager
{
public:
	static RecordBasedFileManager* instance();

	RC createFile(const string &fileName);
  
	RC destroyFile(const string &fileName);
  
	RC openFile(const string &fileName, FileHandle &fileHandle);
  
	RC closeFile(FileHandle &fileHandle);

    //  Format of the data passed into the function is the following:
    //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
    //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
    //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
    //     Each bit represents whether each field contains null value or not.
    //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data.
    //     If k-th bit from the left is set to 0, k-th field contains non-null values.
    //     If thre are more than 8 fields, then you need to find the corresponding byte, then a bit inside that byte.
    //  2) actual data is a concatenation of values of the attributes
    //  3) For int and real: use 4 bytes to store the value;
    //     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
    //  !!!The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute()
    // For example, refer to the Q8 of Project 1 wiki page.
    RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);

    RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);
  
    // This method will be mainly used for debugging/testing
    RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

    /**************************************************************************************************************************************************************
    IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for part 1 of the project
    ***************************************************************************************************************************************************************/
    RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

    // Assume the rid does not change after update
    RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);

    RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data);

    // scan returns an iterator to allow the caller to go through the results one by one. 
    RC scan(FileHandle &fileHandle,
        const vector<Attribute> &recordDescriptor,
        const string &conditionAttribute,
        const CompOp compOp,                  // comparision type such as "<" and "="
        const void *value,                    // used in the comparison
        const vector<string> &attributeNames, // a list of projected attributes
        RBFM_ScanIterator &rbfm_ScanIterator);

    static void getSlotFile(int slotNum, const void *page, int *offset, int *length);
    static bool isFieldNull(const void *data, int i);
    static int extractNumRecords(void *page);
    static int extractFreeSpaceOffset(const void *page);

public:

protected:
    RecordBasedFileManager();
    ~RecordBasedFileManager();

private:
    static RecordBasedFileManager *_rbf_manager;
    void *readingPage;
    RID readingRID;
    PagedFileManager *pfm;

    void compactMemory(int offset, int deletedLength, void *data, int freeSpace);
    std::string extractType(const void *data, int *offset, AttrType t, AttrLength l);
    int findOpenSlot(FileHandle &handle, int size, RID &rid);
    int getFreeSpaceOffset(const void *data);
    void setUpNewPage(void *newPage, const void *data, int length, FileHandle &handle, void *field, int fieldNumBytes, int recSize);
    void updateSlotDirectory(RID &rid, int pageNum, int slotNum);
    int getRecordSize(const void *data, const vector<Attribute> &descriptor, void *field);
    void* determinePageToUse(const RID &rid, FileHandle &handle);
    void transferRecordToPage(void *page, const void *data, void *metaData, int newOffset, int fieldNumBytes, int recSize, int length);
    int incrementNumRecords(void *page);
    int decrementNumRecords(void *page);
    int incrementFreeSpaceOffset(void *page, int length);
    int decrementFreeSpaceOffset(void *page, int length);
    void updateFreeSpace(int numRecords, int freeSpaceOffset, int pageNum, FileHandle &handle);
    void extractFieldData(int numFields, int length, void *data, void *tempData);
};

#endif
