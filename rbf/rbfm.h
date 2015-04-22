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

// function declarations
int getRecordSize(const void *data, const vector<Attribute> &descriptor);


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
int getCompOp(CompOp compOp);
int getAnyTypeOffset(const vector<Attribute> &descriptor, void *data, int cond, AttrType &type);
bool processIntComp(int condOffset, CompOp compOp, const void *value, const void *page);
bool processFloatComp(int condOffset, CompOp compOp, const void *value, const void *page);
bool processStringComp(int condOffset, CompOp compOp, const void *value, const void *page);
void extractScannedData(vector<int> &placement, const vector<Attribute> &descriptor, void *page, int offset, void *data);


class RBFM_ScanIterator {
public:
    FileHandle *handle;
    vector<int> attrPlacement; 
    const vector<Attribute> *descriptor;
    const void *value;
    void *scanPage;
    CompOp compOp;
    AttrType condType;
    int conditionAttribute;
    int descSize;
    int currentOffset;
    int pageNum;
    int slotNum;
    
	RBFM_ScanIterator() { handle = NULL, scanPage = NULL, pageNum = 0, slotNum = 0; };
	~RBFM_ScanIterator() {};

	// "data" follows the same format as RecordBasedFileManager::insertRecord()
	RC getNextRecord(RID &rid, void *data);
	RC close() { return -1; };
};


inline bool lessThan(int a, int b) { return a < b; };
inline bool lessThan(float a, float b) { return a < b; };

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

public:

protected:
	RecordBasedFileManager();
	~RecordBasedFileManager();

private:
	static RecordBasedFileManager *_rbf_manager;
	PagedFileManager *pfm;

    void compactMemory(int offset, int length, const void *data);
    bool isFieldNull(const void *data, int i);
    std::string extractType(const void *data, int *offset, AttrType t, AttrLength l);
    void getSlotFile(int slotNum, const void *page, int *offset, int *length);
    int findOpenSlot(FileHandle &handle, int size, RID &rid);
    int getFreeSpaceOffset(const void *data, RID &rid);
    void setUpNewPage(const void *newPage, const void *data, int length, FileHandle &handle);

};

#endif
