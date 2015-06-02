
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <cstdlib>

#include "../rbf/rbfm.h"
#include "../ix/ix.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator

typedef enum { TypeSystem = 1, TypeUser = 2} AuthorizationType;

// function declarations
void addAttributeToDesc(string name, AttrType type, AttrLength length, vector<Attribute> &descriptor);
void prepareTablesRecord(const int id, const string &table, const string &file, AuthorizationType authType, void *buffer);
void prepareColumnsRecord(const int id, const string &name, const AttrType type, const int length, const int position, void *buffer);
void prepareIndexesRecord(const int tableId, const int columnId, const string &fileName, void *buffer);

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
    RM_ScanIterator() {}; 
    ~RM_ScanIterator() {};

    RBFM_ScanIterator rbfmsi;
    FileHandle *handle;
    RecordBasedFileManager *scanRBFM;

    // "data" follows the same format as RelationManager::insertTuple()
    RC getNextTuple(RID &rid, void *data) { return rbfmsi.getNextRecord(rid, data); };
    RC close();

    // Getters and Setters
    //void setHandle(FileHandle &fileHandle) { handle = fileHandle; };
    void setCompOp(const CompOp op) { compOp = op; };
    void setValue(const void *val) { value = (void *)val; };
    void setDescriptor(const vector<Attribute> desc) { descriptor = desc; };
    void setConditionAttr(const string attr) { conditionAttr = attr; };
    void setAttributeNames(const vector<string> names) { attributeNames = names; };
    //void setRBFM(const RBFM_ScanIterator r) { rbfm = r; };

private:
    vector<Attribute> descriptor;
    string conditionAttr;
    CompOp compOp;
    void* value;
    vector<string> attributeNames;
    //RBFM_ScanIterator rbfmsi;
};

class RM_IndexScanIterator {
 public:
  RM_IndexScanIterator() { ix_ScanIterator = new IX_ScanIterator(); };    // Constructor
  ~RM_IndexScanIterator() { delete ix_ScanIterator; };   // Destructor

  // "key" follows the same format as in IndexManager::insertEntry()
  RC getNextEntry(RID &rid, void *key);    // Get next matching entry
  RC close();                      // Terminate index scan

  /// Getters and Setters
  IX_ScanIterator* getIXScanner() { return ix_ScanIterator; };

 private:
  IX_ScanIterator *ix_ScanIterator;

};


// Relation Manager
class RelationManager
{
public:
    static RelationManager* instance();

    RC createCatalog();

    RC deleteCatalog();

    RC createTable(const string &tableName, const vector<Attribute> &attrs);

    RC deleteTable(const string &tableName);

    RC getAttributes(const string &tableName, vector<Attribute> &attrs);

    RC insertTuple(const string &tableName, const void *data, RID &rid);

    RC deleteTuple(const string &tableName, const RID &rid);

    RC updateTuple(const string &tableName, const void *data, const RID &rid);

    RC readTuple(const string &tableName, const RID &rid, void *data);

    // mainly for debugging
    // Print a tuple that is passed to this utility method.
    RC printTuple(const vector<Attribute> &attrs, const void *data);

    // mainly for debugging
    RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

    // scan returns an iterator to allow the caller to go through the results one by one.
    RC scan(const string &tableName,
        const string &conditionAttribute,
        const CompOp compOp,                  // comparison type such as "<" and "="
        const void *value,                    // used in the comparison
        const vector<string> &attributeNames, // a list of projected attributes
        RM_ScanIterator &rm_ScanIterator);

    RC createIndex(const string &tableName, const string &attributeName);

    RC destroyIndex(const string &tableName, const string &attributeName);

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC indexScan(const string &tableName,
                        const string &attributeName,
                        const void *lowKey,
                        const void *highKey,
                        bool lowKeyInclusive,
                        bool highKeyInclusive,
                        RM_IndexScanIterator &rm_IndexScanIterator);

    // Extra helper functions
    RC createSystemTable(const string &tableName, const vector<Attribute> &attrs);

    vector<Attribute> getTablesDesc() { return this->tablesDescriptor; }
    vector<Attribute> getColumnsDesc() { return this->columnsDescriptor; }
    vector<Attribute> getIndexesDesc() { return this->indexDescriptor; }
    void setTablesDesc(vector<Attribute> desc) { this->tablesDescriptor = desc; }
    void setColumnsDesc(vector<Attribute> desc) { this->columnsDescriptor = desc; }
    void setIndexDesc(vector<Attribute> desc) { this->indexDescriptor = desc; }
    RC getTableFileNameAndAuthType(const string &tableName, string &fileName, int &authType);
    RC getIndexFileName(const string &tableName, const string &attributeName, string &indexName, RID &rid);

    // Extra credit work (10 points)
public:
    RC dropAttribute(const string &tableName, const string &attributeName);

    RC addAttribute(const string &tableName, const Attribute &attr);
protected:
    RelationManager();
    ~RelationManager();

private:
    static RelationManager *_rm;
    RecordBasedFileManager *rbfm;
    IndexManager *ix; 
    vector<Attribute> tablesDescriptor;
    vector<Attribute> columnsDescriptor;
    vector<Attribute> indexDescriptor;
};

#endif
