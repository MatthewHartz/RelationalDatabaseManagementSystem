
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "../rbf/rbfm.h"

using namespace std;

# define RM_EOF (-1)  // end of a scan operator

// function declarations
void addAttributeToDesc(string name, AttrType type, AttrLength length, vector<Attribute> &descriptor);
void prepareTablesRecord(const int id, const string &table, const string &file, void *buffer);
void prepareColumnsRecord(const int id, const string &name, const AttrType type, const int length, const int position, void *buffer);

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data) { return RM_EOF; };
  RC close() { return -1; };
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
};

#endif
