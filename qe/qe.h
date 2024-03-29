#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <map>

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

#define QE_EOF (-1)  // end of the index scan

using namespace std;

// int triplet
struct intMapEntry
{
    int attr;
    void *buffer;
    int size;
};

// real triplet
struct realMapEntry
{
    float attr;
    void *buffer;
    int size;
};

// varChar triplet
struct varCharMapEntry
{
    string attr;
    void *buffer;
    int size;
};

// typedefs
typedef enum{ COUNT=0, SUM, AVG, MIN, MAX } AggregateOp;

typedef map<int, vector<intMapEntry>> intMap;
typedef map<float, vector<realMapEntry>> realMap;
typedef map<string, vector<varCharMapEntry>> varCharMap;
typedef map<int, float> intAggregateMap;
typedef map<float, float> realAggregateMap;
typedef map<string, float> varCharAggregateMap;

// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by
//                 the characters

struct Value {
    AttrType type;          // type of value
    void     *data;         // value
};


struct Condition {
    string  lhsAttr;        // left-hand side attribute
    CompOp  op;             // comparison operator
    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    string  rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};


class Iterator {
    // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;
        virtual void getAttributes(vector<Attribute> &attrs) const = 0;
        virtual ~Iterator() {};
};


class TableScan : public Iterator
{
    // A wrapper inheriting Iterator over RM_ScanIterator
    public:
        RelationManager &rm;
        RM_ScanIterator *iter;
        string tableName;
        vector<Attribute> attrs;
        vector<string> attrNames;
        RID rid;

        TableScan(RelationManager &rm, const string &tableName, const char *alias = NULL):rm(rm)
        {
        	//Set members
        	this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            unsigned i;
            for(i = 0; i < attrs.size(); ++i)
            {
                // convert to char *
                attrNames.push_back(attrs.at(i).name);
            }

            // Call rm scan to get iterator
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator(CompOp compOp, string condAttribute, vector<string> attrs, Value v)
        {
            iter->close();
            delete iter;
            iter = new RM_ScanIterator();
            rm.scan(tableName, condAttribute, compOp, v.data, attrs, *iter);
        };

        RC getNextTuple(void *data)
        {
            return iter->getNextTuple(rid, data);
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        ~TableScan()
        {
        	iter->close();
        };
};


class IndexScan : public Iterator
{
    // A wrapper inheriting Iterator over IX_IndexScan
    public:
        RelationManager &rm;
        RM_IndexScanIterator *iter;
        string tableName;
        string attrName;
        vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;

        IndexScan(RelationManager &rm, const string &tableName, const string &attrName, const char *alias = NULL):rm(rm)
        {
        	// Set members
        	this->tableName = tableName;
        	this->attrName = attrName;


            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void* lowKey,
                         void* highKey,
                         bool lowKeyInclusive,
                         bool highKeyInclusive)
        {
            iter->close();
            delete iter;
            iter = new RM_IndexScanIterator();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive,
                           highKeyInclusive, *iter);
        };

        RC getNextTuple(void *data)
        {
            int rc = iter->getNextEntry(rid, key);
            if(rc == 0)
            {
                rc = rm.readTuple(tableName.c_str(), rid, data);
            }
            return rc;
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        ~IndexScan()
        {
            iter->close();
        };
};


class Filter : public Iterator {
    // Filter operator
    public:
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        );
        ~Filter(){};

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const { in->getAttributes(attrs); };
        bool compareValues(void *left, void* right);
    private:
        Iterator *in;
        int leftConditionPos;
        Attribute leftConditionAttr;
        int rightConditionPos;
        Attribute rightConditoinAttr;
        Condition filterCondition;
};


class Project : public Iterator {
    // Projection operator
    public:
        Project(Iterator *input,                    // Iterator of input R
              const vector<string> &attrNames);   // vector containing attribute names
        ~Project(){};

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const { iterator->getAttributes(attrs); };

        // setters
        void setIterator(Iterator* iter) { iterator = iter; };
        void setAttributeNames(vector<string> attrs) { attributeNames = attrs; };

        // getters
        Iterator *getIterator(void) { return iterator; };
        vector<string> getAttributeNames(void) { return attributeNames; };


    private:
        Iterator *iterator;
        vector<string> attributeNames;
};

// Optional for the undergraduate solo teams. 5 extra-credit points
class BNLJoin : public Iterator {
    // Block nested-loop join operator
    public:
        BNLJoin(Iterator *leftIn,            // Iterator of input R
               TableScan *rightIn,           // TableScan Iterator of input S
               const Condition &condition,   // Join condition
               const unsigned numRecords     // # of records can be loaded into memory, i.e., memory block size (decided by the optimizer)
        );
        ~BNLJoin(){};

        // Start a new iterator given the new key range
        void setIterator(Iterator *iter, const Condition condition)
        {
            iter->~Iterator();
            delete iter;

            // determine which iterate has been given to use (left or right)
            string leftAttr = condition.lhsAttr;


            // determine if

            //string tablename = condition.

            //iter = new RM_ScanIterator();
            //rm.scan(tableName, condAttribute, compOp, v.data, attrs, *iter);
        };

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;

        // setters
        void setLeftIterator(Iterator *input) { leftIn = input; };
        void setRightIterator(TableScan *input) { rightIn = input; };
        void setLeftJoinAttribute(Attribute attribute) { leftJoinAttribute = attribute; };
        void setRightJoinAttribute(Attribute attribute) { rightJoinAttribute = attribute; };
        void setLeftNumAttrs(int num) { leftNumAttrs = num; };
        void setRightNumAttrs(int num) { rightNumAttrs = num; };
        void setNumRecords(int num) { numRecords = num; };

        // getters
        Iterator* getLeftIterator(void) const { return leftIn; };
        TableScan* getRightIterator(void) const { return rightIn; };
        Attribute getLeftJoinAttribute(void) const { return leftJoinAttribute; };
        Attribute getRightJoinAttribute(void) const { return rightJoinAttribute; };
        int getLeftNumAttrs(void) const { return leftNumAttrs; };
        int getRightNumAttrs(void) const { return rightNumAttrs; };
        int getNumRecords(void) const { return numRecords; };

    private:
        Iterator *leftIn;
        TableScan *rightIn;
        Attribute leftJoinAttribute;
        Attribute rightJoinAttribute;
        int leftNumAttrs;
        int rightNumAttrs;
        int numRecords;
        bool innerFinished = true; // This variable lets the right outer loop know it needs to refresh the map
        intMap intHashMap;
        realMap realHashMap;
        varCharMap varCharHashMap;

        // these might be unnecessary
        int intHashFunction(int data, int numRecords);
        int realHashFunction(float data, int numRecords);
        int varCharHashFunction(string data, int numRecords);
};


class INLJoin : public Iterator {
    // Index nested-loop join operator
    public:
        INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition   // Join condition
        );
        ~INLJoin() {};

        RC getNextTuple(void *data);
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;

        // setters
        void setLeftIterator(Iterator *input) { leftIn = input; };
        void setRightIterator(IndexScan *input) { rightIn = input; };
        void setLeftJoinAttribute(Attribute attribute) { leftJoinAttribute = attribute; };
        void setRightJoinAttribute(Attribute attribute) { rightJoinAttribute = attribute; };
        void setLeftNumAttrs(int num) { leftNumAttrs = num; };
        void setRightNumAttrs(int num) { rightNumAttrs = num; };

        // getters
        Iterator* getLeftIterator(void) const { return leftIn; };
        IndexScan* getRightIterator(void) const { return rightIn; };
        Attribute getLeftJoinAttribute(void) const { return leftJoinAttribute; };
        Attribute getRightJoinAttribute(void) const { return rightJoinAttribute; };
        int getLeftNumAttrs(void) const { return leftNumAttrs; };
        int getRightNumAttrs(void) const { return rightNumAttrs; };

    private:
        Iterator *leftIn;
        IndexScan *rightIn;
        Attribute leftJoinAttribute;
        Attribute rightJoinAttribute;
        int leftNumAttrs;
        int rightNumAttrs;
        int numRecords;
        bool innerFinished = true; // This variable lets the right outer loop know it needs to refresh the map
};

// Optional for everyone. 10 extra-credit points
class GHJoin : public Iterator {
    // Grace hash join operator
    public:
      GHJoin(Iterator *leftIn,               // Iterator of input R
            Iterator *rightIn,               // Iterator of input S
            const Condition &condition,      // Join condition (CompOp is always EQ)
            const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
      ){};
      ~GHJoin(){};

      RC getNextTuple(void *data){return QE_EOF;};
      // For attribute in vector<Attribute>, name it as rel.attr
      void getAttributes(vector<Attribute> &attrs) const{};
};

class Aggregate : public Iterator {
    // Aggregation operator
    public:
        // Mandatory for graduate teams only
        // Basic aggregation
        Aggregate(Iterator *input,          // Iterator of input R
                  Attribute aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        );

        // Optional for everyone. 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  Attribute aggAttr,           // The attribute over which we are computing an aggregate
                  Attribute groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op              // Aggregate operation
        );
        ~Aggregate(){};

        RC getNextTuple(void *data);
        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrname = "MAX(rel.attr)"
        void getAttributes(vector<Attribute> &attrs) const;

        // setters
        void setIterator(Iterator *input) { aggregateIterator = input; };
        void setAggAttribute(Attribute attr) { aggregateAttr = attr; };
        void setGroupAttribute(Attribute attr) { groupAttr = attr; };
        void setOperator(AggregateOp op) { aggregateOp = op; };
        void setValue(float value) { aggregateValue = value; };
        void setIsGroupBy() { isGroupBy = true; };

        // getters
        Iterator* getIterator(void) { return aggregateIterator; };
        Attribute getAggAttribute(void) { return aggregateAttr; };
        Attribute getGroupAttribute(void) { return groupAttr; };
        AggregateOp getOperator(void) { return aggregateOp; };
        float getValue(void) { return aggregateValue; };
        int getGroupPosition(void) { return groupPosition; };

    private:
        Iterator *aggregateIterator;
        Attribute aggregateAttr;
        Attribute groupAttr;
        bool isGroupBy = false;
        AggregateOp aggregateOp;
        float aggregateValue; // This is the value that is returned from aggregate ie MAX,MIN,COUNT,AVG,SUM
        int groupPosition = 0; // this saves the state when getting the next tuple with a group by statement

        // for group based aggregations
        intMap intHashMap;
        realMap realHashMap;
        varCharMap varCharHashMap;
        intAggregateMap intAggMap;
        realAggregateMap realAggMap;
        varCharAggregateMap varCharAggMap;
};

static RC joinBufferData(void *buffer1
        , int buffer1Len
        , int numAttrs1
        , void* buffer2
        , int buffer2Len
        , int numAttrs2
        , void* data);

#endif
