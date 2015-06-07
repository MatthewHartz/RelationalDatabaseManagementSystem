
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) {
    in = input;
    string attr = condition.bRhsIsAttr ? condition.rhsAttr : condition.lhsAttr;

    // initializes to return all attributes
    vector<Attribute> attrs;
    in->getAttributes(attrs);

    // Conver to vector of strings
    vector<string> returnAttrs;
    unsigned i;
    for(i = 0; i < attrs.size(); ++i)
    {
        // convert to char *
        attrs.at(i).name.erase(0, attrs.at(i).name.find(".") + 1);
        returnAttrs.push_back(attrs.at(i).name);
    }

    attr.erase(0, attr.find(".") + 1);
    if (dynamic_cast<TableScan*>(in)) {
        static_cast<TableScan*>(in)->setIterator(condition.op, attr, returnAttrs, condition.rhsValue);
    } else {
        bool lowK = false;
        bool highK = false;
        if (!condition.bRhsIsAttr && (condition.op == GE_OP || condition.op == LE_OP)) {
            lowK = true;
            static_cast<IndexScan*>(in)->setIterator(condition.rhsValue.data, NULL, true, false);
        } else if(condition.bRhsIsAttr && (condition.op == GE_OP || condition.op == LE_OP)) {
            // not sure what to do here
        }

    }
}

Project::Project(Iterator* input, const vector<string> &attrNames) {
    in = input;
    attrs = attrNames;
    Value v;
    v.data = NULL;

    // Conver to vector of strings
    vector<string> returnAttrs;
    unsigned i;
    for(i = 0; i < attrs.size(); ++i)
    {
        attrs.at(i).erase(0, attrs.at(i).find(".") + 1);
    }

    if (dynamic_cast<TableScan*>(in)) {
        static_cast<TableScan*>(in)->setIterator(NO_OP, "", attrs, v);
    } else {
       // static_cast<IndeScan*>(in)->set

    }
}

// ... the rest of your implementations go here


RC Filter::getNextTuple(void *data) {
    // here we can use in and cond to do stuff    
    if (dynamic_cast<TableScan*>(in)) {
        if (static_cast<TableScan*>(in)->getNextTuple(data) == -1) {
            return -1;
        }
    } else {
        if (static_cast<IndexScan*>(in)->getNextTuple(data) == -1) {
            return -1;
        }

    }
}

RC Project::getNextTuple(void *data) {
    // here we can use in and cond to do stuff    
    if (dynamic_cast<TableScan*>(in)) {
        if (static_cast<TableScan*>(in)->getNextTuple(data) == -1) {
            return -1;
        }
    } else {
    }
}

// ... the rest of your implementations go here
Aggregate::Aggregate(Iterator *input,          // Iterator of input R
                  Attribute aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        ) {
    setIterator(input);
    setAttribute(aggAttr);
    setOperator(op);
    setValue(0);

    // initialize return attribute
    vector<string> returnAttrs;
    returnAttrs.push_back(aggAttr.name);

    Value v;

    aggAttr.name.erase(0, aggAttr.name.find(".") + 1);
    if (dynamic_cast<TableScan*>(input)) {
        static_cast<TableScan*>(input)->setIterator(NO_OP, "", returnAttrs, v);
    } else {
       // static_cast<IndeScan*>(in)->set

    }
};

RC Aggregate::getNextTuple(void *data) {
    AggregateOp op = getOperator();
    void *buffer = malloc(PAGE_SIZE);
    float value;
    int counter = 0; // used for AVG

    // aggregate values
    switch (op) {
        case MAX:
            while (getIterator()->getNextTuple(buffer) != RM_EOF) {
                int temp;
                memcpy(&temp, buffer, sizeof(float));

                if (!value) {
                    value = temp;
                } else if (temp > value) {
                    value = temp;
                }
            }
            break;
        case MIN:
            while (getIterator()->getNextTuple(buffer) != RM_EOF) {
                int temp;
                memcpy(&temp, buffer, sizeof(float));

                if (!value) {
                    value = temp;
                } else if (temp < value) {
                    value = temp;
                }
            }
            break;
        case SUM:
            while (getIterator()->getNextTuple(buffer) != RM_EOF) {
                int temp;
                memcpy(&temp, buffer, sizeof(float));

                if (!value) {
                    value = temp;
                } else {
                    value += temp;
                }
            }
            break;
        case AVG:
            while (getIterator()->getNextTuple(buffer) != RM_EOF) {
                int temp;
                counter++;
                memcpy(&temp, buffer, sizeof(float));

                if (!value) {
                    value = temp;
                } else {
                    value += temp;
                }

                // TODO: return value / counter
            }
            break;
        case COUNT:
            while (getIterator()->getNextTuple(buffer) != RM_EOF) {
                value++;
            }
            break;
        default:
            return -1;
    }
}

void Aggregate::getAttributes(vector<Attribute> &attrs) const {
    return aggregateIterator->getAttributes(attrs);
}


