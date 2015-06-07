
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) {
    in = input;
    string attr = condition.bRhsIsAttr ? condition.rhsAttr : condition.lhsAttr;
        
    attr.erase(0, attr.find(".") + 1);
    if (dynamic_cast<TableScan*>(in)) {
        static_cast<TableScan*>(in)->setIterator(cond.op, attr, condition.rhsValue);
    } else {
       // static_cast<IndeScan*>(in)->set

    }
}

Project::Project(Iterator* input, const vector<string> &attrNames) {
    in = input;
    attrs = attrNames;
}

// ... the rest of your implementations go here


RC Filter::getNextTuple(void *data) {
    // here we can use in and cond to do stuff    
    if (dynamic_cast<TableScan*>(in)) {
        if (static_cast<TableScan*>(in)->getNextTuple(data) == -1) {
            return -1;
        }
    } else {
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


