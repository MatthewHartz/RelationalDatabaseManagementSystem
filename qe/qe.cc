
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
        if (!condition.bRhsIsAttr && (condition.op == GE_OP || condition.op == LE_OP)) {
            static_cast<IndexScan*>(in)->setIterator(condition.rhsValue.data, NULL, true, false);
        } else if(condition.bRhsIsAttr && (condition.op == GE_OP || condition.op == LE_OP)) {
            // TODO not sure what to do here
        }
    }
}

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
RC Project::getNextTuple(void *data) {
    // here we can use in and cond to do stuff    
    if (dynamic_cast<TableScan*>(in)) {
        if (static_cast<TableScan*>(in)->getNextTuple(data) == -1) {
            return -1;
        }
    } else {
    }
}

/*
 * Aggregate section
 */
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

/*
 * Block Nested Loop Join section
 */
BNLJoin::BNLJoin(Iterator *leftIn,            // Iterator of input R
        TableScan *rightIn,           // TableScan Iterator of input S
        const Condition &condition,   // Join condition
        const unsigned numRecords     // # of records can be loaded into memory, i.e., memory block size (decided by the optimizer)
         ) {
    setLeftIterator(leftIn);
    setRightIterator(rightIn);
    setNumRecords(numRecords);

    string attr = condition.bRhsIsAttr ? condition.rhsAttr : condition.lhsAttr;

    // initializes all the left return attributes
    vector<Attribute> leftAttrs;
    getLeftIterator()->getAttributes(leftAttrs);

    // initializes all the right return attributes
    vector<Attribute> rightAttrs;
    getRightIterator()->getAttributes(rightAttrs);

    // get the left join attribute
    for (int i = 0; i < leftAttrs.size(); i++) {
        if (leftAttrs[i].name == condition.lhsAttr) {
            setLeftJoinAttribute(leftAttrs[i]);
        }
    }

    // get the right join attribute
    for (int i = 0; i < rightAttrs.size(); i++) {
        if (rightAttrs[i].name == condition.rhsAttr) {
            setRightJoinAttribute(rightAttrs[i]);
        }
    }

    // set attrs lengths
    setLeftNumAttrs(leftAttrs.size());
    setRightNumAttrs(rightAttrs.size());
}

RC BNLJoin::getNextTuple(void *data) {
    int counter = 0;

    // Update the map
    if (innerFinished) {
        innerFinished = false;
        bool reachedEnd = false;
        int numRecords = getNumRecords();
        void *buffer = malloc(PAGE_SIZE);

        switch (getLeftJoinAttribute().type) {
            case TypeInt:
                while (getLeftIterator()->getNextTuple(buffer) != -1) {
                    int returnInt;
                    int bufferSize;
                    int offset = 0;

                    // iterate over buffer, collect the correct attribute, then finish iterating
                    // and store the buffer size
                    vector<Attribute> attrs;
                    getLeftIterator()->getAttributes(attrs);

                    // adjust for nullindicator size using ceiling function
                    offset = 1 + ((attrs.size() - 1) / 8);

                    // malloc the new buffer
                    void *buf = malloc(PAGE_SIZE);

                    for (int i = 0; i < attrs.size(); i++) {
                        if (!RecordBasedFileManager::isFieldNull(buffer, i)) {
                            if (attrs[i].name == getLeftJoinAttribute().name) {
                                memcpy(&returnInt, (char*)buffer + offset, sizeof(int));
                            }

                            // skip over the attribute
                            switch (attrs[i].type) {
                            case TypeInt:
                                offset += sizeof(int);
                                break;
                            case TypeReal:
                                offset += sizeof(float);
                                break;
                            case TypeVarChar:
                                int length;
                                memcpy(&length, (char*)buffer + offset, sizeof(int));
                                offset += sizeof(int) + length;
                                break;
                            }
                        }
                    }

                    // save bufferSize
                    bufferSize = offset;
                    void *entryBuffer = malloc(bufferSize);
                    memcpy(entryBuffer, buffer, bufferSize);
                    // store into map
                    //int hash = intHashFunction(returnInt, numRecords);

                    auto hashVal = intHashMap.find(returnInt);
                    intMapEntry entry = { returnInt, entryBuffer, bufferSize };

                    // check and see if the tableID already exists in the map
                    if (hashVal == intHashMap.end()) {
                        vector<intMapEntry> v { entry };
                        intHashMap[returnInt] = v;
                    } else {
                        hashVal->second.push_back(entry);
                    }

                    counter++;

                    if (counter >= numRecords) {
                        break;
                    }
                }
                break;
            case TypeReal:
                while (getLeftIterator()->getNextTuple(buffer) != -1) {
                    float returnReal;
                    int bufferSize;
                    int offset = 0;

                    // iterate over buffer, collect the correct attribute, then finish iterating
                    // and store the buffer size
                    memcpy(&returnReal, (char*)buffer + 1, sizeof(float));

                    // store into map
                    //int hash = realHashFunction(returnReal, numRecords);

                    auto hashVal = realHashMap.find(returnReal);
                    realMapEntry entry = { returnReal, buffer, bufferSize };

                    // check and see if the tableID already exists in the map
                    if (hashVal == realHashMap.end()) {
                        vector<realMapEntry> v { entry };
                        realHashMap[returnReal] = v;
                    } else {
                        hashVal->second.push_back(entry);
                    }

                    counter++;

                    if (counter >= numRecords) {
                        break;
                    }
                }
                break;
            case TypeVarChar:
                while (getLeftIterator()->getNextTuple(buffer) != -1) {
                    string returnVarChar;
                    int bufferSize;
                    int offset = 0;

                    // iterate over buffer, collect the correct attribute, then finish iterating
                    // and store the buffer size
                    int length;
                    int offset = 1;
                    memcpy(&length, (char*)buffer + offset, sizeof(int));
                    offset += sizeof(int);
                    char* value = new char[length + 1];
                    memcpy(value, (char*)buffer + offset, length);
                    offset += length;
                    value[length] = '\0';
                    returnVarChar = std::string(value);

                    // store into map
                    //int hash = varCharHashFunction(returnVarChar, numRecords);

                    auto hashVal = varCharHashMap.find(returnVarChar);
                    varCharMapEntry entry = { returnVarChar, buffer, bufferSize };

                    // check and see if the tableID already exists in the map
                    if (hashVal == varCharHashMap.end()) {
                        vector<varCharMapEntry> v { entry };
                        varCharHashMap[returnVarChar] = v;
                    } else {
                        hashVal->second.push_back(entry);
                    }

                    counter++;

                    if (counter >= numRecords) {
                        break;
                    }
                }
                break;
            default:
                return -1;
        }

        free(buffer);

        // No more records left in left table
        if (counter == 0) {
            return -1;
        }
    }

    // Iterate over the right table and return the first tuple that exists in the memory
    void *rightBuffer = malloc(PAGE_SIZE);
    while (getRightIterator()->getNextTuple(rightBuffer) != -1) {
        switch (getRightJoinAttribute().type) {
        case TypeInt: {
            int returnInt;
            int bufferSize;
            int offset = 0;

            // iterate over buffer, collect the correct attribute, then finish iterating
            // and store the buffer size
            vector<Attribute> attrs;
            getRightIterator()->getAttributes(attrs);

            // adjust for nullindicator size using ceiling function
            offset = 1 + ((attrs.size() - 1) / 8);

            for (int i = 0; i < attrs.size(); i++) {
                if (!RecordBasedFileManager::isFieldNull(rightBuffer, i)) {
                    if (attrs[i].name == getRightJoinAttribute().name) {
                        memcpy(&returnInt, (char*)rightBuffer + offset, sizeof(int));
                    }

                    // skip over the attribute
                    switch (attrs[i].type) {
                    case TypeInt:
                        offset += sizeof(int);
                        break;
                    case TypeReal:
                        offset += sizeof(float);
                        break;
                    case TypeVarChar:
                        int length;
                        memcpy(&length, (char*)rightBuffer + offset, sizeof(int));
                        offset += sizeof(int) + length;
                        break;
                    }
                }
            }

            // save bufferSize
            bufferSize = offset;

            // test to see if the attribute exists in the map
            int mapSize = intHashMap.size();

            auto hashVal = intHashMap.find(returnInt);

            // hash has been found
            if (!(hashVal == intHashMap.end())) {
                // iterate over list and attempt to find key
                for (intMapEntry entry : hashVal->second) {
                    if (entry.attr == returnInt) {
                        // create a new null indicator for merged entries
                        int leftAttrsCount = getLeftNumAttrs();
                        int rightAttrsCount = getRightNumAttrs();
                        int leftIndicatorSize = 1 + (( leftAttrsCount - 1) / 8);
                        int rightIndicatorSize = 1 + (( rightAttrsCount - 1) / 8);
                        int indicatorSize = 1 + (((leftAttrsCount + rightAttrsCount) - 1) / 8);

                        unsigned char *nullsIndicator = (unsigned char *) malloc(indicatorSize);
                        memset(nullsIndicator, 0, indicatorSize);

                        // initialize the indicator
                        // for left indicator, just copy all the bytes straight over.
                        memcpy((char*)nullsIndicator, entry.buffer, leftIndicatorSize);

                        // iterate over all bits for rightAttrs, then merge them into nullsIndicator
                        int position = leftAttrsCount;
                        for (int i = 0; i < rightAttrsCount; i++) {
                            // read the fragmented byte
                            char currentByte;
                            memcpy(&currentByte, nullsIndicator + (leftIndicatorSize - 1), sizeof(char));

                            // add in the remaining bits using the right indicator
                            char rightByte;
                            for (int j = position % 8; j < 8 && i < rightAttrsCount; i++, j++, position++) {
                                //char mask = 128 >> j;
                                //currentByte |= mask;

                                // get bit of right buffer at position i
                                memcpy(&rightByte, rightBuffer + (i / 8), sizeof(char));
                                char mask = 128 >> i % 8;

                                // clear other bits that we don't want
                                char temp = rightByte & mask > 0 ? 128 : 0;

                                // merge rightByte and currentByte
                                temp >>= j;
                                currentByte |= temp;
                            }

                            // copy back into nulls Indicator
                            memcpy((char*)nullsIndicator + (position / 8), &currentByte, sizeof(char));
                        }

                        // return combine null indicator with 2 buffers (minus their seperate indicators)
                        int offset = 0;
                        memcpy((char*)data + offset, nullsIndicator, indicatorSize);
                        offset += indicatorSize;
                        memcpy((char*)data + offset, (char*)entry.buffer + leftIndicatorSize, entry.size - leftIndicatorSize);
                        offset += entry.size - leftIndicatorSize;
                        memcpy((char*)data + offset, (char*)rightBuffer + rightIndicatorSize, bufferSize - rightIndicatorSize);

                        free(rightBuffer);
                        return 0;
                    }
                }
            }
            break;
        }
        case TypeReal:
            break;
        case TypeVarChar:
            break;
        }


        // get the attribute value
    }

    free(rightBuffer);

    // if we reached this point, then we need to refresh the memory and start again
    innerFinished = true;
    intHashMap.clear();
    realHashMap.clear();
    varCharHashMap.clear();

    // restart the right iterator
    TableScan *tc = new TableScan(getRightIterator()->rm, getRightIterator()->tableName);

    // TODO Does this need to be deleted?
    getRightIterator()->~TableScan();
    setRightIterator(tc);

    return getNextTuple(data);
}

int BNLJoin::intHashFunction(int data, int numRecords) {
    return data % numRecords;
}

int BNLJoin::realHashFunction(float data, int numRecords) {
    unsigned int ui;
    memcpy(&ui, &data, sizeof(float));
    return ui & 0xfffff000;
}

int BNLJoin::varCharHashFunction(string data, int numRecords) {
    int length = data.size();
    char c[length + 1];
    strcpy(c, data.c_str());


    int sum = 0;
    for (int i = 0; i < length; i++) {
        sum += c[i];
    }

    return sum % numRecords;
}






