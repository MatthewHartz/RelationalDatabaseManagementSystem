
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) {
    in = input;
    filterCondition = condition;
    //string attr = condition.bRhsIsAttr ? condition.rhsAttr : condition.lhsAttr;

    // initializes to return all attributes
    vector<Attribute> attrs;
    in->getAttributes(attrs);

    // Search for left and right condition positions
    for(unsigned i = 0; i < attrs.size(); ++i)
    {
        if (attrs[i].name == condition.lhsAttr) {
            leftConditionAttr = attrs[i];
            leftConditionPos = i;
        }

        if (condition.bRhsIsAttr && attrs[i].name == condition.rhsAttr) {
            rightConditoinAttr = attrs[i];
            rightConditionPos = i;
        }

    }
}

RC Filter::getNextTuple(void *data) {
    // loop until a tuple is found that satisfies the condition
    vector<Attribute> attrs;
    in->getAttributes(attrs);

    // null indicator information
    int attributeCount = attrs.size();
    int indicatorSize = 1 + ((attributeCount - 1) / 8);
    int offset = indicatorSize;

    void* leftValue = malloc(PAGE_SIZE);
    void* rightValue = malloc(PAGE_SIZE);

    while (in->getNextTuple(data) != -1) {
        // search for condition value
        for (int i = 0; i <  attrs.size(); i++) {
            // TODO Handle Nulls
            // we are at the position, test its value
            if (leftConditionPos == i) {
                switch (leftConditionAttr.type) {
                case TypeInt: memcpy((char*)leftValue, (char *) data + offset, sizeof(int));
                    break;
                case TypeReal: memcpy((char*)leftValue, (char *) data + offset, sizeof(float));
                    break;
                case TypeVarChar: memcpy((char*)leftValue, (char *) data + offset, leftConditionAttr.length + sizeof(int));
                    break;
                }

            }

            if (filterCondition.bRhsIsAttr && rightConditionPos == i) {
                switch (rightConditoinAttr.type) {
                case TypeInt: memcpy((char*)rightValue, (char *) data + offset, sizeof(int));
                    break;
                case TypeReal: memcpy((char*)rightValue, (char *) data + offset, sizeof(float));
                    break;
                case TypeVarChar: memcpy((char*)rightValue, (char *) data + offset, rightConditoinAttr.length + sizeof(int));
                    break;
                }
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
                    memcpy(&length, (char*)data + offset, sizeof(int));
                    offset += sizeof(int) + length;
                    break;
                }
        }

        if ((filterCondition.bRhsIsAttr && compareValues(leftValue, rightValue))
            || compareValues(leftValue, filterCondition.rhsValue.data)){
            return 0;
        }

        // reset offset
        offset = indicatorSize;
    }

    // if it reached this point, we have reached the end of the tuples
    return -1;
}

bool Filter::compareValues(void *left, void* right) {
    switch (leftConditionAttr.type) {
    case TypeInt:
        int leftInt;
        memcpy(&leftInt, (char *) left, sizeof(int));

        int rightInt;
        memcpy(&rightInt, (char *) right, sizeof(int));

        switch(filterCondition.op) {
            case EQ_OP:     return leftInt == rightInt;
            case LT_OP:     return leftInt < rightInt;
            case GT_OP:     return leftInt > rightInt;
            case LE_OP:     return leftInt <= rightInt;
            case GE_OP:     return leftInt >= rightInt;
            case NE_OP:     return leftInt != rightInt;
            case NO_OP:     return true;
            default:        return false;
        }
        break;
    case TypeReal:
        float leftReal;
        memcpy(&leftReal, (char *) left, sizeof(float));

        float rightReal;
        memcpy(&rightReal, (char *) right, sizeof(float));

        switch(filterCondition.op) {
            case EQ_OP:     return leftReal == rightReal;
            case LT_OP:     return leftReal < rightReal;
            case GT_OP:     return leftReal > rightReal;
            case LE_OP:     return leftReal <= rightReal;
            case GE_OP:     return leftReal >= rightReal;
            case NE_OP:     return leftReal != rightReal;
            case NO_OP:     return true;
            default:        return false;
        }

        break;
    case TypeVarChar:
        int leftLength;
        memcpy(&leftLength, (char *) left, sizeof(int));
        char* leftVal = new char[leftLength + 1];
        memcpy(leftVal, (char *) left + sizeof(int), leftLength);
        leftVal[leftLength] = '\0';
        string leftVarChar = std::string(leftVal);

        int rightLength;
        memcpy(&rightLength, (char *) right, sizeof(int));
        char* rightVal = new char[rightLength + 1];
        memcpy(rightVal, (char *) right + sizeof(int), rightLength);
        rightVal[rightLength] = '\0';
        string rightVarChar = std::string(rightVal);

        switch(filterCondition.op) {
            case EQ_OP:     return leftVarChar == rightVarChar;
            case LT_OP:     return leftVarChar < rightVarChar;
            case GT_OP:     return leftVarChar > rightVarChar;
            case LE_OP:     return leftVarChar <= rightVarChar;
            case GE_OP:     return leftVarChar >= rightVarChar;
            case NE_OP:     return leftVarChar != rightVarChar;
            case NO_OP:     return true;
            default:        return false;
        }

        break;
    }

    return 0;
}

Project::Project(Iterator* input, const vector<string> &attrNames) {
    setIterator(input);
    setAttributeNames(attrNames);
}

RC Project::getNextTuple(void *data) {
    // Attrs that will be projected
    vector<Attribute> projectAttrs;
    getAttributes(projectAttrs);

    // get tables actual indicator size
    vector<Attribute> tableAttrs;
    getIterator()->getAttributes(tableAttrs);
    int tableIndicatorSize = 1 + ((tableAttrs.size()- 1) / 8);

    int attributeCount = projectAttrs.size();
    vector<string> attrNames = getAttributeNames();

    // create a new null indicator for merged entries
    int indicatorSize = 1 + ((attributeCount - 1) / 8);
    unsigned char *nullsIndicator = (unsigned char *) malloc(indicatorSize);
    memset(nullsIndicator, 0, indicatorSize);

    // iterate over attrs searching for projected attributes and collect attribute
    // and modify nullsIndicator
    int offset = indicatorSize;
    int dataOffset = tableIndicatorSize;

    // read the next tuple
    void *buffer = malloc(PAGE_SIZE);
    if (getIterator()->getNextTuple(buffer) == -1) {
        free(buffer);
        free(nullsIndicator);
        return -1;
    }

    // TODO OPTIMIZE THIS ALGO
    int tableCounter = 0;
    int counter = 0;

    for (Attribute attr: projectAttrs) {
        // test if null
        char attrByte;
        memcpy(&attrByte, (char*) buffer + (tableCounter / 8), sizeof(char));
        char mask = 128 >> (tableCounter % 8);

        // if attribute is null
        if (attrByte & mask > 0) {
            // get the byte that contains the byte for the nullsIndicator position of attr
            char tempByte;
            memcpy(&tempByte, (char*) nullsIndicator + (counter / 8), sizeof(char));

            // Modify that byte
            char tempMask = 128 >> (counter & 8);
            tempByte |= tempMask;

            // write it back to nullsIndicator
            memcpy((char*) nullsIndicator + (counter / 8), &tempByte, sizeof(char));

            counter++;
            tableCounter++;
            continue;
        }

        // otherwise, collect data
        switch(attr.type) {
            case TypeInt: {
                if (std::find(attrNames.begin(), attrNames.end(), attr.name) != attrNames.end()) {
                    // Write data
                    memcpy((char*)data + dataOffset, (char*)buffer + offset, sizeof(int));

                    dataOffset += sizeof(int);
                    counter++;
                }

                offset += sizeof(int);
                break;
            }
            case TypeReal: {
                if (std::find(attrNames.begin(), attrNames.end(), attr.name) != attrNames.end()) {
                    // TODO determine if null

                    // Write data
                    memcpy((char*)data + dataOffset, (char*)buffer + offset, sizeof(float));

                    dataOffset += sizeof(float);
                    counter++;
                }

                offset += sizeof(float);
                break;
            }
            case TypeVarChar: {
                int len;
                memcpy(&len, (char*)data + offset, sizeof(int));
                if (std::find(attrNames.begin(), attrNames.end(), attr.name) != attrNames.end()) {
                    // TODO determine if null

                    // Write data
                    memcpy((char*)data + dataOffset, &len, sizeof(int));
                    dataOffset += sizeof(int);
                    offset += sizeof(int);
                    memcpy((char*)data + dataOffset, (char*)buffer + offset, len);

                    dataOffset += len;
                    counter++;
                }

                offset += len;
                break;
            }
        }

        tableCounter++;
    }

    // write the null indicator
    memcpy((char*) data, (char*) nullsIndicator, indicatorSize);

    // free memory
    free(nullsIndicator);
    free(buffer);
}

Aggregate::Aggregate(Iterator *input,          // Iterator of input R
                  Attribute aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        ) {
    setIterator(input);
    setAggAttribute(aggAttr);
    setOperator(op);
    setValue(0);
};

Aggregate::Aggregate(Iterator *input,          // Iterator of input R
                  Attribute aggAttr,        // The attribute over which we are computing an aggregate
                  Attribute groupAttr,
                  AggregateOp op            // Aggregate operation
        ) {
    setIterator(input);
    setAggAttribute(aggAttr);
    setGroupAttribute(groupAttr);
    setOperator(op);
    setValue(0);
    setIsGroupBy();
    groupPosition = 0;
};

// Group based aggregation can be assumed to fit on disk, no need to worry about storing partitions
RC Aggregate::getNextTuple(void *data) {
    void *buffer = malloc(PAGE_SIZE);
    int counter = 0; // used for AVG
    float aggregateValue = 0;
    int intTemp;
    float floatTemp;
    bool aggregateFound = false;

    // aggregate values
    vector<Attribute> attrs;
    getAttributes(attrs);
    // null indicator information
    int attributeCount = attrs.size();
    int indicatorSize = 1 + ((attributeCount - 1) / 8);
    int offset = indicatorSize;
    // loop over all tuples and aggregate the aggregate operator

    switch (getOperator()) {
        case MAX:
            while (getIterator()->getNextTuple(buffer) != RM_EOF) {
                aggregateFound = true;
                // iterate over the attributes and find the aggregate attribute
                for (Attribute attr: attrs) {
                    if (attr.name == getAggAttribute().name) {
                        switch (attr.type) {
                            case TypeInt:
                                memcpy(&intTemp, buffer + offset, sizeof(int));
                                if (intTemp > aggregateValue) {
                                    aggregateValue = (float)intTemp;
                                }
                                break;
                            case TypeReal:
                                memcpy(&floatTemp, buffer + offset, sizeof(float));
                                if (floatTemp > aggregateValue) {
                                    aggregateValue = floatTemp;
                                }
                                break;
                        }
                    }
                    switch (attr.type) {
                        case TypeInt: offset += sizeof(int);
                            break;
                        case TypeReal: offset += sizeof(float);
                            break;
                        case TypeVarChar:
                            int len;
                            memcpy(&len, (char*)buffer + offset, sizeof(int));
                            offset += sizeof(int) + len;
                            break;
                        }
                }

                // reset offset
                offset = indicatorSize;
            }
            break;
            case MIN:
                while (getIterator()->getNextTuple(buffer) != RM_EOF) {
                    aggregateFound = true;
                    // iterate over the attributes and find the aggregate attribute
                    for (Attribute attr: attrs) {
                        if (attr.name == getAggAttribute().name) {
                            switch (attr.type) {
                                case TypeInt:
                                    memcpy(&intTemp, buffer + offset, sizeof(int));
                                    if (intTemp < aggregateValue) {
                                        aggregateValue = (float)intTemp;
                                    }
                                break;
                                case TypeReal:
                                    memcpy(&floatTemp, buffer + offset, sizeof(float));
                                    if (floatTemp < aggregateValue) {
                                        aggregateValue = floatTemp;
                                    }
                                break;
                            }
                        }
                        switch (attr.type) {
                            case TypeInt: offset += sizeof(int);
                                break;
                            case TypeReal: offset += sizeof(float);
                                break;
                            case TypeVarChar:
                                int len;
                                memcpy(&len, (char*)buffer + offset, sizeof(int));
                                offset += sizeof(int) + len;
                                break;
                        }
                    }
                    // reset offset
                    offset = indicatorSize;
                }
                break;
            case SUM:
                while (getIterator()->getNextTuple(buffer) != RM_EOF) {
                    aggregateFound = true;
                    // iterate over the attributes and find the aggregate attribute
                    for (Attribute attr: attrs) {
                        if (attr.name == getAggAttribute().name) {
                            switch (attr.type) {
                                case TypeInt: memcpy(&intTemp, buffer + offset, sizeof(int));
                                    aggregateValue += (float)intTemp;
                                    break;
                                case TypeReal: memcpy(&floatTemp, buffer + offset, sizeof(float));
                                    aggregateValue += floatTemp;
                                    break;
                            }
                        }
                        switch (attr.type) {
                            case TypeInt: offset += sizeof(int);
                                break;
                            case TypeReal: offset += sizeof(float);
                                break;
                            case TypeVarChar:
                                int len;
                                memcpy(&len, (char*)buffer + offset, sizeof(int));
                                offset += sizeof(int) + len;
                                break;
                            }
                    }
                    // reset offset
                    offset = indicatorSize;
                }
                break;
            case AVG:
                while (getIterator()->getNextTuple(buffer) != RM_EOF) {
                    aggregateFound = true;
                    // iterate over the attributes and find the aggregate attribute
                    for (Attribute attr: attrs) {
                        if (attr.name == getAggAttribute().name) {
                            switch (attr.type) {
                                case TypeInt: memcpy(&intTemp, buffer + offset, sizeof(int));
                                    aggregateValue += (float)intTemp;
                                    break;
                                case TypeReal: memcpy(&floatTemp, buffer + offset, sizeof(float));
                                    aggregateValue += floatTemp;
                                    break;
                            }
                        }
                        switch (attr.type) {
                            case TypeInt: offset += sizeof(int);
                                break;
                            case TypeReal: offset += sizeof(float);
                                break;
                            case TypeVarChar:
                                int len;
                                memcpy(&len, (char*)buffer + offset, sizeof(int));
                                offset += sizeof(int) + len;
                                break;
                        }
                    }
                    // reset offset
                    offset = indicatorSize;
                    counter++;
                }
                break;
            case COUNT:
                while (getIterator()->getNextTuple(buffer) != RM_EOF) {
                    aggregateValue++;
                }
                break;
                default:
                    return -1;
        }
        if (!aggregateFound) {
            return -1;
        }

        // set aggregate value for data
        if (getOperator() == AVG) aggregateValue = aggregateValue / (float) counter;
            memcpy((char*)data + 1, &aggregateValue, sizeof(float));

    return 0;
}

void Aggregate::getAttributes(vector<Attribute> &attrs) const {
    return aggregateIterator->getAttributes(attrs);
}

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
    // TODO We may need to delete buffers in map on clear
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
                    vector<Attribute> attrs;
                    getLeftIterator()->getAttributes(attrs);

                    // adjust for nullindicator size using ceiling function
                    offset = 1 + ((attrs.size() - 1) / 8);

                    // malloc the new buffer
                    void *buf = malloc(PAGE_SIZE);

                    for (int i = 0; i < attrs.size(); i++) {
                        if (!RecordBasedFileManager::isFieldNull(buffer, i)) {
                            if (attrs[i].name == getLeftJoinAttribute().name) {
                                memcpy(&returnReal, (char*)buffer + offset, sizeof(int));
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

                    auto hashVal = realHashMap.find(returnReal);
                    realMapEntry entry = { returnReal, entryBuffer, bufferSize };

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
                    vector<Attribute> attrs;
                    getLeftIterator()->getAttributes(attrs);

                    // adjust for nullindicator size using ceiling function
                    offset = 1 + ((attrs.size() - 1) / 8);

                    // malloc the new buffer
                    void *buf = malloc(PAGE_SIZE);

                    for (int i = 0; i < attrs.size(); i++) {
                        if (!RecordBasedFileManager::isFieldNull(buffer, i)) {
                            if (attrs[i].name == getLeftJoinAttribute().name) {
                                int length;
                                int loffset = offset;
                                memcpy(&length, (char*)buffer + loffset, sizeof(int));
                                loffset += sizeof(int);
                                char* value = new char[length + 1];
                                memcpy(value, (char*)buffer + loffset, length);
                                loffset += length;
                                value[length] = '\0';
                                returnVarChar = std::string(value);

                                memcpy(&returnVarChar, (char*)buffer + offset, sizeof(int));
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

                    auto hashVal = varCharHashMap.find(returnVarChar);
                    varCharMapEntry entry = { returnVarChar, entryBuffer, bufferSize };

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
                auto hashVal = intHashMap.find(returnInt);

                // hash has been found
                if (!(hashVal == intHashMap.end())) {
                    // iterate over list and attempt to find key
                    for (intMapEntry entry : hashVal->second) {
                        if (entry.attr == returnInt) {
                            joinBufferData(entry.buffer, entry.size, getLeftNumAttrs(), rightBuffer
                                    , bufferSize, getRightNumAttrs(), data);
                            free(rightBuffer);
                            return 0;
                        }
                    }
                }
                break;
            }
            case TypeReal: {
                float returnReal;
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
                            memcpy(&returnReal, (char*)rightBuffer + offset, sizeof(float));
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
                auto hashVal = realHashMap.find(returnReal);

                // hash has been found
                if (!(hashVal == realHashMap.end())) {
                    // iterate over list and attempt to find key
                    for (realMapEntry entry : hashVal->second) {
                        if (entry.attr == returnReal) {
                            joinBufferData(entry.buffer, entry.size, getLeftNumAttrs(), rightBuffer
                                    , bufferSize, getRightNumAttrs(), data);
                            free(rightBuffer);
                            return 0;
                        }
                    }
                }
                break;
            }
            case TypeVarChar: {
                string returnVarChar;
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
                            int length;
                            int loffset = offset;
                            memcpy(&length, (char*)rightBuffer + loffset, sizeof(int));
                            loffset += sizeof(int);
                            char* value = new char[length + 1];
                            memcpy(value, (char*)rightBuffer + loffset, length);
                            loffset += length;
                            value[length] = '\0';
                            returnVarChar = std::string(value);

                            memcpy(&returnVarChar, (char*)rightBuffer + offset, length);
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
                auto hashVal = varCharHashMap.find(returnVarChar);

                // hash has been found
                if (!(hashVal == varCharHashMap.end())) {
                    // iterate over list and attempt to find key
                    for (varCharMapEntry entry : hashVal->second) {
                        if (entry.attr == returnVarChar) {
                            joinBufferData(entry.buffer, entry.size, getLeftNumAttrs(), rightBuffer
                                    , bufferSize, getRightNumAttrs(), data);
                            free(rightBuffer);
                            return 0;
                        }
                    }
                }
                break;
            }
        }
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

void BNLJoin::getAttributes(vector<Attribute> &attrs) const {
    vector<Attribute> leftAttrs;
    getLeftIterator()->getAttributes(leftAttrs);
    vector<Attribute> rightAttrs;
    getRightIterator()->getAttributes(rightAttrs);

    attrs.reserve( leftAttrs.size() + rightAttrs.size());
    attrs.insert( attrs.end(), leftAttrs.begin(), leftAttrs.end());
    attrs.insert( attrs.end(), rightAttrs.begin(), rightAttrs.end());
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

INLJoin::INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition) {  // Join condition
    setLeftIterator(leftIn);
    setRightIterator(rightIn);

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

// TODO Set nulls in null indicator
RC INLJoin::getNextTuple(void *data) {
    void *leftBuffer = malloc(PAGE_SIZE);
    void *rightBuffer = malloc(PAGE_SIZE);
    int leftBufferSize = 0;
    int rightBufferSize = 0;
    vector<Attribute> leftAttrs;
    vector<Attribute> rightAttrs;

    // set the vectors
    getLeftIterator()->getAttributes(leftAttrs);
    getRightIterator()->getAttributes(rightAttrs);

    int offset = 0;
    void *joinValue;

    // loop over left join attribute
    while (getLeftIterator()->getNextTuple(leftBuffer) != -1) {
        // adjust for nullindicator size using ceiling function
        offset = 1 + ((leftAttrs.size() - 1) / 8);

        // collect left join value
        for (unsigned i = 0; i < leftAttrs.size(); i++) {
            if (!RecordBasedFileManager::isFieldNull(leftBuffer, i)) {
                if (leftAttrs[i].name == getLeftJoinAttribute().name) {
                    switch (getLeftJoinAttribute().type) {
                    case TypeInt:
                        joinValue = malloc(sizeof(float));
                        memcpy((char*)joinValue, (char*)leftBuffer + offset, sizeof(int));
                        break;
                    case TypeReal:
                        joinValue = malloc(sizeof(float));
                        memcpy((char*)joinValue, (char*)leftBuffer + offset, sizeof(float));
                        break;
                    case TypeVarChar:
                        int length;
                        memcpy(&length, (char*)leftBuffer + offset, sizeof(int));
                        memcpy((char*)joinValue, (char*)leftBuffer + offset, length + sizeof(int));
                        break;
                    }
                }

                // skip over the attribute
                switch (leftAttrs[i].type) {
                case TypeInt:
                    offset += sizeof(int);
                    break;
                case TypeReal:
                    offset += sizeof(float);
                    break;
                case TypeVarChar:
                    int length;
                    memcpy(&length, (char*)leftBuffer + offset, sizeof(int));
                    offset += sizeof(int) + length;
                    break;
                }
            }
        }

        // set left buffer size and reset offset
        leftBufferSize = offset;
        offset = 0;

        if (joinValue != NULL) {
            // adjust for nullindicator size using ceiling function
            offset = 1 + ((rightAttrs.size() - 1) / 8);

            // set the indexScan to iterate where key value == joinValue
            getRightIterator()->setIterator(joinValue, joinValue, true, true);

            // search for tuple
            while (getRightIterator()->getNextTuple(rightBuffer) != -1) {
                // tuple found, now determine size of buffer
                for (unsigned i = 0; i < rightAttrs.size(); i++) {
                    if (!RecordBasedFileManager::isFieldNull(rightBuffer, i)) {
                        // skip over the attribute
                        switch (rightAttrs[i].type) {
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

                // set right buffer size and reset offset
                rightBufferSize = offset;
                offset = 0;

                joinBufferData(leftBuffer, leftBufferSize, leftAttrs.size(), rightBuffer
                        , rightBufferSize, rightAttrs.size(), data);

                // free memory
                free(leftBuffer);
                free(rightBuffer);
                free(joinValue);

                return 0;
            }
        }

        // reset all sizes and offset
        leftBufferSize = 0;
        rightBufferSize = 0;
        offset = 0;
    }

    // joined tuple was not found
    free(leftBuffer);
    free(rightBuffer);
    return -1;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const {
    vector<Attribute> leftAttrs;
    getLeftIterator()->getAttributes(leftAttrs);
    vector<Attribute> rightAttrs;
    getRightIterator()->getAttributes(rightAttrs);

    attrs.reserve( leftAttrs.size() + rightAttrs.size());
    attrs.insert( attrs.end(), leftAttrs.begin(), leftAttrs.end());
    attrs.insert( attrs.end(), rightAttrs.begin(), rightAttrs.end());
}

RC joinBufferData(void *buffer1, int buffer1Len, int numAttrs1, void* buffer2, int buffer2Len, int numAttrs2, void* data) {
    // create a new null indicator for merged entries
    int leftIndicatorSize = 1 + (( numAttrs1 - 1) / 8);
    int rightIndicatorSize = 1 + (( numAttrs2 - 1) / 8);
    int indicatorSize = 1 + (((numAttrs1 + numAttrs2) - 1) / 8);

    unsigned char *nullsIndicator = (unsigned char *) malloc(indicatorSize);
    memset(nullsIndicator, 0, indicatorSize);

    // initialize the indicator
    // for left indicator, just copy all the bytes straight over.
    memcpy((char*)nullsIndicator, buffer1, leftIndicatorSize);

    // iterate over all bits for rightAttrs, then merge them into nullsIndicator
    int position = numAttrs1;
    for (int i = 0; i < numAttrs2; i++) {
        // read the fragmented byte
        char currentByte;
        memcpy(&currentByte, nullsIndicator + (leftIndicatorSize - 1), sizeof(char));

        // add in the remaining bits using the right indicator
        char rightByte;
        for (int j = position % 8; j < 8 && i < numAttrs2; i++, j++, position++) {
            //char mask = 128 >> j;
            //currentByte |= mask;

            // get bit of right buffer at position i
            memcpy(&rightByte, (char *) buffer2 + (i / 8), sizeof(char));
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
    memcpy((char*)data + offset, (char*)buffer1 + leftIndicatorSize, buffer1Len - leftIndicatorSize);
    offset += buffer1Len - leftIndicatorSize;
    memcpy((char*)data + offset, (char*)buffer2 + rightIndicatorSize, buffer2Len - rightIndicatorSize);

    free(nullsIndicator);
    return 0;
}
