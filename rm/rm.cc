
#include "rm.h"

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
    if (!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
    rbfm = RecordBasedFileManager::instance();
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
    const string tablesName = "Tables";
    const string columnsName = "Columns";

    if (rbfm->createFile(tablesName) == -1 || rbfm->createFile(columnsName) == -1) return -1;

    //Initialize descriptors
    vector<Attribute> columnsDesc;
    vector<Attribute> tablesDesc;

    //Create record descriptor for columns
    addAttributeToDesc("table-id", TypeInt, (AttrLength)4, columnsDesc);
    addAttributeToDesc("column-name", TypeVarChar, (AttrLength)50, columnsDesc);
    addAttributeToDesc("column-type", TypeInt, (AttrLength)4, columnsDesc);
    addAttributeToDesc("column-length", TypeInt, (AttrLength)4, columnsDesc);
    addAttributeToDesc("column-position", TypeInt, (AttrLength)4, columnsDesc);

    // Create record descriptor for tables
    addAttributeToDesc("table-id", TypeInt, (AttrLength)4, tablesDesc);
    addAttributeToDesc("table-name", TypeVarChar, (AttrLength)50, tablesDesc);
    addAttributeToDesc("file-name", TypeVarChar, (AttrLength)50, tablesDesc);

    setTablesDesc(tablesDesc);
    setColumnsDesc(columnsDesc);

    // Creates system tables, if error occurs in either createTable call, returns -1
    if (createTable("Tables", tablesDesc) == -1 || createTable("Columns", columnsDesc) == -1) return -1;

    return 0;
}

RC RelationManager::deleteCatalog()
{
    columnsDescriptor.clear();
    tablesDescriptor.clear();

    // return -1 if error on destroy file
    if (rbfm->destroyFile("Tables") == -1 || rbfm->destroyFile("Columns") == -1) return -1;

    return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
    // Create the new table file
    rbfm->createFile(tableName);

    // Initialize template variables
    int maxTableId = 0;
    RID rid;
    void* buffer = malloc(120);
    vector<string> attributes;
    attributes.push_back("table-id");

    // search for max table id
    RM_ScanIterator rmsi;
    RC rc = RelationManager::scan("Tables", "table-id", NO_OP, NULL, attributes, rmsi);

    if (rc != -1) {
        while (rmsi.getNextTuple(rid, buffer) != RM_EOF){
            memcpy(&maxTableId, (int*)buffer + 1, sizeof(int));
        }
        rmsi.close();
    }

    // Open the "tables" file
    FileHandle tablesHandle;
    if (rbfm->openFile("Tables", tablesHandle) == -1) {
        return -1;
    }

    // Add table desc to tables
    prepareTablesRecord(maxTableId + 1, tableName, tableName, buffer);
    rbfm->insertRecord(tablesHandle, this->getTablesDesc(), buffer, rid);

    // Close tables file
    rbfm->closeFile(tablesHandle);

    // Open "columns" file
    FileHandle columnsHandle;
    if (rbfm->openFile("Columns", columnsHandle) == -1) {
        return -1;
    }

    // Loop through attrs and iterative insert each row into the columns table
    for (int i = 0; i < attrs.size(); i++) {
        prepareColumnsRecord(maxTableId + 1, attrs[i].name, attrs[i].type, attrs[i].length, i + 1, buffer);
        rbfm->insertRecord(columnsHandle, this->getColumnsDesc(), buffer, rid);
    }

    // CLose the columns file
    rbfm->closeFile(columnsHandle);

    free(buffer);
    return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
    int tableId;

    // Get table id
    if (getTableIdByName(tableName, tableId) == -1) return -1;

    // Remove from tables
    // Open the "tables" file
    FileHandle tablesHandle;
    if (rbfm->openFile("Tables", tablesHandle) == -1) {
        return -1;
    }

    RID rid;
    void* buffer = malloc(120);

    // Get number of records currently in the tables table
    tablesHandle.infile->seekg(0, ios::end);
    int length = tablesHandle.infile->tellg();

    int numPages = length / PAGE_SIZE;
    void* record = malloc(120); // will be used to store each record

    // search for the record with the correct ID then remove it
    for (int i = 0; i < numPages; i++) {
        void* pageData = malloc(PAGE_SIZE);
        tablesHandle.readPage(i, pageData);

        int numRecordsOnPage;
        memcpy(&numRecordsOnPage, (char*)pageData + N_OFFSET, sizeof(int));

        // Iterate over all the records on page looking for the record that matches the table name
        int recordNumber = 0;
        int slotNumber = 1;
        while (recordNumber < numRecordsOnPage) {
            //int slotLength;
            int slotOffset;
            int slotPosition = N_OFFSET - (slotNumber * (sizeof(int)* 2));

            memcpy(&slotOffset, (char*)pageData + slotPosition, sizeof(int));

            // record exists in this slot
            if (slotOffset >= 0) {
                rid.pageNum = i;
                rid.slotNum = slotNumber - 1;

                rbfm->readRecord(tablesHandle, getTablesDesc(), rid, record);

                int id;
                memcpy(&id, (char*)record + 1, sizeof(int)); // we know that the table-id immediately follows the null indicator

                if (id == tableId) {
                    rbfm->deleteRecord(tablesHandle, getTablesDesc(), rid);
                }

                recordNumber++;
            }

            // move to next slot
            slotNumber++;
        }
    }

    // TODO
    // Use table id from tables to delete from columns
    // Destroy file

    return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    // Open the file related to tableName
    FileHandle handle;
    if (rbfm->openFile(tableName, handle) == -1) {
        return -1;
    }

    // Get the descriptor from the tables table
    vector<Attribute> descriptor;
    if (getAttributes(tableName, descriptor) == -1) return -1;

    // Insert data
    if (rbfm->insertRecord(handle, descriptor, data, rid) == -1) return -1;

    if (rbfm->closeFile(handle) == -1) return -1;

    return 0;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    // Open the file related to tableName
    FileHandle handle;
    if (rbfm->openFile(tableName, handle) == -1) {
        return -1;
    }

    // Get the descriptor from the tables table
    vector<Attribute> descriptor;
    if (getAttributes(tableName, descriptor) == -1) return -1;

    // Insert data
    if (rbfm->deleteRecord(handle, descriptor, rid) == -1) return -1;

    if (rbfm->closeFile(handle) == -1) return -1;

    return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    // Open the file related to tableName
    FileHandle handle;
    if (rbfm->openFile(tableName, handle) == -1) {
        return -1;
    }

    // Get the descriptor from the tables table
    vector<Attribute> descriptor;
    if (getAttributes(tableName, descriptor) == -1) return -1;

    if (rbfm->updateRecord(handle, descriptor, data, rid) == -1) return -1;

    if (rbfm->closeFile(handle) == -1) return -1;

    return 0;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    // Open the file related to tableName
    FileHandle handle;
    if (rbfm->openFile(tableName, handle) == -1) {
        return -1;
    }

    // Get the descriptor from the tables table
    vector<Attribute> descriptor;
    if (getAttributes(tableName, descriptor) == -1) return -1;

    if (rbfm->readRecord(handle, descriptor, rid, data) == -1) return -1;

    if (rbfm->closeFile(handle) == -1) return -1;

    return 0;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    return -1;
}

// attributeNames are the names of the columns for the select statement
RC RelationManager::scan(const string &tableName,
    const string &conditionAttribute,
    const CompOp compOp,
    const void *value,
    const vector<string> &attributeNames,
    RM_ScanIterator &rm_ScanIterator)
{
    // I NEED TO SAVE IT A HANDLE, DESC, CondAttr, CompOP, value, AttrNames, rbfm
    // Initialize RBFM iterator and file Handle
    RBFM_ScanIterator rbfmsi;
    FileHandle handle;
    RID rid;
    void* data = malloc(PAGE_SIZE);

    // Open "tables" file
    vector<string> names;
    names.push_back("table-id");
    if (rbfm->openFile("Tables", handle) == -1) {
        return -1;
    }

    // Initialize the condition attribute value to be compared to
    int varLength = tableName.length();
    value = malloc(sizeof(int) + varLength);
    memcpy((char *) value, &varLength, sizeof(int)); 
    memcpy((char *) value + sizeof(int), tableName.c_str(), varLength);

    // Initialize RBFMSI to scan through table's records looking for "Columns" and extract id
    if (rbfm->scan(handle, getTablesDesc(), "table-name", EQ_OP, value, names, rbfmsi)
        == -1) {
        rbfm->closeFile(handle);
        return RM_EOF;
    }


    int tableId;

    // Get the first record where table-name matches tableName
    if (rbfmsi.getNextRecord(rid, data) != RBFM_EOF) {
        memcpy(&tableId, (char *) data + 1, sizeof(int));
    }

    // close respective objects
    rbfm->closeFile(handle);
    rbfmsi.close();

    // Open the "Columns" file
    if (rbfm->openFile("Columns", handle) == -1) {
        return -1;
    }
    
    // Initialize a vector of all the attributes i want to extract
    names.clear();
    names.push_back("column-name");
    names.push_back("column-type");
    names.push_back("column-length");

    // Initialize the value to table-id
    varLength = sizeof(tableId);
    value = malloc(varLength);
    memcpy((char *) value, &tableId, sizeof(int));

    // Scan over each row of Columns, looking for where table-id == TableID
    if (rbfm->scan(handle, getColumnsDesc(), "table-id", EQ_OP, value, names, rbfmsi)
        == -1) {
        rbfm->closeFile(handle);
        return RM_EOF;
    }

    // Get the first record where table-name matches tableName
    if (rbfmsi.getNextRecord(rid, data) != RBFM_EOF) {
        //memcpy(&tableId, (char *) data + 1, sizeof(int));
    	//TODO: Collect each column and initialize a descriptor.
    }

    rbfm->closeFile(handle);

    return 0;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Helper function used to add an attribute to descriptor.
void addAttributeToDesc(string name, AttrType type, AttrLength length, vector<Attribute> &descriptor) {
    Attribute attr;
    attr.name = name;
    attr.type = type;
    attr.length = length;
    descriptor.push_back(attr);
}

void prepareTablesRecord(const int id, const string &table, const string &file, void *buffer) {
    int offset = 0;
    int length = 0;
    int numFields = 3;

    char nullSection = 0;

    // Store number of fields (3)
    //memcpy((char *)buffer + offset, &numFields, sizeof(short));
    //offset += sizeof(short);

    // Store null data field, so ReadPage works correctly
    memcpy((char *)buffer + offset, &nullSection, 1);
    offset += 1;

    // Store offsets for each field
    /*int idOffset = offset + (sizeof(short) * numFields);
    memcpy((char *)buffer + offset, &idOffset, sizeof(short));
    offset += sizeof(short);

    idOffset = 13;
    memcpy((char *)buffer + offset, &idOffset, sizeof(short));
    offset += sizeof(short);

    idOffset = idOffset + table.length() + sizeof(int);
    memcpy((char *)buffer + offset, &idOffset, sizeof(short));
    offset += sizeof(short);*/

    // store ID
    memcpy((char *)buffer + offset, &id, sizeof(int));
    offset += sizeof(int);

    // store table name
    length = table.length();
    memcpy((char *)buffer + offset, &length, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)buffer + offset, table.c_str(), length);
    offset += length;

    // store file name
    length = file.length();
    memcpy((char *)buffer + offset, &length, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)buffer + offset, file.c_str(), length);
    offset += length;
}

void prepareColumnsRecord(const int id, const string &name, const AttrType type, const int length, const int position, void *buffer) {
    unsigned int offset = 0;
    int l = 0;
    int numFields = 5;

    char nullSection = 0;

    // Store number of fields (3)
    //memcpy((char *)buffer + offset, &numFields, sizeof(short));
    //offset += sizeof(short);

    // Store null data field, so ReadPage works correctly
    memcpy((char *)buffer + offset, &nullSection, 1);
    offset += 1;

    // Store offsets for each field
    /*int idOffset = offset + (sizeof(short) * numFields); // id
    memcpy((char *)buffer + offset, &idOffset, sizeof(short));
    offset += sizeof(short);

    idOffset = idOffset + sizeof(int); // name
    memcpy((char *)buffer + offset, &idOffset, sizeof(short));
    offset += sizeof(short);

    idOffset = idOffset + name.length() + sizeof(int); // type
    memcpy((char *)buffer + offset, &idOffset, sizeof(short));
    offset += sizeof(short);

    idOffset = idOffset + sizeof(int); // length
    memcpy((char *)buffer + offset, &idOffset, sizeof(short));
    offset += sizeof(short);

    idOffset = idOffset + sizeof(int); // position
    memcpy((char *)buffer + offset, &idOffset, sizeof(short));
    offset += sizeof(short);*/

    // Copy over data 
    memcpy((char *)buffer + offset, &id, sizeof(int));
    offset += sizeof(int);

    l = name.length();
    memcpy((char *)buffer + offset, &l, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)buffer + offset, name.c_str(), name.length());
    offset += name.length();

    memcpy((char *)buffer + offset, &type, sizeof(int));
    offset += sizeof(int);

    memcpy((char *)buffer + offset, &length, sizeof(int));
    offset += sizeof(int);

    memcpy((char *)buffer + offset, &position, sizeof(int));
    offset += sizeof(int);
}

RC RelationManager::getTableIdByName(const string &tableName, int &tableId) {
    // Get table id from tables
    int tableNameLength = tableName.size();

    // Open the "tables" file
    FileHandle tablesHandle;
    if (rbfm->openFile("Tables", tablesHandle) == -1) {
        return -1;
    }

    RID rid;

    // Determine which index is table-name in tabledesc
    vector<Attribute> desc = getTablesDesc();
    string columnName = "table-name";
    int columnNumber = -1;

    for (int i = 0; i < desc.size(); i++) {
        if (columnName.compare(desc[i].name) == 0) {
            columnNumber = i;
        }
    }

    // could not find the table-name in the descriptor
    if (columnNumber == -1) return -1;

    // Get number of records currently in the tables table
    tablesHandle.infile->seekg(0, ios::end);
    int length = tablesHandle.infile->tellg();

    int numPages = length / PAGE_SIZE;
    void* record = malloc(120); // will be used to store each record
    int id;

    // search for the record with the same table-name property	
    for (int i = 0; i < numPages; i++) {
        void* pageData = malloc(PAGE_SIZE);
        tablesHandle.readPage(i, pageData);

        int numRecordsOnPage;
        memcpy(&numRecordsOnPage, (char*)pageData + N_OFFSET, sizeof(int));

        // Iterate over all the records on page looking for the record that matches the table name
        int recordNumber = 0;
        int slotNumber = 1;
        while (recordNumber < numRecordsOnPage) {
            //int slotLength;
            int slotOffset;
            int slotPosition = N_OFFSET - (slotNumber * (sizeof(int)* 2));

            memcpy(&slotOffset, (char*)pageData + slotPosition, sizeof(int));

            // record exists in this slot
            if (slotOffset >= 0) {
                rid.pageNum = i;
                rid.slotNum = slotNumber - 1;

                rbfm->readRecord(tablesHandle, desc, rid, record);

                // generate the offset for the string name
                int offset = 1; // compensate for the null indicator
                int thisStringLength = 0;

                for (int j = 0; j <= columnNumber; j++) {
                    int attrLen = sizeof(int); // Attribute length is defaulted to sizeof int unless TypeVarChar modifies it
                    char* attrName = new char[desc[j].length];
                    if (desc[j].type == TypeVarChar) {
                        memcpy(&attrLen, (char*)record + offset, sizeof(int));
                        offset += sizeof(int);
                        memcpy(attrName, (char*)record + offset, attrLen);
                        attrName[attrLen] = '\0';

                    }

                    // table-id column
                    if (j == 0) {
                        memcpy(&id, (char*)record + offset, sizeof(int));
                    }

                    if (j == columnNumber && attrLen == tableNameLength && strcmp((char*)attrName, tableName.c_str()) == 0) {
                        tableId = id;
                        delete record;
                        return 0;
                    }

                    offset += attrLen;
                }

                recordNumber++;
            }

            // move to next slot
            slotNumber++;
        }
    }

    free(record);
    return -1;
}
