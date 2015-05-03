
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
            if (!rbfm->isFieldNull(buffer, 0)) {
                memcpy(&maxTableId, (int*)buffer + 1, sizeof(int));
            }
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
    string fileName;
    RID rid;
    void* buffer = malloc(PAGE_SIZE);

    // Get table id
    //if (getTableIdByName(tableName, tableId) == -1) return -1;

    // Scan through the Tables table and get the id where table-name == tableName
    // This will be used to delete it from the columns table
    RM_ScanIterator rmsi;
    vector<string> attributes;
    attributes.push_back("table-id");
    attributes.push_back("file-name");

    int varLength = tableName.length();
    void *value = malloc(sizeof(int) + varLength);
    memcpy((char *) value, &varLength, sizeof(int));
    memcpy((char *) value + sizeof(int), tableName.c_str(), varLength);
    RC rc = RelationManager::scan("Tables", "table-name", EQ_OP, value, attributes, rmsi);

    if (rc != -1) {
        while (rmsi.getNextTuple(rid, buffer) != RM_EOF){
        	int offSet = 0;
            if (!rbfm->isFieldNull(buffer, 0)) {
                memcpy(&tableId, (int*)buffer + 1, sizeof(int));
                offSet += sizeof(int);
            }

            int fileNameLength;
            //memcpy(&fileNameLength, (char*)buffer)
        }
        rmsi.close();
    }

    // Delete tuple from Tables table
    if (RelationManager::deleteTuple("Tables", rid) == -1) {
        return -1;
    }

    rmsi.close();

    value = malloc(sizeof(int));
    memcpy((char *) value, &tableId, sizeof(int));
    attributes.clear();
    attributes.push_back("table-id");

    // Scan through the Columns table and get all rows where table-id == tableId
    // This will be used to delete each record from Columns as they are found by their RID
    rc = RelationManager::scan("Columns", "table-id", EQ_OP, value, attributes, rmsi);

    if (rc != -1) {
        while (rmsi.getNextTuple(rid, buffer) != RM_EOF){
            if (RelationManager::deleteTuple("Columns", rid) == -1) {
            	return -1;
            }
        }
        rmsi.close();
    }

    // TODO
    // Destroy the file
    FileHandle fileHandle;
    //rbfm->openFile(tableName)

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

    // Delete data
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
    // Initialize RBFM iterator and file Handle
    RBFM_ScanIterator rbfmsi;
    FileHandle handle;
    RID rid;
    void* data = malloc(PAGE_SIZE);

    // Open "tables" file
    vector<string> names;
    names.push_back("table-id");
    names.push_back("file-name");
    if (rbfm->openFile("Tables", handle) == -1) {
        return -1;
    }

    // Initialize the condition attribute value to be compared to
    int varLength = tableName.length();
    void* compValue = malloc(sizeof(int) + varLength);
    memcpy((char *) compValue, &varLength, sizeof(int));
    memcpy((char *) compValue + sizeof(int), tableName.c_str(), varLength);

    // Initialize RBFMSI to scan through table's records looking for "Columns" and extract id
    if (rbfm->scan(handle, getTablesDesc(), "table-name", EQ_OP, compValue, names, rbfmsi)
        == -1) {
        rbfm->closeFile(handle);
        return RM_EOF;
    }

    int tableId;
    string fileName;

    // Get the first record where table-name matches tableName
    if (rbfmsi.getNextRecord(rid, data) != RBFM_EOF) {
        int offset = 1;

        // If either of these 2 fields are null, return -1
        if (rbfm->isFieldNull(data, 0) || rbfm->isFieldNull(data, 1)) {
            return -1;
        }

        // Get the table ID
        memcpy(&tableId, (char *) data + offset, sizeof(int));
        offset += sizeof(int);

        // Get the file Name
        int nameLength;
        memcpy(&nameLength, (char*)data + offset, sizeof(int));
        offset += sizeof(int);
        char* name = new char[nameLength + 1];
        memcpy(name, (char*)data + offset, nameLength);
        name[nameLength] = '\0';
        fileName = std::string(name);

        delete name;
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
    compValue = malloc(sizeof(int));
    memcpy((char *) compValue, &tableId, sizeof(int));

    // Scan over each row of Columns, looking for where table-id == TableID
    if (rbfm->scan(handle, getColumnsDesc(), "table-id", EQ_OP, compValue, names, rbfmsi)
        == -1) {
        rbfm->closeFile(handle);
        return RM_EOF;
    }

    vector<Attribute> scanDescriptor;
    int fieldCounter = 0;

    // Get each record where table-id matches tableId in the Columns table
    // We will then create the descriptor based off of these records
    while (rbfmsi.getNextRecord(rid, data) != RBFM_EOF) {
        //TODO: Collect each column and initialize a descriptor.
    	Attribute attr;
        int offset = 1; // compensate for nullIndicator of size = 1. ie: 3 fields is 1 byte

        // If reading the descriptor and any of the fields are null, this is bad.
        if (rbfm->isFieldNull(data, 0) || rbfm->isFieldNull(data, 1) || rbfm->isFieldNull(data, 2)) {
            return -1;
        }

        // Read Column Name
        int nameLength;
        memcpy(&nameLength, (char*)data + offset, sizeof(int));
        offset += sizeof(int);
        char* name = new char[nameLength + 1];
        memcpy(name, (char*)data + offset, nameLength);
        offset += nameLength;
        name[nameLength] = '\0';
        attr.name = std::string(name);

        // Read column-Type
        memcpy(&attr.type, (char*)data + offset, sizeof(int));
        offset += sizeof(int);

        // Read column Length
        memcpy(&attr.length, (char*)data + offset, sizeof(int));

        scanDescriptor.push_back(attr);
        delete name;
    }

    rbfmsi.close();
    rbfm->closeFile(handle);

    // Open the handle for the file to be scanned over, this will be attached to the rbfmsi
    if (rbfm->openFile(fileName, handle) == -1) return -1;

    if (rbfm->scan(handle, scanDescriptor, "table-id", EQ_OP, value, attributeNames, rbfmsi)
        == -1) {
        rbfm->closeFile(handle);
        return RM_EOF;
    }

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
