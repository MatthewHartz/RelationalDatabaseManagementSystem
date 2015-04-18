
#include "rm.h"

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
	if(!_rm)
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
	addAttributeToDesc("file-name", TypeInt, (AttrLength)4, tablesDesc);

	setTablesDesc(tablesDesc);
	setColumnsDesc(columnsDesc);

	RID rid;
	void* buffer = malloc(120);

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
	int largestTableId = 0; // Will be used for both inserting into tables and columns
	void* record = malloc(120); // will be used to store each record

	// search for the largest table id through all the pages
	for (int i = 0; i < numPages; i++) {
		void* pageData = malloc(PAGE_SIZE);
		tablesHandle.readPage(i, pageData); 

		int numRecordsOnPage;
		memcpy(&numRecordsOnPage, (char*)pageData + N_OFFSET, sizeof(int));

		// Iterate over all the records on page looking for the largest table id
		int recordNumber = 0;
		int slotNumber = 1;


		while (recordNumber < numRecordsOnPage) {
			//int slotLength;
			int slotOffset;
			int slotPosition = N_OFFSET - (slotNumber * (sizeof(int) * 2));

			memcpy(&slotOffset, (char*)pageData + slotPosition, sizeof(int));

			// record exists in this slot
			if (slotOffset >= 0) {

				rid.pageNum = i;
				rid.slotNum = slotNumber - 1;

				// read the first attribute (table-id) and test if that value is larger that the current max
				rbfm->readRecord(tablesHandle, getTablesDesc(), rid, record);
				int tableId;
				memcpy(&tableId, (char*)record + 1, sizeof(int));

				if (tableId > largestTableId) largestTableId = tableId;

				recordNumber++;
			}
			// TODO: Add code to handle a table row that is updated onto a new page
			else {

			}

			// move to next slot
			slotNumber++;
		}
	}

	// Add table desc to tables
	prepareTablesRecord(largestTableId + 1, tableName, tableName, buffer);
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
		prepareColumnsRecord(largestTableId, attrs[i].name, attrs[i].type, attrs.size(), i, buffer);
		rbfm->insertRecord(columnsHandle, this->getColumnsDesc(), buffer, rid);
	}

	rbfm->closeFile(columnsHandle);

	delete record;
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

	return 0;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	return -1;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	return -1;
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
	// Open the "tables" file
	FileHandle handle;
	if (rbfm->openFile(tableName, handle) == -1) {
		return -1;
	}

	// get size of table
	handle.infile->seekg(0, ios::end);
	int length = handle.infile->tellg();

	// get number of pages
	int numPages = length / PAGE_SIZE;
	void* record = malloc(120); // will be used to store each record

	switch (compOp) {
		case EQ_OP:
			break;
		case LT_OP:
			break;
		case GT_OP:
			break;
		case LE_OP:
			break;
		case GE_OP:
			break;
		case NE_OP:
			break;
		default:
			break;
	}

	



	return -1;
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

	char nullSection = 0;

	// Store null data field, so ReadPage works correctly
	memcpy((char *)buffer + offset, &nullSection, 1);
	offset += 1;

	// Copy over data 
	memcpy((char *)buffer + offset, &id, sizeof(int));
	offset += sizeof(int);

	length = table.length();
	memcpy((char *)buffer + offset, &length, sizeof(int));
	offset += sizeof(int);
	memcpy((char *)buffer + offset, table.c_str(), table.length());
	offset += table.length();

	length = file.length();
	memcpy((char *)buffer + offset, &length, sizeof(int));
	offset += sizeof(int);
	memcpy((char *)buffer + offset, file.c_str(), file.length());
	offset += file.length();
}

void prepareColumnsRecord(const int id, const string &name, const AttrType type, const int length, const int position, void *buffer) {
	unsigned int offset = 0;
	int l = 0;

	char nullSection = 0;

	// Store null data field, so ReadPage works correctly
	memcpy((char *)buffer + offset, &nullSection, 1);
	offset += 1;

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

	delete record;
	return -1;
}
