
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

	// Create system tables
	if (rbfm->createFile(tablesName) == -1 || rbfm->createFile(columnsName) == -1) {
		return -1;
	}

	// Create file handles
	FileHandle tablesHandle;
	FileHandle columnsHandle;

	// Open files
	if (rbfm->openFile(tablesName, tablesHandle) == -1 
		|| rbfm->openFile(columnsName, columnsHandle) == -1) {
		return -1;
	}

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

	// Insert data into Tables
	prepareTablesRecord(1, tablesName, tablesName, buffer);
	rbfm->insertRecord(tablesHandle, tablesDesc, buffer, rid);

	prepareTablesRecord(2, columnsName, columnsName, buffer);
	rbfm->insertRecord(tablesHandle, tablesDesc, buffer, rid);

	// Insert data into Columns
	prepareColumnsRecord(1, "table-id", TypeInt, 4, 1, buffer);
	rbfm->insertRecord(columnsHandle, columnsDesc, buffer, rid);

	prepareColumnsRecord(1, "table-name", TypeVarChar, 50, 2, buffer);
	rbfm->insertRecord(columnsHandle, columnsDesc, buffer, rid);

	prepareColumnsRecord(1, "file-name", TypeVarChar, 50, 3, buffer);
	rbfm->insertRecord(columnsHandle, columnsDesc, buffer, rid);

	prepareColumnsRecord(1, "table-id", TypeInt, 4, 1, buffer);
	rbfm->insertRecord(columnsHandle, columnsDesc, buffer, rid);

	prepareColumnsRecord(1, "column-name", TypeVarChar, 50, 2, buffer);
	rbfm->insertRecord(columnsHandle, columnsDesc, buffer, rid);

	prepareColumnsRecord(1, "column-type", TypeInt, 4, 3, buffer);
	rbfm->insertRecord(columnsHandle, columnsDesc, buffer, rid);

	prepareColumnsRecord(1, "column-length", TypeInt, 4, 4, buffer);
	rbfm->insertRecord(columnsHandle, columnsDesc, buffer, rid);

	prepareColumnsRecord(1, "column-position", TypeInt, 4, 5, buffer);
	rbfm->insertRecord(columnsHandle, columnsDesc, buffer, rid);

	// Close files
	if (rbfm->closeFile(tablesHandle) == -1
		|| rbfm->closeFile(columnsHandle) == -1) {
		return -1;
	}

	delete buffer;
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
	int largestTableId = 0;

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
			int slotLength;
			int slotOffset;
			int slotPosition = N_OFFSET - (slotNumber * (sizeof(int) * 2));

			memcpy(&slotOffset, (char*)pageData + slotPosition, sizeof(int));
			memcpy(&slotLength, (char*)pageData + slotPosition + 4, sizeof(int));

			// record exists in this slot
			if (slotLength > 0) {
				// read the first attribute (table-id) and test if that value is larger that the current max
				int tableId;
				memcpy(&tableId, (char*)pageData + slotOffset + 1, sizeof(int));

				if (tableId > largestTableId) largestTableId = tableId;

				recordNumber++;
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



	return 0;
}

RC RelationManager::deleteTable(const string &tableName)
{
	// Get table id from tables
	// Remove from tables
	// Use table id from tables to delete from columns
	// Destroy file

	return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	return -1;
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

RC RelationManager::scan(const string &tableName,
	  const string &conditionAttribute,
	  const CompOp compOp,                  
	  const void *value,                    
	  const vector<string> &attributeNames,
	  RM_ScanIterator &rm_ScanIterator)
{
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
