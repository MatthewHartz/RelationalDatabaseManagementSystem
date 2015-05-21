#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

// Constants
const int NODE_FREE = PAGE_SIZE - sizeof(int);
const int NODE_TYPE = PAGE_SIZE - ((sizeof(int) * 2) + sizeof(byte));
const int NODE_RIGHT = PAGE_SIZE - ((sizeof(int) * 2));
const int RID_SIZE = 2 * sizeof(int);

const int DEFAULT_FREE = PAGE_SIZE - 9;

// Nodes
typedef enum { TypeNode = 0, TypeLeaf, TypeRoot } NodeType;

class IX_ScanIterator;
class IXFileHandle;

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file
        RC createFile(const string &fileName);

        // Delete an index file
        RC destroyFile(const string &fileName);

        // Open an index and return a file handle
        RC openFile(const string &fileName, IXFileHandle &ixFileHandle);

        // Close a file handle for an index. 
        RC closeFile(IXFileHandle &ixFileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle
        // www.sanfoundry.com/cpp-program-to-implement-b-tree
        RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given fileHandle
        RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to supports a range search
        RC scan(IXFileHandle &ixFileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree JSON record in pre-order
        void printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const;

        // Splits the child into two seperate nodes (odd will push left)
        void splitChild();

        // Traverses the tree
        RC traverse(void *&child, void *&parent, const void *key, const Attribute &attribute, IXFileHandle &ixFileHandle, int &leftPageNum);

        // Determines if the page has enough space
        bool hasEnoughSpace(void *data, const Attribute &attribute);

        // insert the Director Key <key, next pointer> into a non-leaf node
        RC insertDirector(void *node, const void *key, const Attribute &attribute, int nextPageNum, IXFileHandle &ixFileHandle);
        
        // returns a director triplet at a given offset
        RC getDirectorAtOffset(int &offset, void* node, int &leftPointer, int &rightPointer, void* key, const Attribute &attribute);

        // Used to enter a Key into a leaf 
        RC insertIntoLeaf(void *child, const void *key, const Attribute &attribute, const RID &rid);

        // Used to remove a key from a leaf
        RC deleteFromLeaf(IXFileHandle &ixFileHandle, void *child, const void *key, const Attribute &attribute, const RID &rid);

        // Compares the keys and returns -1 if key1 < key2, 0 if key1 == key2, 1 if key1 > key2
        int compareKeys(const void *key1, const void *key2, const Attribute attribute);

        // This function will get the next key offset in a leaf node
        int getNextKeyOffset(int RIDnumOffset, void *node);

        // Function to get the number of RIDs in a <key, pair> entry
        int getNumberOfRids(void *node, int RIDnumOffset);

        // Gets the length of a key
        int getKeyLength(const void *key, Attribute attr);

        // Creates an initial key on the insert of a leaf node
        void createNewLeafEntry(void *data, const void *key, const Attribute &attribute, const RID &rid);

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
        PagedFileManager *pfm;
};

class IX_ScanIterator {
    public:
        IX_ScanIterator();  							// Constructor
        ~IX_ScanIterator(); 							// Destructor

        RC getNextEntry(RID &rid, void *key);  		// Get next matching entry
        RC close();             						// Terminate index scan
};


class IXFileHandle {
    public:
        // Put the current counter values of associated PF FileHandles into variables
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

        IXFileHandle();  							// Constructor
        ~IXFileHandle(); 							// Destructor

        FileHandle &getHandle() { return *this->handle; }
        void setHandle(FileHandle &h) { handle = &h; }
        void setRoot(void *data) { handle->currentPage = data; };
        void setFreeSpace(void *data, int freeSpace) { memcpy((char*) data + NODE_FREE, &freeSpace, sizeof(int)); };
        void setNodeType(void *node, NodeType type);
        void* getRoot() { return handle->currentPage; };
        int getRightPointer(void *node);
        NodeType getNodeType(void *node);
        void setRightPointer(void *node, int rightPageNum);
        int initializeNewNode(void *data, NodeType type); // Initializes a new node, setting it's free space and node type
        int getAvailablePageNumber(); // This helper function will get the first available page

        // static functions that don't require an instance of ixFileHandler
        static int getFreeSpace(void *data);
        static int getFreeSpaceOffset(int freeSpace) { return (DEFAULT_FREE - freeSpace); };

    private:
        FileHandle *handle;
        vector<int> freePages; // when a page becomes free on delete, it is added to this list.  This will be used first when opening a new page.

};

// print out the error message for a given return code
void IX_PrintError (RC rc);

#endif
