#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

// Constants
const int NODE_FREE = PAGE_SIZE - sizeof(int);
const byte NODE_TYPE = PAGE_SIZE - (sizeof(int) + 1); // 0 is node and 1 is leaf (I was tempted to use ENUM but emums are ints.
const int NODE_POINTER = PAGE_SIZE - ((sizeof(int) * 2) + 1); // This will only exist for leaf nodes

// Nodes
typedef enum { TypeNode = 0, TypeLeaf } NodeType;

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
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle
        // www.sanfoundry.com/cpp-program-to-implement-b-tree
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given fileHandle
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to supports a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree JSON record in pre-order
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;

        // Splits the child into two seperate nodes (odd will push left)
        void splitChild();

        // Traverses the tree
        void traverse();

        // Deteremines if the page has enough space
        bool hasEnoughSpace(void *data, AttrType type);

        // Initializes a new node, setting it's free space and node type
        int initializeNewNode(void *data, NodeType type);

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
        void setRoot(void *data) { root = data; }

    private:
        FileHandle *handle;
        void* root;

};

// print out the error message for a given return code
void IX_PrintError (RC rc);

#endif
