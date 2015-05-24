#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

// Constants
const int NODE_FREE = PAGE_SIZE - sizeof(int);
const int NODE_RIGHT = PAGE_SIZE - ((sizeof(int) * 2));
const int NODE_TYPE = PAGE_SIZE - ((sizeof(int) * 3));
const int RID_SIZE = 2 * sizeof(int);
const int SPLIT_THRESHOLD = PAGE_SIZE / 2;

const int DEFAULT_FREE = PAGE_SIZE - (sizeof(int) * 3);

// Nodes
typedef enum { TypeNode = 0, TypeLeaf = 1, TypeRoot = 2} NodeType;

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

        // Splits the child into two seperate nodes
        RC splitChild(void* child, void *parent, const Attribute &attribute, IXFileHandle &ixFileHandle, const void *key, int &childPageNum, int &parentPageNum);

        // Gets the following node based upon key value
        RC getNextNodeByKey(void *&child, void *&parent, const void *key, const Attribute &attribute, IXFileHandle &ixFileHandle, int &leftPageNum, int &parentPageNum);

        // Determines if the page has enough space
        bool hasEnoughSpace(void *data, const Attribute &attribute);

        // insert the Director Key <key, next pointer> into a non-leaf node
        RC insertDirector(void *node, const void *key, const Attribute &attribute, int nextPageNum, IXFileHandle &ixFileHandle);
        
        // returns a director triplet at a given offset
        RC getDirectorAtOffset(int &offset, void* node, int &leftPointer, int &rightPointer, void* key, const Attribute &attribute);

        // Used to enter a Key into a leaf 
        RC insertIntoLeaf(IXFileHandle &ixFileHandle, void *child, const void *key, const Attribute &attribute, const RID &rid);

        // Used to remove a key from a leaf
        RC deleteFromLeaf(IXFileHandle &ixFileHandle, void *child, const void *key, const Attribute &attribute, const RID &rid);

        // Compares the keys and returns -1 if key1 < key2, 0 if key1 == key2, 1 if key1 > key2
        int compareKeys(const void *key1, const void *key2, const Attribute attribute);

        // This function will get the next key offset in a leaf node
        static int getNextKeyOffset(int RIDnumOffset, void *node);

        // Function to get the number of RIDs in a <key, pair> entry
        static int getNumberOfRids(void *node, int RIDnumOffset);

        // Gets the length of a key
        int getKeyLength(const void *key, Attribute attr);

        // Creates an initial key on the insert of a leaf node
        void createNewLeafEntry(void *data, const void *key, const Attribute &attribute, const RID &rid);

        // The recursive function that will iterate over the tree printing out all the nodes contents
        RC printNode(void *node, IXFileHandle &ixFileHandle, const Attribute &attribute, int depth) const;

        // Collects the keys in a leaf node
        RC getKeysInLeaf(IXFileHandle &ixFileHandle, void *node, const Attribute &attribute, vector<string> &keys) const;

        // Collect the keys in a non leaf node
        RC getKeysInNonLeaf(IXFileHandle &ixFileHandle, void *node, const Attribute &attribute, vector<string> &keys, vector<int> &pages) const;

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

        // the Getters and Setters
        void setHandle(IXFileHandle &ixfileHandle) { ixFileHandle = &ixfileHandle; };
        void setAttribute(const Attribute &attr) { attribute = &attr; };
        void setLowKeyValues(const void *lowKey, bool lowKeyInclusive);
        void setHighKeyValues(const void *highKey, bool highKeyInclusive);
        void setLeafNode(void *node) { memcpy((char *) leafNode, (char *) node, PAGE_SIZE); };
        void setType(void (*f)(void*&, void*, int)) { getKey = f; };
        void setFunc(bool (*f)(void*, const void*, const void*, void*, int, bool, bool)) { compareTypeFunc = f; };
        void setLeafOffset(int newOffset) { currentLeafOffset = newOffset; };
        int getLeafOffset() { return currentLeafOffset; };

        // static functions used to extract types
        static void getIntType(void *&type, void *node, int offset);
        static void getRealType(void *&type, void *node, int offset);
        static void getVarCharType(void *&type, void *node, int offset); 
        static bool compareInts(void *incomingKey, const void *low, const void *high, void *node, int offset, bool lowInc, bool highInc);
        static bool compareReals(void *incomingKey, const void *low, const void *high, void *node, int offset, bool lowInc, bool highInc);
        static bool compareVarChars(void *incomingKey, const void *low, const void *high, void *node, int offset, bool lowInc, bool highInc);

    private:
        void *leafNode;
        IXFileHandle *ixFileHandle;
        const Attribute *attribute;
        const void *lowKey;
        const void *highKey;
        bool lowKeyInclusive;
        bool highKeyInclusive;
        int currentLeafOffset;
        void (*getKey)(void*&, void*, int);
        bool (*compareTypeFunc)(void*, const void*, const void*, void*, int, bool, bool); 
};


class IXFileHandle {
    public:
        // Put the current counter values of associated PF FileHandles into variables
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

        IXFileHandle();  							// Constructor
        ~IXFileHandle(); 							// Destructor

        FileHandle* getHandle() { return handle; };
        void setHandle(FileHandle &h) { handle = &h; };
        void setRoot(void *data) { handle->currentPage = data; };
        void setFreeSpace(void *data, int freeSpace) { memcpy((char*) data + NODE_FREE, &freeSpace, sizeof(int)); };
        void setNodeType(void *node, NodeType type);
        void* getRoot() { return handle->currentPage; };
        RC getNode(int pageNum, void *node) { return getHandle()->readPage(pageNum, node); };
        int getRightPointer(void *node);
        void setRightPointer(void *node, int rightPageNum);
        int initializeNewNode(void *data, NodeType type); // Initializes a new node, setting it's free space and node type
        int getAvailablePageNumber(); // This helper function will get the first available page

        // static functions that don't require an instance of ixFileHandler
        static int getFreeSpace(void *data);
        static int getFreeSpaceOffset(int freeSpace) { return (DEFAULT_FREE - freeSpace); }; 
        static NodeType getNodeType(void *node);

        // these functions will determine if the left and right nodes of a director are null
        // they return a bool and as a side effect the pagenumber of the child, 0 if null
        static bool isLeftNodeNull(void *node, int offset, int &childPageNum);
        static bool isRightNodeNull(void *node, const Attribute &attribute, int offset, int &childPageNum);

    private:
        FileHandle *handle;
        vector<int> freePages; // when a page becomes free on delete, it is added to this list.  This will be used first when opening a new page.

};

// print out the error message for a given return code
//void IX_PrintError (RC rc);

#endif
