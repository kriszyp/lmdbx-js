
// This file is part of node-lmdbx, the Node.js binding for lmdbx
// Copyright (c) 2013-2017 Timur Kristóf
// Licensed to you under the terms of the MIT license
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:

// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.

// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

#ifndef NODE_LMDBX_H
#define NODE_LMDBX_H

#include <vector>
#include <algorithm>
#include <v8.h>
#include <node.h>
#include <node_buffer.h>
#include <nan.h>
#include <uv.h>
#include "mdbx.h"
#include "lz4.h"

using namespace v8;
using namespace node;

enum class NodeLmdbxKeyType {

    // Invalid key (used internally by node-lmdbx)
    InvalidKey = -1,
    
    // Default key (used internally by node-lmdbx)
    DefaultKey = 0,

    // UCS-2/UTF-16 with zero terminator - Appears to V8 as string
    StringKey = 1,
    
    // LMDBX fixed size integer key with 32 bit keys - Appearts to V8 as an Uint32
    Uint32Key = 2,
    
    // LMDBX default key format - Appears to V8 as node::Buffer
    BinaryKey = 3,

};
enum class KeyCreation {
    Reset = 0,
    Continue = 1,
    InArray = 2,
};

class TxnWrap;
class DbiWrap;
class EnvWrap;
class CursorWrap;
class Compression;
class KeySpace;

// Exports misc stuff to the module
void setupExportMisc(Local<Object> exports);

// Helper callback
typedef void (*argtokey_callback_t)(MDBX_val &key);

void consoleLog(Local<Value> val);
void consoleLog(const char *msg);
void consoleLogN(int n);
void setFlagFromValue(int *flags, int flag, const char *name, bool defaultValue, Local<Object> options);
void writeValueToEntry(const Local<Value> &str, MDBX_val *val);
argtokey_callback_t argToKey(const Local<Value> &val, MDBX_val &key, NodeLmdbxKeyType keyType, bool &isValid);
bool valueToMDBXKey(const Local<Value> &key, MDBX_val &val, KeySpace &keySpace);

NodeLmdbxKeyType inferAndValidateKeyType(const Local<Value> &key, const Local<Value> &options, NodeLmdbxKeyType dbiKeyType, bool &isValid);
NodeLmdbxKeyType inferKeyType(const Local<Value> &val);
NodeLmdbxKeyType keyTypeFromOptions(const Local<Value> &val, NodeLmdbxKeyType defaultKeyType = NodeLmdbxKeyType::DefaultKey);
Local<Value> keyToHandle(MDBX_val &key, NodeLmdbxKeyType keyType);
Local<Value> getVersionAndUncompress(MDBX_val &data, DbiWrap* dw, Local<Value> (*successFunc)(MDBX_val&));
NAN_METHOD(getLastVersion);
NAN_METHOD(setLastVersion);
NAN_METHOD(bufferToKeyValue);
NAN_METHOD(keyValueToBuffer);

class KeySpaceHolder {
public:
    uint8_t* data;
    KeySpaceHolder* previousSpace;
    KeySpaceHolder(KeySpaceHolder* existingPreviousSpace, uint8_t* existingData);
    KeySpaceHolder();
    ~KeySpaceHolder();
};
class KeySpace : public KeySpaceHolder {
public:
    int position;
    int size;
    bool fixedSize;
    uint8_t* getTarget();
    KeySpace(bool fixed);
};

#ifndef thread_local
#ifdef __GNUC__
# define thread_local __thread
#elif __STDC_VERSION__ >= 201112L
# define thread_local _Thread_local
#elif defined(_MSC_VER)
# define thread_local __declspec(thread)
#else
# define thread_local
#endif
#endif

// markers for special cases
const double ANY_VERSION = -3.3434325325532E-199;
const double NO_EXIST_VERSION = -4.2434325325532E-199;

static thread_local double lastVersion = 0;
static thread_local DbiWrap* currentDb = nullptr;
static thread_local KeySpace* fixedKeySpace;
void setLastVersion(double version);

Local<Value> valToUtf8(MDBX_val &data);
Local<Value> valToString(MDBX_val &data);
Local<Value> valToStringUnsafe(MDBX_val &data);
Local<Value> valToBinary(MDBX_val &data);
Local<Value> valToBinaryUnsafe(MDBX_val &data);
Local<Value> valToNumber(MDBX_val &data);
Local<Value> valToBoolean(MDBX_val &data);

Local<Value> MDBXKeyToValue(MDBX_val &data);
void makeGlobalUnsafeBuffer(size_t size);

int putWithVersion(MDBX_txn *   txn,
        MDBX_dbi     dbi,
        MDBX_val *   key,
        MDBX_val *   data,
        unsigned int flags, double version);

void throwLmdbxError(int rc);

class TxnWrap;
class DbiWrap;
class EnvWrap;
class CursorWrap;
struct env_path_t {
    MDBX_env* env;
    char* path;
    int count;
};

/*
    `Env`
    Represents a database environment.
    (Wrapper for `MDBX_env`)
*/
class EnvWrap : public Nan::ObjectWrap {
private:
    // The wrapped object
    MDBX_env *env;
    // Current write transaction
    TxnWrap *currentWriteTxn;
    // List of open read transactions
    std::vector<TxnWrap*> readTxns;
    // Constructor for TxnWrap
    static thread_local Nan::Persistent<Function>* txnCtor;
    // Constructor for DbiWrap
    static thread_local Nan::Persistent<Function>* dbiCtor;
    static uv_mutex_t* envsLock;
    static std::vector<env_path_t> envs;
    static uv_mutex_t* initMutex();
    // compression settings and space
    Compression *compression;

    // Cleans up stray transactions
    void cleanupStrayTxns();

    friend class TxnWrap;
    friend class DbiWrap;

public:
    EnvWrap();
    ~EnvWrap();

    // Sets up exports for the Env constructor
    static void setupExports(Local<Object> exports);

    /*
        Constructor of the database environment. You need to `open()` it before you can use it.
        (Wrapper for `mdbx_env_create`)
    */
    static NAN_METHOD(ctor);
    
    /*
        Gets statistics about the database environment.
    */
    static NAN_METHOD(stat);

    /*
        Gets statistics about the free space database
    */
    static NAN_METHOD(freeStat);
    
    /*
        Detaches a buffer from the backing store
    */
    static NAN_METHOD(detachBuffer);

    /*
        Gets information about the database environment.
    */
    static NAN_METHOD(info);

    /*
        Opens the database environment with the specified options. The options will be used to configure the environment before opening it.
        (Wrapper for `mdbx_env_open`)

        Parameters:

        * Options object that contains possible configuration options.

        Possible options are:

        * maxDbs: the maximum number of named databases you can have in the environment (default is 1)
        * maxReaders: the maximum number of concurrent readers of the environment (default is 126)
        * mapSize: maximal size of the memory map (the full environment) in bytes (default is 10485760 bytes)
        * path: path to the database environment
    */
    static NAN_METHOD(open);

    /*
        Resizes the maximal size of the memory map. It may be called if no transactions are active in this process.
        (Wrapper for `mdbx_env_set_mapsize`)

        Parameters:

        * maximal size of the memory map (the full environment) in bytes (default is 10485760 bytes)
    */
    static NAN_METHOD(resize);

    /*
        Copies the database environment to a file.
        (Wrapper for `mdbx_env_copy2`)

        Parameters:

        * path - Path to the target file
        * compact (optional) - Copy using compact setting
        * callback - Callback when finished (this is performed asynchronously)
    */
    static NAN_METHOD(copy);    

    /*
        Closes the database environment.
        (Wrapper for `mdbx_env_close`)
    */
    static NAN_METHOD(close);

    /*
        Starts a new transaction in the environment.
        (Wrapper for `mdbx_txn_begin`)

        Parameters:

        * Options object that contains possible configuration options.

        Possible options are:

        * readOnly: if true, the transaction is read-only
    */
    static NAN_METHOD(beginTxn);

    /*
        Opens a database in the environment.
        (Wrapper for `mdbx_dbi_open`)

        Parameters:

        * Options object that contains possible configuration options.

        Possible options are:

        * name: the name of the database (or null to use the unnamed database)
        * create: if true, the database will be created if it doesn't exist
        * keyIsUint32: if true, keys are treated as 32-bit unsigned integers
        * dupSort: if true, the database can hold multiple items with the same key
        * reverseKey: keys are strings to be compared in reverse order
        * dupFixed: if dupSort is true, indicates that the data items are all the same size
        * integerDup: duplicate data items are also integers, and should be sorted as such
        * reverseDup: duplicate data items should be compared as strings in reverse order
    */
    static NAN_METHOD(openDbi);

    /*
        Flushes all data to the disk asynchronously.
        (Asynchronous wrapper for `mdbx_env_sync`)

        Parameters:

        * Callback to be executed after the sync is complete.
    */
    static NAN_METHOD(sync);

    /*
        Performs a set of operations asynchronously, automatically wrapping it in its own transaction

        Parameters:

        * Callback to be executed after the sync is complete.
    */
    static NAN_METHOD(batchWrite);
};

/*
    `Txn`
    Represents a transaction running on a database environment.
    (Wrapper for `MDBX_txn`)
*/
class TxnWrap : public Nan::ObjectWrap {
private:
    // The wrapped object
    MDBX_txn *txn;

    // Reference to the MDBX_env of the wrapped MDBX_txn
    MDBX_env *env;

    // Environment wrapper of the current transaction
    EnvWrap *ew;
    
    // Flags used with mdbx_txn_begin
    MDBX_txn_flags_t flags;
    
    // Remove the current TxnWrap from its EnvWrap
    void removeFromEnvWrap();

    friend class CursorWrap;
    friend class DbiWrap;
    friend class EnvWrap;

public:
    TxnWrap(MDBX_env *env, MDBX_txn *txn);
    ~TxnWrap();

    // Constructor (not exposed)
    static NAN_METHOD(ctor);

    // Helper for all the get methods (not exposed)
    static Nan::NAN_METHOD_RETURN_TYPE getCommon(Nan::NAN_METHOD_ARGS_TYPE info, Local<Value> (*successFunc)(MDBX_val&));

    // Helper for all the put methods (not exposed)
    static Nan::NAN_METHOD_RETURN_TYPE putCommon(Nan::NAN_METHOD_ARGS_TYPE info, void (*fillFunc)(Nan::NAN_METHOD_ARGS_TYPE info, MDBX_val&), void (*freeFunc)(MDBX_val&));

    /*
        Commits the transaction.
        (Wrapper for `mdbx_txn_commit`)
    */
    static NAN_METHOD(commit);

    /*
        Aborts the transaction.
        (Wrapper for `mdbx_txn_abort`)
    */
    static NAN_METHOD(abort);

    /*
        Aborts a read-only transaction but makes it renewable with `renew`.
        (Wrapper for `mdbx_txn_reset`)
    */
    static NAN_METHOD(reset);

    /*
        Renews a read-only transaction after it has been reset.
        (Wrapper for `mdbx_txn_renew`)
    */
    static NAN_METHOD(renew);

    /*
        Gets string data (JavaScript string type) associated with the given key from a database as UTF-8. You need to open a database in the environment to use this.
        This method is not zero-copy and the return value will usable as long as there is a reference to it.
        (Wrapper for `mdbx_get`)

        Parameters:

        * database instance created with calling `openDbi()` on an `Env` instance
        * key for which the value is retrieved
    */
    static NAN_METHOD(getUtf8);

    /*
        Gets string data (JavaScript string type) associated with the given key from a database. You need to open a database in the environment to use this.
        This method is not zero-copy and the return value will usable as long as there is a reference to it.
        (Wrapper for `mdbx_get`)

        Parameters:

        * database instance created with calling `openDbi()` on an `Env` instance
        * key for which the value is retrieved
    */
    static NAN_METHOD(getString);

    /*
        Gets string data (JavaScript string type) associated with the given key from a database. You need to open a database in the environment to use this.
        This method is zero-copy and the return value can only be used until the next put operation or until the transaction is committed or aborted.
        (Wrapper for `mdbx_get`)

        Parameters:

        * database instance created with calling `openDbi()` on an `Env` instance
        * key for which the value is retrieved
    */
    static NAN_METHOD(getStringUnsafe);

    /*
        Gets binary data (Node.js Buffer) associated with the given key from a database. You need to open a database in the environment to use this.
        This method is not zero-copy and the return value will usable as long as there is a reference to it.
        (Wrapper for `mdbx_get`)

        Parameters:

        * database instance created with calling `openDbi()` on an `Env` instance
        * key for which the value is retrieved
    */
    static NAN_METHOD(getBinary);

    /*
        Gets binary data (Node.js Buffer) associated with the given key from a database. You need to open a database in the environment to use this.
        This method is zero-copy and the return value can only be used until the next put operation or until the transaction is committed or aborted.
        (Wrapper for `mdbx_get`)

        Parameters:

        * database instance created with calling `openDbi()` on an `Env` instance
        * key for which the value is retrieved
    */
    static NAN_METHOD(getBinaryUnsafe);

    /*
        Gets number data (JavaScript number type) associated with the given key from a database. You need to open a database in the environment to use this.
        This method will copy the value out of the database.
        (Wrapper for `mdbx_get`)

        Parameters:

        * database instance created with calling `openDbi()` on an `Env` instance
        * key for which the value is retrieved
    */
    static NAN_METHOD(getNumber);

    /*
        Gets boolean data (JavaScript boolean type) associated with the given key from a database. You need to open a database in the environment to use this.
        This method will copy the value out of the database.
        (Wrapper for `mdbx_get`)

        Parameters:

        * database instance created with calling `openDbi()` on an `Env` instance
        * key for which the value is retrieved
    */
    static NAN_METHOD(getBoolean);

    /*
        Puts string data (JavaScript string type) into a database.
        (Wrapper for `mdbx_put`)

        Parameters:

        * database instance created with calling `openDbi()` on an `Env` instance
        * key for which the value is stored
        * data to store for the given key
    */
    static NAN_METHOD(putString);

    /*
        Puts string data (JavaScript string type) into a database as UTF-8.
        (Wrapper for `mdbx_put`)

        Parameters:

        * database instance created with calling `openDbi()` on an `Env` instance
        * key for which the value is stored
        * data to store for the given key
    */
    static NAN_METHOD(putUtf8);

    /*
        Puts binary data (Node.js Buffer) into a database.
        (Wrapper for `mdbx_put`)

        Parameters:

        * database instance created with calling `openDbi()` on an `Env` instance
        * key for which the value is stored
        * data to store for the given key
    */
    static NAN_METHOD(putBinary);

    /*
        Puts number data (JavaScript number type) into a database.
        (Wrapper for `mdbx_put`)

        Parameters:

        * database instance created with calling `openDbi()` on an `Env` instance
        * key for which the value is stored
        * data to store for the given key
    */
    static NAN_METHOD(putNumber);

    /*
        Puts boolean data (JavaScript boolean type) into a database.
        (Wrapper for `mdbx_put`)

        Parameters:

        * database instance created with calling `openDbi()` on an `Env` instance
        * key for which the value is stored
        * data to store for the given key
    */
    static NAN_METHOD(putBoolean);

    /*
        Deletes data with the given key from the database.
        (Wrapper for `mdbx_del`)

        * database instance created with calling `openDbi()` on an `Env` instance
        * key for which the value is stored
    */
    static NAN_METHOD(del);
};

/*
    `Dbi`
    Represents a database instance in an environment.
    (Wrapper for `MDBX_dbi`)
*/
class DbiWrap : public Nan::ObjectWrap {
public:
    // Tells how keys should be treated
    NodeLmdbxKeyType keyType;
    // Stores flags set when opened
    MDBX_db_flags_t flags;
    // The wrapped object
    MDBX_dbi dbi;
    // Reference to the MDBX_env of the wrapped MDBX_dbi
    MDBX_env *env;
    // The EnvWrap object of the current Dbi
    EnvWrap *ew;
    // Whether the Dbi was opened successfully
    bool isOpen;
    // compression settings and space
    Compression* compression;
    // versions stored in data
    bool hasVersions;
    // current unsafe buffer for this db
    char* lastUnsafePtr;
    void setUnsafeBuffer(char* unsafePtr, const Persistent<Object> &unsafeBuffer);

    friend class TxnWrap;
    friend class CursorWrap;
    friend class EnvWrap;

    DbiWrap(MDBX_env *env, MDBX_dbi dbi);
    ~DbiWrap();

    // Constructor (not exposed)
    static NAN_METHOD(ctor);

    /*
        Closes the database instance.
        Wrapper for `mdbx_dbi_close`)
    */
    static NAN_METHOD(close);

    /*
        Drops the database instance, either deleting it completely (default) or just freeing its pages.

        Parameters:

        * Options object that contains possible configuration options.

        Possible options are:

        * justFreePages - indicates that the database pages need to be freed but the database shouldn't be deleted

    */
    static NAN_METHOD(drop);

    static NAN_METHOD(stat);
};

class Compression : public Nan::ObjectWrap {
public:
    char* dictionary;
    char* decompressBlock;
    char* decompressTarget;
    unsigned int decompressSize;
    unsigned int compressionThreshold;
    Persistent<Object> unsafeBuffer;
    // compression acceleration (defaults to 1)
    int acceleration;
    static thread_local LZ4_stream_t* stream;
    void decompress(MDBX_val& data, bool &isValid);
    argtokey_callback_t compress(MDBX_val* value, argtokey_callback_t freeValue);
    void makeUnsafeBuffer();
    void expand(unsigned int size);
    static NAN_METHOD(ctor);
    Compression();
    ~Compression();
    friend class EnvWrap;
    friend class DbiWrap;
};

/*
    `Cursor`
    Represents a cursor instance that is assigned to a transaction and a database instance
    (Wrapper for `MDBX_cursor`)
*/
class CursorWrap : public Nan::ObjectWrap {

private:

    // The wrapped object
    MDBX_cursor *cursor;
    // Stores how key is represented
    NodeLmdbxKeyType keyType;
    // Key/data pair where the cursor is at
    MDBX_val key, data;
    // Free function for the current key
    argtokey_callback_t freeKey;
    
    DbiWrap *dw;
    TxnWrap *tw;
    
    template<size_t keyIndex, size_t optionsIndex>
    friend argtokey_callback_t cursorArgToKey(CursorWrap* cw, Nan::NAN_METHOD_ARGS_TYPE info, MDBX_val &key, bool &keyIsValid);

public:
    CursorWrap(MDBX_cursor *cursor);
    ~CursorWrap();

    // Sets up exports for the Cursor constructor
    static void setupExports(Local<Object> exports);

    /*
        Opens a new cursor for the specified transaction and database instance.
        (Wrapper for `mdbx_cursor_open`)

        Parameters:

        * Transaction object
        * Database instance object
    */
    static NAN_METHOD(ctor);

    /*
        Closes the cursor.
        (Wrapper for `mdbx_cursor_close`)

        Parameters:

        * Transaction object
        * Database instance object
    */
    static NAN_METHOD(close);

    // Helper method for getters (not exposed)
    static Nan::NAN_METHOD_RETURN_TYPE getCommon(
        Nan::NAN_METHOD_ARGS_TYPE info, MDBX_cursor_op op,
        argtokey_callback_t (*setKey)(CursorWrap* cw, Nan::NAN_METHOD_ARGS_TYPE info, MDBX_val&, bool&),
        void (*setData)(CursorWrap* cw, Nan::NAN_METHOD_ARGS_TYPE info, MDBX_val&),
        void (*freeData)(CursorWrap* cw, Nan::NAN_METHOD_ARGS_TYPE info, MDBX_val&),
        Local<Value> (*convertFunc)(MDBX_val &data));

    // Helper method for getters (not exposed)
    static Nan::NAN_METHOD_RETURN_TYPE getCommon(Nan::NAN_METHOD_ARGS_TYPE info, MDBX_cursor_op op);

    /*
        Gets the current key-data pair that the cursor is pointing to. Returns the current key.
        This method is not zero-copy and the return value will usable as long as there is a reference to it.
        (Wrapper for `mdbx_cursor_get`)

        Parameters:

        * Callback that accepts the key and value
    */
    static NAN_METHOD(getCurrentUtf8);

    /*
        Gets the current key-data pair that the cursor is pointing to. Returns the current key.
        This method is not zero-copy and the return value will usable as long as there is a reference to it.
        (Wrapper for `mdbx_cursor_get`)

        Parameters:

        * Callback that accepts the key and value
    */
    static NAN_METHOD(getCurrentString);

    /*
        Gets the current key-data pair that the cursor is pointing to. Returns the current key.
        This method is zero-copy and the value can only be used until the next put operation or until the transaction is committed or aborted.
        (Wrapper for `mdbx_cursor_get`)

        Parameters:

        * Callback that accepts the key and value
    */
    static NAN_METHOD(getCurrentStringUnsafe);

    /*
        Gets the current key-data pair that the cursor is pointing to. Returns the current key.
        (Wrapper for `mdbx_cursor_get`)

        Parameters:

        * Callback that accepts the key and value
    */
    static NAN_METHOD(getCurrentBinary);


    /*
        Gets the current key-data pair with zero-copy that the cursor is pointing to. Returns the current key.
        This method is zero-copy and the value can only be used until the next put operation or until the transaction is committed or aborted.
        (Wrapper for `mdbx_cursor_get`)

        Parameters:

        * Callback that accepts the key and value
    */
    static NAN_METHOD(getCurrentBinaryUnsafe);

    /*
        Gets the current key-data pair that the cursor is pointing to. Returns the current key.
        (Wrapper for `mdbx_cursor_get`)

        Parameters:

        * Callback that accepts the key and value
    */
    static NAN_METHOD(getCurrentNumber);

    /*
        Gets the current key-data pair that the cursor is pointing to.
        (Wrapper for `mdbx_cursor_get`)

        Parameters:

        * Callback that accepts the key and value
    */
    static NAN_METHOD(getCurrentBoolean);

    /*
    Is the current cursor a database
    (Wrapper for `mdbx_cursor_is_db`)
    */
    static NAN_METHOD(getCurrentIsDatabase);

    /*
        Asks the cursor to go to the first key-data pair in the database.
        (Wrapper for `mdbx_cursor_get`)
    */
    static NAN_METHOD(goToFirst);

    /*
        Asks the cursor to go to the last key-data pair in the database.
        (Wrapper for `mdbx_cursor_get`)
    */
    static NAN_METHOD(goToLast);

    /*
        Asks the cursor to go to the next key-data pair in the database.
        (Wrapper for `mdbx_cursor_get`)
    */
    static NAN_METHOD(goToNext);

    /*
        Asks the cursor to go to the previous key-data pair in the database.
        (Wrapper for `mdbx_cursor_get`)
    */
    static NAN_METHOD(goToPrev);

    /*
        Asks the cursor to go to the specified key in the database.
        (Wrapper for `mdbx_cursor_get`)
    */
    static NAN_METHOD(goToKey);

    /*
        Asks the cursor to go to the first key greater than or equal to the specified parameter in the database.
        (Wrapper for `mdbx_cursor_get`)
    */
    static NAN_METHOD(goToRange);

    /*
        For databases with the dupSort option. Asks the cursor to go to the first occurence of the current key.
        (Wrapper for `mdbx_cursor_get`)
    */
    static NAN_METHOD(goToFirstDup);

    /*
        For databases with the dupSort option. Asks the cursor to go to the last occurence of the current key.
        (Wrapper for `mdbx_cursor_get`)
    */
    static NAN_METHOD(goToLastDup);

    /*
        For databases with the dupSort option. Asks the cursor to go to the next occurence of the current key.
        (Wrapper for `mdbx_cursor_get`)
    */
    static NAN_METHOD(goToNextDup);

    /*
        For databases with the dupSort option. Asks the cursor to go to the previous occurence of the current key.
        (Wrapper for `mdbx_cursor_get`)
    */
    static NAN_METHOD(goToPrevDup);

    /*
        Go to the entry for next key.
        (Wrapper for `mdbx_cursor_get`)
    */
    static NAN_METHOD(goToNextNoDup);

    /*
        Go to the entry for previous key.
        (Wrapper for `mdbx_cursor_get`)
    */
    static NAN_METHOD(goToPrevNoDup);

    /*
        For databases with the dupSort option. Asks the cursor to go to the specified key/data pair.
        (Wrapper for `mdbx_cursor_get`)
    */
    static NAN_METHOD(goToDup);

    /*
        For databases with the dupSort option. Asks the cursor to go to the specified key with the first data that is greater than or equal to the specified.
        (Wrapper for `mdbx_cursor_get`)
    */
    static NAN_METHOD(goToDupRange);

    /*
        Deletes the key/data pair to which the cursor refers.
        (Wrapper for `mdbx_cursor_del`)
    */
    static NAN_METHOD(del);
};

// External string resource that glues MDBX_val and v8::String
class CustomExternalStringResource : public String::ExternalStringResource {
private:
    const uint16_t *d;
    size_t l;

public:
    CustomExternalStringResource(MDBX_val *val);
    ~CustomExternalStringResource();

    void Dispose();
    const uint16_t *data() const;
    size_t length() const;

    static void writeTo(Local<String> str, MDBX_val *val);
};

class CustomExternalOneByteStringResource : public String::ExternalOneByteStringResource {
private:
    const char *d;
    size_t l;

public:
    CustomExternalOneByteStringResource(MDBX_val *val);
    ~CustomExternalOneByteStringResource();

    void Dispose();
    const char *data() const;
    size_t length() const;

};


#endif // NODE_LMDBX_H
