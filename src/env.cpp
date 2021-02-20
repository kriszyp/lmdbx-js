
// This file is part of node-lmdbx, the Node.js binding for lmdbx
// Copyright (c) 2013-2017 Timur Kristóf
// Copyright (c) 2021 Kristopher Tate
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

#include "node-lmdbx.h"

using namespace v8;
using namespace node;

#define IGNORE_NOTFOUND    (1)
thread_local Nan::Persistent<Function>* EnvWrap::txnCtor;
thread_local Nan::Persistent<Function>* EnvWrap::dbiCtor;
//Nan::Persistent<Function> EnvWrap::txnCtor;
//Nan::Persistent<Function> EnvWrap::dbiCtor;
uv_mutex_t* EnvWrap::envsLock = EnvWrap::initMutex();
std::vector<env_path_t> EnvWrap::envs;

uv_mutex_t* EnvWrap::initMutex() {
    uv_mutex_t* mutex = new uv_mutex_t;
    uv_mutex_init(mutex);
    return mutex;
}

EnvWrap::EnvWrap() {
    this->env = nullptr;
    this->currentWriteTxn = nullptr;
}

EnvWrap::~EnvWrap() {
    // Close if not closed already
    if (this->env) {
        this->cleanupStrayTxns();
        mdbx_env_close(env);
    }
    if (this->compression)
        this->compression->Unref();
}

void EnvWrap::cleanupStrayTxns() {
    if (this->currentWriteTxn) {
        mdbx_txn_abort(this->currentWriteTxn->txn);
        this->currentWriteTxn->txn = nullptr;
        this->currentWriteTxn->removeFromEnvWrap();
    }
    while (this->readTxns.size()) {
        TxnWrap *tw = *this->readTxns.begin();
        mdbx_txn_abort(tw->txn);
        tw->removeFromEnvWrap();
        tw->txn = nullptr;
    }
}

NAN_METHOD(EnvWrap::ctor) {
    Nan::HandleScope scope;

    int rc;

    EnvWrap* ew = new EnvWrap();
    rc = mdbx_env_create(&(ew->env));

    if (rc != 0) {
        mdbx_env_close(ew->env);
        return throwLmdbxError(rc);
    }

    ew->Wrap(info.This());
    ew->Ref();

    return info.GetReturnValue().Set(info.This());
}

template<class T>
int applyUint32Setting(int (*f)(MDBX_env *, T), MDBX_env* e, Local<Object> options, T dflt, const char* keyName) {
    int rc;
    const Local<Value> value = options->Get(Nan::GetCurrentContext(), Nan::New<String>(keyName).ToLocalChecked()).ToLocalChecked();
    if (value->IsUint32()) {
        rc = f(e, value->Uint32Value(Nan::GetCurrentContext()).FromJust());
    }
    else {
        rc = f(e, dflt);
    }

    return rc;
}

class SyncWorker : public Nan::AsyncWorker {
  public:
    SyncWorker(MDBX_env* env, Nan::Callback *callback)
      : Nan::AsyncWorker(callback), env(env) {}

    void Execute() {
/*       int rc = mdbx_env_sync(env, 1);
        if (rc != 0) {
            SetErrorMessage(mdbx_strerror(rc));
        }*/
    }

    void HandleOKCallback() {
        Nan::HandleScope scope;
        Local<v8::Value> argv[] = {
            Nan::Null()
        };

        callback->Call(1, argv, async_resource);
    }

  private:
    MDBX_env* env;
};

class CopyWorker : public Nan::AsyncWorker {
  public:
    CopyWorker(MDBX_env* env, char* inPath, int flags, Nan::Callback *callback)
      : Nan::AsyncWorker(callback), env(env), path(strdup(inPath)), flags(flags) {
      }
    ~CopyWorker() {
        free(path);
    }

    void Execute() {
/*        int rc = mdbx_env_copy2(env, path, flags);
        if (rc != 0) {
            fprintf(stderr, "Error on copy code: %u\n", rc);
            SetErrorMessage("Error on copy");
        }*/
    }

    void HandleOKCallback() {
        Nan::HandleScope scope;
        Local<v8::Value> argv[] = {
            Nan::Null()
        };

        callback->Call(1, argv, async_resource);
    }

  private:
    MDBX_env* env;
    char* path;
    int flags;
};

struct condition_t {
    MDBX_val key;
    MDBX_val data;
    MDBX_dbi dbi;
    bool matchSize;
    argtokey_callback_t freeKey;
};

struct action_t {
    int actionType;
    MDBX_val key;
    union {
        struct {
            DbiWrap* dw;
        };
        struct {
            MDBX_val data;
            double ifVersion;
            double version;
            argtokey_callback_t freeValue;
        };
    };
};/*
struct action_t {
    MDBX_val key;
    MDBX_val data;
    DbiWrap* dw;
    condition_t *condition;
    double ifVersion;
    double version;
    argtokey_callback_t freeKey;
    argtokey_callback_t freeValue;
};*/

const int CHANGE_DB = 8;
const int RESET_CONDITION = 9;
const int CONDITION = 1;
const int WRITE_WITH_VALUE = 2;
const int DELETE_OPERATION = 4;

const int FAILED_CONDITION = 1;
const int SUCCESSFUL_OPERATION = 0;
const int BAD_KEY = 3;
const int NOT_FOUND = 2;

class BatchWorker : public Nan::AsyncWorker {
  public:
    BatchWorker(MDBX_env* env, action_t *actions, int actionCount, int putFlags, KeySpace* keySpace, uint8_t* results, Nan::Callback *callback)
      : Nan::AsyncWorker(callback, "node-lmdbx:Batch"),
      env(env),
      actionCount(actionCount),
      results(results),
      actions(actions),
      putFlags(putFlags),
      keySpace(keySpace)
       {
    }

    ~BatchWorker() {
        delete[] actions;
        delete keySpace;
    }

    void Execute() {
        MDBX_txn *txn;
        // we do compression in this thread to offload from main thread, but do it before transaction to minimize time that the transaction is open
        DbiWrap* dw;
        for (int i = 0; i < actionCount; i++) {
            action_t* action = &actions[i];
            int actionType = action->actionType;
            if (actionType == CHANGE_DB)
                dw = action->dw;
            else if (actionType & WRITE_WITH_VALUE) {
                Compression* compression = dw->compression;
                if (compression) {
                    action->freeValue = compression->compress(&action->data, action->freeValue);
                }
            }
        }
        int rc = mdbx_txn_begin(env, nullptr, MDBX_TXN_READWRITE, &txn);
        if (rc != 0) {
            return SetErrorMessage(mdbx_strerror(rc));
        }
        int validatedDepth = 0;
        int conditionDepth = 0;

        for (int i = 0; i < actionCount;) {
            action_t* action = &actions[i];
            int actionType = action->actionType;
            if (actionType >= 8) {
                if (actionType == CHANGE_DB) {
                    // reset target db
                    dw = action->dw;
                } else { // actionType == CHANGE_DB
                    // reset last condition
                    conditionDepth--;
                    if (validatedDepth > conditionDepth)
                        validatedDepth--;
                }
                results[i++] = SUCCESSFUL_OPERATION;
                continue;
            }
            bool validated;
            if (validatedDepth < conditionDepth) {
                // we are in an invalidated branch, just need to track depth
                results[i] = FAILED_CONDITION;
                validated = false;
            } else if (actionType & CONDITION) { // has precondition
                MDBX_val value;
                // TODO: Use a cursor
                rc = mdbx_get(txn, dw->dbi, &action->key, &value);
                if (rc == MDBX_BAD_VALSIZE) {
                    results[i] = BAD_KEY;
                    validated = false;
                } else {
                    if (action->ifVersion == NO_EXIST_VERSION) {
                        validated = rc;
                    }
                    else {
                        if (rc)
                            validated = false;
                        else
                            validated = action->ifVersion == *((double*)value.iov_base);
                    }
                    results[i] = validated ? SUCCESSFUL_OPERATION : FAILED_CONDITION;
                }
                rc = 0;
            } else {
                validated = true;
                results[i] = SUCCESSFUL_OPERATION;
            }
            if (actionType & (WRITE_WITH_VALUE | DELETE_OPERATION)) { // has write operation to perform
                if (validated) {
                    if (actionType & DELETE_OPERATION) {
                        rc = mdbx_del(txn, dw->dbi, &action->key, (actionType & WRITE_WITH_VALUE) ? &action->data : nullptr);
                        if (rc == MDBX_NOTFOUND) {
                            rc = 0; // ignore not_found errors
                            results[i] = NOT_FOUND;
                        }
                    } else {
                        if (dw->hasVersions)
                            rc = putWithVersion(txn, dw->dbi, &action->key, &action->data, putFlags, action->version);
                        else
                            rc = mdbx_put(txn, dw->dbi, &action->key, &action->data, (MDBX_put_flags_t) putFlags);
                    }
                    if (rc != 0) {
                        if (rc == MDBX_BAD_VALSIZE) {
                            results[i] = BAD_KEY;
                            rc = 0;
                        } else {
                            mdbx_txn_abort(txn);
                            return SetErrorMessage(mdbx_strerror(rc));
                        }
                    }
                }
                if (action->freeValue) {
                    action->freeValue(action->data);
                }
            } else {
                // starting condition branch
                conditionDepth++;
                if (validated)
                    validatedDepth++;
            }
            i++;
        }

        rc = mdbx_txn_commit(txn);
        if (rc != 0) {
            if ((putFlags & 1) > 0) // sync mode
                return Nan::ThrowError(mdbx_strerror(rc));
            else
                return SetErrorMessage(mdbx_strerror(rc));
        }
    }

   
    void HandleOKCallback() {
        Nan::HandleScope scope;
        Local<v8::Value> argv[] = {
            Nan::Null()
        };

        callback->Call(1, argv, async_resource);
    }

  private:
    MDBX_env* env;
    int actionCount;
    uint8_t* results;
    int resultIndex = 0;
    bool hasResultsArray = false;
    action_t* actions;
    int putFlags;
    KeySpace* keySpace;
    friend class DbiWrap;
};


NAN_METHOD(EnvWrap::open) {
    Nan::HandleScope scope;

    int rc;
    MDBX_env_flags_t flags = MDBX_ENV_DEFAULTS;

    // Get the wrapper
    EnvWrap *ew = Nan::ObjectWrap::Unwrap<EnvWrap>(info.This());

    if (!ew->env) {
        return Nan::ThrowError("The environment is already closed.");
    }
    ew->compression = nullptr;

    Local<Object> options = Local<Object>::Cast(info[0]);
    Local<String> path = Local<String>::Cast(options->Get(Nan::GetCurrentContext(), Nan::New<String>("path").ToLocalChecked()).ToLocalChecked());
    Nan::Utf8String charPath(path);
    uv_mutex_lock(envsLock);
    for (env_path_t envPath : envs) {
        char* existingPath = envPath.path;
        if (!strcmp(existingPath, *charPath)) {
            envPath.count++;
            mdbx_env_close(ew->env);
            ew->env = envPath.env;
            uv_mutex_unlock(envsLock);
            return;
        }
    }

    // Parse the maxDbs option
    rc = applyUint32Setting<unsigned>(&mdbx_env_set_maxdbs, ew->env, options, 1, "maxDbs");
    if (rc != 0) {
        uv_mutex_unlock(envsLock);
        return throwLmdbxError(rc);
    }

    // Parse the mapSize option
    Local<Value> mapSizeOption = options->Get(Nan::GetCurrentContext(), Nan::New<String>("mapSize").ToLocalChecked()).ToLocalChecked();
    fprintf(stderr, "Checking mapSize\n");
    if (mapSizeOption->IsNumber()) {
        intptr_t mapSizeSizeT = mapSizeOption->NumberValue(Nan::GetCurrentContext()).FromJust();
        fprintf(stderr, "Got mapSize %u\n", mapSizeSizeT > 100);
        intptr_t size_lower = 0x4000;
        intptr_t size_upper = 0x10000000000;
        intptr_t growth_step = 0x1000;
        intptr_t shrink_threshold = 0x2000;
        intptr_t pagesize = 0x1000;
        rc = mdbx_env_set_geometry(ew->env, 0x4000, 0x4000, mapSizeSizeT, 0x1000, 0x2000, 0x1000);
        fprintf(stderr, "Set mapSize %llu\n", mapSizeSizeT);
        if (rc != 0) {
            uv_mutex_unlock(envsLock);
            return throwLmdbxError(rc);
        }
    }
    Local<Value> compressionOption = options->Get(Nan::GetCurrentContext(), Nan::New<String>("compression").ToLocalChecked()).ToLocalChecked();
    if (compressionOption->IsObject()) {
        ew->compression = Nan::ObjectWrap::Unwrap<Compression>(Nan::To<Object>(compressionOption).ToLocalChecked());
        ew->compression->Ref();
    }

    // Parse the maxReaders option
    // NOTE: mdbx.c defines DEFAULT_READERS as 126
    rc = applyUint32Setting<unsigned>(&mdbx_env_set_maxreaders, ew->env, options, 126, "maxReaders");
    if (rc != 0) {
        return throwLmdbxError(rc);
    }

    // NOTE: MDBX_FIXEDMAP is not exposed here since it is "highly experimental" + it is irrelevant for this use case
    // NOTE: MDBX_NOTLS is not exposed here because it is irrelevant for this use case, as node will run all this on a single thread anyway
    setFlagFromValue(&(int)flags, (int)MDBX_NOSUBDIR, "noSubdir", false, options);
    setFlagFromValue(&(int)flags, (int)MDBX_RDONLY, "readOnly", false, options);
    setFlagFromValue(&(int)flags, (int)MDBX_WRITEMAP, "useWritemap", false, options);
    //setFlagFromValue(&flags, MDBX_PREVSNAPSHOT, "usePreviousSnapshot", false, options);
    setFlagFromValue(&(int)flags, (int)MDBX_NOMEMINIT , "noMemInit", false, options);
    setFlagFromValue(&(int)flags, (int)MDBX_NORDAHEAD , "noReadAhead", false, options);
    setFlagFromValue(&(int)flags, (int)MDBX_NOMETASYNC, "noMetaSync", false, options);
    setFlagFromValue(&(int)flags, (int)MDBX_SAFE_NOSYNC, "noSync", false, options);
    setFlagFromValue(&(int)flags, (int)MDBX_MAPASYNC, "mapAsync", false, options);
    //setFlagFromValue(&flags, MDBX_NOLOCK, "unsafeNoLock", false, options);*/

    /*if ((int) flags & (int) MDBX_NOLOCK) {
        fprintf(stderr, "You chose to use MDBX_NOLOCK which is not officially supported by node-lmdbx. You have been warned!\n");
    }*/

    // Set MDBX_NOTLS to enable multiple read-only transactions on the same thread (in this case, the nodejs main thread)
    flags |= MDBX_NOTLS;

    // TODO: make file attributes configurable
    #if NODE_VERSION_AT_LEAST(12,0,0)
    rc = mdbx_env_open(ew->env, *String::Utf8Value(Isolate::GetCurrent(), path), flags, 0664);
    #else
    rc = mdbx_env_open(ew->env, *String::Utf8Value(path), flags, 0664);
    #endif

    if (rc != 0) {
        mdbx_env_close(ew->env);
        uv_mutex_unlock(envsLock);
        ew->env = nullptr;
        return throwLmdbxError(rc);
    }
    env_path_t envPath;
    envPath.path = strdup(*charPath);
    envPath.env = ew->env;
    envPath.count = 1;
    envs.push_back(envPath);
    uv_mutex_unlock(envsLock);
}

NAN_METHOD(EnvWrap::resize) {
    Nan::HandleScope scope;

    // Get the wrapper
    EnvWrap *ew = Nan::ObjectWrap::Unwrap<EnvWrap>(info.This());

    if (!ew->env) {
        return Nan::ThrowError("The environment is already closed.");
    }

    // Check that the correct number/type of arguments was given.
    if (info.Length() != 1 || !info[0]->IsNumber()) {
        return Nan::ThrowError("Call env.resize() with exactly one argument which is a number.");
    }

    // Since this function may only be called if no transactions are active in this process, check this condition.
    if (ew->currentWriteTxn/* || ew->readTxns.size()*/) {
        return Nan::ThrowError("Only call env.resize() when there are no active transactions. Please close all transactions before calling env.resize().");
    }

    size_t mapSizeSizeT = info[0]->IntegerValue(Nan::GetCurrentContext()).FromJust();
    int rc = mdbx_env_set_mapsize(ew->env, mapSizeSizeT);
    if (rc == EINVAL) {
        //fprintf(stderr, "Resize failed, will try to get transaction and try again");
        MDBX_txn *txn;
        rc = mdbx_txn_begin(ew->env, nullptr, MDBX_TXN_READWRITE, &txn);
        if (rc != 0)
            return throwLmdbxError(rc);
        rc = mdbx_txn_commit(txn);
        if (rc != 0)
            return throwLmdbxError(rc);
        rc = mdbx_env_set_mapsize(ew->env, mapSizeSizeT);
    }
    if (rc != 0) {
        return throwLmdbxError(rc);
    }
}

NAN_METHOD(EnvWrap::close) {
    EnvWrap *ew = Nan::ObjectWrap::Unwrap<EnvWrap>(info.This());
    ew->Unref();

    if (!ew->env) {
        return Nan::ThrowError("The environment is already closed.");
    }
    ew->cleanupStrayTxns();

    uv_mutex_lock(envsLock);
    for (auto envPath = envs.begin(); envPath != envs.end(); ) {
        if (envPath->env == ew->env) {
            envPath->count--;
            if (envPath->count <= 0) {
                // last thread using it, we can really close it now
                envs.erase(envPath);
                mdbx_env_close(ew->env);
            }
            break;
        }
        ++envPath;
    }
    uv_mutex_unlock(envsLock);

    ew->env = nullptr;
}

NAN_METHOD(EnvWrap::stat) {
    Nan::HandleScope scope;

    // Get the wrapper
    EnvWrap *ew = Nan::ObjectWrap::Unwrap<EnvWrap>(info.This());
    if (!ew->env) {
        return Nan::ThrowError("The environment is already closed.");
    }

    int rc;
    MDBX_stat stat;

    rc = mdbx_env_stat(ew->env, &stat, sizeof(MDBX_stat));
    if (rc != 0) {
        return throwLmdbxError(rc);
    }

    Local<Context> context = Nan::GetCurrentContext();
    Local<Object> obj = Nan::New<Object>();
    (void)obj->Set(context, Nan::New<String>("pageSize").ToLocalChecked(), Nan::New<Number>(stat.ms_psize));
    (void)obj->Set(context, Nan::New<String>("treeDepth").ToLocalChecked(), Nan::New<Number>(stat.ms_depth));
    (void)obj->Set(context, Nan::New<String>("treeBranchPageCount").ToLocalChecked(), Nan::New<Number>(stat.ms_branch_pages));
    (void)obj->Set(context, Nan::New<String>("treeLeafPageCount").ToLocalChecked(), Nan::New<Number>(stat.ms_leaf_pages));
    (void)obj->Set(context, Nan::New<String>("entryCount").ToLocalChecked(), Nan::New<Number>(stat.ms_entries));
    (void)obj->Set(context, Nan::New<String>("overflowPages").ToLocalChecked(), Nan::New<Number>(stat.ms_overflow_pages));

    info.GetReturnValue().Set(obj);
}

NAN_METHOD(EnvWrap::freeStat) {
    Nan::HandleScope scope;

    // Get the wrapper
    EnvWrap *ew = Nan::ObjectWrap::Unwrap<EnvWrap>(info.This());
    if (!ew->env) {
        return Nan::ThrowError("The environment is already closed.");
    }

    if (info.Length() != 1) {
        return Nan::ThrowError("env.freeStat should be called with a single argument which is a txn.");
    }

    TxnWrap *txn = Nan::ObjectWrap::Unwrap<TxnWrap>(Local<Object>::Cast(info[0]));

    int rc;
    MDBX_stat stat;

    rc = mdbx_dbi_stat(txn->txn, 0, &stat, sizeof(MDBX_stat));
    if (rc != 0) {
        return throwLmdbxError(rc);
    }

    Local<Context> context = Nan::GetCurrentContext();
    Local<Object> obj = Nan::New<Object>();
    (void)obj->Set(context, Nan::New<String>("pageSize").ToLocalChecked(), Nan::New<Number>(stat.ms_psize));
    (void)obj->Set(context, Nan::New<String>("treeDepth").ToLocalChecked(), Nan::New<Number>(stat.ms_depth));
    (void)obj->Set(context, Nan::New<String>("treeBranchPageCount").ToLocalChecked(), Nan::New<Number>(stat.ms_branch_pages));
    (void)obj->Set(context, Nan::New<String>("treeLeafPageCount").ToLocalChecked(), Nan::New<Number>(stat.ms_leaf_pages));
    (void)obj->Set(context, Nan::New<String>("entryCount").ToLocalChecked(), Nan::New<Number>(stat.ms_entries));
    (void)obj->Set(context, Nan::New<String>("overflowPages").ToLocalChecked(), Nan::New<Number>(stat.ms_overflow_pages));

    info.GetReturnValue().Set(obj);
}

NAN_METHOD(EnvWrap::info) {
    Nan::HandleScope scope;

    // Get the wrapper
    EnvWrap *ew = Nan::ObjectWrap::Unwrap<EnvWrap>(info.This());
    if (!ew->env) {
        return Nan::ThrowError("The environment is already closed.");
    }

    int rc;
    MDBX_envinfo envinfo;

    rc = mdbx_env_info(ew->env, &envinfo, sizeof(MDBX_envinfo));
    if (rc != 0) {
        return throwLmdbxError(rc);
    }

    Local<Context> context = Nan::GetCurrentContext();
    Local<Object> obj = Nan::New<Object>();
//    (void)obj->Set(context, Nan::New<String>("mapAddress").ToLocalChecked(), Nan::New<Number>((uint64_t) envinfo.mi_mapaddr));
    (void)obj->Set(context, Nan::New<String>("mapSize").ToLocalChecked(), Nan::New<Number>(envinfo.mi_mapsize));
    (void)obj->Set(context, Nan::New<String>("lastPageNumber").ToLocalChecked(), Nan::New<Number>(envinfo.mi_last_pgno));
    //(void)obj->Set(context, Nan::New<String>("lastTxnId").ToLocalChecked(), Nan::New<Number>(envinfo.mi_last_txnid));
    (void)obj->Set(context, Nan::New<String>("maxReaders").ToLocalChecked(), Nan::New<Number>(envinfo.mi_maxreaders));
    (void)obj->Set(context, Nan::New<String>("numReaders").ToLocalChecked(), Nan::New<Number>(envinfo.mi_numreaders));

    info.GetReturnValue().Set(obj);
}

NAN_METHOD(EnvWrap::copy) {
    /*Nan::HandleScope scope;

    // Get the wrapper
    EnvWrap *ew = Nan::ObjectWrap::Unwrap<EnvWrap>(info.This());

    if (!ew->env) {
        return Nan::ThrowError("The environment is already closed.");
    }

    // Check that the correct number/type of arguments was given.
    if (!info[0]->IsString()) {
        return Nan::ThrowError("Call env.copy(path, compact?, callback) with a file path.");
    }
    if (!info[info.Length() - 1]->IsFunction()) {
        return Nan::ThrowError("Call env.copy(path, compact?, callback) with a file path.");
    }
    Nan::Utf8String path(info[0].As<String>());

    int flags = 0;
    if (info.Length() > 1 && info[1]->IsTrue()) {
        flags = MDBX_CP_COMPACT;
    }

    Nan::Callback* callback = new Nan::Callback(
      Local<v8::Function>::Cast(info[info.Length()  > 2 ? 2 : 1])
    );

    CopyWorker* worker = new CopyWorker(
      ew->env, *path, flags, callback
    );

    Nan::AsyncQueueWorker(worker);*/
}

NAN_METHOD(EnvWrap::detachBuffer) {
    Nan::HandleScope scope;
    #if NODE_VERSION_AT_LEAST(12,0,0)
    Local<v8::ArrayBuffer>::Cast(info[0])->Detach();
    #endif
}

NAN_METHOD(EnvWrap::beginTxn) {
    Nan::HandleScope scope;

    const int argc = 2;

    Local<Value> argv[argc] = { info.This(), info[0] };
    Nan::MaybeLocal<Object> maybeInstance = Nan::NewInstance(Nan::New(*txnCtor), argc, argv);

    // Check if txn could be created
    if ((maybeInstance.IsEmpty())) {
        // The maybeInstance is empty because the txnCtor called Nan::ThrowError.
        // No need to call that here again, the user will get the error thrown there.
        return;
    }

    Local<Object> instance = maybeInstance.ToLocalChecked();
    info.GetReturnValue().Set(instance);
}

NAN_METHOD(EnvWrap::openDbi) {
    Nan::HandleScope scope;

    const unsigned argc = 2;
    Local<Value> argv[argc] = { info.This(), info[0] };
    Nan::MaybeLocal<Object> maybeInstance = Nan::NewInstance(Nan::New(*dbiCtor), argc, argv);

    // Check if database could be opened
    if ((maybeInstance.IsEmpty())) {
        // The maybeInstance is empty because the dbiCtor called Nan::ThrowError.
        // No need to call that here again, the user will get the error thrown there.
        return;
    }

    Local<Object> instance = maybeInstance.ToLocalChecked();
    info.GetReturnValue().Set(instance);
}

NAN_METHOD(EnvWrap::sync) {
    Nan::HandleScope scope;

    EnvWrap *ew = Nan::ObjectWrap::Unwrap<EnvWrap>(info.This());

    if (!ew->env) {
        return Nan::ThrowError("The environment is already closed.");
    }

    Nan::Callback* callback = new Nan::Callback(
      Local<v8::Function>::Cast(info[0])
    );

    SyncWorker* worker = new SyncWorker(
      ew->env, callback
    );

    Nan::AsyncQueueWorker(worker);
    return;
}


NAN_METHOD(EnvWrap::batchWrite) {
    Nan::HandleScope scope;

    EnvWrap *ew = Nan::ObjectWrap::Unwrap<EnvWrap>(info.This());
    Local<Context> context = Nan::GetCurrentContext();

    if (!ew->env) {
        return Nan::ThrowError("The environment is already closed.");
    }
    Local<v8::Array> array = Local<v8::Array>::Cast(info[0]);

    int length = array->Length();
    action_t* actions = new action_t[length];

    MDBX_put_flags_t putFlags = MDBX_UPSERT;
    KeySpace* keySpace = new KeySpace(false);
    Nan::Callback* callback;
    uint8_t* results = (uint8_t*) node::Buffer::Data(Local<Object>::Cast(info[1]));
    Local<Value> options = info[2];

    if (!info[2]->IsNull() && !info[2]->IsUndefined() && info[2]->IsObject() && !info[2]->IsFunction()) {
        Local<Object> optionsObject = Local<Object>::Cast(options);
        setFlagFromValue(&(int) putFlags, (int) MDBX_NODUPDATA, "noDupData", false, optionsObject);
        setFlagFromValue(&(int) putFlags, (int) MDBX_NOOVERWRITE, "noOverwrite", false, optionsObject);
        setFlagFromValue(&(int) putFlags, (int) MDBX_APPEND, "append", false, optionsObject);
        setFlagFromValue(&(int) putFlags, (int) MDBX_APPENDDUP, "appendDup", false, optionsObject);
        callback = new Nan::Callback(
            Local<v8::Function>::Cast(info[3])
        );
    } else {
        if (info.Length() > 2 || info[0]->IsFunction())
            callback = new Nan::Callback(
                Local<v8::Function>::Cast(info[2])
            );
        else {
            // sync mode
            //putFlags &= ;
            callback = nullptr;
        }
    }

    BatchWorker* worker = new BatchWorker(
        ew->env, actions, length, putFlags, keySpace, results, callback
    );
    bool keyIsValid = false;
    NodeLmdbxKeyType keyType;
    DbiWrap* dw;

    for (unsigned int i = 0; i < array->Length(); i++) {
        //Local<Value> element = array->Get(context, i).ToLocalChecked(); // checked/enforce in js
        //if (!element->IsObject())
          //  continue;
        action_t* action = &actions[i];
        Local<Value> operationValue = array->Get(context, i).ToLocalChecked();

        bool isArray = operationValue->IsArray();
        if (!isArray) {
            // change target db
            if (operationValue->IsObject()) {
                dw = action->dw = Nan::ObjectWrap::Unwrap<DbiWrap>(Local<Object>::Cast(operationValue));
                action->actionType = CHANGE_DB;
            } else {
                // reset condition
                action->actionType = RESET_CONDITION;
            }
            continue;
            // if we did not coordinate to always reference the object on the JS side, we would need this (but it is expensive):
            // worker->SaveToPersistent(persistedIndex++, currentDb);
        }
        Local<Object> operation = Local<Object>::Cast(operationValue);
        Local<v8::Value> key = operation->Get(context, 0).ToLocalChecked();
        
        if (!keyIsValid) {
            // just execute this the first time so we didn't need to re-execute for each iteration
            keyType = keyTypeFromOptions(options, dw->keyType);
        }
        if (keyType == NodeLmdbxKeyType::DefaultKey) {
            keyIsValid = valueToMDBXKey(key, action->key, *keySpace);
        }
        else {
            argToKey(key, action->key, keyType, keyIsValid);
            if (!keyIsValid) {
                // argToKey already threw an error
                delete worker;
                return;
            }
        }
        // if we did not coordinate to always reference the object on the JS side, we would need this (but it is expensive):
        //if (!action->freeKey)
          //  worker->SaveToPersistent(persistedIndex++, key);
        Local<v8::Value> value = operation->Get(context, 1).ToLocalChecked();

        if (dw->hasVersions) {
            if (value->IsNumber()) {
                action->actionType = CONDITION; // checking version action type
                action->ifVersion = Nan::To<v8::Number>(value).ToLocalChecked()->Value();
                continue;
            }
            action->actionType = CONDITION | WRITE_WITH_VALUE; // conditional save value
            // TODO: Check length before continuing?
            double version = 0;
            Local<v8::Value> versionValue = operation->Get(context, 2).ToLocalChecked();
            if (versionValue->IsNumber())
                version = Nan::To<v8::Number>(versionValue).ToLocalChecked()->Value();
            action->version = version;

            versionValue = operation->Get(context, 3).ToLocalChecked();
            if (versionValue->IsNumber())
                version = Nan::To<v8::Number>(versionValue).ToLocalChecked()->Value();
            else if (versionValue->IsNull())
                version = NO_EXIST_VERSION;
            else
                action->actionType = WRITE_WITH_VALUE;
            action->ifVersion = version;
        } else {
            Local<v8::Value> deleteValue = operation->Get(context, 2).ToLocalChecked();
            if (deleteValue->IsTrue()) // useful for dupsort so we can specify a specfic value to delete
                action->actionType = DELETE_OPERATION | WRITE_WITH_VALUE;
            else
                action->actionType = WRITE_WITH_VALUE;
        }

        if (value->IsNull() || value->IsUndefined()) {
            // standard delete (no regard for value)
            action->actionType = DELETE_OPERATION | (action->actionType & CONDITION); // only DELETE_OPERATION, no WRITE_WITH_VALUE
            action->freeValue = nullptr;
        } else if (value->IsArrayBufferView()) {
            action->data.iov_len = node::Buffer::Length(value);
            action->data.iov_base = node::Buffer::Data(value);
            action->freeValue = nullptr; // don't free, belongs to node
            //worker->SaveToPersistent(persistedIndex++, value); // this is coordinated to always be referenced on the JS side
        } else {
            writeValueToEntry(Nan::To<v8::String>(value).ToLocalChecked(), &action->data);
            action->freeValue = ([](MDBX_val &value) -> void {
                delete[] (char*)value.iov_base;
            });
        }
    }

    //worker->SaveToPersistent("env", info.This()); // this is coordinated to always be referenced on the JS side
    if (callback) {
        Nan::AsyncQueueWorker(worker);
    } else {
        // sync mode
        worker->Execute();
        delete worker;
    }
    return;
}



void EnvWrap::setupExports(Local<Object> exports) {
    // EnvWrap: Prepare constructor template
    Local<FunctionTemplate> envTpl = Nan::New<FunctionTemplate>(EnvWrap::ctor);
    envTpl->SetClassName(Nan::New<String>("Env").ToLocalChecked());
    envTpl->InstanceTemplate()->SetInternalFieldCount(1);
    // EnvWrap: Add functions to the prototype
    Isolate *isolate = Isolate::GetCurrent();
    envTpl->PrototypeTemplate()->Set(isolate, "open", Nan::New<FunctionTemplate>(EnvWrap::open));
    envTpl->PrototypeTemplate()->Set(isolate, "close", Nan::New<FunctionTemplate>(EnvWrap::close));
    envTpl->PrototypeTemplate()->Set(isolate, "beginTxn", Nan::New<FunctionTemplate>(EnvWrap::beginTxn));
    envTpl->PrototypeTemplate()->Set(isolate, "openDbi", Nan::New<FunctionTemplate>(EnvWrap::openDbi));
    envTpl->PrototypeTemplate()->Set(isolate, "sync", Nan::New<FunctionTemplate>(EnvWrap::sync));
    envTpl->PrototypeTemplate()->Set(isolate, "batchWrite", Nan::New<FunctionTemplate>(EnvWrap::batchWrite));
    envTpl->PrototypeTemplate()->Set(isolate, "stat", Nan::New<FunctionTemplate>(EnvWrap::stat));
    envTpl->PrototypeTemplate()->Set(isolate, "freeStat", Nan::New<FunctionTemplate>(EnvWrap::freeStat));
    envTpl->PrototypeTemplate()->Set(isolate, "info", Nan::New<FunctionTemplate>(EnvWrap::info));
    envTpl->PrototypeTemplate()->Set(isolate, "resize", Nan::New<FunctionTemplate>(EnvWrap::resize));
    envTpl->PrototypeTemplate()->Set(isolate, "copy", Nan::New<FunctionTemplate>(EnvWrap::copy));
    envTpl->PrototypeTemplate()->Set(isolate, "detachBuffer", Nan::New<FunctionTemplate>(EnvWrap::detachBuffer));

    // TxnWrap: Prepare constructor template
    Local<FunctionTemplate> txnTpl = Nan::New<FunctionTemplate>(TxnWrap::ctor);
    txnTpl->SetClassName(Nan::New<String>("Txn").ToLocalChecked());
    txnTpl->InstanceTemplate()->SetInternalFieldCount(1);
    // TxnWrap: Add functions to the prototype
    txnTpl->PrototypeTemplate()->Set(isolate, "commit", Nan::New<FunctionTemplate>(TxnWrap::commit));
    txnTpl->PrototypeTemplate()->Set(isolate, "abort", Nan::New<FunctionTemplate>(TxnWrap::abort));
    txnTpl->PrototypeTemplate()->Set(isolate, "getString", Nan::New<FunctionTemplate>(TxnWrap::getString));
    txnTpl->PrototypeTemplate()->Set(isolate, "getStringUnsafe", Nan::New<FunctionTemplate>(TxnWrap::getStringUnsafe));
    txnTpl->PrototypeTemplate()->Set(isolate, "getUtf8", Nan::New<FunctionTemplate>(TxnWrap::getUtf8));
    txnTpl->PrototypeTemplate()->Set(isolate, "getBinary", Nan::New<FunctionTemplate>(TxnWrap::getBinary));
    txnTpl->PrototypeTemplate()->Set(isolate, "getBinaryUnsafe", Nan::New<FunctionTemplate>(TxnWrap::getBinaryUnsafe));
    txnTpl->PrototypeTemplate()->Set(isolate, "getNumber", Nan::New<FunctionTemplate>(TxnWrap::getNumber));
    txnTpl->PrototypeTemplate()->Set(isolate, "getBoolean", Nan::New<FunctionTemplate>(TxnWrap::getBoolean));
    txnTpl->PrototypeTemplate()->Set(isolate, "putString", Nan::New<FunctionTemplate>(TxnWrap::putString));
    txnTpl->PrototypeTemplate()->Set(isolate, "putBinary", Nan::New<FunctionTemplate>(TxnWrap::putBinary));
    txnTpl->PrototypeTemplate()->Set(isolate, "putNumber", Nan::New<FunctionTemplate>(TxnWrap::putNumber));
    txnTpl->PrototypeTemplate()->Set(isolate, "putBoolean", Nan::New<FunctionTemplate>(TxnWrap::putBoolean));
    txnTpl->PrototypeTemplate()->Set(isolate, "putUtf8", Nan::New<FunctionTemplate>(TxnWrap::putUtf8));
    txnTpl->PrototypeTemplate()->Set(isolate, "del", Nan::New<FunctionTemplate>(TxnWrap::del));
    txnTpl->PrototypeTemplate()->Set(isolate, "reset", Nan::New<FunctionTemplate>(TxnWrap::reset));
    txnTpl->PrototypeTemplate()->Set(isolate, "renew", Nan::New<FunctionTemplate>(TxnWrap::renew));
    // TODO: wrap mdbx_cmp too
    // TODO: wrap mdbx_dcmp too
    // TxnWrap: Get constructor
    EnvWrap::txnCtor = new Nan::Persistent<Function>();
    EnvWrap::txnCtor->Reset( txnTpl->GetFunction(Nan::GetCurrentContext()).ToLocalChecked());

    // DbiWrap: Prepare constructor template
    Local<FunctionTemplate> dbiTpl = Nan::New<FunctionTemplate>(DbiWrap::ctor);
    dbiTpl->SetClassName(Nan::New<String>("Dbi").ToLocalChecked());
    dbiTpl->InstanceTemplate()->SetInternalFieldCount(1);
    // DbiWrap: Add functions to the prototype
    dbiTpl->PrototypeTemplate()->Set(isolate, "close", Nan::New<FunctionTemplate>(DbiWrap::close));
    dbiTpl->PrototypeTemplate()->Set(isolate, "drop", Nan::New<FunctionTemplate>(DbiWrap::drop));
    dbiTpl->PrototypeTemplate()->Set(isolate, "stat", Nan::New<FunctionTemplate>(DbiWrap::stat));
    // TODO: wrap mdbx_stat too
    // DbiWrap: Get constructor
    EnvWrap::dbiCtor = new Nan::Persistent<Function>();
    EnvWrap::dbiCtor->Reset( dbiTpl->GetFunction(Nan::GetCurrentContext()).ToLocalChecked());

    Local<FunctionTemplate> compressionTpl = Nan::New<FunctionTemplate>(Compression::ctor);
    compressionTpl->SetClassName(Nan::New<String>("Compression").ToLocalChecked());
    compressionTpl->InstanceTemplate()->SetInternalFieldCount(1);
    (void)exports->Set(Nan::GetCurrentContext(), Nan::New<String>("Compression").ToLocalChecked(), compressionTpl->GetFunction(Nan::GetCurrentContext()).ToLocalChecked());

    // Set exports
    (void)exports->Set(Nan::GetCurrentContext(), Nan::New<String>("Env").ToLocalChecked(), envTpl->GetFunction(Nan::GetCurrentContext()).ToLocalChecked());
}
