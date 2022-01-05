#include "lmdbx-js.h"
using namespace v8;
using namespace node;

#define IGNORE_NOTFOUND    (1)
thread_local Nan::Persistent<Function>* EnvWrap::txnCtor;
thread_local Nan::Persistent<Function>* EnvWrap::dbiCtor;

pthread_mutex_t* EnvWrap::envsLock = EnvWrap::initMutex();
std::vector<env_path_t> EnvWrap::envs;

pthread_mutex_t* EnvWrap::initMutex() {
    pthread_mutex_t* mutex = new pthread_mutex_t;
    pthread_mutex_init(mutex, nullptr);
    return mutex;
}

EnvWrap::EnvWrap() {
    this->env = nullptr;
    this->currentWriteTxn = nullptr;
	this->currentReadTxn = nullptr;
    this->writeTxn = nullptr;
    this->writeWorker = nullptr;
	this->readTxnRenewed = false;
    this->writingLock = new pthread_mutex_t;
    this->writingCond = new pthread_cond_t;
    pthread_mutex_init(this->writingLock, nullptr);
    pthread_cond_init(this->writingCond, nullptr);
}

EnvWrap::~EnvWrap() {
    // Close if not closed already
    if (this->env) {
        this->cleanupStrayTxns();
        mdbx_env_close(env);
    }
    if (this->compression)
        this->compression->Unref();
    pthread_mutex_destroy(this->writingLock);
    pthread_cond_destroy(this->writingCond);
    
}

void EnvWrap::cleanupStrayTxns() {
    if (this->currentWriteTxn) {
        mdbx_txn_abort(this->currentWriteTxn->txn);
        this->currentWriteTxn->removeFromEnvWrap();
    }
    while (this->readTxns.size()) {
        TxnWrap *tw = *this->readTxns.begin();
        mdbx_txn_abort(tw->txn);
        tw->removeFromEnvWrap();
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
MDBX_txn* EnvWrap::getReadTxn() {
    MDBX_txn* txn = writeTxn ? writeTxn->txn : nullptr;
    if (txn)
        return txn;
    txn = currentReadTxn;
    if (readTxnRenewed)
        return txn;
    if (txn)
        mdbx_txn_renew(txn);
    else {
        Nan::ThrowError("No current read transaction available");
        return nullptr;
    }
    readTxnRenewed = true;
    return txn;
}

void cleanup(void* data) {
    ((EnvWrap*) data)->closeEnv();
}

NAN_METHOD(EnvWrap::open) {
    Nan::HandleScope scope;

    int rc;
    // Get the wrapper
    EnvWrap *ew = Nan::ObjectWrap::Unwrap<EnvWrap>(info.This());

    if (!ew->env) {
        return Nan::ThrowError("The environment is already closed.");
    }
    Local<Object> options = Local<Object>::Cast(info[0]);
    Local<Number> flagsValue = Local<Number>::Cast(info[1]);
    int flags = flagsValue->IntegerValue(Nan::GetCurrentContext()).FromJust();
    Local<Number> jsFlagsValue = Local<Number>::Cast(info[2]);
    int jsFlags = jsFlagsValue->IntegerValue(Nan::GetCurrentContext()).FromJust();

    Compression* compression = nullptr;
    Local<Value> compressionOption = options->Get(Nan::GetCurrentContext(), Nan::New<String>("compression").ToLocalChecked()).ToLocalChecked();
    if (compressionOption->IsObject()) {
        compression = ew->compression = Nan::ObjectWrap::Unwrap<Compression>(Nan::To<Object>(compressionOption).ToLocalChecked());
        ew->compression->Ref();
    }
    char* keyBuffer;
    Local<Value> keyBytesValue = options->Get(Nan::GetCurrentContext(), Nan::New<String>("keyBytes").ToLocalChecked()).ToLocalChecked();
    if (!keyBytesValue->IsArrayBufferView())
        fprintf(stderr, "Invalid key buffer\n");
    keyBuffer = node::Buffer::Data(keyBytesValue);
    Local<String> path = Local<String>::Cast(options->Get(Nan::GetCurrentContext(), Nan::New<String>("path").ToLocalChecked()).ToLocalChecked());
    int pathLength = path->Length();
    uint8_t* pathBytes = new uint8_t[pathLength + 1];
    int bytes = path->WriteOneByte(Isolate::GetCurrent(), pathBytes, 0, pathLength + 1, v8::String::WriteOptions::NO_OPTIONS);
    if (bytes != pathLength)
        fprintf(stderr, "Bytes do not match %u %u", bytes, pathLength);
    if (pathBytes[bytes])
        fprintf(stderr, "String is not null-terminated");
    // Parse the maxDbs option
    int maxDbs = 12;
    Local<Value> option = options->Get(Nan::GetCurrentContext(), Nan::New<String>("maxDbs").ToLocalChecked()).ToLocalChecked();
    if (option->IsNumber())
        maxDbs = option->IntegerValue(Nan::GetCurrentContext()).FromJust();


    size_t mapSize = 0;
    // Parse the mapSize option
    option = options->Get(Nan::GetCurrentContext(), Nan::New<String>("mapSize").ToLocalChecked()).ToLocalChecked();
    if (option->IsNumber())
        mapSize = option->IntegerValue(Nan::GetCurrentContext()).FromJust();
    int pageSize = 4096;
    // Parse the mapSize option
    option = options->Get(Nan::GetCurrentContext(), Nan::New<String>("pageSize").ToLocalChecked()).ToLocalChecked();
    if (option->IsNumber())
        pageSize = option->IntegerValue(Nan::GetCurrentContext()).FromJust();
    int maxReaders = 126;
    // Parse the mapSize option
    option = options->Get(Nan::GetCurrentContext(), Nan::New<String>("maxReaders").ToLocalChecked()).ToLocalChecked();
    if (option->IsNumber())
        maxReaders = option->IntegerValue(Nan::GetCurrentContext()).FromJust();

    rc = ew->openEnv(flags, jsFlags, (const char*)pathBytes, keyBuffer, compression, maxDbs, maxReaders, mapSize, pageSize);
    delete pathBytes;
    if (rc < 0)
        return throwLmdbxError(rc);
    node::AddEnvironmentCleanupHook(Isolate::GetCurrent(), cleanup, ew);
    return info.GetReturnValue().Set(Nan::New<Number>(rc));
}
int EnvWrap::openEnv(int flags, int jsFlags, const char* path, char* keyBuffer, Compression* compression, int maxDbs,
        int maxReaders, size_t mapSize, int pageSize) {
    pthread_mutex_lock(envsLock);
    this->keyBuffer = keyBuffer;
    this->compression = compression;
    this->jsFlags = jsFlags;
    MDBX_env* env = this->env;
    for (auto envPath = envs.begin(); envPath != envs.end();) {
        char* existingPath = envPath->path;
        if (!strcmp(existingPath, path)) {
            envPath->count++;
            mdbx_env_close(env);
            this->env = envPath->env;
            pthread_mutex_unlock(envsLock);
            return 0;
        }
        ++envPath;
    }
    int rc;
    rc = mdbx_env_set_maxdbs(env, maxDbs);
    if (rc) goto fail;
    rc = mdbx_env_set_maxreaders(env, maxReaders);
    if (rc) goto fail;
    rc = mdbx_env_set_geometry(env, -1, -1, mapSize, -1, -1, pageSize);
    if (rc) goto fail;

    // Set MDBX_NOTLS to enable multiple read-only transactions on the same thread (in this case, the nodejs main thread)
    flags |= (int) MDBX_NOTLS;
    // TODO: make file attributes configurable
    // *String::Utf8Value(Isolate::GetCurrent(), path)
    rc = mdbx_env_open(env, path, (MDBX_env_flags_t) flags, 0664);
    mdbx_env_get_flags(env, (unsigned int*) &flags);

    if (rc != 0) {
        mdbx_env_close(env);
        goto fail;
    }
    env_path_t envPath;
    envPath.path = strdup(path);
    envPath.env = env;
    envPath.count = 1;
    envs.push_back(envPath);
    pthread_mutex_unlock(envsLock);
    return 0;

    fail:
    pthread_mutex_unlock(envsLock);
    this->env = nullptr;
    return rc;
}
NAN_METHOD(EnvWrap::getMaxKeySize) {
    EnvWrap *ew = Nan::ObjectWrap::Unwrap<EnvWrap>(info.This());
    return info.GetReturnValue().Set(Nan::New<Number>(mdbx_env_get_maxkeysize(ew->env)));
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

void EnvWrap::closeEnv() {
    if (!env)
        return;
    node::RemoveEnvironmentCleanupHook(Isolate::GetCurrent(), cleanup, this);
    cleanupStrayTxns();
    pthread_mutex_lock(envsLock);
    for (auto envPath = envs.begin(); envPath != envs.end(); ) {
        if (envPath->env == env) {
            envPath->count--;
            if (envPath->count <= 0) {
                // last thread using it, we can really close it now
                envs.erase(envPath);
                mdbx_env_close(env);
            }
            break;
        }
        ++envPath;
    }
    pthread_mutex_unlock(envsLock);

    env = nullptr;
}
extern "C" EXTERN void closeEnv(double ewPointer) {
    EnvWrap* ew = (EnvWrap*) (size_t) ewPointer;
    ew->closeEnv();
}

NAN_METHOD(EnvWrap::close) {
    EnvWrap *ew = Nan::ObjectWrap::Unwrap<EnvWrap>(info.This());
    ew->Unref();

    if (!ew->env) {
        return Nan::ThrowError("The environment is already closed.");
    }
    ew->closeEnv();
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

NAN_METHOD(EnvWrap::readerCheck) {
    Nan::HandleScope scope;

    // Get the wrapper
    EnvWrap *ew = Nan::ObjectWrap::Unwrap<EnvWrap>(info.This());
    if (!ew->env) {
        return Nan::ThrowError("The environment is already closed.");
    }

    int rc, dead;
    rc = mdbx_reader_check(ew->env, &dead);
    if (rc != 0) {
        return throwLmdbxError(rc);
    }

    info.GetReturnValue().Set(Nan::New<Number>(dead));
}

/*Local<Array> readerStrings;
MDBX_msg_func* printReaders = ([](const char* message, void* ctx) -> int {
    readerStrings->Set(Nan::GetCurrentContext(), readerStrings->Length(), Nan::New<String>(message).ToLocalChecked()).ToChecked();
    return 0;
});*/

NAN_METHOD(EnvWrap::readerList) {
    /*Nan::HandleScope scope;

    // Get the wrapper
    EnvWrap* ew = Nan::ObjectWrap::Unwrap<EnvWrap>(info.This());
    if (!ew->env) {
        return Nan::ThrowError("The environment is already closed.");
    }

    readerStrings = Nan::New<Array>(0);
    int rc;
    rc = mdbx_reader_list(ew->env, printReaders, nullptr);
    if (rc != 0) {
        return throwLmdbxError(rc);
    }
    info.GetReturnValue().Set(readerStrings);*/
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

    Nan::MaybeLocal<Object> maybeInstance;

    int flags = info[0]->IntegerValue(Nan::GetCurrentContext()).FromJust();
    if (!(flags & (int) MDBX_TXN_RDONLY)) {
        EnvWrap *ew = Nan::ObjectWrap::Unwrap<EnvWrap>(info.This());
        MDBX_env *env = ew->env;
        unsigned int envFlags;
        mdbx_env_get_flags(env, &envFlags);
        MDBX_txn *txn;

        if (ew->writeTxn)
            txn = ew->writeTxn->txn;
        else if (ew->writeWorker) {
            // try to acquire the txn from the current batch
            txn = ew->writeWorker->AcquireTxn(&flags);
            //fprintf(stderr, "acquired %p %p %p\n", ew->writeWorker, txn, flags);
        } else {
            pthread_mutex_lock(ew->writingLock);
            txn = nullptr;
        }

        if (txn) {
            if (flags & TXN_ABORTABLE) {
                if (envFlags & MDBX_WRITEMAP)
                    flags &= ~TXN_ABORTABLE;
                else {
                    // child txn
                    mdbx_txn_begin(env, txn, (MDBX_txn_flags_t)(flags & 0xf0000), &txn);
                    TxnTracked* childTxn = new TxnTracked(txn, flags);
                    childTxn->parent = ew->writeTxn;
                    ew->writeTxn = childTxn;
                    return;
                }
            }
        } else {
            mdbx_txn_begin(env, nullptr, (MDBX_txn_flags_t)(flags & 0xf0000), &txn);
            flags |= TXN_ABORTABLE;
        }
        ew->writeTxn = new TxnTracked(txn, flags);
        return;
    }

    if (info.Length() > 1) {
        const int argc = 3;

        Local<Value> argv[argc] = { info.This(), info[0], info[1] };
        maybeInstance = Nan::NewInstance(Nan::New(*txnCtor), argc, argv);

    } else {
        const int argc = 2;

        Local<Value> argv[argc] = { info.This(), info[0] };
        //fprintf(stdout, "beginTxn %u", info[0]->IsTrue());
        maybeInstance = Nan::NewInstance(Nan::New(*txnCtor), argc, argv);
    }

    // Check if txn could be created
    if ((maybeInstance.IsEmpty())) {
        // The maybeInstance is empty because the txnCtor called Nan::ThrowError.
        // No need to call that here again, the user will get the error thrown there.
        return;
    }

    Local<Object> instance = maybeInstance.ToLocalChecked();
    info.GetReturnValue().Set(instance);
}
NAN_METHOD(EnvWrap::commitTxn) {
    EnvWrap *ew = Nan::ObjectWrap::Unwrap<EnvWrap>(info.This());
    TxnTracked *currentTxn = ew->writeTxn;
    //fprintf(stderr, "commitTxn %p\n", currentTxn);
    int rc = 0;
    if (currentTxn->flags & TXN_ABORTABLE) {
        //fprintf(stderr, "txn_commit\n");
        rc = mdbx_txn_commit(currentTxn->txn);
    }
    ew->writeTxn = currentTxn->parent;
    if (!ew->writeTxn) {
        //fprintf(stderr, "unlock txn\n");
        if (ew->writeWorker)
            ew->writeWorker->UnlockTxn();
        else
            pthread_mutex_unlock(ew->writingLock);
    }
    delete currentTxn;
    if (rc)
        throwLmdbxError(rc);
}
NAN_METHOD(EnvWrap::abortTxn) {
    EnvWrap *ew = Nan::ObjectWrap::Unwrap<EnvWrap>(info.This());
    TxnTracked *currentTxn = ew->writeTxn;
    if (currentTxn->flags & TXN_ABORTABLE) {
        mdbx_txn_abort(currentTxn->txn);
    } else {
        Nan::ThrowError("Can not abort this transaction");
    }
    ew->writeTxn = currentTxn->parent;
    if (!ew->writeTxn) {
        if (ew->writeWorker)
            ew->writeWorker->UnlockTxn();
        else
            pthread_mutex_unlock(ew->writingLock);
    }
    delete currentTxn;
}
extern "C" EXTERN int commitEnvTxn(double ewPointer) {
    EnvWrap* ew = (EnvWrap*) (size_t) ewPointer;
    TxnTracked *currentTxn = ew->writeTxn;
    int rc = 0;
    if (currentTxn->flags & TXN_ABORTABLE) {
        //fprintf(stderr, "txn_commit\n");
        rc = mdbx_txn_commit(currentTxn->txn);
    }
    ew->writeTxn = currentTxn->parent;
    if (!ew->writeTxn) {
        //fprintf(stderr, "unlock txn\n");
        if (ew->writeWorker)
            ew->writeWorker->UnlockTxn();
        else
            pthread_mutex_unlock(ew->writingLock);
    }
    delete currentTxn;
    return rc;
}
extern "C" EXTERN void abortEnvTxn(double ewPointer) {
    EnvWrap* ew = (EnvWrap*) (size_t) ewPointer;
    TxnTracked *currentTxn = ew->writeTxn;
    if (currentTxn->flags & TXN_ABORTABLE) {
        mdbx_txn_abort(currentTxn->txn);
    } else {
        Nan::ThrowError("Can not abort this transaction");
    }
    ew->writeTxn = currentTxn->parent;
    if (!ew->writeTxn) {
        if (ew->writeWorker)
            ew->writeWorker->UnlockTxn();
        else
            pthread_mutex_unlock(ew->writingLock);
    }
    delete currentTxn;
}


NAN_METHOD(EnvWrap::openDbi) {
    Nan::HandleScope scope;

    const unsigned argc = 5;
    Local<Value> argv[argc] = { info.This(), info[0], info[1], info[2], info[3] };
    Nan::MaybeLocal<Object> maybeInstance = Nan::NewInstance(Nan::New(*dbiCtor), argc, argv);

    // Check if database could be opened
    if ((maybeInstance.IsEmpty())) {
        // The maybeInstance is empty because the dbiCtor called Nan::ThrowError.
        // No need to call that here again, the user will get the error thrown there.
        return;
    }

    Local<Object> instance = maybeInstance.ToLocalChecked();
    DbiWrap *dw = Nan::ObjectWrap::Unwrap<DbiWrap>(instance);
    if (dw->dbi == (MDBX_dbi) 0xffffffff)
        info.GetReturnValue().Set(Nan::Undefined());
    else
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

NAN_METHOD(EnvWrap::resetCurrentReadTxn) {
    EnvWrap* ew = Nan::ObjectWrap::Unwrap<EnvWrap>(info.This());
    mdbx_txn_reset(ew->currentReadTxn);
    ew->readTxnRenewed = false;
}

void EnvWrap::setupExports(Local<Object> exports) {
    // EnvWrap: Prepare constructor template
    Local<FunctionTemplate> envTpl = Nan::New<FunctionTemplate>(EnvWrap::ctor);
    envTpl->SetClassName(Nan::New<String>("Env").ToLocalChecked());
    envTpl->InstanceTemplate()->SetInternalFieldCount(1);
    // EnvWrap: Add functions to the prototype
    Isolate *isolate = Isolate::GetCurrent();
    envTpl->PrototypeTemplate()->Set(isolate, "open", Nan::New<FunctionTemplate>(EnvWrap::open));
    envTpl->PrototypeTemplate()->Set(isolate, "getMaxKeySize", Nan::New<FunctionTemplate>(EnvWrap::getMaxKeySize));
    envTpl->PrototypeTemplate()->Set(isolate, "close", Nan::New<FunctionTemplate>(EnvWrap::close));
    envTpl->PrototypeTemplate()->Set(isolate, "beginTxn", Nan::New<FunctionTemplate>(EnvWrap::beginTxn));
    envTpl->PrototypeTemplate()->Set(isolate, "commitTxn", Nan::New<FunctionTemplate>(EnvWrap::commitTxn));
    envTpl->PrototypeTemplate()->Set(isolate, "abortTxn", Nan::New<FunctionTemplate>(EnvWrap::abortTxn));
    envTpl->PrototypeTemplate()->Set(isolate, "openDbi", Nan::New<FunctionTemplate>(EnvWrap::openDbi));
    envTpl->PrototypeTemplate()->Set(isolate, "sync", Nan::New<FunctionTemplate>(EnvWrap::sync));
    envTpl->PrototypeTemplate()->Set(isolate, "startWriting", Nan::New<FunctionTemplate>(EnvWrap::startWriting));
    envTpl->PrototypeTemplate()->Set(isolate, "compress", Nan::New<FunctionTemplate>(EnvWrap::compress));
    envTpl->PrototypeTemplate()->Set(isolate, "stat", Nan::New<FunctionTemplate>(EnvWrap::stat));
    envTpl->PrototypeTemplate()->Set(isolate, "freeStat", Nan::New<FunctionTemplate>(EnvWrap::freeStat));
    envTpl->PrototypeTemplate()->Set(isolate, "info", Nan::New<FunctionTemplate>(EnvWrap::info));
    envTpl->PrototypeTemplate()->Set(isolate, "readerCheck", Nan::New<FunctionTemplate>(EnvWrap::readerCheck));
    envTpl->PrototypeTemplate()->Set(isolate, "readerList", Nan::New<FunctionTemplate>(EnvWrap::readerList));
    envTpl->PrototypeTemplate()->Set(isolate, "resize", Nan::New<FunctionTemplate>(EnvWrap::resize));
    envTpl->PrototypeTemplate()->Set(isolate, "copy", Nan::New<FunctionTemplate>(EnvWrap::copy));
    envTpl->PrototypeTemplate()->Set(isolate, "detachBuffer", Nan::New<FunctionTemplate>(EnvWrap::detachBuffer));
    envTpl->PrototypeTemplate()->Set(isolate, "resetCurrentReadTxn", Nan::New<FunctionTemplate>(EnvWrap::resetCurrentReadTxn));

    // TxnWrap: Prepare constructor template
    Local<FunctionTemplate> txnTpl = Nan::New<FunctionTemplate>(TxnWrap::ctor);
    txnTpl->SetClassName(Nan::New<String>("Txn").ToLocalChecked());
    txnTpl->InstanceTemplate()->SetInternalFieldCount(1);
    // TxnWrap: Add functions to the prototype
    txnTpl->PrototypeTemplate()->Set(isolate, "commit", Nan::New<FunctionTemplate>(TxnWrap::commit));
    txnTpl->PrototypeTemplate()->Set(isolate, "abort", Nan::New<FunctionTemplate>(TxnWrap::abort));
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
    #if ENABLE_FAST_API && NODE_VERSION_AT_LEAST(16,6,0)
    auto getFast = CFunction::Make(DbiWrap::getByBinaryFast);
    dbiTpl->PrototypeTemplate()->Set(isolate, "getByBinary", v8::FunctionTemplate::New(
          isolate, DbiWrap::getByBinary, v8::Local<v8::Value>(),
          v8::Local<v8::Signature>(), 0, v8::ConstructorBehavior::kThrow,
          v8::SideEffectType::kHasNoSideEffect, &getFast));
    auto writeFast = CFunction::Make(EnvWrap::writeFast);
    envTpl->PrototypeTemplate()->Set(isolate, "write", v8::FunctionTemplate::New(
        isolate, EnvWrap::write, v8::Local<v8::Value>(),
        v8::Local<v8::Signature>(), 0, v8::ConstructorBehavior::kThrow,
        v8::SideEffectType::kHasNoSideEffect, &writeFast));

    #else
    dbiTpl->PrototypeTemplate()->Set(isolate, "getByBinary", v8::FunctionTemplate::New(
          isolate, DbiWrap::getByBinary, v8::Local<v8::Value>(),
          v8::Local<v8::Signature>(), 0, v8::ConstructorBehavior::kThrow,
          v8::SideEffectType::kHasNoSideEffect));
    envTpl->PrototypeTemplate()->Set(isolate, "write", v8::FunctionTemplate::New(
        isolate, EnvWrap::write, v8::Local<v8::Value>(),
        v8::Local<v8::Signature>(), 0, v8::ConstructorBehavior::kThrow,
        v8::SideEffectType::kHasNoSideEffect));
    #endif
    dbiTpl->PrototypeTemplate()->Set(isolate, "getStringByBinary", Nan::New<FunctionTemplate>(DbiWrap::getStringByBinary));
    dbiTpl->PrototypeTemplate()->Set(isolate, "prefetch", Nan::New<FunctionTemplate>(DbiWrap::prefetch));


    // TODO: wrap mdbx_stat too
    // DbiWrap: Get constructor
    EnvWrap::dbiCtor = new Nan::Persistent<Function>();
    EnvWrap::dbiCtor->Reset( dbiTpl->GetFunction(Nan::GetCurrentContext()).ToLocalChecked());

    Local<FunctionTemplate> compressionTpl = Nan::New<FunctionTemplate>(Compression::ctor);
    compressionTpl->SetClassName(Nan::New<String>("Compression").ToLocalChecked());
    compressionTpl->InstanceTemplate()->SetInternalFieldCount(1);
    compressionTpl->PrototypeTemplate()->Set(isolate, "setBuffer", Nan::New<FunctionTemplate>(Compression::setBuffer));
    (void)exports->Set(Nan::GetCurrentContext(), Nan::New<String>("Compression").ToLocalChecked(), compressionTpl->GetFunction(Nan::GetCurrentContext()).ToLocalChecked());

    // Set exports
    (void)exports->Set(Nan::GetCurrentContext(), Nan::New<String>("Env").ToLocalChecked(), envTpl->GetFunction(Nan::GetCurrentContext()).ToLocalChecked());
}

extern "C" EXTERN int64_t envOpen(int flags, int jsFlags, char* path, char* keyBuffer, double compression, int maxDbs,
        int maxReaders, double mapSize, int pageSize) {
    EnvWrap* ew = new EnvWrap();
    int rc = mdbx_env_create(&(ew->env));
    if (rc)
        return rc;
    rc = ew->openEnv(flags, jsFlags, path, keyBuffer, (Compression*) (size_t) compression,
        maxDbs, maxReaders, (size_t) mapSize, pageSize);
    if (rc)
        return rc;
    return (ssize_t) ew;
}

extern "C" EXTERN uint32_t getMaxKeySize(double ew) {
    return mdbx_env_get_maxkeysize(((EnvWrap*) (size_t) ew)->env);
}
extern "C" EXTERN int32_t readerCheck(double ew) {
    int rc, dead;
    rc = mdbx_reader_check(((EnvWrap*) (size_t) ew)->env, &dead);
    return rc || dead;
}
extern "C" EXTERN int64_t openDbi(double ewPointer, int flags, char* name, int keyType, double compression) {
    EnvWrap* ew = (EnvWrap*) (size_t) ewPointer;
    DbiWrap* dw = new DbiWrap(ew->env, 0);
    dw->ew = ew;
    if (((size_t) name) < 100) // 1 means nullptr?
        name = nullptr;
    int rc = dw->open(flags & ~HAS_VERSIONS, name, flags & HAS_VERSIONS,
        (LmdbxKeyType) keyType, (Compression*) (size_t) compression);
    if (rc) {
        delete dw;
        return rc;
    }
    return (int64_t) dw;
}

extern "C" EXTERN int64_t beginTxn(double ewPointer, int flags) {
    EnvWrap* ew = (EnvWrap*) (size_t) ewPointer;
    TxnWrap* tw = new TxnWrap(ew->env, nullptr);
    int rc = tw->begin(ew, flags);
    if (rc) {
        delete tw;
        return rc;
    }
    return (int64_t) tw;
}


// This file contains code from the node-lmdb project
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

