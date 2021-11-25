#include "lmdbx-js.h"
#include <cstdio>

using namespace v8;
using namespace node;

void setFlagFromValue(int *flags, int flag, const char *name, bool defaultValue, Local<Object> options);

DbiWrap::DbiWrap(MDBX_env *env, MDBX_dbi dbi) {
    this->env = env;
    this->dbi = dbi;
    this->keyType = NodeLmdbxKeyType::DefaultKey;
    this->compression = nullptr;
    this->isOpen = false;
    this->getFast = false;
    this->ew = nullptr;
}

DbiWrap::~DbiWrap() {
    // Imagine the following JS:
    // ------------------------
    //     var dbi1 = env.openDbi({ name: "hello" });
    //     var dbi2 = env.openDbi({ name: "hello" });
    //     dbi1.close();
    //     txn.putString(dbi2, "world");
    // -----
    // The above DbiWrap objects would both wrap the same MDBX_dbi, and if closing the first one called mdbx_dbi_close,
    // that'd also render the second DbiWrap instance unusable.
    //
    // For this reason, we will never call mdbx_dbi_close
    // NOTE: according to LMDBX authors, it is perfectly fine if mdbx_dbi_close is never called on an MDBX_dbi

    if (this->ew) {
        this->ew->Unref();
    }
    if (this->compression)
        this->compression->Unref();
}

NAN_METHOD(DbiWrap::ctor) {
    Nan::HandleScope scope;

    MDBX_dbi dbi;
    MDBX_txn *txn;
    int rc;
    MDBX_db_flags_t flags = MDBX_DB_DEFAULTS;
    MDBX_txn_flags_t txnFlags = MDBX_TXN_READWRITE;
    Local<String> name;
    bool nameIsNull = false;
    NodeLmdbxKeyType keyType = NodeLmdbxKeyType::DefaultKey;
    bool needsTransaction = true;
    bool isOpen = false;
    bool hasVersions = false;

    EnvWrap *ew = Nan::ObjectWrap::Unwrap<EnvWrap>(Local<Object>::Cast(info[0]));
    Compression* compression = nullptr;

    if (info[1]->IsObject()) {
        Local<Object> options = Local<Object>::Cast(info[1]);
        nameIsNull = options->Get(Nan::GetCurrentContext(), Nan::New<String>("name").ToLocalChecked()).ToLocalChecked()->IsNull();
        name = Local<String>::Cast(options->Get(Nan::GetCurrentContext(), Nan::New<String>("name").ToLocalChecked()).ToLocalChecked());

        // Get flags from options

        // NOTE: mdbx_set_relfunc is not exposed because MDBX_FIXEDMAP is "highly experimental"
        // NOTE: mdbx_set_relctx is not exposed because MDBX_FIXEDMAP is "highly experimental"
        setFlagFromValue((int*) &flags, (int)MDBX_REVERSEKEY, "reverseKey", false, options);
        setFlagFromValue((int*) &flags, (int)MDBX_DUPSORT, "dupSort", false, options);
        setFlagFromValue((int*) &flags, (int)MDBX_DUPFIXED, "dupFixed", false, options);
        setFlagFromValue((int*) &flags, (int)MDBX_INTEGERDUP, "integerDup", false, options);
        setFlagFromValue((int*) &flags, (int)MDBX_REVERSEDUP, "reverseDup", false, options);
        setFlagFromValue((int*) &flags, (int)MDBX_CREATE, "create", false, options);

        // TODO: wrap mdbx_set_compare
        // TODO: wrap mdbx_set_dupsort

        keyType = keyTypeFromOptions(options);
        if (keyType == NodeLmdbxKeyType::InvalidKey) {
            // NOTE: Error has already been thrown inside keyTypeFromOptions
            return;
        }
        
        if (keyType == NodeLmdbxKeyType::Uint32Key) {
            flags = MDBX_INTEGERKEY;
        }
        Local<Value> compressionOption = options->Get(Nan::GetCurrentContext(), Nan::New<String>("compression").ToLocalChecked()).ToLocalChecked();
        if (compressionOption->IsObject()) {
            compression = Nan::ObjectWrap::Unwrap<Compression>(Nan::To<v8::Object>(compressionOption).ToLocalChecked());
        }

        // Set flags for txn used to open database
        Local<Value> create = options->Get(Nan::GetCurrentContext(), Nan::New<String>("create").ToLocalChecked()).ToLocalChecked();
        #if NODE_VERSION_AT_LEAST(12,0,0)
        if (create->IsBoolean() ? !create->BooleanValue(Isolate::GetCurrent()) : true) {
        #else
        if (create->IsBoolean() ? !create->BooleanValue(Nan::GetCurrentContext()).FromJust() : true) {
        #endif
            txnFlags = MDBX_TXN_RDONLY;
        }
        Local<Value> hasVersionsLocal = options->Get(Nan::GetCurrentContext(), Nan::New<String>("useVersions").ToLocalChecked()).ToLocalChecked();
        hasVersions = hasVersionsLocal->IsTrue();

        if (ew->writeTxn) {
            needsTransaction = false;
            txn = ew->writeTxn->txn;
        }
    }
    else {
        return Nan::ThrowError("Invalid parameters.");
    }

    if (needsTransaction) {
        // Open transaction
        rc = mdbx_txn_begin(ew->env, nullptr, txnFlags, &txn);
        if (rc != 0) {
            // No need to call mdbx_txn_abort, because mdbx_txn_begin already cleans up after itself
            return throwLmdbxError(rc);
        }
    }

    // Open database
    // NOTE: nullptr in place of the name means using the unnamed database.
    #if NODE_VERSION_AT_LEAST(12,0,0)
    rc = mdbx_dbi_open(txn, nameIsNull ? nullptr : *String::Utf8Value(Isolate::GetCurrent(), name), flags, &dbi);
    #else
    rc = mdbx_dbi_open(txn, nameIsNull ? nullptr : *String::Utf8Value(name), flags, &dbi);
    #endif
    if (rc != 0) {
        if (needsTransaction) {
            mdbx_txn_abort(txn);
        }
        return throwLmdbxError(rc);
    }
    else {
        isOpen = true;
    }
    // Create wrapper
    DbiWrap* dw = new DbiWrap(ew->env, dbi);
    if (isOpen) {
        dw->ew = ew;
        dw->ew->Ref();
    }
    if (needsTransaction) {
        // Commit transaction
        rc = mdbx_txn_commit(txn);
        if (rc != 0) {
            return throwLmdbxError(rc);
        }
    }

    dw->keyType = keyType;
    dw->flags = flags;
    dw->isOpen = isOpen;
    if (compression)
        compression->Ref();
    dw->compression = compression;
    dw->hasVersions = hasVersions;
    dw->Wrap(info.This());
    info.This()->Set(Nan::GetCurrentContext(), Nan::New<String>("dbi").ToLocalChecked(), Nan::New<Number>(dbi));

    return info.GetReturnValue().Set(info.This());
}

NAN_METHOD(DbiWrap::close) {
    Nan::HandleScope scope;

    DbiWrap *dw = Nan::ObjectWrap::Unwrap<DbiWrap>(info.This());
    if (dw->isOpen) {
        mdbx_dbi_close(dw->env, dw->dbi);
        dw->isOpen = false;
        dw->ew->Unref();
        dw->ew = nullptr;
    }
    else {
        return Nan::ThrowError("The Dbi is not open, you can't close it.");
    }
}

NAN_METHOD(DbiWrap::drop) {
    Nan::HandleScope scope;

    DbiWrap *dw = Nan::ObjectWrap::Unwrap<DbiWrap>(info.This());
    int del = 1;
    int rc;
    if (!dw->isOpen) {
        return Nan::ThrowError("The Dbi is not open, you can't drop it.");
    }

    // Check if the database should be deleted
    if (info.Length() == 1 && info[0]->IsObject()) {
        Local<Object> options = Local<Object>::Cast(info[0]);
        
        // Just free pages
        Local<Value> opt = options->Get(Nan::GetCurrentContext(), Nan::New<String>("justFreePages").ToLocalChecked()).ToLocalChecked();
        #if NODE_VERSION_AT_LEAST(12,0,0)
        del = opt->IsBoolean() ? !(opt->BooleanValue(Isolate::GetCurrent())) : 1;
        #else
        del = opt->IsBoolean() ? !(opt->BooleanValue(Nan::GetCurrentContext()).FromJust()) : 1;
        #endif
    }

    // Drop database
    rc = mdbx_drop(dw->ew->writeTxn->txn, dw->dbi, del);
    if (rc != 0) {
        return throwLmdbxError(rc);
    }

    // Only close database if del == 1
    if (del == 1) {
        dw->isOpen = false;
        dw->ew->Unref();
        dw->ew = nullptr;
    }
}

NAN_METHOD(DbiWrap::stat) {
    Nan::HandleScope scope;

    DbiWrap *dw = Nan::ObjectWrap::Unwrap<DbiWrap>(info.This());

    if (info.Length() != 1) {
        return Nan::ThrowError("dbi.stat should be called with a single argument which is a txn.");
    }

    TxnWrap *txn = Nan::ObjectWrap::Unwrap<TxnWrap>(Local<Object>::Cast(info[0]));

    MDBX_stat stat;
    mdbx_dbi_stat(txn->txn, dw->dbi, &stat, sizeof(MDBX_stat));

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

#if ENABLE_FAST_API && NODE_VERSION_AT_LEAST(16,6,0)
uint32_t DbiWrap::getByBinaryFast(Local<Object> receiver_obj, uint32_t keySize, FastApiCallbackOptions& options) {
	DbiWrap* dw = static_cast<DbiWrap*>(
        receiver_obj->GetAlignedPointerFromInternalField(0));
    EnvWrap* ew = dw->ew;
    char* keyBuffer = ew->keyBuffer;
    MDBX_txn* txn = ew->getReadTxn();
    MDBX_val key, data;
    key.iov_len = keySize;
    key.iov_base = (void*) keyBuffer;

    int result = mdbx_get(txn, dw->dbi, &key, &data);
    if (result) {
        if (result == MDBX_NOTFOUND)
            return 0xffffffff;
        // let the slow handler handle throwing errors
        options.fallback = true;
        return result;
    }
    dw->getFast = true;
    result = getVersionAndUncompress(data, dw);
    if (result)
        result = valToBinaryFast(data, dw);
    if (!result) {
        // this means an allocation or error needs to be thrown, so we fallback to the slow handler
        // or since we are using signed int32 (so we can return error codes), need special handling for above 2GB entries
        options.fallback = true;
    }
    dw->getFast = false;
    /*
    alternately, if we want to send over the address, which can be used for direct access to the LMDB shared memory, but all benchmarking shows it is slower
    *((size_t*) keyBuffer) = data.iov_len;
    *((uint64_t*) (keyBuffer + 8)) = (uint64_t) data.iov_base;
    return 0;*/
    return data.iov_len;
}
#endif

void DbiWrap::getByBinary(
  const v8::FunctionCallbackInfo<v8::Value>& info) {
    v8::Local<v8::Object> instance =
      v8::Local<v8::Object>::Cast(info.Holder());
    DbiWrap* dw = Nan::ObjectWrap::Unwrap<DbiWrap>(instance);
    char* keyBuffer = dw->ew->keyBuffer;
    MDBX_txn* txn = dw->ew->getReadTxn();
    MDBX_val key;
    MDBX_val data;
    key.iov_len = info[0]->Uint32Value(Nan::GetCurrentContext()).FromJust();
    key.iov_base = (void*) keyBuffer;
    int rc = mdbx_get(txn, dw->dbi, &key, &data);
    if (rc) {
        if (rc == MDBX_NOTFOUND)
            return info.GetReturnValue().Set(Nan::New<Number>(0xffffffff));
        else
            return throwLmdbxError(rc);
    }   
    rc = getVersionAndUncompress(data, dw);
    return info.GetReturnValue().Set(valToBinaryUnsafe(data, dw));
}

NAN_METHOD(DbiWrap::getStringByBinary) {
    v8::Local<v8::Object> instance =
      v8::Local<v8::Object>::Cast(info.Holder());
    DbiWrap* dw = Nan::ObjectWrap::Unwrap<DbiWrap>(instance);
    char* keyBuffer = dw->ew->keyBuffer;
    MDBX_txn* txn = dw->ew->getReadTxn();
    MDBX_val key;
    MDBX_val data;
    key.iov_len = info[0]->Uint32Value(Nan::GetCurrentContext()).FromJust();
    key.iov_base = (void*) keyBuffer;
    int rc = mdbx_get(txn, dw->dbi, &key, &data);
    if (rc) {
        if (rc == MDBX_NOTFOUND)
            return info.GetReturnValue().Set(Nan::Undefined());
        else
            return throwLmdbxError(rc);
    }
    rc = getVersionAndUncompress(data, dw);
    if (rc)
        return info.GetReturnValue().Set(valToUtf8(data));
    else
        return info.GetReturnValue().Set(Nan::New<Number>(data.iov_len));
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

