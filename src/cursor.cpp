#include "lmdbx-js.h"
#include <string.h>

using namespace v8;
using namespace node;

CursorWrap::CursorWrap(MDBX_cursor *cursor) {
    this->cursor = cursor;
    this->keyType = LmdbxKeyType::StringKey;
    this->freeKey = nullptr;
    this->endKey.iov_len = 0; // indicates no end key (yet)
}

CursorWrap::~CursorWrap() {
    if (this->cursor) {
        this->dw->Unref();
        // Don't close cursor here, it is possible that the environment may already be closed, which causes it to crash
        //mdbx_cursor_close(this->cursor);
    }
    if (this->freeKey) {
        this->freeKey(this->key);
    }
}

NAN_METHOD(CursorWrap::ctor) {
    Nan::HandleScope scope;

    if (info.Length() < 1) {
      return Nan::ThrowError("Wrong number of arguments");
    }

    // Extra pessimism...
    Nan::MaybeLocal<v8::Object> arg0 = Nan::To<v8::Object>(info[0]);
    if (arg0.IsEmpty()) {
        return Nan::ThrowError("Invalid arguments to the Cursor constructor. First must be a Txn, second must be a Dbi.");
    }

    DbiWrap *dw = Nan::ObjectWrap::Unwrap<DbiWrap>(arg0.ToLocalChecked());

    // Get key type
    auto keyType = keyTypeFromOptions(info[1], dw->keyType);
    if (dw->keyType == LmdbxKeyType::Uint32Key && keyType != LmdbxKeyType::Uint32Key) {
        return Nan::ThrowError("You specified uint32 keys on the Dbi, so you can't use other key types with it.");
    }

    // Open the cursor
    MDBX_cursor *cursor;
    MDBX_txn *txn = dw->ew->getReadTxn();
    int rc = mdbx_cursor_open(txn, dw->dbi, &cursor);
    if (rc != 0) {
        return throwLmdbxError(rc);
    }

    // Create wrapper
    CursorWrap* cw = new CursorWrap(cursor);
    cw->dw = dw;
    cw->dw->Ref();
    cw->txn = txn;
    cw->keyType = keyType;
    cw->Wrap(info.This());

    return info.GetReturnValue().Set(info.This());
}

NAN_METHOD(CursorWrap::close) {
    Nan::HandleScope scope;

    CursorWrap *cw = Nan::ObjectWrap::Unwrap<CursorWrap>(info.This());
    if (!cw->cursor) {
      return Nan::ThrowError("cursor.close: Attempt to close a closed cursor!");
    }
    mdbx_cursor_close(cw->cursor);
    cw->dw->Unref();
    cw->cursor = nullptr;
}
extern "C" EXTERN void cursorClose(double cwPointer) {
    CursorWrap *cw = (CursorWrap*) (size_t) cwPointer;
    mdbx_cursor_close(cw->cursor);
    cw->cursor = nullptr;
}


NAN_METHOD(CursorWrap::del) {
    Nan::HandleScope scope;

    if (info.Length() != 0 && info.Length() != 1) {
        return Nan::ThrowError("cursor.del: Incorrect number of arguments provided, arguments: options (optional).");
    }

    MDBX_put_flags_t flags = MDBX_UPSERT;

    if (info.Length() == 1) {
        if (!info[0]->IsObject()) {
            return Nan::ThrowError("cursor.del: Invalid options argument. It should be an object.");
        }
        
        auto options = Nan::To<v8::Object>(info[0]).ToLocalChecked();
        setFlagFromValue((int*) &flags, (int)MDBX_NODUPDATA, "noDupData", false, options);
    }

    CursorWrap *cw = Nan::ObjectWrap::Unwrap<CursorWrap>(info.This());

    int rc = mdbx_cursor_del(cw->cursor, flags);
    if (rc != 0) {
        return throwLmdbxError(rc);
    }
}
int CursorWrap::returnEntry(int lastRC, MDBX_val &key, MDBX_val &data) {
    if (lastRC) {
        if (lastRC == MDBX_NOTFOUND)
            return 0;
        else {
            return lastRC | 0x10000000;
        }
    }   
    if (endKey.iov_len > 0) {
        int comparison;
        if (flags & 0x800)
            comparison = mdbx_dcmp(txn, dw->dbi, &endKey, &data);
        else
            comparison = mdbx_cmp(txn, dw->dbi, &endKey, &key);
        if ((flags & 0x400) ? comparison >= 0 : (comparison <= 0)) {
            return 0;
        }
    }
	char* keyBuffer = dw->ew->keyBuffer;
	if (flags & 0x100) {
        bool result = getVersionAndUncompress(data, dw);
        if (result)
            result = valToBinaryFast(data, dw);
		*((size_t*)keyBuffer) = data.iov_len;
	}
	if (!(flags & 0x800))
        memcpy(keyBuffer + 32, key.iov_base, key.iov_len);

    return key.iov_len;
}

const int START_ADDRESS_POSITION = 4064;
uint32_t CursorWrap::doPosition(uint32_t offset, uint32_t keySize, uint64_t endKeyAddress) {
    //char* keyBuffer = dw->ew->keyBuffer;
    MDBX_val key, data;
    int rc;
    if (flags & 0x2000) // TODO: check the txn_id to determine if we need to renew
        mdbx_cursor_renew(mdbx_cursor_txn(cursor), cursor);
    if (endKeyAddress) {
        uint32_t* keyBuffer = (uint32_t*) endKeyAddress;
        endKey.iov_len = *keyBuffer;
        endKey.iov_base = (char*)(keyBuffer + 1);
    } else
        endKey.iov_len = 0;
    iteratingOp = (flags & 0x400) ?
        (flags & 0x100) ?
            (flags & 0x800) ? MDBX_PREV_DUP : MDBX_PREV :
            MDBX_PREV_NODUP :
        (flags & 0x100) ?
            (flags & 0x800) ? MDBX_NEXT_DUP : MDBX_NEXT :
            MDBX_NEXT_NODUP;
    key.iov_len = keySize;
    key.iov_base = dw->ew->keyBuffer;
    if (keySize == 0) {
        rc = mdbx_cursor_get(cursor, &key, &data, flags & 0x400 ? MDBX_LAST : MDBX_FIRST);  
    } else {
        if (flags & 0x800) { // only values for this key
            // take the next part of the key buffer as a pointer to starting data
            uint32_t* startValueBuffer = (uint32_t*)(size_t)(*(double*)(dw->ew->keyBuffer + START_ADDRESS_POSITION));
            data.iov_len = endKeyAddress ? *((uint32_t*)startValueBuffer) : 0;
            data.iov_base = startValueBuffer + 1;
            if (flags & 0x400) {// reverse through values
                MDBX_val startValue = data; // save it for comparison
                rc = mdbx_cursor_get(cursor, &key, &data, data.iov_len ? MDBX_GET_BOTH_RANGE : MDBX_SET_KEY);
                if (rc) {
                    if (startValue.iov_len) {
                        // value specified, but not found, so find key and go to last item
                        rc = mdbx_cursor_get(cursor, &key, &data, MDBX_SET_KEY);
                        if (!rc)
                            rc = mdbx_cursor_get(cursor, &key, &data, MDBX_LAST_DUP);
                    } // else just couldn't find the key
                } else { // found entry
                    if (startValue.iov_len == 0) // no value specified, so go to last value
                        rc = mdbx_cursor_get(cursor, &key, &data, MDBX_LAST_DUP);
                    else if (mdbx_dcmp(txn, dw->dbi, &startValue, &data)) // the range found the next value *after* the start
                        rc = mdbx_cursor_get(cursor, &key, &data, MDBX_PREV_DUP);
                }
            } else // forward, just do a get by range
                rc = mdbx_cursor_get(cursor, &key, &data, data.iov_len ?
                    (flags & 0x4000) ? MDBX_GET_BOTH : MDBX_GET_BOTH_RANGE : MDBX_SET_KEY);

            if (rc == MDBX_NOTFOUND)
                return 0;
            if (flags & 0x1000 && (!endKeyAddress || (flags & 0x4000))) {
                size_t count;
                rc = mdbx_cursor_count(cursor, &count);
                if (rc)
                    throwLmdbxError(rc);
                return count;
            }
        } else {
            if (flags & 0x400) {// reverse
                MDBX_val firstKey = key; // save it for comparison
                rc = mdbx_cursor_get(cursor, &key, &data, MDBX_SET_RANGE);
                if (rc)
                    rc = mdbx_cursor_get(cursor, &key, &data, MDBX_LAST);
                else if (mdbx_cmp(txn, dw->dbi, &firstKey, &key)) // the range found the next entry *after* the start
                    rc = mdbx_cursor_get(cursor, &key, &data, MDBX_PREV);
            } else // forward, just do a get by range
                rc = mdbx_cursor_get(cursor, &key, &data, (flags & 0x4000) ? MDBX_SET_KEY : MDBX_SET_RANGE);
        }
    }
    while (offset-- > 0 && !rc) {
        rc = mdbx_cursor_get(cursor, &key, &data, iteratingOp);
    }
    if (flags & 0x1000) {
        uint32_t count = 0;
        bool useCursorCount = false;
        // if we are in a dupsort database, and we are iterating over all entries, we can just count all the values for each key
        if (dw->flags & (int) MDBX_DUPSORT) {
            if (iteratingOp == MDBX_PREV) {
                iteratingOp = MDBX_PREV_NODUP;
                useCursorCount = true;
            }
            if (iteratingOp == MDBX_NEXT) {
                iteratingOp = MDBX_NEXT_NODUP;
                useCursorCount = true;
            }
        }

        while (!rc) {
            if (endKey.iov_len > 0) {
                int comparison;
                if (flags & 0x800)
                    comparison = mdbx_dcmp(txn, dw->dbi, &endKey, &data);
                else
                    comparison = mdbx_cmp(txn, dw->dbi, &endKey, &key);
                if ((flags & 0x400) ? comparison >= 0 : (comparison <=0)) {
                    return count;
                }
            }
            if (useCursorCount) {
                size_t countForKey;
                rc = mdbx_cursor_count(cursor, &countForKey);
                if (rc)
                    throwLmdbxError(rc);
                count += countForKey;
            } else
                count++;
            rc = mdbx_cursor_get(cursor, &key, &data, iteratingOp);
        }
        return count;
    }
    return returnEntry(rc, key, data);
}
#if ENABLE_FAST_API && NODE_VERSION_AT_LEAST(16,6,0)
uint32_t CursorWrap::positionFast(Local<Object> receiver_obj, uint32_t flags, uint32_t offset, uint32_t keySize, uint64_t endKeyAddress, FastApiCallbackOptions& options) {
    CursorWrap* cw = static_cast<CursorWrap*>(
        receiver_obj->GetAlignedPointerFromInternalField(0));
    DbiWrap* dw = cw->dw;
    dw->getFast = true;
    cw->flags = flags;
    uint32_t result = cw->doPosition(offset, keySize, endKeyAddress);
    if (dw->getFast)
        dw->getFast = false;
    else
        options.fallback = true;
    return result;
}
#endif
void CursorWrap::position(
  const v8::FunctionCallbackInfo<v8::Value>& info) {
    v8::Local<v8::Object> instance =
      v8::Local<v8::Object>::Cast(info.Holder());
    CursorWrap* cw = Nan::ObjectWrap::Unwrap<CursorWrap>(instance);
    cw->flags = info[0]->Uint32Value(Nan::GetCurrentContext()).FromJust();
    uint32_t offset = info[1]->Uint32Value(Nan::GetCurrentContext()).FromJust();
    uint32_t keySize = info[2]->IntegerValue(Nan::GetCurrentContext()).FromJust();
    uint64_t endKeyAddress = info[3]->IntegerValue(Nan::GetCurrentContext()).FromJust();
    uint32_t result = cw->doPosition(offset, keySize, endKeyAddress);
    info.GetReturnValue().Set(Nan::New<Number>(result));
}
extern "C" EXTERN int cursorPosition(double cwPointer, uint32_t flags, uint32_t offset, uint32_t keySize, double endKeyAddress) {
    CursorWrap *cw = (CursorWrap*) (size_t) cwPointer;
    cw->flags = flags;
    return cw->doPosition(offset, keySize, (uint64_t) endKeyAddress);
}

#ifdef ENABLE_FAST_API
int32_t CursorWrap::iterateFast(Local<Object> receiver_obj, FastApiCallbackOptions& options) {
    CursorWrap* cw = static_cast<CursorWrap*>(
        receiver_obj->GetAlignedPointerFromInternalField(0));
    DbiWrap* dw = cw->dw;
    dw->getFast = true;
    MDBX_val key, data;
    int rc = mdbx_cursor_get(cw->cursor, &key, &data, cw->iteratingOp);
    return cw->returnEntry(rc, key, data);
}
#endif
void CursorWrap::iterate(
  const v8::FunctionCallbackInfo<v8::Value>& info) {
    v8::Local<v8::Object> instance =
      v8::Local<v8::Object>::Cast(info.Holder());
    CursorWrap* cw = Nan::ObjectWrap::Unwrap<CursorWrap>(instance);
    MDBX_val key, data;
    int rc = mdbx_cursor_get(cw->cursor, &key, &data, cw->iteratingOp);
    return info.GetReturnValue().Set(Nan::New<Number>(cw->returnEntry(rc, key, data)));
}
extern "C" EXTERN int cursorIterate(double cwPointer) {
    CursorWrap *cw = (CursorWrap*) (size_t) cwPointer;
    MDB_val key, data;
    int rc = mdbx_cursor_get(cw->cursor, &key, &data, cw->iteratingOp);
    return cw->returnEntry(rc, key, data);
}

NAN_METHOD(CursorWrap::getCurrentValue) {
    CursorWrap* cw = Nan::ObjectWrap::Unwrap<CursorWrap>(info.Holder());
    MDBX_val key, data;
    int rc = mdbx_cursor_get(cw->cursor, &key, &data, MDBX_GET_CURRENT);
    return info.GetReturnValue().Set(Nan::New<Number>(cw->returnEntry(rc, key, data)));
}
extern "C" EXTERN int cursorCurrentValue(double cwPointer) {
    CursorWrap *cw = (CursorWrap*) (size_t) cwPointer;
    MDB_val key, data;
    int rc = mdbx_cursor_get(cw->cursor, &key, &data, MDB_GET_CURRENT);
    return cw->returnEntry(rc, key, data);
}

NAN_METHOD(CursorWrap::renew) {
    CursorWrap* cw = Nan::ObjectWrap::Unwrap<CursorWrap>(info.Holder());
    // Unwrap Txn and Dbi
    int rc = mdbx_cursor_renew(cw->txn = cw->dw->ew->getReadTxn(), cw->cursor);
    if (rc != 0) {
        return throwLmdbxError(rc);
    }
}
extern "C" EXTERN int cursorRenew(double cwPointer) {
    CursorWrap *cw = (CursorWrap*) (size_t) cwPointer;
    return mdbx_cursor_renew(cw->txn = cw->dw->ew->getReadTxn(), cw->cursor);
}
void CursorWrap::setupExports(Local<Object> exports) {
    // CursorWrap: Prepare constructor template
    Local<FunctionTemplate> cursorTpl = Nan::New<FunctionTemplate>(CursorWrap::ctor);
    cursorTpl->SetClassName(Nan::New<String>("Cursor").ToLocalChecked());
    cursorTpl->InstanceTemplate()->SetInternalFieldCount(1);
    // CursorWrap: Add functions to the prototype
    cursorTpl->PrototypeTemplate()->Set(Nan::New<String>("close").ToLocalChecked(), Nan::New<FunctionTemplate>(CursorWrap::close));
    cursorTpl->PrototypeTemplate()->Set(Nan::New<String>("del").ToLocalChecked(), Nan::New<FunctionTemplate>(CursorWrap::del));
    cursorTpl->PrototypeTemplate()->Set(Nan::New<String>("getCurrentValue").ToLocalChecked(), Nan::New<FunctionTemplate>(CursorWrap::getCurrentValue));

    Isolate *isolate = Isolate::GetCurrent();
    #ifdef ENABLE_FAST_API
    auto positionFast = CFunction::Make(CursorWrap::positionFast);
    cursorTpl->PrototypeTemplate()->Set(isolate, "position", v8::FunctionTemplate::New(
          isolate, CursorWrap::position, v8::Local<v8::Value>(),
          v8::Local<v8::Signature>(), 0, v8::ConstructorBehavior::kThrow,
          v8::SideEffectType::kHasNoSideEffect, &positionFast));

    auto iterateFast = CFunction::Make(CursorWrap::iterateFast);
    cursorTpl->PrototypeTemplate()->Set(isolate, "iterate", v8::FunctionTemplate::New(
          isolate, CursorWrap::iterate, v8::Local<v8::Value>(),
          v8::Local<v8::Signature>(), 0, v8::ConstructorBehavior::kThrow,
          v8::SideEffectType::kHasNoSideEffect, &iterateFast));
    #else
    cursorTpl->PrototypeTemplate()->Set(isolate, "position", v8::FunctionTemplate::New(
          isolate, CursorWrap::position, v8::Local<v8::Value>(),
          v8::Local<v8::Signature>(), 0, v8::ConstructorBehavior::kThrow,
          v8::SideEffectType::kHasNoSideEffect));

    cursorTpl->PrototypeTemplate()->Set(isolate, "iterate", v8::FunctionTemplate::New(
          isolate, CursorWrap::iterate, v8::Local<v8::Value>(),
          v8::Local<v8::Signature>(), 0, v8::ConstructorBehavior::kThrow,
          v8::SideEffectType::kHasNoSideEffect));
    #endif

    cursorTpl->PrototypeTemplate()->Set(Nan::New<String>("renew").ToLocalChecked(), Nan::New<FunctionTemplate>(CursorWrap::renew));

    // Set exports
    (void)exports->Set(Nan::GetCurrentContext(), Nan::New<String>("Cursor").ToLocalChecked(), cursorTpl->GetFunction(Nan::GetCurrentContext()).ToLocalChecked());
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

