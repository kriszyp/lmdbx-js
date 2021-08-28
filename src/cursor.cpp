
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
#include <string.h>

using namespace v8;
using namespace node;

CursorWrap::CursorWrap(MDBX_cursor *cursor) {
    this->cursor = cursor;
    this->keyType = NodeLmdbxKeyType::StringKey;
    this->freeKey = nullptr;
}

CursorWrap::~CursorWrap() {
    if (this->cursor) {
        this->dw->Unref();
        this->tw->Unref();
        // Don't close cursor here, it is possible that the environment may already be closed, which causes it to crash
        //mdbx_cursor_close(this->cursor);
    }
    if (this->freeKey) {
        this->freeKey(this->key);
    }
}

NAN_METHOD(CursorWrap::ctor) {
    Nan::HandleScope scope;

    if (info.Length() < 2) {
      return Nan::ThrowError("Wrong number of arguments");
    }

    // Extra pessimism...
    Nan::MaybeLocal<v8::Object> arg0 = Nan::To<v8::Object>(info[0]);
    Nan::MaybeLocal<v8::Object> arg1 = Nan::To<v8::Object>(info[1]);
    if (arg0.IsEmpty() || arg1.IsEmpty()) {
        return Nan::ThrowError("Invalid arguments to the Cursor constructor. First must be a Txn, second must be a Dbi.");
    }

    // Unwrap Txn and Dbi
    TxnWrap *tw = Nan::ObjectWrap::Unwrap<TxnWrap>(arg0.ToLocalChecked());
    DbiWrap *dw = Nan::ObjectWrap::Unwrap<DbiWrap>(arg1.ToLocalChecked());

    // Get key type
    auto keyType = keyTypeFromOptions(info[2], dw->keyType);
    if (dw->keyType == NodeLmdbxKeyType::Uint32Key && keyType != NodeLmdbxKeyType::Uint32Key) {
        return Nan::ThrowError("You specified keyIsUint32 on the Dbi, so you can't use other key types with it.");
    }

    // Open the cursor
    MDBX_cursor *cursor;
    int rc = mdbx_cursor_open(tw->txn, dw->dbi, &cursor);
    if (rc != 0) {
        return throwLmdbxError(rc);
    }

    // Create wrapper
    CursorWrap* cw = new CursorWrap(cursor);
    cw->dw = dw;
    cw->dw->Ref();
    cw->tw = tw;
    cw->tw->Ref();
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
    cw->tw->Unref();
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

Nan::NAN_METHOD_RETURN_TYPE CursorWrap::getCommon(
    Nan::NAN_METHOD_ARGS_TYPE info,
    MDBX_cursor_op op,
    argtokey_callback_t (*setKey)(CursorWrap* cw, Nan::NAN_METHOD_ARGS_TYPE info, MDBX_val&, bool&),
    void (*setData)(CursorWrap* cw, Nan::NAN_METHOD_ARGS_TYPE info, MDBX_val&),
    void (*freeData)(CursorWrap* cw, Nan::NAN_METHOD_ARGS_TYPE info, MDBX_val&),
    Local<Value> (*convertFunc)(MDBX_val &data)
) {
    Nan::HandleScope scope;

    int al = info.Length();
    CursorWrap *cw = Nan::ObjectWrap::Unwrap<CursorWrap>(info.This());

    // When a new key is manually set
    if (setKey) {
        // Free old key if necessary
        if (cw->freeKey) {
            cw->freeKey(cw->key);
            cw->freeKey = nullptr;
        }

        // Set new key and assign the deleter function
        bool keyIsValid;
        cw->freeKey = setKey(cw, info, cw->key, keyIsValid);
        if (!keyIsValid) {
            // setKey already threw an error, no need to throw here
            return;
        }
    }

    // When data is manually set
    if (setData) {
        setData(cw, info, cw->data);
    }

    // Temporary thing, so that we can free up the data if we want to
    MDBX_val tempdata;
    tempdata.iov_len = cw->data.iov_len;
    tempdata.iov_base = cw->data.iov_base;

    // Temporary bookkeeping for the current key
    MDBX_val tempKey;
    tempKey.iov_len = cw->key.iov_len;
    tempKey.iov_base = cw->key.iov_base;

    // Call LMDBX
    int rc = mdbx_cursor_get(cw->cursor, &(cw->key), &(cw->data), op);

    // Check if key points inside LMDBX
    if (tempKey.iov_base != cw->key.iov_base) {
        // cw->key points inside the database now,
        // so we should free the old key now.
        if (cw->freeKey) {
            cw->freeKey(tempKey);
            cw->freeKey = nullptr;
        }
    }

    if (rc == MDBX_NOTFOUND) {
        return info.GetReturnValue().Set(Nan::Undefined());
    }
    else if (rc != 0) {
        return throwLmdbxError(rc);
    }


    Local<Value> dataHandle = Nan::Undefined();
    if (convertFunc) {
    //    fprintf(stdout, "getVersionAndUncompress\n");

        dataHandle = getVersionAndUncompress(cw->data, cw->dw, convertFunc);

        if (al > 0) {
            const auto &callbackFunc = info[al - 1];

            if (callbackFunc->IsFunction()) {
                // In this case, we expect the key/data pair to be correctly filled
                constexpr const unsigned argc = 2;
                Local<Value> keyHandle = Nan::Undefined();
                if (cw->key.iov_len) {
            //        fprintf(stdout, "cw->key.iov_len %u\n", cw->key.iov_len);
              //      fprintf(stdout, "cw->key.iov_base %X %X %X\n", ((char*)cw->key.iov_base)[0], ((char*)cw->key.iov_base)[1], ((char*)cw->key.iov_base)[2]);
                    keyHandle = keyToHandle(cw->key, cw->keyType);
                }
                Local<Value> argv[argc] = { keyHandle, dataHandle };
                
                Nan::Call(Nan::Callback(Local<Function>::Cast(callbackFunc)), argc, argv);
            }
        }
    }
    //fprintf(stdout, "freeData");

    if (freeData) {
        freeData(cw, info, tempdata);
    }

    if (convertFunc) {
        return info.GetReturnValue().Set(dataHandle);
    }
    else if (cw->key.iov_len) {
        return info.GetReturnValue().Set(keyToHandle(cw->key, cw->keyType));
    }

    return info.GetReturnValue().Set(Nan::True());
}

Nan::NAN_METHOD_RETURN_TYPE CursorWrap::getCommon(Nan::NAN_METHOD_ARGS_TYPE info, MDBX_cursor_op op) {
    return getCommon(info, op, nullptr, nullptr, nullptr, nullptr);
}

NAN_METHOD(CursorWrap::getCurrentString) {
    return getCommon(info, MDBX_GET_CURRENT, nullptr, nullptr, nullptr, valToString);
}

NAN_METHOD(CursorWrap::getCurrentStringUnsafe) {
    return getCommon(info, MDBX_GET_CURRENT, nullptr, nullptr, nullptr, valToStringUnsafe);
}

NAN_METHOD(CursorWrap::getCurrentUtf8) {
    return getCommon(info, MDBX_GET_CURRENT, nullptr, nullptr, nullptr, valToUtf8);
}

NAN_METHOD(CursorWrap::getCurrentBinary) {
    return getCommon(info, MDBX_GET_CURRENT, nullptr, nullptr, nullptr, valToBinary);
}

NAN_METHOD(CursorWrap::getCurrentBinaryUnsafe) {
    return getCommon(info, MDBX_GET_CURRENT, nullptr, nullptr, nullptr, valToBinaryUnsafe);
}

NAN_METHOD(CursorWrap::getCurrentNumber) {
    return getCommon(info, MDBX_GET_CURRENT, nullptr, nullptr, nullptr, valToNumber);
}

NAN_METHOD(CursorWrap::getCurrentBoolean) {
    return getCommon(info, MDBX_GET_CURRENT, nullptr, nullptr, nullptr, valToBoolean);
}

NAN_METHOD(CursorWrap::getCurrentIsDatabase) {
    /*CursorWrap* cw = Nan::ObjectWrap::Unwrap<CursorWrap>(info.This());
    int isDatabase = mdbx_cursor_is_db(cw->cursor);
    return info.GetReturnValue().Set(Nan::New<Boolean>(isDatabase));*/
}

#define MAKE_GET_FUNC(name, op) NAN_METHOD(CursorWrap::name) { return getCommon(info, op); }

MAKE_GET_FUNC(goToFirst, MDBX_FIRST);

MAKE_GET_FUNC(goToLast, MDBX_LAST);

MAKE_GET_FUNC(goToNext, MDBX_NEXT);

MAKE_GET_FUNC(goToPrev, MDBX_PREV);

MAKE_GET_FUNC(goToFirstDup, MDBX_FIRST_DUP);

MAKE_GET_FUNC(goToLastDup, MDBX_LAST_DUP);

MAKE_GET_FUNC(goToNextDup, MDBX_NEXT_DUP);

MAKE_GET_FUNC(goToPrevDup, MDBX_PREV_DUP);

MAKE_GET_FUNC(goToNextNoDup, MDBX_NEXT_NODUP);

MAKE_GET_FUNC(goToPrevNoDup, MDBX_PREV_NODUP);

static void fillDataFromArg1(CursorWrap* cw, Nan::NAN_METHOD_ARGS_TYPE info, MDBX_val &data) {
    if (info[1]->IsString()) {
        CustomExternalStringResource::writeTo(Local<String>::Cast(info[1]), &data);
    }
    else if (node::Buffer::HasInstance(info[1])) {
        data.iov_len = node::Buffer::Length(info[1]);
        data.iov_base = node::Buffer::Data(info[1]);
    }
    else if (info[1]->IsNumber()) {
        data.iov_len = sizeof(double);
        data.iov_base = new double;
        auto local = Nan::To<v8::Number>(info[1]).ToLocalChecked();
        *((double*)data.iov_base) = local->Value();
    }
    else if (info[1]->IsBoolean()) {
        data.iov_len = sizeof(double);
        data.iov_base = new bool;
        auto local = Nan::To<v8::Boolean>(info[1]).ToLocalChecked();
        *((bool*)data.iov_base) = local->Value();
    }
    else {
        Nan::ThrowError("Invalid data type.");
    }
}

static void freeDataFromArg1(CursorWrap* cw, Nan::NAN_METHOD_ARGS_TYPE info, MDBX_val &data) {
    if (info[1]->IsString()) {
        delete[] (uint16_t*)data.iov_base;
    }
    else if (node::Buffer::HasInstance(info[1])) {
        // I think the data is owned by the node::Buffer so we don't need to free it - need to clarify
    }
    else if (info[1]->IsNumber()) {
        delete (double*)data.iov_base;
    }
    else if (info[1]->IsBoolean()) {
        delete (bool*)data.iov_base;
    }
    else {
        Nan::ThrowError("Invalid data type.");
    }
}

template<size_t keyIndex, size_t optionsIndex>
inline argtokey_callback_t cursorArgToKey(CursorWrap* cw, Nan::NAN_METHOD_ARGS_TYPE info, MDBX_val &key, bool &keyIsValid) {
    auto keyType = keyTypeFromOptions(info[optionsIndex], cw->keyType);
    return argToKey(info[keyIndex], key, keyType, keyIsValid);
}

NAN_METHOD(CursorWrap::goToKey) {
    if (info.Length() != 1 && info.Length() != 2) {
        return Nan::ThrowError("You called cursor.goToKey with an incorrect number of arguments. Arguments are: key (mandatory), options (optional).");
    }
    return getCommon(info, MDBX_SET_KEY, cursorArgToKey<0, 1>, nullptr, nullptr, nullptr);
}

NAN_METHOD(CursorWrap::goToRange) {
    if (info.Length() != 1 && info.Length() != 2) {
        return Nan::ThrowError("You called cursor.goToRange with an incorrect number of arguments. Arguments are: key (mandatory), options (optional).");
    }
    return getCommon(info, MDBX_SET_RANGE, cursorArgToKey<0, 1>, nullptr, nullptr, nullptr);
}

NAN_METHOD(CursorWrap::goToDup) {
    if (info.Length() != 2 && info.Length() != 3) {
        return Nan::ThrowError("You called cursor.goToDup with an incorrect number of arguments. Arguments are: key (mandatory), data (mandatory), options (optional).");
    }
    return getCommon(info, MDBX_GET_BOTH, cursorArgToKey<0, 2>, fillDataFromArg1, freeDataFromArg1, nullptr);
}

NAN_METHOD(CursorWrap::goToDupRange) {
    if (info.Length() != 2 && info.Length() != 3) {
        return Nan::ThrowError("You called cursor.goToDupRange with an incorrect number of arguments. Arguments are: key (mandatory), data (mandatory), options (optional).");
    }
    return getCommon(info, MDBX_GET_BOTH_RANGE, cursorArgToKey<0, 2>, fillDataFromArg1, freeDataFromArg1, nullptr);
}

void CursorWrap::setupExports(Local<Object> exports) {
    // CursorWrap: Prepare constructor template
    Local<FunctionTemplate> cursorTpl = Nan::New<FunctionTemplate>(CursorWrap::ctor);
    cursorTpl->SetClassName(Nan::New<String>("Cursor").ToLocalChecked());
    cursorTpl->InstanceTemplate()->SetInternalFieldCount(1);
    // CursorWrap: Add functions to the prototype
    cursorTpl->PrototypeTemplate()->Set(Nan::New<String>("close").ToLocalChecked(), Nan::New<FunctionTemplate>(CursorWrap::close));
    cursorTpl->PrototypeTemplate()->Set(Nan::New<String>("getCurrentString").ToLocalChecked(), Nan::New<FunctionTemplate>(CursorWrap::getCurrentString));
    cursorTpl->PrototypeTemplate()->Set(Nan::New<String>("getCurrentStringUnsafe").ToLocalChecked(), Nan::New<FunctionTemplate>(CursorWrap::getCurrentStringUnsafe));
    cursorTpl->PrototypeTemplate()->Set(Nan::New<String>("getCurrentUtf8").ToLocalChecked(), Nan::New<FunctionTemplate>(CursorWrap::getCurrentUtf8));
    cursorTpl->PrototypeTemplate()->Set(Nan::New<String>("getCurrentBinary").ToLocalChecked(), Nan::New<FunctionTemplate>(CursorWrap::getCurrentBinary));
    cursorTpl->PrototypeTemplate()->Set(Nan::New<String>("getCurrentBinaryUnsafe").ToLocalChecked(), Nan::New<FunctionTemplate>(CursorWrap::getCurrentBinaryUnsafe));
    cursorTpl->PrototypeTemplate()->Set(Nan::New<String>("getCurrentNumber").ToLocalChecked(), Nan::New<FunctionTemplate>(CursorWrap::getCurrentNumber));
    cursorTpl->PrototypeTemplate()->Set(Nan::New<String>("getCurrentBoolean").ToLocalChecked(), Nan::New<FunctionTemplate>(CursorWrap::getCurrentBoolean));
    cursorTpl->PrototypeTemplate()->Set(Nan::New<String>("getCurrentIsDatabase").ToLocalChecked(), Nan::New<FunctionTemplate>(CursorWrap::getCurrentIsDatabase));
    cursorTpl->PrototypeTemplate()->Set(Nan::New<String>("goToFirst").ToLocalChecked(), Nan::New<FunctionTemplate>(CursorWrap::goToFirst));
    cursorTpl->PrototypeTemplate()->Set(Nan::New<String>("goToLast").ToLocalChecked(), Nan::New<FunctionTemplate>(CursorWrap::goToLast));
    cursorTpl->PrototypeTemplate()->Set(Nan::New<String>("goToNext").ToLocalChecked(), Nan::New<FunctionTemplate>(CursorWrap::goToNext));
    cursorTpl->PrototypeTemplate()->Set(Nan::New<String>("goToPrev").ToLocalChecked(), Nan::New<FunctionTemplate>(CursorWrap::goToPrev));
    cursorTpl->PrototypeTemplate()->Set(Nan::New<String>("goToKey").ToLocalChecked(), Nan::New<FunctionTemplate>(CursorWrap::goToKey));
    cursorTpl->PrototypeTemplate()->Set(Nan::New<String>("goToRange").ToLocalChecked(), Nan::New<FunctionTemplate>(CursorWrap::goToRange));
    cursorTpl->PrototypeTemplate()->Set(Nan::New<String>("goToFirstDup").ToLocalChecked(), Nan::New<FunctionTemplate>(CursorWrap::goToFirstDup));
    cursorTpl->PrototypeTemplate()->Set(Nan::New<String>("goToLastDup").ToLocalChecked(), Nan::New<FunctionTemplate>(CursorWrap::goToLastDup));
    cursorTpl->PrototypeTemplate()->Set(Nan::New<String>("goToNextDup").ToLocalChecked(), Nan::New<FunctionTemplate>(CursorWrap::goToNextDup));
    cursorTpl->PrototypeTemplate()->Set(Nan::New<String>("goToPrevDup").ToLocalChecked(), Nan::New<FunctionTemplate>(CursorWrap::goToPrevDup));
    cursorTpl->PrototypeTemplate()->Set(Nan::New<String>("goToNextNoDup").ToLocalChecked(), Nan::New<FunctionTemplate>(CursorWrap::goToNextNoDup));
    cursorTpl->PrototypeTemplate()->Set(Nan::New<String>("goToPrevNoDup").ToLocalChecked(), Nan::New<FunctionTemplate>(CursorWrap::goToPrevNoDup));
    cursorTpl->PrototypeTemplate()->Set(Nan::New<String>("goToDup").ToLocalChecked(), Nan::New<FunctionTemplate>(CursorWrap::goToDup));
    cursorTpl->PrototypeTemplate()->Set(Nan::New<String>("goToDupRange").ToLocalChecked(), Nan::New<FunctionTemplate>(CursorWrap::goToDupRange));
    cursorTpl->PrototypeTemplate()->Set(Nan::New<String>("del").ToLocalChecked(), Nan::New<FunctionTemplate>(CursorWrap::del));

    // Set exports
    (void)exports->Set(Nan::GetCurrentContext(), Nan::New<String>("Cursor").ToLocalChecked(), cursorTpl->GetFunction(Nan::GetCurrentContext()).ToLocalChecked());
}
