
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
#include <stdio.h>

static thread_local char* globalUnsafePtr;
static thread_local size_t globalUnsafeSize;
static thread_local Persistent<Object>* globalUnsafeBuffer;

void setupExportMisc(Local<Object> exports) {
    Local<Object> versionObj = Nan::New<Object>();

    /*int major, minor, patch;
    char *str = mdbx_version(&major, &minor, &patch);
    Local<Context> context = Nan::GetCurrentContext();
    (void)versionObj->Set(context, Nan::New<String>("versionString").ToLocalChecked(), Nan::New<String>(str).ToLocalChecked());
    (void)versionObj->Set(context, Nan::New<String>("major").ToLocalChecked(), Nan::New<Integer>(major));
    (void)versionObj->Set(context, Nan::New<String>("minor").ToLocalChecked(), Nan::New<Integer>(minor));
    (void)versionObj->Set(context, Nan::New<String>("patch").ToLocalChecked(), Nan::New<Integer>(patch));
    (void)exports->Set(context, Nan::New<String>("version").ToLocalChecked(), versionObj);*/
    Nan::SetMethod(exports, "getLastVersion", getLastVersion);
    Nan::SetMethod(exports, "setLastVersion", setLastVersion);
    Nan::SetMethod(exports, "bufferToKeyValue", bufferToKeyValue);
    Nan::SetMethod(exports, "keyValueToBuffer", keyValueToBuffer);
    globalUnsafeBuffer = new Persistent<Object>();
    makeGlobalUnsafeBuffer(8);
    fixedKeySpace = new KeySpace(true);
}

void setFlagFromValue(int *flags, int flag, const char *name, bool defaultValue, Local<Object> options) {
    Local<Context> context = Nan::GetCurrentContext();
    Local<Value> opt = options->Get(context, Nan::New<String>(name).ToLocalChecked()).ToLocalChecked();
    #if NODE_VERSION_AT_LEAST(12,0,0)
    if (opt->IsBoolean() ? opt->BooleanValue(Isolate::GetCurrent()) : defaultValue) {
    #else
    if (opt->IsBoolean() ? opt->BooleanValue(context).FromJust() : defaultValue) {
    #endif
        *flags |= flag;
    }
}

NodeLmdbxKeyType keyTypeFromOptions(const Local<Value> &val, NodeLmdbxKeyType defaultKeyType) {
    if (!val->IsObject()) {
        return defaultKeyType;
    }
    auto obj = Local<Object>::Cast(val);

    NodeLmdbxKeyType keyType = defaultKeyType;
    int keyIsUint32 = 0;
    int keyIsBuffer = 0;
    int keyIsString = 0;
    
    setFlagFromValue(&keyIsUint32, 1, "keyIsUint32", false, obj);
    setFlagFromValue(&keyIsString, 1, "keyIsString", false, obj);
    setFlagFromValue(&keyIsBuffer, 1, "keyIsBuffer", false, obj);
    
    const char *keySpecificationErrorText = "You can't specify multiple key types at once. Either set keyIsUint32, or keyIsBuffer or keyIsString (default).";
    
    if (keyIsUint32) {
        keyType = NodeLmdbxKeyType::Uint32Key;
        if (keyIsBuffer || keyIsString) {
            Nan::ThrowError(keySpecificationErrorText);
            return NodeLmdbxKeyType::InvalidKey;
        }
    }
    else if (keyIsBuffer) {
        keyType = NodeLmdbxKeyType::BinaryKey;
        
        if (keyIsUint32 || keyIsString) {
            Nan::ThrowError(keySpecificationErrorText);
            return NodeLmdbxKeyType::InvalidKey;
        }
    }
    else if (keyIsString) {
        keyType = NodeLmdbxKeyType::StringKey;
    }
    
    return keyType;
}

NodeLmdbxKeyType inferKeyType(const Local<Value> &val) {
    if (val->IsString()) {
        return NodeLmdbxKeyType::StringKey;
    }
    if (val->IsUint32()) {
        return NodeLmdbxKeyType::Uint32Key;
    }
    if (node::Buffer::HasInstance(val)) {
        return NodeLmdbxKeyType::BinaryKey;
    }
    
    return NodeLmdbxKeyType::InvalidKey;
}

NodeLmdbxKeyType inferAndValidateKeyType(const Local<Value> &key, const Local<Value> &options, NodeLmdbxKeyType dbiKeyType, bool &isValid) {
    auto keyType = keyTypeFromOptions(options, NodeLmdbxKeyType::DefaultKey);
    auto inferredKeyType = inferKeyType(key);
    isValid = false;
    
    if (keyType != NodeLmdbxKeyType::DefaultKey && inferredKeyType != keyType) {
        Nan::ThrowError("Specified key type doesn't match the key you gave.");
        return NodeLmdbxKeyType::InvalidKey;
    }
    else {
        keyType = inferredKeyType;
    }
    if (dbiKeyType == NodeLmdbxKeyType::Uint32Key && keyType != NodeLmdbxKeyType::Uint32Key) {
        Nan::ThrowError("You specified keyIsUint32 on the Dbi, so you can't use other key types with it.");
        return NodeLmdbxKeyType::InvalidKey;
    }
    
    isValid = true;
    return keyType;
}

argtokey_callback_t argToKey(const Local<Value> &val, MDBX_val &key, NodeLmdbxKeyType keyType, bool &isValid) {
    isValid = false;

    if (keyType == NodeLmdbxKeyType::DefaultKey) {
        isValid = valueToMDBXKey(val, key, *fixedKeySpace);
    } else if (keyType == NodeLmdbxKeyType::StringKey) {
        if (!val->IsString()) {
            Nan::ThrowError("Invalid key. Should be a string. (Specified with env.openDbi)");
            return nullptr;
        }
        
        isValid = true;
        CustomExternalStringResource::writeTo(Local<String>::Cast(val), &key);
        return ([](MDBX_val &key) -> void {
            delete[] (uint16_t*)key.iov_base;
        });
    }
    else if (keyType == NodeLmdbxKeyType::Uint32Key) {
        if (!val->IsUint32()) {
            Nan::ThrowError("Invalid key. Should be an unsigned 32-bit integer. (Specified with env.openDbi)");
            return nullptr;
        }
        
        isValid = true;
        uint32_t* uint32Key = new uint32_t;
        *uint32Key = val->Uint32Value(Nan::GetCurrentContext()).FromJust();
        key.iov_len = sizeof(uint32_t);
        key.iov_base = uint32Key;

        return ([](MDBX_val &key) -> void {
            delete (uint32_t*)key.iov_base;
        });
    }
    else if (keyType == NodeLmdbxKeyType::BinaryKey) {
        if (!node::Buffer::HasInstance(val)) {
            Nan::ThrowError("Invalid key. Should be a Buffer. (Specified with env.openDbi)");
            return nullptr;
        }
        
        isValid = true;
        key.iov_len = node::Buffer::Length(val);
        key.iov_base = node::Buffer::Data(val);
        
        return nullptr;
    }
    else if (keyType == NodeLmdbxKeyType::InvalidKey) {
        Nan::ThrowError("Invalid key type. This might be a bug in node-lmdbx.");
    }
    else {
        Nan::ThrowError("Unknown key type. This is a bug in node-lmdbx.");
    }

    return nullptr;
}

Local<Value> keyToHandle(MDBX_val &key, NodeLmdbxKeyType keyType) {
    switch (keyType) {
    case NodeLmdbxKeyType::DefaultKey:
        return MDBXKeyToValue(key);
    case NodeLmdbxKeyType::Uint32Key:
        return Nan::New<Integer>(*((uint32_t*)key.iov_base));
    case NodeLmdbxKeyType::BinaryKey:
        return valToBinary(key);
    case NodeLmdbxKeyType::StringKey:
        return valToString(key);
    default:
        Nan::ThrowError("Unknown key type. This is a bug in node-lmdbx.");
        return Nan::Undefined();
    }
}

Local<Value> valToStringUnsafe(MDBX_val &data) {
    auto resource = new CustomExternalOneByteStringResource(&data);
    auto str = Nan::New<v8::String>(resource);

    return str.ToLocalChecked();
}

Local<Value> valToUtf8(MDBX_val &data) {
    //const uint8_t *buffer = (const uint8_t*)(data.iov_base);
    //Isolate *isolate = Isolate::GetCurrent();
    //auto str = v8::String::NewFromOneByte(isolate, buffer, v8::NewStringType::kNormal, data.iov_len);
    const char *buffer = (const char*)(data.iov_base);
    auto str = Nan::New<v8::String>(buffer, data.iov_len);

    return str.ToLocalChecked();
}

Local<Value> valToString(MDBX_val &data) {
    // UTF-16 buffer
    const uint16_t *buffer = reinterpret_cast<const uint16_t*>(data.iov_base);
    // Number of UTF-16 code points
    size_t n = data.iov_len / sizeof(uint16_t);
    
    // Check zero termination
    if (n < 1 || buffer[n - 1] != 0) {
        Nan::ThrowError("Invalid zero-terminated UTF-16 string");
        return Nan::Undefined();
    }
    
    size_t length = n - 1;
    auto str = Nan::New<v8::String>(buffer, length);

    return str.ToLocalChecked();
}

Local<Value> valToBinary(MDBX_val &data) {
    return Nan::CopyBuffer(
        (char*)data.iov_base,
        data.iov_len
    ).ToLocalChecked();
}

void makeGlobalUnsafeBuffer(size_t size) {
    globalUnsafeSize = size;
    Local<Object> newBuffer = Nan::NewBuffer(size).ToLocalChecked();
    globalUnsafePtr = node::Buffer::Data(newBuffer);
    globalUnsafeBuffer->Reset(Isolate::GetCurrent(), newBuffer);
}

Local<Value> valToBinaryUnsafe(MDBX_val &data) {
    DbiWrap* dw = currentDb;
    Compression* compression = dw->compression;
    if (compression) {
        if (data.iov_base == compression->decompressTarget) {
            // already decompressed to the target, nothing more to do
        } else {
            if (data.iov_len > compression->decompressSize) {
                compression->expand(data.iov_len);
            }
            // copy into the buffer target
            memcpy(compression->decompressTarget, data.iov_base, data.iov_len);
        }
        dw->setUnsafeBuffer(compression->decompressTarget, compression->unsafeBuffer);
    } else {
        if (data.iov_len > globalUnsafeSize) {
            // TODO: Provide a direct reference if for really large blocks, but we do that we need to detach that in the next turn
            /* if(data.iov_len > 64000) {
                dw->SetUnsafeBuffer(data.iov_base, data.iov_len);
                return Nan::New<Number>(data.iov_len);
            }*/
            makeGlobalUnsafeBuffer(data.iov_len * 2);
        }
        memcpy(globalUnsafePtr, data.iov_base, data.iov_len);
        dw->setUnsafeBuffer(globalUnsafePtr, *globalUnsafeBuffer);
    }
    return Nan::New<Number>(data.iov_len);
}

Local<Value> valToNumber(MDBX_val &data) {
    return Nan::New<Number>(*((double*)data.iov_base));
}

Local<Value> valToBoolean(MDBX_val &data) {
    return Nan::New<Boolean>(*((bool*)data.iov_base));
}

Local<Value> getVersionAndUncompress(MDBX_val &data, DbiWrap* dw, Local<Value> (*successFunc)(MDBX_val&)) {
    //fprintf(stdout, "uncompressing %u\n", compressionThreshold);
    unsigned char* charData = (unsigned char*) data.iov_base;
    if (dw->hasVersions) {
        lastVersion = *((double*) charData);
//        fprintf(stderr, "getVersion %u\n", lastVersion);
        charData = charData + 8;
        data.iov_base = charData;
        data.iov_len -= 8;
    }
    if (data.iov_len == 0) {
        currentDb = dw;
        return successFunc(data);
    }
    unsigned char statusByte = dw->compression ? charData[0] : 0;
        //fprintf(stdout, "uncompressing status %X\n", statusByte);
    if (statusByte >= 250) {
        bool isValid;
        dw->compression->decompress(data, isValid);
        if (!isValid)
            return Nan::Null();
    }
    currentDb = dw;
    return successFunc(data);
}

NAN_METHOD(getLastVersion) {
    if (lastVersion == NO_EXIST_VERSION)
        return info.GetReturnValue().Set(Nan::Null());
    return info.GetReturnValue().Set(Nan::New<Number>(lastVersion));
}
void setLastVersion(double version) {
    lastVersion = version;
}
NAN_METHOD(setLastVersion) {
    lastVersion = Nan::To<v8::Number>(info[0]).ToLocalChecked()->Value();
}

void throwLmdbxError(int rc) {
    auto err = Nan::Error(mdbx_strerror(rc));
    (void)err.As<Object>()->Set(Nan::GetCurrentContext(), Nan::New("code").ToLocalChecked(), Nan::New(rc));
    return Nan::ThrowError(err);
}

void consoleLog(const char *msg) {
    Local<String> str = Nan::New("console.log('").ToLocalChecked();
    //str = String::Concat(str, Nan::New<String>(msg).ToLocalChecked());
    //str = String::Concat(str, Nan::New("');").ToLocalChecked());

    Local<Script> script = Nan::CompileScript(str).ToLocalChecked();
    Nan::RunScript(script);
}

void consoleLog(Local<Value> val) {
    Local<String> str = Nan::New<String>("console.log('").ToLocalChecked();
    //str = String::Concat(str, Local<String>::Cast(val));
    //str = String::Concat(str, Nan::New<String>("');").ToLocalChecked());

    Local<Script> script = Nan::CompileScript(str).ToLocalChecked();
    Nan::RunScript(script);
}

void consoleLogN(int n) {
    char c[20];
    memset(c, 0, 20 * sizeof(char));
    sprintf(c, "%d", n);
    consoleLog(c);
}

void writeValueToEntry(const Local<Value> &value, MDBX_val *val) {
    if (value->IsString()) {
        Local<String> str = Local<String>::Cast(value);
        int strLength = str->Length();
        // an optimized guess at buffer length that works >99% of time and has good byte alignment
        int byteLength = str->IsOneByte() ? strLength :
            (((strLength >> 3) + ((strLength + 116) >> 6)) << 3);
        char *data = new char[byteLength];
        int utfWritten = 0;
#if NODE_VERSION_AT_LEAST(11,0,0)
        int bytes = str->WriteUtf8(Isolate::GetCurrent(), data, byteLength, &utfWritten, v8::String::WriteOptions::NO_NULL_TERMINATION);
#else
        int bytes = str->WriteUtf8(data, byteLength, &utfWritten, v8::String::WriteOptions::NO_NULL_TERMINATION);
#endif        
        if (utfWritten < strLength) {
            // we didn't allocate enough memory, need to expand
            delete[] data;
            byteLength = strLength * 3;
            data = new char[byteLength];
#if NODE_VERSION_AT_LEAST(11,0,0)
            bytes = str->WriteUtf8(Isolate::GetCurrent(), data, byteLength, &utfWritten, v8::String::WriteOptions::NO_NULL_TERMINATION);
#else
            bytes = str->WriteUtf8(data, byteLength, &utfWritten, v8::String::WriteOptions::NO_NULL_TERMINATION);
#endif        
        }
        val->iov_base = data;
        val->iov_len = bytes;
        //fprintf(stdout, "size of data with string %u header size %u\n", val->iov_len, headerSize);
    } else {
        Nan::ThrowError("Unknown value type");
    }
}

int putWithVersion(MDBX_txn *   txn,
        MDBX_dbi     dbi,
        MDBX_val *   key,
        MDBX_val *   data,
        unsigned int    flags, double version) {
    // leave 8 header bytes available for version and copy in with reserved memory
    char* sourceData = (char*) data->iov_base;
    int size = data->iov_len;
    data->iov_len = size + 8;
    int rc = mdbx_put(txn, dbi, key, data, MDBX_RESERVE);
    if (rc == 0) {
        // if put is successful, data->iov_base will point into the database where we copy the data to
        memcpy((char*) data->iov_base + 8, sourceData, size);
        *((double*) data->iov_base) = version;
    }
    data->iov_base = sourceData; // restore this so that if it points to data that needs to be freed, it points to the right place
    return rc;
}


void CustomExternalStringResource::writeTo(Local<String> str, MDBX_val *val) {
    unsigned int l = str->Length() + 1;
    uint16_t *d = new uint16_t[l];
    #if NODE_VERSION_AT_LEAST(12,0,0)
    str->Write(Isolate::GetCurrent(), d);
    #else
    str->Write(d);
    #endif
    d[l - 1] = 0;

    val->iov_base = d;
    val->iov_len = l * sizeof(uint16_t);
}

CustomExternalStringResource::CustomExternalStringResource(MDBX_val *val) {
    // The UTF-16 data
    this->d = (uint16_t*)(val->iov_base);
    // Number of UTF-16 characters in the string
    size_t n = val->iov_len / sizeof(uint16_t);
    // Silently generate a 0 length if length invalid
    this->l = n ? (n - 1) : 0;
}

CustomExternalStringResource::~CustomExternalStringResource() { }

void CustomExternalStringResource::Dispose() {
    // No need to do anything, the data is owned by LMDBX, not us
    
    // But actually need to delete the string resource itself:
    // the docs say that "The default implementation will use the delete operator."
    // while initially I thought this means using delete on the string,
    // apparently they meant just calling the destructor of this class.
    delete this;
}

const uint16_t *CustomExternalStringResource::data() const {
    return this->d;
}

size_t CustomExternalStringResource::length() const {
    return this->l;
}

CustomExternalOneByteStringResource::CustomExternalOneByteStringResource(MDBX_val *val) {
    // The Latin data
    this->d = (char*)(val->iov_base);
    // Number of Latin characters in the string
    this->l = val->iov_len;
}

CustomExternalOneByteStringResource::~CustomExternalOneByteStringResource() { }

void CustomExternalOneByteStringResource::Dispose() {
    // No need to do anything, the data is owned by LMDBX, not us
    
    // But actually need to delete the string resource itself:
    // the docs say that "The default implementation will use the delete operator."
    // while initially I thought this means using delete on the string,
    // apparently they meant just calling the destructor of this class.
    delete this;
}

const char *CustomExternalOneByteStringResource::data() const {
    return this->d;
}

size_t CustomExternalOneByteStringResource::length() const {
    return this->l;
}
