/*
 * Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.gemstone.gemfire.internal.offheap;

import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.DSCODE;
import com.gemstone.gemfire.internal.LogWriterImpl;
import com.gemstone.gemfire.internal.cache.BytesAndBitsForCompactor;
import com.gemstone.gemfire.internal.cache.CachedDeserializableFactory;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.offheap.annotations.Unretained;

/**
 * This abstract class is intended to be used by {@link MemoryChunk} implementations that also want
 * to be a CachedDeserializable.
 * 
 * @author darrel
 *
 */
public abstract class OffHeapCachedDeserializable implements MemoryChunkWithRefCount {
  public abstract void setSerializedValue(byte[] value);
  @Override
  public abstract byte[] getSerializedValue();
  @Override
  public abstract int getSizeInBytes();
  @Override
  public abstract int getValueSizeInBytes();
  @Override
  public abstract Object getDeserializedValue(Region r, RegionEntry re);

  @Override
  public Object getValueAsDeserializedHeapObject() {
    return getDeserializedValue(null, null);
  }
  
  @Override
  public byte[] getValueAsHeapByteArray() {
    if (isSerialized()) {
      return getSerializedValue();
    } else {
      return (byte[])getDeserializedForReading();
    }
  }
  
  @Override
  public final Object getDeserializedForReading() {
    return getDeserializedValue(null, null);
  }

  @Override
  public String getStringForm() {
    try {
      return LogWriterImpl.forceToString(getDeserializedForReading());
    } catch (RuntimeException ex) {
      return "Could not convert object to string because " + ex;
    }
  }

  @Override
  public Object getDeserializedWritableCopy(Region r, RegionEntry re) {
    return getDeserializedValue(null, null);
  }

  @Override
  public Object getValue() {
    if (isSerialized()) {
      return getSerializedValue();
    } else {
      throw new IllegalStateException("Can not call getValue on StoredObject that is not serialized");
    }
  }

  @Override
  public void writeValueAsByteArray(DataOutput out) throws IOException {
    DataSerializer.writeByteArray(getSerializedValue(), out);
  }

  @Override
  public void fillSerializedValue(BytesAndBitsForCompactor wrapper,
      byte userBits) {
    byte[] value = this.getValueAsHeapByteArray();
    wrapper.setData(value, userBits, value.length, true);
  }
  
  String getShortClassName() {
    String cname = getClass().getName();
    return cname.substring(getClass().getPackage().getName().length()+1);
  }

  @Override
  public String toString() {
    return getShortClassName()+"@"+this.hashCode();
  }
  @Override
  public void sendTo(DataOutput out) throws IOException {
    Object objToSend;
    if (CachedDeserializableFactory.preferObject()) {
      objToSend = getValueAsDeserializedHeapObject();
    }
    else if (isSerialized()) {
      objToSend = CachedDeserializableFactory.create(getSerializedValue()); // deserialized as a cd
    } else {
      objToSend = (byte[]) getDeserializedForReading(); // deserialized as a byte[]
    }
    DataSerializer.writeObject(objToSend, out);
  }
  public boolean checkDataEquals(@Unretained OffHeapCachedDeserializable other) {
    if (this == other) {
      return true;
    }
    if (isSerialized() != other.isSerialized()) {
      return false;
    }
    int mySize = getValueSizeInBytes();
    if (mySize != other.getValueSizeInBytes()) {
      return false;
    }
    // We want to be able to do this operation without copying any of the data into the heap.
    // Hopefully the jvm is smart enough to use our stack for this short lived array.
    final byte[] dataCache1 = new byte[1024];
    final byte[] dataCache2 = new byte[dataCache1.length];
    int i;
    // inc it twice since we are reading two different off-heap objects
    SimpleMemoryAllocatorImpl.getAllocator().getStats().incReads();
    SimpleMemoryAllocatorImpl.getAllocator().getStats().incReads();
    for (i=0; i < mySize-(dataCache1.length-1); i+=dataCache1.length) {
      this.readBytes(i, dataCache1);
      other.readBytes(i, dataCache2);
      for (int j=0; j < dataCache1.length; j++) {
        if (dataCache1[j] != dataCache2[j]) {
          return false;
        }
      }
    }
    int bytesToRead = mySize-i;
    if (bytesToRead > 0) {
      // need to do one more read which will be less than dataCache.length
      this.readBytes(i, dataCache1, 0, bytesToRead);
      other.readBytes(i, dataCache2, 0, bytesToRead);
      for (int j=0; j < bytesToRead; j++) {
        if (dataCache1[j] != dataCache2[j]) {
          return false;
        }
      }
    }
    return true;
  }
  
  public boolean isSerializedPdxInstance() {
    byte dsCode = this.readByte(0);
    return dsCode == DSCODE.PDX || dsCode == DSCODE.PDX_ENUM || dsCode == DSCODE.PDX_INLINE_ENUM;
  }
  
  public boolean checkDataEquals(byte[] serializedObj) {
    // caller was responsible for checking isSerialized
    int mySize = getValueSizeInBytes();
    if (mySize != serializedObj.length) {
      return false;
    }
    // We want to be able to do this operation without copying any of the data into the heap.
    // Hopefully the jvm is smart enough to use our stack for this short lived array.
    final byte[] dataCache = new byte[1024];
    int idx=0;
    int i;
    SimpleMemoryAllocatorImpl.getAllocator().getStats().incReads();
    for (i=0; i < mySize-(dataCache.length-1); i+=dataCache.length) {
      this.readBytes(i, dataCache);
      for (int j=0; j < dataCache.length; j++) {
        if (dataCache[j] != serializedObj[idx++]) {
          return false;
        }
      }
    }
    int bytesToRead = mySize-i;
    if (bytesToRead > 0) {
      // need to do one more read which will be less than dataCache.length
      this.readBytes(i, dataCache, 0, bytesToRead);
      for (int j=0; j < bytesToRead; j++) {
        if (dataCache[j] != serializedObj[idx++]) {
          return false;
        }
      }
    }
    return true;
  }
}
