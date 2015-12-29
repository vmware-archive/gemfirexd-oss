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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache;

import static com.gemstone.gemfire.internal.offheap.annotations.OffHeapIdentifier.ENTRY_EVENT_NEW_VALUE;

import com.gemstone.gemfire.internal.offheap.annotations.Retained;
import com.gemstone.gemfire.internal.offheap.annotations.Unretained;

/**
 * @author sbawaska
 * @author rdubey
 *
 */
public class KeyInfo {

  // Rahul: This class should actually be renamed as RoutingInfo or BucketIdInfo
  // since that is exactly what an instance of this class is.

  public static final int UNKNOWN_BUCKET = -1;

  protected Object key;
  protected Object callbackArg;
  protected int bucketId;

  // Rahul: The value field is here since Gfxd Partition resolver also relies on
  // the value part to calculate the routing object if the table is not
  // partitioned on primary key.
  @Retained(ENTRY_EVENT_NEW_VALUE)
  protected Object newValue;

  /** for deserialization */
  protected KeyInfo() {
  }

  public KeyInfo(Object key, Object value, Object callbackArg) {
    this.key = key;
    this.callbackArg = callbackArg;
    this.bucketId = UNKNOWN_BUCKET;
    this.newValue =  value;
  }

  public KeyInfo(Object key, Object callbackArg, int bucketId) {
    this.key = key;
    this.callbackArg = callbackArg;
    this.bucketId = bucketId;
    this.newValue = null;
  }

  public KeyInfo(KeyInfo keyInfo) {
    setKeyInfo(keyInfo);
  }

  public final Object getKey() {
    return this.key;
  }

  public final Object getCallbackArg() {
    return this.callbackArg;
  }

  @Unretained(ENTRY_EVENT_NEW_VALUE)
  public Object getValue() {
    return this.newValue;
  }

  public final int getBucketId() {
    return this.bucketId;
  }

  public final void setBucketId(int id) {
    this.bucketId = id;
  }

  public void setCallbackArgument(final Object callbackArg) {
    this.callbackArg = callbackArg;
  }

  public final void setKey(Object key) {
    this.key = key;
  }

  public final void setKeyInfo(KeyInfo keyInfo) {
    this.key = keyInfo.key;
    this.callbackArg = keyInfo.callbackArg;
    this.bucketId = keyInfo.bucketId;
    this.newValue = keyInfo.newValue;
  }

  @Override
  public String toString() {
    return "(key=" + key + ",value=" + newValue + ",bucketId=" + bucketId + ')';
  }
}
