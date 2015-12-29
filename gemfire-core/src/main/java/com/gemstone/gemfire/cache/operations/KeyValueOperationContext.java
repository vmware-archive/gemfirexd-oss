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

package com.gemstone.gemfire.cache.operations;


/**
 * Encapsulates a region operation that requires both key and serialized value
 * for the pre-operation and post-operation cases.
 * 
 * @author Sumedh Wale
 * @since 5.5
 */
public abstract class KeyValueOperationContext extends KeyOperationContext {

  /**
   * The value of the create/update operation.
   * @since 6.5
   */
  protected Object value;
  
  /**
   * The serialized value of the create/update operation.
   */
  private byte[] serializedValue;

  /**
   * True when the serialized object is a normal object; false when it is a raw
   * byte array.
   */
  private boolean isObject;

  /**
   * Constructor for the operation.
   * 
   * @param key
   *                the key for this operation
   * @param value
   *                the value for this operation
   * @param isObject
   *                true when the value is an object; false when it is a raw
   *                byte array
   * @since 6.5
   */
  public KeyValueOperationContext(Object key, Object value,
      boolean isObject) {
    super(key);
    setValue(value,isObject);
    //this.value = value;
    // this.isObject = isObject;
  }

  /**
   * Constructor for the operation.
   * 
   * @param key
   *                the key for this operation
   * @param value
   *                the value for this operation
   * @param isObject
   *                true when the value is an object; false when it is a raw
   *                byte array
   * @param postOperation
   *                true if the context is at the time of sending updates
   * @since 6.5
   */
  public KeyValueOperationContext(Object key, Object value,
      boolean isObject, boolean postOperation) {
    super(key, postOperation);
    setValue(value,isObject);
    //this.value = value;
    //this.isObject = isObject;
  }

  /**
   * Get the serialized value for this operation.
   * 
   * @return the serialized value for this operation.
   */
  public byte[] getSerializedValue() {
    return this.serializedValue;
  }

  /**
   * Get the value for this operation.
   * 
   * @return the value for this operation.
   * @since 6.5
   */
  public Object getValue() {
    
    if (serializedValue != null) {
      return serializedValue;
    }
    else {
      return value;
    }
  }
  
  /**
   * Return true when the value is an object and not a raw byte array.
   * 
   * @return true when the value is an object; false when it is a raw byte array
   */
  public boolean isObject() {
    return this.isObject;
  }

  /**
   * Set the serialized value object for this operation.
   * 
   * @param serializedValue
   *                the serialized value for this operation
   * @param isObject
   *                true when the value is an object; false when it is a raw
   *                byte array
   */
  public void setSerializedValue(byte[] serializedValue, boolean isObject) {
    this.serializedValue = serializedValue;
    this.value = null;
    this.isObject = isObject;
  }
  

  /**
   * Set the result value of the object for this operation.
   * 
   * @param value
   *                the result of this operation; can be a serialized byte array
   *                or a deserialized object
   * @param isObject
   *                true when the value is an object (either serialized or
   *                deserialized); false when it is a raw byte array
   * @since 6.5
   */
  public void setValue(Object value, boolean isObject) {
    if (value instanceof byte[]) {
      setSerializedValue((byte[])value, isObject);
    }
    else {
      this.serializedValue = null;
      this.value = value;
      this.isObject = isObject;
    }
  }
  
}
