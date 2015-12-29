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

import java.util.Map;

import com.gemstone.gemfire.cache.operations.OperationContext;

/**
 * Encapsulates a {@link com.gemstone.gemfire.cache.operations.OperationContext.OperationCode#KEY_SET} operation for both the
 * pre-operation and post-operation cases.
 * 
 * @author Gester Zhou
 * @since 5.7
 */
public class PutAllOperationContext extends OperationContext {

  /** The set of keys for the operation */
  private Map map;
  
  /** True if this is a post-operation context */
  private boolean postOperation = false;

  /**
   * Constructor for the operation.
   * 
   */
  public PutAllOperationContext(Map map) {
    this.map = map;
  }

  /**
   * Return the operation associated with the <code>OperationContext</code>
   * object.
   * 
   * @return <code>OperationCode.PUTALL</code>.
   */
  @Override
  public OperationCode getOperationCode() {
    return OperationCode.PUTALL;
  }

  /**
   * True if the context is for post-operation.
   */
  @Override
  public boolean isPostOperation() {
    return this.postOperation;
  }

  /**
   * Set the post-operation flag to true.
   */
  protected void setPostOperation() {
    this.postOperation = true;
  }

  /**
   * get the authorized map.
   */
  public Map getMap() {
    return this.map;
  }

  /**
   * set the authorized map.
   */
  public void setMap(Map map) {
    this.map = map;
  }
  /**
   * get the isObject for current entry.
   */
//  public boolean getIsObject(Object key) {
//	Boolean bool_obj = (Boolean)this.isObjectMap.get(key); 
//    return bool_obj.booleanValue();
//  }

  /**
   * set the entry in the map.
   */
//  public void setMapEntry(Object key, byte[] serializedValue, boolean isObject) {
//	  // no need to modify the map when isObject == false
//	if (isObject) {
//		map.put(key, CachedDeserializableFactory.create(serializedValue));
//	}
//  }

}
