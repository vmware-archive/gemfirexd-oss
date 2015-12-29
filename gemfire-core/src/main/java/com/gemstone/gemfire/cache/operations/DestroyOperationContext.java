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
 * Encapsulates a {@link com.gemstone.gemfire.cache.operations.OperationContext.OperationCode#DESTROY} region operation having the key
 * object for both the pre-operation case and for post-operation updates.
 * 
 * @author Sumedh Wale
 * @since 5.5
 */
public class DestroyOperationContext extends KeyOperationContext {

  /**
   * Constructor for the operation.
   * 
   * @param key
   *                the key for this operation
   */
  public DestroyOperationContext(Object key) {
    super(key);
  }

  /**
   * Constructor for the operation to use for post-operation in updates.
   * 
   * @param key
   *                the key for this operation
   * @param postOperation
   *                true to set the post-operation flag
   */
  public DestroyOperationContext(Object key, boolean postOperation) {
    super(key, postOperation);
  }

  /**
   * Return the operation associated with the <code>OperationContext</code>
   * object.
   * 
   * @return <code>OperationCode.DESTROY</code>.
   */
  @Override
  public OperationCode getOperationCode() {
    return OperationCode.DESTROY;
  }

}
