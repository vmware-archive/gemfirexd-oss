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

package com.gemstone.gemfire.cache.query;

import com.gemstone.gemfire.cache.CacheRuntimeException;
/**
 * Thrown if an error occurs while updating query indexes during
 * region modification.
 *
 * @author Eric Zoerner
 * @since 4.0
 */
public class IndexMaintenanceException extends CacheRuntimeException {
private static final long serialVersionUID = 3326023943226474039L;
  
  /**
   * Creates a new instance of <code>IndexMaintenanceException</code> without detail message.
   */
  public IndexMaintenanceException() {
  }
  
  
  /**
   * Constructs an instance of <code>IndexMaintenanceException</code> with the specified detail message.
   * @param msg the detail message.
   */
  public IndexMaintenanceException(String msg) {
    super(msg);
  }
  
  
  /**
   * Constructs an instance of <code>IndexMaintenanceException</code> with the specified detail message
   * and cause.
   * @param msg the detail message
   * @param cause the causal Throwable
   */
  public IndexMaintenanceException(String msg, Throwable cause) {
    super(msg, cause);
  }
  
  /**
   * Constructs an instance of <code>IndexMaintenanceException</code> with the specified cause.
   * @param cause the causal Throwable
   */
  public IndexMaintenanceException(Throwable cause) {
    super(cause);
  }
  
  
}
