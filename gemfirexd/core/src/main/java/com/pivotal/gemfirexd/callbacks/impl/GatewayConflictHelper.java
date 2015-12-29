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

package com.pivotal.gemfirexd.callbacks.impl;

/**
 * GatewayConflictHelper is used by an GatewayConflictResolver to decide what to
 * do with an event received from another site
 * 
 * @author sjigyasu
 */
public interface GatewayConflictHelper {
  /**
   * Skips applying the event 
   */
  public void disallowEvent();

  /**
   * Sets value of column at given index in the event's row. Restrictions to
   * changing column values though user DMLs also apply here. For example,
   * columns which are primary keys or columns which partipate in partitioning
   * cannot be changed. An attempt to do so will throw an exception.
   * 
   * @param index
   * @param value
   */
  public void setColumnValue(int index, Object value) throws Exception;
}
