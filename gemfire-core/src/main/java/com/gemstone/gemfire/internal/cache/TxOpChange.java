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

package com.gemstone.gemfire.internal.cache;

/**
 * This will represent the change in the transactional state corresponding to one dml op
 * 
 * @author kneeraj
 *
 */
public interface TxOpChange {

  public static enum TxOpType {
    INSERT,
    UPDATE,
    DESTROY,
    SELECT;
  }

  public TxOpType getTxOpType();
  
  /** Return the change which this op brought. 
   * If it is an update it will return the delta. If a destroy/select then nothing.
   * If it is an insert then the full row
   **/
  public Object getChange();
  
  /** The sequence number of the op from the beginning of the tx **/
  public int getSequenceNumber();
}
