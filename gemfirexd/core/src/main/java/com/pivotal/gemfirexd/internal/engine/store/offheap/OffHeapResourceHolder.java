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
package com.pivotal.gemfirexd.internal.engine.store.offheap;

import com.gemstone.gemfire.internal.offheap.Releasable;
import com.gemstone.gemfire.internal.offheap.annotations.Retained;
import com.pivotal.gemfirexd.internal.impl.sql.execute.IndexRowToBaseRowResultSet;

/**
 * This interface is implemented by those resultset object which scan the region
 * and obtain retained byte sources. Those resultsets need to cache the
 * OffHeapAddresses so that they can be released by the GemFireTransaction.
 * These resources need to register with the GemFireTransaction .
 * 
 * @see IndexRowToBaseRowResultSet
 * @see BulkTableScanResultSet
 * @see TableScanResultSet
 * 
 * @author asifs
 */
public interface OffHeapResourceHolder extends Releasable {

  public void addByteSource(@Retained OffHeapByteSource byteSource);

  /**
   * Any OffHeapResourceHolder class needs to register itself with
   * GemFireTransaction so that the resources could be released at the end of
   * DML op. This is just to enforce the need for OffHeapResourceHolder to
   * register.
   * 
   * @param releasable
   */
  public void registerWithGemFireTransaction(OffHeapResourceHolder owner);

  /**
   * Returns true if the OffHeapResourceHolder has optimized OHAddressCache (
   * BatchOHAddressCache or SingleOHAddressCache). Returns false if it is of
   * type LinkedListOHAddressCache Whether the ResultSet can have optimized
   * address cache, depends upon the type of DML and its parent resultset nodes.
   * If the value returned is false, it also means that the offheap address wont
   * be released by the call of releasePreviousByteSource.
   * 
   * @return
   */
  public boolean optimizedForOffHeap();

  /**
   * 
   * 
   * @param positionFromEnd
   *          index from the end.
   */
  public void releaseByteSource(int positionFromEnd);

}
