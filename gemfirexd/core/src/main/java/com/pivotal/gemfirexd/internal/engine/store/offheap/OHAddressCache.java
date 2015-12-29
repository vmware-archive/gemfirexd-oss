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
import com.pivotal.gemfirexd.internal.impl.sql.execute.TableScanResultSet;

/**
 * Cache used to store OffHeap addresses retained during the scan of the table.
 * These cached addresses will be released either at the end of the DML
 * operation or during filtering of rows or on movement of the top level
 * iterator. The concrete implementations of it are
 * BulkTableScanResultSet.BatchOHAddressCache,
 * TableScanResultSet.SingleOHAddressCache and LinkedListOHAddressCache
 * 
 * @See {@link LinkedListOHAddressCache}
 * 
 * @author asifs
 * 
 */
public interface OHAddressCache extends Releasable {

  /**
   * 
   * @param address
   *          to store . For non offheap byte source it is 0
   */
  public void put(long address);

  /**
   * 
   * 
   * @param positionFromEnd
   *          index from the end of the collection. This position is given as
   *          per the requirement of the LinkedListOHAddressCache, where the
   *          elements scanned are inserted on LIFO basis ( i.e the last address
   *          retained is added at 0 th position of the list)
   */
  public void releaseByteSource(int positionFromEnd);

}
