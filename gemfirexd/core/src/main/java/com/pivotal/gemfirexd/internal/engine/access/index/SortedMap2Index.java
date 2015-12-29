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

package com.pivotal.gemfirexd.internal.engine.access.index;

import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.internal.concurrent.ConcurrentSkipListMap;
import com.gemstone.gemfire.internal.util.ArrayUtils;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.access.index.key.IndexKeyComparator;
import com.pivotal.gemfirexd.internal.engine.access.index.key.OffHeapIndexKeyComparator;
import com.pivotal.gemfirexd.internal.engine.sql.catalog.ExtraIndexInfo;
import com.pivotal.gemfirexd.internal.engine.store.CompactCompositeIndexKey;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.store.access.conglomerate.TransactionManager;
import com.pivotal.gemfirexd.internal.iapi.store.raw.LockingPolicy;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Transaction;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;

/**
 * Implements the local index based on the <tt>ConcurrentSkipListMap</tt>(CSLM).
 * <br>
 * 
 * @purpose This index is supposed to be adopted by the range select query for
 *          scanning the base table. <br>
 *          <p>
 *          The CSLM adopts the lock-free mechanism to improve the concurrency
 *          by avoiding the blocking among contention threads. The
 *          <em>expected</em> average time cost of the insert, search, and
 *          delete operations is <b> O(lgn) </b>.
 *          <p>
 *          As the <b>first</b> release does not consider the issue on
 *          transaction and locking, no more mechanism is provided here to
 *          implement the isolation level of transaction. The current
 *          implementation depends on the CSLM to guarantee the thread safety,
 *          but it only supports the <b>read uncommitted</b> isolation level.
 *          <p>
 *          In addition, it cannot ensure that the row referenced by
 *          {@link#RowLocation RowLocation} from this index really exists in the
 *          base table. e.g. one thread deletes the row in the base table and
 *          not deletes the corresponding item in this index yet; another thread
 *          could reads the deleted row's RowLocation and try to access it.
 *          <p>
 *          This index supports multiple rows with the same key values.
 *          <p>
 *          <p>
 *          This index is a local index. It means:
 *          <ol>
 *          <li> It stores in the memory instead of the underlying regions.
 *          </li>
 *          <li> It cannot be replicated into other nodes in the image way. A
 *          replicated node will first get the base table from the primary node,
 *          then build this kind of index from scratch.
 * 
 * </li>
 * </ol>
 * 
 * <br>
 * <p>
 * <b>Implementation</b>
 * <ol>
 * <li>This class uses a CSLM object to store the index row. As an entry in the
 * CSLM is a key-value association instead of a complete index row, a protocol
 * is designed to resolve this mismatch. Given an index row
 * (DataValueDescriptor[]), the last element holds the RowLocation object, which
 * points to the row in the base table. </li>
 * 
 * <li> The key object is only a wrapper of a DataValueDesciptor[] containing
 * the values of the columns to be indexed. <b>This class must implements
 * Comparable interface</b>. Although the CSLM provides another way to compare
 * two keys using Comparator, it is less efficient than the former because it
 * needs to generate comparable objects on the fly. </li>
 * 
 * <li> The value object depends on the relationship between key and the number
 * of referenced rows. If a key object has only one corresponding row, the value
 * object is a RowLocation Object. However, if there are more than one rows
 * corresponding a key value, a RowLocation[] value is used to store all these
 * rows to reduce the footprint. </li>
 * 
 * </ol>
 * <br>
 * 
 * @todo
 * <ol>
 * <li> What static or dynamic information is provided to the optimizer for
 * optimization decision. Optimal, worst, or other formula? </li>
 * <li> The sort order of key? e.g. for each column, has different order. </li>
 * <li> The priority of the columns to be compared. </li>
 * <li> What if a column value is null. </li>
 * <li> The transaction isolation level support in the future? </li>
 * <li> Is the performance of the SkipList better than other data structures,
 * such as TreeMap? </li>
 * </ol>
 * @author yjing
 * 
 */

public final class SortedMap2Index extends MemIndex {

  private boolean caseSensitive;

  /**
   * Zero arg constructor for Monitor to create empty object.
   */
  public SortedMap2Index() {
    this.caseSensitive = true;
  }

  @Override
  public boolean fetchMaxOnBTree(TransactionManager tran, Transaction xact,
      long conglomId, int open_mode, int lock_level,
      LockingPolicy locking_policy, int isolation_level,
      FormatableBitSet scanColumnList, DataValueDescriptor[] fetchRow)
      throws StandardException {
    final ConcurrentSkipListMap<Object, Object> skipList = this
        .getGemFireContainer().getSkipListMap();
    Object key;
    int len = this.ascDescInfo.length - 1;
    if (this.ascDescInfo[0]) {
      key = skipList.lastValidKey();
    }
    else {
      key = skipList.firstValidKey();
    }
    if (key == null) {
      return false;
    }
    if (key instanceof CompactCompositeIndexKey) {
      CompactCompositeIndexKey indexKey = (CompactCompositeIndexKey)key;
      if (scanColumnList == null) {
        for (int i = 0; i < len; i++) {
          fetchRow[i] = indexKey.getKeyColumn(i);
        }
      }
      else {
        int beyondBit = scanColumnList.anySetBit();
        int i = 0;
        while (beyondBit != -1) {
          fetchRow[i] = indexKey.getKeyColumn(beyondBit);
          i++;
          beyondBit = scanColumnList.anySetBit(beyondBit);
        }
      }
    }
    else if (len == 1) {
      fetchRow[0] = (DataValueDescriptor)key;
    }
    else {
      DataValueDescriptor[] indexKey = (DataValueDescriptor[])key;
      if (scanColumnList == null) {
        for (int i = 0; i < len; i++) {
          fetchRow[i] = indexKey[i];
        }
      }
      else {
        int beyondBit = scanColumnList.anySetBit();
        int i = 0;
        while (beyondBit != -1) {
          fetchRow[i] = indexKey[beyondBit];
          i++;
          beyondBit = scanColumnList.anySetBit(beyondBit);
        }
      }
    }
    return true;
  }

  @Override
  protected void allocateMemory(Properties properties, int tmpFlag)
      throws StandardException {
    // Create an extra index info and comparator.
    ExtraIndexInfo extraInfo = new ExtraIndexInfo();
    properties.put(MemIndex.PROPERTY_EXTRA_INDEX_INFO, extraInfo);
    String caseSensitiveStr = properties
        .getProperty(GfxdConstants.INDEX_CASE_SENSITIVE_PROP);
    this.caseSensitive = caseSensitiveStr == null
        || !"false".equalsIgnoreCase(caseSensitiveStr);
    Comparator<Object> comparator = this.baseContainer.isOffHeap()
        ? new OffHeapIndexKeyComparator(this.keyColumns, this.ascDescInfo,
            this.caseSensitive)
        : new IndexKeyComparator(this.keyColumns, this.ascDescInfo,
            this.caseSensitive);
    properties.put(MemIndex.PROPERTY_INDEX_COMPARATOR, comparator);
    properties.put(MemIndex.PROPERTY_BASECONGLOMID, this.baseContainer.getId());
  }

  @Override
  protected MemIndexController newMemIndexController() {
    SortedMap2IndexController ret=
       new SortedMap2IndexController();
    return ret;
  }

  @Override
  protected MemIndexCostController newMemIndexCostController() {
    SortedMap2IndexCostController ret=
        new SortedMap2IndexCostController(); 
    return ret;
  }

  @Override
  protected MemIndexScanController newMemIndexScanController() {
    SortedMap2IndexScanController ret=
      new SortedMap2IndexScanController();
    return ret;
  }

  public int getType() {
    return SORTEDMAP2INDEX;
  }

  @Override
  public String getIndexTypeName() {
    return LOCAL_SORTED_MAP_INDEX;
  }

  @Override
  public boolean caseSensitive() {
    return this.caseSensitive;
  }

  @Override
  public int getHeight() throws StandardException {
    return this.getGemFireContainer().getSkipListMap().height();
  }

  @Override
  public void dumpIndex(String marker) {
    if (marker == null) {
      marker = "Skiplist iteration";
    }
    SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_INDEX,
        "##### For SortedMap2Index " + this.container + ' ' + marker
            + " ----- begin #####");

    final ConcurrentSkipListMap<Object, Object> skipList = this.container
        .getSkipListMap();
    final Set<Map.Entry<Object, Object>> s = skipList.entrySet();
    final String lineSep = SanityManager.lineSeparator;
    StringBuilder sb = new StringBuilder().append(lineSep);
    for (Map.Entry<Object, Object> entry : s) {
      sb.append("\tkey=");
      ArrayUtils.objectString(entry.getKey(), sb);
      sb.append(", value=");
      ArrayUtils.objectStringNonRecursive(entry.getValue(), sb);
      sb.append(lineSep);
      if (sb.length() > (4 * 1024 * 1024)) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_INDEX, sb.toString());
        sb.setLength(0);
      }
    }
    SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_INDEX, sb.toString());

    SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_INDEX,
        "##### For SortedMap2Index " + this.container + ' ' + marker
            + " ----- end #####");
  }
}
