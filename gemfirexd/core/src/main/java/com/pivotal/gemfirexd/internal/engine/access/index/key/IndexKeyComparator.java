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

package com.pivotal.gemfirexd.internal.engine.access.index.key;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.engine.sql.catalog.ExtraInfo;
import com.pivotal.gemfirexd.internal.engine.store.CompactCompositeIndexKey;
import com.pivotal.gemfirexd.internal.engine.store.ExtractingIndexKey;
import com.pivotal.gemfirexd.internal.engine.store.RegionEntryUtils;
import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ColumnDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeUtilities;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.SQLChar;

/**
 * A {@link Comparator} for on-heap index keys that avoids off-heap byte source
 * checks. It allows for partial key matches.
 * 
 * @author yjing, dsmith, swale
 */
public class IndexKeyComparator implements Comparator<Object>, Serializable {

  private static final long serialVersionUID = -2636906134615261052L;

  /** the ascending or descending information for each key column */
  protected final int[] columnOrders;

  /**
   * contains 1,2,3,... as the key positions for use as positions when snapshot
   * index key bytes are being used
   */
  protected final int[] keySnapshotPositions;

  /** number of actual key columns in the index */
  protected final int nCols;

  /**
   * if set to true then index comparisons will be case-sensitive (default) else
   * case-insensitive (--GEMFIREXD-PROPERTIES caseSensitive=false in CREATE INDEX)
   */
  protected final boolean caseSensitive;

  /**
   * @param nCols
   *          number of key columns in the key
   */
  public IndexKeyComparator(int nCols, boolean[] columnOrders,
      boolean caseSensitive) {
    this.nCols = nCols;
    this.columnOrders = new int[columnOrders.length];
    this.keySnapshotPositions = new int[nCols];
    for (int i = 0; i < columnOrders.length; i++) {
      this.columnOrders[i] = columnOrders[i] ? 1 : -1;
    }
    for (int i = 0; i < nCols; i++) {
      this.keySnapshotPositions[i] = (i + 1);
    }
    this.caseSensitive = caseSensitive;
  }

  /**
   * Compare two arrays of DataValueDescriptor, or
   * {@link CompactCompositeIndexKey}s or combination from low index to high
   * index. If one array is shorter than the other, than do a partial key match.
   * 
   * @return <0 if first key is smaller, 0 if the two keys are equal, >0 if the
   *         first key is larger
   * 
   * @see DataValueDescriptor#compare
   */
  @Override
  public int compare(final Object key, final Object mapKey) {

    if (key == mapKey) {
      return 0;
    }
    // second one is usually the map key (probably always)
    final CompactCompositeIndexKey ccik1, ccik2;
    DataValueDescriptor dvd1, dvd2;
    final DataValueDescriptor[] dvds1, dvds2;
    // if rows are not of the same length, then find the shorter length
    int cmpLen = this.nCols;
    int rowLength;

    final Class<?> keyClass = key.getClass();
    final Class<?> mapKeyClass = mapKey.getClass();

    // all type checks inlined here to avoid multiple instanceof invocations
    if (mapKeyClass == CompactCompositeIndexKey.class) {
      ccik2 = (CompactCompositeIndexKey)mapKey;
      dvd2 = null;
      dvds2 = null;
      if (keyClass == CompactCompositeIndexKey.class
          || keyClass == ExtractingIndexKey.class) {
        ccik1 = (CompactCompositeIndexKey)key;
        dvd1 = null;
        dvds1 = null;
      }
      else if (keyClass == DataValueDescriptor[].class) {
        ccik1 = null;
        dvd1 = null;
        dvds1 = (DataValueDescriptor[])key;
        rowLength = dvds1.length;
        if (rowLength < cmpLen) {
          cmpLen = rowLength;
        }
      }
      else {
        ccik1 = null;
        dvd1 = (DataValueDescriptor)key;
        dvds1 = null;
        cmpLen = 1;
      }
    }
    else if (keyClass == CompactCompositeIndexKey.class
        || keyClass == ExtractingIndexKey.class) {
      ccik1 = (CompactCompositeIndexKey)key;
      dvd1 = null;
      dvds1 = null;
      if (mapKeyClass == DataValueDescriptor[].class) {
        ccik2 = null;
        dvd2 = null;
        dvds2 = (DataValueDescriptor[])mapKey;
        rowLength = dvds2.length;
        if (rowLength < cmpLen || cmpLen <= 0) {
          cmpLen = rowLength;
        }
      }
      else {
        ccik2 = null;
        dvd2 = (DataValueDescriptor)mapKey;
        dvds2 = null;
        cmpLen = 1;
      }
    }
    else {
      ccik1 = null;
      ccik2 = null;
      if (keyClass == DataValueDescriptor[].class) {
        dvd1 = null;
        dvds1 = (DataValueDescriptor[])key;
        rowLength = dvds1.length;
        if (rowLength < cmpLen || cmpLen <= 0) {
          cmpLen = rowLength;
        }
      }
      else {
        dvd1 = (DataValueDescriptor)key;
        dvds1 = null;
        cmpLen = 1;
      }
      if (mapKeyClass == DataValueDescriptor[].class) {
        dvd2 = null;
        dvds2 = (DataValueDescriptor[])mapKey;
        rowLength = dvds2.length;
        if (rowLength < cmpLen) {
          cmpLen = rowLength;
        }
      }
      else {
        dvd2 = (DataValueDescriptor)mapKey;
        dvds2 = null;
        cmpLen = 1;
      }
    }
    // get the bytes+formatter from CCIK keys in one round so as to avoid
    // doing it for every column
    byte[] ccik1Bytes = null, ccik2Bytes = null;
    RowFormatter ccik1RF = null, ccik2RF = null;
    int[] key1Positions = null, key2Positions = null;
    byte[] kbytes;

    if (ccik1 != null) {
      final ExtraInfo indexInfo = ccik1.tableInfo;
      if (indexInfo == null) {
        throw RegionEntryUtils
            .checkCacheForNullTableInfo("IndexKeyComparator#compare");
      }
      int tries = 1;
      for (;;) {
        kbytes = ccik1.getKeyBytes();
        if (kbytes != null) {
          ccik1Bytes = kbytes;
          ccik1RF = indexInfo.getPrimaryKeyFormatter();
          key1Positions = this.keySnapshotPositions;
          break;
        }
        kbytes = (byte[])ccik1.getRawValueByteSource();
        if (kbytes != null) {
          ccik1Bytes = kbytes;
          ccik1RF = indexInfo.getRowFormatter(kbytes);
          key1Positions = indexInfo.getPrimaryKeyColumns();
          break;
        }
        if ((tries++ % CompactCompositeIndexKey.MAX_TRIES_YIELD) == 0) {
          // enough tries; give other threads a chance to proceed
          Thread.yield();
        }
        if (tries > CompactCompositeIndexKey.MAX_TRIES) {
          throw RegionEntryUtils
              .checkCacheForNullKeyValue("IndexKeyComparator#compare");
        }
      }
    }
    if (ccik2 != null) {
      final ExtraInfo indexInfo = ccik2.tableInfo;
      if (indexInfo == null) {
        throw RegionEntryUtils
            .checkCacheForNullTableInfo("IndexKeyComparator#compare");
      }
      int tries = 1;
      for (;;) {
        kbytes = ccik2.getKeyBytes();
        if (kbytes != null) {
          ccik2Bytes = kbytes;
          ccik2RF = indexInfo.getPrimaryKeyFormatter();
          key2Positions = this.keySnapshotPositions;
          break;
        }
        kbytes = (byte[])ccik2.getRawValueByteSource();
        if (kbytes != null) {
          ccik2Bytes = kbytes;
          ccik2RF = indexInfo.getRowFormatter(kbytes);
          key2Positions = indexInfo.getPrimaryKeyColumns();
          break;
        }
        if ((tries++ % CompactCompositeIndexKey.MAX_TRIES_YIELD) == 0) {
          // enough tries; give other threads a chance to proceed
          Thread.yield();
        }
        if (tries > CompactCompositeIndexKey.MAX_TRIES) {
          throw RegionEntryUtils
              .checkCacheForNullKeyValue("IndexKeyComparator#compare");
        }
      }
    }

    for (int i = 0; i < cmpLen; i++) {
      try {
        int cmp;
        if (ccik1 != null) {
          final int rfPos1 = key1Positions[i];
          final ColumnDescriptor cd = ccik1RF.getColumnDescriptor(rfPos1 - 1);
          final long offsetWidth1 = ccik1RF.getOffsetAndWidth(rfPos1,
              ccik1Bytes, cd);
          if (ccik2 != null) {
            cmp = DataTypeUtilities.compare(ccik1Bytes, ccik2Bytes,
                offsetWidth1, ccik2RF.getOffsetAndWidth(key2Positions[i],
                    ccik2Bytes, cd), false, this.caseSensitive, cd);
          }
          else {
            // key1 is definitely DVD or DVD[]
            if (dvds2 != null) {
              dvd2 = dvds2[i];
            }
            // reverse the result of compare here since lhs,rhs are swapped
            // below due to dvd2 being passed as lhsDVD
            cmp = DataTypeUtilities.compare(dvd2, ccik1Bytes, offsetWidth1,
                false, this.caseSensitive, cd);
            if (cmp < 0) {
              return this.columnOrders[i];
            }
            else if (cmp > 0) {
              return -this.columnOrders[i];
            }
            else {
              continue;
            }
          }
        }
        else {
          // key1 is definitely DVD or DVD[]
          if (dvds1 != null) {
            dvd1 = dvds1[i];
          }
          if (ccik2 != null) {
            final int rfPos2 = key2Positions[i];
            final ColumnDescriptor cd = ccik2RF.getColumnDescriptor(rfPos2 - 1);
            final long offsetWidth2 = ccik2RF.getOffsetAndWidth(rfPos2,
                ccik2Bytes, cd);
            // ccik1 is null below
            cmp = DataTypeUtilities.compare(dvd1, ccik2Bytes, offsetWidth2,
                false, this.caseSensitive, cd);
          }
          else {
            if (dvds2 != null) {
              dvd2 = dvds2[i];
            }
            if (this.caseSensitive || !(dvd1 instanceof SQLChar)) {
              cmp = dvd1.compare(dvd2);
            }
            else {
              cmp = ((SQLChar)dvd1).compareIgnoreCase(dvd2);
            }
          }
        }
        if (cmp < 0) {
          return -this.columnOrders[i];
        }
        else if (cmp > 0) {
          return this.columnOrders[i];
        }
      } catch (Exception e) {
        GemFireXDRuntimeException re;
        if (key instanceof CompactCompositeIndexKey) {
          CompactCompositeIndexKey ckey = (CompactCompositeIndexKey)key;
          re = new GemFireXDRuntimeException(
              "IndexKeyComparator#compare: failed for key=" + ckey
                  + ", value=" + ckey.getTransientValue());
        } else {
          String keyString;
          if (key instanceof DataValueDescriptor[]) {
            keyString = Arrays.toString((DataValueDescriptor[])key);
          } else {
            keyString = String.valueOf(key);
          }
          re = new GemFireXDRuntimeException(
              "IndexKeyComparator#compare: failed for key=" + keyString);
        }
        re.initCause(e);
        throw re;
      }
    }

    // we have a partial key match

    //Trick to replace the bytes of a key when
    //removing the value from the index.
    if (ccik1 != null) {
      if (keyClass == ExtractingIndexKey.class) {
        if (ccik2 != null) {
          ((ExtractingIndexKey)key).afterCompareWith(ccik2);
        }
      }
      /* now handling this via MapCallback.afterUpdate
      // if we have snapshotted the key previously and this is an insert
      // then replace with the inserted value bytes
      else if (ccik2 != null) {
        // we always assume that ccik1 is the incoming key while ccik2 is the
        // map key (this is always the case for our CSLM implementation and all
        // JDK ones too)
        if (ccik1.forInsert() && vbytesToRelease2 == null) {
          final Object vbytes = vbytesToRelease1;
          if (vbytes != null) {
            ccik2.update(vbytes, null);
          }
        }
      }
      */
    }

    return 0;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(getClass().getSimpleName()).append(" nCols: ").append(this.nCols)
        .append(" columnOrders: ").append(Arrays.toString(columnOrders));
    return sb.toString();
  }
}
