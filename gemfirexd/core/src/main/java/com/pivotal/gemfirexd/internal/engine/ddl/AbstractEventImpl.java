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

package com.pivotal.gemfirexd.internal.engine.ddl;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import com.gemstone.gemfire.internal.offheap.annotations.Released;
import com.gemstone.gemfire.internal.offheap.annotations.Retained;
import com.gemstone.gemfire.internal.offheap.annotations.Unretained;
import com.pivotal.gemfirexd.callbacks.AsyncEventHelper;
import com.pivotal.gemfirexd.callbacks.Event;
import com.pivotal.gemfirexd.callbacks.TableMetaData;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.ddl.JavaObjectsList.BytesJavaObjectsList;
import com.pivotal.gemfirexd.internal.engine.ddl.JavaObjectsList.DVDArrayJavaObjectsList;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.engine.sql.catalog.ExtraTableInfo;
import com.pivotal.gemfirexd.internal.engine.store.CompactCompositeRegionKey;
import com.pivotal.gemfirexd.internal.engine.store.DVDStoreResultSet;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer.SerializableDelta;
import com.pivotal.gemfirexd.internal.engine.store.offheap.OffHeapByteSource;
import com.pivotal.gemfirexd.internal.engine.store.offheap.OffHeapRow;
import com.pivotal.gemfirexd.internal.engine.store.offheap.OffHeapRowWithLobs;
import com.pivotal.gemfirexd.internal.engine.store.RawStoreResultSet;
import com.pivotal.gemfirexd.internal.engine.store.RawStoreResultSetWithByteSource;
import com.pivotal.gemfirexd.internal.engine.store.RegionKey;
import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.engine.store.SingleColumnLongResultSet;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;

/**
 * 
 * @author Neeraj
 * 
 */
public abstract class AbstractEventImpl implements Event {

  private ResultSet newRow;

  private ResultSet oldRow;

  final private Type type;

  final private boolean isOriginRemote;

  private int[] updateColsIndex;

  private transient GemFireContainer container;

  private transient RowFormatter formatter;

  public AbstractEventImpl(Type type, boolean isOriginRemote) {
    this.type = type;
    this.isOriginRemote = isOriginRemote;
  }

  @Deprecated
  @Override
  public List<Object> getNewRow() {
    @Unretained Object newVal = this.getNewValue();
    return this.prepareValue(newVal);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultSet getNewRowsAsResultSet() {
    if (this.newRow == null) {
      // first check for UPDATE ops to get the delta
      if (this.type == Type.BEFORE_UPDATE || this.type == Type.AFTER_UPDATE) {
        final SerializableDelta delta = getSerializableDelta();
        final GemFireContainer container = getContainer();
        assert container.isByteArrayStore();
        // We ensure that all WAN queues are drained completely before changing
        // schema in ALTER TABLE (see GemFireContainer.incrementSchemaVersion)
        // so using getCurrentRowFormatter below is safe.
        return (this.newRow = new DVDStoreResultSet(delta.getChangedRow(), -1,
            container.getCurrentRowFormatter(), delta.getChangedColumns(),
            null));
      }
      Object newVal = getNewValue();
      if (newVal != null) {
        assert getContainer().isByteArrayStore();
        assert !(newVal instanceof OffHeapByteSource);
        if (newVal.getClass() == byte[].class) {
          // table shape may have changed by the time event is replayed in
          // AsyncEventListener so use RowFormatter using the row bytes
          final byte[] row = (byte[])newVal;
          return (this.newRow = new RawStoreResultSet(row,
              getRowFormatter(row)));
        }
        else {
          // table shape may have changed by the time event is replayed in
          // AsyncEventListener so use RowFormatter using the row bytes
          final byte[][] row = (byte[][])newVal;
          return (this.newRow = new RawStoreResultSet( row,
              getRowFormatter(row)));
        }
      }
      else {
        return null;
      }
    }
    return this.newRow;
  }

  @Deprecated
  @Override
  public List<Object> getOldRow() {
    @Unretained Object oldVal = this.getOldValue();
    return this.prepareValue(oldVal);
  }
  
  private List<Object> prepareValue(@Unretained Object value) {
    if (value == null) {
      return null;
    }
    final GemFireContainer container = getContainer();
    assert container.isByteArrayStore();
    final int size = container.getNumColumns();
    final Class<?> valClass = value.getClass();
    if (valClass == byte[].class) {
      return new BytesJavaObjectsList(size, (byte[])value, container);
    }
    else if (valClass == byte[][].class) {
      return new BytesJavaObjectsList(size, (byte[][])value, container);
    }
    else if (valClass == OffHeapRow.class) {
      return new BytesJavaObjectsList(size,
          ((OffHeapRow)value).getRowBytes(), container);
    }
    else if (valClass == OffHeapRowWithLobs.class) {
      return new BytesJavaObjectsList(size,
          ((OffHeapRowWithLobs)value).getRowByteArrays(), container);
    }
    else {
      return new DVDArrayJavaObjectsList((DataValueDescriptor[])value);
    }
  }

  @Override
  public ResultSet getOldRowAsResultSet() {
    if (this.oldRow == null) {
      @Unretained Object oldVal = this.getOldValue();
      if (oldVal != null) {
        assert getContainer().isByteArrayStore();

        final Class<?> oldValClass = oldVal.getClass();
        if (oldValClass == byte[].class) {
          // table shape may have changed by the time event is replayed in
          // AsyncEventListener so use RowFormatter using the row bytes
          final byte[] row = (byte[])oldVal;
          return (this.oldRow = new RawStoreResultSet(row,
              getRowFormatter(row)));
        }
        else if (oldValClass == byte[][].class) {
          // table shape may have changed by the time event is replayed in
          // AsyncEventListener so use RowFormatter using the row bytes
          final byte[][] row = (byte[][])oldVal;
          return (this.oldRow = new RawStoreResultSet(row,
              getRowFormatter(row)));
        }
        else if (oldValClass == OffHeapRow.class) {
          // copy to heap
          final byte[] row = ((OffHeapRow)oldVal).getRowBytes();
          return (this.oldRow = new RawStoreResultSet(row,
              getRowFormatter(row)));
        }
        else {
          // copy to heap
          final byte[][] row = ((OffHeapRowWithLobs)oldVal).getRowByteArrays();
          return (this.oldRow = new RawStoreResultSet(row,
              getRowFormatter(row)));
        }
      }
      else {
        return null;
      }
    }
    return this.oldRow;
  }

  @Override
  public TableMetaData getResultSetMetaData() {
    return getContainer().getCurrentRowFormatter().getMetaData();
  }

  @Override
  public int[] getModifiedColumns() {
    if (this.updateColsIndex == null
        && (this.type == Type.AFTER_UPDATE
        || this.type == Type.BEFORE_UPDATE)) {
      final SerializableDelta delta = getSerializableDelta();
      final FormatableBitSet fbs = delta.getChangedColumns();
      final int numChangedColumns = fbs.getNumBitsSet();
      this.updateColsIndex = new int[numChangedColumns];
      int index = 0;
      for (int colIndex = fbs.anySetBit(); colIndex >= 0; colIndex = fbs
          .anySetBit(colIndex), ++index) {
        this.updateColsIndex[index] = colIndex + 1;
      }
    }
    return this.updateColsIndex;
  }

  @Override
  public Type getType() {
    return this.type;
  }

  @Override
  public boolean isOriginRemote() {
    return this.isOriginRemote;
  }

  @Override
  public boolean hasParameters() {
    return false;
  }

  @Override
  public String getSchemaName() {
    return getContainer().getSchemaName();
  }

  @Override
  public String getTableName() {
    return getContainer().getQualifiedTableName();
  }

  @Override
  public int getTableSchemaVersion() {
    return getContainer().getCurrentSchemaVersion();
  }

  @Override
  public boolean tableHasAutogeneratedColumns() {
    return getContainer().getExtraTableInfo().hasAutoGeneratedColumns();
  }

  public final GemFireContainer getContainer() {
    final GemFireContainer container = this.container;
    if (container != null) {
      return container;
    }
    else {
      return (this.container = getGemFireContainer());
    }
  }

  protected final RowFormatter getRowFormatter(final byte[] rowBytes) {
    if (this.formatter != null) {
      return this.formatter;
    }
    return (this.formatter = getContainer().getRowFormatter(rowBytes));
  }

  protected final RowFormatter getRowFormatter(final OffHeapByteSource rowBytes) {
    if (this.formatter != null) {
      return this.formatter;
    }
    return (this.formatter = getContainer().getRowFormatter(rowBytes));
  }

  protected final RowFormatter getRowFormatter(final byte[][] rowBytes) {
    if (this.formatter != null) {
      return this.formatter;
    }
    return (this.formatter = getContainer().getRowFormatter(rowBytes[0]));
  }

  public abstract Object getNewValue();

  public abstract Object getOldValue();

  protected abstract GemFireContainer getGemFireContainer();

  public abstract SerializableDelta getSerializableDelta();

  @Override
  public Object[] getPrimaryKey() {
    Object[] retVal = null;
    Object key = extractKey();
    try {
      if (key instanceof RegionKey) {
        final RegionKey rk = (RegionKey)key;
        final int size = rk.nCols();
        retVal = new Object[size];
        rk.getKeyColumns(retVal);
      }
      else {
        retVal = new Object[] { key };
      }
    } catch (Exception e) {
      throw GemFireXDRuntimeException.newRuntimeException(
          "exception encountered while retrieving primary key for event ="
              + this.toString(), e);
    }
    return retVal;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultSet getPrimaryKeysAsResultSet() {
    Object key = extractKey();
    final Class<?> cls = key.getClass();
    if (cls == CompactCompositeRegionKey.class) {
      final CompactCompositeRegionKey ccrk = (CompactCompositeRegionKey) key;
      final ExtraTableInfo tableInfo = ccrk.getTableInfo();
      final RowFormatter pkFormatter = tableInfo.getPrimaryKeyFormatter();
      final byte[] kbytes = ccrk.getKeyBytes();
      if (kbytes != null) {
        return new RawStoreResultSet(kbytes, pkFormatter);
      }
      else {
        @Retained @Released final Object vbytes = ccrk.getValueByteSource();
        if (vbytes != null) {
          final Class<?> vclass = vbytes.getClass();
          if (vclass == byte[].class) {
            final byte[] row = (byte[])vbytes;
            return new RawStoreResultSet(row, getRowFormatter(row),
                tableInfo.getPrimaryKeyColumns(), pkFormatter.getMetaData());
          }
          else if (vclass == byte[][].class) {
            final byte[][] row = (byte[][])vbytes;
            return new RawStoreResultSet(row, getRowFormatter(row),
                tableInfo.getPrimaryKeyColumns(), pkFormatter.getMetaData());
          }
          else {
            final OffHeapByteSource bs = (OffHeapByteSource)vbytes;
            try {
              return new RawStoreResultSetWithByteSource(bs,
                  getRowFormatter(bs),
                  tableInfo.getPrimaryKeyColumns(), pkFormatter.getMetaData());
            } finally {
              bs.release();
            }
          }
        }
        else {
          return new RawStoreResultSet((byte[])null,
              getRowFormatter((byte[])null), tableInfo.getPrimaryKeyColumns(),
              pkFormatter.getMetaData());
        }
      }
    }
    else if (cls == Long.class) {
      return new SingleColumnLongResultSet(((Long)key).longValue());
    }
    else {
      throw new UnsupportedOperationException("unknown key [" + key
          + "] class: " + cls.getName());
    }
  }

  // [sjigyasu] In AbstractEventImpl, we only need to return the strings for
  // AFTER_INSERT, AFTER_UPDATE and AFTER_DELETE because the
  // listeners are going to set only these types. AbstractEventImpl does
  // not hold the bulk dml string anyway.
  @Override
  public String getDMLString() {
    ResultSetMetaData metadata, pkMetaData;
    // Return empty string
    String dmlString = "";
    ResultSet rows, pkResultSet;
    final String tableName = this.getTableName();
    try {
      switch (this.getType()) {
        case AFTER_INSERT:
        case BEFORE_INSERT:
          metadata = getResultSetMetaData();
          dmlString = AsyncEventHelper.getInsertString(tableName,
              (TableMetaData)metadata, false);
          break;
        case AFTER_UPDATE:
        case BEFORE_UPDATE:
          rows = getNewRowsAsResultSet();
          metadata = rows.getMetaData();
          pkResultSet = getPrimaryKeysAsResultSet();
          pkMetaData = pkResultSet.getMetaData();
          dmlString = AsyncEventHelper.getUpdateString(tableName, pkMetaData,
              metadata);
          break;
        case AFTER_DELETE:
        case BEFORE_DELETE:
          pkResultSet = getPrimaryKeysAsResultSet();
          pkMetaData = pkResultSet.getMetaData();
          dmlString = AsyncEventHelper.getDeleteString(tableName, pkMetaData);
          break;
        case BULK_DML:
          // This should never happen 
          dmlString = this.toString();
          break;
        case BULK_INSERT:
       // This should never happen
          dmlString = this.toString();
          break;
        default:
          // should never happen
          dmlString = this.toString();
      }
    } catch (SQLException e) {
      if(Misc.getCacheLogWriter().infoEnabled()) {
        Misc.getCacheLogWriter().info("AbstractEventImpl##getDMLString::" + e.getMessage());
      }
    }
    return dmlString;
  }

  public abstract Object extractKey();

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("old row = ");
    sb.append(this.getOldRowAsResultSet());
    sb.append("; new row = ");
    sb.append(this.getNewRowsAsResultSet());
    sb.append("; Table Name = ");
    sb.append(this.getTableName());    
    return sb.toString();
  }
}
