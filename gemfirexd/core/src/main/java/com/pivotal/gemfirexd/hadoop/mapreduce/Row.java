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
package com.pivotal.gemfirexd.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.hdfs.internal.PersistedEventImpl;
import com.pivotal.gemfirexd.callbacks.Event.Type;
import com.pivotal.gemfirexd.internal.engine.hadoop.mapreduce.OutputFormatUtil;
import com.pivotal.gemfirexd.internal.engine.store.CustomRowsResultSet;
import com.pivotal.gemfirexd.internal.engine.store.CustomRowsResultSet.FetchDVDRows;
import com.pivotal.gemfirexd.internal.engine.store.entry.HDFSEventRowLocationRegionEntry;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultColumnDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedResultSet;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedResultSetMetaData;
import com.pivotal.gemfirexd.internal.impl.sql.GenericColumnDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.execute.TableScanResultSet;

/**
 * A container of table row impacted by a create, update or delete operation. An
 * instance of this class will provide user with the operation's metadata and
 * row after the DML operation. This includes operation type, operation
 * timestamp, key, column metadata and new values of the columns.
 * 
 * @author ashvina
 */
public class Row implements Writable {
  private final EmbedResultSet ers;
  private ResultSet rs;
  SerializableGcdProxy[] rowGcdArray;
  DataValueDescriptor[] rowDvdArray;

  static final Logger logger = LoggerFactory.getLogger(Row.class);

  public Row() {
    this(null);
  }

  public Row(EmbedResultSet resultSet) {
    this.ers = resultSet;
  }

  public final EmbedResultSet getEmbedResultSet() {
    return this.ers;
  }

  /**
   * Get the changed row as a {@link ResultSet#TYPE_FORWARD_ONLY}
   * {@link ResultSet}. The returned {@link ResultSet} is already positioned at
   * the returned single row with {@link ResultSet#next()} always returning
   * false, whose column values can be retrieved using the ResultSet getter
   * methods.
   * 
   * @return the changed row
   */
  public final ResultSet getRowAsResultSet() throws IOException {

    final ResultSet rs = this.rs;
    if (rs != null) {
      return rs;
    }

    final EmbedResultSet ers = this.ers;
    if (ers != null) {
      return (this.rs = OutputFormatUtil.getNonIterableResultSet(ers));
    }

    if (rowGcdArray == null) {
      throw new IllegalStateException("Invalid state: ResultSet is null");
    }

    GenericColumnDescriptor[] gcds = createGcdArray(rowGcdArray);
    // construct row from GCD and DVD
    try {
      CustomRowsResultSet crs = new CustomRowsResultSet(new FetchDVDRowsImpl(
          rowDvdArray), gcds);
      if (!crs.next()) {
        throw new IllegalStateException("No row data found in deserialized row");
      }
      return (this.rs = crs);
      //return OutputFormatUtil.getNonIterableResultSet(rs);
    } catch (Exception e) {
      String msg = "Failed to create result set from deserialized row.";
      logger.error(msg, e);
      throw new RuntimeException(msg, e);
    }
  }

  /**
   * Indicates whether this row change event is a possibly duplicate. Duplicate
   * events may get persisted on HDFS when a fail-over happens during DML
   * statement execution.
   * 
   * @return true if the event is a possible duplicate.
   * @throws IOException 
   */
  public boolean isPossibleDuplicate() throws IOException {
    PersistedEventImpl event = getEvent();
    return event == null ? false : event.isPossibleDuplicate();
  }

  /**
   * Get the type of DML operation that changed this row.
   * 
   * @return the type of this {@link Operation}.
   * @throws IOException
   */
  public Type getEventType() throws IOException {
    final EmbedResultSet ers = this.ers;
    if (ers != null) {
      return getEventType(getEvent(ers));
    }
    else {
      return null;
    }
  }

  public static final Type getEventType(final PersistedEventImpl event)
      throws IOException {
    Operation op = event.getOperation();
    if (op != null) {
      Type type = null;
      if (op.isCreate()) {
        type = Type.AFTER_INSERT;
      }
      else if (op.isUpdate()) {
        type = Type.AFTER_UPDATE;
      }
      else if (op.isDestroy()) {
        type = Type.AFTER_DELETE;
      }

      return type;
    }
    else {
      return null;
    }
  }

  /**
   * @return timestamp when this row operation was executed
   * @throws IOException
   */
  public long getTimestamp() throws IOException {
    PersistedEventImpl event = getEvent();
    return event == null ? 0L : event.getTimstamp();
  }

  /**
   * Returns persisted event instance contained in the result set row
   */
  private PersistedEventImpl getEvent() throws IOException {
    // it is important to perform these steps on every invocation as the result
    // set advances outside this class
    final EmbedResultSet ers = this.ers;
    if (ers != null) {
      return getEvent(ers);
    }

    return null;
  }

  public static final PersistedEventImpl getEvent(final EmbedResultSet ers)
      throws IOException {
    TableScanResultSet sourceSet;
    sourceSet = (TableScanResultSet)ers.getSourceResultSet();

    HDFSEventRowLocationRegionEntry row;
    try {
      row = (HDFSEventRowLocationRegionEntry)sourceSet.getRowLocation();
      return row.getEvent();
    } catch (StandardException e) {
      logger.error("Error reading event info from result set row.", e);
      throw new IOException("Error reading event info from result set row.", e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void write(DataOutput out) throws IOException {
    /**
     * Writes the following information on output stream
     * 
     * <pre>
     * 1. Number of table columns
     * 2. GCD for each column (There is scope for optimization here)
     * 3. DVD class type
     * 4. Each columns DVD
     * </pre>
     * 
     */
    SerializableGcdProxy[] gcdProxyArray = getGcdArray();
    out.writeInt(gcdProxyArray.length);
    for (SerializableGcdProxy gcdProxy : gcdProxyArray) {
      gcdProxy.toData(out);
    }

    DataValueDescriptor[] dvdArray = getDvdArray();
    DataSerializer.writeObjectArray(dvdArray, out);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void readFields(DataInput in) throws IOException {
    /**
     * Reads the following information on input stream
     * 
     * <pre>
     * 1. Number of table columns
     * 2. GCD for each column
     * 3. DVD class type
     * 4. Each columns DVD
     * </pre>
     * 
     */

    // make rs null to avoid conflict with existing rs
    this.rs = null;

    int colCount = in.readInt();
    this.rowGcdArray = new SerializableGcdProxy[colCount];
    for (int i = 0; i < colCount; i++) {
      SerializableGcdProxy proxy = new SerializableGcdProxy();
      proxy.fromData(in);
      rowGcdArray[i] = proxy;
    }

    rowDvdArray = new DataValueDescriptor[colCount];
    try {
      rowDvdArray = (DataValueDescriptor[]) DataSerializer.readObjectArray(in);
    } catch (ClassNotFoundException e) {
      logger.error("Failed to deserialize object while reading row", e);
      throw new IOException("Failed to deserialize object while reading row", e);
    }
  }

  /**
   * Wrapper class to provide FetchDVDRows view to DVDs of a single row
   * 
   * @author ashvina
   */
  private static class FetchDVDRowsImpl implements FetchDVDRows {
    private DataValueDescriptor[] dvds;

    public FetchDVDRowsImpl(DataValueDescriptor[] dvds) {
      this.dvds = dvds;
    }

    @Override
    public boolean getNext(DataValueDescriptor[] template) throws SQLException,
        StandardException {
      if (dvds == null) {
        return false;
      }

      for (int i = 0; i < dvds.length; i++) {
        template[i] = dvds[i];
      }
      dvds = null;

      return true;
    }
  }

  /**
   * Returns array of GCD objects for this row. GCD will be null when Row is
   * created by {@link RowInputFormat}. In that case GCD will be created from
   * resultSet provided by InputFormat. GCD will not be null when Row is created
   * by deserializing Row from input stream
   * 
   * @throws IOException
   */
  private SerializableGcdProxy[] getGcdArray() throws IOException {
    if (rowGcdArray != null) {
      return rowGcdArray;
    }

    final EmbedResultSet ers = this.ers;
    if (ers == null) {
      throw new IllegalStateException("Invalid state: ResultSet is null");
    }

    try {
      ResultSetMetaData meta = ers.getMetaData();
      int count = meta.getColumnCount();
      rowGcdArray = new SerializableGcdProxy[count];
      for (int i = 0; i < count; i++) {
        String name = meta.getColumnName(i + 1);
        int type = meta.getColumnType(i + 1);
        SerializableGcdProxy proxy = new SerializableGcdProxy(name, type);
        rowGcdArray[i] = proxy;
      }
    } catch (SQLException e) {
      logger.error("Meta serialization failed while writing row.", e);
      throw new IOException("Meta serialization failed while writing row.", e);
    }

    return rowGcdArray;
  }

  /**
   * Returns array of DVD objects for this row. DVDs will be null when Row is
   * created by {@link RowInputFormat}. In that case DVD will be created from
   * resultSet provided by InputFormat. DVD will not be null when Row is created
   * by deserializing Row from input stream
   * 
   * @throws IOException
   */
  private DataValueDescriptor[] getDvdArray() {
    if (rowDvdArray != null) {
      return rowDvdArray;
    }

    final EmbedResultSet ers = this.ers;
    if (ers == null) {
      throw new IllegalStateException("Invalid state: ResultSet is null");
    }

    ExecRow row = ers.getCurrentRow();
    rowDvdArray = row.getRowArray();

    return rowDvdArray;
  }

  /**
   * This class enables efficient serialization of column descriptors. Only name
   * and column type are needed for creation of result set. this class will not
   * serialize any other field of column descriptor
   * 
   * @author ashvina
   */
  private static class SerializableGcdProxy {
    private String columnName;
    private int columnType;

    public SerializableGcdProxy() {
    }

    public SerializableGcdProxy(String name, int type) {
      this.columnName = name;
      this.columnType = type;
    }

    public void toData(DataOutput out) throws IOException {
      out.writeInt(columnType);
      DataSerializer.writeString(columnName, out);
    }

    public void fromData(DataInput in) throws IOException {
      this.columnType = in.readInt();
      this.columnName = DataSerializer.readString(in);
    }
  }

  private GenericColumnDescriptor[] createGcdArray(SerializableGcdProxy[] proxy) {
    GenericColumnDescriptor[] gcdArray = new GenericColumnDescriptor[proxy.length];
    for (int i = 0; i < proxy.length; i++) {
      String name = proxy[i].columnName;
      int type = proxy[i].columnType;
      ResultColumnDescriptor cd;
      cd = EmbedResultSetMetaData.getResultColumnDescriptor(name, type, true);
      gcdArray[i] = (GenericColumnDescriptor) cd;
    }
    return gcdArray;
  }
}
