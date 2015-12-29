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
package com.pivotal.pxf.plugins.gemfirexd;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.cache.hdfs.internal.PersistedEventImpl;
import com.gemstone.gemfire.internal.offheap.annotations.Unretained;
import com.pivotal.gemfirexd.callbacks.Event.Type;
import com.pivotal.gemfirexd.hadoop.mapred.Row;
import com.pivotal.gemfirexd.internal.engine.distributed.ByteArrayDataOutput;
import com.pivotal.gemfirexd.internal.engine.store.AbstractCompactExecRow;
import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.engine.store.offheap.OffHeapRow;
import com.pivotal.gemfirexd.internal.engine.store.offheap.OffHeapRowWithLobs;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedResultSet;
import com.pivotal.pxf.api.OneField;
import com.pivotal.pxf.api.OneRow;
import com.pivotal.pxf.api.ReadResolver;
import com.pivotal.pxf.api.UnsupportedTypeException;
import com.pivotal.pxf.api.io.DataType;
import com.pivotal.pxf.api.utilities.ColumnDescriptor;
import com.pivotal.pxf.api.utilities.InputData;
import com.pivotal.pxf.api.utilities.Plugin;
import com.pivotal.pxf.plugins.gemfirexd.util.GemFireXDManager;

public class GemFireXDResolver extends Plugin implements ReadResolver {

  private String regionName = "";

  private final boolean isBinaryFormat;

  private boolean processMetaData = true;

  /**
   * Array index is external table's column index and the value at the index is
   * gfxd table's column index.
   * 
   * Simply put, maps given index of external-table's column to that of
   * gemfirexd-table's column.
   */
  private int[] columnIndexMapping = null;

  /**
   * Maps column index to column type for external table.
   */
  private final DataType[] columnTypeMapping;

  private ByteArrayDataOutput buffer = new ByteArrayDataOutput();

  private int timestampColumnIndex = -1;

  private int eventTypeColumnIndex = -1;

  //private Logger logger = LoggerFactory.getLogger(GemFireXDResolver.class);

  public GemFireXDResolver(InputData metaData) {
    super(metaData);
    this.isBinaryFormat = !"TEXT".equalsIgnoreCase(metaData
        .getUserProperty("FORMAT"));
    String table = GemFireXDAccessor.tableName.get();
    if (table != null) {
      int idx = table.indexOf(".");
      this.regionName = table.substring(idx + 1);
    }

    int cols = this.inputData.getColumns();
    this.columnTypeMapping = new DataType[cols];
    for (int i = 0; i < cols; i++) {
      this.columnTypeMapping[i] = DataType.get(this.inputData.getColumn(i)
          .columnTypeCode());
    }
  }

  public final String getRegionName() {
    return this.regionName;
  }

  /**
   * Returns only the values of the columns defined in the external table and in
   * the sequence they are defined there. Column sequence in Gemfirexd table is
   * ignored.
   */
  @Override
  public List<OneField> getFields(OneRow row) throws Exception {
    try {
      return this.isBinaryFormat ? parseData(row) : parseDataText(row);
    } catch (Exception e) {
      // This means accessors' closeForRead() won't be called.
      GemFireXDAccessor.resetLonerRefCount(GemFireXDAccessor.tableName.get());
      throw e;
    }
  }

  private final void writeColumnsAsUTF8PXFBytes(
      final ByteArrayDataOutput buffer,
      final AbstractCompactExecRow compactRow, final int[] positions,
      final int obj1Index, final String obj1, final int obj2Index,
      final String obj2) throws StandardException {
    final RowFormatter formatter = compactRow.getRowFormatter();
    @Unretained final Object source = compactRow.getBaseByteSource();
    if (source != null) {
      final int npos = positions.length;

      byte[] bytes = null;
      byte[][] byteArrays = null;
      OffHeapRow offHeapBytes = null;
      OffHeapRowWithLobs offHeapByteArrays = null;
      int firstBytesLen = 0;
      long firstBSAddr = 0;

      final Class<?> sclass = source.getClass();
      if (sclass == byte[].class) {
        bytes = (byte[])source;
      }
      else if (sclass == byte[][].class) {
        byteArrays = (byte[][])source;
      }
      else if (sclass == OffHeapRow.class) {
        offHeapBytes = (OffHeapRow)source;
        firstBytesLen = offHeapBytes.getLength();
        firstBSAddr = offHeapBytes.getUnsafeAddress(0, firstBytesLen);
      }
      else {
        offHeapByteArrays = (OffHeapRowWithLobs)source;
        firstBytesLen = offHeapByteArrays.getLength();
        firstBSAddr = offHeapByteArrays.getUnsafeAddress(0, firstBytesLen);
      }
      for (int i = 0; i < npos; i++) {
        if (i != 0) {
          buffer.write(RowFormatter.DELIMITER_FOR_PXF);
        }
        if (i != obj1Index) {
          if (i != obj2Index) {
            final int position = positions[i];
            if (bytes != null) {
              formatter.writeAsUTF8BytesForPXF(position, bytes, buffer);
            }
            else if (byteArrays != null) {
              formatter.writeAsUTF8BytesForPXF(position, byteArrays, buffer);
            }
            else if (offHeapBytes != null) {
              final int index = position - 1;
              formatter.writeAsUTF8BytesForPXF(index,
                  formatter.getColumnDescriptor(index),
                  com.gemstone.gemfire.internal.offheap.UnsafeMemoryChunk
                      .getUnsafeWrapper(), firstBSAddr,
                  firstBytesLen, offHeapBytes, buffer);
            }
            else {
              final int index = position - 1;
              formatter.writeAsUTF8BytesForPXF(index,
                  formatter.getColumnDescriptor(index),
                  com.gemstone.gemfire.internal.offheap.UnsafeMemoryChunk
                      .getUnsafeWrapper(), firstBSAddr,
                  firstBytesLen, offHeapByteArrays, buffer);
            }
          }
          else {
            buffer.writeBytes(obj2);
          }
        }
        else {
          buffer.writeBytes(obj1);
        }
      }
    }
  }

  /**
   * 1. Get handle to resultSet
   * 2. Parse each field value in OneField instances
   */
  private List<OneField> parseData(OneRow row) throws Exception {
    if (this.processMetaData) {
      mapColumnIndexes(row);
    }
    //final long start = System.nanoTime();
    final Row gfxdRow = (Row)row.getData();
    final EmbedResultSet ers = gfxdRow.getEmbedResultSet();
    final ResultSet rs = ers != null ? ers : gfxdRow.getRowAsResultSet();
    final int extColumns = this.inputData.getColumns();
    ArrayList<OneField> result = new ArrayList<OneField>(extColumns);
    final int timestampColumnIndex = this.timestampColumnIndex;
    final int eventTypeColumnIndex = this.eventTypeColumnIndex;

    for (int i = 0; i < extColumns; i++) {
      if (i == timestampColumnIndex) {
        result.add(new OneField(DataType.TIMESTAMP.getOID(), new Timestamp(
            gfxdRow.getTimestamp())));
      }
      else if (i == eventTypeColumnIndex) {
        Type op = gfxdRow.getEventType();
        result.add(new OneField(DataType.VARCHAR.getOID(), op == null ? "" : op
            .toString()));
      }
      else {
        result.add(parseField(rs, i));
      }
    }
    return result;
  }

  /**
   * 1. Get handle to resultSet
   * 2. Parse each field value in OneField instances
   */
  private List<OneField> parseDataText(OneRow row) throws Exception {
    if (this.processMetaData) {
      mapColumnIndexes(row);
    }
    //final long start = System.nanoTime();
    final Row gfxdRow = (Row)row.getData();
    final EmbedResultSet ers = gfxdRow.getEmbedResultSet();
    final int extColumns = this.inputData.getColumns();
    ArrayList<OneField> result = new ArrayList<OneField>(extColumns);
    final int timestampColumnIndex = this.timestampColumnIndex;
    final int eventTypeColumnIndex = this.eventTypeColumnIndex;
    final AbstractCompactExecRow compactRow;

    if (ers != null && ers.getCurrentRow() instanceof AbstractCompactExecRow) {
      compactRow = (AbstractCompactExecRow)ers.getCurrentRow();
      PersistedEventImpl event = null;

      String timestampColumn = null, eventTypeColumn = null;
      if (timestampColumnIndex >= 0 && timestampColumnIndex < extColumns) {
        event = Row.getEvent(ers);
        timestampColumn = new Timestamp(event != null ? event.getTimstamp()
            : gfxdRow.getTimestamp()).toString();
      }
      if (eventTypeColumnIndex >= 0 && eventTypeColumnIndex < extColumns) {
        if (event == null) {
          event = Row.getEvent(ers);
        }
        final Type op = event != null ? Row.getEventType(event) : gfxdRow
            .getEventType();
        eventTypeColumn = op != null ? op.toString() : "";
      }

      final ByteArrayDataOutput buffer = this.buffer;
      writeColumnsAsUTF8PXFBytes(buffer, compactRow, this.columnIndexMapping,
          timestampColumnIndex, timestampColumn, eventTypeColumnIndex,
          eventTypeColumn);
      // add terminating newline
      buffer.write('\n');
      result.add(new OneField(DataType.BYTEA.getOID(), buffer.toByteArray()));
      // reset buffer for reuse
      buffer.clearForReuse();
      return result;
    }

    final ResultSet rs = ers != null ? ers : gfxdRow.getRowAsResultSet();
    for (int i = 0; i < extColumns; i++) {
      if (i == timestampColumnIndex) {
        result.add(new OneField(DataType.TIMESTAMP.getOID(), new Timestamp(
            gfxdRow.getTimestamp())));
      }
      else if (i == eventTypeColumnIndex) {
        Type op = gfxdRow.getEventType();
        result.add(new OneField(DataType.VARCHAR.getOID(), op == null ? "" : op
            .toString()));
      }
      else {
        result.add(parseField(rs, i));
      }
    }
    return result;
  }

  private OneField parseField(ResultSet rs, int i) throws Exception {
    // Indexing in rs starts at 1.
    final DataType type = this.columnTypeMapping[i];
    switch (type) {
      case SMALLINT:
        return new OneField(DataType.SMALLINT.getOID(),
            rs.getShort(this.columnIndexMapping[i]));

      case INTEGER:
        return new OneField(DataType.INTEGER.getOID(),
            rs.getInt(this.columnIndexMapping[i]));

      case BIGINT:
        return new OneField(DataType.BIGINT.getOID(),
            rs.getLong(this.columnIndexMapping[i]));

      case REAL:
        return new OneField(DataType.REAL.getOID(),
            rs.getFloat(this.columnIndexMapping[i]));

      case FLOAT8:
        return new OneField(DataType.FLOAT8.getOID(),
            rs.getDouble(this.columnIndexMapping[i]));

      case VARCHAR:
        return new OneField(DataType.VARCHAR.getOID(),
            rs.getString(this.columnIndexMapping[i]));

      case BOOLEAN:
        return new OneField(DataType.BOOLEAN.getOID(),
            rs.getBoolean(this.columnIndexMapping[i]));

      case NUMERIC:
        return new OneField(DataType.NUMERIC.getOID(),
            rs.getBigDecimal(this.columnIndexMapping[i]));

      case TIMESTAMP:
        return new OneField(DataType.TIMESTAMP.getOID(),
            rs.getTimestamp(this.columnIndexMapping[i]));

      case DATE:
        return new OneField(DataType.DATE.getOID(),
            rs.getDate(this.columnIndexMapping[i]));

      case TIME:
        return new OneField(DataType.TIME.getOID(),
            rs.getTime(this.columnIndexMapping[i]));

      case CHAR:
        return new OneField(DataType.CHAR.getOID(),
            rs.getString(this.columnIndexMapping[i]));

      case BPCHAR:
        return new OneField(DataType.BPCHAR.getOID(),
            rs.getString(this.columnIndexMapping[i]));

      case BYTEA:
        return new OneField(DataType.BYTEA.getOID(),
            rs.getBytes(this.columnIndexMapping[i]));

      case TEXT:
        return new OneField(DataType.TEXT.getOID(),
            rs.getString(this.columnIndexMapping[i]));

      default:
        throw new Exception("Column type " + type + " is not supported.");
    }
  }

  private void mapColumnIndexes(OneRow row) throws Exception {
    this.processMetaData = false;

    ResultSet rs = ((Row)row.getData()).getRowAsResultSet();
    ResultSetMetaData gfxdMetadata = rs.getMetaData();
    int gfxdCols = gfxdMetadata.getColumnCount();
    int extCols = this.inputData.getColumns();
    this.columnIndexMapping = new int[extCols];

    for (int i = 0; i < extCols; i++) {
      ColumnDescriptor extColumn = this.inputData.getColumn(i);
      String extColName = extColumn.columnName();
      if (extColName.equalsIgnoreCase(GemFireXDManager.RESERVED_COLUMN_TIMESTAMP)) {
        if (extColumn.columnTypeCode() != DataType.TIMESTAMP.getOID()) {
          throw new UnsupportedTypeException(
              "External table schema is invalid. Column " + extColName
                  + " must be of type 'TIMESTAMP', if defined.");
        }
        this.timestampColumnIndex = i;
        // GemFireXDManager.TS_COLUMN_NAME should not be defined in a
        // Gemfirexd table. But someone did.
        // TODO Make it a reserved name in GemFireXD
        continue;
      }

      if (extColName.equalsIgnoreCase(GemFireXDManager.RESERVED_COLUMN_EVENTTYPE)) {
        if (extColumn.columnTypeCode() != DataType.VARCHAR.getOID()) {
          throw new UnsupportedTypeException(
              "External table schema is invalid. Column " + extColName
                  + " must be of type 'VARCHAR', if defined.");
        }
        this.eventTypeColumnIndex = i;
        // TODO Make it a reserved name in GemFireXD
        continue;
      }

      // Column index starts from one.
      for (int j = 1; j <= gfxdCols; j++) {
        if (extColName.equalsIgnoreCase(gfxdMetadata.getColumnName(j))) {
          if (!GemFireXDManager.matchColumnTypes(gfxdMetadata.getColumnType(j),
              extColumn.columnTypeCode())) {
            throw new UnsupportedTypeException(
                "External table schema is invalid. Column " + extColName + "("
                    + extColumn.columnTypeName()
                    + ") does not match with the one in GemFireXD table.");
          }
          this.columnIndexMapping[i] = j;
          break;
        }
      }

      if (this.columnIndexMapping[i] == 0) {
        throw new SQLException("External table schema is invalid. Column "
            + extColName + "(" + extColumn.columnTypeName()
            + ") is undefined in GemFireXD table.");
      }
    }
  }
}
