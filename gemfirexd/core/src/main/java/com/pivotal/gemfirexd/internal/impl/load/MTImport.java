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

package com.pivotal.gemfirexd.internal.impl.load;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.concurrent.ConcurrentTLongObjectHashMap;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.impl.jdbc.Util;
import com.pivotal.gemfirexd.internal.vti.VTITemplate;

/**
 * An intermediate VTI around a blocking queue to allow for multiple threads to
 * do the processing for IMPORT while the main thread only pushes into the
 * queue.
 * 
 * @author swale
 * @since gfxd 1.0
 */
public class MTImport extends VTITemplate {

  private static final AtomicLong currentImportId = new AtomicLong(0);
  private static final ConcurrentTLongObjectHashMap<ArrayBlockingQueue<QueueData>>
    queueMap = new ConcurrentTLongObjectHashMap<ArrayBlockingQueue<QueueData>>(4);

  public static final QueueData QUEUE_EOF_TOKEN = new QueueData(null, 0);
  public static final long QUEUE_INVALID_ID = -1;

  static long putNewQueue(ArrayBlockingQueue<QueueData> queue) {
    long id = currentImportId.incrementAndGet();
    if (id == QUEUE_INVALID_ID) { // invalid ID
      id = currentImportId.incrementAndGet();
    }
    queueMap.putPrimitive(id, queue);
    return id;
  }

  static ArrayBlockingQueue<QueueData> getQueue(long id) {
    ArrayBlockingQueue<QueueData> queue = queueMap.getPrimitive(id);
    if (queue != null) {
      return queue;
    }
    else {
      throw new IllegalStateException("import ID=" + id + " not found!");
    }
  }

  static void removeData(long id) {
    queueMap.removePrimitive(id);
  }

  public static class QueueData {
    final char[][] rows;
    final int startLineNumber;

    public QueueData(char[][] rows, int lineNumber) {
      this.rows = rows;
      this.startLineNumber = lineNumber;
    }
  }

  private final ArrayBlockingQueue<QueueData> queue;
  private final ImportBase importer;

  public MTImport(long queueId, String importClassName, String inputFileName,
      String columnDelimiter, String characterDelimiter, String codeset,
      long offset, long endPosition, int noOfColumnsExpected,
      String columnTypes, boolean lobsInExtFile, int importCounter,
      String columnTypeNames, String udtClassNamesString) throws SQLException {
    this(queueId, importClassName, inputFileName, columnDelimiter,
        characterDelimiter, codeset, offset, endPosition, false,
        noOfColumnsExpected, columnTypes, lobsInExtFile, importCounter,
        columnTypeNames, udtClassNamesString);
  }

  public MTImport(long queueId, String importClassName, String inputFileName,
      String columnDelimiter, String characterDelimiter, String codeset,
      long offset, long endPosition, boolean hasColumnDefinition,
      int noOfColumnsExpected, String columnTypes, boolean lobsInExtFile,
      int importCounter, String columnTypeNames, String udtClassNamesString)
      throws SQLException {
    this.queue = getQueue(queueId);
    if (ImportBase._importers.containsKeyPrimitive(importCounter)) {
      this.importer = ImportBase._importers.getPrimitive(importCounter);
    }
    else {
    try {
      Class<?> importClass = ClassPathLoader.getLatest().forName(
          importClassName);
      if (hasColumnDefinition) {
        this.importer = (ImportBase)importClass.getConstructor(String.class,
            String.class, String.class, String.class, long.class, long.class,
            boolean.class, int.class, String.class, boolean.class, int.class,
            String.class, String.class).newInstance(inputFileName,
            columnDelimiter, characterDelimiter, codeset, offset, endPosition,
            hasColumnDefinition, noOfColumnsExpected, columnTypes,
            lobsInExtFile, importCounter, columnTypeNames, udtClassNamesString);
      }
      else {
        this.importer = (ImportBase)importClass.getConstructor(String.class,
            String.class, String.class, String.class, long.class, long.class,
            int.class, String.class, boolean.class, int.class, String.class,
            String.class).newInstance(inputFileName, columnDelimiter,
            characterDelimiter, codeset, offset, endPosition,
            noOfColumnsExpected, columnTypes, lobsInExtFile, importCounter,
            columnTypeNames, udtClassNamesString);
      }
      // set a StringReaderOpt to use as wrapper around the char[]s
      this.importer.importReadData.setCharsReader(new CharsReader(null, 0, 0));
    } catch (RuntimeException re) {
      throw LoadError.unexpectedError(re);
    } catch (Exception e) {
      throw LoadError.unexpectedError(e);
    }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean next() throws SQLException {
    if (this.importer.eofReceived) {
      return false;
    }
    while (true) {
      if (this.importer.currentRows != null) {
        char[] line = this.importer.currentRows[this.importer.currentRowIndex];
        if (line != null) {
          // parse the row into columns at this point
          try {
            this.importer.importReadData.resetCharsReader(line, 0, line.length);
            this.importer.next();
            return true;
          } finally {
            this.importer.currentRowIndex++;
          }
        }
        else {
          // current batch has ended
          this.importer.currentRows = null;
          this.importer.currentRowIndex = 0;
        }
      }
      Throwable t = null;
      try {
        QueueData qdata = this.queue.poll(1, TimeUnit.SECONDS);
        if (qdata != QUEUE_EOF_TOKEN) {
          if (qdata != null) {
            this.importer.currentRows = qdata.rows;
            this.importer.lineNumber = this.importer.importReadData.lineNumber =
                qdata.startLineNumber;
            this.importer.currentRowIndex = 0;
            continue;
          }
        }
        else {
          this.importer.nextRow = null;
          this.importer.eofReceived = true;
          return false;
        }
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        t = ie;
      }
      try {
        Misc.checkIfCacheClosing(t);
      } catch (RuntimeException re) {
        throw Util.generateCsSQLException(Misc.processRuntimeException(re,
            "MTImport", null));
      }
    }
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    return this.importer.getString(columnIndex);
  }

  @Override
  public java.sql.Clob getClob(int columnIndex) throws SQLException {
    return this.importer.getClob(columnIndex);
  }

  @Override
  public java.sql.Blob getBlob(int columnIndex) throws SQLException {
    return this.importer.getBlob(columnIndex);
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    return this.importer.getBytes(columnIndex);
  }

  @Override
  public Object getObject(int columnIndex) throws SQLException {
    return this.importer.getObject(columnIndex);
  }

  @Override
  public boolean wasNull() {
    return this.importer.wasNull();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getRow() throws SQLException {
    return this.importer.getRow();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    return this.importer.getMetaData();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() throws SQLException {
    this.importer.close();
  }
}
