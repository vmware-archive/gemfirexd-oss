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
package com.pivotal.gemfirexd.internal.tools.dataextractor.snapshot;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSGatewayEventImpl;
import com.gemstone.gemfire.internal.cache.Token;
import com.gemstone.gemfire.internal.cache.snapshot.SnapshotPacket.SnapshotRecord;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.util.Hex;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.ddl.DDLConflatable;
import com.pivotal.gemfirexd.internal.engine.ddl.GfxdDDLRegion.RegionValue;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore;
import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.engine.store.offheap.OffHeapRow;
import com.pivotal.gemfirexd.internal.engine.store.offheap.OffHeapRowWithLobs;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SchemaDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TableDescriptor;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.store.raw.ContainerHandle;
import com.pivotal.gemfirexd.internal.iapi.store.raw.ContainerKey;
import com.pivotal.gemfirexd.internal.iapi.types.CollatorSQLClob;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.JSON;
import com.pivotal.gemfirexd.internal.iapi.types.SQLBlob;
import com.pivotal.gemfirexd.internal.iapi.types.SQLClob;
import com.pivotal.gemfirexd.internal.iapi.types.SQLVarchar;
import com.pivotal.gemfirexd.internal.iapi.types.XML;
import com.pivotal.gemfirexd.internal.tools.dataextractor.Timer;
import com.pivotal.gemfirexd.internal.tools.dataextractor.extractor.GemFireXDDataExtractorImpl;
import com.pivotal.gemfirexd.internal.tools.dataextractor.report.views.RegionViewInfoPerMember;

/**
 * Writes a snapshot file.
 */
public final class GFXDSnapshotExporter {
  private static final String DDL_META_REGION_PATH = Region.SEPARATOR
      + GemFireStore.DDL_STMTS_REGION;
  private static final String SQL_COMMENT = "--";
  private static final String NEW_LINE = System.getProperty("line.separator");
  private static final String QUEUE_FILE_PREFIX = "QUEUE__";
  private static final String NULL_STRING = "NULL";
  private static final String EMPTY_STRING = "";
  /***
   * Number of rows that are to be kept in memory.
   */

  private String schemaName = null;
  private String tableName = null;
  private String bucketName = null;
  private String queueSchemaName = null;
  private String queueTableName = null;
  private BufferedWriter writer;
  private long newestTimeStamp = Integer.MIN_VALUE;
  private List listOfStats;
  private GFXDSnapshotExportStat stats;
  private File file;
  private String regionName; 
  /*****
   * Cache for the RowFormatters, populated right before salvaging and overwritten when moving onto next nodes/ddls
   */
  public static Map<String, RowFormatter> tableNameRowFormatterMap;

  private boolean isDDLExport = false;
  private boolean isQueueExport = false;

  private Map<Long, String> seqIdDdlMap = new TreeMap<Long, String>();
  // I don't think this variable is used...
  private Map<Long, DDLConflatable> seqIdDdlConflatableMap = new TreeMap<Long, DDLConflatable>();

  private long maxSeqId = Long.MIN_VALUE;
  private int numDdlStmts = 0;
  private String stringDelimiter;
  // this is used for ddl export. When the last schema does not match current
  // schema, we will need to export a change schema into the ddl
  private String lastSchema = "APP";

  private Map<Long, HDFSGatewayEventImpl> seqIdToEventsMap = new TreeMap<Long, HDFSGatewayEventImpl>();
  private Timer exportTimer = new Timer();
  private boolean initOk = false;

  public GFXDSnapshotExporter(File out, String region, List listOfStats,
      RegionViewInfoPerMember regionViewInfo, String stringDelimiter)
      throws IOException {
    // super(out, region);
    file = out;
    this.listOfStats = listOfStats;
    this.stringDelimiter = stringDelimiter;
    this.regionName = region;
    
    if (region.equals(DDL_META_REGION_PATH)) {
      tableName = region;
      isDDLExport = true;
      initOk = true;
    } else if (region.startsWith("/_") ||
        region.equals(GFXDSnapshot.PDX_TYPES_REGION)) {
      initOk = true;
      tableName = region;
    } else if (region.contains(GFXDSnapshot.QUEUE_REGION_SHORT)) {
      isQueueExport = true;
      initOk = true;
    } else {
      String[] regionStrings = region.split("/");
      schemaName = regionStrings[1];
      tableName = regionStrings[2];
      if (regionStrings.length > 4) {
        bucketName = regionStrings[4];
        bucketName = bucketName.replace("/", "_");
      }
      try {
        RowFormatter formatter = getRowFormatter(schemaName, tableName);
        initOk = true;
      } catch (StandardException e) {
        GemFireXDDataExtractorImpl.logInfo("Unable to locate meta data for table: " + schemaName + "." + tableName);
        return;
      } catch (NullPointerException e) {
        GemFireXDDataExtractorImpl.logInfo("Could not get the meta data for " + schemaName + "." + tableName);
        return;
      }
    }

    writer = new BufferedWriter(new FileWriter(out), 64 * 1024);
    exportTimer.start();
    stats = new GFXDSnapshotExportStat(schemaName, tableName, bucketName,
        regionViewInfo);
  }

  public void writeSnapshotEntry(Object key, Object value,
      VersionTag versionTag, long lastModified) throws IOException {
    if (isDDLExport) {
      if (value instanceof RegionValue) {
        try {
          Long seqId = (Long) key;
          if (maxSeqId < seqId) {
            maxSeqId = seqId;
          }
          //Add a check to make sure its a DDLConflatable and not any other statement in the data dictionary
          if (((RegionValue) value).getValue() instanceof DDLConflatable) {
            DDLConflatable ddl = (DDLConflatable) ((RegionValue) value).getValue();
            StringBuilder sb = new StringBuilder();
            sb.append(ddl.getValueToConflate()).append(";").append(SQL_COMMENT)
                .append(Long.toString(lastModified));
            seqIdDdlMap.put(seqId, sb.toString());
            seqIdDdlConflatableMap.put(seqId, ddl);

            if (newestTimeStamp < lastModified) {
              newestTimeStamp = lastModified;
            }
            numDdlStmts++;
         }
        } catch (Exception e) {
          e.printStackTrace();
        }
      } 
    } else if (isQueueExport) {
       Object entryObj = value;
       //Store the the event in a map sorted by the sequence id
        if (entryObj instanceof HDFSGatewayEventImpl) {
          HDFSGatewayEventImpl event = (HDFSGatewayEventImpl) entryObj;
          String regionName = event.getRegionToConflate();
          
          String[] tokens = regionName.split(Region.SEPARATOR);
          String schemaName = tokens[1];
          String tableName = tokens[2];
          RowFormatter formatter = null;
          try {
            formatter = getRowFormatter(schemaName, tableName);
          } catch (NullPointerException npe) {
            //GemFireXDDataExtractor.logInfo("Could not get meta data for  " + schemaName + "." + tableName);
          } catch (StandardException e) {
            // TODO Auto-generated catch block
          }
          
          if (formatter !=null) {
            seqIdToEventsMap.put(event.getEventSequenceID().getSequenceID(),
                event);
          }
        } 
    } else if (tableName.startsWith("/_") ||
        tableName.equals(GFXDSnapshot.PDX_TYPES_REGION)) {
      return;
    } else {
      try {
        RowFormatter formatter = getRowFormatter(schemaName, tableName);
        writeRow(formatter, null, value);

        if (versionTag != null) {
          if (newestTimeStamp < versionTag.getVersionTimeStamp()) {
            newestTimeStamp = versionTag.getVersionTimeStamp();
          }
        } else {
          if (newestTimeStamp < lastModified) {
            newestTimeStamp = lastModified;
          }
        }
      } catch (StandardException e) {
        stats.addEntryDecodeError("Did not complete decoding file for table:"
            + schemaName + "/" + tableName);
        stats.setCompletedFile(false);
        e.printStackTrace();
      }
    }
  }

  /**
   * Writes an entry in the snapshot.
   * 
   * @param entry
   *          the snapshot entry
   * @throws IOException
   *           unable to write entry
   */
  public void writeSnapshotEntry(SnapshotRecord entry,
      VersionTag versionTag, long lastModified) throws IOException {
  }

  /*****
   * Gets the {@link RowFormatter} for a given schema name and table name.
   * 
   * @param schemaName
   * @param tableName
   * @return
   * @throws StandardException
   */
  public RowFormatter getRowFormatter(String schemaName, String tableName)
      throws StandardException {
    StringBuilder sb = new StringBuilder(schemaName).append(tableName);
    String tableNameSchemaName = sb.toString();
    RowFormatter rowFormatter = tableNameRowFormatterMap
        .get(tableNameSchemaName);
    if (rowFormatter == null) {
      DataDictionary dd = Misc.getMemStore().getDatabase()
          .getDataDictionary();
      TransactionController tc = Misc.getLanguageConnectionContext()
          .getTransactionCompile();
      SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName, tc, true);
      TableDescriptor tableDescriptor = dd.getTableDescriptor(tableName, sd,
          tc);

      if (tableDescriptor == null) {
        sd = dd.getSchemaDescriptor("APP", tc, true);
        tableDescriptor = dd.getTableDescriptor(tableName, sd, tc);
      }

      GemFireContainer container = Misc.getMemStore().getContainer(
          ContainerKey.valueOf(ContainerHandle.TABLE_SEGMENT,
              tableDescriptor.getHeapConglomerateId()));
      rowFormatter = container.getCurrentRowFormatter();
      tableNameRowFormatterMap.put(tableNameSchemaName, rowFormatter);
    }
    return rowFormatter;
  }


  public void writeRowFromHDFSEvent(HDFSGatewayEventImpl event,
      BufferedWriter writer, SnapshotRecord entry,
      Object newParam) throws IOException, StandardException {
    
    
    StringBuilder sb = new StringBuilder();
    String regionName = event.getRegionToConflate();
    
    String[] tokens = regionName.split(Region.SEPARATOR);
    String schemaName = tokens[1];
    String tableName = tokens[2];
    RowFormatter formatter = null;
    try {
      formatter = getRowFormatter(schemaName, tableName);
    } catch (NullPointerException npe) {
      //GemFireXDDataExtractor.logInfo("Could not get meta data for  " + schemaName + "." + tableName);
    }
    
    if (formatter != null) {
      sb.append("--");
      sb.append(" Operation : ").append(event.getOperation());
      sb.append(" Version : ").append(event.getVersionTag().getEntryVersion());
      sb.append(" Table : " + tableName);
      sb.append(" Schema : " + schemaName);
      
      if (queueSchemaName == null && queueTableName == null) {
        queueSchemaName = schemaName;
        queueTableName = tableName;
      } 
      writer.write(sb.toString());
      writer.newLine();
      writeRow(formatter, entry, event.getValue());
    } 
  }

  // TODO: PERF: [sumedh] below approach is very inefficient; use
  // PXF's GemFireXDResolver.parseText instead which is at least
  // 4X faster than this
  public void writeRow(RowFormatter formatter, SnapshotRecord entry,
      Object deserializedObject) throws IOException {
    if (deserializedObject == null
        || Token.isInvalidOrRemoved(deserializedObject)
        || Token.isDestroyed(deserializedObject)) {
      stats.incrementNumValuesNotDecoded();
      return;
    }
    stats.incrementNumValuesDecoded();
    final int numColumns = formatter.getNumColumns();
    //final StringBuilder sb = new StringBuilder();
    final DataValueDescriptor[] dvds = new DataValueDescriptor[numColumns];

    Timer decodeTimer = new Timer();
    decodeTimer.start();

    final Class<?> c = deserializedObject.getClass();
    try {
      if (c == byte[].class) {
        formatter.getColumns((byte[])deserializedObject, dvds, null);
      }
      else if (c == byte[][].class) {
        formatter.getColumns((byte[][])deserializedObject, dvds, null);
      }
      else if (c == OffHeapRow.class) {
        formatter.getColumns((OffHeapRow)deserializedObject, dvds, null);
      }
      else {
        formatter.getColumns((OffHeapRowWithLobs)deserializedObject, dvds,
            null);
      }
    } catch (StandardException se) {
      // stats.addEntryDecodeError("Error decoding row for key:" +
      // Arrays.toString(entry.getKey()));
      // TODO: [sumedh] this should have proper handling
      se.printStackTrace();
    }
    for (int index = 0; index < numColumns; ++index) {
      final DataValueDescriptor dvd = dvds[index];
      try {
        if (dvd != null) {
          final Class<?> cls = dvd.getClass();
          if (cls == SQLClob.class || cls == CollatorSQLClob.class) {
            writer.append(stringDelimiter);
            char[] chars = ((SQLClob)dvd).getCharArray();
            for (int i = 0; i < chars.length; i++) {
              writer.append(chars[i]);
            }
            writer.append(stringDelimiter);
          }
          else if (cls == JSON.class) {
            writer.append(stringDelimiter);
            writer.append("'{\"JSON\":\"JSON\"}'");
            writer.append(stringDelimiter);
          }
          else if (cls == SQLBlob.class) {
            writeString(writer, Hex.toHex(((SQLBlob)dvd).getBytes()),
                true);
          }
          else if (cls == XML.class) {
            writeString(writer, (((XML)dvd).getString()), true);
          }
          else if (SQLVarchar.class.isAssignableFrom(cls)) {
            writeString(writer, dvd.toString(), true);
          }
          else {
            writeString(writer, dvd.toString(), false);
          }
        }
        else {
          writeString(writer, "NULL", false);
        }

        if (index + 1 < numColumns)
          writer.append(",");
      } catch (StandardException e) {
        // stats.addEntryDecodeError("Error decoding row for key:" +
        // Arrays.toString(entry.getKey()));
        // TODO: [sumedh] this should have proper handling
        e.printStackTrace();
      }
    }
    writer.newLine();

  }

  private void writeString(Writer writer, String string,
      boolean useStringDelimiter) throws IOException {
    if (string.equals(NULL_STRING)) {
      writer.append(EMPTY_STRING);
    } else {
      if (useStringDelimiter) {
        writer.append(stringDelimiter).append(string).append(stringDelimiter);
      } else {
        writer.append(string);
      }
    }
  }

  public long getNewestTimeStamp() {
    return newestTimeStamp;
  }

  public boolean isInitOk() {
    return initOk;
  }


  public void close() throws IOException {

    // Write the sorted ddls to the file
    if (isDDLExport) {

      for (Map.Entry<Long, DDLConflatable> entry : seqIdDdlConflatableMap
          .entrySet()) {
        Long seqId = entry.getKey();
        DDLConflatable ddl = entry.getValue();
        writer.write(NEW_LINE);

        if (!ddl.getCurrentSchema().equals(lastSchema)) {
          lastSchema = ddl.getCurrentSchema();
          writer.write("SET CURRENT SCHEMA " + lastSchema /*
                                                           * which is
                                                           * currently the
                                                           * current schema
                                                           */+ ";");
          writer.write(NEW_LINE);
        }

        StringBuilder sb = new StringBuilder();
        // seqIdWithOffset = seqId + schemaChangeOffset;
        String ddlStatment = ddl.getValueToConflate();
        
        //If the statement already has a semi-colon don't add it while exporting
        if (ddlStatment.endsWith(";")) {
          writer.write(ddlStatment);
        } else {
          writer.write(sb.append(ddl.getValueToConflate()).append(";")
              .toString());// .append(SQL_COMMENT).append(Long.toString(lastModified));
        }
        
        writer.write(NEW_LINE);
      }
    } else if (isQueueExport) {

      if (!seqIdToEventsMap.isEmpty()) {
        Iterator<Map.Entry<Long, HDFSGatewayEventImpl>> queueIter =
            seqIdToEventsMap.entrySet().iterator();
        GemFireXDDataExtractorImpl.logInfo("Writing hdfs events from Queue :  "
            + regionName);

        while (queueIter.hasNext()) {
          Map.Entry<Long, HDFSGatewayEventImpl> entry = queueIter.next();
          try {
            writeRowFromHDFSEvent(entry.getValue(), writer, null, null);
          } catch (StandardException e) {
            GemFireXDDataExtractorImpl.logInfo(
                "Error occurred while writing the HDFS queue event", e);
          }
        }
        seqIdToEventsMap.clear();
      }
      else {
        GemFireXDDataExtractorImpl.logInfo("No queued HDFS events in Queue : "
            + regionName);
        file.delete();
        return;
      }
    } 
    writer.flush();
    writer.close();
    
    if (!isDDLExport && !isQueueExport && this.stats.numValuesDecoded == 0 && this.stats.numValuesNotDecoded == 0) {
      file.delete();
    }
    else {
      // rename the file
      String newFileName = "";
      boolean renamed = false;
      String tableType = "";

      if (isDDLExport) {
        //newFileName = "exported_ddl_" + maxSeqId + ".sql";
        newFileName = "exported_ddl" + ".sql";
        renamed = file.renameTo(new File(file.getParentFile(), newFileName));
        stats.setMaxSeqId(maxSeqId);
        stats.setNumDdlStmts(numDdlStmts);
        stats.setLastModifiedTime(getNewestTimeStamp());
        
      } else if (isQueueExport) {
        newFileName = QUEUE_FILE_PREFIX + "_" + queueSchemaName + "_"
            + queueTableName + ".csv";
        renamed = file.renameTo(new File(file.getParentFile(), newFileName));
        stats.setQueueExport(true);
        
      } else {
        tableType = (bucketName == null ? "RR" : "PR");
        newFileName = tableType + "-" + schemaName + "-" + tableName;
        if (bucketName != null) {
          newFileName += "-" + bucketName;
        }
        newFileName += "-" + getNewestTimeStamp() + ".csv";

        if (stats.isCompletedFile() == false
            || stats.getEntryDecodeErrors().size() > 0) {
          newFileName = "ERROR_" + newFileName;
        }
        renamed = file.renameTo(new File(file.getParentFile(), newFileName));
      }

      if (!isQueueExport && tableName.startsWith("/_")
          && !tableName.equals(DDL_META_REGION_PATH)) {
        return;
      }
      stats.setFileName(file.getParentFile().getAbsolutePath() + file.separator
          + newFileName);
      stats.setTableType(tableType);
      
      listOfStats.add(stats);
      if (renamed == false) {
        throw new IOException("unable to rename file to desired csv file name: "
            + newFileName);
      }
    }
  }
}
