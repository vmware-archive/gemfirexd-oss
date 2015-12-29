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
package com.pivotal.gemfirexd.internal.engine.distributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.internal.StructImpl;
import com.gemstone.gemfire.cache.query.internal.types.ObjectTypeImpl;
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.execute.BucketMovedException;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.message.BitSetSet;
import com.pivotal.gemfirexd.internal.engine.distributed.message.GfxdConfigMessage;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedResultSet;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedStatement;
import com.pivotal.gemfirexd.internal.jdbc.InternalDriver;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

/**
 * 
 * @author Asif
 */
public class GfxdDumpLocalResultMessage extends GfxdMessage {

  private boolean dumpBackingMap;
  
  public GfxdDumpLocalResultMessage() {
    dumpBackingMap = false;
  }
  
  public GfxdDumpLocalResultMessage(boolean dumpBackingMap) {
    this.dumpBackingMap = dumpBackingMap;
  }

  @Override
  protected void processMessage(DistributionManager dm) {
    LogWriter logger = dm.getLoggerI18n().convertToLogWriter();
    try {
      this.executeLocally(dm, this.dumpBackingMap);
    } catch (Exception ex) {
      // Log a severe log in case of an exception
      if (logger.severeEnabled()) {
        logger.severe("GfxdDumpLocalResultMessage: SQL exception in executing "
            + "message with fields as " + this.toString(), ex);
      }
      if (this.processorId > 0 || dm == null) {
        throw new ReplyException("Unexpected SQLException on member "
            + dm.getDistributionManagerId(), ex);
      }
    }
  }

  @Override
  protected void sendReply(ReplyException ex, DistributionManager dm) {
    ReplyMessage.send(getSender(), this.processorId, ex, dm, null);
  }

  @Override
  protected boolean waitForNodeInitialization() {
    return true;
  }

  private void dumpTables(EmbedConnection conn, String sql) throws SQLException {
    EmbedStatement stmnt = (EmbedStatement)conn.createStatement();
    ResultSet rs = stmnt.executeQueryByPassQueryInfo(sql,
        false /* needGfxdSubactivation*/, true, 0, null);
    StructTypeImpl sti = getStructType(rs);

    StringBuilder aStr = new StringBuilder("ResultSet from gemfirexd is ");
    
    int size = 0;
    
    String[] fieldNames = sti.getFieldNames();
    int columnSize = fieldNames.length;
    
    try {

      while (((EmbedResultSet)rs).lightWeightNext()) {
        Object[] objects = new Object[columnSize];
  
        for (int i = 0; i < columnSize; i++) {
          objects[i] = rs.getObject(fieldNames[i]);
        } // to get the values for each column
        
        StructImpl aStruct = new StructImpl(sti, objects);
        aStr.append(aStruct.toString()).append("\n"); 
        size++;
        
        if (aStr.length() > (4 * 1024 * 1024)) {
          SanityManager.DEBUG_PRINT("dump:localdataset", aStr.toString());
          aStr.setLength(0);
        }
      }
      
      SanityManager.DEBUG_PRINT("dump:localdataset", aStr.toString());
    } finally {
      ((EmbedResultSet)rs).lightWeightClose();
    }

    aStr.append("The size of list is ").append(size).append("\n");
  }

  public void executeLocally(DM dm, boolean dumpBackingMap) throws SQLException {
  
    SanityManager.DEBUG_PRINT("dump:localdataset",
        "GfxdDumpLocalResultMessage: Executing with fields as: "
            + this.toString());
  
    EmbedConnection conn = (EmbedConnection)InternalDriver.activeDriver()
        .connect(GfxdConstants.PROTOCOL, new Properties());
    conn = conn.getRootConnection();
    List<String> tablesNames = new ArrayList<String>();
    EmbedStatement stmnt1 = (EmbedStatement)conn.createStatement();
    try {
      ResultSet rs1 = stmnt1.executeQuery("select tableschemaname, tablename "
          + "from sys.systables where tabletype = 'T' ");
      while (rs1.next()) {
        String schemaname = rs1.getString(1);
        String tablename = rs1.getString(2);
        tablesNames.add(schemaname + "." + tablename);
      }
      rs1.close();
    } finally {
      stmnt1.close();
    }
    
    Iterator<String> tableItr = tablesNames.iterator();
    while (tableItr.hasNext()) {
      final String tablename = tableItr.next();
      final String query = "select * from " + tablename;
      SanityManager.DEBUG_PRINT("dump:localdataset",
          "GfxdDumpLocalResultMessage: Dumping result of query =: " + query);
      boolean success = false;
      try {
        if (conn.isClosed() || !conn.isValid(0)) {
            conn.close();
            conn = (EmbedConnection)InternalDriver.activeDriver().connect(
                GfxdConstants.PROTOCOL, new Properties());
        }
        dumpTables(conn, query);
        success = true;
      } catch (BucketMovedException ex) {
        // Ignore BucketMovedException
      } catch (SQLException ex) {
        boolean ignoreException = false;
        if (ex.getCause() != null) {
          if (ex.getCause() instanceof BucketMovedException) {
            ignoreException = true;
          }
          else if (ex.getCause().getCause() != null) {
            if (ex.getCause().getCause() instanceof BucketMovedException) {
              ignoreException = true;
            }
            // no more nesting ??
          }
        }

        if (ignoreException) {
          SanityManager.DEBUG_PRINT("GfxdDumpLocalResultMessage:",
              " Ignoring BucketMovedException for sql " + query);
        }
        else {
          throw ex;
        }
      if (this.dumpBackingMap) {
        SanityManager.DEBUG_PRINT("dump:backingmap", "GfxdDumpLocalResultMessage: Dumping backingmap for " + tablename);
        Region r = Misc.getRegionForTableByPath(tablename, false);
        if ( r instanceof LocalRegion) {
          ((LocalRegion)r).dumpBackingMap();
        }
      }
    }
      if (success) {
        SanityManager.DEBUG_PRINT("dump:localdataset",
            "GfxdDumpLocalResultMessage: Successfully executed " + "query:= "
                + query);
      }
    }
  }

  @Override
  public byte getGfxdID() {
    return DUMP_LOCAL_RESULT;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void toData(final DataOutput out) throws IOException {
    super.toData(out);
    out.writeBoolean(this.dumpBackingMap);
  }

  public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.dumpBackingMap = in.readBoolean();
  }
  /**
   * To construct StructType from the resultSet given.
   * 
   * @param rs
   *          -- resultSet used to get StructType
   * @return StructType for each row of the resultSet
   */
  private static StructTypeImpl getStructType(ResultSet rs) throws SQLException {
    int numOfColumns;
    ResultSetMetaData rsmd;

    rsmd = rs.getMetaData();
    numOfColumns = rsmd.getColumnCount();

    ObjectType[] oTypes = new ObjectType[numOfColumns];
    String[] fieldNames = new String[numOfColumns];
    for (int i = 0; i < numOfColumns; i++) {
      try {
        String className = rsmd.getColumnClassName(i + 1);
        if ("byte[]".equals(className)) {
          oTypes[i] = new ObjectTypeImpl(byte[].class);
        }
        else if ("char[]".equals(className)) {
          oTypes[i] = new ObjectTypeImpl(char[].class);
        }
        else {
          oTypes[i] = new ObjectTypeImpl(Class.forName(rsmd
              .getColumnClassName(i + 1))); // resultSet column starts from 1
        }
      } catch (ClassNotFoundException cnfe) {
        LogWriter logger = Misc.getCacheLogWriter();
        if (logger.warningEnabled()) {
          logger.warning("GfxdDumpLocalResultMessage#getStructType: class "
              + "not found for " + rsmd.getColumnClassName(i + 1), cnfe);
        }
      }
      // resultSet column starts from 1
      fieldNames[i] = rsmd.getColumnName(i + 1);
    }

    StructTypeImpl sType = new StructTypeImpl(fieldNames, oTypes);
    return sType;
  }

  public static void sendBucketInfoDumpMsg(Set<DistributedMember> targetMembers, boolean dumpBackingMap) {
    try {
      GfxdConfigMessage<Object> msg = new GfxdConfigMessage<Object>(
          new GfxdListResultCollector(), targetMembers,
          GfxdConfigMessage.Operation.DUMP_BUCKETS,
          dumpBackingMap ? Boolean.TRUE : Boolean.FALSE, false);
      // disable HA
      msg.setHA(false);
      // don't need the results
      msg.executeFunction();
    } catch (Exception ex) {
      throw GemFireXDRuntimeException.newRuntimeException(
          "GfxdDumpLocalResultMessage: unexpected exception", ex);
    }
  }
}
