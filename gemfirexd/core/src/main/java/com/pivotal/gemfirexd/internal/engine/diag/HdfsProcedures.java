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
package com.pivotal.gemfirexd.internal.engine.diag;

import java.sql.SQLException;
import java.sql.Timestamp;

import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.ddl.callbacks.CallbackProcedures;
import com.pivotal.gemfirexd.internal.engine.ddl.catalog.GfxdSystemProcedures;
import com.pivotal.gemfirexd.internal.engine.ddl.catalog.messages.GfxdSystemProcedureMessage;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.PublicAPI;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.ConnectionUtil;
import com.pivotal.gemfirexd.internal.iapi.util.StringUtil;
import com.pivotal.gemfirexd.internal.impl.jdbc.Util;

/**
 * Contains bodies of procedures for various HDFS related tasks.
 * 
 * @author sbawaska
 */
public final class HdfsProcedures extends GfxdSystemProcedures {

  // no instances
  private HdfsProcedures() {
  }

  // to enable this class to be included in gemfirexd.jar
  public static void dummy() {
  }

  /**
   * Get the estimate of the number of rows in the table. This is faster than
   * count(*) for tables with HDFSSTORE. The error is less than 2%
   * 
   * @param fqtn
   *          the fully qualified table name
   * @return the estimate of number of rows in table
   * @throws SQLException
   */
  public static long countEstimate(String fqtn) throws SQLException {
    String schema;
    String table;
    int dotIndex;
    // NULL table name is illegal
    if (fqtn == null) {
      throw Util.generateCsSQLException(SQLState.ENTITY_NAME_MISSING);
    }

    if ((dotIndex = fqtn.indexOf('.')) >= 0) {
      schema = fqtn.substring(0, dotIndex);
      table = fqtn.substring(dotIndex + 1);
    } else {
      schema = Misc.getDefaultSchemaName(ConnectionUtil.getCurrentLCC());
      table = fqtn;
    }
    try {
      final GemFireContainer container = CallbackProcedures
          .getContainerForTable(schema, table);
      return container.getRegion().sizeEstimate();
    } catch (StandardException se) {
      throw PublicAPI.wrapStandardException(se);
    }
  }

  /**
   * force compaction on the given table
   * 
   * @param fqtn
   *          fully qualified name of the table
   * @param maxWaitTime
   *          timeout in seconds, 0 waits forever
   * @throws SQLException
   * @throws StandardException
   */
  public static void forceCompaction(String fqtn, Integer maxWaitTime) throws SQLException, StandardException {
    String schema;
    String table;
    int dotIndex;
    // NULL table name is illegal
    if (fqtn == null) {
      throw Util.generateCsSQLException(SQLState.ENTITY_NAME_MISSING);
    }

    if ((dotIndex = fqtn.indexOf('.')) >= 0) {
      schema = fqtn.substring(0, dotIndex);
      table = fqtn.substring(dotIndex + 1);
    } else {
      schema = Misc.getDefaultSchemaName(ConnectionUtil.getCurrentLCC());
      table = fqtn;
    }
      try {
        final GemFireContainer container = CallbackProcedures
            .getContainerForTable(schema, table);
        container.getRegion().forceHDFSCompaction(true, maxWaitTime);
      } catch (StandardException se) {
        throw PublicAPI.wrapStandardException(se);
      } catch (UnsupportedOperationException e) {
        throw StandardException.newException(
            SQLState.HDFS_COMPACTION_NOT_SUPPORTED, e, fqtn);
      } catch (FunctionException e) {
        throw StandardException.newException(
            SQLState.GENERIC_PROC_EXCEPTION, e, e.getLocalizedMessage());
      }
  }

  /**
   * Forces the currently enqueued events to be flushed to HDFS immediately
   * regardless of batch time/size constraints.
   * 
   * @param fqtn fully qualified table name
   * @param maxWaitTime timeout in seconds, 0 is forever
   * @throws SQLException
   * @throws StandardException
   */
  public static void flushQueue(String fqtn, Integer maxWaitTime) throws SQLException, StandardException {
    String schema;
    String table;
    int dotIndex;
    // NULL table name is illegal
    if (fqtn == null) {
      throw Util.generateCsSQLException(SQLState.ENTITY_NAME_MISSING);
    }

    if ((dotIndex = fqtn.indexOf('.')) >= 0) {
      schema = fqtn.substring(0, dotIndex);
      table = fqtn.substring(dotIndex + 1);
    } else {
      schema = Misc.getDefaultSchemaName(ConnectionUtil.getCurrentLCC());
      table = fqtn;
    }
      try {
        final GemFireContainer container = CallbackProcedures
            .getContainerForTable(schema, table);
        container.getRegion().flushHDFSQueue(maxWaitTime);
      } catch (StandardException se) {
        throw PublicAPI.wrapStandardException(se);
      } catch (FunctionException e) {
        throw StandardException.newException(
            SQLState.GENERIC_PROC_EXCEPTION, e, e.getLocalizedMessage());
      }
  }
  
  /**
   * Returns the date/time when the previous major compaction
   * completed
   * @param fqtn the fully qualified table name
   * @return the Date
   * @throws StandardException
   */
  public static Timestamp HDFS_LAST_MAJOR_COMPACTION(String fqtn)
      throws SQLException, StandardException {
    long ts = 0;
    String schema;
    String table;
    int dotIndex;
    // NULL table name is illegal
    if (fqtn == null) {
      throw Util.generateCsSQLException(SQLState.ENTITY_NAME_MISSING);
    }

    if ((dotIndex = fqtn.indexOf('.')) >= 0) {
      schema = fqtn.substring(0, dotIndex);
      table = fqtn.substring(dotIndex + 1);
    } else {
      schema = Misc.getDefaultSchemaName(ConnectionUtil.getCurrentLCC());
      table = fqtn;
    }
    try {
      final GemFireContainer container = CallbackProcedures
          .getContainerForTable(schema, table);
      ts = container.getRegion().lastMajorHDFSCompaction();
    } catch (StandardException se) {
      throw PublicAPI.wrapStandardException(se);
    } catch (UnsupportedOperationException e) {
      throw StandardException.newException(
          SQLState.HDFS_COMPACTION_NOT_SUPPORTED, e, fqtn);
    }
    return new Timestamp(ts);
  }
  
  /**
   * Rolls over the write only HDFS table files that are being written. This makes the 
   * files available for processing by MapReduce and HAWQ. 
   * 
   * @param fqtn the fully qualified table name
   * @param minSizeForFileRollover in KBs. Files that are less than this size won't 
   * be rolled over. 
   * @throws StandardException
   */
  public static void HDFS_FORCE_WRITEONLY_FILEROLLOVER(String fqtn, 
      int minSizeForFileRollover) throws SQLException,
      StandardException {

    fqtn = StringUtil.SQLToUpperCase(fqtn);
    
    LocalRegion region = (LocalRegion)Misc.getRegionForTableByPath(fqtn, false);
    
    if (region == null)
      throw StandardException.newException(SQLState.LANG_TABLE_NOT_FOUND,  fqtn);
    
    if (!region.isHDFSRegion() ||  region.isHDFSReadWriteRegion())
      throw StandardException.newException(
          SQLState.HDFS_FILEROLLOVER_NOTSUPPORTED,  fqtn);
    
    final Object[] params = new Object[] { fqtn, minSizeForFileRollover};
    // first process locally
    GfxdSystemProcedureMessage.SysProcMethod.forceHDFSWriteonlyFileRollover
        .processMessage(params, Misc.getMyId());
    
    // then publish to other members excluding locators
    publishMessage(params, false,
        GfxdSystemProcedureMessage.SysProcMethod.forceHDFSWriteonlyFileRollover,
        false, false);
  }
}
