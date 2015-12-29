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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.GfxdVTITemplate;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.PublicAPI;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.jdbc.ConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.services.context.ContextService;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultColumnDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.SystemColumn;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedResultSetMetaData;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.XPLAINStatementDescriptor;
/**
 * Returns statement plans captured in this local VM. This automatically filters
 * out statements that originated from remote members.
 * 
 * @author soubhikc
 * 
 */
public class StatementPlansVTI extends GfxdVTITemplate {

  private ResultSet statementPlans = null;
  private EmbedConnection conn = null;
  boolean runTimeStaticsMode;
  
  @Override
  public boolean next() throws SQLException {

    this.runTimeStaticsMode = false;
    PreparedStatement ps = null;
    if (statementPlans == null) {
      boolean contextSet = false;
      try {
        ConnectionContext cc = (ConnectionContext)ContextService.getFactory()
            .getCurrentContextManager()
            .getContext(ConnectionContext.CONTEXT_ID);
        conn = (EmbedConnection)cc.getNestedConnection(true);

        if (conn == null) {
          conn = GemFireXDUtils.getTSSConnection(true, true, true);
        }
        conn.getTR().setupContextStack();
        contextSet = true;

        LanguageConnectionContext lcc = conn.getLanguageConnection();
        this.runTimeStaticsMode = lcc.getRunTimeStatisticsMode();
        lcc.setRunTimeStatisticsMode(false, true);

        ps = conn.prepareStatementByPassQueryInfo(-1,
            "select * from " + GfxdConstants.PLAN_SCHEMA
                + ".SYSXPLAIN_STATEMENTS where ORIGIN_MEMBER_ID = CURRENT_MEMBER_ID and LOCALLY_EXECUTED = 'false' ", false,
            false, false, null, 0, 0);
//        ps.setString(1, DiagProcedures.getDistributedMemberId());
        statementPlans = ps.executeQuery();

        if (SanityManager.DEBUG) {
          if (GemFireXDUtils.TracePlanGeneration) {
            final String region = Misc.getRegionPath(GfxdConstants.PLAN_SCHEMA
                + "." + XPLAINStatementDescriptor.TABLENAME_STRING);
            SanityManager.DEBUG_PRINT(
                GfxdConstants.TRACE_PLAN_GENERATION,
                "plans collected locally in this VM is "
                    + Misc.getRegion(region, true, false).size());
          }
        }
      } catch (StandardException se) {
        throw PublicAPI.wrapStandardException(se);
      } finally {
        if (contextSet) {
          conn.getTR().restoreContextStack();
        }
        if (statementPlans == null && conn != null) {
          conn.getLanguageConnection().setRunTimeStatisticsMode(
              this.runTimeStaticsMode, true);
          conn.close();
        }
      }
    }

    boolean retVal = statementPlans.next();
    if (retVal == false) {
      try {
        statementPlans.close();
        if (ps != null) {
          ps.close();
        }
      } finally {
        conn.getLanguageConnection().setRunTimeStatisticsMode(
            this.runTimeStaticsMode, true);
        conn.close();
      }
    }
    if(SanityManager.DEBUG) {
      if (GemFireXDUtils.TracePlanGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
            " StatementPlanVTI: returning has more rows = " + retVal);
      }
    }
    return retVal;
  }

  @Override
  protected Object getObjectForColumn(int columnNumber) throws SQLException {
    return statementPlans.getObject(columnNumber);
  }

  @Override
  public String getString(int columnIndex) throws SQLException {
    return statementPlans.getString(columnIndex);
  }

  @Override
  public boolean getBoolean(int columnIndex) throws SQLException {
    return statementPlans.getBoolean(columnIndex);
  }

  @Override
  public byte getByte(int columnIndex) throws SQLException {
    return statementPlans.getByte(columnIndex);
  }

  @Override
  public short getShort(int columnIndex) throws SQLException {
    return statementPlans.getShort(columnIndex);
  }

  @Override
  public int getInt(int columnIndex) throws SQLException {
    return statementPlans.getInt(columnIndex);
  }

  @Override
  public long getLong(int columnIndex) throws SQLException {
    return statementPlans.getLong(columnIndex);
  }

  @Override
  public float getFloat(int columnIndex) throws SQLException {
    return statementPlans.getFloat(columnIndex);
  }

  @Override
  public double getDouble(int columnIndex) throws SQLException {
    return statementPlans.getDouble(columnIndex);
  }

  @Override
  public byte[] getBytes(int columnIndex) throws SQLException {
    return statementPlans.getBytes(columnIndex);
  }

  @Override
  public java.sql.Date getDate(int columnIndex) throws SQLException {
    return statementPlans.getDate(columnIndex);
  }

  @Override
  public java.sql.Time getTime(int columnIndex) throws SQLException {
    return statementPlans.getTime(columnIndex);
  }

  @Override
  public java.sql.Timestamp getTimestamp(int columnIndex) throws SQLException {
    return statementPlans.getTimestamp(columnIndex);
  }

  private static ResultColumnDescriptor[] columnInfo;

  private static ResultSetMetaData metadata;

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    if (metadata == null) {
      SystemColumn[] stmtCols = new XPLAINStatementDescriptor()
          .buildColumnList();
      columnInfo = new ResultColumnDescriptor[stmtCols.length];

      for (int i = 0; i < stmtCols.length; i++) {
        columnInfo[i] = EmbedResultSetMetaData.getResultColumnDescriptor(
            stmtCols[i].getName(), stmtCols[i].getType());
      }

      metadata = new EmbedResultSetMetaData(columnInfo);
    }
    return metadata;
  }

}
