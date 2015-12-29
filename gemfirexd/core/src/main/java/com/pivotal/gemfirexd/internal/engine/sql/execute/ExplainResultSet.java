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
package com.pivotal.gemfirexd.internal.engine.sql.execute;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.w3c.dom.Element;

import com.gemstone.gemfire.internal.cache.TXState;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.ddl.catalog.GfxdSystemProcedures;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.iapi.error.PublicAPI;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.jdbc.ConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.services.loader.GeneratedMethod;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.ParameterValueSet;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.TargetResultSet;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnectionContext;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedPreparedStatement;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedResultSet;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedStatement;
import com.pivotal.gemfirexd.internal.impl.jdbc.Util;
import com.pivotal.gemfirexd.internal.impl.sql.compile.ValueNodeList;
import com.pivotal.gemfirexd.internal.impl.sql.execute.BasicNoPutResultSetImpl;
import com.pivotal.gemfirexd.internal.impl.sql.execute.PlanUtils;
import com.pivotal.gemfirexd.internal.impl.sql.execute.ResultSetStatisticsVisitor;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;
import com.pivotal.gemfirexd.internal.jdbc.InternalDriver;
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;
import com.pivotal.gemfirexd.tools.planexporter.AccessDistributedSystem;
import com.pivotal.gemfirexd.tools.planexporter.CreateXML;
import com.pivotal.gemfirexd.tools.utils.ExecutionPlanUtils;

/**
 * This result gets generated as part of Derby's ResultSet class hierarchy.
 * 
 * @author soubhikc
 * 
 */
public class ExplainResultSet extends BasicNoPutResultSetImpl {

  private ExecRow explainRow = null;

  private List<char[]> queryPlanSet;

  private Iterator<char[]> queryPlansIterator;

  private final String userQueryStr;
  
  private final ArrayList<ArrayList<Object>> queryParameters;
  
  private final XPLAINUtil.XMLForms xmlForm;
  private final String embedXslFileName;

  private final GeneratedMethod explainRowAllocator;
  
  @SuppressWarnings("unchecked")
  public ExplainResultSet(Activation act, GeneratedMethod explainRowAllocator,
      String userQuery, int queryParamIndex, int xmlForm,
      String embedXslFileName) {
    super(act, 0d, 0d);
    this.userQueryStr = userQuery;
    this.queryParameters = (ArrayList<ArrayList<Object>>)act.getSavedObject(queryParamIndex);
    this.xmlForm = XPLAINUtil.XMLForms.values()[xmlForm];
    this.embedXslFileName = embedXslFileName;
    this.explainRowAllocator = explainRowAllocator;
    this.isTopResultSet = true;
  }

  @Override
  public ExecRow getNextRowCore() throws StandardException {
   
    if (queryPlansIterator != null) {
      Element e = null;
      String planText = "Nothing";
      String member = "";
      while (queryPlansIterator.hasNext()) {
        ExecRow r = this.explainRow.getClone();
        char[] o = queryPlansIterator.next();
        if(o == null) {
          continue;
        }

        if (xmlForm == XPLAINUtil.XMLForms.none && (e = CreateXML.transformToXML(o)) != null) {
          member = e.getAttribute("member_node");
          planText = String.valueOf(getPlanAsText(e, null));
        }
        else {
          planText = String.valueOf(o);
        }
        
        r.getColumn(1).setValue(planText);
        return r;
      }
    }

    // in case no plan got captured, explainRow will be non-null.
    if (this.queryPlansIterator == null && this.explainRow != null) {
      createNoPlanRow();
      ExecRow r = this.explainRow;
      this.explainRow = null;
      return r;
    }

    return null;
  }

  @Override
  public void openCore() throws StandardException {

    isOpen = true;
    
    explainRow = (ExecRow)explainRowAllocator.invoke(activation);

    extractPlan(this.activation);
  }

  @Override
  public void close(boolean cleanupOnError) throws StandardException {
    this.isOpen = false;
    this.localTXState = null;
    this.localTXStateSet = false;
  }

  boolean isDerbyActivation;
  private void extractPlan(final Activation activation)
      throws StandardException {

    if (userQueryStr == null) {
      return;
    }

    EmbedConnectionContext eCtx = (EmbedConnectionContext)activation
        .getContextManager().getContext(ConnectionContext.CONTEXT_ID);

    final EmbedConnection nestedConn = createNestedConnection(eCtx.getEmbedConnection());

    final LanguageConnectionContext nestedConnLcc = nestedConn.getLanguageConnection();

    boolean runtimeOnOff = nestedConnLcc != null ? nestedConnLcc.getRunTimeStatisticsMode() : false;
    Statement st = null;
    try {
      // switch off the flag for preparation.
      if (nestedConnLcc != null) {
        nestedConnLcc.setRunTimeStatisticsMode(false, true);
      }

      final String aQuery = userQueryStr;
      
      final String queryID;
      st = nestedConn.createStatement();
      
      // Allow PREVIOUS as a keyword here for debugging purposes
      if (aQuery.matches("PREVIOUS")) {
        // We want the most recently run query that has an execution plan
        ResultSet rs = st.executeQuery(
            "SELECT STMT_ID FROM SYS.STATEMENTPLANS ORDER BY XPLAIN_TIME DESC");

        if (rs.next()) {
          queryID = rs.getString(1);
        }
        else {
          // maybe there is no previous statement!
          throw GemFireXDRuntimeException.newRuntimeException(
              "Previous statement to explain couldn't be determined ", null);
        }
        rs.close();
      }
      else {
        String type = XPLAINUtil.getStatementType(aQuery);
        if (type == null || type.length() <= 0) {
          // UUID
          queryID = aQuery.trim();
          assert queryID.length() == 36 && queryID.split("-").length == 5: "Wrong UUID="
              + queryID;
        }
        else {
          queryID = recordExecutionPlan(nestedConn, type, aQuery);
        }
      }
      
      queryPlanSet = new CreateXML(new AccessDistributedSystem(nestedConn, null, queryID, null), false, xmlForm, embedXslFileName).getPlan();
      queryPlansIterator = queryPlanSet.iterator();
    } catch (SQLException e) {
      throw StandardException.unexpectedUserException(e);
    } finally {
      if (st != null) {
        try {
          st.close();
        } catch (SQLException e) {
          // ignore
        }
      }
      if (nestedConnLcc != null) {
        nestedConnLcc.setRunTimeStatisticsMode(runtimeOnOff, true);
      }
      try {
        nestedConn.close();
      } catch (SQLException e) {
        //
      }
    }    
    
    /*
    try {
      planUtils = new ExecutionPlanUtils(eCtx.getEmbedConnection(),
          userQueryStr, queryParameters, true);
      
      AccessDistributedSystem ds = new AccessDistributedSystem(eCtx.getEmbedConnection(), userQueryStr,
          queryParameters);
      (new CreateXML(ds, false)).getPlan();      
      
      queryPlanSet = planUtils.getPlanAsXML();
      queryPlansIterator = queryPlanSet.iterator();
      
    } catch (SQLException sqle) {
      throw Misc.wrapSQLException(sqle, sqle);
    } finally {
    }
    */
  }
  
  public static EmbedConnection createNestedConnection(EmbedConnection conn) throws StandardException {
    final InternalDriver id = conn.getLocalDriver();
    if (id != null) {
      Connection nestedconn = id.getNewNestedConnection(conn);
      if (nestedconn != null) {
        assert nestedconn instanceof EmbedConnection;
        return (EmbedConnection)nestedconn;
      }
    }
    throw StandardException.newException(
        SQLState.NO_CURRENT_CONNECTION);
  }
  
  private String recordExecutionPlan(EmbedConnection nestedConn, String type, String aQuery) throws StandardException {

    if (XPLAINUtil.DDL_STMT_TYPE.equals(type)) {
      throw GemFireXDRuntimeException.newRuntimeException(
          "DDLs cannot be profiled", null);
    }
    EmbedPreparedStatement eps = null;
    try {

      GfxdSystemProcedures.SET_EXPLAIN_CONNECTION(1);

      ParameterValueSet pvs = null;
      if (XPLAINUtil.SELECT_APPROXIMATE_STMT_TYPE.equals(type)
          || XPLAINUtil.SELECT_STMT_TYPE.equals(type)) {

        final ResultSet rs;
        if (queryParameters != null) {
          if (queryParameters.size() > 1) {
            throw StandardException
                .newException(SQLState.LANG_ONLY_ONE_VALUE_LIST_ALLOWED,
                    queryParameters);
          }
          PreparedStatement ps = nestedConn.prepareStatement(aQuery);
          // set parameters.
          for (ArrayList<Object> v : queryParameters) {
            int i = 1;
            for (Object o : v) {
              ps.setObject(i++, o);
            }
          }
          rs = ps.executeQuery();
        }
        else if ( (pvs = getActivation().getParameterValueSet()) != null) {
          eps = (EmbedPreparedStatement)nestedConn.prepareStatement(aQuery);
          eps.getActivation().setParameters(pvs, null);
          rs = eps.executeQuery();
        }
        else {
          rs = nestedConn.createStatement().executeQuery(aQuery);
        }

        // just to consume every row/column data of the user query.
        // Misc.resultSetToXMLElement(rs, false);
        {
          ResultSetMetaData rsMD = rs.getMetaData();
          int numCols = rsMD.getColumnCount();
          int rowCount = 0;
          while (rs.next()) {
            StringBuilder row = new StringBuilder();
            rowCount++;
            for (int index = 1; index <= numCols; ++index) {
              String colName = rsMD.getColumnName(index);
              Object value = rs.getObject(index);
              row.append(colName).append("=").append(value);
            }
            if (SanityManager.DEBUG) {
              if (GemFireXDUtils.TracePlanGeneration) {
                SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
                    "Retrieved row " + rowCount + " as " + row.toString());
              }
            }
          }
        }

        rs.close();

        assert rs instanceof EmbedResultSet;
        com.pivotal.gemfirexd.internal.iapi.sql.ResultSet iapiResultSet = ((EmbedResultSet)rs)
            .getSourceResultSet();

        // if we are executing with a derby activation, CreateResultSet
        // shouldn't try to
        // generate compressed plan for isLocallyExecuted = false.
        isDerbyActivation = !(iapiResultSet instanceof AbstractGemFireResultSet);
        return iapiResultSet.getExecutionPlanID().toString();
      }
      else if (XPLAINUtil.INSERT_STMT_TYPE.equals(type)
          || XPLAINUtil.UPDATE_STMT_TYPE.equals(type)
          || XPLAINUtil.DELETE_STMT_TYPE.equals(type)
          || XPLAINUtil.CALL_STMT_TYPE.equals(type)) {

        if (queryParameters != null) {
          if (queryParameters.size() == 0) {
            throw StandardException.newException(
                SQLState.LANG_NO_VALUE_LIST_SUPPLIED, type, queryParameters);
          }
          PreparedStatement ps = nestedConn.prepareStatement(aQuery);

          boolean isBatchStatement = false;
          // set parameters.
          for (ArrayList<Object> v : queryParameters) {
            int i = 1;
            for (Object o : v) {
              ps.setObject(i++, o);
            }
            if (queryParameters.size() > 1) {
              ps.addBatch();
              isBatchStatement = true;
            }
          }

          if (!isBatchStatement) {
            ps.executeUpdate();
          }
          else {
            ps.executeBatch();
            ps.clearBatch();
          }

          return ((EmbedStatement)ps).getResultsToWrap().getExecutionPlanID()
              .toString();
        }
        else if ( (pvs = getActivation().getParameterValueSet()) != null) {
          eps = (EmbedPreparedStatement)nestedConn.prepareStatement(aQuery);
          eps.getActivation().setParameters(pvs, null);
          eps.executeUpdate();
          return eps.getResultsToWrap().getExecutionPlanID().toString();
        }
        else {
          EmbedStatement est = (EmbedStatement)nestedConn.createStatement();
          try {
            est.execute(aQuery);
            return est.getResultsToWrap().getExecutionPlanID().toString();
          } finally {
            est.close();
          }
        }
        
      }
      else {
        return null;
      }
    } catch (SQLException e) {
      throw StandardException.unexpectedUserException(e);
    } finally {
      try {
        GfxdSystemProcedures.SET_EXPLAIN_CONNECTION(0);
      } catch (SQLException e) {
        if (!SQLState.NO_CURRENT_CONNECTION.equals(e.getSQLState())) {
           throw StandardException.unexpectedUserException(e);
        }
      }
      
      if (eps != null) {
        try {
          eps.close();
        } catch (SQLException ignore) { }
      }
    }
  }
  
  private char[] getPlanAsText(Element e, String styleSheet) {
    if (styleSheet == null || styleSheet.length() <= 0) {
      styleSheet = "vanilla_text.xsl";
    }
    List<Element> l = new ArrayList<Element>();
    l.add(e);
    return Misc.serializeXMLAsCharArr(l, styleSheet);
  }
  
  private void createNoPlanRow() throws StandardException {
//    explainRow.getColumn(1).setValue(
//        Misc.getGemFireCache().getMyId().toString());
    explainRow.getColumn(1).setValue("No Query Plan available ");
  }

  @Override
  public void filteredRowLocationPostRead(TXState localTXState) throws StandardException {
    throw new AssertionError(this.getClass().getName()
        + " #filteredRowLocationPostRead is not expected to be called.");

  }

  @Override
  public void markRowAsDeleted() throws StandardException {
    throw new AssertionError(this.getClass().getName()
        + " #markRowAsDeleted is not expected to be called.");

  }

  @Override
  public void positionScanAtRowLocation(RowLocation rLoc)
      throws StandardException {

  }

  @Override
  public void setCurrentRow(ExecRow row) {
    throw new AssertionError(this.getClass().getName()
        + " #setCurrentRow is not expected to be called.");
  }

  @Override
  public void setNeedsRowLocation(boolean needsRowLocation) {
    throw new AssertionError(this.getClass().getName()
        + " #setNeedsRowLocation is not expected to be called.");

  }

  @Override
  public void setTargetResultSet(TargetResultSet trs) {
    throw new AssertionError(this.getClass().getName()
        + " #setTargetResultSet is not expected to be called.");

  }

  @Override
  public void updateRow(ExecRow row) throws StandardException {
    throw new AssertionError(this.getClass().getName()
        + " #updateRow is not expected to be called.");

  }

  @Override
  public void updateRowLocationPostRead() throws StandardException {
    throw new AssertionError(this.getClass().getName()
        + " #updateRowLocationPostRead is not expected to be called.");

  }

  @Override
  public void accept(ResultSetStatisticsVisitor visitor) {
    throw new AssertionError(this.getClass().getName()
        + " #accept is not expected to be called.");
  }

  @Override
  public void clearCurrentRow() {
    this.currentRow = null;
  }

  @Override
  public String getCursorName() {
    throw new AssertionError(this.getClass().getName()
        + " #getCursorName is not expected to be called.");
  }

  @Override
  public long getTimeSpent(int type, int timeType) {
    throw new AssertionError(this.getClass().getName()
        + " #getTimeSpent is not expected to be called.");
  }

  @Override
  public boolean needsRowLocation() {
    throw new AssertionError(this.getClass().getName()
        + " #needsRowLocation is not expected to be called.");
  }

  @Override
  public void rowLocation(RowLocation rl) throws StandardException {
    throw new AssertionError(this.getClass().getName()
        + " #rowLocation is not expected to be called.");
  }

  @Override
  public void closeRowSource() {
    throw new AssertionError(this.getClass().getName()
        + " #closeRowSource is not expected to be called.");
  }

  @Override
  public ExecRow getNextRowFromRowSource() throws StandardException {
    throw new AssertionError(this.getClass().getName()
        + " #getNextRowFromRowSource is not expected to be called.");
  }

  @Override
  public FormatableBitSet getValidColumns() {
    throw new AssertionError(this.getClass().getName()
        + " #getValidColumns is not expected to be called.");
  }

  @Override
  public boolean needsToClone() {
    throw new AssertionError(this.getClass().getName()
        + " #needsToClone is not expected to be called.");
  }
  
  @Override
  public StringBuilder buildQueryPlan(final StringBuilder builder, final PlanUtils.Context context) {
    return super.buildQueryPlan(builder, context);
  }
}
