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
package com.pivotal.gemfirexd.internal.impl.sql.execute;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.pivotal.gemfirexd.internal.catalog.UUID;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.access.MemConglomerate;
import com.pivotal.gemfirexd.internal.engine.access.index.MemIndex;
import com.pivotal.gemfirexd.internal.engine.distributed.ResultHolder;
import com.pivotal.gemfirexd.internal.engine.distributed.message.StatementExecutorMessage;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.procedure.coordinate.ProcedureProcessorResultSet;
import com.pivotal.gemfirexd.internal.engine.sql.catalog.XPLAINDistPropsDescriptor;
import com.pivotal.gemfirexd.internal.engine.sql.execute.DistributionPlanCollector;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireDeleteResultSet;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireDistributedResultSet;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireRegionSizeResultSet;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireResultSet;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireUpdateResultSet;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GfxdSubqueryResultSet;
import com.pivotal.gemfirexd.internal.engine.sql.execute.NcjPullResultSet;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore;
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore.StoreStatistics;
import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.jdbc.ConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.services.i18n.MessageService;
import com.pivotal.gemfirexd.internal.iapi.services.info.JVMInfo;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableProperties;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultSet;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ColumnDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.DataDictionary;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecPreparedStatement;
import com.pivotal.gemfirexd.internal.iapi.store.access.Qualifier;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedStatement;
import com.pivotal.gemfirexd.internal.impl.services.uuid.BasicUUID;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.XPLAINResultSetDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.XPLAINResultSetTimingsDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.XPLAINScanPropsDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.XPLAINSortPropsDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.XPLAINStatementDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.XPLAINStatementTimingsDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.XPLAINTableDescriptor;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;
import com.pivotal.gemfirexd.tools.planexporter.CreateXML;
import com.pivotal.gemfirexd.tools.planexporter.StatisticsCollectionObserver;

/**
 * Class that will capture per statement per execution statistics for statement
 * plan generation.
 * 
 * Implementation similar to Derby 10.7 XPLAINSystemTableVisitor
 * 
 * @author soubhikc
 * 
 */
public final class StatementPlanCollector extends AbstractStatisticsCollector {

  private final boolean no_call_stmts = true;

  // ---------------------------------------------------------
  // member variables
  // ---------------------------------------------------------

  // the needed system objects for writing to the dictionary
  private LanguageConnectionContext lcc;

  private Connection nestedConnection = null;
  
  private DataDictionary dd;

  // the stmt activation object
  private BaseActivation activation;

  // a flag which is used to reflect if the statistics timings is on
  private boolean considerTimingInformation = false;
  
  private ExecPreparedStatement preStmt; 

  // the different tuple descriptors describing the query characteristics
  // regarding the stmt
  private XPLAINStatementDescriptor stmt;

  private XPLAINStatementTimingsDescriptor stmtTimings = null;

  private UUID stmtUUID; // the UUID to save for the resultsets

  // now the lists of descriptors regarding the resultsets
  private final ArrayList<XPLAINResultSetDescriptor> rsets; // for the resultset

  // descriptors

  private final List<XPLAINResultSetTimingsDescriptor> rsetsTimings; // for the

  // resultset
  // timings

  // descriptors

  private final List<XPLAINSortPropsDescriptor> sortrsets; // for the sort props

  // descriptors

  private final List<XPLAINScanPropsDescriptor> scanrsets; // for the scan props

  // descriptors

  private final List<XPLAINDistPropsDescriptor> dsets; // for the

  // distribution

  // props decriptor

  // this stack keeps track of the result set UUIDs, which get popped by the
  // children of the current explained node
  private final ArrayDeque<UUID> UUIDStack;
  
  private final XPLAINUtil.ChildNodeTimeCollector childTiming;

  private final DistributionPlanCollector distributionPlan;
  
  private final StoreStatistics stats;
  
  public final StatisticsCollectionObserver observer = StatisticsCollectionObserver.getInstance();

  public StatementPlanCollector(final ResultSetStatisticsVisitor nextCollector) {
    super(nextCollector);
    // System.out.println("System Table Visitor created...");
    // initialize lists
    rsets = new ArrayList<XPLAINResultSetDescriptor>();
    rsetsTimings = new ArrayList<XPLAINResultSetTimingsDescriptor>();
    sortrsets = new ArrayList<XPLAINSortPropsDescriptor>();
    scanrsets = new ArrayList<XPLAINScanPropsDescriptor>();
    dsets = new ArrayList<XPLAINDistPropsDescriptor>();

    // init UUIDStack
    UUIDStack = new ArrayDeque<UUID>();

    childTiming = new XPLAINUtil.ChildNodeTimeCollector(
        null);
    distributionPlan = new DistributionPlanCollector(
        this,
        dsets);
    stats = Misc.getMemStore().getStoreStatistics();
  }

  @Override
  public ResultSetStatisticsVisitor getClone() {
    return new StatementPlanCollector(
        super.getClone());
  }

  /**
   * helper method, which pushes the UUID, "number of Children" times onto the
   * UUIDStack.
   * 
   * @param uuid
   *          the UUID to push
   */
  private void pushUUIDnoChildren(
      final UUID uuid) {
    for (int i = 0; i < noChildren; i++) {
      UUIDStack.push(uuid);
    }
  }

  public UUID popUUIDFromStack() {
    return UUIDStack.pop();
  }

  public void pushUUIDToStack(
      final UUID id) {
    UUIDStack.push(id);
  }

  // ---------------------------------------------------------
  // XPLAINVisitor Implementation
  // ---------------------------------------------------------
  // called for remote DMLs other than SELECTs
  @Override
  public <T> void process(EmbedConnection conn,
      final StatementExecutorMessage<T> msg, final EmbedStatement est,
      boolean isLocallyExecuted) throws StandardException {

    final long beginTime = NanoTimer.getTime();
    XPLAINTableDescriptor.registerStatements(conn);
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TracePlanGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
            "Generating plan for " + est.getSQLText());
      }
    }
    
    if( msg.getSender() != null) {
      sender = msg.getSender().toString();
    }
    
    final ResultSet resultsToWrap = est.getResultsToWrap();
    
    processResultSet(conn, msg, null, resultsToWrap, est.getGPrepStmt(), isLocallyExecuted);
    
    stats.collectStatementPlanStats( (NanoTimer.getTime() - beginTime), true /*remote*/);
    if (nextCollector != null) {
      nextCollector.process(conn, msg, est, isLocallyExecuted);
    }
    else {
      est.getResultsToWrap().resetStatistics();
    }
  }

  // called for remote SELECTs
  @Override
  public <T> void process(EmbedConnection conn,
      final StatementExecutorMessage<T> msg, final ResultHolder rh,
      boolean isLocallyExecuted) throws StandardException {

    final long beginTime = NanoTimer.getTime();
    XPLAINTableDescriptor.registerStatements(conn);
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TracePlanGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
            "Generating plan for message : " + msg);
      }
    }
    
    if( msg.getSender() != null) {
      sender = msg.getSender().toString();
    }
    
    final ResultSet rs = rh.getERS().getSourceResultSet();
    
    processResultSet(conn, msg, rh, rs, rh.getGPrepStmt(), isLocallyExecuted);
    
    stats.collectStatementPlanStats( (NanoTimer.getTime() - beginTime), true /*remote*/);
    if (nextCollector != null) {
      nextCollector.process(conn, msg, rh, isLocallyExecuted);
    }
    else {
      rs.resetStatistics();
    }
  }

  private <T> void processResultSet(EmbedConnection conn,
      final StatementExecutorMessage<T> msg, final ResultHolder rh,
      final com.pivotal.gemfirexd.internal.iapi.sql.ResultSet rs,
      final com.pivotal.gemfirexd.internal.iapi.sql.PreparedStatement gps,
      final boolean isLocallyExecuted) throws StandardException {

    if (conn == null || conn.isClosed()) {
      return;
    }
    
    init((BaseActivation)rs.getActivation(), gps);
    
    // for nested queries runtimeStats mode will be temporarily switched off 
    // on the data store.
    if(!lcc.getRunTimeStatisticsMode() || nestedConnection == null) {
      return;
    }

    this.activation.setExecutionID(msg.getExecutionId());

    boolean continuePlanCapture = true;

    continuePlanCapture = generateStatementDescriptor(
        msg.getConstructTime(),
        msg.getEndProcessTime(),
        isLocallyExecuted);
    
    if (continuePlanCapture) {
      distributionPlan.setup(activation);

      distributionPlan.processMessage(msg, rh, isLocallyExecuted);

      // doesn't matter here as statisticTiming is already set.
      // moreover, per message timing flag gets set on the lcc and hence
      // no race here.
      doXPLAIN(rs, activation, false, considerTimingInformation, isLocallyExecuted);
    }
  }

  /*
   * called in iapi.ResultSet.close
   * 
   * timeStatsEnabled flag added for single VM case handling (#44201)
   */
  @Override
  public void doXPLAIN(
      final ResultSet rs,
      final Activation activation,
      final boolean genStatementDesc, 
      final boolean timeStatsEnabled, 
      final boolean isLocallyExecuted) throws StandardException {

    final long beginTime = NanoTimer.getTime();

    try {
      if (SanityManager.DEBUG) {
        if (GemFireXDUtils.TracePlanGeneration) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
                  "Capturing ResultSet processing plan for connectionID=" + activation.getConnectionID() 
                      + " statementID=" + activation.getStatementID() 
                      + " executionID=" + activation.getExecutionID());
        }
      }

      boolean continuePlanCapture = true;
      if (genStatementDesc) {
        Activation act = rs.getActivation();
        if (SanityManager.ASSERT) {
          SanityManager.ASSERT(!act.isClosed(),
              "activation shouldn't be closed at this point");
        }
        init((BaseActivation)act, act.getPreparedStatement());

        if (nestedConnection == null) {
          return;
        }

        // get the timings settings
        considerTimingInformation = timeStatsEnabled;

        continuePlanCapture = generateStatementDescriptor(
            rs.getBeginExecutionTimestamp(), rs.getEndExecutionTimestamp(),
            isLocallyExecuted);
      }

      if (continuePlanCapture) {
        GemFireTransaction parentTran = null;
        try {

          if (SanityManager.DEBUG) {
            if (GemFireXDUtils.TracePlanGeneration) {
              SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
                  "StatementPlanCollector: starting from root resultset " + rs);
            }
          }
          // get TopRSS and start the traversal of the RSS-tree
          rs.accept(this);

          if (observer != null) {
            observer.processSelectMessage(stmt, rsets, rsetsTimings, scanrsets,
                sortrsets, dsets);
          }

          parentTran = lcc.getParentOfNestedTransactionExecute();
          if (parentTran != null) {
            parentTran.suspendTransaction();
          }
          // add the filled lists to the dictionary
          addArraysToSystemCatalogs();
        } catch (final SQLException e) {
          throw Misc.wrapSQLException(e, e);
        } finally {
          if (parentTran != null) {
            parentTran.resumeTransactionIfSuspended();
          }
        }

      }
    } finally {
      clean();
    }
    
    stats.collectStatementPlanStats( (NanoTimer.getTime() - beginTime), false /*queryNode*/);
    if (genStatementDesc && nextCollector != null) {
      nextCollector.doXPLAIN(rs, activation, genStatementDesc, timeStatsEnabled, isLocallyExecuted);
    }
    
    if (observer != null) {
      observer.end();
    }
  }

  @Override
  public UUID getStatementUUID() {
    return stmtUUID;
  }
  
  /*this method should get called exactly once per doXPLAIN */
  private boolean generateStatementDescriptor(
      Timestamp beginExeTime,
      Timestamp endExeTime,
      boolean isLocallyExecuted) throws StandardException {

    if (SanityManager.ASSERT) {
      if (preStmt == null) {
        SanityManager.THROWASSERT("statement null for activation "
            + activation);
      }
    }
    
    // extract stmt type
    final String type = XPLAINUtil.getStatementType(preStmt.getUserQueryString(activation.getLanguageConnectionContext()));

    // don`t explain CALL Statements, quick implementation
    if (type == null || type.equalsIgnoreCase("C") && no_call_stmts) {
      return false;
    }

    // placeholder for the stmt timings UUID
    UUID stmtTimingsUUID = null;

    // 1. create new Statement Descriptor

    // create new UUID
    long stmt_id = this.activation.getStatementID();
    if(stmt_id == -1) {
      if (GemFireXDUtils.TracePlanGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
            "Returning without plan capture due to stmt_id == -1");
      }
      return false;
    }
    stmtUUID = dd.getUUIDFactory().createUUID(stmt_id,
        this.activation.getExecutionID());
    
    if(isLocallyExecuted) {
      ((BasicUUID)stmtUUID).setLocallyExecuted(1);
    }
    
    // get transaction ID
    final String xaID = lcc.getTransactionExecute().getTransactionIdString();
    // get session ID
    final String sessionID = Integer.toString(lcc.getInstanceNumber());
    // get the JVM ID
    final String jvmID = Integer.toString(JVMInfo.JDK_ID);
    // get the OS ID
    final String osID = System.getProperty("os.name");
    // the current system time
    final long current = System.currentTimeMillis();
    // the xplain type
    final String XPLAINtype = lcc.explainConnection() ? XPLAINUtil.XPLAIN_ONLY
        : XPLAINUtil.XPLAIN_FULL;
    // the xplain time
    final Timestamp time = new Timestamp(
        current);
    // the thread id
    final String threadID = Thread.currentThread().toString();

    final String origin_member;
    if (lcc.isConnectionForRemote() && sender != null) {
      origin_member = sender;
    }
    else {
      
      if(SanityManager.ASSERT) {
        // remote connection won't execute a query locally ever.
        SanityManager.ASSERT(isLocallyExecuted || !lcc.isConnectionForRemote());
      }
      final GemFireCacheImpl c = Misc.getGemFireCacheNoThrow();
      if (c != null) {
        origin_member = c
            .getDistributedSystem()
            .getDistributedMember()
            .toString();
      }
      else {
        origin_member = null;
      }
    }

    long exeTime = -1;
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TracePlanGeneration) {
        if (endExeTime == null || beginExeTime == null) {
          SanityManager.THROWASSERT("beginExeTs=" + beginExeTime + " endExeTs="
              + endExeTime);
        }
      }
    }
    
    exeTime = endExeTime.getTime() - beginExeTime.getTime();
    
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TracePlanGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
            "Introducing Statement Descriptor for "
                + preStmt.getUserQueryString(activation.getLanguageConnectionContext())
                + " statementId="
                + activation.getStatementID()
                + " executionId="
                + activation.getExecutionID()
                + " uuid="
                + stmtUUID
                + (this.considerTimingInformation ? " with time stats "
                    : " without time stats ") + " isLocallyExecuted="
                + isLocallyExecuted, new Throwable());
      }
    }
    

    stmt = new XPLAINStatementDescriptor(
        stmtUUID, // unique statement UUID
        activation.getCursorName(), // the statement name
        type, // the statement type
        preStmt.getUserQueryString(activation.getLanguageConnectionContext()), // the statement text
        jvmID, // the JVM ID
        osID, // the OS ID
        String.valueOf(GemFireStore.getMyId()),
        origin_member,
        Boolean.valueOf(isLocallyExecuted).toString(),
        XPLAINtype, // the EXPLAIN tpye
        time, // the EXPLAIN Timestamp
        threadID, // the Thread ID
        xaID, // the transaction ID
        sessionID, // the Session ID
        lcc.getDbname(), // the Database name
        lcc.getDrdaID(), // the DRDA ID
        Long.valueOf(preStmt.getParseTimeInMillis()), // the Parse Time
        Long.valueOf(preStmt.getBindTimeInMillis()), // the Bind Time
        Long.valueOf(preStmt.getOptimizeTimeInMillis()), // the Optimize Time
        Long.valueOf(preStmt.getRoutingInfoTimeInMillis()), // the QueryInfo Time
        Long.valueOf(preStmt.getGenerateTimeInMillis()), // the Generate Time
        Long.valueOf(preStmt.getCompileTimeInMillis()), // the Compile Time
        Long.valueOf(exeTime), // the Execute Time
        preStmt.getBeginCompileTimestamp(), // the Begin Compilation TS
        preStmt.getEndCompileTimestamp(), // the End Compilation TS
        beginExeTime, // the Begin Execution TS
        endExeTime // the End Execution TS
    );

    // add it to system catalog
    GemFireTransaction parentTran = null;
    try {
      parentTran = lcc.getParentOfNestedTransactionExecute();
      if(parentTran != null) {
        parentTran.suspendTransaction();
      }
      addStmtDescriptorsToSystemCatalog();
    } catch (SQLException e) {
      if (GemFireXDUtils.TracePlanGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
            "Got Exception while adding statement descriptors to system catalog "
                + e, e);
      }
      throw StandardException.plainWrapException(e);
    } finally {
      if (parentTran != null) {
        parentTran.resumeTransactionIfSuspended();
      }
    }
    return true;
  }

  public void init(BaseActivation act,
      com.pivotal.gemfirexd.internal.iapi.sql.PreparedStatement preparedStatement) throws StandardException {
    activation = act;
    lcc = activation.getLanguageConnectionContext();
    dd = lcc.getDataDictionary();
    considerTimingInformation = lcc.getStatisticsTiming()
        || lcc.explainConnection();
    preStmt = (ExecPreparedStatement)preparedStatement;
    if (nestedConnection == null) {
      nestedConnection = getDefaultConn();
    }
  }
  
  @Override
  public void clear() {
    preStmt = null;
    activation = null;
    lcc = null;
    dd  = null;
  }
  
  // ---------------------------------------------------------
  // helper methods
  // ---------------------------------------------------------
  
  public UUID getStmtUUID() {
    return stmtUUID;
  }
  
  /**
   * This method cleans up things after explanation. It frees kept resources and
   * still holded references.
   */
  private void clean() {

    // forget about all the system objects
    activation = null;
    lcc = null;
    if (nestedConnection != null) {
      try {
        nestedConnection.close();
      } catch (SQLException ignore) { }
    }
    nestedConnection = null;
    dd = null;

    // forget about the stmt descriptors and the Stmt UUID
    stmt = null;
    stmtTimings = null;

    // reset the descriptor lists to keep memory low
    rsets.clear();
    rsetsTimings.clear();
    sortrsets.clear();
    scanrsets.clear();
    dsets.clear();

    // clear stack, although it must be already empty...
    UUIDStack.clear();
  }

  /**
   * Open a nested Connection with which to execute INSERT statements.
   */
  private final Connection getDefaultConn() throws StandardException {
    final ConnectionContext cc = (ConnectionContext)lcc
        .getContextManager()
        .getContext(
            ConnectionContext.CONTEXT_ID);
    
    if (cc == null) {
      return null;
    }
    
    Connection conn = null;
    try {
      conn = cc.getNestedConnection(true);
    } catch (SQLException sqle) {
      // ignore no current connection. instead return null.
      if (!SQLState.NO_CURRENT_CONNECTION.equals(sqle.getSQLState())) {
        throw Misc.wrapSQLException(sqle, sqle);
      }
    }

    if (SanityManager.ASSERT) {
      assert conn instanceof EmbedConnection;
      if (conn != null && ((EmbedConnection)conn).getLanguageConnectionContext() != lcc) {
        SanityManager
            .THROWASSERT("Nested Connection returning with different LCC ");
      }
    }

    return conn;
  }

  /**
   * This method writes only the stmt and its timing descriptor to the
   * dataDictionary
   * 
   */
  private void addStmtDescriptorsToSystemCatalog() throws StandardException,
      SQLException {
    final boolean statsSave = lcc.getRunTimeStatisticsMode();
    try {
      lcc.setRunTimeStatisticsMode(false, true);
      assert nestedConnection != null : "NestedConnection shouldn't be null at this point";
      if(GemFireXDUtils.TracePlanGeneration) {
        //sb fix lcc#cleanupOnError 3418 ac.reset() verifyStmtId(stmtUUID, conn);
      }
      PreparedStatement ps = nestedConnection.prepareStatement(lcc
          .getExplainStatement(XPLAINStatementDescriptor.TABLENAME_STRING));
      stmt.setStatementParameters(ps);
      int updateCount = ps.executeUpdate();
      if (SanityManager.ASSERT) {
        if (GemFireXDUtils.TracePlanGeneration) {
          if (updateCount != 1) {
            SanityManager.DEBUG_PRINT("warning", "insert for " + stmt
                + " statement should have succeeded. updateCount=" + updateCount);
          }
        }
      }
      ps.close();
    } finally {
      lcc.setRunTimeStatisticsMode(statsSave, true);
    }
  }

  /**
   * This method writes the created descriptor arrays to the cooresponding
   * system catalogs.
   * @throws IOException 
   */
  private void addArraysToSystemCatalogs() throws StandardException,
      SQLException {
    if (rsets.size() == 0) {
      if (GemFireXDUtils.TracePlanGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
            "NO XML to create as no query layers are present for " + stmtUUID);
      }
      return;
    }
    StringBuilder xmlFragment = new StringBuilder();
    try {
      rankResultSetsByTimings();
      createXMLFragment(0, 0, xmlFragment, new StringBuilder("root/"));
      
      if (GemFireXDUtils.TracePlanGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION, "XML Fragment aquired " + xmlFragment);
      
        if (GemFireXDUtils.TracePlanAssertion) {
          SanityManager.ASSERT(CreateXML.testXML(xmlFragment));
        }
      }
    }
    catch(Throwable t) {
      System.out.println("exception occurred.");
      t.printStackTrace(System.err);
      return;
    }
    
    final boolean statsSave = lcc.getRunTimeStatisticsMode();
    try {
      Iterator<? extends XPLAINTableDescriptor> iter;
      lcc.setRunTimeStatisticsMode(false, true);
      assert nestedConnection != null : "NestedConnection shouldn't be null at this point";

      PreparedStatement ps = nestedConnection.prepareStatement(lcc
          .getExplainStatement("SYSXPLAIN_RESULTSETS"));
      XPLAINResultSetDescriptor.setStatementParameters(nestedConnection, ps, stmtUUID, xmlFragment);
      final int updateCount = ps.executeUpdate();
      assert updateCount == 1: "insert for " + xmlFragment
          + " should have succeeded ";
      ps.close();
      
      /*
      iter = rsets.iterator();
      while (iter.hasNext()) {
        final XPLAINResultSetDescriptor rset = (XPLAINResultSetDescriptor)iter
            .next();
        rset.setStatementParameters(ps);
        if (SanityManager.DEBUG) {
          if (GemFireXDUtils.TracePlanGeneration) {
            SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
                "XPLAINResultSetDescriptor " + rset);
          }
        }
        final int updateCount = ps.executeUpdate();
        assert updateCount == 1: "insert for " + rset
            + " should have succeeded ";
        if (observer != null) {
          observer.processedResultSetDescriptor(rset);
        }
      }
      ps.close();

      // add the resultset timings descriptors, if timing is on
      if (considerTimingInformation) {
        ps = conn.prepareStatement(lcc
            .getExplainStatement("SYSXPLAIN_RESULTSET_TIMINGS"));
        iter = rsetsTimings.iterator();
        while (iter.hasNext()) {
          final XPLAINResultSetTimingsDescriptor rsetT = (XPLAINResultSetTimingsDescriptor)iter
              .next();
          rsetT.setStatementParameters(ps);
          if (SanityManager.DEBUG) {
            if (GemFireXDUtils.TracePlanGeneration) {
              SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
                  "XPLAINResultSetTimingsDescriptor " + rsetT);
            }
          }
          final int updateCount = ps.executeUpdate();
          assert updateCount == 1: "insert for " + rsetT
              + " timing info should have succeeded ";
          if (observer != null) {
            observer.processedResultSetTimingDescriptor(rsetT);
          }
        }
        ps.close();
      }
      ps = conn.prepareStatement(lcc
          .getExplainStatement("SYSXPLAIN_SCAN_PROPS"));
      iter = scanrsets.iterator();
      while (iter.hasNext()) {
        final XPLAINScanPropsDescriptor scanProps = (XPLAINScanPropsDescriptor)iter
            .next();
        scanProps.setStatementParameters(ps);
        if (SanityManager.DEBUG) {
          if (GemFireXDUtils.TracePlanGeneration) {
            SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
                "XPLAINScanPropsDescriptor " + scanProps);
          }
        }
        final int updateCount = ps.executeUpdate();
        assert updateCount == 1: "insert for " + scanProps
            + " scan info should have succeeded ";
        if (observer != null) {
          observer.processedScanPropsDescriptor(scanProps);
        }
      }
      ps.close();

      ps = conn.prepareStatement(lcc
          .getExplainStatement("SYSXPLAIN_SORT_PROPS"));
      iter = sortrsets.iterator();
      while (iter.hasNext()) {
        final XPLAINSortPropsDescriptor sortProps = (XPLAINSortPropsDescriptor)iter
            .next();
        sortProps.setStatementParameters(ps);
        if (SanityManager.DEBUG) {
          if (GemFireXDUtils.TracePlanGeneration) {
            SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
                "XPLAINSortPropsDescriptor " + sortProps);
          }
        }
        final int updateCount = ps.executeUpdate();
        assert updateCount == 1: "insert for " + sortProps
            + " sort info should have succeeded ";
        if (observer != null) {
          observer.processedSortPropsDescriptor(sortProps);
        }
      }
      ps.close();

      ps = conn.prepareStatement(lcc
          .getExplainStatement("SYSXPLAIN_DIST_PROPS"));
      iter = dsets.iterator();
      while (iter.hasNext()) {
        final XPLAINDistPropsDescriptor distProps = (XPLAINDistPropsDescriptor)iter
            .next();
        distProps.setStatementParameters(ps);
        if (SanityManager.DEBUG) {
          if (GemFireXDUtils.TracePlanGeneration) {
            SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
                "XPLAINDistributionPropsDescriptor " + distProps);
          }
        }
        final int updateCount = ps.executeUpdate();
        assert updateCount == 1: "insert for " + distProps
            + " distribution info should have succeeded ";
        if (observer != null) {
          observer.processedDistPropsDescriptor(distProps);
        }
      }
      ps.close();

      */
      
    } finally {
      lcc.setRunTimeStatisticsMode(statsSave, true);
    }
  }
  
  private void rankResultSetsByTimings() {
    
    // have to make a copy to determine the sort, note rsets captures the order of insertion.
    XPLAINResultSetDescriptor[] sortedDescs = rsets.toArray(new XPLAINResultSetDescriptor[rsets.size()]);
    Arrays.sort(sortedDescs);
    
    double totalExecuteTimeNanos = 0;
    for (XPLAINResultSetDescriptor r : rsets) {
      totalExecuteTimeNanos += r.getExecuteTime();
    }
    
    if (GemFireXDUtils.TracePlanGeneration) {
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
          "Ranking resultSets of size " + rsets.size()
              + " with TotalExecutionTimeNanos=" + totalExecuteTimeNanos
              + " ResultSets[]" + Arrays.toString(sortedDescs));
    }

    int rank = 1;
    for (XPLAINResultSetDescriptor r : sortedDescs) {
      r.setRank(rank++);
      r.setTotalExecuteTimeNanos(totalExecuteTimeNanos); 
    }
  }

  private int createXMLFragment(final int xmlDepth, int currentLevel, final StringBuilder sb, final StringBuilder lineage) {
    
     if (currentLevel >= rsets.size()) {
       return currentLevel;
     }
    
     final XPLAINResultSetDescriptor rdesc = rsets.get(currentLevel);
     
     final StringBuilder currentlineage = new StringBuilder(lineage).append(rdesc.rs_name).append("/");
     
     PlanUtils.addSpaces(sb, xmlDepth).append("<node");
     PlanUtils.xmlAttribute(sb, "lineage", currentlineage);
     rdesc.getXMLAttributes(sb, observer);
     if ( rdesc.num_children > 0) {
       sb.append(">\n");
     }

     for (int i = 1; i <= rdesc.num_children; i++) {
       currentLevel = createXMLFragment(xmlDepth + 1, currentLevel + 1, sb, currentlineage);
     }
    
     if ( rdesc.num_children > 0) {
       PlanUtils.addSpaces(sb, xmlDepth).append("</node>\n");
     }
     else {
       sb.append("/>\n");
     }
     
    if (observer != null) {
      observer.processedResultSetDescriptor(rdesc);
    }
     
     return currentLevel;
  }
  
  /**
   * Return the time for all operations performed by this node, but not the time
   * for the children of this node.
   * 
   */
  private long getNodeTime(
      final BasicNoPutResultSetImpl currentrs) {
    
    //TODO:[sb]:QP: implement appropriately for GemFireDistributedResultSet (excluding iteration timing).
    long time = currentrs.getTimeSpent(ResultSet.CURRENT_RESULTSET_ONLY, ResultSet.ALL);
    if (SanityManager.ASSERT) {
      if (GemFireXDUtils.TracePlanGeneration) {
        /*SanityManager.ASSERT(time > 0, currentrs
            + " had execute time zero for statementId=" + stmtUUID);*/
      }
    }
    
    // in case the clock didn't ticked.
    return time == 0 ? 1 : time;
  }

  private XPLAINResultSetTimingsDescriptor createResultSetTimingDescriptor(
      final BasicNoPutResultSetImpl bnprs,
      final UUID timingID) {

    return createResultSetTimingDescriptor(
        timingID,
        bnprs.constructorTime,
        bnprs.openTime,
        bnprs.nextTime,
        bnprs.closeTime,
        getNodeTime(bnprs),
        bnprs.rowsSeen,
        -1,
        -1);
  }

  private XPLAINResultSetTimingsDescriptor createResultSetTimingDescriptor(
      final NoRowsResultSetImpl nrrs,
      final UUID timingID) {

    return createResultSetTimingDescriptor(
        timingID,
        -1,
        -1,
        -1,
        -1,
        nrrs.getExecuteTime(),
        -1,
        -1,
        -1);
  }

  private XPLAINResultSetTimingsDescriptor createResultSetTimingDescriptor(
      final ProjectRestrictResultSet rs,
      final UUID timingID) {

    return createResultSetTimingDescriptor(
        timingID,
        rs.constructorTime,
        rs.openTime,
        rs.nextTime,
        rs.closeTime,
        getNodeTime(rs),
        rs.rowsSeen,
        rs.projectionTime,
        rs.restrictionTime);
  }

  public XPLAINResultSetTimingsDescriptor createResultSetTimingDescriptor(
      final UUID timingID,
      final long constructorTime,
      final long openTime,
      final long nextTime,
      final long closeTime,
      final long nodeTime,
      final int rowsSeen,
      final long projectionTime,
      final long restrictionTime) {

    final XPLAINResultSetTimingsDescriptor timing_desc = new XPLAINResultSetTimingsDescriptor(
        timingID, constructorTime, openTime, nextTime, closeTime, nodeTime,
        nextTime >= 0 ? XPLAINUtil.getAVGNextTime(nextTime, rowsSeen) : -1,
        projectionTime, restrictionTime, -1, // the
        // temp_cong_create_time
        -1 // the temo_cong_fetch_time
    );
    
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TracePlanGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
            "StatementPlanCollector: recording timing of resultset " + timing_desc);
      }
    }
    rsetsTimings.add(timing_desc);

    return timing_desc;
  }

  private XPLAINResultSetDescriptor createResultSetDescriptor(
      final BasicNoPutResultSetImpl rs,
      final XPLAINResultSetTimingsDescriptor timingID,
      final String lockMode,
      final String lockGran,
      final String rsxplaintype,
      final String rsxplaindetail,
      final XPLAINScanPropsDescriptor scan,
      final XPLAINSortPropsDescriptor sort, 
      final int rowsReturned) {

    return createResultSetDescriptor(
        rs.getClass().getSimpleName(),
        timingID,
        lockMode,
        lockGran,
        rsxplaintype,
        rsxplaindetail,
        rs.numOpens,
        rs.optimizerEstimatedRowCount,
        rs.optimizerEstimatedCost,
        rs.rowsSeen,
        rs.rowsFiltered,
        rowsReturned == -1 ? rs.rowsSeen - rs.rowsFiltered : rowsReturned,
        scan,
        sort, null);
  }

  private XPLAINResultSetDescriptor createResultSetDescriptor(
      final NoRowsResultSetImpl rs,
      final XPLAINResultSetTimingsDescriptor timingID,
      final String rsxplaintype,
      final String rsxplaindetail) {

    return createResultSetDescriptor(
        rs.getClass().getSimpleName(),
        timingID,
        null,
        null,
        rsxplaintype,
        rsxplaindetail,
        -1,
        -1,
        -1,
        -1,
        -1,
        -1,
        null,
        null, null);
  }

  public XPLAINResultSetDescriptor createResultSetDescriptor(
      final String rs_name,
      final XPLAINResultSetTimingsDescriptor timingID,
      final String lockMode,
      final String lockGran,
      final String rsxplaintype,
      final String rsxplaindetail,
      final int numOpens,
      final double optimizerEstimatedRowCount,
      final double optimizerEstimatedCost,
      final int rowsSeen,
      final int rowsFiltered,
      final int returned_rows,
      final XPLAINScanPropsDescriptor scan,
      final XPLAINSortPropsDescriptor sort, final XPLAINDistPropsDescriptor distdesc) {

    final UUID rsID = dd.getUUIDFactory().createUUID();

    final XPLAINResultSetDescriptor rsdesc = new XPLAINResultSetDescriptor(
        rs_name,
        noChildren,
        rsets.size(),
        rsID,
        rsxplaintype,
        rsxplaindetail,
        numOpens < 0 ? null : Integer.valueOf(numOpens),
        null, // the number of index updates
        lockMode, // lock mode
        lockGran, // lock granularity
        (UUIDStack.isEmpty() ? null : UUIDStack.pop()),
        optimizerEstimatedRowCount < 0 ? null : Double
            .valueOf(optimizerEstimatedRowCount),
        optimizerEstimatedCost < 0 ? null : Double
            .valueOf(optimizerEstimatedCost),
        null, // the affected rows
        null, // the deferred rows
        null, // the input rows
        Integer.valueOf(rowsSeen), // the seen rows
        null, // the seen rows right
        Integer.valueOf(rowsFiltered), // the filtered rows
        returned_rows < 0 ? null : returned_rows,// the returned rows
        null, // the empty right rows
        null, // index key optimization
        scan,
        sort,
        stmtUUID,
        timingID, // the stmt UUID
        distdesc);

    pushUUIDnoChildren(rsID);

    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TracePlanGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
            "StatementPlanCollector: recording resultset " + rsdesc);
      }
    }
    rsets.add(rsdesc);

    return rsdesc;
  }

  public XPLAINScanPropsDescriptor createScanPropDescriptor(
      final String scanObjectType,
      final String scanObjectName,
      final String startPosition,
      final String stopPosition,
      final UUID scanID,
      final int isoLevel,
      final int rowsPerRead,
      final Qualifier[][] qualifiers,
      final Properties scanProperties,
      final int[] hashkey_columns) {

    final XPLAINScanPropsDescriptor scanRSDescriptor = new XPLAINScanPropsDescriptor(
        scanID,
        scanObjectName,
        scanObjectType,
        null, // the scan type:
        // heap, btree, sort
        XPLAINUtil.getIsolationLevelCode(isoLevel), // the isolation level
        null, // the number of visited pages
        null, // the number of visited rows
        null, // the number of qualified rows
        null, // the number of visited deleted rows
        null, // the number of fetched columns
        null, // the bitset of fetched columns
        null, // the btree height
        rowsPerRead < 0 ? null : Integer.valueOf(rowsPerRead), // fetchSize
        startPosition,
        stopPosition,
        NoPutResultSetImpl.printQualifiers(qualifiers, true),
        null, // the next qualifiers
        XPLAINUtil.getHashKeyColumnNumberString(hashkey_columns), // the hash
        // key column
        // numbers
        null // the hash table size
    );

    final FormatableProperties props = new FormatableProperties();
    if (scanProperties != null) {
      for (final Enumeration<?> e = scanProperties.keys(); e.hasMoreElements();) {
        final String key = (String)e.nextElement();
        props.put(
            key,
            scanProperties.get(key));
      }
    }

    scanrsets.add(XPLAINUtil.extractScanProps(
        scanRSDescriptor,
        props));

    return scanRSDescriptor;
  }

  public XPLAINSortPropsDescriptor createSortPropDescriptor(
      final UUID sortID,
      final Properties props,
      final String sorttype,
      final int inputrows,
      final int outputrows,
      final int mergerows,
      final boolean eliminateDuplicates,
      final boolean inSortedOrder) {

    final XPLAINSortPropsDescriptor sortRSDescriptor = new XPLAINSortPropsDescriptor(
        sortID, // the sort props UUID
        sorttype, // the sort type, either (C)onstraint, (I)ndex or (T)able
        inputrows < 0 ? null : inputrows, // the number of input rows
        outputrows < 0 ? null : outputrows, // the number of output rows
        mergerows < 0 ? null : mergerows, // the number of merge runs
        null, // merge run details
        XPLAINUtil.getYesNoCharFromBoolean(eliminateDuplicates),// eliminate
        // duplicates
        XPLAINUtil.getYesNoCharFromBoolean(inSortedOrder), // in sorted order
        null // distinct_aggregate
    );

    sortrsets.add(XPLAINUtil.extractSortProps(
        sortRSDescriptor,
        props));
    
    return sortRSDescriptor;
  }

  public void createDistPropDescriptor(
      final XPLAINDistPropsDescriptor desc) {

    if(desc.getRSID() == null) {
      desc.setDistRSID(dd.getUUIDFactory().createUUID());
    }

    dsets.add(desc);
  }

  // ---------------------------------------------------------------------
  // visitor methods overridden 
  // ---------------------------------------------------------------------

  @Override
  public void visit(
      final GemFireDeleteResultSet rs) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(
      final DistinctGroupedAggregateResultSet rs) {
    // TODO Auto-generated method stub

  }
  
  @Override
  public void visit(
      final GroupedAggregateResultSet rs) {
    XPLAINResultSetTimingsDescriptor time_desc = null;

    if (considerTimingInformation) {
      UUID timingID = dd.getUUIDFactory().createUUID();
      time_desc = createResultSetTimingDescriptor(
          rs,
          timingID);
    }

    final String rsxplaintype;
    //TODO - print out the grouped column names underlying the operator
    final String rsxplaindetail;

    rsxplaintype = XPLAINUtil.OP_GROUP;
    
    createResultSetDescriptor(
        rs,
        time_desc,
        null,
        null,
        rsxplaintype,
        null, 
        null,
        null,
        rs.rowsReturned);

  }
  
  @Override
  public void visit(
      final DistinctScanResultSet rs) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(
      final ProjectRestrictResultSet rs) {
    XPLAINResultSetTimingsDescriptor time_desc = null;

    if (considerTimingInformation) {
      UUID timingID = dd.getUUIDFactory().createUUID();
      time_desc = createResultSetTimingDescriptor(
          rs,
          timingID);
    }

    final String rsxplaintype;
    final String rsxplaindetail;

    if (rs.restriction != null && rs.doesProjection) {
      rsxplaintype = XPLAINUtil.OP_PROJ_RESTRICT;
    }
    else if (rs.doesProjection) {
      rsxplaintype = XPLAINUtil.OP_PROJECT;
    }
    else if (rs.restriction != null) {
      rsxplaintype = XPLAINUtil.OP_FILTER;
    }
    else {
      rsxplaintype = XPLAINUtil.OP_PROJ_RESTRICT;
    }

    // Send projected column list as detail
    // TODO : also generate string equivalent of restriction nodes
    // and append to this detail
    // And handle scenarios with no projected columns as well
    if (rs.projectedColumns != null)
    {
      rsxplaindetail = rs.projectedColumns;
    }
    else
    {
      rsxplaindetail = null;
    }
    
    createResultSetDescriptor(
        rs,
        time_desc,
        null,
        null,
        rsxplaintype,
        rsxplaindetail, 
        null,
        null, 
        -1);
  }

  @Override
  public void visit(
      final IndexRowToBaseRowResultSet rs) {

    XPLAINResultSetTimingsDescriptor time_desc = null;

    if (considerTimingInformation) {
      UUID timingID = dd.getUUIDFactory().createUUID();
      time_desc = createResultSetTimingDescriptor(
          rs,
          timingID);
    }

    // GemStone changes BEGIN
    // Get string consisting of column names and put it in OP_DETAILS
    // For printing out during EXPLAIN
    // i.e. for a ROWIDSCAN details 
    //   LINEITEM : L_ORDERKEY, L_SHIPDATE, L_PARTKEY
    GemFireContainer gfc = rs.gfc;
    RowFormatter rf = gfc.getCurrentRowFormatter();
    StringBuilder accessedCols = new StringBuilder(rs.indexName).append(" : ");
    boolean first = true;
    if (rs.accessedHeapCols != null && rf != null)
    {
      for (int inPosition = 0; inPosition < rs.accessedHeapCols.getLength(); inPosition++)
      {
            if (rs.accessedHeapCols.isSet(inPosition))
            {
                    ColumnDescriptor cd = rf.getColumnDescriptor(inPosition);
                    if (cd == null) {
                      continue;
                    }
                    if (!first) {
                      accessedCols.append(", ");
                    }
                    else {
                      first = false;
                    }
                    accessedCols.append(cd.getColumnName());
            }
      }
    }
    
    createResultSetDescriptor(
        rs,
        time_desc,
        null,
        null,
        XPLAINUtil.OP_ROWIDSCAN,
        accessedCols.toString(),
        null,
        null, 
        -1);
    //Gemstone changes END
  }

  @Override
  public void visit(
      final ScrollInsensitiveResultSet rs) {

    XPLAINResultSetTimingsDescriptor time_desc = null;

    if (considerTimingInformation) {
      UUID timingID = dd.getUUIDFactory().createUUID();
      time_desc = createResultSetTimingDescriptor(
          rs,
          timingID);
    }

    createResultSetDescriptor(
        rs,
        time_desc,
        null,
        null,
        XPLAINUtil.OP_SCROLL,
        "(" + rs.resultSetNumber + "), " + "[" + rs.numFromHashTable + ", "
            + rs.numToHashTable + "]",
        null,
        null, 
        -1);
  }

  @Override
  public void visit(final GemFireResultSet rs) {
    
    XPLAINResultSetTimingsDescriptor time_desc = null;

    if (considerTimingInformation) {
      UUID timingID = dd.getUUIDFactory().createUUID();
      time_desc = createResultSetTimingDescriptor(
          timingID,
          -1,
          rs.openTime,
          rs.nextTime,
          rs.closeTime,
          rs.getTimeSpent(ResultSet.CURRENT_RESULTSET_ONLY, ResultSet.ALL),
          0,
          -1,
          -1);
    }

    // Make detail string for EXPLAIN showing
    //  Region name, column name that is source of GET
    GemFireContainer gfc = rs.getGFContainer();
    RowFormatter rf = rs.getProjectionFormat();
    StringBuilder rsexplaindetail = new StringBuilder(gfc.getSchemaName()).append(".").append(gfc.getTableName());
    if (rf != null)
    {
      boolean first = true;
      
      for (int inPosition = 0; inPosition < rf.getNumColumns(); inPosition++)
      {
        ColumnDescriptor cd = rf.getColumnDescriptor(inPosition);
        if (cd == null) {
          continue;
        }
        if (!first) {
          rsexplaindetail.append(", ");
        }
        else {
          first = false;
          rsexplaindetail.append(" ");
        }
        rsexplaindetail.append(cd.getColumnName());
      }
    }

    createResultSetDescriptor(
        rs.getClass().getSimpleName(),
        time_desc,
        null,
        null,
        rs.isGetAllLocalIndexPlan() ? XPLAINUtil.OP_LI_GETTALL : (rs
            .isGetAllPlan() ? XPLAINUtil.OP_GETTALL : XPLAINUtil.OP_GET),
        rsexplaindetail.toString(),
        1,
        rs.getEstimatedRowCount(),
        -1,
        -1,
        -1,
        rs.rowsReturned,
        null,
        null, null);    
  }
  
  @Override
  public void visit(
      final NormalizeResultSet rs) {

  }

  @Override
  public void visit(
      final AnyResultSet anyResultSet) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(
      final GemFireDistributedResultSet rs) {

    // createRe....();

    // create sort properties
    // aggregation , distinct, group by, special case outer join, n-way merge
    // happening
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TracePlanGeneration) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_PLAN_GENERATION,
            "StatementPlanCollector: Processing " + rs);
      }
    }

    distributionPlan.processGFDistResultSet(rs);
  }

  @Override
  public void visit(
      final LastIndexKeyResultSet rs) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(
      final MiscResultSet rs) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(
      final OnceResultSet rs) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(
      final ProcedureProcessorResultSet rs) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(
      final GfxdSubqueryResultSet rs) {
    // TODO Auto-generated method stub
    distributionPlan.processDistribution(rs);
  }
  
  @Override
  public void visit(
      final NcjPullResultSet rs) {
    // TODO Auto-generated method stub
    distributionPlan.processDistribution(rs);
  }

  @Override
  public void visit(
      final TemporaryRowHolderResultSet rs) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(
      final WindowResultSet rs) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(
      final SortResultSet rs) {
    XPLAINResultSetTimingsDescriptor time_desc = null;

    if (considerTimingInformation) {
      UUID timingID = dd.getUUIDFactory().createUUID();
      time_desc = createResultSetTimingDescriptor(
          rs,
          timingID);
    }

    UUID sortID = dd.getUUIDFactory().createUUID();

    final XPLAINSortPropsDescriptor sort_desc = createSortPropDescriptor(
        sortID,
        rs.sortProperties,
        null,
        -1,
        -1,
        -1,
        rs.distinct,
        rs.isInSortedOrder);

    XPLAINResultSetDescriptor rsdesc = createResultSetDescriptor(
        rs,
        time_desc,
        null,
        null,
        XPLAINUtil.OP_SORT,
        String.valueOf(rs.resultSetNumber),
        null,
        sort_desc, 
        rs.rowsReturned);

    // we have inputRows extra info than others... so update it.
    rsdesc.setInputRows(rs.rowsInput);
  }

  @Override
  public void visit(
      final HashTableResultSet rs) {

    XPLAINResultSetTimingsDescriptor time_desc = null;

    if (considerTimingInformation) {
      UUID timingID = dd.getUUIDFactory().createUUID();
      time_desc = createResultSetTimingDescriptor(
          rs,
          timingID);
    }

    final UUID scanID = dd.getUUIDFactory().createUUID();

    final XPLAINScanPropsDescriptor scan_desc = createScanPropDescriptor(
        null,
        "Temporary HashTable",
        null,
        null,
        scanID,
        TransactionController.ISOLATION_READ_COMMITTED,
        -1,
        rs.nextQualifiers,
        rs.scanProperties,
        rs.keyColumns);

    scan_desc.setHashtableSize(rs.hashtableSize);

    createResultSetDescriptor(
        rs,
        time_desc,
        null,
        null,
        XPLAINUtil.OP_HASHTABLE,
        "(" + rs.resultSetNumber + ")",
        scan_desc,
        null, 
        -1);
  }

  @Override
  public void visit(
      final UpdateResultSet rs) {

    XPLAINResultSetTimingsDescriptor time_desc = null;

    if (considerTimingInformation) {
      UUID timingID = dd.getUUIDFactory().createUUID();
      time_desc = createResultSetTimingDescriptor(
          rs,
          timingID);
    }

    final XPLAINResultSetDescriptor rsdesc = createResultSetDescriptor(
        rs,
        time_desc,
        XPLAINUtil.OP_UPDATE,
        null);

    rsdesc.setAffectedRows(rs.rowCount);
    rsdesc.setDeferredRows(rs.deferred);
    rsdesc.setIndexesUpdated(rs.constantAction.irgs.length);
  }

  @Override
  public void visit(
      final DeleteResultSet rs,
      final int overridable) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(
      final GemFireUpdateResultSet rs,
      final int overridable) {
    // TODO Auto-generated method stub

  }

  @Override
  public String toString() {
    return "STATEMENT PLAN COLLECTOR"
        + (nextCollector != null ? " + " + nextCollector.toString() : "");
  }

  @Override
  public void visit(
      GemFireRegionSizeResultSet regionSizeResultSet) {
    // TODO Auto-generated method stub

  }

  @Override
  public void visit(RowCountResultSet rs) {
    XPLAINResultSetTimingsDescriptor time_desc = null;
    
    if (considerTimingInformation) {
      UUID timingID = dd.getUUIDFactory().createUUID();
      time_desc = createResultSetTimingDescriptor(
          rs,
          timingID);
    }

    createResultSetDescriptor(
        rs,
        time_desc,
        null,
        null,
        XPLAINUtil.OP_ROW_COUNT,
        "(" + rs.resultSetNumber + ")",
        null,
        null, 
        -1);
  }

  @Override
  public void visit(RowResultSet rs) {
    XPLAINResultSetTimingsDescriptor time_desc = null;
    
    if (considerTimingInformation) {
      UUID timingID = dd.getUUIDFactory().createUUID();
      time_desc = createResultSetTimingDescriptor(
          rs,
          timingID);
    }
    
    createResultSetDescriptor(
        rs,
        time_desc,
        null,
        null,
        XPLAINUtil.OP_ROW,
        "(" + rs.resultSetNumber + ")",
        null,
        null,
        rs.rowsReturned);
  }
  
  @Override
  public void visit(UnionResultSet rs) {
    XPLAINResultSetTimingsDescriptor time_desc = null;
    
    if (considerTimingInformation) {
      UUID timingID = dd.getUUIDFactory().createUUID();
      time_desc = createResultSetTimingDescriptor(
          rs,
          timingID);
    }
    
    createResultSetDescriptor(
        rs,
        time_desc,
        null,
        null,
        XPLAINUtil.OP_UNION,
        "(" + rs.resultSetNumber + ")",
        null,
        null,
        rs.rowsReturned);
  }
  
  // ---------------------------------------------------------------------------------------
  // visitor methods that are more driven by AbstractStatisticsCollector.
  // ---------------------------------------------------------------------------------------

  @Override
  public void visit(
      final TableScanResultSet rs,
      final int overridable) {
    XPLAINResultSetTimingsDescriptor time_desc = null;

    if (considerTimingInformation) {
      UUID timingID = dd.getUUIDFactory().createUUID();
      time_desc = createResultSetTimingDescriptor(
          rs,
          timingID);
    }

    String lockString = null;
    if (rs.forUpdate()) {
      lockString = MessageService.getTextMessage(SQLState.LANG_EXCLUSIVE);
    }
    else {
      if (rs.isolationLevel == TransactionController.ISOLATION_READ_COMMITTED_NOHOLDLOCK) {
        lockString = MessageService
            .getTextMessage(SQLState.LANG_INSTANTANEOUS_SHARE);
      }
      else {
        lockString = MessageService.getTextMessage(SQLState.LANG_SHARE);
      }
    }
    final String lockMode = XPLAINUtil.getLockModeCode(lockString);
    final String lockGran = XPLAINUtil.getLockGranularityCode(lockString);

    String rsxplaintype = null, scanObjectType = null;
    String rsxplaindetail = null, scanObjectName = null;
    String startPosition = null;
    String stopPosition = null;

    if (rs.indexName != null) {
      if (rs.isConstraint()) {
        rsxplaintype = XPLAINUtil.OP_CONSTRAINTSCAN;
        scanObjectType = "C"; // constraint
        rsxplaindetail = "C: " + rs.indexName;
        //GemStone changes BEGIN
        // Try setting scanObject name correctly
        //scanObjectName = rs.indexName;
        GemFireContainer gfc = ((MemConglomerate)rs.scoci).getGemFireContainer();
        // If container is NULL, this is a constraint over a hash table, not a 
        // table-defined constraint
        if (gfc != null)
        {
          scanObjectName = gfc.getQualifiedTableName();
        }
        else
        {
          scanObjectName = "HASH SCAN:" + rs.tableName;
        }

        // Send back non-qualifier predicates used in this scan as
        // detail information
        // Qualifier preds are already sent back in scan_qualifiers
        if (rs.nonQualPreds != null)
        {
          rsxplaindetail = "WHERE : "+rs.nonQualPreds;
        }
        else
        {
          rsxplaindetail = null;
        }

        //Gemstone changes end
      }
      else {
        rsxplaintype = XPLAINUtil.OP_INDEXSCAN;
        scanObjectType = "I"; // index
        rsxplaindetail = "";
        // If this is a case-insensitive comparison, explain it
        // (Case sensitive is the norm - if needed, can print out
        // either state)
        if (!((MemIndex)rs.scoci).caseSensitive())
        {
          rsxplaindetail += "(Case Insensitive) ";
        }
        scanObjectName = rs.indexName;
        // Send back non-qualifier predicates used in this scan as
        // detail information
        // Qualifier preds are already sent back in scan_qualifiers
        if (rs.nonQualPreds != null)
        {
          rsxplaindetail += "WHERE : "+rs.nonQualPreds;
        }
      }

      /* Start and stop position strings will be non-null
       * if the TSRS has been closed.  Otherwise, we go off
       * and build the strings now.
       */
      startPosition = rs.startPositionString;
      if (startPosition == null) {
        startPosition = rs.printStartPosition();
      }
      stopPosition = rs.stopPositionString;
      if (stopPosition == null) {
        stopPosition = rs.printStopPosition();
      }

    }
    else {
      rsxplaintype = XPLAINUtil.OP_TABLESCAN;
      scanObjectType = "T"; // table
      rsxplaindetail = "T: " + rs.tableName;
      //Gemstone changes BEGIN
      // Add in schema name as well for EXPLAIN
      //scanObjectName = rs.tableName;
      scanObjectName = rs.regionName;
      //Gemstone changes END
    }

    final UUID scanID = dd.getUUIDFactory().createUUID();

    final XPLAINScanPropsDescriptor scan_desc = createScanPropDescriptor(
        scanObjectType,
        scanObjectName,
        startPosition,
        stopPosition,
        scanID,
        rs.isolationLevel,
        rs.rowsPerRead,
        rs.qualifiers,
        rs.getScanProperties(),
        null);

    createResultSetDescriptor(
        rs,
        time_desc,
        lockMode,
        lockGran,
        rsxplaintype,
        rsxplaindetail,
        scan_desc,
        null,
        -1);
  }

  @Override
  public void visit(
      ScalarAggregateResultSet rs,
      int overridable) {
    XPLAINResultSetTimingsDescriptor time_desc = null;

    if (considerTimingInformation) {
      UUID timingID = dd.getUUIDFactory().createUUID();
      time_desc = createResultSetTimingDescriptor(
          rs,
          timingID);
    }

    int count = rs.aggInfoList.size();

    //Gemstone changes BEGIN
    // Print out aggregate descriptions in OP_DETAILS
    // For later inclusion in EXPLAIN plans
    // This operator may be doing multiple aggregations
    String aggDescription = "";
    for (int i = 0; i < count; i++) {
      AggregatorInfo aggInfo = (AggregatorInfo)rs.aggInfoList.elementAt(i);
      if (i > 0)
      {
        aggDescription += ",";
      }
      if (aggInfo.isDistinct())
      {
        aggDescription += "DISTINCT ";
      }
      aggDescription += aggInfo.getAggregateName();
    }
    //Gemstone changes END
    
    XPLAINResultSetDescriptor rsdesc = createResultSetDescriptor(
        rs,
        time_desc,
        null,
        null,
        XPLAINUtil.OP_AGGREGATE,
        aggDescription,
        null,
        null, 
        1);    // Scalar Aggregate always returns 1 row

    rsdesc.setInputRows(rs.rowsInput);
    rsdesc.setIndexKeyOptimization(rs.singleInputRow ? "Y" : "N");
  }

  @Override
  public void visit(
      final JoinResultSet rs,
      int overridable) {

    XPLAINResultSetTimingsDescriptor time_desc = null;

    if (considerTimingInformation) {
      UUID timingID = dd.getUUIDFactory().createUUID();
      time_desc = createResultSetTimingDescriptor(
          rs,
          timingID);
      time_desc.setAvgNextTime(XPLAINUtil.getAVGNextTime(
          rs.nextTime,
          (rs.rowsSeenLeft + rs.rowsSeenRight)));
    }

    final String xplainType;
    final int emptyRightRowsReturned;
    switch (overridable) {
      case 1:
        xplainType = XPLAINUtil.OP_JOIN_NL;
        emptyRightRowsReturned = -1;
        break;
      case 2:
        xplainType = XPLAINUtil.OP_JOIN_NL_LO;
        emptyRightRowsReturned = ((NestedLoopLeftOuterJoinResultSet)rs).emptyRightRowsReturned;
        break;
      case 3:
        xplainType = XPLAINUtil.OP_JOIN_HASH;
        emptyRightRowsReturned = -1;
        break;
      case 4:
        xplainType = XPLAINUtil.OP_JOIN_HASH_LO;
        emptyRightRowsReturned = ((HashLeftOuterJoinResultSet)rs).emptyRightRowsReturned;
        break;
      case 5:
        xplainType = XPLAINUtil.OP_JOIN_MERGE;
        emptyRightRowsReturned = -1;
        break;
      default:
        xplainType = null;
        emptyRightRowsReturned = -1;
    }

    StringBuilder op_details = new StringBuilder();
    op_details.append(
            "(").append(
            rs.resultSetNumber).append(
            ")");
    if (rs.oneRowRightSide)
      op_details.append(", EXISTS JOIN");

    final XPLAINResultSetDescriptor rsdesc = createResultSetDescriptor(
        rs,
        time_desc,
        null,
        null,
        xplainType,
        op_details.toString(),
        null,
        null, 
        rs.rowsReturned);

    rsdesc.setRowsSeenRight(rs.rowsSeenRight);
    rsdesc.setEmptyRightRowsReturned(emptyRightRowsReturned);
  }

  @Override
  public void visit(
      HashScanResultSet rs,
      int overridable) {

    boolean instantaneousLocks = false;
    HashScanResultSet hsrs = rs;
    String startPosition = null;
    String stopPosition = null;
    String lockString = null;

    if (hsrs.forUpdate) {
      lockString = MessageService.getTextMessage(SQLState.LANG_EXCLUSIVE);
    }
    else {
      if (instantaneousLocks) {
        lockString = MessageService
            .getTextMessage(SQLState.LANG_INSTANTANEOUS_SHARE);
      }
      else {
        lockString = MessageService.getTextMessage(SQLState.LANG_SHARE);
      }
    }

    switch (hsrs.lockMode) {
      case TransactionController.MODE_TABLE:
        // RESOLVE: Not sure this will really work, as we
        // are tacking together English words to make a phrase.
        // Will this work in other languages?
        lockString = lockString + " "
            + MessageService.getTextMessage(SQLState.LANG_TABLE);
        break;

      case TransactionController.MODE_RECORD:
        // RESOLVE: Not sure this will really work, as we
        // are tacking together English words to make a phrase.
        // Will this work in other languages?
        lockString = lockString + " "
            + MessageService.getTextMessage(SQLState.LANG_ROW);
        break;
    }

    if (hsrs.indexName != null) {
      /* Start and stop position strings will be non-null
      * if the HSRS has been closed.  Otherwise, we go off
      * and build the strings now.
      */
      startPosition = hsrs.startPositionString;
      if (startPosition == null) {
        startPosition = hsrs.printStartPosition();
      }
      stopPosition = hsrs.stopPositionString;
      if (stopPosition == null) {
        stopPosition = hsrs.printStopPosition();
      }
    }

    XPLAINResultSetTimingsDescriptor time_desc = null;

    if (considerTimingInformation) {
      UUID timingID = dd.getUUIDFactory().createUUID();
      time_desc = createResultSetTimingDescriptor(
          rs,
          timingID);
    }

    final UUID scanID = dd.getUUIDFactory().createUUID();

    final String scanObjectType, scanObjectName;
    final String xplaindetail;

    if (rs.indexName != null) {
      if (rs.isConstraint) {
        scanObjectType = "C"; // constraint
        scanObjectName = rs.indexName;
        xplaindetail = "C: " + rs.indexName;
      }
      else {
        scanObjectType = "I"; // index
        scanObjectName = rs.indexName;
        xplaindetail = "I: " + rs.indexName;
      }
    }
    else {
      scanObjectType = "T"; // table
      scanObjectName = rs.tableName;
      xplaindetail = "T: " + rs.tableName;
    }

    final XPLAINScanPropsDescriptor scan_desc = createScanPropDescriptor(
        scanObjectType,
        scanObjectName,
        startPosition,
        stopPosition,
        scanID,
        rs.isolationLevel,
        -1,
        rs.scanQualifiers,
        rs.scanProperties,
        rs.keyColumns);

    scan_desc.setHashtableSize(rs.hashtableSize);

    final String lockMode = XPLAINUtil.getLockModeCode(lockString);
    final String lockGran = XPLAINUtil.getLockGranularityCode(lockString);

    createResultSetDescriptor(
        rs,
        time_desc,
        lockMode,
        lockGran,
        (overridable == 2 ? XPLAINUtil.OP_DISTINCTSCAN : XPLAINUtil.OP_HASHSCAN),
        xplaindetail,
        scan_desc,
        null,
        -1);
  }

}
