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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.TXStateInterface;
import com.gemstone.gemfire.internal.shared.Version;
import com.pivotal.gemfirexd.Attribute;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.distributed.message.GfxdFunctionMessage;
import com.pivotal.gemfirexd.internal.engine.distributed.message.PrepStatementExecutorMessage;
import com.pivotal.gemfirexd.internal.engine.distributed.message.StatementExecutorMessage;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.ParameterValueSet;
import com.pivotal.gemfirexd.internal.iapi.sql.StatementType;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedPreparedStatement;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedResultSet;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedStatement;
import com.pivotal.gemfirexd.internal.impl.sql.execute.xplain.XPLAINUtil;
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState;

/**
 * Base class for execution of Statements and PreparedStatements on remote
 * nodes. The main methods here are static to allow for usage by both
 * {@link Function}s and {@link GfxdFunctionMessage}s. The query node invokes
 * the execute method on the data node for execution of Statement queries. It
 * creates an instance of {@link ResultHolder} which is recieved on the query
 * node as a return value.
 * 
 * @author asif
 * @author swale
 */
public abstract class StatementQueryExecutor {

  protected StatementQueryExecutor() {
  }

  /**
   * Static method to execute a {@link Statement} on a data store node. This is
   * static to allow both {@code StatementQueryExecutorFunction} (old code no
   * longer used) and {@link StatementExecutorMessage} to use it.
   */
  public static <T> void executeStatement(final String defaultSchema,
      final long connId, final long stmtId, long rootID, int stmtLevel,
      final String source, final boolean isSelect, final int activationFlags,
      final boolean needGfxdSubActivation, final boolean enableStats,
      final boolean enableTimeStats, final FunctionContext fc,
      final String argsStr, final TXStateInterface tx,
      final ParameterValueSet constantValueSet, final boolean flattenSubquery,
      final boolean isInsertAsSubSelect, StatementExecutorMessage<T> msg)
      throws Exception {
    boolean compileOutOfDatePlan = true;
    while (compileOutOfDatePlan) {
      compileOutOfDatePlan = false;
      ResultHolder rh = null;
      boolean isPossibleDuplicate = false;
      if (fc instanceof RegionFunctionContext) {
        isPossibleDuplicate = ((RegionFunctionContext)fc).isPossibleDuplicate();
      }
      if (GemFireXDUtils.TraceQuery | GemFireXDUtils.TraceNCJ) {
        StringBuilder builder = new StringBuilder(
            "StatementQueryExecutor#executeStatement: Arguments=")
            .append(argsStr);
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_QUERYDISTRIB, builder
            .append("; possibleDuplicate=").append(isPossibleDuplicate)
            .toString());
      }

      // Get the connection from the holder
      Properties props = new Properties();
      props.setProperty(Attribute.QUERY_HDFS, Boolean.toString(msg.getQueryHDFS()));
      final GfxdConnectionWrapper wrapper = GfxdConnectionHolder
          .getOrCreateWrapper(defaultSchema, connId, false, props);
      final EmbedConnection conn = wrapper.getConnectionForSynchronization();
      // acquire the lock on connection so that subsequent requests on the same
      // connection get sequenced out
      synchronized (conn.getConnectionSynchronization()) {
        boolean needContextStackRestore = false;
        EmbedStatement stmt = null;
        EmbedResultSet ers = null;
        final int syncVersion = wrapper.convertToHardReference(conn);
        LanguageConnectionContext lcc = null;
        GemFireTransaction tc = null;
        int oldLCCFlags = 0;
        Activation act = null;
        try {
          lcc = wrapper.getLanguageConnectionContext();
          oldLCCFlags = lcc.getFlags();
          wrapper.setLccFlags(lcc, isPossibleDuplicate, enableStats,
              enableTimeStats, constantValueSet, msg);
          tc = (GemFireTransaction)lcc.getTransactionExecute();
          GfxdConnectionWrapper.checkForTransaction(conn, tc, tx);
          stmt = wrapper.getStatement(source, stmtId,
              false /* is prep stmnt */, needGfxdSubActivation,
              flattenSubquery, false /*allReplicated, doesnt matter*/,
              msg.getNCJMetaDataOnRemote(), false, rootID, stmtLevel);
          stmt.setQueryTimeout((int) (msg.getTimeOutMillis() / 1000)); //convert to seconds
          final GemFireXDQueryObserver observer = GemFireXDQueryObserverHolder
              .getInstance();
          if (observer != null) {
            observer.beforeQueryExecutionByStatementQueryExecutor(wrapper,
                stmt, source);
          }
          if (GemFireXDUtils.TraceQuery | GemFireXDUtils.TraceNCJ) {
            SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_QUERYDISTRIB,
                "StatementQueryExecutor#executeStatement: reached data node "
                    + "for query=" + source + " pvs=" + constantValueSet
                    + " isInsertAsSubSelect=" + isInsertAsSubSelect);
          }
          // keep FunctionContext and other execution flags in Activation
          // rather than LCC since the same connection may have two back
          // to back execute() calls with ResultSet consumption side-by-side
          // (e.g. see DistributedQueryDUnit.testDDLDML) that can switch
          // between the two execution contexts and hence the ones stored
          // in LCC may become incorrect
          if (isSelect && !isInsertAsSubSelect) {
            // context restore for the case of exceptions inside execute() will
            // be handled inside execute() itself
            ers = (EmbedResultSet)stmt.executeQueryByPassQueryInfo(source,
                needGfxdSubActivation, flattenSubquery, activationFlags, fc);
            act = ers.getSourceResultSet().getActivation();
            act.setConnectionID(connId);
            needContextStackRestore = true;
            // currently all activationFlags lead to rakInfo = true
            rh = new ResultHolder(ers, stmt, wrapper, oldLCCFlags, msg,
                (activationFlags & Activation.FLAGS_NEED_KEYS) != 0);
            
            if (msg.isLocallyExecuted()) {
              if (rh.setupResults(msg.getGfxdResultCollector(), act)) {
                // prevent cleanup of LCC etc for local execution that will
                // happen incrementally
                lcc = null;
                act = null;
              }
            }
            else {
              InternalDistributedMember m = msg.getSenderForReply();
              final Version v = m != null ? m.getVersionObject() : null;
              while (rh.prepareSend(v)) {
                msg.sendResult(rh);
              }
            }
            msg.lastResult(rh,
                  false /* TX flush will be done by ResultHolder */, 
                  false /*TXChanges send will be done byResultHolder*/,
                  false /* TX cleanup will be done by ResultHolder */);
            msg.lastResultSent = true;            
          }
          else {
            rh = null;
            int numUpdate = stmt.executeUpdateByPassQueryInfo(source,
                needGfxdSubActivation, flattenSubquery, activationFlags, fc);
            msg.lastResult(Integer.valueOf(numUpdate));
            msg.lastResultSent = true;
          }
          if (observer != null) {
            observer.afterQueryExecutionByStatementQueryExecutor(wrapper, stmt,
                source);
          }
          if (msg.process_time != 0) {
            msg.process_time = XPLAINUtil.recordTiming(msg.process_time);
          }

          if (lcc != null) {
            wrapper.process(conn, rh, stmt, msg);
          }
          if (rh == null) {
            stmt.close();
          }
        } catch (SQLException se) {
          if (!SQLState.LANG_STATEMENT_NEEDS_RECOMPILE_STATE.equals(se
              .getSQLState())) {
            throw se;
          }
          compileOutOfDatePlan = true;
          continue;
        } finally {
          if (rh != null && !msg.lastResultSent && GemFireXDUtils.isOffHeapEnabled() ) {
            if(msg.isLocallyExecuted()) {
              rh.freeOffHeapForCachedRowsAndCloseResultSet();
            }else {
              rh.close(null, null);
            }
          }
          // irrespective of the connection state, if there is an exception,
          // ensure to restore the context stack
          if (needContextStackRestore) {
            GfxdConnectionWrapper.restoreContextStack(stmt, ers);
          }
          // clear LCC flags and other data
          if (lcc != null) {
            if (rh != null) {
              rh.clear();
            }
            lcc.setFlags(oldLCCFlags);
            lcc.setPossibleDuplicate(false);
            lcc.setConstantValueSet(null, null);
          }
          if (act != null) {
            act.setFlags(0);
            act.setFunctionContext(null);
          }
          // reset cached TXState
          if (tc != null) {
            tc.resetTXState();
          }
          // wrapper can get closed for a non-cached Connection
          if (!wrapper.isClosed()) {
            if (stmt != null && !stmt.isPrepared()) {
              // un-prepared statement is added to map so that 
              // statement handle is available for cancellation
              // no need to keep the map post execution
              wrapper.removeStmtFromMap(stmtId);
            }
            if (wrapper.convertToSoftReference(syncVersion)) {
              msg.wrapperForMarkUnused = wrapper;
            }
          }
          else if (lcc != null && !wrapper.isCached()) {
            wrapper.close(false, false);
          }
          
          //#49353: set msg.wrapperForMarkUnused, even if wrapper is closed.
          msg.wrapperForMarkUnused = wrapper;
        }
      }
      // this should be *outside* of sync(conn) block and always the last thing
      // to be done related to connection set/reset
      msg.endMessage();
    }
  }

  /**
   * Static method to execute a {@link PreparedStatement} on a data store node.
   * This is static to allow both {@code PrepStatementQueryExecutorFunction}
   * (old implementation no longer used) and
   * {@link PrepStatementExecutorMessage} to use it.
   */
  public static <T, M> void executePrepStatement(final String userName,
      final long connId, final long stmtId, final String source,
      final int activationFlags, final boolean enableStats,
      final boolean enableTimeStats, final FunctionContext fc,
      final String argsStr, final TXStateInterface tx,
      final PrepStatementExecutorMessage<M> msg) throws Exception {
    ResultHolder rh = null;
    boolean compileOutOfDatePlan = true;
    while (compileOutOfDatePlan) {
      compileOutOfDatePlan = false;
      boolean isPossibleDuplicate = false;
      if (fc instanceof RegionFunctionContext) {
        isPossibleDuplicate = ((RegionFunctionContext)fc).isPossibleDuplicate();
      }
      if (GemFireXDUtils.TraceQuery | GemFireXDUtils.TraceNCJ) {
        StringBuilder builder = new StringBuilder(
            "StatementQueryExecutor#executePrepStatement: Arguments=")
            .append(argsStr);
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_QUERYDISTRIB, builder
            .append("; posDup = ").append(isPossibleDuplicate).toString());
      }
      ParameterValueSet pvs = null;
      final GfxdConnectionWrapper wrapper = msg.getAndClearWrapper(userName,
          connId);
      final EmbedConnection conn = wrapper.getConnectionForSynchronization();
      // acquire the lock on connection so that subsequent requests on the same
      // connection get sequenced out
      synchronized (conn.getConnectionSynchronization()) {
        boolean needContextStackRestore = false;
        EmbedPreparedStatement pstmt = null;
        EmbedResultSet ers = null;
        final int syncVersion = wrapper.convertToHardReference(conn);
        msg.wrapperForMarkUnused = null;
        LanguageConnectionContext lcc = null;
        Activation act = null;
        int oldLCCFlags = 0;
        GemFireTransaction tc = null;
        try {
          lcc = wrapper.getLanguageConnectionContext();
          oldLCCFlags = lcc.getFlags();
          wrapper.setLccFlags(lcc, isPossibleDuplicate, enableStats,
              enableTimeStats, msg);
          tc = (GemFireTransaction)lcc.getTransactionExecute();
          GfxdConnectionWrapper.checkForTransaction(conn, tc, tx);
          pstmt = msg.getAndClearPreparedStatement();
          pvs = pstmt.getParms();
          final GemFireXDQueryObserver observer = GemFireXDQueryObserverHolder
              .getInstance();
          if (observer != null) {
            observer.beforeQueryExecutionByPrepStatementQueryExecutor(wrapper,
                pstmt, source);
          }
          act = pstmt.getActivation();
          if (GemFireXDUtils.TraceQuery | GemFireXDUtils.TraceNCJ) {
            SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_QUERYDISTRIB,
                "StatementQueryExecutor#executePrepStatement: Member ID of self="
                    + GemFireStore.getMyId() + " connID=" + connId
                    + " statementID=" + stmtId + " query string = " + source
                    + "; pvs=" + pvs + "; posDup=" + isPossibleDuplicate
                    + " activation: " + act);
          }

          act.setFlags(activationFlags);
          act.setFunctionContext(fc);
          act.setConnectionID(connId);
          pstmt.setQueryTimeout((int) (msg.getTimeOutMillis() / 1000)); // convert to seconds
          final int statementType = pstmt.getStatementType();
          // UNKNOWN type is SELECT as per QueryTreeNode.getStatementType()
          if (statementType == StatementType.UNKNOWN) {
            ers = (EmbedResultSet)pstmt.executeQueryByPassQueryInfo();
            needContextStackRestore = true;
            rh = new ResultHolder(ers, pstmt, wrapper, oldLCCFlags, msg,
                (activationFlags & Activation.FLAGS_NEED_KEYS) != 0);
           
            if (msg.isLocallyExecuted()) {
              if (rh.setupResults(msg.getGfxdResultCollector(), act)) {
                // prevent cleanup of LCC etc for local execution that will
                // happen incrementally
                lcc = null;
                act = null;
              }
            }
            else {
              InternalDistributedMember m = msg.getSenderForReply();
              final Version v = m != null ? m.getVersionObject() : null;
              while (rh.prepareSend(v)) {
                msg.sendResult(rh);
              }
            }
            msg.lastResult(rh,
                false /* TX flush will be done by ResultHolder */, 
                false /* TXChanges send will be done by ResultHolder*/,
                false /* TX cleanup will be done by ResultHolder */);
            msg.lastResultSent = true;
            
          }
          else if (statementType == StatementType.UPDATE
              || statementType == StatementType.INSERT
              || statementType == StatementType.DELETE) {
            rh = null;
            ers = null;
            int numUpdate = pstmt.executeUpdate();
            msg.lastResult(Integer.valueOf(numUpdate));
            msg.lastResultSent = true;
          }
          else {
            throw new IllegalArgumentException("The statement type = "
                + statementType + " not supported");
          }
          if (observer != null) {
            observer.afterQueryExecutionByPrepStatementQueryExecutor(wrapper,
                pstmt, source);
          }
          if (msg.process_time != 0) {
            msg.process_time = XPLAINUtil.recordTiming(msg.process_time);
          }
          if (lcc != null) {
            wrapper.process(conn, rh, pstmt, msg);
          }
        } catch (SQLException se) {          
          if (!SQLState.LANG_STATEMENT_NEEDS_RECOMPILE_STATE.equals(se
              .getSQLState())) {
            throw se;
          }
          compileOutOfDatePlan = true;
          continue;
        } finally {
          if (!msg.lastResultSent && rh != null && GemFireXDUtils.isOffHeapEnabled() ) {
            if(msg.isLocallyExecuted()) {
              rh.freeOffHeapForCachedRowsAndCloseResultSet();
            }else {
              rh.close(null, null);
            }
          } 
          // irrespective of the connection state, if there is an exception,
          // ensure to restore the context stack.
          if (needContextStackRestore) {
            GfxdConnectionWrapper.restoreContextStack(pstmt, ers);
          }
          if (lcc != null) {
            lcc.setFlags(oldLCCFlags);
            lcc.setPossibleDuplicate(false);
          }
          if (act != null) {
            act.setFlags(0);
            act.setFunctionContext(null);
          }
          // reset cached TXState
          if (tc != null) {
            tc.resetTXState();
          }
          // wrapper can get closed for a non-cached Connection
          if (!wrapper.isClosed()) {
            if (wrapper.convertToSoftReference(syncVersion)) {
              msg.wrapperForMarkUnused = wrapper;
            }
          }
          else if (lcc != null && !wrapper.isCached()) {
            try {
              if (pstmt != null && !pstmt.isClosed()) {
                pstmt.close();
              }
            } catch (SQLException sqle) {
              // ignore any exception in statement close
            }
            wrapper.close(false, false);
          }

          //#49353: set msg.wrapperForMarkUnused, even if wrapper is closed.
          msg.wrapperForMarkUnused = wrapper;
        }
      }
      // this should be *outside* of sync(conn) block and always the last thing
      // to be done related to connection set/reset
      msg.endMessage();
    }
  }

  public static GfxdConnectionWrapper getOrCreateWrapper(String defaultSchema,
      long connId, Properties props) throws SQLException {
    return GfxdConnectionHolder.getHolder().createWrapper(defaultSchema,
        connId, false, props);
  }
}
