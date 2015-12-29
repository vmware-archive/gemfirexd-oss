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
package com.pivotal.gemfirexd.internal.engine.ddl.wan;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueImpl;
import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.wan.GatewayQueueEvent;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.InternalDeltaEvent;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.GatewayEventEnqueueFilter;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventCallbackArgument;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.ddl.GfxdDDLRegionQueue;
import com.pivotal.gemfirexd.internal.engine.ddl.wan.messages.GfxdCBArgForSynchPrms;
import com.pivotal.gemfirexd.internal.engine.ddl.wan.messages.GfxdGatewaySenderStartMessage;
import com.pivotal.gemfirexd.internal.engine.ddl.wan.messages.GfxdGatewaySenderStopMessage;
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdCallbackArgument;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.ServerGroupUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.reference.SQLState;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecIndexRow;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.store.access.TransactionController;
import com.pivotal.gemfirexd.internal.iapi.types.SQLBoolean;
import com.pivotal.gemfirexd.internal.iapi.types.SQLChar;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.DataDictionaryImpl;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.GfxdSysAsyncEventListenerRowFactory;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.GfxdSysGatewaySenderRowFactory;
import com.pivotal.gemfirexd.internal.impl.sql.catalog.TabInfoImpl;
import com.pivotal.gemfirexd.internal.jdbc.InternalDriver;

public class WanProcedures {

  public static void stopGatewaySender(String id) throws StandardException {
    // NULL id is illegal
    if (id == null) {
      throw StandardException.newException(SQLState.ENTITY_NAME_MISSING);
    }
    try {
      stopGatewaySenderLocally(id);
      GfxdGatewaySenderStopMessage msg = new GfxdGatewaySenderStopMessage(id,
          false);
      // Add the GfxdGatewaySenderStopMessage message to the DDLQueue
      final GfxdDDLRegionQueue queue = Misc.getMemStore().getDDLStmtQueue();
      final long replayKey = queue.newUUID();
      msg.setReplayKey(replayKey);
      queue.put(Long.valueOf(replayKey), msg);
      // Distribute the message
      msg.send(Misc.getGemFireCache().getDistributedSystem(), null);
    } catch (StandardException ex) {
      throw ex;
    } catch (Exception ex) {
      throw StandardException.newException(
          SQLState.UNEXPECTED_EXCEPTION_FOR_ASYNC_LISTENER, ex, id,
          ex.toString());
    }
  }

  public static void stopGatewaySenderLocally(String id) throws Exception {
    GatewaySender sender = Misc.getGemFireCache().getGatewaySender(
        id.toUpperCase());

    if (sender != null) {
      sender.stop();
      updateStartStatusToSysTable(sender.getId(), false, false);
    }
    else {
      if (ServerGroupUtils.isDataStore()) {
        LogWriterI18n logger = Misc.getI18NLogWriter();
        if (logger.warningEnabled()) {
          logger.convertToLogWriter().warning(
              "SYS.STOP_GATEWAY_SENDER: GatewaySender " + id
                  + " is not present on this member");
        }

        LanguageConnectionContext lcc = Misc.getLanguageConnectionContext();
        EmbedConnection conn = null;
        Connection c;
        boolean contextSet = false;
        try {
          if (lcc != null) {
            InternalDriver driver = InternalDriver.activeDriver();
            c = driver.connect("jdbc:default:connection", null);
          }
          else {
            conn = GemFireXDUtils.getTSSConnection(true, true, false);
            conn.getTR().setupContextStack();
            contextSet = true;
            c = conn;
          }
          PreparedStatement ps = c
              .prepareStatement("select * from SYS.GATEWAYSENDERS where sender_id = ?");
          ps.setString(1, id.toUpperCase());
          ResultSet rs = ps.executeQuery();
          if (!rs.next()) {
            throw StandardException.newException(
                SQLState.NO_GATEWAYSENDER_ASYNCEVENTLISTENER_FOUND, id);
          }
        } finally {
          if (contextSet) {
            try {
              conn.internalCommit();
            } catch (SQLException ex) {
              SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_EXECUTION,
                  "WanProcedures#stopGatewaySenderLocally: "
                      + "exception encountered in commit", ex);
            }
            conn.getTR().restoreContextStack();
          }
        }
      }
    }
  }

  public static void stopAsyncQueue(String id) throws StandardException {
    // NULL id is illegal
    if (id == null) {
      throw StandardException.newException(SQLState.ENTITY_NAME_MISSING);
    }
    try {
      stopAsyncQueueLocally(id);
      GfxdGatewaySenderStopMessage msg = new GfxdGatewaySenderStopMessage(id,
          true);

      // Add the GfxdGatewaySenderStopMessage message to the DDLQueue
      final GfxdDDLRegionQueue queue = Misc.getMemStore().getDDLStmtQueue();
      final long replayKey = queue.newUUID();
      msg.setReplayKey(replayKey);
      queue.put(Long.valueOf(replayKey), msg);
      // Distribute the message
      msg.send(Misc.getGemFireCache().getDistributedSystem(), null);
    } catch(StandardException ex){
      throw ex;
    } catch (Exception ex) {
      throw StandardException.newException(
          SQLState.UNEXPECTED_EXCEPTION_FOR_ASYNC_LISTENER, ex, id,
          ex.toString());
    }
  }

  public static void stopAsyncQueueLocally(String id) throws Exception {
    AsyncEventQueue asyncQueue = Misc.getGemFireCache().getAsyncEventQueue(
        id.toUpperCase());
    if (asyncQueue != null) {
      asyncQueue.stop();
      updateStartStatusToSysTable(asyncQueue.getId(), true, false);
    }
    else {
      if (ServerGroupUtils.isDataStore()) {
        LogWriterI18n logger = Misc.getI18NLogWriter();
        if (logger.warningEnabled()) {
          logger.convertToLogWriter().warning(
              "SYS.STOP_ASYNCEVENTLISTENER : AsyncEventListener " + id
                  + " is not present on this member");
        }
//        InternalDriver driver = InternalDriver.activeDriver();
//        Connection c = driver.connect("jdbc:default:connection", null);
////        Connection c = DriverManager
////            .getConnection("jdbc:default:connection");
//        PreparedStatement ps = c
//            .prepareStatement("select * from SYS.ASYNCEVENTLISTENERS where id = ?");
//        ps.setString(1, id.toUpperCase());
//        ResultSet rs = ps.executeQuery();
//        if (!rs.next()) {
//          throw StandardException.newException(
//              SQLState.NO_GATEWAYSENDER_ASYNCEVENTLISTENER_FOUND, id);
//        }
      }
    }
  }

  public static void startGatewaySender(String id) throws StandardException {
    // NULL id is illegal
    if (id == null) {
      throw StandardException.newException(SQLState.ENTITY_NAME_MISSING);
    }
    try {
      startGatewaySenderLocally(id);
      GfxdGatewaySenderStartMessage senderStart = new GfxdGatewaySenderStartMessage(
          id, false);

      // Add the GatewaySenderStart message to the DDLQueue
      final GfxdDDLRegionQueue queue = Misc.getMemStore().getDDLStmtQueue();
      final long replayKey = queue.newUUID();
      senderStart.setReplayKey(replayKey);
      queue.put(Long.valueOf(replayKey), senderStart);

      // Distribute the message
      senderStart.send(Misc.getGemFireCache().getDistributedSystem(), null);
    } catch(StandardException ex){
      throw ex;
    } catch (Exception ex) {
      throw StandardException.newException(
          SQLState.UNEXPECTED_EXCEPTION_FOR_ASYNC_LISTENER, ex, id,
          ex.toString());
    }
  }

  public static void startGatewaySenderLocally(String id) throws Exception {
    AbstractGatewaySender sender = (AbstractGatewaySender)Misc
        .getGemFireCache().getGatewaySender(id.toUpperCase());
    if (sender != null) {
      CancelCriterion cc = sender.getCancelCriterion();
      String str = cc.cancelInProgress();
      if (str == null) {
        try {
          sender.start();
          updateStartStatusToSysTable(sender.getId(), false, true);
        } catch (Exception ex) {
          throw StandardException.newException(
              SQLState.UNEXPECTED_EXCEPTION_FOR_ASYNC_LISTENER, ex, id,
              ex.toString());
        }
      }
      else {
        // Cancellation in progress. Do not start Hub. Just log the warning.
        LogWriter logger = Misc.getCacheLogWriter();
        if (logger.warningEnabled()) {
          logger.warning(str);
        }
        // check for cache close
        Misc.getGemFireCache().getCancelCriterion().checkCancelInProgress(null);
      }
    }
    else {
      if (ServerGroupUtils.isDataStore()) {
        LogWriterI18n logger = Misc.getI18NLogWriter();
        if (logger.warningEnabled()) {
          logger.convertToLogWriter().warning(
              "SYS.START_GATEWAY_SENDER : GatewaySender " + id
                  + " is not present on this member");
//          InternalDriver driver = InternalDriver.activeDriver();
//          Connection c = driver.connect("jdbc:default:connection", null);
////          Connection c = DriverManager
////              .getConnection("jdbc:default:connection");
//          PreparedStatement ps = c
//              .prepareStatement("select * from SYS.GATEWAYSENDERS where sender_id = ?");
//          ps.setString(1, id.toUpperCase());
//          ResultSet rs = ps.executeQuery();
//          if (!rs.next()) {
//            throw StandardException.newException(
//                SQLState.NO_GATEWAYSENDER_ASYNCEVENTLISTENER_FOUND, id);
//          }
        }
      }
    }
  }

  public static void startAsyncQueue(String id) throws StandardException {
    // NULL id is illegal
    if (id == null) {
      throw StandardException.newException(SQLState.ENTITY_NAME_MISSING);
    }
    try {
      startAsyncQueueLocally(id);
      GfxdGatewaySenderStartMessage senderStart = new GfxdGatewaySenderStartMessage(
          id, true);
      // Add the GatewayHubAddition message to the DDLQueue
      final GfxdDDLRegionQueue queue = Misc.getMemStore().getDDLStmtQueue();
      final long replayKey = queue.newUUID();
      senderStart.setReplayKey(replayKey);
      queue.put(Long.valueOf(replayKey), senderStart);

      // Distribute the message
      senderStart.send(Misc.getGemFireCache().getDistributedSystem(), null);
    } catch(StandardException ex){
      String asyncListenerRunningWarning = SQLState.LANG_ASYNCEVENTLISTENER_ALREADY_RUNNING;
      //exception can come from local node or remote node. In case it is from remote node,
      //need to verify the cause of it
      if (ex.getSQLState().equals(asyncListenerRunningWarning)
          || (ex.getCause() instanceof StandardException && ((StandardException)ex
              .getCause()).getSQLState().equals(asyncListenerRunningWarning))) {
        LanguageConnectionContext lcc = Misc.getLanguageConnectionContext();
        if (lcc != null) {
          lcc.getLastActivation().addWarning(
              StandardException.newWarning(
                  SQLState.LANG_ASYNCEVENTLISTENER_ALREADY_RUNNING, id));
        }
      }
      else {
        throw ex;
      }
    } catch (Exception ex) {
      throw StandardException.newException(
          SQLState.UNEXPECTED_EXCEPTION_FOR_ASYNC_LISTENER, ex, id,
          ex.toString());
    }
  }

  public static void startAsyncQueueLocally(String id) throws Exception {
    AsyncEventQueueImpl asyncQueue = (AsyncEventQueueImpl)Misc
        .getGemFireCache().getAsyncEventQueue(id.toUpperCase());
    if (asyncQueue != null) {
      CancelCriterion cc = asyncQueue.getSender().getCancelCriterion();
      String str = cc.cancelInProgress();
      if (str == null) {
        if (asyncQueue.isRunning()) {
          throw StandardException.newException(
              SQLState.LANG_ASYNCEVENTLISTENER_ALREADY_RUNNING, id);
        }
        try {
          final AsyncEventListener gfListener = asyncQueue
              .getAsyncEventListener();
          if (!(gfListener instanceof GfxdGatewayEventListener)) {
            Assert.fail("UnExpected Listener type " + gfListener);
          }
          GfxdGatewayEventListener listener = (GfxdGatewayEventListener)gfListener;
          listener.reInit();
          asyncQueue.start();
          listener.start();
          updateStartStatusToSysTable(asyncQueue.getId(), true, true);
        } catch (Exception ex) {
          throw StandardException.newException(
              SQLState.UNEXPECTED_EXCEPTION_FOR_ASYNC_LISTENER, ex, id,
              ex.toString());
        }
      }
      else {
        // Cancellation in progress. Do not start gateway. Just log the warning.
        LogWriter logger = Misc.getCacheLogWriter();
        if (logger.warningEnabled()) {
          logger.warning(str);
        }
        // check for cache close
        Misc.getGemFireCache().getCancelCriterion().checkCancelInProgress(null);
      }
    }
    else {
      if (ServerGroupUtils.isDataStore()) {
        LogWriterI18n logger = Misc.getI18NLogWriter();
        if (logger.warningEnabled()) {
          logger.convertToLogWriter().warning(
              "SYS.START_ASYNCEVENTLISTENER : AsyncEventListener " + id
                  + " is not present on this member");
////          Connection c = DriverManager
////              .getConnection("jdbc:default:connection");
//          InternalDriver driver = InternalDriver.activeDriver();
//          Connection c = driver.connect("jdbc:default:connection", null);
//          PreparedStatement ps = c
//              .prepareStatement("select * from SYS.ASYNCEVENTLISTENERS where id = ?");
//          ps.setString(1, id.toUpperCase());
//          ResultSet rs = ps.executeQuery();
//          if (!rs.next()) {
//            throw StandardException.newException(
//                SQLState.NO_GATEWAYSENDER_ASYNCEVENTLISTENER_FOUND, id);
//          }
        }
      }
    }
  }

  private static void updateStartStatusToSysTable(String id,
      boolean isAsyncEventQueue, boolean isStart) throws StandardException {
    LanguageConnectionContext lcc = Misc.getLanguageConnectionContext();
    EmbedConnection conn = null;
    boolean contextSet = false;
    try {
      if (lcc == null) {
        conn = GemFireXDUtils.getTSSConnection(true, true, false);
        lcc = conn.getLanguageConnectionContext();
        conn.getTR().setupContextStack();
        contextSet = true;
      }
      DataDictionaryImpl dd = (DataDictionaryImpl)lcc.getDataDictionary();
      TransactionController tc = lcc.getTransactionExecute();
      ExecIndexRow keyRow = dd.getExecutionFactory().getIndexableRow(1);
      keyRow.setColumn(1, new SQLChar(id));
      TabInfoImpl ti = null;
      if (isAsyncEventQueue) {
        ti = dd
            .getNonCoreTI(DataDictionaryImpl.ASYNCEVENTLISTENERS_CATALOG_NUM);
        GfxdSysAsyncEventListenerRowFactory rf = (GfxdSysAsyncEventListenerRowFactory)ti
            .getCatalogRowFactory();
        ExecRow row = rf.makeEmptyRow();
        row.setColumn(GfxdSysAsyncEventListenerRowFactory.IS_STARTED,
            new SQLBoolean(isStart));
        boolean[] bArray = { false };
        int[] colsToUpdate = { GfxdSysAsyncEventListenerRowFactory.IS_STARTED };
        ti.updateRow(keyRow, row, 0, bArray, colsToUpdate, tc);
      }
      else {
        ti = dd.getNonCoreTI(DataDictionaryImpl.GATEWAYSENDERS_CATALOG_NUM);
        GfxdSysGatewaySenderRowFactory rf = (GfxdSysGatewaySenderRowFactory)ti
            .getCatalogRowFactory();
        ExecRow row = rf.makeEmptyRow();
        row.setColumn(GfxdSysGatewaySenderRowFactory.IS_STARTED,
            new SQLBoolean(isStart));
        boolean[] bArray = { false };
        int[] colsToUpdate = { GfxdSysGatewaySenderRowFactory.IS_STARTED };
        ti.updateRow(keyRow, row, 0, bArray, colsToUpdate, tc);
      }
    } finally {
      if (contextSet) {
        conn.getTR().restoreContextStack();
      }
    }
    LogWriterI18n logger = Misc.getI18NLogWriter();
    if (logger.infoEnabled()) {
      logger.convertToLogWriter().info(
          "WanProcedures:: updated AsyncEventListener/GatewaySender " + id
              + " isAsyncEventListener=" + isAsyncEventQueue + " start="
              + isStart + " status in SYS table");
    }
  }

  public static void dummy() {
  }

  /**
   * Basic filter for listeners other than DBSynchronizer
   */
  public static class AsyncEventFilter extends GatewayEventEnqueueFilter {

    private static final GatewayEventFilter filter = new AsyncEventFilter();

    @Override
    public boolean beforeEnqueue(
        @SuppressWarnings("rawtypes") GatewayQueueEvent event) {
      if (GemFireXDUtils.TraceDBSynchronizer) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
            "WanProcedures::AsyncEventFilter:enqueueEvent: "
                + "Event received for enqueuing: " + event);
      }
      final GemFireContainer gc = (GemFireContainer)event.getRegion()
          .getUserAttribute();
      if (gc.isTemporaryContainer() || gc.isSYSTABLES()) {
        if (GemFireXDUtils.TraceDBSynchronizer) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
              "WanProcedures::AsyncEventFilter:enqueueEvent: "
                  + "Returning false for temporary/SYS tables");
        }
        return false;
      }
      else if (event.getOperation() == Operation.BULK_DML_OP) {
        // only allow transactional BULK_DML operations to go through
        // while the rest come as PK events to AsyncEventListeners
        final Object cbArg = event.getCallbackArgument();
        final boolean isTX = cbArg != null
            && cbArg instanceof GfxdCBArgForSynchPrms
            && ((GfxdCBArgForSynchPrms)cbArg).isTransactional();
        if (GemFireXDUtils.TraceDBSynchronizer) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
              "WanProcedures::AsyncEventFilter:enqueueEvent: "
                  + "Returning " + isTX + " for BULK_DML_OP isTX=" + isTX);
        }
        return isTX;
      }
      else {
        boolean val = enqueuePart2(event);
        if (GemFireXDUtils.TraceDBSynchronizer) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
              "WanProcedures::AsyncEventFilter:enqueueEvent: Returning enqueuePart2 " + val);
        }
        return val;
      }
    }

    boolean enqueuePart2(GatewayQueueEvent<?, ?> event) {
      final Object callbackArg = event.getCallbackArgument();
      if (callbackArg != null && callbackArg instanceof GfxdCallbackArgument) {
        return !((GfxdCallbackArgument)callbackArg).isSkipListeners();
      }
      return true;
    }
  }

  
  /**
   * Filter for Serial DBSynchronizer.  Parallel DBSynchronizer is not supported.
   */
  public static final class SerialDBSynchronizerFilter extends GatewayEventEnqueueFilter {

    private static final GatewayEventFilter filter = new SerialDBSynchronizerFilter();
    
    public static void logMessage(String message) {
      if (GemFireXDUtils.TraceDBSynchronizer) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER, 
            "WanProcedures::SerialDBSynchronizerFilter:enqueueEvent: "+ message);
      }
    }

    private static boolean isEventFromRemoteSite(GatewayQueueEvent event) {
      if (event instanceof EntryEventImpl) {
        Object rawCallbackArg = ((EntryEventImpl)event).getRawCallbackArgument();
        int origDSId = -1;
        int thisDSId = -1; 
        if (rawCallbackArg != null && rawCallbackArg instanceof GatewaySenderEventCallbackArgument) {
          origDSId = ((GatewaySenderEventCallbackArgument)rawCallbackArg).getOriginatingDSId();
          thisDSId = InternalDistributedSystem.getAnyInstance().getDistributionManager().getDistributedSystemId();
          // Always apply events coming from other distributed systems.
          if (origDSId >=0 && thisDSId >=0 && origDSId != thisDSId) {
            logMessage("DSID[" + origDSId + "]. My DSID is ["+thisDSId+"]");
            return true;
          }
        }
      }
      return false;
    }
      
    @Override
    public boolean beforeEnqueue(GatewayQueueEvent event) {
      // [sjigyasu] The following if statements could have been more compact but
      // we want to log details of the conditions before returning.
      logMessage("beforeEnqueue");
      final GemFireContainer container = (GemFireContainer)event.getRegion().getUserAttribute();
      Object cbArg = event.getCallbackArgument();
      GfxdCallbackArgument cb = null;
      if (cbArg != null && cbArg instanceof GfxdCallbackArgument) {
        cb = (GfxdCallbackArgument)cbArg;
      }
      
      if (cb != null && cb.isSkipListeners()) {
        logMessage("Return false because skipListeners is true");
        return false;
      } 

      // Don't enqueue cache loaded events
      if (cb != null && cb.isCacheLoaded()) {
        logMessage("Returning false for cache loaded event");
        return false;
      }

      // Dont enqueue events on temp tables or system tables
      if (container.isTemporaryContainer() || container.isSYSTABLES()) {
        logMessage("Returning false for temporary/SYS tables");
        return false;
      }

      // Always enqueue events from other WAN site
      if (isEventFromRemoteSite(event)) {
        logMessage ("Returning true for event from other WAN site");
        return true;
      }

      // Always enqueue bulk DML events except when they are inserts on tables with autogenerated columns
      Operation op = event.getOperation();
      boolean isInsert = ((InternalDeltaEvent)event).isGFXDCreate(false);
      boolean autogen = container.getExtraTableInfo().hasAutoGeneratedColumns();
      logMessage("op=" + op + ",isInsert=" + isInsert + ",autogen="+autogen);
      
      if(autogen) {
        if (op == Operation.BULK_DML_OP) {
          logMessage ("Returning false for bulk DML op because table has autogen columns");
          return false;
        } else {
          logMessage ("Returning true for non-bulk DML op because table has autogen columns");
          return true;
        }
      }
      
      if (op == Operation.BULK_DML_OP) {
        logMessage("Returning true for bulk DML op");
        return true;
      } 

      // Always enqueue PK based events
      if (cb.isPkBased()) {
        logMessage("Returning true for PK based op");
        return true;
      }
      
      logMessage("Returning false for all other cases");
      return false;
    }
    
  }
  /**
   * Filter for Serial WAN
   */
  public static class SerialWanFilter extends GatewayEventEnqueueFilter {

    private static final GatewayEventFilter filter = new SerialWanFilter();
    public static void logMessage(String message) {
      if (GemFireXDUtils.TraceDBSynchronizer) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER, 
            "WanProcedures::SerialWanFilter:enqueueEvent: "+ message);
      }
    }
    @Override
    public boolean beforeEnqueue(GatewayQueueEvent event) {
      logMessage("beforeEnqueue");
      final GemFireContainer container = (GemFireContainer)event.getRegion().getUserAttribute();
      Object cbArg = event.getCallbackArgument();
      GfxdCallbackArgument cb = null;
      if (cbArg != null && cbArg instanceof GfxdCallbackArgument) {
        cb = (GfxdCallbackArgument)cbArg;
      }
      Operation op = event.getOperation();
      
      // [sjigyasu] The following if statements could have been more compact but
      // we want to log details of the conditions before returning.

      if (op == Operation.BULK_DML_OP) {
        logMessage("Returning false for bulk DML op since in WAN only events are sent across.");
        return false;
      }
      
      // Don't enqueue cache loaded events
      if (cb != null && cb.isCacheLoaded()) {
        logMessage("Returning false for cache loaded event");
        return false;
      }

      // Dont enqueue events on temp tables or system tables
      if (container.isTemporaryContainer() || container.isSYSTABLES()) {
        logMessage("Returning false for temporary/SYS tables");
        return false;
      }
      /*
      // Always enqueue events from other WAN site -- needs to be tested
      if (isEventFromRemoteSite(event)) {
        logMessage ("Returning true for event from other WAN site");
        return true;
      }*/
      return true;
    }
  }

  /**
   * Filter for Parallel WAN. 
   */
  public static class ParallelWanFilter extends AsyncEventFilter {

    private static final GatewayEventFilter filter = new ParallelWanFilter();

    @Override
    public boolean beforeEnqueue(
        @SuppressWarnings("rawtypes") GatewayQueueEvent event) {
      Operation op = event.getOperation();
      if (op == Operation.BULK_DML_OP) {
        // We should not allow BULK DMLs at all in case of Parallel.
        // In fact the code should not reach here for the case of Parallel.
        throw new GemFireXDRuntimeException("For ParallelWAN, BULKDML ops should not have come to the filter.");
      }
      
      // In all cases return true.
      // We may not need the following code and return whatever
      // super.beforeEnqueue returns
      if (super.beforeEnqueue(event)) {
        Object cb = event.getCallbackArgument();
        if (cb instanceof GfxdCallbackArgument) {
          final GfxdCallbackArgument ecb = (GfxdCallbackArgument)cb;
          // It is PK based alright, but if it is transactional or
          // obtained via cache loading , we do not want it to go to
          // db synchronizer .For tx not being added
          // as tx statements will take bulk op route
          // In Cheetah, in case of tx we are sending the event as it is
          // and not as a DML string.
          if (ecb.isTransactional()) {
            if (GemFireXDUtils.TraceDBSynchronizer) {
              SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
                  "WanProcedures::ParallelWanFilter:enqueueEvent: Returning "
                      + "true as even when it is PK based, because it is "
                      + "transactional or inserted via cache loading "
                      + "during get");
            }
            return true;
          }
          else if (ecb.isPkBased()) {
            final GemFireContainer gfc = (GemFireContainer)event.getRegion()
                .getUserAttribute();
            // don't enqueue if there are auto-generated columns
            if (!((InternalDeltaEvent)event).isGFXDCreate(false)
                || !gfc.getExtraTableInfo().hasAutoGeneratedColumns()) {
              if (GemFireXDUtils.TraceDBSynchronizer) {
                SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
                    "WanProcedures::ParallelWanFilter:enqueueEvent: "
                        + "Returning true for PK based event");
              }
              return true;
            }
            else {
              // auto-generated columns will take the bulk DML route
              // instead of sending the dml we should send the event.
              if (GemFireXDUtils.TraceDBSynchronizer) {
                SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
                    "WanProcedures::ParallelWanFilter:enqueueEvent: Returning true "
                        + "for PK based event due to auto-generated columns: "
                        + Arrays.toString(gfc.getExtraTableInfo()
                            .getAutoGeneratedColumns()));
              }
              // let auto-generated columns also go
              return true;
            }
          }
          else {
            if (GemFireXDUtils.TraceDBSynchronizer) {
              SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
                  "WanProcedures::GfxdParallelWanFilter:enqueueEvent: "
                      + "Returning true");
            }
            return true;
          }
        }
        else if (cb == null || cb instanceof Integer) {
          if (GemFireXDUtils.TraceDBSynchronizer) {
            SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
                "WanProcedures::ParallelWanFilter:enqueueEvent: Returning "
                    + "false as it is not a PK based event nor bulk DML");
          }
          // This block is added just to trap unexpected code path, if any.
          // it can be removed later.
          return false;
        }
        else {
          throw new InternalGemFireError("Unexpected callback argument type."
              + " callbackArg=" + cb + "; type=" + cb.getClass().getName());
        }
      }
      return false;
    }

    @Override
    boolean enqueuePart2(GatewayQueueEvent<?, ?> event) {
      return true;
    }
  }
  
  public static final class BULKDMLOptimizedDBSynchronizerFilter extends GfxdWanBULKDMLOptimizedFilter {

    private static final GatewayEventFilter filter = new BULKDMLOptimizedDBSynchronizerFilter();

    @Override
    public boolean beforeEnqueue(
        @SuppressWarnings("rawtypes") GatewayQueueEvent event) {
      
      if (GemFireXDUtils.TraceDBSynchronizer) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
            "WanProcedures::BULKDMLOptimizedDBSynchronizerFilter:enqueueEvent: Into the beforeEnqueue");
      }
      
      if (event instanceof EntryEventImpl) {
        Object rawCallbackArg = ((EntryEventImpl)event).getRawCallbackArgument();
        if (rawCallbackArg instanceof GatewaySenderEventCallbackArgument) {
          int origDSId = ((GatewaySenderEventCallbackArgument)rawCallbackArg).getOriginatingDSId();
          int thisDSId = Misc.getDistributedSystem().getDM().getDistributedSystemId();
          // Always apply events coming from other distributed systems.
          if (origDSId >=0 && thisDSId >=0 && origDSId != thisDSId) {
            if (GemFireXDUtils.TraceDBSynchronizer) {
              SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
                  "WanProcedures::BULKDMLOptimizedDBSynchronizerFilter:enqueueEvent: "
                      + "Returning true for event from other WAN site with DSID[" + origDSId + "]. My DSID is ["+thisDSId+"]");
            }
            return true;
          } 
        }
      }
      
      if (super.beforeEnqueue(event)) {
        Object cb = event.getCallbackArgument();
        if (cb instanceof GfxdCallbackArgument
            && ((GfxdCallbackArgument)cb).isCacheLoaded()) {
          // It is PK based alright, but if it is transactional or
          // obtained via cache loading , we do not want it to go to
          // db synchronizer .For tx not being added
          // as tx statements will take bulk op route

          if (GemFireXDUtils.TraceDBSynchronizer) {
            SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
                "WanProcedures::BULKDMLOptimizedDBSynchronizerFilter:enqueueEvent: "
                    + "Returning false as even when it is PK based, "
                    + "it is inserted via cache loading during get");
          }
          return false;
        }
        else {
          return true;
        }
      }
      return false;
    }

    @Override
    boolean enqueuePart2(GatewayQueueEvent<?, ?> event) {
      Object callbackArg = event.getCallbackArgument();
      if (callbackArg != null && callbackArg instanceof GfxdCallbackArgument) {
        return !((GfxdCallbackArgument)callbackArg).isSkipListeners();
      }
      return true;
    }
  }
  
  /**
   * UNUSED
   */
  /*
  public static final class DBSynchronizerFilter extends GfxdWanFilter {

    private static final GatewayEventFilter filter = new DBSynchronizerFilter();

    @Override
    public boolean beforeEnqueue(
        @SuppressWarnings("rawtypes") GatewayQueueEvent event) {
      
      if (GemFireXDUtils.TraceDBSynchronizer) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
            "WanProcedures::DBSynchronizerFilter:enqueueEvent: Into the beforeEnqueue");
      }
      
      if (event instanceof EntryEventImpl) {
        Object rawCallbackArg = ((EntryEventImpl)event).getRawCallbackArgument();
        if (rawCallbackArg instanceof GatewaySenderEventCallbackArgument) {
          int origDSId = ((GatewaySenderEventCallbackArgument)rawCallbackArg).getOriginatingDSId();
          int thisDSId = InternalDistributedSystem.getAnyInstance().getDistributionManager().getDistributedSystemId();
          // Always apply events coming from other distributed systems.
          if (origDSId != thisDSId) {
            if (GemFireXDUtils.TraceDBSynchronizer) {
              SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
                  "WanProcedures::DBSynchronizerFilter:enqueueEvent: "
                      + "Returning true for event from other WAN site with DSID[" + origDSId + "]. My DSID is ["+thisDSId+"]");
            }
            return true;
          } 
        }
      }
      
      if (super.beforeEnqueue(event)) {
        Object cb = event.getCallbackArgument();
        if (cb instanceof GfxdCallbackArgument
            && ((GfxdCallbackArgument)cb).isCacheLoaded()) {
          // It is PK based alright, but if it is transactional or
          // obtained via cache loading , we do not want it to go to
          // db synchronizer .For tx not being added
          // as tx statements will take bulk op route

          if (GemFireXDUtils.TraceDBSynchronizer) {
            SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
                "WanProcedures::DBSynchronizerFilter:enqueueEvent: "
                    + "Returning false as even when it is PK based, "
                    + "it is inserted via cache loading during get");
          }
          return false;
        }
        else {
          return true;
        }
      }
      return false;
    }

    @Override
    boolean enqueuePart2(GatewayQueueEvent<?, ?> event) {
      Object callbackArg = event.getCallbackArgument();
      if (callbackArg != null && callbackArg instanceof GfxdCallbackArgument) {
        return !((GfxdCallbackArgument)callbackArg).isSkipListeners();
      }
      return true;
    }
  }*/
  
  /**
   * UNUSED
   */
  /*
  public static final class ParallelDBSynchronizerFilter extends GfxdParallelWanFilter {

    private static final GatewayEventFilter filter = new ParallelDBSynchronizerFilter();

    @Override
    public boolean beforeEnqueue(
        @SuppressWarnings("rawtypes") GatewayQueueEvent event) {
      if (super.beforeEnqueue(event)) {
        Object cb = event.getCallbackArgument();
        if (cb instanceof GfxdCallbackArgument
            && ((GfxdCallbackArgument)cb).isCacheLoaded()) {
          // It is PK based alright, but if it is transactional or
          // obtained via cache loading , we do not want it to go to
          // db synchronizer .For tx not being added
          // as tx statements will take bulk op route

          if (GemFireXDUtils.TraceDBSynchronizer) {
            SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
                "WanProcedures::DBSynchronizerFilter:enqueueEvent: "
                    + "Returning false as even when it is PK based, "
                    + "it is inserted via cache loading during get");
          }
          return false;
        }
        else {
          return true;
        }
      }
      return false;
    }

    @Override
    boolean enqueuePart2(GatewayQueueEvent<?, ?> event) {
      Object callbackArg = event.getCallbackArgument();
      if (callbackArg != null && callbackArg instanceof GfxdCallbackArgument) {
        return !((GfxdCallbackArgument)callbackArg).isSkipListeners();
      }
      return true;
    }
  }
  */
  

  public static class GfxdWanBULKDMLOptimizedFilter extends AsyncEventFilter {

    private static final GatewayEventFilter filter = new GfxdWanBULKDMLOptimizedFilter();

    @Override
    public boolean beforeEnqueue(
        @SuppressWarnings("rawtypes") GatewayQueueEvent event) {
      Operation op = event.getOperation();

      if (GemFireXDUtils.TraceDBSynchronizer) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
            "WanProcedures::GfxdWanBULKDMLOptimizedFilter:enqueueEvent: Into the beforeEnqueue");
      }
      
      if (op == Operation.BULK_DML_OP) {
        if (GemFireXDUtils.TraceDBSynchronizer) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
              "WanProcedures::GfxdWanBULKDMLOptimizedFilter:enqueueEvent: "
                  + "Returning true for bulk DML op");
        }
        return true;
      }
      if (super.beforeEnqueue(event)) {
        Object cb = event.getCallbackArgument();
        if (cb instanceof GfxdCallbackArgument) {
          final GfxdCallbackArgument ecb = (GfxdCallbackArgument)cb;
          // It is PK based alright, but if it is transactional or
          // obtained via cache loading , we do not want it to go to
          // db synchronizer .For tx not being added
          // as tx statements will take bulk op route
          if (ecb.isTransactional()) {
            if (GemFireXDUtils.TraceDBSynchronizer) {
              SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
                  "WanProcedures::GfxdWanBULKDMLOptimizedFilter:enqueueEvent: Returning "
                      + "false as even when it is PK based, because it is "
                      + "transactional or inserted via cache loading "
                      + "during get");
            }
            return false;
          }
          else if (ecb.isPkBased()) {
            final GemFireContainer gfc = (GemFireContainer)event.getRegion()
                .getUserAttribute();
            // don't enqueue if there are auto-generated columns
            if (!((InternalDeltaEvent)event).isGFXDCreate(false)
                || !gfc.getExtraTableInfo().hasAutoGeneratedColumns()) {
              if (GemFireXDUtils.TraceDBSynchronizer) {
                SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
                    "WanProcedures::GfxdWanBULKDMLOptimizedFilter:enqueueEvent: "
                        + "Returning true for PK based event");
              }
              return true;
            }
            else {
              // Operations on tables with auto-generated columns now always go as full rows
              // and not via the bulk DML route
              if (GemFireXDUtils.TraceDBSynchronizer) {
                SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
                    "WanProcedures::GfxdWanBULKDMLOptimizedFilter:enqueueEvent: Returning true "
                        + "for PK based event due to auto-generated columns: "
                        + Arrays.toString(gfc.getExtraTableInfo()
                            .getAutoGeneratedColumns()));
              }
              return true;
            }
          }
          else {
            if (GemFireXDUtils.TraceDBSynchronizer) {
              SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
                  "WanProcedures::GfxdWanBULKDMLOptimizedFilter:enqueueEvent: "
                      + "Returning false");
            }
          }
        }
        else if (cb == null || cb instanceof Integer) {
          if (GemFireXDUtils.TraceDBSynchronizer) {
            SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
                "WanProcedures::GfxdWanBULKDMLOptimizedFilter:enqueueEvent: Returning "
                    + "false as it is not a PK based event nor bulk DML");
          }
          // This block is added just to trap unexpected code path, if any.
          // it can be removed later.
          return false;
        }
        else {
          throw new InternalGemFireError("Unexpected callback argument type."
              + " callbackArg=" + cb + "; type=" + cb.getClass().getName());
        }
      }
      return false;
    }

    @Override
    boolean enqueuePart2(GatewayQueueEvent<?, ?> event) {
      return true;
    }
  }
  

  
  /**
   * UNUSED
   */
  /*
  public static class GfxdWanFilter extends AsyncEventFilter {

    private static final GatewayEventFilter filter = new GfxdWanFilter();

    @Override
    public boolean beforeEnqueue(
        @SuppressWarnings("rawtypes") GatewayQueueEvent event) {
      Operation op = event.getOperation();

      if (GemFireXDUtils.TraceDBSynchronizer) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
            "WanProcedures::GfxdWanFilter:enqueueEvent: Into the beforeEnqueue");
      }
      
      if (op == Operation.BULK_DML_OP) {
        // For serial with BULK DML optimization we do not have to 
        // send bulk dml string but actual events.
        if (GemFireXDUtils.TraceDBSynchronizer) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
              "WanProcedures::GfxdWanFilter:enqueueEvent: "
                  + "Returning false for bulk DML op");
        }
        return false;
      }
      // Rest all events must go.
      // Below code is no required and can just be returned
      // what super.beforeEnqueue returns.
      if (super.beforeEnqueue(event)) {
        Object cb = event.getCallbackArgument();
        if (cb instanceof GfxdCallbackArgument) {
          final GfxdCallbackArgument ecb = (GfxdCallbackArgument)cb;
          // It is PK based alright, but if it is transactional or
          // obtained via cache loading , we do not want it to go to
          // db synchronizer .For tx not being added
          // as tx statements will take bulk op route
          if (ecb.isTransactional()) {
            if (GemFireXDUtils.TraceDBSynchronizer) {
              SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
                  "WanProcedures::GfxdWanFilter:enqueueEvent: Returning "
                      + "true as even when it is PK based, because it is "
                      + "transactional or inserted via cache loading "
                      + "during get");
            }
            return true;
          }
          else if (ecb.isPkBased()) {
            final GemFireContainer gfc = (GemFireContainer)event.getRegion()
                .getUserAttribute();
            // don't enqueue if there are auto-generated columns
            if (!((InternalDeltaEvent)event).isGFXDCreate(false)
                || !gfc.getExtraTableInfo().hasAutoGeneratedColumns()) {
              if (GemFireXDUtils.TraceDBSynchronizer) {
                SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
                    "WanProcedures::GfxdWanFilter:enqueueEvent: "
                        + "Returning true for PK based event");
              }
              return true;
            }
            else {
              // auto-generated columns will take the bulk DML route
              if (GemFireXDUtils.TraceDBSynchronizer) {
                SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
                    "WanProcedures::GfxdWanFilter:enqueueEvent: Returning true "
                        + "for PK based event due to auto-generated columns: "
                        + Arrays.toString(gfc.getExtraTableInfo()
                            .getAutoGeneratedColumns()));
              }
              // let auto-generated columns also go
              return true;
            }
          }
          else {
            if (GemFireXDUtils.TraceDBSynchronizer) {
              SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
                  "WanProcedures::GfxdWanFilter:enqueueEvent: "
                      + "Returning true");
            }
            return true;
          }
        }
        else if (cb == null || cb instanceof Integer) {
          if (GemFireXDUtils.TraceDBSynchronizer) {
            SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_DB_SYNCHRONIZER,
                "WanProcedures::GfxdWanFilter:enqueueEvent: Returning "
                    + "false as it is not a PK based event nor bulk DML");
          }
          // This block is added just to trap unexpected code path, if any.
          // it can be removed later.
          return false;
        }
        else {
          throw new InternalGemFireError("Unexpected callback argument type."
              + " callbackArg=" + cb + "; type=" + cb.getClass().getName());
        }
      }
      return false;
    }

    @Override
    boolean enqueuePart2(GatewayQueueEvent<?, ?> event) {
      return true;
    }
  }
  */
  
  /*
  public static GatewayEventFilter getDBSynchronizerFilter() {
    return DBSynchronizerFilter.filter;
  }

  public static GatewayEventFilter getBULKDMLOptimizedDBSynchronizerFilter() {
    return BULKDMLOptimizedDBSynchronizerFilter.filter;
  }
  
  public static GatewayEventFilter getSerialDBSynchronizerFilter() {
    return SerialDBSynchronizerFilter.filter;
  }

  public static GatewayEventFilter getSerialWanFilter() {
    return GfxdSerialWanFilter.filter;
  }
  
  public static GatewayEventFilter getBULKDMLWanFilter() {
    return GfxdWanBULKDMLOptimizedFilter.filter; 
  }
  
  public static GatewayEventFilter getParallelWanFilter() {
    return GfxdParallelWanFilter.filter;
  }
  
  public static GatewayEventFilter getParallelDBSynchronizerFilter() {
    return ParallelDBSynchronizerFilter.filter;
  }
  */

  public static GatewayEventFilter getAsyncEventFilter() {
    return AsyncEventFilter.filter;
  }

  public static GatewayEventFilter getSerialDBSynchronizerFilter(boolean sendBulkDMLAsString) {
    // Ignore sendBulkDMLAsString for now
    //return SerialDBSynchronizerFilter.filter;
    return BULKDMLOptimizedDBSynchronizerFilter.filter;
  }
  
  public static GatewayEventFilter getSerialWanFilter(boolean sendBulkDMLAsString) {
    // Ignore sendBulkDMLAsString for now
    return SerialWanFilter.filter;
  }
  public static GatewayEventFilter getParallelWanFilter() {
    return ParallelWanFilter.filter;
  }
}
