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
package cacheperf.comparisons.gemfirexd.useCase1;

import cacheperf.comparisons.gemfirexd.QueryPerfException;
import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.internal.NanoTimer;
import perffmwk.HistogramStats;
import perffmwk.PerformanceStatistics;

/**
 * Implements statistics related to UseCase1.
 */
public class UseCase1Stats extends PerformanceStatistics {

  public static enum Stmt {
        countChunkedMessages,
        countUnprocessedChunksOnAckQueue,
        getBORawDataByPrimaryKey,
        getByBackOfficeTxnId2,
        getByBackOfficeTxnId,
        getByFircosoftMessageId,
        getFsMessageId,
        getMQNameBOMap,
        insertBORawData,
        insertChnHist,
        insertChnHistConstraintViolation,
        insertChnHistQuery1,
        insertChnHistQuery2,
        insertExtraBOTables,
        insertSectChannelData,
        insertStatusHist,
        insertToBOLogTable,
        persistChunkedMessages2,
        persistChunkedMessages,
        persistFsMessage,
        selectBasedOnAckStatus,
        selectBasedOnChunkId,
        selectBasedOnFircMsgId,
        selectBasedOnOutStatus,
        selectInitialOFACMsgOrdered,
        selectInitialOFACMsg;
  }

  /** <code>UseCase1Stats</code> are maintained on a per-thread basis */
  private static final int SCOPE = THREAD_SCOPE;

  public static final String VM_COUNT = "vmCount";

  protected static final String DUMMY_OPS_COMPLETED = "dummyOpsCompleted";
  protected static final String DUMMY_OP_TIME = "dummyOpTime";

  protected static final String INBOUND_OPS_COMPLETED = "inboundOpsCompleted";
  protected static final String INBOUND_OP_TIME = "inboundOpTime";

  protected static final String OUTBOUND_OPS_COMPLETED = "outboundOpsCompleted";
  protected static final String OUTBOUND_OP_TIME = "outboundOpTime";

  protected static final String PURGE_OPS_COMPLETED = "purgeOpsCompleted";
  protected static final String PURGE_OP_TIME = "purgeOpTime";
  protected static final String PURGE_ERRORS = "purgeErrors";
  protected static final String PURGE_ERROR_TIME = "purgeErrorTime";

  protected static final String MATCHED = "matched";
  protected static final String MULTIMATCHED = "multiMatched";
  protected static final String UNMATCHED = "unmatched";
  protected static final String MATCH_ERRORS = "matchErrors";
  protected static final String TOTAL_MATCHED_ROWS = "totalMatchedRows";

  protected static final String MATCHED_TIME = "matchedTime";
  protected static final String MULTIMATCHED_TIME = "multiMatchedTime";
  protected static final String UNMATCHED_TIME = "unmatchedTime";
  protected static final String MATCH_ERROR_TIME = "matchErrorTime";
  protected static final String TOTAL_MATCHED_ROW_TIME = "totalMatchedRowTime";

  protected static final String countChunkedMessages = "countChunkedMessages";
  protected static final String countChunkedMessagesTime = "countChunkedMessagesTime";

  protected static final String countUnprocessedChunksOnAckQueue = "countUnprocessedChunksOnAckQueue";
  protected static final String countUnprocessedChunksOnAckQueueTime = "countUnprocessedChunksOnAckQueueTime";

  protected static final String getBORawDataByPrimaryKey = "getBORawDataByPrimaryKey";
  protected static final String getBORawDataByPrimaryKeyTime = "getBORawDataByPrimaryKeyTime";

  protected static final String getByBackOfficeTxnId2 = "getByBackOfficeTxnId2";
  protected static final String getByBackOfficeTxnId2Time = "getByBackOfficeTxnId2Time";

  protected static final String getByBackOfficeTxnId = "getByBackOfficeTxnId";
  protected static final String getByBackOfficeTxnIdTime = "getByBackOfficeTxnIdTime";

  protected static final String getByFircosoftMessageId = "getByFircosoftMessageId";
  protected static final String getByFircosoftMessageIdTime = "getByFircosoftMessageIdTime";

  protected static final String getFsMessageId = "getFsMessageId";
  protected static final String getFsMessageIdTime = "getFsMessageIdTime";

  protected static final String getMQNameBOMap = "getMQNameBOMap";
  protected static final String getMQNameBOMapTime = "getMQNameBOMapTime";

  protected static final String insertBORawData = "insertBORawData";
  protected static final String insertBORawDataTime = "insertBORawDataTime";

  protected static final String insertChnHist = "insertChnHist";
  protected static final String insertChnHistTime = "insertChnHistTime";

  protected static final String insertChnHistConstraintViolation = "insertChnHistConstraintViolation";
  protected static final String insertChnHistConstraintViolationTime = "insertChnHistConstraintViolationTime";

  protected static final String insertChnHistQuery1 = "insertChnHistQuery1";
  protected static final String insertChnHistQuery1Time = "insertChnHistQuery1Time";

  protected static final String insertChnHistQuery2 = "insertChnHistQuery2";
  protected static final String insertChnHistQuery2Time = "insertChnHistQuery2Time";

  protected static final String insertExtraBOTables = "insertExtraBOTables";
  protected static final String insertExtraBOTablesTime = "insertExtraBOTablesTime";

  protected static final String insertSectChannelData = "insertSectChannelData";
  protected static final String insertSectChannelDataTime = "insertSectChannelDataTime";

  protected static final String insertStatusHist = "insertStatusHist";
  protected static final String insertStatusHistTime = "insertStatusHistTime";

  protected static final String insertToBOLogTable = "insertToBOLogTable";
  protected static final String insertToBOLogTableTime = "insertSToBOLogTableTime";

  protected static final String persistChunkedMessages2 = "persistChunkedMessages2";
  protected static final String persistChunkedMessages2Time = "persistChunkedMessages2Time";

  protected static final String persistChunkedMessages = "persistChunkedMessages";
  protected static final String persistChunkedMessagesTime = "persistChunkedMessagesTime";

  protected static final String persistFsMessage = "persistFsMessage";
  protected static final String persistFsMessageTime = "persistFsMessageTime";

  protected static final String selectBasedOnAckStatus = "selectBasedOnAckStatus";
  protected static final String selectBasedOnAckStatusTime = "selectBasedOnAckStatusTime";

  protected static final String selectBasedOnChunkId = "selectBasedOnChunkId";
  protected static final String selectBasedOnChunkIdTime = "selectBasedOnChunkIdTime";

  protected static final String selectBasedOnFircMsgId = "selectBasedOnFircMsgId";
  protected static final String selectBasedOnFircMsgIdTime = "selectBasedOnFircMsgIdTime";

  protected static final String selectBasedOnOutStatus = "selectBasedOnOutStatus";
  protected static final String selectBasedOnOutStatusTime = "selectBasedOnOutStatusTime";

  protected static final String selectInitialOFACMsgOrdered = "selectInitialOFACMsgOrdered";
  protected static final String selectInitialOFACMsgOrderedTime = "selectInitialOFACMsgOrderedTime";

  protected static final String selectInitialOFACMsg = "selectInitialOFACMsg";
  protected static final String selectInitialOFACMsgTime = "selectInitialOFACMsgTime";

  ////////////////////////  Static Methods  ////////////////////////

  /**
   * Returns the statistic descriptors for <code>UseCase1Stats</code>.
   */
  public static StatisticDescriptor[] getStatisticDescriptors() {
    boolean largerIsBetter = true;
    return new StatisticDescriptor[] {
      factory().createIntGauge
      (
        VM_COUNT,
        "When aggregated, the number of VMs using this statistics object.",
        "VMs"
      ),
      factory().createIntCounter
      (
        countChunkedMessages,
        "Number of countChunkedMessages statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        countChunkedMessagesTime,
        "Total time spent executing countChunkedMessages statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        countUnprocessedChunksOnAckQueue,
        "Number of countUnprocessedChunksOnAckQueue statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        countUnprocessedChunksOnAckQueueTime,
        "Total time spent executing countUnprocessedChunksOnAckQueue statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        getBORawDataByPrimaryKey,
        "Number of getBORawDataByPrimaryKey statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        getBORawDataByPrimaryKeyTime,
        "Total time spent executing getBORawDataByPrimaryKey statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        getByBackOfficeTxnId2,
        "Number of getByBackOfficeTxnId2 statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        getByBackOfficeTxnId2Time,
        "Total time spent executing getByBackOfficeTxnId2 statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        getByBackOfficeTxnId,
        "Number of getByBackOfficeTxnId statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        getByBackOfficeTxnIdTime,
        "Total time spent executing getByBackOfficeTxnId statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        getByFircosoftMessageId,
        "Number of getByFircosoftMessageId statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        getByFircosoftMessageIdTime,
        "Total time spent executing getByFircosoftMessageId statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        getFsMessageId,
        "Number of getFsMessageId statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        getFsMessageIdTime,
        "Total time spent executing getFsMessageId statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        getMQNameBOMap,
        "Number of getMQNameBOMap statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        getMQNameBOMapTime,
        "Total time spent executing getMQNameBOMap statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        insertBORawData,
        "Number of insertBORawData statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        insertBORawDataTime,
        "Total time spent executing insertBORawData statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        insertChnHist,
        "Number of insertChnHist statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        insertChnHistTime,
        "Total time spent executing insertChnHist statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        insertChnHistConstraintViolation,
        "Number of insertChnHist statement executions that got constraint violations.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        insertChnHistConstraintViolationTime,
        "Total time spent executing insertChnHist statements that got constraint violations.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        insertChnHistQuery1,
        "Number of insertChnHistQuery1 statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        insertChnHistQuery1Time,
        "Total time spent executing insertChnHistQuery1 statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        insertChnHistQuery2,
        "Number of insertChnHistQuery2 statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        insertChnHistQuery2Time,
        "Total time spent executing insertChnHistQuery2 statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        insertExtraBOTables,
        "Number of insertExtraBOTables statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        insertExtraBOTablesTime,
        "Total time spent executing insertExtraBOTables statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        insertSectChannelData,
        "Number of insertSectChannelData statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        insertSectChannelDataTime,
        "Total time spent executing insertSectChannelData statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        insertStatusHist,
        "Number of insertStatusHist statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        insertStatusHistTime,
        "Total time spent executing insertStatusHist statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        insertToBOLogTable,
        "Number of insertToBOLogTable statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        insertToBOLogTableTime,
        "Total time spent executing insertToBOLogTable statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        persistChunkedMessages2,
        "Number of persistChunkedMessages2 statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        persistChunkedMessages2Time,
        "Total time spent executing persistChunkedMessages2 statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        persistChunkedMessages,
        "Number of persistChunkedMessages statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        persistChunkedMessagesTime,
        "Total time spent executing persistChunkedMessages statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        persistFsMessage,
        "Number of persistFsMessage statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        persistFsMessageTime,
        "Total time spent executing persistFsMessage statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        selectBasedOnAckStatus,
        "Number of selectBasedOnAckStatus statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        selectBasedOnAckStatusTime,
        "Total time spent executing selectBasedOnAckStatus statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        selectBasedOnChunkId,
        "Number of selectBasedOnChunkId statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        selectBasedOnChunkIdTime,
        "Total time spent executing selectBasedOnChunkId statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        selectBasedOnFircMsgId,
        "Number of selectBasedOnFircMsgId statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        selectBasedOnFircMsgIdTime,
        "Total time spent executing selectBasedOnFircMsgId statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        selectBasedOnOutStatus,
        "Number of selectBasedOnOutStatus statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        selectBasedOnOutStatusTime,
        "Total time spent executing selectBasedOnOutStatus statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        selectInitialOFACMsgOrdered,
        "Number of selectInitialOFACMsgOrdered statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        selectInitialOFACMsgOrderedTime,
        "Total time spent executing selectInitialOFACMsgOrdered statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        selectInitialOFACMsg,
        "Number of selectInitialOFACMsg statements executed.",
        "executions",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        selectInitialOFACMsgTime,
        "Total time spent executing selectInitialOFACMsg statements.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        DUMMY_OPS_COMPLETED,
        "Number of dummy ops completed.",
        "operations",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        DUMMY_OP_TIME,
        "Total time spent on dummy ops that were completed.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        INBOUND_OPS_COMPLETED,
        "Number of inbound ops completed.",
        "operations",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        INBOUND_OP_TIME,
        "Total time spent on inbound ops that were completed.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        OUTBOUND_OPS_COMPLETED,
        "Number of outbound ops completed.",
        "operations",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        OUTBOUND_OP_TIME,
        "Total time spent on outbound ops that were completed.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        PURGE_ERRORS,
        "Number of errors in purge stored procedure calls.",
        "errors",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        PURGE_ERROR_TIME,
        "Total time spent on errors in purge stored procedure calls.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        PURGE_OPS_COMPLETED,
        "Number of purge stored procedure calls completed.",
        "calls",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        PURGE_OP_TIME,
        "Total time spent on purge stored procedure calls that were completed.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        MATCHED,
        "Number of MATCHED results for stored procedure.",
        "results",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        MATCHED_TIME,
        "Total time spent getting MATCHED results.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        MULTIMATCHED,
        "Number of MULTI-MATCHED results for stored procedure.",
        "results",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        MULTIMATCHED_TIME,
        "Total time spent getting MULTIMATCHED results.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        UNMATCHED,
        "Number of UNMATCHED results for stored procedure.",
        "results",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        UNMATCHED_TIME,
        "Total time spent getting UNMATCHED results.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        MATCH_ERRORS,
        "Number of match ERRORS for stored procedure.",
        "results",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        MATCH_ERROR_TIME,
        "Total time spent getting match ERRORS.",
        "nanoseconds",
        !largerIsBetter
      ),
      factory().createIntCounter
      (
        TOTAL_MATCHED_ROWS,
        "Total number of matched rows for stored procedure.",
        "results",
        largerIsBetter
      ),
      factory().createLongCounter
      (
        TOTAL_MATCHED_ROW_TIME,
        "Total time spent getting matches.",
        "nanoseconds",
        !largerIsBetter
      )
    };
  }

  public static UseCase1Stats getInstance() {
    UseCase1Stats tps =
         (UseCase1Stats)getInstance(UseCase1Stats.class, SCOPE);
    tps.incVMCount();
    return tps;
  }
  public static UseCase1Stats getInstance(String name) {
    UseCase1Stats tps =
         (UseCase1Stats)getInstance(UseCase1Stats.class, SCOPE, name);
    tps.incVMCount();
    return tps;
  }
  public static UseCase1Stats getInstance( String name, String trimspecName ) {
    UseCase1Stats tps =
         (UseCase1Stats)getInstance(UseCase1Stats.class, SCOPE, name, trimspecName);
    tps.incVMCount();
    return tps;
  }

/////////////////// Construction / initialization ////////////////

  public UseCase1Stats( Class cls, StatisticsType type, int scope,
                    String instanceName, String trimspecName ) {
    super( cls, type, scope, instanceName, trimspecName );
  }

/////////////////// Updating stats /////////////////////////

// vm count --------------------------------------------------------------------

  /**
   * increase the count on the vmCount
   */
  private synchronized void incVMCount() {
    if (!VMCounted) {
      statistics().incInt(VM_COUNT, 1);
      VMCounted = true;
    }
  }
  private static boolean VMCounted = false;

// histogram -------------------------------------------------------------------

  /**
   * increase the time on the optional histogram by the supplied amount
   */
  public void incHistogram(HistogramStats histogram, long amount) {
    if (histogram != null) {
      histogram.incBin(amount);
    }
  }

// workload---------------------------------------------------------------------

  public long startCommit() {
    return NanoTimer.getTime();
  }

  public long startDummy() {
    return NanoTimer.getTime();
  }

  public void endDummy(long start) {
    long end = NanoTimer.getTime();
    long elapsed = end - start;
    statistics().incInt(DUMMY_OPS_COMPLETED, 1);
    statistics().incLong(DUMMY_OP_TIME, elapsed);
  }

  public long startInbound() {
    return NanoTimer.getTime();
  }

  public void endInbound(long start) {
    long end = NanoTimer.getTime();
    long elapsed = end - start;
    statistics().incInt(INBOUND_OPS_COMPLETED, 1);
    statistics().incLong(INBOUND_OP_TIME, elapsed);
  }

  public long startOutbound() {
    return NanoTimer.getTime();
  }

  public void endOutbound(long start) {
    long end = NanoTimer.getTime();
    long elapsed = end - start;
    statistics().incInt(OUTBOUND_OPS_COMPLETED, 1);
    statistics().incLong(OUTBOUND_OP_TIME, elapsed);
  }

  public long startPurge() {
    return NanoTimer.getTime();
  }

  public void endPurge(long start) {
    long end = NanoTimer.getTime();
    long elapsed = end - start;
    statistics().incInt(PURGE_OPS_COMPLETED, 1);
    statistics().incLong(PURGE_OP_TIME, elapsed);
  }

  public void endPurgeError(long start) {
    long end = NanoTimer.getTime();
    long elapsed = end - start;
    statistics().incInt(PURGE_ERRORS, 1);
    statistics().incLong(PURGE_ERROR_TIME, elapsed);
  }

  public long startMatch() {
    return NanoTimer.getTime();
  }

  public void endMatch(long start, int numRows, int errorState) {
    long end = NanoTimer.getTime();
    long elapsed = end - start;
    statistics().incInt(TOTAL_MATCHED_ROWS, numRows);
    statistics().incLong(TOTAL_MATCHED_ROW_TIME, elapsed);
    switch (numRows) {
      case 0:
        statistics().incInt(UNMATCHED, 1);
        statistics().incLong(UNMATCHED_TIME, elapsed);
        break;
      case 1:
        statistics().incInt(MATCHED, 1);
        statistics().incLong(MATCHED_TIME, elapsed);
        break;
      default:
        statistics().incInt(MULTIMATCHED, 1);
        statistics().incLong(MULTIMATCHED_TIME, elapsed);
        break;
    }
    if (errorState > 0) {
      statistics().incInt(MATCH_ERRORS, 1);
      statistics().incLong(MATCH_ERROR_TIME, elapsed);
    }
  }

// statements ------------------------------------------------------------------

  public long startStmt(Stmt stmt) {
    return NanoTimer.getTime();
  }

  public void endStmt(Stmt stmt, long start) {
    endStmt(stmt, start, 1, null);
  }

  public void endStmt(Stmt stmt, long start, HistogramStats histogram) {
    endStmt(stmt, start, 1, histogram);
  }

  public void endStmt(Stmt stmt, long start, int amount, HistogramStats histogram) {
    long elapsed = NanoTimer.getTime() - start;
    switch (stmt) {
      case countChunkedMessages:
        statistics().incInt(countChunkedMessages, amount);
        statistics().incLong(countChunkedMessagesTime, elapsed);
        break;
      case countUnprocessedChunksOnAckQueue:
        statistics().incInt(countUnprocessedChunksOnAckQueue, amount);
        statistics().incLong(countUnprocessedChunksOnAckQueueTime, elapsed);
        break;
      case getBORawDataByPrimaryKey:
        statistics().incInt(getBORawDataByPrimaryKey, amount);
        statistics().incLong(getBORawDataByPrimaryKeyTime, elapsed);
        break;
      case getByBackOfficeTxnId2:
        statistics().incInt(getByBackOfficeTxnId2, amount);
        statistics().incLong(getByBackOfficeTxnId2Time, elapsed);
        break;
      case getByBackOfficeTxnId:
        statistics().incInt(getByBackOfficeTxnId, amount);
        statistics().incLong(getByBackOfficeTxnIdTime, elapsed);
        break;
      case getByFircosoftMessageId:
        statistics().incInt(getByFircosoftMessageId, amount);
        statistics().incLong(getByFircosoftMessageIdTime, elapsed);
        break;
      case getFsMessageId:
        statistics().incInt(getFsMessageId, amount);
        statistics().incLong(getFsMessageIdTime, elapsed);
        break;
      case getMQNameBOMap:
        statistics().incInt(getMQNameBOMap, amount);
        statistics().incLong(getMQNameBOMapTime, elapsed);
        break;
      case insertBORawData:
        statistics().incInt(insertBORawData, amount);
        statistics().incLong(insertBORawDataTime, elapsed);
        break;
      case insertChnHist:
        statistics().incInt(insertChnHist, amount);
        statistics().incLong(insertChnHistTime, elapsed);
        break;
      case insertChnHistConstraintViolation:
        statistics().incInt(insertChnHistConstraintViolation, amount);
        statistics().incLong(insertChnHistConstraintViolationTime, elapsed);
        break;
      case insertChnHistQuery1:
        statistics().incInt(insertChnHistQuery1, amount);
        statistics().incLong(insertChnHistQuery1Time, elapsed);
        break;
      case insertChnHistQuery2:
        statistics().incInt(insertChnHistQuery2, amount);
        statistics().incLong(insertChnHistQuery2Time, elapsed);
        break;
      case insertExtraBOTables:
        statistics().incInt(insertExtraBOTables, amount);
        statistics().incLong(insertExtraBOTablesTime, elapsed);
        break;
      case insertSectChannelData:
        statistics().incInt(insertSectChannelData, amount);
        statistics().incLong(insertSectChannelDataTime, elapsed);
        break;
      case insertStatusHist:
        statistics().incInt(insertStatusHist, amount);
        statistics().incLong(insertStatusHistTime, elapsed);
        break;
      case insertToBOLogTable:
        statistics().incInt(insertToBOLogTable, amount);
        statistics().incLong(insertToBOLogTableTime, elapsed);
        break;
      case persistChunkedMessages2:
        statistics().incInt(persistChunkedMessages2, amount);
        statistics().incLong(persistChunkedMessages2Time, elapsed);
        break;
      case persistChunkedMessages:
        statistics().incInt(persistChunkedMessages, amount);
        statistics().incLong(persistChunkedMessagesTime, elapsed);
        break;
      case persistFsMessage:
        statistics().incInt(persistFsMessage, amount);
        statistics().incLong(persistFsMessageTime, elapsed);
        break;
      case selectBasedOnAckStatus:
        statistics().incInt(selectBasedOnAckStatus, amount);
        statistics().incLong(selectBasedOnAckStatusTime, elapsed);
        break;
      case selectBasedOnChunkId:
        statistics().incInt(selectBasedOnChunkId, amount);
        statistics().incLong(selectBasedOnChunkIdTime, elapsed);
        break;
      case selectBasedOnFircMsgId:
        statistics().incInt(selectBasedOnFircMsgId, amount);
        statistics().incLong(selectBasedOnFircMsgIdTime, elapsed);
        break;
      case selectBasedOnOutStatus:
        statistics().incInt(selectBasedOnOutStatus, amount);
        statistics().incLong(selectBasedOnOutStatusTime, elapsed);
        break;
      case selectInitialOFACMsgOrdered:
        statistics().incInt(selectInitialOFACMsgOrdered, amount);
        statistics().incLong(selectInitialOFACMsgOrderedTime, elapsed);
        break;
      case selectInitialOFACMsg:
        statistics().incInt(selectInitialOFACMsg, amount);
        statistics().incLong(selectInitialOFACMsgTime, elapsed);
        break;
      default:
        String s = "Should not happen";
        throw new QueryPerfException(s);
    }
    if (histogram != null) {
      incHistogram(histogram, elapsed);
    }
  }
}
