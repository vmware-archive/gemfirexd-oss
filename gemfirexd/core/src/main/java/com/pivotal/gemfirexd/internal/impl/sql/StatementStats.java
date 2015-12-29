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

package com.pivotal.gemfirexd.internal.impl.sql;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.StatisticsTypeFactory;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.StatisticsTypeFactoryImpl;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;

/**
 * Statistics class for statements executing in GemFireXD.
 *
 * @author rdubey
 * @author soubhikc
 */
public final class StatementStats {
  /*notes:
   *   1. Add common to QN and DN statistics before the comment
   *      "// stats specific to data nodes BEGIN" and adjust the first field
   *      past this comment accordingly.
   *   2. Add datanode specific stats before the comment
   *      "// stats specific to data nodes END" and adjust the first field
   *      past this comment, if any.
   *   3. If String[]s needs another parameter for StatsDescriptor creation,
   *      it will need adjustment to numElementsAsStat variable.
   */

  private boolean enableClockStats = true;

  private final boolean isQueryNode;

  private final boolean createdWithExistingStats;

  /** The Statistics object that we delegate most behavior to */
  private final Statistics stats;

  /** Statistics type */
  private static final StatisticsType type;

  /** Index for compute node time */
  // private static final int computeNodeTimeId;
  // stats common to QN and DN BEGIN.
  /** Index for number of times statement compiled */
  public static final int numCompiledId = 0;

  /** Index for activation execute time */
  public static final int numExecutionsId = numCompiledId + 1;

  /** Index for activation execute time */
  public static final int numExecutionsInProgressId = numExecutionsId + 1;

  public static final int numGlobalIndexLookupId = numExecutionsInProgressId + 1;

  /** Index for no of dml write operations - insert/delete/update */
  public static final int numRowsModifiedId = numGlobalIndexLookupId + 1;

  /** Index for bind time */
  public static final int parseTimeId = numRowsModifiedId + 1;

  /** Index for bind time */
  public static final int bindTimeId = parseTimeId + 1;

  /** Index for time spent optimizing */
  public static final int optimizeTimeId = bindTimeId + 1;

  /** Index for compute query info */
  public static final int routingInfoTimeId = optimizeTimeId + 1;

  /** Index for activation class generation time */
  public static final int generateTimeId = routingInfoTimeId + 1;

  /** Index for the complete query plan compilation */
  public static final int totCompilationTimeId = generateTimeId + 1;

  public static final int executeTimeId = totCompilationTimeId + 1;

  public static final int projectionTimeId = executeTimeId + 1;

  /** Index for total query execution time (execute/open/next/close) */
  public static final int totExecutionTimeId = projectionTimeId + 1;

  /** Index for time taken in dml write operations - insert/delete/update */
  public static final int rowsModificationTimeId = totExecutionTimeId + 1;

  // update this with the last common stat added above this line.
  public static final int COMMON_STATS_END_OFFSET = rowsModificationTimeId;

  // stats common to QN and DN END.

  // stats specific to query nodes BEGIN.
  /* first QN specific statistic past common elements */
  public static final int qn_numRowsSeenId = COMMON_STATS_END_OFFSET + 1;

  public static final int qn_msgSendTime = qn_numRowsSeenId + 1;

  public static final int qn_msgSerTime = qn_msgSendTime + 1;

  public static final int qn_respDeSerTime = qn_msgSerTime + 1;

  public static final int QUERY_NODE_STATS_END_OFFSET = qn_respDeSerTime;

  // stats specific to query nodes END.

  // stats specific to data nodes BEGIN.
  /*first DN specific statistic past common elements*/
  public static final int dn_numRowsProjectedId = COMMON_STATS_END_OFFSET + 1;

  public static final int dn_numNLJoinRowsReturnedId = dn_numRowsProjectedId + 1;

  public static final int dn_numHASHJoinRowsReturnedId = dn_numNLJoinRowsReturnedId + 1;

  public static final int dn_numTableRowsScannedId = dn_numHASHJoinRowsReturnedId + 1;

  public static final int dn_numRowsHashScannedId = dn_numTableRowsScannedId + 1;

  public static final int dn_numIndexRowsScannedId = dn_numRowsHashScannedId + 1;

  public static final int dn_numRowsSortedId = dn_numIndexRowsScannedId + 1;

  public static final int dn_numSortRowsOverflowedId = dn_numRowsSortedId + 1;
  
  public static final int dn_numRowsGroupedAggregatedId = dn_numSortRowsOverflowedId + 1;

  public static final int dn_numSingleHopExecutions = dn_numRowsGroupedAggregatedId + 1;

  public static final int dn_numSubQueryRowsSeen = dn_numSingleHopExecutions + 1;

  public static final int dn_MsgDeSerTimeId = dn_numSubQueryRowsSeen + 1;

  public static final int dn_MsgProcessTimeId = dn_MsgDeSerTimeId + 1;

  public static final int dn_ResultIterationTimeId = dn_MsgProcessTimeId + 1;

  public static final int dn_RespSerTimeId = dn_ResultIterationTimeId + 1;

  public static final int dn_ThrottleTimeId = dn_RespSerTimeId + 1;

  public static final int dn_nlJoinTimeId = dn_ThrottleTimeId + 1;

  public static final int dn_hashJoinTimeId = dn_nlJoinTimeId + 1;

  public static final int dn_tableScanTimeId = dn_hashJoinTimeId + 1;

  public static final int dn_hashScanTimeId = dn_tableScanTimeId + 1;

  public static final int dn_indexScanTimeId = dn_hashScanTimeId + 1;

  public static final int dn_sortTimeId = dn_indexScanTimeId + 1;
  
  public static final int dn_groupedAggregationTimeId = dn_sortTimeId + 1;

  public static final int dn_subQueryScanTimeId = dn_groupedAggregationTimeId + 1;

  public static final int DATA_NODE_STATS_END_OFFSET = dn_subQueryScanTimeId;

  // stats specific to data nodes END.

  private static final int[] querynode_stats;

  private static final int[] datanode_stats;

  private static final StatisticDescriptor[] qnStatsDesc;

  private static final StatisticDescriptor[] dnStatsDesc;

  /** Name for this statistics class. */
  public static final String name = "StatementStats";

  static {

    try {
      final StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
      /* notes: number of String[] elements that constitutes one stat descriptor.
         change them when we need to add additional parameter for each stats descriptor creation.
         but then it has to be mentioned for each stat item.*/
      final int numElementsAsStat = 3;

      final String[] numDetails = new String[] {
          "NumTimesCompiled",
          "Number of times this statement is compiled (including recompilations)",
          "operations",
          "NumExecutions",
          "Number of times this statement is executed",
          "operations",
          "NumExecutionsInProgress",
          "Statements that are actively being processed during the statistics snapshot",
          "operations",
          "NumTimesGlobalIndexLookup",
          "Number of times global index lookup message exchanges occurred ",
          "operations",
          "NumRowsModified",
          "Number of rows modified by DML operation of insert/delete/update ",
          "operations", };

      final String[] timeDetails = new String[] {
          "ParseTime",
          "Time spent in parsing the query string",
          "milliseconds",
          "BindTime",
          "Time spent mapping this statement with database object's metadata (bind)",
          "milliseconds",
          "OptimizeTime",
          "Time spent determining the best execution path for this statement (optimize) ",
          "milliseconds",
          "RoutingInfoTime",
          "Time spent compiling details about routing information of query strings to data node(s) (processQueryInfo) ",
          "milliseconds",
          "GenerateTime",
          "Time spent to generate query execution plan definition (activation class)",
          "milliseconds",
          "TotalCompilationTime",
          "Total compilation time of the statement on this node (prepMinion)",
          "milliseconds",
          "ExecuteTime",
          "Time spent in creation of all the layers of query processing (ac.execute)",
          "nanoseconds",

          // rs.open expansion BEGIN
          "ProjectionTime",
          "Time to apply the projection and additional filters. (projectrestrict)",
          "nanoseconds",
          // rs.open expansion END

          "TotalExecutionTime",
          "Total execution time taken to process the statement on this node (execute/open/next/close) ",
          "nanoseconds",
          "RowsModificationTime",
          "Time taken to modify rows by DML operation of insert/delete/update",
          "nanoseconds", };

      final String[] numQueryNodeDetails = new String[] {
          "NumRowsSeen",
          "Number of rows returned from remote nodes (ResultHolder/Get convertibles)",
          "rowcounts", };

      final String[] timeQueryNodeDetails = new String[] {
          "MsgSendTime",
          "TCP send time of all the messages including serialization time and queue wait time",
          "nanoseconds",
          "MsgSerTime",
          "Serialization time for all the messages while sending to remote node(s)",
          "nanoseconds",
          "RespDeSerTime",
          "Response message deserialization time from remote node(s) excluding resultset deserialization",
          "nanoseconds", };

      final String[] numDataNodeDetails = new String[] {
          "NumProjectedRows",
          "Number of rows returned after applying projection at different stages of processing",
          "rowcounts",
          "NumNLJoinRowsReturned",
          "Number of rows output by nested loop joins of the query",
          "rowcounts",
          "NumHASHJoinRowsReturned",
          "Number of rows output by hash joins of the query",
          "rowcounts",
          "NumTableRowsScanned",
          "Number of rows returned from all the involved table scans ",
          "rowcounts",
          "NumRowsHashScanned",
          "Number of rows returned from a hash scan or distinct scan ",
          "rowcounts",
          "NumIndexRowsScanned",
          "Number of rows returned from all the involved index scans ",
          "rowcounts",
          "NumRowsSorted",
          "Number of rows that is sorted due to group by/order by clauses ",
          "rowcounts",
          "NumSortRowsOverflowed",
          "Number of rows overflowed to temporary files due to sorting ",
          "rowcounts",
          "NumRowsGroupedAggregated",
          "Number of rows returned after grouped aggregation",
          "rowcounts",
          "NumSingleHopExecutions",
          "Number of times the prepared statement executed locally due to single hop ",
          "rowcounts",
          "SubQueryNumRowsSeen",
          "Number of rows seen while subquery execution (by parent query) ",
          "rowcounts", };

      final String[] timeDataNodeDetails = new String[] {
          "MsgDeSerTime",
          "Message deserialization time including parameters but excluding MsgProcessTime",
          "nanoseconds",
          "MsgProcessTime",
          "Message process time including local data set creation, query execute and response sending time",
          "nanoseconds",
          "ResultIterationTime",
          "Time to iterate through determined set of results on this node ",
          "nanoseconds",
          "RespSerTime",
          "Response message serialization time excluding ResultIterationTime",
          "nanoseconds",
          "ThrottleTime",
          "Response throttle wait time on data node",
          "nanoseconds",
          "NLJoinTime",
          "Time to process all the nested loop join relations of the query",
          "nanoseconds",
          "HASHJoinTime",
          "Time to process all the hash join relations of the query",
          "nanoseconds",
          "TableScanTime",
          "Time taken to scan all the tables involved in the query",
          "nanoseconds",
          "HashScanTime",
          "Time taken to build a hash table for distinct evaluation or other optimization purpose ",
          "nanoseconds",
          "IndexScanTime",
          "Time taken to scan all the indexes involved in the query",
          "nanoseconds",
          "SortTime",
          "Time taken to sort rows while applying group by or order by clause",
          "nanoseconds",
          "GroupedAggregationTime",
          "Time taken to aggregate with group by clause",
          "nanoseconds",
          "SubQueryExecutionTime",
          "Time taken to scan rows from Subquery (by parent query)",
          "nanoseconds", };

      // Add QueryNode version of the stats
      final int qnStatItems = (numDetails.length + timeDetails.length
          + numQueryNodeDetails.length + timeQueryNodeDetails.length)
          / numElementsAsStat;
      assert qnStatItems == QUERY_NODE_STATS_END_OFFSET + 1;

      qnStatsDesc = new StatisticDescriptor[qnStatItems];
      int endOffset = numDetails.length / numElementsAsStat;
      int si = 0;
      final String QueryNodePrefix = "QN";
      for (int sd = 0; si < endOffset; sd += numElementsAsStat, si++) {
        if (numDetails[sd].endsWith("InProgress")) {
          qnStatsDesc[si] = f.createLongGauge(QueryNodePrefix + numDetails[sd],
              numDetails[sd + 1], numDetails[sd + 2]);
        }
        else {
          qnStatsDesc[si] = f.createLongCounter(QueryNodePrefix
              + numDetails[sd], numDetails[sd + 1], numDetails[sd + 2]);
        }
      }

      endOffset += timeDetails.length / numElementsAsStat;
      for (int sd = 0; si < endOffset; sd += numElementsAsStat, si++) {
        qnStatsDesc[si] = f.createLongCounter(
            QueryNodePrefix + timeDetails[sd], timeDetails[sd + 1],
            timeDetails[sd + 2]);
      }

      // Stats specific to query nodes BEGIN
      endOffset += numQueryNodeDetails.length / numElementsAsStat;
      for (int sd = 0; si < endOffset; sd += numElementsAsStat, si++) {
        qnStatsDesc[si] = f.createLongCounter(QueryNodePrefix
            + numQueryNodeDetails[sd], numQueryNodeDetails[sd + 1],
            numQueryNodeDetails[sd + 2]);
      }

      endOffset += timeQueryNodeDetails.length / numElementsAsStat;
      for (int sd = 0; si < endOffset; sd += numElementsAsStat, si++) {
        qnStatsDesc[si] = f.createLongCounter(QueryNodePrefix
            + timeQueryNodeDetails[sd], timeQueryNodeDetails[sd + 1],
            timeQueryNodeDetails[sd + 2]);
      }
      // Stats specific to query nodes END

      // Add DataNode version of the stats
      final int dnStatItems = (numDetails.length + timeDetails.length
          + numDataNodeDetails.length + timeDataNodeDetails.length)
          / numElementsAsStat;

      // assert dnStatItems == DATA_NODE_STATS_END_OFFSET + 1;
      dnStatsDesc = new StatisticDescriptor[dnStatItems];
      endOffset = numDetails.length / numElementsAsStat;
      si = 0;
      final String DataNodePrefix = "DN";
      for (int sd = 0; si < endOffset; sd += numElementsAsStat, si++) {
        if (numDetails[sd].endsWith("InProgress")) {
          dnStatsDesc[si] = f.createLongGauge(DataNodePrefix + numDetails[sd],
              numDetails[sd + 1], numDetails[sd + 2]);
        }
        else {
          dnStatsDesc[si] = f.createLongCounter(
              DataNodePrefix + numDetails[sd], numDetails[sd + 1],
              numDetails[sd + 2]);
        }
      }

      endOffset += timeDetails.length / numElementsAsStat;
      for (int sd = 0; si < endOffset; sd += numElementsAsStat, si++) {
        dnStatsDesc[si] = f.createLongCounter(DataNodePrefix + timeDetails[sd],
            timeDetails[sd + 1], timeDetails[sd + 2]);
      }

      // Stats specific to data nodes BEGIN
      endOffset += numDataNodeDetails.length / numElementsAsStat;
      for (int sd = 0; si < endOffset; sd += numElementsAsStat, si++) {
        dnStatsDesc[si] = f.createLongCounter(DataNodePrefix
            + numDataNodeDetails[sd], numDataNodeDetails[sd + 1],
            numDataNodeDetails[sd + 2]);
      }

      endOffset += timeDataNodeDetails.length / numElementsAsStat;
      for (int sd = 0; si < endOffset; sd += numElementsAsStat, si++) {
        dnStatsDesc[si] = f.createLongCounter(DataNodePrefix
            + timeDataNodeDetails[sd], timeDataNodeDetails[sd + 1],
            timeDataNodeDetails[sd + 2]);
      }
      // Stats specific to data nodes END

      {
        /*
         * TODO:[sb]:SC:
         *    Statistics Descriptor can be based on whether host data is false or true.
         *    If hostdata is true, we cannot say deterministically whether query will be
         *    executed locally. E.g DAP, Trigger and User Application executing the query,
         *    sub-query distribution etc might get sprayed everywhere.
         *
         *    We might still be able to cut down the stats for host-data false.
         */
        final StatisticDescriptor[] statsDesc = new StatisticDescriptor[qnStatsDesc.length
            + dnStatsDesc.length];
        for (int i = 0; i < qnStatsDesc.length; i++) {
          statsDesc[i] = qnStatsDesc[i];
        }
        for (int i = qnStatsDesc.length, j = 0; i < qnStatsDesc.length
            + dnStatsDesc.length; i++, j++) {
          statsDesc[i] = dnStatsDesc[j];
        }

        type = f
            .createType(
                name,
                "Statistics of statement aggregated across self and remote execution",
                statsDesc);
      }

      // -----------------------------------
      // Common Statistics for QN
      // -----------------------------------
      querynode_stats = new int[qnStatItems];
      querynode_stats[numCompiledId] = type.nameToId(qnStatsDesc[numCompiledId]
          .getName());
      //SanityManager.DEBUG_PRINT("info:sb:",
      //    "initializing " + querynode_stats[numCompiledId]
      //        + " querynode_stats[numComiledId] from "
      //        + qnStatsDesc[numCompiledId].getName());
      querynode_stats[parseTimeId] = type.nameToId(qnStatsDesc[parseTimeId]
                                                              .getName());
      querynode_stats[bindTimeId] = type.nameToId(qnStatsDesc[bindTimeId]
          .getName());
      querynode_stats[optimizeTimeId] = type
          .nameToId(qnStatsDesc[optimizeTimeId].getName());
      querynode_stats[routingInfoTimeId] = type
          .nameToId(qnStatsDesc[routingInfoTimeId].getName());
      querynode_stats[generateTimeId] = type
          .nameToId(qnStatsDesc[generateTimeId].getName());
      querynode_stats[totCompilationTimeId] = type
          .nameToId(qnStatsDesc[totCompilationTimeId].getName());

      querynode_stats[numExecutionsId] = type
          .nameToId(qnStatsDesc[numExecutionsId].getName());
      querynode_stats[numExecutionsInProgressId] = type
          .nameToId(qnStatsDesc[numExecutionsInProgressId].getName());
      querynode_stats[numGlobalIndexLookupId] = type
          .nameToId(qnStatsDesc[numGlobalIndexLookupId].getName());
      querynode_stats[numRowsModifiedId] = type
      .nameToId(qnStatsDesc[numRowsModifiedId].getName());

      querynode_stats[executeTimeId] = type.nameToId(qnStatsDesc[executeTimeId]
          .getName());
      querynode_stats[projectionTimeId] = type
          .nameToId(qnStatsDesc[projectionTimeId].getName());
      querynode_stats[totExecutionTimeId] = type
          .nameToId(qnStatsDesc[totExecutionTimeId].getName());
      querynode_stats[rowsModificationTimeId] = type
          .nameToId(qnStatsDesc[rowsModificationTimeId].getName());

      // -----------------------------------
      // QN specific items
      // -----------------------------------
      querynode_stats[qn_numRowsSeenId] = type
          .nameToId(qnStatsDesc[qn_numRowsSeenId].getName());

      // -----------------------------------
      // Common Statistics for DN
      // -----------------------------------
      datanode_stats = new int[dnStatItems];
      datanode_stats[numCompiledId] = type.nameToId(dnStatsDesc[numCompiledId]
          .getName());
      //SanityManager.DEBUG_PRINT("info:sb:",
      //    "initializing " + datanode_stats[numCompiledId]
      //        + " datanode_stats[numComiledId] from "
      //        + dnStatsDesc[numCompiledId].getName());
      datanode_stats[parseTimeId] = type.nameToId(dnStatsDesc[parseTimeId]
                                                             .getName());
      datanode_stats[bindTimeId] = type.nameToId(dnStatsDesc[bindTimeId]
                                                             .getName());
      datanode_stats[optimizeTimeId] = type
          .nameToId(dnStatsDesc[optimizeTimeId].getName());
      datanode_stats[routingInfoTimeId] = type
          .nameToId(dnStatsDesc[routingInfoTimeId].getName());
      datanode_stats[generateTimeId] = type
          .nameToId(dnStatsDesc[generateTimeId].getName());
      datanode_stats[totCompilationTimeId] = type
          .nameToId(dnStatsDesc[totCompilationTimeId].getName());

      datanode_stats[numExecutionsId] = type
          .nameToId(dnStatsDesc[numExecutionsId].getName());
      datanode_stats[numExecutionsInProgressId] = type
          .nameToId(dnStatsDesc[numExecutionsInProgressId].getName());
      datanode_stats[numGlobalIndexLookupId] = type
          .nameToId(dnStatsDesc[numGlobalIndexLookupId].getName());
      datanode_stats[numRowsModifiedId] = type
          .nameToId(dnStatsDesc[numRowsModifiedId].getName());

      datanode_stats[executeTimeId] = type.nameToId(dnStatsDesc[executeTimeId]
          .getName());
      datanode_stats[projectionTimeId] = type
          .nameToId(dnStatsDesc[projectionTimeId].getName());
      datanode_stats[totExecutionTimeId] = type
          .nameToId(dnStatsDesc[totExecutionTimeId].getName());
      datanode_stats[rowsModificationTimeId] = type
          .nameToId(dnStatsDesc[rowsModificationTimeId].getName());

      // -----------------------------------
      // DN specific items
      // -----------------------------------
      datanode_stats[dn_numRowsProjectedId] = type
          .nameToId(dnStatsDesc[dn_numRowsProjectedId].getName());
      datanode_stats[dn_numNLJoinRowsReturnedId] = type
          .nameToId(dnStatsDesc[dn_numNLJoinRowsReturnedId].getName());
      datanode_stats[dn_numHASHJoinRowsReturnedId] = type
          .nameToId(dnStatsDesc[dn_numHASHJoinRowsReturnedId].getName());
      datanode_stats[dn_numTableRowsScannedId] = type
          .nameToId(dnStatsDesc[dn_numTableRowsScannedId].getName());
      datanode_stats[dn_numRowsHashScannedId] = type
          .nameToId(dnStatsDesc[dn_numRowsHashScannedId].getName());
      datanode_stats[dn_numIndexRowsScannedId] = type
          .nameToId(dnStatsDesc[dn_numIndexRowsScannedId].getName());
      datanode_stats[dn_numRowsSortedId] = type
          .nameToId(dnStatsDesc[dn_numRowsSortedId].getName());
      datanode_stats[dn_numSortRowsOverflowedId] = type
          .nameToId(dnStatsDesc[dn_numSortRowsOverflowedId].getName());
      datanode_stats[dn_numRowsGroupedAggregatedId] = type
          .nameToId(dnStatsDesc[dn_numRowsGroupedAggregatedId].getName());
      datanode_stats[dn_numSingleHopExecutions] = type
          .nameToId(dnStatsDesc[dn_numSingleHopExecutions].getName());
      datanode_stats[dn_numSubQueryRowsSeen] = type
      .nameToId(dnStatsDesc[dn_numSubQueryRowsSeen].getName());

      datanode_stats[dn_MsgDeSerTimeId] = type
          .nameToId(dnStatsDesc[dn_MsgDeSerTimeId].getName());
      datanode_stats[dn_MsgProcessTimeId] = type
          .nameToId(dnStatsDesc[dn_MsgProcessTimeId].getName());
      datanode_stats[dn_ResultIterationTimeId] = type
          .nameToId(dnStatsDesc[dn_ResultIterationTimeId].getName());
      datanode_stats[dn_RespSerTimeId] = type
          .nameToId(dnStatsDesc[dn_RespSerTimeId].getName());
      datanode_stats[dn_ThrottleTimeId] = type
          .nameToId(dnStatsDesc[dn_ThrottleTimeId].getName());
      datanode_stats[dn_nlJoinTimeId] = type
          .nameToId(dnStatsDesc[dn_nlJoinTimeId].getName());
      datanode_stats[dn_hashJoinTimeId] = type
          .nameToId(dnStatsDesc[dn_hashJoinTimeId].getName());
      datanode_stats[dn_tableScanTimeId] = type
          .nameToId(dnStatsDesc[dn_tableScanTimeId].getName());
      datanode_stats[dn_hashScanTimeId] = type
          .nameToId(dnStatsDesc[dn_hashScanTimeId].getName());
      datanode_stats[dn_indexScanTimeId] = type
          .nameToId(dnStatsDesc[dn_indexScanTimeId].getName());
      datanode_stats[dn_sortTimeId] = type.nameToId(dnStatsDesc[dn_sortTimeId]
          .getName());
      datanode_stats[dn_groupedAggregationTimeId] = type
          .nameToId(dnStatsDesc[dn_groupedAggregationTimeId].getName());
      datanode_stats[dn_subQueryScanTimeId] = type
          .nameToId(dnStatsDesc[dn_subQueryScanTimeId].getName());

      // -----------------------------------

    } catch (Exception e) {
      SanityManager.DEBUG_PRINT("warning:ClassLoad",
          "Got exception while loading class " + StatementStats.class.getName()
              + "  ex = " + e, e);
      throw new RuntimeException(e);
    }
  }

  private StatementStats(final StatisticsFactory factory,
      final String statementString, final boolean queryNode, int uniqueId) {
    stats = factory.createAtomicStatistics(type, statementString, 0, uniqueId);
    isQueryNode = queryNode;
    this.createdWithExistingStats = false;
  }

  private StatementStats(Statistics s, boolean queryNode) {
    stats = s;
    isQueryNode = queryNode;
    this.createdWithExistingStats = true;
  }

  public final static StatementStats newInstance(final String statementString,
      final boolean queryNode, final int uniqueId) {

    final StatisticsFactory factory = Misc.getGemFireCache()
        .getDistributedSystem();

    // in case DD in write mode, we won't find a prepStatement but a
    // corresponding stats might have already been created.
    final Statistics existingStat = factory
        .findStatisticsByUniqueId(uniqueId);

    if (existingStat != null) {
        if (!existingStat.getTextId().equals(statementString)) {
          SanityManager.THROWASSERT(" uniqueId clashed with existing stat "
              + existingStat.getTextId() + " while creating statistics for "
              + statementString);
        }
        return new StatementStats(existingStat, queryNode);
    }

    return new StatementStats(factory, statementString, queryNode, uniqueId);
  }

  public String getStatsId() {
    return stats.getTextId();
  }

  public Statistics getStatistics() {
    return this.stats;
  }

  public boolean isQueryNode() {
    return isQueryNode;
  }

  /**
   * @return the whether this instance created new {@link Statistics} 
   */
  public boolean isCreatedWithExistingStats() {
    return this.createdWithExistingStats;
  }

  /*/**
   * Increment compute node time by delta mills.
   * @param delta
   *//*
     public void incComputeNodeTime(long delta) {
     this.stats.incLong(computeNodeTimeId, delta);
     }*/

  /**
   * Increment bind time by delta mills.
   */
  public final void incStat(final int id, boolean queryNode,
      final long value) {

    final int finalid = queryNode ? querynode_stats[id] : datanode_stats[id];
    if (SanityManager.DEBUG) {
      if (GemFireXDUtils.TraceStatsGeneration && SanityManager.isFinerEnabled) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_STATS_GENERATION,
            "StatementStatisticsCollector: incrementing offset="
            + finalid
            + " by "
            + value
            + " for "
            + this.stats.getTextId()
            + " id="
            + id
            + " "
            + (queryNode ? qnStatsDesc[id].getName() : dnStatsDesc[id]
                .getName()));
      }
    }

    if (SanityManager.ASSERT) {
      // some elements in the *node_stats array is uninitialized and defaulted
      // to zero.
      if (finalid == 0 && id != 0) {
        SanityManager.THROWASSERT("incrementing offset="
            + finalid
            + " by "
            + value
            + " for "
            + this.stats.getTextId()
            + " id="
            + id
            + " "
            + (queryNode ? qnStatsDesc[id].getName() : dnStatsDesc[id]
                .getName()));
      }
    }

    this.stats.incLong(finalid,
        value);
  }

  public final long getLong(final int id, final boolean isQueryNode) {
    return this.stats.getLong(isQueryNode ? querynode_stats[id]
        : datanode_stats[id]);
  }

  /**
   * Increment bind time by delta mills.
   */
  public void incBindTime(long bindTime) {
    this.stats.incLong(bindTimeId, bindTime);
  }

  /**
   * Increment compute query info time by delta mills.
   */
  public void incRoutingInfoTime(long begin) {
    if (this.enableClockStats) {
      long current = getStatTime();
      this.stats.incLong(routingInfoTimeId, current - begin);
    }
  }

  /**
   * Increment execute time by delta mills.
   */
  public void incExecuteTime(long begin) {
    if (this.enableClockStats) {
      long current = getStatTime();
      this.stats.incLong(totExecutionTimeId, (current - begin));
    }
  }

  /**
   * Increment execute time by delta mills.
   */
  public void incOpenTime(long begin) {
    if (this.enableClockStats) {
      long current = getStatTime();
      this.stats.incLong(projectionTimeId, (current - begin));
    }
  }

  /**
   * Increment executions started.
   */
  public void incExecutionsStarted() {
    // this.stats.incLong(qn_numTimesExecutionStartedId, 1);
  }

  /**
   * Increment executions ended.
   */
  public void incExecutionsEnded() {
    // this.stats.incLong(qn_numTimesExecutionsEndedId, 1);
  }

  /**
   * Increment get time by elapsed time in nano for this statement.
   *
   * @param begin
   *          start time.
   */
  public void incGetNextCoreDvdTime(long begin) {
    if (this.enableClockStats) {
      long current = getStatTime();
      long elapsed = current - begin;
      // this.stats.incLong(qn_getNextCoreDVDTimeId, elapsed);
    }
  }

  /**
   * Increment get time by elapsed time in nano for this statement.
   *
   * @param begin
   *          start time.
   */
  public void incGetNextCoreByteTime(long begin) {
    if (this.enableClockStats) {
      long current = getStatTime();
      long elapsed = current - begin;
      // this.stats.incLong(qn_getNextCoreByteTimeId, elapsed);
    }
  }

  /**
   * Increment number of times gets started for this statement
   */
  public void incNumGetsStartedDvd() {
    // this.stats.incLong(qn_numGetNextCoreDvdInProgressId, 1);
  }

  /**
   * Increment number of times gets started for this statement in byte format.
   */
  public void incNumGetsStartedByte() {
    // this.stats.incLong(qn_numGetNextCoreByteStoreInProgressId, 1);
  }

  /**
   * Increment number of gets ended for this statement.
   */
  public void incNumGetsEndedDvd() {
    // this.stats.incLong(qn_numGetNextCoreDvdEndedId, 1);
  }

  /**
   * Increment number of gets ended for this statement in byte format.
   */
  public void incNumGetsEndedByte() {
    // this.stats.incLong(qn_numGetNextCoreByteStoreEndedId, 1);
  }

  /**
   * Increment number of times this statement is optimized.
   *
   */
  public void incNumTimesCompiled() {
    this.stats.incLong(numCompiledId, 1);
  }

  /**
   * Increment time spent in optimizing this statement.
   *
   * @return
   */
  public void incOptimizeTime(long begin) {
    if (this.enableClockStats) {
      long current = getStatTime();
      this.stats.incLong(projectionTimeId, (current - begin));
    }
  }

  /**
   * Returns the current NanoTime or, if clock stats are disabled, zero.
   *
   * @return long 0 if time stats disabled or the current time in nano secounds.
   */
  public long getStatTime() {
    return this.enableClockStats ? NanoTimer.getTime() : 0;
  }

  /**
   * Increment remote execution time for this query.
   */
  public void incRemoteExecutionTime(long begin) {
    if (this.enableClockStats) {
      long current = getStatTime();
      // this.stats.incLong(qn_remoteExecutionTimeId, (current - begin));
    }
  }

  /**
   * Increment number of remote executions.
   */
  public void incNumRemoteExecution() {
    // this.stats.incLong(qn_numRemoteExecutionId, 1);
  }

  /**
   * Increment number of index or base table scan, especially reload array.
   */
  public void incNumReloadArray() {
    // this.stats.incLong(qn_numReloadArrayId, 1);
  }

  public void incNumRowsLoaded(int numRows) {
    // this.stats.incLong(qn_numRowsLoadedId, numRows);
  }

  public void incExecutionTimeDataNode(long begin) {
    if (this.enableClockStats) {
      long current = getStatTime();
      // this.stats.incLong(qn_executionTimeDataStoreId, (current - begin));
    }
  }

  public void incNumRowsReturnedFromDataNode(long numRows) {
    // this.stats.incLong(qn_numRowsReturnedFromDataNodeId, numRows);
  }

  public void incNumBytesWrittenPreQuery(long numBytes) {
    // this.stats.incLong(qn_numBytesWrittenPerQueryId, numBytes);
  }

  public void incWriteResutlSetDataNode(long begin) {
    if (this.enableClockStats) {
      long current = getStatTime();
      // this.stats.incLong(qn_writeResultSetDataNodeTimeId, (current - begin));
    }
  }

  public void close() {
    final Statistics stats = this.stats;
    if (stats != null) {
      this.enableClockStats = false;
      synchronized (stats) {
        if (!stats.isClosed()) {
          stats.close();
          Misc.getDistributedSystem().destroyStatistics(stats);
        }
      }
    }
  }

  public String toString() {
    return GemFireXDUtils.addressOf(this) + " textId=" + stats.getTextId()
        + " uniqueId=" + stats.getUniqueId()
        + " isQueryNode=" + isQueryNode;
  }
}

// --------- notes --------------
/*              new StatisticDescriptor[] {
f.createLongCounter("QueryNode_NumTimesCompiled",
    numTimesCompiled, "operations"),
f.createLongCounter("QueryNode_BindTime", bindTime,
    "nanoseconds"),
f.createLongCounter("QueryNode_OptimizeTime", optimizeTime,
    "nanoseconds"),
f.createLongCounter("QueryNode_RoutingInfoTime",
    qn_routingInfoTime, "nanoseconds"),
f.createLongCounter("QueryNode_GenerateTime", generateTime,
    "nanoseconds"),
f.createLongCounter("QueryNode_TotalCompilationTime",
    totCompilationTime, "miliseconds"),

f.createLongCounter("QueryNode_TotalExecutionTime",
    totExecuteTime, "miliseconds"),
//                    f.createLongCounter("QueryNode_OpenTime", rs_openTime,
//                    "nanoseconds"),
f.createLongCounter("QueryNode_ProjectionTime",
    rs_projectionTime, "nanoseconds"),
f.createLongCounter("QueryNode_NumExecutesStarted",
    numExecutesStarted, "operations"),
f.createLongCounter("QueryNode_NumExecutesEnded",
    numExecutesEnded, "operations"),
f.createLongCounter("QueryNode_GetNextRowCoreDVDTime",
    getNextRowCoreDvd, "nanoseconds"),
f.createLongCounter("QueryNode_NumGetsDvdInProgress",
    numGetNextRowCoreDvdInProg, "operations"),
f.createLongCounter("QueryNode_NumGetsDvdEnded",
    numGetNextRowCoreDvdEnded, "operations"),
f.createLongCounter("QueryNode_NumGetsByteInProgress",
    numGetNextRowCoreByteStoreInProg, "operations"),
f.createLongCounter("QueryNode_NumGetsByteEnded",
    numGetNextRowCoreByteStoreEnded, "operations"),
f.createLongCounter("QueryNode_GetNextRowCoreByteTime",
    timeSpentDoingGetsInByteFormat, "operations"),
f.createLongCounter("QueryNode_RemoteExecutionTime",
    timeSpentExecutingRemoteQuery, "nanoseconds"),
f.createLongCounter("QueryNode_NumRemoteExecutions",
    numRemoteExecution, "operations"),
f.createLongCounter("QueryNode_NumReloadArray",
    numReloadArray, "operations"),
f.createLongCounter("QueryNode_NumRowsLoaded", numRowsLoaded,
    "operations"),
f.createLongCounter("QueryNode_DataNodeQueryTime",
    executionTimeDataNode, "nanoseconds"),
f.createLongCounter("QueryNode_NumRowsReturnedPerExecution",
    numRowsReturnedFromDataNode, "operations"),
f.createLongCounter("QueryNode_NumBytesWrittenPerExecution",
    numBytesWittenPerQuery, "operations"),
f.createLongCounter("QueryNode_DataNodeRSWriteTime",
    timeSpentWritingResultSet, "nanoseconds")
*/

/*

      qn_numTimesExecutionStartedId = type
          .nameToId("QueryNode_NumExecutesStarted");
      qn_numTimesExecutionsEndedId = type
          .nameToId("QueryNode_NumExecutesEnded");
      qn_getNextCoreDVDTimeId = type
          .nameToId("QueryNode_GetNextRowCoreDVDTime");
      qn_numGetNextCoreDvdInProgressId = type
          .nameToId("QueryNode_NumGetsDvdInProgress");
      qn_numGetNextCoreDvdEndedId = type.nameToId("QueryNode_NumGetsDvdEnded");
      qn_numGetNextCoreByteStoreInProgressId = type
          .nameToId("QueryNode_NumGetsByteInProgress");
      qn_numGetNextCoreByteStoreEndedId = type
          .nameToId("QueryNode_NumGetsByteEnded");
      qn_getNextCoreByteTimeId = type
          .nameToId("QueryNode_GetNextRowCoreByteTime");
      qn_remoteExecutionTimeId = type.nameToId("QueryNode_RemoteExecutionTime");
      qn_numRemoteExecutionId = type.nameToId("QueryNode_NumRemoteExecutions");
      qn_numReloadArrayId = type.nameToId("QueryNode_NumReloadArray");
      qn_numRowsLoadedId = type.nameToId("QueryNode_NumRowsLoaded");
      qn_executionTimeDataStoreId = type
          .nameToId("QueryNode_DataNodeQueryTime");
      qn_numRowsReturnedFromDataNodeId = type
          .nameToId("QueryNode_NumRowsReturnedPerExecution");
      qn_numBytesWrittenPerQueryId = type
          .nameToId("QueryNode_NumBytesWrittenPerExecution");
      qn_writeResultSetDataNodeTimeId = type
          .nameToId("QueryNode_DataNodeRSWriteTime");

*/
