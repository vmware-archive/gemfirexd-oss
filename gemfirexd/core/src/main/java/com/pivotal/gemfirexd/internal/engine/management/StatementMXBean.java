package com.pivotal.gemfirexd.internal.engine.management;

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

/**
 * Interface for Statement
 * 
 * @author ajayp
 * 
 */
public interface StatementMXBean {

  /**
   * For query node, Number of times this statement is compiled (including
   * recompilations)
   * 
   */
  long getQNNumTimesCompiled();

  /**
   * For query node, Number of times this statement is executed
   */
  long getQNNumExecutions();

  /**
   * For query node, Statements that are actively being processed
   */
  long getQNNumExecutionsInProgress();

  /**
   * For query node, Number of times global index lookup message exchanges
   * occurred
   * 
   */
  long getQNNumTimesGlobalIndexLookup();

  /**
   * For query node, Number of rows modified by DML operation of
   * insert/delete/update
   * 
   */
  long getQNNumRowsModified();

  /**
   * For query node, Time spent(in milliseconds) in parsing the query string
   * 
   */
  long getQNParseTime();

  /**
   * For query node, Time spent (in milliseconds) mapping this statement with
   * database object's metadata (bind)
   * 
   */
  long getQNBindTime();

  /**
   * For query node, Time spent (in milliseconds) determining the best execution
   * path for this statement (optimize)
   * 
   */
  long getQNOptimizeTime();

  /**
   * For query node, Time spent (in milliseconds) compiling details about
   * routing information of query strings to data node(s) (processQueryInfo)
   * 
   */
  long getQNRoutingInfoTime();

  /**
   * For query node, Time spent (in milliseconds) to generate query execution
   * plan definition (activation class)
   * 
   */
  long getQNGenerateTime();

  /**
   * For query node, Total compilation time (in milliseconds) of the statement
   * on this node (prepMinion)
   * 
   */
  long getQNTotalCompilationTime();

  /**
   * For query node, Time spent (in nanoseconds) in creation of all the layers
   * of query processing (ac.execute)
   * 
   */
  long getQNExecuteTime();

  /**
   * For query node, Time to apply (in nanoseconds) the projection and
   * additional filters
   * 
   */
  long getQNProjectionTime();

  /**
   * For query node, Total execution time (in nanoseconds) taken to process the
   * statement on this node (execute/open/next/close)
   * 
   */
  long getQNTotalExecutionTime();

  /**
   * For query node, Time taken (in nanoseconds) to modify rows by DML operation
   * of insert/delete/update
   * 
   */
  long getQNRowsModificationTime();

  /**
   * Number of rows returned after applying projection at different stages of
   * processing rowcounts
   * 
   */
  long getNumProjectedRows();

  /**
   * Number of rows output by nested loop joins of the query
   * 
   */
  long getNumNLJoinRowsReturned();

  /**
   * Number of rows output by nested loop joins of the query
   * 
   */
  long getNumHASHJoinRowsReturned();

  /**
   * Number of rows returned from all the involved table scans
   * 
   */
  long getNumTableRowsScanned();

  /**
   * Number of rows returned from a hash scan or distinct scan
   * 
   */
  long getNumRowsHashScanned();

  /**
   * Number of rows returned from all the involved index scans
   * 
   */
  long getNumIndexRowsScanned();

  /**
   * Number of rows that is sorted due to group by/order by clauses
   * 
   */
  long getNumRowsSorted();

  /**
   * Number of rows overflowed to temporary files due to sorting
   * 
   */
  long getNumSortRowsOverflowed();

  /**
   * Number of times the prepared statement executed locally due to single hop
   * 
   */
  long getNumSingleHopExecutions();

  /**
   * Number of rows seen while subquery execution (by parent query)
   * 
   */
  long getSubQueryNumRowsSeen();

  /**
   * Message deserialization time including parameters but excluding
   * MsgProcessTime
   * 
   */
  long getMsgDeSerTime();

  /**
   * Message process time, in nanoseconds, including local data set creation,
   * query execute and response sending time
   * 
   */
  long getMsgProcessTime();

  /**
   * Time, in nanoseconds, to iterate through determined set of results on this
   * node
   * 
   */
  long getResultIterationTime();

  /**
   * Response, in nanoseconds, message serialization time excluding
   * ResultIterationTime
   * 
   */
  long getRespSerTime();

  /**
   * Response, in nanoseconds, throttle wait time on data node
   * 
   */
  long getThrottleTime();

  /**
   * Time, in nanoseconds, to process all the nested loop join relations of the
   * query
   * 
   */
  long getNLJoinTime();

  /**
   * Time, in nanoseconds, to process all the hash join relations of the query
   * 
   */
  long getHASHJoinTime();

  /**
   * Time, in nanoseconds, taken to scan all the tables involved in the query
   * 
   */
  long getTableScanTime();

  /**
   * Time, in nanoseconds, taken to build a hash table for distinct evaluation
   * or other optimization purpose
   * 
   */
  long getHashScanTime();

  /**
   * Time, in nanoseconds, taken to scan all the indexes involved in the query
   * 
   */
  long getIndexScanTime();

  /**
   * Time, in nanoseconds, taken to sort rows while applying group by or order
   * by clause.
   * 
   */
  long getSortTime();

  /**
   * Time, in nanoseconds, taken to scan rows from Subquery (by parent query)
   * 
   */
  long getSubQueryExecutionTime();

  /**
   * gets statement
   * 
   */
  String retrievString();

  /**
   * For data node, Number of times this statement is compiled (including
   * recompilations)
   * 
   */
  long getDNNumTimesCompiled();

  /**
   * For data node, Number of times this statement is executed
   */
  long getDNNumExecution();

  /**
   * For data node, Statements that are actively being processed
   */
  long getDNNumExecutionsInProgress();

  /**
   * For data node, Number of times global index lookup message exchanges
   * occurred
   * 
   */
  long getDNNumTimesGlobalIndexLookup();

  /**
   * For data node, Number of rows modified by DML operation of
   * insert/delete/update
   * 
   */
  long getDNNumRowsModified();

  /**
   * For data node, Time spent(in milliseconds) in parsing the query string
   * 
   */
  long getDNParseTime();

  /**
   * For data node, Time spent (in milliseconds) mapping this statement with
   * database object's metadata (bind)
   * 
   */
  long getDNBindTime();

  /**
   * For data node, Time spent (in milliseconds) determining the best execution
   * path for this statement (optimize)
   * 
   */
  long getDNOptimizeTime();

  /**
   * For data node, Time spent (in milliseconds) compiling details about routing
   * information of query strings to data node(s) (processQueryInfo)
   * 
   */
  long getDNRoutingInfoTime();

  /**
   * For data node, Time spent (in milliseconds) to generate query execution
   * plan definition (activation class)
   * 
   */
  long getDNGenerateTime();

  /**
   * For data node, Total compilation time (in milliseconds) of the statement on
   * this node (prepMinion)
   * 
   */
  long getDNTotalCompilationTime();

  /**
   * For data node, Time spent (in nanoseconds) in creation of all the layers of
   * query processing (ac.execute)
   * 
   */
  long getDNExecutionTime();

  /**
   * For data node, Time to apply (in nanoseconds) the projection and additional
   * filters
   * 
   */
  long getDNProjectionTime();

  /**
   * For data node, Total execution time (in nanoseconds) taken to process the
   * statement on this node (execute/open/next/close)
   * 
   */
  long getDNTotalExecutionTime();

  /**
   * For data node, Time taken (in nanoseconds) to modify rows by DML operation
   * of insert/delete/update
   * 
   */
  long getDNRowsModificationTime();

  /**
   * For query node, Number of rows returned from remote nodes (ResultHolder/Get
   * convertibles)
   * 
   */
  long getQNNumRowsSeen();

  /**
   * For query node, TCP send time (in nanoseconds) of all the messages
   * including serialization time and queue wait time
   * 
   */
  long getQNMsgSendTime();

  /**
   * For query node, Serialization time (in nanoseconds) for all the messages
   * while sending to remote node(s)
   * 
   */
  long getQNMsgSerTime();

  /**
   * For query node, Response message deserialization time (in nano seconds )
   * from remote node(s) excluding resultset deserialization
   * 
   */
  long getQNRespDeSerTime();

}