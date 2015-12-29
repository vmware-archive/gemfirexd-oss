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

package com.pivotal.gemfirexd.internal.engine.management;

/**
 * This MBean provide different Statistics aggregate for StatementStats. It only
 * provide details about query node.
 * 
 * This MBean will show only rate between two sampling interval. Sampling
 * interval being DistributionConfig.JMX_MANAGER_UPDATE_RATE_NAME.
 * {jmx-manager-update-rate}. Default interval is 2 secs.
 * 
 * e.g. if Sample:1{NumExecution = 2} & Sample:2 {NumExecution = 5} this MBean
 * will show a value of 3 if queried after Sample :2 & before Sample:3
 * 
 * 
 * @author rishim
 * 
 */
public interface AggregateStatementMXBean {

  /**
   * Number of times this statement is compiled (including recompilations)
   * between two sampling interval.
   * 
   */
  public long getNumTimesCompiled();

  /**
   * Number of times this statement is executed between two sampling interval.
   */
  public long getNumExecution();

  /**
   * Statements that are actively being processed during the statistics snapshot
   * between two sampling interval.
   */
  public long getNumExecutionsInProgress();

  /**
   * Number of times global index lookup message exchanges occurred between two
   * sampling interval.
   * 
   */
  public long getNumTimesGlobalIndexLookup();

  /**
   * Number of rows modified by DML operation of insert/delete/update between
   * two sampling interval.
   * 
   */
  public long getNumRowsModified();

  /**
   * Time spent(in milliseconds) in parsing the query string between two
   * sampling interval.
   * 
   */
  public long getParseTime();

  /**
   * Time spent (in milliseconds) mapping this statement with database object's
   * metadata (bind) between two sampling interval.
   * 
   */
  public long getBindTime();

  /**
   * Time spent (in milliseconds) determining the best execution path for this
   * statement (optimize) between two sampling interval.
   * 
   */
  public long getOptimizeTime();

  /**
   * Time spent (in milliseconds) compiling details about routing information of
   * query strings to data node(s) (processQueryInfo) between two sampling interval.
   * 
   */
  public long getRoutingInfoTime();

  /**
   * Time spent (in milliseconds) to generate query execution plan definition
   * (activation class) between two sampling interval.
   * 
   */
  public long getGenerateTime();

  /**
   * Total compilation time (in milliseconds) of the statement on this node
   * (prepMinion) between two sampling interval.
   * 
   */
  public long getTotalCompilationTime();

  /**
   * Time spent (in nanoseconds) in creation of all the layers of query
   * processing (ac.execute) between two sampling interval.
   * 
   */
  public long getExecutionTime();

  /**
   * Time to apply (in nanoseconds) the projection and additional filters
   * between two sampling interval.
   * 
   */
  public long getProjectionTime();

  /**
   * Total execution time (in nanoseconds) taken to process the statement on
   * this node (execute/open/next/close) between two sampling interval.
   * 
   */
  public long getTotalExecutionTime();

  /**
   * Time taken (in nanoseconds) to modify rows by DML operation of
   * insert/delete/update between two sampling interval.
   * 
   */
  public long getRowsModificationTime();

  /**
   * Number of rows returned from remote nodes (ResultHolder/Get convertibles)
   * between two sampling interval.
   * 
   */
  public long getQNNumRowsSeen();

  /**
   * TCP send time (in nanoseconds) of all the messages including serialization
   * time and queue wait time between two sampling interval.
   * 
   */
  public long getQNMsgSendTime();

  /**
   * Serialization time (in nanoseconds) for all the messages while sending to
   * remote node(s) between two sampling interval.
   * 
   */
  public long getQNMsgSerTime();

  /**
   * Response message deserialization time (in nano seconds ) from remote
   * node(s) excluding resultset deserialization between two sampling interval.
   * 
   */
  public long getQNRespDeSerTime();

}
