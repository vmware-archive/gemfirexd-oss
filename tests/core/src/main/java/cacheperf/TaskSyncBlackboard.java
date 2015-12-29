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

package cacheperf;

//import hydra.*;
import hydra.blackboard.*;

/**
 *
 *  Manages the blackboard used to synchronize client threads for a task.
 *  Defines counters used to synchronize clients executing particular tasks.
 *  Also provides a signal that can be used by a clients to signal others.
 *
 */

public class TaskSyncBlackboard extends Blackboard {

  public static final String RECOVERY_KEY = "recovery";

  public static int testTaskNoTxn;
  public static int testTaskTxn;
  public static int createDataTask;
  public static int createSqlDataTask;
  public static int createKeysTask;
  public static int createQueryDataTask;
  public static int directGetDataTask;
  public static int putDataTask;
  public static int putDataComparisonTask;
  public static int getDataTask;
  public static int getDataComparisonTask;
  public static int getRecentKeyDataTask;
  public static int mixCreateGetDataTask;
  public static int mixPutGetDataTask;
  public static int combinePutGetDataTask;
  public static int destroyDataTask;
  public static int updateEqStructTask;
  public static int feedDataTask;
  public static int statArchiverTask;
  public static int cycleDistributedSystemConnectionOnlyTask;
  public static int cycleDistributedSystemConnectionTask;
  public static int cycleDurableClientTask;
  public static int cyclePoolAndRegionTask;
  public static int cyclePoolTask;
  public static int putDataAndSyncTask;
  public static int putDataGatewayTask;
  public static int putDataGWSenderTask;
  public static int cycleGatewayHubConnectionTask;
  public static int cycleCQsTask;
  public static int cycleLockTask;
  public static int cycleRegisterInterestRegexTask;
  public static int queryTask;
  public static int queryDataTask;
  public static int querySqlDataTask;
  public static int queryRegionDataTask;
  public static int queryRangeRegionDataTask;
  public static int queryQueryDataTask;
  public static int preparedCreateQueryDataTask;
  public static int preparedDeleteQueryDataTask;
  public static int preparedMixQueryQueryDataTask;
  public static int preparedQueryQueryDataTask;
  public static int preparedUpdateQueryDataTask;
  public static int preparedMixUpdateAndQueryQueryDataTask;
  public static int requestServerLocationTask;
  public static int createAllDataTask;
  public static int getAllDataTask;
  public static int putAllTask;
  public static int putAllDataTask;
  public static int putAllEntryMapTask;
  public static int getExtraDataTask;
  public static int putExtraDataTask;
  public static int addDataHostSignal;
  public static int trimSignal;
  public static int signal;
  public static int executions;
  public static int queryContextTask;
  public static int updateDataTask;
  public static int updateQueryDataTask;
  public static int entryOpsAndQueryTask;
  public static int entryOpsTask;
  // memcached (see https://svn.gemstone.com/repos/utilities/trunk/hydraExt)
  public static int createMemcachedDataTask;
  public static int putMemcachedDataTask;

  public static int updateDeltaDataTask;
  public static int getMemcachedDataTask;

  // added for distributed locking perf test support (tests/dlock)
  public static int lockTask;
  public static int unlockTask;
  public static int queryUseCase3DataTask;
  public static int startLocatorAndDSTask;
  public static int getFunctionExecutionDataTask;
  public static int putFunctionExecutionDataTask;

  public static int executeTPCCTransactionsTask;
  public static int loadStockDataTask;
  public static int delivGetCustIdTask;
  public static int delivGetOrderIdTask;
  public static int delivSumOrderAmountTask;
  public static int ordStatCountCustTask;
  public static int ordStatGetCustTask;
  public static int ordStatGetNewestOrdTask;
  public static int payCursorCustByNameTask;
  public static int payGetDistTask;
  public static int payGetWhseTask;
  public static int payUpdateWhseTask;
  public static int stmtGetCustWhseTask;
  public static int stockGetCountStockTask;

  public static int useCase5InsertTask;
  public static int useCase1WorkloadTask;
  public static int sellCashBetsTask;
  public static int payCashBetsTask;
  public static int executeBenchmarkTask;
  public static int runQueriesTask;
  
  public static int selectQueryTask;
  
  public static int HydraTask_runTPCETxns;
  public static int HydraTask_runTPCHQueries;

  public static int ops;    // generic "stat" for statless clients
  public static int opTime; // generic "stat" for statless clients
  public static int opTime1; // generic "stat" for statless clients
  public static int opTime2; // generic "stat" for statless clients
  public static int opTime3; // generic "stat" for statless clients

  private static TaskSyncBlackboard blackboard;

  /**
   *  Zero-arg constructor for remote method invocations.
   */
  public TaskSyncBlackboard() {
  }
  /**
   *  Creates a blackboard using the specified name and transport type.
   */
  public TaskSyncBlackboard( String name, String type ) {
    super( name, type, TaskSyncBlackboard.class );
  }
  /**
   *  Creates a blackboard.
   */
  public static synchronized TaskSyncBlackboard getInstance() {
    if (blackboard == null) {
      blackboard = new TaskSyncBlackboard("TaskSyncBlackboard", "rmi");
    }
    return blackboard;
  }
}
