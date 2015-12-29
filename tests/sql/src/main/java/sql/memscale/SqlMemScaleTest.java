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
 * 
 */
package sql.memscale;

import hydra.GsRandom;
import hydra.HydraInternalException;
import hydra.HydraRuntimeException;
import hydra.HydraThreadLocal;
import hydra.Log;
import hydra.MasterController;
import hydra.RemoteTestModule;
import hydra.StopSchedulingOrder;
import hydra.TestConfig;
import hydra.blackboard.Blackboard;
import hydra.blackboard.SharedCounters;
import hydra.gemfirexd.FabricServerHelper;
import hydra.gemfirexd.GatewayReceiverHelper;
import hydra.gemfirexd.GatewaySenderHelper;
import hydra.gemfirexd.GatewaySenderPrms;
import hydra.gemfirexd.NetworkServerHelper;
import hydra.gemfirexd.NetworkServerHelper.Endpoint;

import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Vector;

import memscale.MemScaleBB;
import memscale.OffHeapHelper;
import util.RandomValues;
import util.TestException;
import util.TestHelper;
import cacheperf.CachePerfClient;

import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl;

/**
 * @author lynng
 *
 */
public class SqlMemScaleTest {

  // thread locals
  private static HydraThreadLocal threadLocal_clientConnection = new HydraThreadLocal();
  private static HydraThreadLocal threadLocal_sequencer = new HydraThreadLocal();
  private static HydraThreadLocal threadLocal_isLeader = new HydraThreadLocal();
  private static HydraThreadLocal threadLocal_currentTaskStep = new HydraThreadLocal();
  private static HydraThreadLocal threadLocal_insertPreparedStatements = new HydraThreadLocal();

  // blackboard keys
  private static final String DESIGNATED_THIN_CLIENT_THREAD_KEY = "thinClientThrIdForWanSite_";
  private static final String DESIGNATED_SERVER_THREAD_KEY = "serverThrIdForWanSite_";
  private static final Object TABLES_KEY = "tables";

  // constants
  private static final String SCHEMA_NAME = "fragTest";
  private static final String INSERT_STEP = "insert";
  private static final String DELETE_STEP = "delete";
  private static final boolean LOG_TASK_STATEMENTS = true;

  /** Hydra task to create a locator
   * 
   */
  public static void HydraTask_createLocatorTask() {
    FabricServerHelper.createLocator();
  }

  /** Hydra task to start a locator
   * 
   */
  public static void HydraTask_startLocatorTask() {
    FabricServerHelper.startLocator("networkServer");
  }

  /** Hydra task to start a fabric server
   * 
   */
  public static void HydraTask_startFabricServer() {
    FabricServerHelper.startFabricServer();
  }

  /** Hydra task to start a network server
   * 
   */
  public static void HydraTask_startNetworkServers() {
    NetworkServerHelper.startNetworkServers("networkServer");
  }

  /** Task to sleep for a number of seconds, specified by the hydra parameter
   *  SqlMemScalePrms.sleepSec.
   */
  public static void HydraTask_sleep() {
    int sleepSec = SqlMemScalePrms.getSleepSec();
    int sleepMs = sleepSec * 1000;
    Log.getLogWriter().info("Sleeping for " + sleepSec + " seconds");
    MasterController.sleepForMs(sleepMs);
  }

  /** Write hydra thread ids to the blackboard as follows: write one thin client thread 
   *  id per wan site and one server thread id per wan site.
   */
  public static void HydraTask_setDesignatedThreads() {
    String clientName = RemoteTestModule.getMyClientName();
    int myWanSiteNumber = getMyWanSiteNumber();
    if (clientName.contains("client")) {
      String key = DESIGNATED_THIN_CLIENT_THREAD_KEY + myWanSiteNumber;
      SqlMemScaleBB.getBB().getSharedMap().put(key, RemoteTestModule.getCurrentThread().getThreadId());
    } else if (clientName.contains("server")) {
      String key = DESIGNATED_SERVER_THREAD_KEY + myWanSiteNumber;
      SqlMemScaleBB.getBB().getSharedMap().put(key, RemoteTestModule.getCurrentThread().getThreadId());
    } else {
      throw new TestException("Unable to designate threads for hydra client " + clientName);
    }
  }

  /** Hydra task to create a thin client connection to a locator in this wan site
   * 
   * @throws SQLException Thrown if the connection attempt fails. 
   */
  public static void HydraTask_connectThinClientToLocator() throws SQLException {
    List<Endpoint> endpoints = NetworkServerHelper.getNetworkLocatorEndpointsInWanSite();
    Endpoint locEndpoint = endpoints.get(TestConfig.tab().getRandGen().nextInt(endpoints.size()-1));
    String connectStr = "jdbc:gemfirexd://" + locEndpoint.getHost() + ":" + locEndpoint.getPort();
    Properties props = new Properties();
    loadDriver();
    Log.getLogWriter().info("Connecting to locator with: " + connectStr + " and properties: " + props);
    Connection conn = DriverManager.getConnection(connectStr, props);
    Log.getLogWriter().info("Connection " + conn + " connected to locator with " + locEndpoint);
    threadLocal_clientConnection.set(conn);
  }

  /** Load the jdbc driver to use when connecting
   * 
   */
  private static void loadDriver() {
    try {
      Class.forName("com.pivotal.gemfirexd.jdbc.ClientDriver");
    } catch (ClassNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  /** Hydra task to create tables. This executes the create table sql
   *  generated by HydraTask_generateCreateTableSql().
   * 
   */
  public synchronized static void HydraTask_createTables() {
    if (thisThreadIsDesignated()) {
      createTables();
    }
  }

  /** Hydra task to generate the sql for creating tables, but this task does not execute the sql.
   * 
   */
  public synchronized static void HydraTask_generateTableDefinitions() {
    generateTableDefinitions();
  }

  /** Initialize prepared statements for inserts (in the thin clients)
   * 
   */
  public static void HydraTask_createPreparedStatements() {
    List<TableDefinition> tableList = (List<TableDefinition>) SqlMemScaleBB.getBB().getSharedMap().get(TABLES_KEY);
    if (tableList == null) {
      throw new TestException("Test problem, no tableList defined, this task should execute after HydraTask_generateTableDefinitions");
    }
    threadLocal_insertPreparedStatements.set(createPreparedInsertStmtMap(tableList));
  }

  /** Hydra task to insert data, run off-heap memory validation, then delete a certain
   *  percentage of date, and run off-heap memory validation again. This causes off-heap
   *  fragmentation.
   *  
   *  This task is run by thin clients.
   */
  public static void HydraTask_insertDelete() {
    // Create a NumberSequencer for all threads
    int numThreads = CachePerfClient.numThreads();
    NumberSequencer mySequencer = (NumberSequencer) threadLocal_sequencer.get();
    if (mySequencer == null) {
      mySequencer = new NumberSequencer(SqlMemScalePrms.getNumRowsPerTable(), numThreads);
    }
    Log.getLogWriter().info("NumberSequencer: " + mySequencer);

    // determine if this thread is the leader; if so this thread is leader for the duration of the test
    Object value = threadLocal_isLeader.get();
    boolean isLeader = false;
    if (value == null) {
      long leaderCounter = SqlMemScaleBB.getBB().getSharedCounters().incrementAndRead(SqlMemScaleBB.leader);
      isLeader = (leaderCounter == 1);
      threadLocal_isLeader.set(isLeader);
    } else {
      isLeader = (Boolean)value;
    }
    Log.getLogWriter().info("isLeader: " + isLeader);

    // initialize which step we are on (insert or delete)
    List<TableDefinition> tableList = (List<TableDefinition>) SqlMemScaleBB.getBB().getSharedMap().get(TABLES_KEY);
    String currentTaskStep = (String)(threadLocal_currentTaskStep.get());
    if (currentTaskStep == null) {
      currentTaskStep = INSERT_STEP;
      threadLocal_currentTaskStep.set(currentTaskStep);
      if (isLeader) {
        SqlMemScaleBB.getBB().getSharedCounters().increment(SqlMemScaleBB.executionNumber);
      }
    }
    if (isLeader) {
      Log.getLogWriter().info("Execution number is " + SqlMemScaleBB.getBB().getSharedCounters().read(SqlMemScaleBB.executionNumber));
    }

    // do the inserts or deletes; for larger scale tests the task doesn't need to complete all
    // inserts or deletes in one task execution as this task will pick up from where it left off
    // to continue the inserts or deletes.
    if (currentTaskStep.equals(INSERT_STEP)) {
      Log.getLogWriter().info("Inserting rows...");
      boolean finished = doDmlOps(true, tableList, mySequencer);
      if (finished) {
        pause(isLeader, "pause1", new String[] {"pause3"}, numThreads); // pause the clients
        checkForLastIteration();
        directServersToVerifyOffHeapMemory(isLeader);
        mySequencer.setStopPercentage(30);
        mySequencer.setRandomStart(); // get a new starting point for the upcoming delete step
        pause(isLeader, "pause2", new String[] {"pause4"}, numThreads);
        Log.getLogWriter().info("Changing to delete step");
        threadLocal_currentTaskStep.set(DELETE_STEP);
        if (isLeader) {
          SqlMemScaleBB.getBB().getSharedCounters().increment(SqlMemScaleBB.executionNumber);
        }
      }
    } else if (currentTaskStep.equals(DELETE_STEP)) {
      Log.getLogWriter().info("Deleting rows...");
      boolean finished = doDmlOps(false, tableList, mySequencer);
      if (finished) {
        pause(isLeader, "pause3", new String[] {"pause1"}, numThreads);
        checkForLastIteration();
        directServersToVerifyOffHeapMemory(isLeader);
        mySequencer.reset(); // replay the same sequence for the upcoming insert step
        pause(isLeader, "pause4", new String[] {"pause2"}, numThreads);
        Log.getLogWriter().info("Changing to insert step");
        threadLocal_currentTaskStep.set(INSERT_STEP);
        if (isLeader) {
          SqlMemScaleBB.getBB().getSharedCounters().increment(SqlMemScaleBB.executionNumber);
        }
      }
    }
    threadLocal_sequencer.set(mySequencer);

    // see if it's time to stop the test
    long counter = SqlMemScaleBB.getBB().getSharedCounters().read(SqlMemScaleBB.timeToStop);
    if (counter >= 1) {
      throw new StopSchedulingOrder("Num controller executions is " +
          SqlMemScaleBB.getBB().getSharedCounters().read(SqlMemScaleBB.executionNumber));
    }

  }

  /** Direct the servers to verify off-heap memory through blackboard counters
   *  This returns when all servers have completed the verify.
   *  
   * @param isLeader Thread that drives the test. Only one thread should be the leader.
   */
  private static void directServersToVerifyOffHeapMemory(boolean isLeader) {
    if (isLeader) { // direct the servers to verify offHeapMemory
      Log.getLogWriter().info("Zeroing MemScaleBB.finishedMemCheck");
      MemScaleBB.getBB().getSharedCounters().zero(MemScaleBB.finishedMemCheck);
      Log.getLogWriter().info("Leader thread is directing servers to verify off-heap memory");
      long counter = SqlMemScaleBB.getBB().getSharedCounters().incrementAndRead(SqlMemScaleBB.verifyOffHeapMemory);
      Log.getLogWriter().info("SqlMemScaleBB.verifyOffHeapMemory is now " + counter);
      // wait for the servers to complete verification
      TestHelper.waitForCounter(SqlMemScaleBB.getBB(), "SqlMemScaleBB.verifyOffHeapMemory", SqlMemScaleBB.verifyOffHeapMemory,
          0, true, -1, 1000);
    }
  }

  /** Check if we have run for the desired length of time. We cannot use 
   *  hydra's taskTimeSec parameter because of a small window of opportunity 
   *  for the test to hang due to the test's "concurrent round robin" type 
   *  of strategy. Here we set a blackboard counter if time is up and this
   *  is the last concurrent round.
   */
  protected static void checkForLastIteration() {
    // determine if this is the last iteration
    int secondsToRun = SqlMemScalePrms.getSecondsToRun();
    long taskStartTime = 0;
    final String START_KEY = "taskStartTime";
    Object anObj = SqlMemScaleBB.getBB().getSharedMap().get(START_KEY);
    if (anObj == null) {
      taskStartTime = System.currentTimeMillis();
      SqlMemScaleBB.getBB().getSharedMap().put(START_KEY, new Long(taskStartTime));
      Log.getLogWriter().info("Initialized taskStartTime to " + taskStartTime);
    } else {
      taskStartTime = ((Long)anObj).longValue();
    }
    if (System.currentTimeMillis() - taskStartTime >= secondsToRun * 1000) {
      Log.getLogWriter().info("This is the last iteration of this task");
      SqlMemScaleBB.getBB().getSharedCounters().increment(SqlMemScaleBB.timeToStop);
    } else {
      Log.getLogWriter().info("Running for " + secondsToRun + " seconds; time remaining is " +
          (secondsToRun - ((System.currentTimeMillis() - taskStartTime) / 1000)) + " seconds");
    }
  }

  /** Verify off-heap memory when the designated blackboard counter indicates it is time to do so.
   *  This task is to be run on the servers.
   */
  public static void HydraTask_verifyOffHeapMemory() {
    // wait for the verifyOffHeapMemory counter to become 1 signaling it is time to do off-heap memory validation
    // if we don't detect a counter value of 1 in a certain amount of time, then return from this task
    final int WAIT_SEC = 60;
    try {
      TestHelper.waitForCounter(SqlMemScaleBB.getBB(), "SqlMemScaleBB.verifyOffHeapMemory", SqlMemScaleBB.verifyOffHeapMemory,
          1, true, (WAIT_SEC * 1000), 1000);
    } catch (TestException e) {
      Log.getLogWriter().info("Did not receive directive to verify off-heap memory in " + WAIT_SEC + " seconds, returning");
      return;
    } finally {
      Log.getLogWriter().info("Zeroing MemScaleBB.finishedMemCheck");
      MemScaleBB.getBB().getSharedCounters().zero(MemScaleBB.finishedMemCheck);
    }

    // do the verify step; waiting for silence waits for the wan queues to drain
    OffHeapHelper.waitForOffHeapSilence(15);
    OffHeapHelper.verifyOffHeapMemoryConsistencyOnce();

    // wait for all server threads to finish off-heap verify before zeroing the verifyOffHeapMemoryCounter
    TestHelper.waitForCounter(MemScaleBB.getBB(), "MemScaleBB.finishedMemCheck", MemScaleBB.finishedMemCheck,
        CachePerfClient.numThreads(), true, -1, 100);
    // let the thin client threads know that off-heap memory validation has completed
    Log.getLogWriter().info("Zeroing SqlMemScaleBB.verifyOffHeapMemory");
    SqlMemScaleBB.getBB().getSharedCounters().zero(SqlMemScaleBB.verifyOffHeapMemory);
  }

  /** This task does two things:
   *     1) periodically force an off-heap compaction
   *     2) verify off-heap memory when the designated blackboard counter indicates it is time to do so.
   *  This task is to be run on the servers.
   */
  public static void HydraTask_serverTask() {
    // wait for the verifyOffHeapMemory counter to become 1 signaling it is time to do off-heap memory validation
    // if we don't detect a counter value of 1 in a certain amount of time, then return from this task
    final int WAIT_SEC = SqlMemScalePrms.getCompactionIntervalSec();
    try {
      TestHelper.waitForCounter(SqlMemScaleBB.getBB(), "SqlMemScaleBB.verifyOffHeapMemory", SqlMemScaleBB.verifyOffHeapMemory,
          1, true, (WAIT_SEC * 1000), 1000);
    } catch (TestException e) {
      Log.getLogWriter().info("Did not receive directive to verify off-heap memory in " + WAIT_SEC + " seconds, returning");
      return;
    } finally {
      if (OffHeapHelper.isOffHeapMemoryConfigured()) {
        forceOffHeapCompaction();
      }
    }

    // do the verify step; waiting for silence waits for the wan queues to drain
    OffHeapHelper.waitForGlobalOffHeapSilence();
    OffHeapHelper.verifyOffHeapMemoryConsistencyOnce();

    // wait for all server threads to finish off-heap verify before zeroing the verifyOffHeapMemoryCounter
    TestHelper.waitForCounter(MemScaleBB.getBB(), "MemScaleBB.finishedMemCheck", MemScaleBB.finishedMemCheck,
        CachePerfClient.numThreads(), true, -1, 100);
    // let the thin client threads know that off-heap memory validation has completed
    Log.getLogWriter().info("Zeroing SqlMemScaleBB.verifyOffHeapMemory");
    SqlMemScaleBB.getBB().getSharedCounters().zero(SqlMemScaleBB.verifyOffHeapMemory);
  }

  /** Pause all threads executing this task, return when all threads are paused.
   * 
   * @param isLeader If true, this thread is the concurrent leader which controls each step of the test (including pausing!)
   * @param pauseCounterName The name of the shared counter on the blackboard to use for pausing.
   * @param counterNamesToZero The name any counters to reset to zero when the leader determines all threads have paused
   *                           (this provides a safe point for zeroing counters)
   * @param counterTarget The target counter value wait for to pause.
   */
  private static void pause(boolean isLeader, String pauseCounterName, String[] counterNamesToZero, int counterTarget) {
    Blackboard bb = SqlMemScaleBB.getBB();
    SharedCounters sc = bb.getSharedCounters();
    if (isLeader) {
      // the leader waits for all OTHER threads to pause (but not itself, the leader has not yet incremented the counter)
      TestHelper.waitForCounter(bb, pauseCounterName, bb.getSharedCounter(pauseCounterName), counterTarget-1, true, -1,1000);
      // all threads have paused
      // zero the given counters (if any)
      if (counterNamesToZero != null) {
        for (String counterToZero: counterNamesToZero) {
          Log.getLogWriter().info("Zeroing " + counterToZero);
          sc.zero(bb.getSharedCounter(counterToZero));
        }
      }
      // now increment our counter, which allows us (the leader) and all other threads to return
      sc.increment(bb.getSharedCounter(pauseCounterName));
    } else {
      // we are not the leader, increment that we are pausing, then wait for ALL threads (including the leader) to indicated they have paused
      sc.increment(bb.getSharedCounter(pauseCounterName));
      TestHelper.waitForCounter(bb, pauseCounterName, bb.getSharedCounter(pauseCounterName), counterTarget, true, -1, 1000);
    }
  }

  /** Do DML ops for a time period before returning.
   * 
   * @param doInserts If true, then DML ops are inserts, if false then DML ops are destroys
   * @param tableList The List of information about the tables to operate on.
   * @param sequencer A NumberSequencer to stream a series of primary key indexes.
   * @return true if all DML ops were completed, false otherwise.
   */
  private static boolean doDmlOps(boolean doInserts, List<TableDefinition> tableList, NumberSequencer sequencer) {
    final int MS_TO_RUN = 60000;
    long startTime = System.currentTimeMillis();
    do {
      int sequenceNum = sequencer.next();
      Log.getLogWriter().info("Obtained sequence number: " + sequenceNum);
      if (sequenceNum == -1) { // done with all sequence numbers
        return true;
      } else {
        if (doInserts) {
          insertIntoAllTables(tableList, sequenceNum);
        } else {
          deleteFromAllTables(tableList, sequenceNum);
        }
      }
    } while (System.currentTimeMillis() - startTime < MS_TO_RUN);
    return false;
  }

  /** Force an off-heap memory compaction
   * 
   */
  private static void forceOffHeapCompaction() {
    Log.getLogWriter().info("Test is forcing off-heap memory compaction...");
    long startCompactionTime = System.currentTimeMillis();
    SimpleMemoryAllocatorImpl.forceCompaction();
    long duration = System.currentTimeMillis() - startCompactionTime;
    Log.getLogWriter().info("Off-heap memory compaction completed in " + duration + " ms");
  }

  /** Given a primary key index, delete that primary key's row from all tables
   * 
   * @param tableList The List of tables to delete from.
   * @param pkIndex The index of the primary key to delete.
   */
  private static void deleteFromAllTables(List<TableDefinition> tableList, int pkIndex) {
    for (TableDefinition table: tableList) {
      StringBuilder stmt = new StringBuilder();
      String pk = getPrimaryKeyForIndex(pkIndex);
      stmt.append("DELETE FROM " + table.getFullTableName() + 
          " WHERE pKey = '" + getPrimaryKeyForIndex(pkIndex) + "'");
      executeSqlStatement(stmt.toString(), LOG_TASK_STATEMENTS);
    }
  }

  /** Given an index, return the primary key value for it
   * 
   * @param pkIndex A primary key index used by the test (an int), to be converted
   *                to the actual primary key value (a String).
   * @return The primary key value for pkIndex.
   */
  private static String getPrimaryKeyForIndex(int pkIndex) {
    return "PK_" + pkIndex;
  }

  /** Given a primary key index, insert a row with that primary key into all tables
   * 
   * @param tableList The List of tables to insert into.
   * @param pkIndex The index of the primary key to insert.
   */
  private static void insertIntoAllTables(List<TableDefinition> tableList, int pkIndex) {
    GsRandom rand = TestConfig.tab().getRandGen();
    RandomValues rv = new RandomValues();
    RandomValues.setPrintableChars(true);
    for (TableDefinition table: tableList) {
      Map<String, List> aMap = (Map<String, List>) threadLocal_insertPreparedStatements.get();
      List aList = aMap.get(table.getFullTableName());
      String stmt = (String) aList.get(0);
      CallableStatement preparedStmt = null;
      boolean closePreparedStatement = false;
      if (SqlMemScalePrms.getReusePreparedStatements()) { // reuse prepared statement from the map
        preparedStmt = (CallableStatement) aList.get(1);
        Log.getLogWriter().info("Reusing prepared statement " + preparedStmt + " for " + stmt);
      } else { // get a new prepared statement each insert
        Connection conn = (Connection) threadLocal_clientConnection.get();
        try {
          preparedStmt = conn.prepareCall(stmt);
          Log.getLogWriter().info("Creating a new prepared statement " + preparedStmt + " for " + stmt);
        } catch (SQLException e) {
          throw new TestException(TestHelper.getStackTrace(e));
        }
        closePreparedStatement = true; // close the prepared statement if creating a new one each insert
      }

      // create a list of arguments for the prepared statement
      List<Object> preparedStmtArgs = new ArrayList<Object>();
      try {
        int numColumns = table.getNumColumns();
        for (int i = 0; i < numColumns; i++) {
          String dataType = table.getColumnType(i);
          if (dataType.equalsIgnoreCase("INTEGER")) {
            preparedStmtArgs.add(pkIndex);
          } else if (dataType.equalsIgnoreCase("VARCHAR")) {
            if (i == 0) { // primary key
              preparedStmtArgs.add(getPrimaryKeyForIndex(pkIndex));
            } else {
              String lengthStr = table.getColumnLength(i);
              int maxLength = Integer.valueOf(lengthStr);
              int desiredLength = maxLength;
              if (rand.nextInt(1, 100) <= 80) { // random length sometimes
                desiredLength = rand.nextInt(1, maxLength);
              }
              String value = rv.getRandom_String('\'', desiredLength);
              preparedStmtArgs.add(value);
            }
          } else if (dataType.equalsIgnoreCase("CLOB")) {
            // get a Clob object
            String clobLength = table.getColumnLength(i);
            long numClobBytes = getNumBytes(clobLength);
            int firstThird = (int) (numClobBytes * 0.33);
            int secondThird = (int) (numClobBytes * 0.66);
            long desiredBytes = 0;
            int randInt = rand.nextInt(1, 100);
            if (randInt <= 25) { // use a value from the first third
              desiredBytes = rand.nextLong(1, firstThird);
            } else if (randInt <= 50) { // use a value from the second third
              desiredBytes = rand.nextLong(firstThird+1, secondThird);
            } else if (randInt <= 75) { // use a value from the last third
              desiredBytes = rand.nextLong(secondThird+1, numClobBytes);
            } else { // use the full size
              desiredBytes = numClobBytes;
            }
            Connection conn = (Connection) threadLocal_clientConnection.get();
            Clob clobObj;
            try {
              clobObj = conn.createClob();
              Log.getLogWriter().info("Creating CLOB of size " + desiredBytes + " for a CLOB field of size " + clobLength);
              clobObj.setString(1, rv.getRandom_String('\'', desiredBytes));
            } catch (SQLException e) {
              throw new TestException(TestHelper.getStackTrace(e));
            }
            preparedStmtArgs.add(clobObj);
          } else {
            throw new TestException("Test does not currently support dataType " + dataType);
          }
        } // done iterating all columns for this table
        
        // execute the prepared statement
        executePreparedStatement(preparedStmt, stmt, preparedStmtArgs, LOG_TASK_STATEMENTS);
        if (closePreparedStatement) {
          try {
            preparedStmt.close();
            Log.getLogWriter().info("Closed prepared statement " + preparedStmt);
          } catch (SQLException e) {
            throw new TestException(TestHelper.getStackTrace(e));
          }
        }
      } finally {
        // release the Clob objects in a finally block
        if (preparedStmtArgs.size() > 0) {
          for (Object stmtArg: preparedStmtArgs) {
            if (stmtArg instanceof Clob) {
              try {
                ((Clob)stmtArg).free();
              } catch (SQLException e) {
                throw new TestException(TestHelper.getStackTrace(e));
              }
            }
          }
        }
      }
    }
  }

  /** Create and return a Map containing insert statements and prepared statements per table.
   * @param tableList A List of TableDefinitions, one per table 
   * @return A Map
   *     key: (String) fully qualified table name
   *     value: (List), at index 0: String insert statement for the table
   *                    at index 1: A prepared statement for the table
   */
  private static Map<String, List> createPreparedInsertStmtMap(List<TableDefinition> tableList) {
    Map<String, List> aMap = new HashMap<String, List>();
    for (TableDefinition table: tableList) {
      // Create the insert statement, suitable for a prepared statement
      StringBuilder stmt = new StringBuilder();
      String fullTableName = table.getFullTableName();
      stmt.append("INSERT INTO " + fullTableName + " (");
      int numColumns = table.getNumColumns();
      for (int i = 0; i < numColumns; i++) {
        stmt.append(table.getColumnName(i));
        if (i != numColumns-1) {
          stmt.append(", ");
        }
      }
      stmt.append(") VALUES (");
      for (int i = 0; i < numColumns; i++) {
        stmt.append("?");
        if (i != numColumns -1) {
          stmt.append(", ");
        }
      } // done iterating all columns for this table
      stmt.append(")");

      // create the prepared statement
      Connection conn = (Connection) threadLocal_clientConnection.get();
      CallableStatement preparedStmt;
      try {
        preparedStmt = conn.prepareCall(stmt.toString());
      } catch (SQLException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }

      // add to the map
      List aList = new ArrayList();
      aList.add(stmt.toString());
      aList.add(preparedStmt);
      aMap.put(fullTableName, aList);
    }
    Log.getLogWriter().info("Created prepared statement map: " + aMap);
    return aMap;
  }

  /** Given a String that is a byte specification in the form nK, nM or nG where n 
   *  is an integer.
   * @param byteSpec A byte specification.
   * @return The total number of bytes specified in byteSpec.
   */
  private static long getNumBytes(String byteSpec) {
    String intStr = "";
    int index = 0;
    while (index < byteSpec.length()) {
      char ch = byteSpec.charAt(index);
      if (Character.isDigit(ch)) {
        intStr = intStr + ch;
      } else {
        break;
      }
      index++;
    }
    int n = Integer.valueOf(intStr);
    String units = byteSpec.substring(index);
    if (units.equalsIgnoreCase("K")) {
      return n * 1024;
    } else if (units.equalsIgnoreCase("M")) {
      return n * (1024 * 1024);
    } else if (units.equalsIgnoreCase("G")) {
      return n * (1024 * 1024 * 1024);
    } else {
      throw new TestException("Test problem: unknown byteSpec " + byteSpec);
    }
  }

  /** Create any disk stores needed by the gateway sender
   * 
   */
  public static void HydraTask_createDiskStores() {
    if (thisThreadIsDesignated()) {
      Vector<String> aVec = TestConfig.tab().vecAt(GatewaySenderPrms.diskStoreName);
      for (String diskStoreName: aVec) {
        executeSqlStatement("CREATE DISKSTORE " + diskStoreName + " AUTOCOMPACT true", true);
      }
    }
  }

  /** Determine if the current thread is a designated thread. Designated threads
   *  are those thread ids written to the blackboard in HydraTask_setDesignatedThreads.
   *  
   * @return True if this thread is a thread id previously written to the hydra blackboard
   *         false otherwise. 
   */
  private static boolean thisThreadIsDesignated() {
    String clientName = RemoteTestModule.getMyClientName();
    int myThreadId = RemoteTestModule.getCurrentThread().getThreadId();
    int thisWanSiteNumber = getMyWanSiteNumber();
    String key = null;
    if (clientName.contains("client")) {
      key = DESIGNATED_THIN_CLIENT_THREAD_KEY + thisWanSiteNumber;
    } else if (clientName.contains("server")) {
      key = DESIGNATED_SERVER_THREAD_KEY + thisWanSiteNumber;
    } else {
      throw new TestException(clientName + " does not have designated threads");
    }
    int designatedThreadId = (Integer) SqlMemScaleBB.getBB().getSharedMap().get(key);
    if (myThreadId == designatedThreadId) {
      Log.getLogWriter().info("This is a designated thread");
      SqlMemScaleBB.getBB().printSharedMap();
      return true;
    }
    return false;
  }

  /** Create gateway senders
   * 
   */
  public static void HydraTask_createGatewaySenders() {
    if (thisThreadIsDesignated()) {
      int wanSite = getMyWanSiteNumber(); // this is the DSID
      List<String> ddls = GatewaySenderHelper.getGatewaySenderDDL(wanSite);
      for (String ddl : ddls) {
        Log.getLogWriter().info("Executing " + ddl);
        executeSqlStatement(ddl, true);
        Log.getLogWriter().info("Executed " + ddl);
      }
    }
  }

  /** Start the gateway senders
   * 
   */
  public static void HydraTask_startGatewaySenders() {
    if (thisThreadIsDesignated()) {
      int wanSite = getMyWanSiteNumber(); // this is the DSID
      executeStoredProcedureOncePerArg("CALL SYS.START_GATEWAYSENDER(?)", GatewaySenderHelper.getGatewaySenderIds(wanSite));
    }
  }

  /** Create gateway receivers
   * 
   */
  public static void HydraTask_createGatewayReceivers() {
    if (thisThreadIsDesignated()) {
      int wanSite = getMyWanSiteNumber(); // this is the DSID
      List<String> ddls = GatewayReceiverHelper.getGatewayReceiverDDL(wanSite);
      for (String ddl : ddls) {
        executeSqlStatement(ddl, true);
      }
    }
  }

  /**
   * Returns the wan site for this JVM based on the hydra logical client name.
   * Assumes that the client name is of the form name_site_vm.
   */
  static protected int getMyWanSiteNumber() {
    String clientName = RemoteTestModule.getMyClientName();
    String arr[] = clientName.split("_");
    if (arr.length != 3) {
      return 1;
    }
    try {
      return Integer.parseInt(arr[1]);
    } catch (NumberFormatException e) {
      String s = clientName + " is not in the form <name>_<wanSiteNumber>_<itemNumber>";
      throw new TestException(s, e);
    }
  }

  /** Create tables already defined in the blackboard.
   * 
   */
  private static void createTables() {
    List<TableDefinition> tableList = (List<TableDefinition>) SqlMemScaleBB.getBB().getSharedMap().get(TABLES_KEY);
    int numWanSites = getNumWanSites();
    int myWanSiteNumber = getMyWanSiteNumber();
    for (TableDefinition table: tableList) {
      String createStmt = table.getCreateTableStatement();
      createStmt = createStmt + " " + SqlMemScalePrms.getTableClause();
      createStmt = createStmt + " GATEWAYSENDER (";
      for (int i = 1; i <= numWanSites; i++) {
        if (i == myWanSiteNumber) {
          continue;
        }
        if (createStmt.contains("SENDER_")) {  // there is already a sender in this stmt
          createStmt = createStmt + ", ";
        }
        createStmt = createStmt + "SENDER_" + myWanSiteNumber + "_TO_" + i;
      }
      createStmt = createStmt + ")";
      executeSqlStatement(createStmt, true);

      // add a listener
      //don't call listener due to bug 48881
      //executeStoredProcedure("CALL SYS.ADD_LISTENER(?,?,?,?,?,?)", 
      //    Arrays.asList(new String[] {"LogListener", SCHEMA_NAME, table.getTableName(), 
      //       SqlLogListener.class.getName(), "none", null}));
    }
    Log.getLogWriter().info("Created " + tableList.size() + " tables");
    logTablesInSchema(SCHEMA_NAME);
  }

  /** Generate random table definitions and write them to the blackboard.
   *  This allows the test to use randomization to define tables, then each client to 
   *  create the tables can read from the blackboard so all clients can create the same tables.
   * 
   */
  private static void generateTableDefinitions() {
    int numTables = SqlMemScalePrms.getNumTables();
    if (numTables <= 0) {
      throw new TestException("Expected " + SqlMemScalePrms.class.getName() + ".numTables to be > 0)");
    }
    List<TableDefinition> tableList = new ArrayList<TableDefinition>();

    // determine which tables have lobs
    long numTablesWithLobs = Math.round((SqlMemScalePrms.getPercentTablesWithLobs() * 0.01) * numTables);
    for (int tableNum = 1; tableNum <= numTables; tableNum++) {
      String tableName = "Table_" + tableNum;
      int numColumnsThisTable = SqlMemScalePrms.getNumColumnsPerTable();
      if (numColumnsThisTable <= 0) {
        throw new TestException("Expected " + SqlMemScalePrms.class.getName() + ".numColumnsPerTable to be > 0");
      }
      TableDefinition tableDef = new TableDefinition(SCHEMA_NAME, tableName);

      // determine which columns are lobs
      boolean[] columnHasLob = new boolean[numColumnsThisTable-1]; // -1 to skip the primary key which is not a lob
      GsRandom rand = TestConfig.tab().getRandGen();
      if (tableNum <= numTablesWithLobs) { // this table has lobs
        long numColumnsToHaveLobs = Math.round(numColumnsThisTable * (SqlMemScalePrms.getPercentLobColumns() * 0.01));
        numColumnsToHaveLobs = Math.max(1, numColumnsToHaveLobs);
        numColumnsToHaveLobs = Math.min(numColumnsThisTable-1, numColumnsToHaveLobs);
        for (int i = 1; i <= numColumnsToHaveLobs; i++) { 
          int randIndex = rand.nextInt(0, columnHasLob.length-1);
          int startIndex = randIndex;
          while (columnHasLob[randIndex]) { // already set
            randIndex++;
            if (randIndex >= columnHasLob.length){
              randIndex = 0;
            }
            if (randIndex == startIndex) {
              break; // we went all the way around
            }
          }
          columnHasLob[rand.nextInt(0, columnHasLob.length-1)] = true;
        }
      }

      // build the table definition
      tableDef.addColumn("pKey", "VARCHAR", "20", true); // primary key
      int startingColNum = 2; // skip primary key
      for (int columnNum = startingColNum; columnNum <= numColumnsThisTable; columnNum++) {
        String columnName = "col_" + columnNum;
        String dataType = SqlMemScalePrms.getColumnType();
        if (columnHasLob[columnNum-startingColNum]) {
          dataType = SqlMemScalePrms.getLobColumnType();
        }
        if (dataType.equalsIgnoreCase("varchar")) {
          tableDef.addColumn(columnName, dataType, "" + SqlMemScalePrms.getVarCharLength(), false);
        } else if (dataType.equalsIgnoreCase("clob")) {
          tableDef.addColumn(columnName, dataType, SqlMemScalePrms.getLobLength(), false);
        } else {
          tableDef.addColumn(columnName, dataType, false);
        }
      }
      tableList.add(tableDef);

    }
    Log.getLogWriter().info("Created " + tableList.size() + " table definitions");
    SqlMemScaleBB.getBB().getSharedMap().put(TABLES_KEY, tableList);
  }

  /** Return the total number of wan sites in this test
   * 
   * @return The number of wan sites.
   */
  protected static int getNumWanSites() {
    Vector clientNames = TestConfig.getInstance().getClientNames();
    int maxSite = 0;
    for (Iterator i = clientNames.iterator(); i.hasNext();) {
      String clientName = (String)i.next();
      String arr[] = clientName.split("_");
      if (arr.length != 3) {
        return 1; // assume single site
      }
      int site;
      try {
        site = Integer.parseInt(arr[1]);
      } catch (NumberFormatException e) {
        String s = clientName
            + " is not in the form <name>_<wanSiteNumber>_<itemNumber>";
        throw new HydraRuntimeException(s, e);
      }
      maxSite = Math.max(site, maxSite);
    }
    if (maxSite == 0) {
      String s = "Should not happen";
      throw new HydraInternalException(s);
    }
    return maxSite;
  }

  /** Execute a sql statement using jdbc with this thread's connection
   * 
   * @param sqlStatement The string containing the sql statement to execute.
   * @param logStatement If true, then log the statement, if false do not log.
   */
  static private void executeSqlStatement(String sqlStatement, boolean logStatement) {
    Connection conn = (Connection) threadLocal_clientConnection.get();
    try {
      Statement stmt = conn.createStatement();
      try {
        if (logStatement) {
          Log.getLogWriter().info("Executing sql statement: \"" + sqlStatement + "\"");
        }
        long startTime = System.currentTimeMillis();
        stmt.execute(sqlStatement);
        long duration = System.currentTimeMillis() - startTime;
        if (logStatement) {
          Log.getLogWriter().info("Done executing sql statement: \"" + sqlStatement + "\" in " + duration + "ms");
        }
      } finally {
        if (stmt != null) {
          stmt.close();
        }
      }
    } catch (SQLException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  /** Execute an sql query and return the result set.
   * 
   * @param sqlQuery The query string to execute.
   * @return The result set for the query.
   */
  static private ResultSet executeSqlQuery(String sqlQuery) {
    Connection conn = (Connection) threadLocal_clientConnection.get();
    try {
      Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
      Log.getLogWriter().info("Executing sql query: " + sqlQuery);
      long startTime = System.currentTimeMillis();
      ResultSet rs = stmt.executeQuery(sqlQuery);
      long duration = System.currentTimeMillis() - startTime;
      Log.getLogWriter().info("Done executing sql query: " + sqlQuery + " in " + duration + "ms");
      return rs;
    } catch (SQLException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  /** Execute the given stored procedure once per String argument.
   * 
   * @param procedure The stored procedure command.
   * @param procedureArgs A List of procedure args.
   */
  static private void executeStoredProcedureOncePerArg(String procedure, List<String> procedureArgs) {
    try {
      Connection conn = (Connection) threadLocal_clientConnection.get();
      CallableStatement call = conn.prepareCall(procedure);
      for (String arg : procedureArgs) {
        Log.getLogWriter().info("Executing stored procedure: " + procedure + " with arg(s): " + procedureArgs);
        call.setString(1, arg);
        call.execute();
        Log.getLogWriter().info("Done executing stored procedure: " + procedure + " with arg(s): " + procedureArgs);
      }
    } catch (SQLException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  /** Execute the given sql statement as a prepared statement with the given args.
   * 
   * @param stmt The sql statement to execute.
   * @param args A List of statement args.
   * @param logStatement If true, then log the statement, if false do not log.
   */
  static private void executePreparedStatement(CallableStatement preparedStmt, String stmt, List<Object> args, boolean logStatement) {
    try {
      StringBuilder argsStr = new StringBuilder();
      if (logStatement) {
        for (Object arg: args) {
          if (arg instanceof String) {
            argsStr.append("String of size " + ((String)arg).length() + " ");
          } else if (arg instanceof Clob) {
            argsStr.append("Clob of size " + ((Clob)arg).length() + " ");
          } else if (arg instanceof Integer) {
            argsStr.append("Integer " + arg + " ");
          } else {
            throw new TestException("Test does not handle prepared statement arg class " + arg.getClass().getName());
          }
        }
        Log.getLogWriter().info("Executing prepared statement: " + stmt + " with arg(s): " + argsStr + ", primaryKey is " + args.get(0));
      }
      for (int i = 0; i < args.size(); i++) {
        int argIndex = i + 1;
        Object arg = args.get(i);
        if (arg instanceof String) {
          String strArg = (String)(args.get(i));
          preparedStmt.setString(argIndex, strArg);
        } else if (arg instanceof Clob) {
          Clob clobArg = (Clob)(args.get(i));
          preparedStmt.setClob(argIndex, clobArg);
        } else if (arg instanceof Integer) {
          int intArg = (Integer) args.get(i);
          preparedStmt.setInt(argIndex, intArg);
        }
      }
      preparedStmt.execute();
      if (logStatement) {
        Log.getLogWriter().info("Done executing prepared statement: " + stmt + " with arg(s): " + argsStr);
      }
    } catch (SQLException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  /** Log the names of all existing tables in the given schema
   * 
   * @param schemaName Log the names of all tables in this schema.
   */
  static protected void logTablesInSchema(String schemaName) {
    ResultSet rs = executeSqlQuery("SELECT tablename FROM sys.systables WHERE tabletype = 'T' " +
        "AND tableschemaname = '" + schemaName.toUpperCase() + "'");
    StringBuilder aStr = new StringBuilder("Tables from " + schemaName + " schema:\n");
    try {
      if (!rs.next()) {
        aStr.append("   Table is empty");
      }
      rs.beforeFirst();
      while (rs.next()) {
        String tableName = rs.getString(1);
        aStr.append("  " + tableName + "\n");
      }
    } catch (SQLException e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
    Log.getLogWriter().info(aStr.toString());
  }
}
