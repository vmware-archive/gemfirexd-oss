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
package sql.rollingUpgrade;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Vector;

import hydra.ClientVmInfo;

import hydra.ClientVmMgr;
import hydra.HydraVector;
import hydra.Log;
import hydra.MasterController;
import hydra.RemoteTestModule;
import hydra.StopSchedulingTaskOnClientOrder;
import hydra.TestConfig;
import rollingupgrade.RollingUpgradeTest;
import sql.ddlStatements.DDLStmtIF;
import sql.rollingUpgrade.SQLRollingUpgradeBB;
import sql.rollingUpgrade.SQLRollingUpgradePrms;
import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import util.PRObserver;
import util.SilenceListener;
import util.StopStartVMs;
import util.TestException;
import util.TestHelper;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.gemstone.gemfire.internal.GemFireVersion;
public class SQLRollingUpgradeTest extends SQLTest {
  static HydraVector threadGroupNames = null;
  static HydraVector clientVMsToRestart = null;
  static String[] ddlCreateTableStatements = null;
  static String[] ddlCreateTableExtensions = null;
  static int threadCount = 0;
  static protected SQLRollingUpgradeTest sqlruTest = null; 
  static Random rand = new Random();
  public static synchronized void HydraTask_initialize() {
    if (sqlruTest == null) {
      sqlruTest = new SQLRollingUpgradeTest();
    }
   
    if (threadGroupNames == null) {
      threadGroupNames = TestConfig.tab().vecAt(
          SQLRollingUpgradePrms.threadGroupNames);
      if (threadGroupNames == null) {
        throw new TestException ("No thread groups specified for operations threads");
      }
      for (int i = 0; i < threadGroupNames.size(); i++) {
        String threadGroupName = threadGroupNames.get(i).toString();
        Log.getLogWriter().info("Thread group name: " + threadGroupName);
        Log.getLogWriter().info("ThreadGroup:"+TestConfig.getInstance().getThreadGroup(threadGroupName));
        threadCount += TestConfig.getInstance().getThreadGroup(threadGroupName).getTotalThreads();
      }
    }
    String clientName = RemoteTestModule.getMyClientName();
    Log.getLogWriter().info("My client name: " + clientName);
    if (clientName.equals("peer") ||
        clientName.startsWith("peerServer") ||
        clientName.startsWith("server") ||
        clientName.startsWith("datastore")) {
      Log.getLogWriter().info("My client name: " + clientName);
      Log.getLogWriter().info("Initializing PRObserver with my vmId: " + RemoteTestModule.getMyVmid());
          
      PRObserver.installObserverHook();
      PRObserver.initialize(RemoteTestModule.getMyVmid());
    }
    Vector statements = TestConfig.tab().vecAt(SQLRollingUpgradePrms.ddlCreateTableStatements, new HydraVector());
    Vector extensions = TestConfig.tab().vecAt(SQLRollingUpgradePrms.ddlCreateTableExtensions, new HydraVector());
    ddlCreateTableStatements = new String[statements.size()];
    ddlCreateTableExtensions = new String[extensions.size()];
    for (int i = 0; i < statements.size(); i++) {
      ddlCreateTableStatements[i] = (String)statements.elementAt(i);
      ddlCreateTableExtensions[i] = (String)extensions.elementAt(i);
    }
    HydraTask_initializeFabricServer();
    
  }
  
  public static void HydraTask_doDMLOpPauseAndVerify() {
    sqlruTest.doOperationsAndPauseVerify();
  }
  
  public static void HydraTask_doOperations() {
    sqlruTest.doOperations();
  }
  
  private void doOperations() {
    long opsTaskGranularitySec = TestConfig.tab().longAt(SQLRollingUpgradePrms.opsTaskGranularitySec);
    long minTaskGranularityMS = opsTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    performOps(minTaskGranularityMS);
    if (SQLRollingUpgradeBB.getBB().getSharedCounters().read(SQLRollingUpgradeBB.recycledAllVMs) != 0) {
      throw new StopSchedulingTaskOnClientOrder("All vms have paused");
    }
  }
  
  private void doOperationsAndPauseVerify() {
    int numTotalVMs = StopStartVMs.getAllVMs().size();
    long opsTaskGranularitySec = TestConfig.tab().longAt(SQLRollingUpgradePrms.opsTaskGranularitySec);
    long minTaskGranularityMS = opsTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    if (SQLRollingUpgradeBB.getBB().getSharedCounters().read(SQLRollingUpgradeBB.pausing) == 0) {
      performOps(minTaskGranularityMS);
    }
    
    Log.getLogWriter().info("Done performing one invocation of performOps()");
    if (SQLRollingUpgradeBB.getBB().getSharedCounters().read(SQLRollingUpgradeBB.pausing) > 0) {
      // Controller has restarted one VM and we are ready for validation.
      SQLRollingUpgradeBB.getBB().getSharedCounters().increment(SQLRollingUpgradeBB.pausing);
     
      int desiredCounterValue = threadCount + 1; // +1 because the controller initiates pausing by incrementing the counter
      TestHelper.waitForCounter(SQLRollingUpgradeBB.getBB(), "SQLRollingUpgradeBB.pausing", 
          SQLRollingUpgradeBB.pausing, desiredCounterValue, true, -1, 2000);
      // now all the vm threads have paused
      SilenceListener.waitForSilence(30, 2000);

      verifyResultSets();
      // Ops thread is here means the controller is waiting for threads to finish verification
      // So its  a safe time to reset pausing to 0 here.
      SQLRollingUpgradeBB.getBB().getSharedCounters().decrement(SQLRollingUpgradeBB.pausing);
      
      if (SQLRollingUpgradeBB.getBB().getSharedCounters().read(SQLRollingUpgradeBB.pausing) == 1) {
        // This is the last thread, so make it zero so that controller gets unblocked. 
        SQLRollingUpgradeBB.getBB().getSharedCounters().setIfSmaller(SQLRollingUpgradeBB.pausing, 0);
      }
      TestHelper.waitForCounter(SQLRollingUpgradeBB.getBB(), "SQLRollingUpgradeBB.pausing", 
          SQLRollingUpgradeBB.pausing, 0, true, -1, 2000);
    }
    if (SQLRollingUpgradeBB.getBB().getSharedCounters().read(SQLRollingUpgradeBB.recycledAllVMs) != 0) {
      throw new StopSchedulingTaskOnClientOrder("All vms have paused");
    }
  }

  private void performOps(long taskTimeMS) {
    long startTime = System.currentTimeMillis();
    boolean performDDLOps = TestConfig.tab().booleanAt(SQLRollingUpgradePrms.performDDLOps, false);
    do {
      if (performDDLOps && rand.nextInt(10) > 6) {
        doDDLOp();
      } else {
        Connection dConn =null;
        if (hasDerbyServer) {
          dConn = getDiscConnection();
        }  //when not test uniqueKeys and not in serial execution, only connection to gfe is provided.
        Connection gConn = getGFEConnection();
        doDMLOp(dConn, gConn);
        if (dConn!=null) {
          closeDiscConnection(dConn);
          //Log.getLogWriter().info("closed the disc connection");
        }
        closeGFEConnection(gConn);
      }
      
    } while((System.currentTimeMillis() - startTime < taskTimeMS));
    Log.getLogWriter().info("Done performing one batch of operations");
  }

  protected void doDDLOp(Connection dConn, Connection gConn) {
    
    if (random.nextInt(2) == 0) {
      //perform the procedure or function operations
      int ddl = ddls[random.nextInt(ddls.length)];
      DDLStmtIF ddlStmt= ddlFactory.createDDLStmt(ddl); //dmlStmt of a table
      ddlStmt.doDDLOp(dConn, gConn);
      commit(dConn); 
      commit(gConn);
    } else {
      
      // perform create and drop table operations.
      int i = random.nextInt(ddlCreateTableStatements.length);
      
      try {
        Statement stmt = gConn.createStatement();
        Log.getLogWriter().info("RemoteTestModule.getCurrentThread().getThreadId(): " + RemoteTestModule.getCurrentThread().getThreadId());
        Log.getLogWriter().info("RemoteTestModule.getMyVmid(): " + RemoteTestModule.getMyVmid());
        Log.getLogWriter().info("RemoteTestModule.getMyBaseThreadId(): " + RemoteTestModule.getMyBaseThreadId());
        String UniqueId = "_"+ RemoteTestModule.getMyVmid() + "_"+RemoteTestModule.getCurrentThread().getThreadId();
        
        Log.getLogWriter().info("Dropping the sql table trade.temp (if exists)");
        stmt.execute("drop table if exists "+ "trade.temp" + UniqueId);
        Log.getLogWriter().info("Creating the sql table trade.temp");
        String sqlCommand = "create table " + "trade.temp"+ UniqueId + " "+ ddlCreateTableStatements[i] +" " + ddlCreateTableExtensions[i];
        Log.getLogWriter().info("Commiting the sql create table");
        Log.getLogWriter().info("sqlCommand: " + sqlCommand);
        stmt.execute(sqlCommand);
        commit(gConn);
      } catch (SQLException se) {
        SQLHelper.handleSQLException(se);
      }
      
    }

  }
  
  /** Initialize the compatibility test controller
   * 
   */
  public synchronized static void HydraTask_initController() {
    if (sqlruTest == null) {
      sqlruTest = new SQLRollingUpgradeTest();  
    }
    
    clientVMsToRestart = TestConfig.tab().vecAt(
        SQLRollingUpgradePrms.clientVMNamesForRestart);
    if (clientVMsToRestart == null) {
      throw new TestException ("No client vm names specified for restart sequence");
    }
  }
  
  public static void HydraTask_UpgradeController() {
    sqlruTest.rollUpgradeVMs(true);
  }
  
  public static void HydraTask_UpgradeControllerNoVerify() {
    sqlruTest.rollUpgradeVMs(false);
  }
  
  private void rollUpgradeVMs(boolean waitForVerification) {
    List<ClientVmInfo> vms = new ArrayList<ClientVmInfo>();
    List<ClientVmInfo> allVMs = new ArrayList<ClientVmInfo>();
    for (int i = 0; i < clientVMsToRestart.size(); i++) {
      allVMs.addAll(StopStartVMs.getMatchVMs(StopStartVMs.getAllVMs(), clientVMsToRestart.get(i).toString()));
      if (clientVMsToRestart.get(i).equals("peer") ||
          clientVMsToRestart.get(i).equals("peerServer") ||
          clientVMsToRestart.get(i).equals("server") ||
          clientVMsToRestart.get(i).equals("datastore")) {
        vms.addAll(StopStartVMs.getMatchVMs(StopStartVMs.getAllVMs(), clientVMsToRestart.get(i).toString()));
      }
    }
    
    do {
      
      // This is check to make sure no thread is in its verification state.
      if(waitForVerification) {
        TestHelper.waitForCounter(SQLRollingUpgradeBB.getBB(), "SQLRollingUpgradeBB.pausing", 
            SQLRollingUpgradeBB.pausing, 0, true, -1, 2000);
      }
      
      ClientVmInfo vmInfo = null;
      // For upgrade we will follow a sequence of locators -> bridge servers -> edge clients.
      // This order is predefined and being recommended to customers for rolling upgrade.
      if (allVMs.size() != 0) {
        vmInfo = allVMs.get(0);
        allVMs.remove(0);
      }      
      MasterController.sleepForMs(15000);
      StopStartVMs.stopVM(vmInfo, "nice_exit");
      Log.getLogWriter().info("Sleeping for " + 2 + " seconds to allow ops to run...");
      MasterController.sleepForMs(2 * 1000);
      StopStartVMs.startVM(vmInfo);
      
      MasterController.sleepForMs(15000);
      if ((vmInfo.getClientName().startsWith("peer") &&
          !vmInfo.getClientName().startsWith("peerClient")) ||
          vmInfo.getClientName().startsWith("datastore") ||
          vmInfo.getClientName().startsWith("server")) {
        int numOfPRs = (int) SQLBB.getBB().getSharedCounters().read(SQLBB.numOfPRs);
        Log.getLogWriter().info("Total number of PR is " + numOfPRs);
        if (numOfPRs != 0) {
          vms = new ArrayList<ClientVmInfo>();
          vms.add(vmInfo);
          PRObserver.waitForRebalRecov(vms, 1, numOfPRs, null, null, false);
        }
      }
      if (waitForVerification) {
        SQLRollingUpgradeBB.getBB().getSharedCounters().increment(SQLRollingUpgradeBB.pausing);
      }
      Log.getLogWriter().info("Remaining VMs size: " + allVMs.size());
      for (ClientVmInfo info : allVMs) {
        Log.getLogWriter().info("VM: " + info.getClientName() + ":" + info.getVmid());
      }
    } while (allVMs.size() != 0);
    SQLRollingUpgradeBB.getBB().getSharedCounters().increment(SQLRollingUpgradeBB.recycledAllVMs);
  }
  

  public static void HydraTask_UpgradeLocators() throws Exception {
    if (sqlruTest == null) {
      sqlruTest = new SQLRollingUpgradeTest();      
    }
    sqlruTest.upgradeLocators();
  }
  
  private void upgradeLocators() throws Exception {
    boolean locatorUpgradeCompleted = (SQLRollingUpgradeBB.getBB().getSharedCounters().read(SQLRollingUpgradeBB.locatorUpgradeComplete)) == 1;
    if (locatorUpgradeCompleted) {
      return;
    }
    List vmInfoList = StopStartVMs.getAllVMs();
    int myVmID = RemoteTestModule.getMyVmid();
    List<ClientVmInfo> locatorVMs = new ArrayList();
    Log.getLogWriter().info("VMInfo list" + vmInfoList);
    for (int i = 0; i < vmInfoList.size(); i++) {
      
      Object anObj = vmInfoList.get(i);
      Log.getLogWriter().info("VM info obj :" + anObj);
      if (anObj instanceof ClientVmInfo) {
        ClientVmInfo info = (ClientVmInfo) (anObj);
        Log.getLogWriter().info("info.getClientName()" + info.getClientName());
        if (info.getClientName().indexOf("locator") >= 0) { // its a match
          locatorVMs.add(info);      
        }
      }
    }

    Log.getLogWriter().info("locatorVMs" + locatorVMs);
    while (locatorVMs.size() != 0) {
      ClientVmInfo vmInfo = null;
      if (locatorVMs.size() != 0) {
        vmInfo = locatorVMs.get(0);
        locatorVMs.remove(0);
      }
      MasterController.sleepForMs(5000);

      StopStartVMs.stopVM(vmInfo, "nice_exit");
      Log.getLogWriter().info(
          "Sleeping for " + 2 + " seconds to allow ops to run...");
      MasterController.sleepForMs(2 * 1000);
      StopStartVMs.startVM(vmInfo);

      MasterController.sleepForMs(5000);
      Log.getLogWriter().info("Locator VMs size: " + locatorVMs.size());

    }
    MasterController.sleepForMs(5000);
    SQLRollingUpgradeBB.getBB().getSharedCounters().increment(SQLRollingUpgradeBB.locatorUpgradeComplete);
    //ClientVmMgr.stopAsync("Killing self at version: " + GemFireVersion.getGemFireVersion(), ClientVmMgr.NICE_KILL, ClientVmMgr.IMMEDIATE);
  }
  
  
}
