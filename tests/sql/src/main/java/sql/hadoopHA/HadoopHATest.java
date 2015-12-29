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
package sql.hadoopHA;

import hdfs.HDFSPrms;
import hdfs.HDFSUtil;
import hdfs.HDFSUtilBB;

import hydra.ConfigPrms;
import hydra.HadoopDescription;
import hydra.HadoopHelper;
import hydra.HostDescription;
import hydra.HydraRuntimeException;
import hydra.HydraTimeoutException;
import hydra.Log;
import hydra.NetworkHelper;
import hydra.MasterController;
import hydra.Prms;
import hydra.ProcessMgr;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import hydra.blackboard.SharedCounters;
import hydra.gemfirexd.FabricServerHelper;
import hydra.gemfirexd.HDFSStoreDescription;
import hydra.gemfirexd.HDFSStoreHelper;
import hydra.gemfirexd.NetworkServerHelper;
import hydra.gemfirexd.GfxdConfigPrms;

import java.io.File;
import java.rmi.RemoteException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import sql.dmlStatements.DMLStmtIF;
import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import sql.sqlutil.ResultSetHelper;

import com.gemstone.gemfire.cache.hdfs.HDFSIOException;

import util.PRObserver;
import util.TestException;
import util.TestHelper;

public class HadoopHATest extends SQLTest {

  public static final Random random = TestConfig.getInstance().getParameters().getRandGen();  

  public static synchronized void HydraTask_initializeGFXD() {
    if (sqlTest == null) {
      sqlTest = new HadoopHATest();
      PRObserver.installObserverHook();
      PRObserver.initialize(RemoteTestModule.getMyVmid());
    }
    ((HadoopHATest)sqlTest).initialize();
  }

  public static synchronized void HydraTask_initializeFabricServer() {
    if (sqlTest == null) {
      sqlTest = new HadoopHATest();
      PRObserver.installObserverHook();
      PRObserver.initialize(RemoteTestModule.getMyVmid());
    }
    ((HadoopHATest)sqlTest).initialize();
  }

/*
  public static void HydraTask_rebalanceTables() {
    Log.getLogWriter().info("Rebalancing tables");
    Properties p = FabricServerHelper.getBootProperties();
    if (p != null &&
        "false".equalsIgnoreCase(p.getProperty(Attribute.GFXD_HOST_DATA)) && 
        "true".equalsIgnoreCase(p.getProperty(Attribute.GFXD_PERSIST_DD))) {
      p.setProperty(Attribute.GFXD_PERSIST_DD, "false");
    }
    sqlTest.rebalanceTables(p);
    Log.getLogWriter().info("Rebalanced tables");
  }

  private void rebalanceTables(Properties p) throws InterruptedException, SQLException {
    Connection conn = getGFEConnection(p);
    try {
      CallableStatement cs = conn.prepareCall("call SYS.REBALANCE_ALL_BUCKETS()");
      cs.execute();
      cs.close();

      final ResourceManager rm = CacheFactory.getAnyInstance().getResourceManager();
      Log.getLogWriter().info("Waiting for existing rebalance");
      for (RebalanceOperation op : rm.getRebalanceOperations()) {
        op.getResults(); // blocking call
      }
      Log.getLogWriter().info("Waited for existing rebalance");
    } finally {
      closeGFEConnection(conn);
    }
  }
*/

  public static void HydraTask_createHDFSStore() {
    ((HadoopHATest)sqlTest).createHDFSStore();
  }

  private void createHDFSStore() {
    Connection conn = getGFEConnection();
    String createHDFSStoreStmt = HDFSStoreHelper.getHDFSStoreDDL(GfxdConfigPrms.getHDFSStoreConfig());
    Log.getLogWriter().info("Creating hdfs store: " + createHDFSStoreStmt);
    try {
      Statement s = conn.createStatement();
      s.execute(createHDFSStoreStmt);
      s.close();
      closeGFEConnection(conn);
    } catch (Throwable t) {
      handleException(t, HadoopHAPrms.expectExceptions());
      Log.getLogWriter().info("createHDFSStore() caught expected exception: " + TestHelper.getStackTrace(t) + " continuing test");
    }
  }

  //perform dml statement
  public static void HydraTask_doDMLOp() {
    ((HadoopHATest)sqlTest).doDMLOp();
  }

  protected void doDMLOp() {
    Connection dConn =null;
    if (hasDerbyServer) {
      //when not test uniqueKeys and not in serial execution, only connection to gfe is provided.
      dConn = getDiscConnection();
    }  
    Connection gConn = getGFEConnection();
    try {
      doDMLOp(dConn, gConn);
    } catch (TestException te) {
      long recycleInProgress = HDFSUtilBB.getBB().getSharedCounters().read(HDFSUtilBB.recycleInProgress);
      boolean allowExceptions = recycleInProgress > 0;
      handleException(te, allowExceptions);
      Log.getLogWriter().info("doDMLOp caught expected exception: " + TestHelper.getStackTrace(te) + " continuing test");
    }
    if (dConn!=null) {
      closeDiscConnection(dConn);
    }
    closeGFEConnection(gConn);
    Log.getLogWriter().info("done dmlOp");
  }

  protected void doDMLOp(Connection dConn, Connection gConn) {
    Log.getLogWriter().info("doDMLOp-performing dmlOp, myTid is " + getMyTid());
    int table = dmlTables[random.nextInt(dmlTables.length-1)];   //get random table to perform dml
    DMLStmtIF dmlStmt = dmlFactory.createDMLStmt(table);       //dmlStmt of a table
    int numOfOp = random.nextInt(5) + 1;
    int size = 1;

    String operation = TestConfig.tab().stringAt(SQLPrms.dmlOperations);
    if (operation.equals("insert")) {
      for (int i = 0;i < numOfOp;i++) {
        if (setCriticalHeap) {
          resetCanceledFlag();
        }
        dmlStmt.insert(dConn, gConn, size);
        commit(dConn);
        commit(gConn);
      }
    } else if (operation.equals("put")) {
      for (int i = 0;i < numOfOp;i++) {
        if (setCriticalHeap) {
          resetCanceledFlag();
        }
        dmlStmt.put(dConn, gConn, size);
        commit(dConn);
        commit(gConn);
      }
    } else if (operation.equals("update")) {
      for (int i = 0;i < numOfOp;i++) {
        if (setCriticalHeap) {
          resetCanceledFlag();
        }
        dmlStmt.update(dConn, gConn, size);
        commit(dConn);
        commit(gConn);
      }
    } else if (operation.equals("delete")) {
      if (setCriticalHeap) {
        resetCanceledFlag();
      }
      dmlStmt.delete(dConn, gConn);
    } else if (operation.equals("query")) {
      if (setCriticalHeap) {
        resetCanceledFlag();
      }
      dmlStmt.query(dConn, gConn);
    } else {
      throw new TestException("Unknown entry operation: " + operation);
    }
    commit(dConn);
    commit(gConn);
  }

  /** handleException(Throwable t) - look for specific SQLException X0Z30 Caused by HDFSIOException.
   */
  public static void handleException(Throwable t, boolean allowHDFSExceptions) {
    boolean allowableException = false;
    SQLException se = null;
    Throwable cause = TestHelper.findCause(t, SQLException.class);
    if (cause != null) {
      se = (SQLException)cause;
      if (se.getSQLState().equalsIgnoreCase("X0Z30")) {
        Throwable hdfsio = TestHelper.findCause(se, HDFSIOException.class);
        if (hdfsio != null) {
          allowableException = true;
        }
      } 
    }

    // some lower layer test code throws a TestException with a stack trace of the SQLException
    // rather than incorporating the SQLException into the Exception ... accommodate this
    if (t.getMessage().contains("java.sql.SQLException(X0Z30)")) {
      if (t.getMessage().contains("Caused by: com.gemstone.gemfire.cache.hdfs.HDFSIOException")) {
        allowableException = true;
      }
    }

    if (!allowHDFSExceptions || !allowableException) {
      throw new TestException("caught unexpected Exception: " + t + " " + TestHelper.getStackTrace(t) + 
                              " allowHDFSExceptions = " + allowHDFSExceptions + 
                              " and allowableException = " + allowableException);
    } 
  }

  /** Stops a single data node (assumes multiple data nodes are active and HadoopPrms.dataNodeReplication > 1).
   */
  public static void stopDataNode() {
    String target = getTargetHost();
    int waitTimeSec = HDFSPrms.hadoopStopWaitSec();
    if (waitTimeSec > 0) {
      Log.getLogWriter().info("HadoopHATest.stopDataNode sleeping for " +  waitTimeSec + " seconds before stopping data node on " + target);
      MasterController.sleepForMs( waitTimeSec * 1000 );
    }
    HadoopHelper.stopDataNode(ConfigPrms.getHadoopConfig(), target);
  }

  /** Drop and restore the connection between the two test hosts 
   * (gemfirexd members and HDFS Cluster).
   *  The dropType is always 2 way (we want to simulate a network partition).
   */
  public static void dropAndRestoreConnection() {
     String src = getDataStoreHost();
     String target = getTargetHost();

     // drop connection
     int waitTimeSec = HDFSPrms.hadoopStopWaitSec();
     if (waitTimeSec > 0) {
        Log.getLogWriter().info("HadoopHATest.dropConnections sleeping for " +  waitTimeSec + " seconds before dropping network connection");
        MasterController.sleepForMs( waitTimeSec * 1000 );
     }
     dropConnection(src, target);

     // restore connection
     waitTimeSec = HDFSPrms.hadoopStartWaitSec();
     if (waitTimeSec > 0) {
        Log.getLogWriter().info("HadoopHATest.dropConnections sleeping for " +  waitTimeSec + " seconds before restoring network connection");
        MasterController.sleepForMs( waitTimeSec * 1000 );
     }
     restoreConnection(src, target);

     // wait before returning (allow time for name node to leave 'safe mode')
     waitTimeSec = HDFSPrms.hadoopReturnWaitSec();
     if (waitTimeSec > 0) {
        Log.getLogWriter().info("HadoopHATest.dropConnections sleeping for " +  waitTimeSec + " seconds before returning (allow namenode time to exit safe mode)");
        MasterController.sleepForMs(waitTimeSec * 1000);
     }
  }
  
  /** Drop the connection between the two test hosts (gemfirexd members and HDFS Cluster).
   *  The dropType is always 2 way (we want to simulate a network partition).
   */
  public static void dropConnection() {
    MasterController.sleepForMs( HDFSPrms.hadoopStopWaitSec() * 1000 );
    String src = getDataStoreHost();
    String target = getTargetHost();
    dropConnection(src, target);
    int waitTimeSec = HDFSPrms.hadoopReturnWaitSec();
    if (waitTimeSec > 0) {
       Log.getLogWriter().info("HadoopHATest.dropConnections sleeping for " +  waitTimeSec + " seconds");
       MasterController.sleepForMs(waitTimeSec * 1000);
    }
  }
  
  public static void dropConnection(String src, String target) {
     HDFSUtilBB.getBB().getSharedCounters().increment(HDFSUtilBB.recycleInProgress);
     NetworkHelper.printConnectionState();
     NetworkHelper.dropConnectionTwoWay(src, target);
     NetworkHelper.printConnectionState();
  }

  /**
   * Restore Connectivity between test hosts (gemfirexd members and HDFS Cluster).
   */
  public static void restoreConnection() {
    MasterController.sleepForMs( HDFSPrms.hadoopStartWaitSec() * 1000 );
    String src = getDataStoreHost();
    String target = getTargetHost();
    restoreConnection(src, target);
    int waitTimeSec = HDFSPrms.hadoopReturnWaitSec();
    if (waitTimeSec > 0) {
       Log.getLogWriter().info("HadoopHATest.dropConnections sleeping for " +  waitTimeSec + " seconds");
       MasterController.sleepForMs(waitTimeSec * 1000);
    }
    HDFSUtilBB.getBB().getSharedCounters().zero(HDFSUtilBB.recycleInProgress);

  }
  
  /**
   * Restore Connectivity between src and target hosts.
   */
  public static void restoreConnection(String src, String target) {

    NetworkHelper.printConnectionState();
    try {
        NetworkHelper.restoreConnectionTwoWay(src, target);
    } catch (IllegalStateException e) {
      Log.getLogWriter().info("Caught " + e + " indicating that connections are already restored");
    }
    NetworkHelper.printConnectionState();
  }

  private static String getDataStoreHost() {
    String dataStoreDescription = HadoopHAPrms.gemfirexdHostDescription();
    return getDataStoreHost(dataStoreDescription);
  }

  private static String getDataStoreHost(String dataStoreDescription) {
    return TestConfig.getInstance().getHostDescription(dataStoreDescription).getHostName();
  }

  private static String getTargetHost() {
    // Are we trying to isolate an HDFS Component or a datastore?
    String description =  HadoopHAPrms.hdfsComponentDescription().toLowerCase();
    String targetHost = null;
    if (description.startsWith("datastore")) {
      targetHost = getDataStoreHost(description);
    } else {
      targetHost = getHadoopHost();
    }
    return targetHost;
  }

  private static String getHadoopHost() {
    String hdfsNodeDescription =  HadoopHAPrms.hdfsComponentDescription();
    HadoopDescription hd = HadoopHelper.getHadoopDescription(ConfigPrms.getHadoopConfig());
    //List<HadoopDescription.NodeDescription> ndList = null;
    List ndList = null;
    if (HadoopHAPrms.hdfsComponentDescription().equalsIgnoreCase("namenode")) {
      //List<HadoopDescription.NameNodeDescription> ndList = hd.getNameNodeDescriptions();
      ndList = hd.getNameNodeDescriptions();
    } else if (HadoopHAPrms.hdfsComponentDescription().equalsIgnoreCase("datanode")) {
      //List<HadoopDescription.DataNodeDescription> ndList = hd.getDataNodeDescriptions();
      ndList = hd.getDataNodeDescriptions();
    }
    if (ndList.size() < 1) {
      throw new TestException("No name nodes configured, check local.conf to ensure HDFS nodes are configured");
    } 
    return ((HadoopDescription.NodeDescription)ndList.get(0)).getHostName();
  }
}
