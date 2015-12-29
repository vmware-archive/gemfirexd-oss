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
package gfxdperf.ycsb.gfxd;

import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.cache.partition.PartitionMemberInfo;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;
import com.gemstone.gemfire.cache.partition.PartitionRegionInfo;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.pivotal.gemfirexd.Attribute;
import com.pivotal.gemfirexd.tools.utils.ExecutionPlanUtils;

import gfxdperf.PerfTestException;
import gfxdperf.ycsb.YCSBClient;
import gfxdperf.ycsb.core.DB;
import gfxdperf.ycsb.core.DBException;
import gfxdperf.ycsb.core.WorkloadException;
import gfxdperf.ycsb.core.workloads.CoreWorkload;
import gfxdperf.ycsb.core.workloads.CoreWorkloadPrms;
import gfxdperf.ycsb.gfxd.GFXDPrms.ConnectionType;

import hydra.CacheHelper;
import hydra.ClientVmMgr;
import hydra.ClientVmNotFoundException;
import hydra.ConfigPrms;
import hydra.DistributedSystemHelper;
import hydra.EnvHelper;
import hydra.FileUtil;
import hydra.HadoopHelper;
import hydra.HostDescription;
import hydra.HydraThreadLocal;
import hydra.Log;
import hydra.MasterController;
import hydra.ProcessMgr;
import hydra.RemoteTestModule;
import hydra.StopSchedulingTaskOnClientOrder;
import hydra.TestConfig;
import hydra.gemfirexd.DiskStoreHelper;
import hydra.gemfirexd.FabricServerDescription;
import hydra.gemfirexd.FabricServerHelper;
import hydra.gemfirexd.GfxdConfigPrms;
import hydra.gemfirexd.GfxdTestConfig;
import hydra.gemfirexd.HDFSStoreHelper;
import hydra.gemfirexd.NetworkServerHelper;
import hydra.gemfirexd.ThinClientDescription;

import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 
 * Client for measuring GFXD YCSB performance with GemFireXD.
 * <p> 
 * This class expects a schema <key> <field1> <field2> <field3> ...
 * All attributes are of type VARCHAR. All accesses are through the primary key.
 * Only one index on the primary key is needed.
 */
public class GFXDClient extends YCSBClient {

  public static final String DRIVER = "com.pivotal.gemfirexd.jdbc.EmbeddedDriver";
  public static final String PROTOCOL = "jdbc:gemfirexd:";
  public static final String TABLE_DATAPOLICY_QUERY = "SELECT tablename, datapolicy FROM sys.systables WHERE tableschemaname='APP'";
  public static final String QUERY_PLAN_QUERY = "select STMT_ID, STMT_TEXT from SYS.STATEMENTPLANS";
  public static final String HDFS_AEQ = "AsyncEventQueue_GEMFIRE_HDFS_BUCKETSORTED_QUEUE";
  public static final String HDFS_FLUSH_QUEUE_CALL = "call sys.HDFS_FLUSH_QUEUE(?,?)";
  public static final String HDFS_FORCE_COMPACTION_CALL = "call sys.HDFS_FORCE_COMPACTION(?,?)";

  /** Prefix for each column in the table */
  public static String COLUMN_PREFIX = "field";

  /** File in the test result directory for generated DDL */
  public static final String DDL_FILE = "ddl.sql";

  /** Table DDL used to evict all incoming data */ 
  public static final String EVICT_INCOMING = "EVICTION BY CRITERIA (1=1 OR YCSB_KEY='') EVICT INCOMING";

  /** File in the test result directory for the HDFS client config file */
  public static final String HDFS_CLIENT_CONFIG_FILE = "gfxd-client-config.xml";

  /** Primary key column in the table */
  public static final String PRIMARY_KEY = "YCSB_KEY";

  /** Query to look up tables and their data policies */
  public static final String TABLE_QUERY = "SELECT tablename, datapolicy FROM sys.systables WHERE tableschemaname='APP'";

  /** Table name for the base YCSB table */
  public static final String TABLE_NAME = CoreWorkloadPrms.getTableName();

  protected static HydraThreadLocal localgfxdstats = new HydraThreadLocal();

  protected GFXDStats gfxdstats;

//------------------------------------------------------------------------------
// HydraThreadLocals

  public void initHydraThreadLocals() {
    super.initHydraThreadLocals();
    this.gfxdstats = (GFXDStats)localgfxdstats.get();
  }

  public void updateHydraThreadLocals() {
    super.updateHydraThreadLocals();
    localgfxdstats.set(this.gfxdstats);
  }

//------------------------------------------------------------------------------
// HDFS CLIENT CONFIG

  /**
   * Generates the client configuration file for the HDFS store by reading
   * {@link GFXDPrms#hdfsClientConfigFile}, replacing variables, then writing
   * it out to the test directory as {@link #HDFS_CLIENT_CONFIG_FILE}.
   */
  public static void generateHDFSClientConfigTask() throws IOException {
    GFXDClient client = new GFXDClient();
    client.initialize();
    if (client.ttgid == 0) {
      client.generateHDFSClientConfig();
    }
  }

  private void generateHDFSClientConfig() throws IOException {
    String ifn = GFXDPrms.getHDFSClientConfigFile();
    HostDescription hd = getHostDescription();
    String xml = FileUtil.getText(EnvHelper.expandEnvVars(ifn, hd));
    String hadoopConfig = ConfigPrms.getHadoopConfig();
    xml = xml.replace("${dfs.replication}", String.valueOf(HadoopHelper
             .getHadoopDescription(hadoopConfig).getReplication()));
    xml = xml.replace("${user}", System.getProperty("user.name"));
    String ofn = System.getProperty("user.dir") + hd.getFileSep()
               + HDFS_CLIENT_CONFIG_FILE;
    FileUtil.writeToFile(ofn, xml);
  }

//------------------------------------------------------------------------------
// DDL

  /**
   * Generates the DDL and write it out to the test directory as {@link
   * #DDL_FILE}. This includes the basic schema, disk store configuration, and
   * HDFS store configuration.
   */
  public static void generateDDLTask() throws IOException {
    GFXDClient client = new GFXDClient();
    client.initialize();
    if (client.ttgid == 0) {
      client.generateDDL();
    }
  }

  private void generateDDL() throws IOException {
    String ddl = "";
    String diskStoreConfig = GfxdConfigPrms.getDiskStoreConfig();
    if (diskStoreConfig != null) {
      ddl += DiskStoreHelper.getDiskStoreDDL(diskStoreConfig) + ";\n";
    }
    String hdfsStoreConfig = GfxdConfigPrms.getHDFSStoreConfig();
    if (hdfsStoreConfig != null) {
      ddl += HDFSStoreHelper.getHDFSStoreDDL(hdfsStoreConfig) + ";\n";
    }
    ddl += generateTableDDL(diskStoreConfig) + ";\n";

    FileUtil.writeToFile(DDL_FILE, ddl);
  }

  private String generateTableDDL(String diskStoreConfig) {
    int fieldcount = CoreWorkloadPrms.getFieldCount();
    int buckets = GFXDPrms.getBucketCount();
    int redundancy = GFXDPrms.getPartitionRedundancy();
    StringBuilder sql = new StringBuilder("CREATE TABLE ");
    sql.append(TABLE_NAME).append(" (").append(PRIMARY_KEY)
       .append(" VARCHAR(100) PRIMARY KEY");
    for (int i = 0; i < fieldcount; i++) {
      sql.append(", FIELD").append(i).append(" VARCHAR(100)");
    }
    String primaryKey = GFXDPrms.useGlobalIndex() ? "FIELD0" : PRIMARY_KEY;
    sql.append(")")
       .append(" partition by (").append(primaryKey).append(") ")
       .append(" buckets ").append(buckets)
       .append(" redundancy ").append(redundancy);

    Map<String,FabricServerDescription> fsds =
      GfxdTestConfig.getInstance().getFabricServerDescriptions();
    for (FabricServerDescription fsd : fsds.values()) {
      if (fsd.getOffHeapMemorySize() != null) {
        sql.append(" offheap");
        break;
      }
    }
    for (FabricServerDescription fsd : fsds.values()) {
      if (fsd.getPersistTables()) {
        sql.append(" persistent '").append(diskStoreConfig);
        if (GFXDPrms.persistSynchronous()) {
          sql.append("' synchronous");
        } else {
          sql.append("' asynchronous");
        }
        break;
      }
    }

    String hdfsStoreConfig = GfxdConfigPrms.getHDFSStoreConfig();
    if (hdfsStoreConfig != null) {
      // @todo put in optional hdfs write-only
      boolean evictIncoming = GFXDPrms.evictIncoming();
      if (evictIncoming) {
        sql.append(" ").append(EVICT_INCOMING);
      }
      String hdfsStoreName = HDFSStoreHelper
        .getHDFSStoreDescription(hdfsStoreConfig).getName();
      sql.append(" HDFSSTORE (").append(hdfsStoreName).append(")");
    }
    return sql.toString();
  }

  /**
   * Executes the DDL generated in {@link #generateDDLTask} as found in {@link
   * #DDL_FILE}.
   */
  public static void executeDDLTask() throws IOException, SQLException {
    GFXDClient client = new GFXDClient();
    client.initialize();
    if (client.ttgid == 0) {
      client.executeDDL();
    }
  }

  private void executeDDL() throws IOException, SQLException {
    final Connection conn = openTmpConnection();
    try {
      Statement stmt = conn.createStatement();
      String fn = System.getProperty("user.dir") + "/" + DDL_FILE;
      List<String> ddls = FileUtil.getTextAsList(fn);
      for (String ddl : ddls) {
        if (ddl.length() > 0) {
          Log.getLogWriter().info("Executing DDL: " + ddl);
          boolean result = stmt.execute(ddl);
          Log.getLogWriter().info("Executed DDL: " + ddl + " with result "
                                 + result);
        }
      }
      stmt.close();
    } finally {
      closeTmpConnection(conn);
    }
  }

//------------------------------------------------------------------------------
// HDFS

  /**
   * Configures Hadoop.
   */
  public static void configureHadoopTask() throws SQLException {
    String hadoopConfig = ConfigPrms.getHadoopConfig();
    if (hadoopConfig != null) {
      HadoopHelper.configureHadoop(hadoopConfig);
    }
  }

  /**
   * Starts the HDFS cluster. Omits YARN.
   */
  public static void startHDFSClusterTask() throws SQLException {
    String hadoopConfig = ConfigPrms.getHadoopConfig();
    if (hadoopConfig != null) {
      HadoopHelper.startHDFSCluster(hadoopConfig);
    }
  }

  /**
   * Stops the HDFS cluster. Omits YARN.
   */
  public static void stopHDFSClusterTask() throws SQLException {
    String hadoopConfig = ConfigPrms.getHadoopConfig();
    if (hadoopConfig != null) {
      HadoopHelper.stopHDFSCluster(hadoopConfig);
    }
  }

//------------------------------------------------------------------------------
// LOCATOR

  /**
   * Creates locator endpoints for use by {@link #startLocatorTask}.
   */
  public static void createLocatorTask() throws SQLException {
    FabricServerHelper.createLocator();
  }

  /**
   * Starts a locator in this JVM using the endpoints created in {@link
   * #createLocatorTask}.
   */
  public static void startLocatorTask() throws SQLException {
    String networkServerConfig = GfxdConfigPrms.getNetworkServerConfig();
    if (networkServerConfig == null) {
      Log.getLogWriter().info("Starting peer locator only");
      FabricServerHelper.startLocator();
    } else {
      Log.getLogWriter().info("Starting network locator");
      FabricServerHelper.startLocator(networkServerConfig);
    }
  }

//------------------------------------------------------------------------------
// FABRIC SERVER AND NETWORK SERVER

  /**
   * Starts a fabric server in this JVM. For thin client tests, also starts a
   * network server in this JVM to allow thin clients to connect.
   */
  public static void startFabricServerTask() throws SQLException {
    FabricServerHelper.startFabricServer();
    if (GFXDPrms.getConnectionType() == ConnectionType.thin) {
      String networkServerConfig = GfxdConfigPrms.getNetworkServerConfig();
      NetworkServerHelper.startNetworkServers(networkServerConfig);
    }
  }

  /**
   * Stops a fabric server in this JVM. For thin client tests, also stops the
   * network server in this JVM.
   */
  public static void stopFabricServerTask() throws SQLException {
    if (GFXDPrms.getConnectionType() == ConnectionType.thin) {
      NetworkServerHelper.stopNetworkServers();
    }
    FabricServerHelper.stopFabricServer();
  }

//------------------------------------------------------------------------------
// SHUTDOWN

  /**
   * Issues shut-down-all. Use {@link #waitForServerShutdownTask} to wait for
   * the shutdown to complete. Use {@link #bounceSelfTask} to bounce the server
   * JVMs.
   */
  public static void shutDownAllTask() {
    GFXDClient client = new GFXDClient();
    client.initialize();
    if (client.ttgid == 0) {
      FabricServerHelper.shutDownAllFabricServers(300);
    }
  }

  /**
   * Wait for the fabric server in this JVM to shut down. Run this task in all
   * servers to wait for shut-down-all to complete, for example.
   */
  public static synchronized void waitForServerShutdownTask() {
    GFXDClient client = new GFXDClient();
    client.initialize();
    if (client.jid == 0) {
      while (true) {
        if (!FabricServerHelper.isFabricServerStopped()) {
          MasterController.sleepForMs(500);
        } else {
          return;
        }
      }
    }
  }

//------------------------------------------------------------------------------
// BOUNCE

  /**
   * Bounces this JVM using a nice exit and immediate restart. Assign it to all
   * servers to bounce them all, for example.
   */
  public static void bounceSelfTask() throws ClientVmNotFoundException {
    GFXDClient client = new GFXDClient();
    client.initialize();
    if (client.jid == 0) {
      Log.getLogWriter().info("Bouncing self, catch you later...");
      ClientVmMgr.stopAsync("Killing myself nicely with immediate restart",
                             ClientVmMgr.NICE_KILL, ClientVmMgr.IMMEDIATE);
    }
  }

//------------------------------------------------------------------------------
// LOAD BALANCE

  /**
   * Creates all buckets to ensure perfect balance.
   */
  public static void createBucketsTask() throws IOException, SQLException {
    GFXDClient client = new GFXDClient();
    client.initialize();
    if (client.ttgid == 0) {
      client.createBuckets();
    }
  }

  private void createBuckets() throws SQLException {
    final Connection conn = openTmpConnection();
    try {
      CallableStatement cs = conn.prepareCall("call SYS.CREATE_ALL_BUCKETS(?)");
      List<String> tableNames = getPartitionedTables();
      for (String tableName : tableNames) {
        Log.getLogWriter().info("Creating buckets for table " + tableName);
        cs.setString(1, tableName);
        cs.execute();
        Log.getLogWriter().info("Created buckets for table " + tableName);
      }
      cs.close();
    } finally {
      closeTmpConnection(conn);
    }
  }

  /**
   * Checks the balance for buckets and primary buckets for each partitioned
   * table hosted by this datahost. Execute this task on every datahost. Use
   * {@link GFXDPrms#failOnLoadImbalance} to configure whether the test should
   * fail if data is imbalanced.
   */
  public static void checkBucketLoadBalanceTask() throws SQLException {
    GFXDClient client = new GFXDClient();
    client.initialize();
    if (client.jid == 0) {
      client.checkBucketLoadBalance();
    }
  }

  /**
   * Checks the balance for buckets and primary buckets for each partitioned
   * table hosted by this datahost.
   */
  protected void checkBucketLoadBalance() throws SQLException {
    for (String tableName : getPartitionedTables()) {
      checkBucketLoadBalance(tableName);
    }
  }

  /**
   * Checks the bucket and primary bucket load balance for the given partitioned
   * table on this datahost.
   */
  protected void checkBucketLoadBalance(String tableName) {
    Statistics prstats = getPartitionedRegionStats(tableName);
    int buckets = 0;
    int primaries = 0;
    if (prstats != null) {
      List<String> errs = new ArrayList();
      int datahosts = getNumServersHosting(tableName);
      if (datahosts > 1) {
        Log.getLogWriter().info("Checking bucket load balance for " + tableName
                               + " with " + datahosts + " datahosts");
        primaries = prstats.getInt("primaryBucketCount");
        buckets = prstats.getInt("bucketCount");
        int copies = prstats.getInt("configuredRedundantCopies") + 1;
        int totalPrimaries = prstats.getInt("totalNumBuckets");
        int totalBuckets = totalPrimaries * copies;
        int primariesPerDatahost = totalPrimaries / datahosts;
        int bucketsPerDatahost = totalBuckets / datahosts;
        if (buckets != bucketsPerDatahost && (totalBuckets%datahosts == 0
            || buckets != bucketsPerDatahost + 1)) {
          errs.add(tableName + " has " + buckets + " buckets");
        }
        if (primaries != primariesPerDatahost && (totalPrimaries%datahosts == 0
            || primaries != primariesPerDatahost + 1)) {
          errs.add(tableName + " has " + primaries + " primary buckets");
        }
      }
      if (errs.size() > 0) {
        printBucketIdsAndSizes(tableName);
        String s = "Bucket load balance failures: " + errs;
        if (GFXDPrms.getFailOnLoadImbalance()) {
          Log.getLogWriter().warning(s);
        } else {
          throw new PerfTestException(s);
        }
      } else {
        Log.getLogWriter().info("Table " + tableName + " is bucket-balanced"
           + " with " + buckets + " buckets and " + primaries + " primaries");
      }
    }
  }

  /**
   * Checks the data load balance for each partitioned table hosted by this
   * datahost, in bytes. Execute this task on every datahost. Use {@link
   * GFXDPrms#failOnLoadImbalance} to configure whether the test should fail
   * if data is imbalanced.
   */
  public static void checkDataLoadBalanceTask() throws SQLException {
    GFXDClient client = new GFXDClient();
    client.initialize();
    if (client.jid == 0) {
      client.checkDataLoadBalance();
    }
  }

  /**
   * Checks the data load balance for each partitioned table hosted by this
   * datahost, in bytes.
   */
  protected void checkDataLoadBalance() throws SQLException {
    for (String tableName : getPartitionedTables()) {
      checkDataLoadBalance(tableName);
    }
  }

  /**
   * Checks the data load balance for the given table. Complains if any two
   * datahosts differ by more than 10%.
   */
  protected void checkDataLoadBalance(String tableName) {
    List<String> errs = new ArrayList();
    for (PartitionRegionInfo pri : getPartitionRegionInfo()) {
      String regionPath = pri.getRegionPath();
      if (regionPath.contains(tableName) && !regionPath.contains(HDFS_AEQ)) {
        int datahosts = getNumServersHosting(tableName);
        if (datahosts > 1) {
          Log.getLogWriter().info("Checking entry load for " + tableName
                                 + " with " + datahosts + " datahosts");
          List<Long> sizes = new ArrayList();
          Set<PartitionMemberInfo> pmis = pri.getPartitionMemberInfo();
          long maxSize = 0;
          for (PartitionMemberInfo pmi : pmis) {
            long size = pmi.getSize();
            if (size < 0) {
              errs.add("Negative size for " + tableName + ": " + size);
            }
            if (size > maxSize) maxSize = size;
            sizes.add(size);
          }
          if (maxSize > 0) {
            for (Long size : sizes) {
              double ratio = size < maxSize ? (double)size/(double)maxSize
                                            : (double)maxSize/(double)size;
              if (ratio < 0.90) {
                errs.add(tableName + " is imbalanced: " + sizes);
                break;
              }
            }
          }
          Log.getLogWriter().info("Checked entry load for " + tableName
             + ", found entry sizes " + sizes + " bytes");
        }
      }
    }
    if (errs.size() > 0) {
      printBucketIdsAndSizes(tableName);
      String s = "Data load balance failures: " + errs;
      if (GFXDPrms.getFailOnLoadImbalance()) {
        Log.getLogWriter().warning(s);
      } else {
        throw new PerfTestException(s);
      }
    } else {
      Log.getLogWriter().info("Table " + tableName + " is load-balanced");
    }
  }

  /**
   * Prints the bucket ids and sizes for buckets and primary buckets for each
   * partitioned table hosted on this datahost. Execute this task on every
   * datahost.
   */
  public static void printBucketIdsAndSizesTask() throws SQLException {
    GFXDClient client = new GFXDClient();
    client.initialize();
    if (client.jid == 0) {
      client.printBucketIdsAndSizes();
    }
  }

  protected void printBucketIdsAndSizes() throws SQLException {
    for (String tableName : getPartitionedTables()) {
      printBucketIdsAndSizes(tableName);
    }
  }

  /**
   * Prints a list of bucket and primary bucket ids for the given partitioned
   * table on this datahost, and the size of each bucket.
   */
  protected void printBucketIdsAndSizes(String tableName) {
    for (PartitionRegionInfo pri : getPartitionRegionInfo()) {
      String regionPath = pri.getRegionPath();
      if (regionPath.contains(tableName)) {
        StringBuilder sb = new StringBuilder();
        PartitionedRegion pr =
          (PartitionedRegion)CacheHelper.getCache().getRegion(regionPath);
        // buckets
        List<Integer> bids = (List<Integer>)pr.getLocalBucketsListTestOnly();
        sb.append("Buckets for " + tableName
          + " at " + regionPath + "/" + pr.getName() + ": " + bids + "\n");
        for (Integer bid : bids) {
          sb.append("size of bucket[" + bid + "]=")
            .append(pr.getDataStore().getBucketSize(bid) + "\n");
        }
        // primary buckets
        List<Integer> pbids =
                (List<Integer>)pr.getLocalPrimaryBucketsListTestOnly();
        sb.append("Primary buckets for " + tableName
          + " at " + regionPath + "/" + pr.getName() + ": " + pbids + "\n");
        for (Integer pbid : pbids) {
          sb.append("size of primary bucket[" + pbid + "]=")
            .append(pr.getDataStore().getBucketSize(pbid) + "\n");
        }
        Log.getLogWriter().info(sb.toString());
        return;
      }
    }
  }

  /**
   * Returns a list of all partitioned tables in the APP schema. Opens a
   * temporary connection if needed.
   */
  protected List<String> getPartitionedTables() throws SQLException {
    List<String> tableNames = new ArrayList();
    final Connection conn = openTmpConnection();
    try {
      PreparedStatement ps = conn.prepareStatement(TABLE_DATAPOLICY_QUERY);
      ResultSet rs = ps.executeQuery();
      while (rs.next()) {
        String tablename = rs.getString("tablename");
        String datapolicy = rs.getString("datapolicy");
        if (datapolicy.contains("PARTITION")) {
          tableNames.add(tablename);
        }
      }
      rs.close();
      rs = null;
      ps.close();
    } finally {
      closeTmpConnection(conn);
    }
    return tableNames;
  }

  /**
   * Returns a list of all HDFS partitioned tables in the APP schema. Opens a
   * temporary connection if needed.
   */
  protected List<String> getHDFSPartitionedTables() throws SQLException {
    List<String> tableNames = new ArrayList();
    final Connection conn = openTmpConnection();
    try {
      PreparedStatement ps = conn.prepareStatement(TABLE_DATAPOLICY_QUERY);
      ResultSet rs = ps.executeQuery();
      while (rs.next()) {
        String tablename = rs.getString("tablename");
        String datapolicy = rs.getString("datapolicy");
        if (datapolicy.contains("HDFS") && datapolicy.contains("PARTITION")) {
          tableNames.add(tablename);
        }
      }
      rs.close();
      rs = null;
      ps.close();
    } finally {
      closeTmpConnection(conn);
    }
    return tableNames;
  }

  /**
   * Returns the PartitionedRegionStats for the given table in the APP schema.
   */
  protected Statistics getPartitionedRegionStats(String tableName) {
    Statistics[] stats = DistributedSystemHelper.getDistributedSystem()
      .findStatisticsByTextId("/APP/" + tableName);
    Statistics prStats = null;
    for (int i = 0; i < stats.length; i++) {
      if (stats[i].getType().getName().equals("PartitionedRegionStats")) {
        prStats = stats[i];
      }
    }
    return prStats;
  }

  /**
   * Returns info for each partitioned table.
   */
  protected Set<PartitionRegionInfo> getPartitionRegionInfo() {
    return PartitionRegionHelper.getPartitionRegionInfo(CacheHelper.getCache());
  }

  /**
   * Returns the number of datahosts hosting the given partitioned table.
   */
  protected int getNumServersHosting(String tableName) {
    for (PartitionRegionInfo pri : getPartitionRegionInfo()) {
      if (pri.getRegionPath().contains(tableName)) {
        Set<PartitionMemberInfo> pmis = pri.getPartitionMemberInfo();
        return pmis.size();
      }
    }
    return 0;
  }

//------------------------------------------------------------------------------
// MONITORING

  /**
   * Monitors the workload for flatlines in each server. The server dumps
   * stacks if the disk store statistics show that the writes in progress
   * is equal to the number of threads doing the workload for several samples
   * in a row.
   */
  public static void monitorWorkloadTask() throws InterruptedException {
    GFXDClient client = new GFXDClient();
    client.initialize();
    client.monitorWorkload();
  }

  protected void monitorWorkload() throws InterruptedException {
    String host = TestConfig.getInstance()
      .getClientDescription(RemoteTestModule.getMyClientName())
      .getVmDescription().getHostDescription().getHostName();
    int pid = RemoteTestModule.getMyPid();
    String diskStoreName = GfxdConfigPrms.getDiskStoreConfig();
    if (diskStoreName == null) {
      Log.getLogWriter().info("There is no diskstore to monitor.");
      throw new StopSchedulingTaskOnClientOrder();
    }
    Statistics diskStoreStats = getDiskStoreStats(diskStoreName);
    if (diskStoreStats == null) {
      String s = "Found diskstore " + diskStoreName + " but no statistics";
      throw new PerfTestException(s);
    }
    int threadCount = GFXDPrms.getThreadCount();
    int blockedCount = 0;

    Log.getLogWriter().info("Monitoring DiskStoreStats.writesInProgress for "
       + threadCount + " threads using disk store " + diskStoreName);

    this.terminator.startBatch();
    while (!this.terminator.batchComplete()) {
      if (this.terminator.warmupComplete()) {
        this.terminator.startWork();
      }
      if (this.terminator.workComplete()) {
        this.terminator.terminateTask();
      }
      Thread.sleep(1000);
      int writesInProgress = diskStoreStats.getInt("writesInProgress");
      if (writesInProgress >= threadCount) {
        ++blockedCount;
        if (blockedCount > 2) {
          ProcessMgr.printProcessStacks(host, pid);
          ProcessMgr.printProcessStacks(host, pid);
          Log.getLogWriter().info("DiskStoreStats.writesInProgress="
             + writesInProgress + " blockedCount=" + blockedCount
             + " printed process stacks");
          while (blockedCount != 0) {
            Thread.sleep(1000);
            writesInProgress = diskStoreStats.getInt("writesInProgress");
            if (writesInProgress >= threadCount) {
              ++blockedCount;
              ProcessMgr.printProcessStacks(host, pid);
              Log.getLogWriter().info("DiskStoreStats.writesInProgress="
                 + writesInProgress + " blockedCount=" + blockedCount
                 + " printed process stacks");
            } else {
              blockedCount = 0;
            }
          }
        } else {
          Log.getLogWriter().info("DiskStoreStats.writesInProgress="
             + writesInProgress + " blockedCount=" + blockedCount);
        }
      }
    }
    updateHydraThreadLocals(); // save state for next batch
    this.terminator.terminateBatch();
  }

  /**
   * Returns the DiskStoreStats for the given disk store.
   */
  protected Statistics getDiskStoreStats(String diskStoreName) {
    Statistics[] stats = DistributedSystemHelper.getDistributedSystem()
      .findStatisticsByTextId(diskStoreName.toUpperCase());
    for (int i = 0; i < stats.length; i++) {
      if (stats[i].getType().getName().equals("DiskStoreStatistics")) {
        return stats[i];
      }
    }
    return null;
  }

//------------------------------------------------------------------------------
// QUERY PLANS

  /**
   * Dumps the query plans. Opens a temporary connection if needed.
   */
  public static void dumpQueryPlansTask() throws SQLException {
    GFXDClient client = new GFXDClient();
    client.initialize();
    if (client.ttgid == 0) {
      client.dumpQueryPlans();
    }
  }

  private void dumpQueryPlans() throws SQLException {
    final Connection conn = openTmpConnection();
    try {
      Log.getLogWriter().info("Extracting query plans...");
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery(QUERY_PLAN_QUERY);
      int numQueryPlans = 0;
      while (rs.next()) {
        ++numQueryPlans;
        final String stmt_id = rs.getString("STMT_ID");
        String stmtInfo = "stmt_id = " + stmt_id + " statement = "
                        + rs.getString("STMT_TEXT");
        ExecutionPlanUtils plan =
                 new ExecutionPlanUtils(conn, stmt_id, null, true);
        String planAsText = String.valueOf(plan.getPlanAsText(null)).trim();
        Log.getLogWriter().info("Query plan...\n" + stmtInfo + "\n"
                                                  + planAsText);
      }
      Log.getLogWriter().info("Extracted " + numQueryPlans + " query plans");
      rs.close();
      rs = null;
    } finally {
      closeTmpConnection(conn);
    }
  }

//------------------------------------------------------------------------------
// TMP CONNECTION (peers only)

  /**
   * Opens a temporary embedded connection if there is not one currently open.
   * @returns an open connection
   */
  protected Connection openTmpConnection() throws SQLException {
    Connection conn = getDBConnection();
    if (conn == null) {
      if (isThinClient()) {
        String s = "Cannot create an embedded tmp connection in a thin client.";
        s += " Either open a connection or assign the task to a peer.";
        throw new PerfTestException(s);
      } else {
        return GFXDUtil.openBasicEmbeddedConnection();
      }
    } else {
      return conn;
    }
  }

  /**
   * Closes the given connection, but only if it was a temporary one.
   */
  protected void closeTmpConnection(Connection conn) throws SQLException {
    Connection existingConn = getDBConnection();
    if (existingConn == null) {
      conn.close();
    }
  }

  /**
   * Returns the connection from the DB, if it exists.
   */
  protected Connection getDBConnection() {
    if (this.db == null ) {
      return null;
    } else {
      return ((GFXDDB)this.db).getConnection();
    }
  }

  /**
   * Returns true if this is a thin client.
   */
  protected boolean isThinClient() {
    String clientName = RemoteTestModule.getMyClientName();
    Collection<ThinClientDescription> tcds =
      GfxdTestConfig.getInstance().getThinClientDescriptions().values();
    for (ThinClientDescription tcd : tcds) {
      if (tcd.getClientNames().contains(clientName)) {
        return true;
      }
    }
    return false;
  }

//------------------------------------------------------------------------------
// DB initialization and cleanup

  public static void initDBTask() throws DBException, WorkloadException {
    GFXDClient client = new GFXDClient();
    client.initialize();
    client.initDB();
    client.updateHydraThreadLocals();
  }

  protected void initDB() throws DBException {
    this.db = new GFXDDB();
    this.db.init();
    this.gfxdstats = GFXDStats.getInstance();
  }

  public static void cleanupDBTask() throws DBException, InterruptedException {
    GFXDClient client = new GFXDClient();
    client.initialize();
    client.cleanupDB();
    client.updateHydraThreadLocals();
  }

  protected void cleanupDB() throws DBException, InterruptedException {
    if (this.gfxdstats != null) {
      Thread.sleep(2000);
      this.gfxdstats.close();
      this.gfxdstats = null;
    }
    this.db.cleanup();
  }

//------------------------------------------------------------------------------
// Workload initialization

  public static void initWorkloadTask() throws WorkloadException {
    GFXDClient client = new GFXDClient();
    client.initialize();
    client.initWorkload();
    client.updateHydraThreadLocals();
  }

  protected void initWorkload() throws WorkloadException {
    this.workload = new CoreWorkload();
    this.workload.init(null, this.ttgid, this.numThreads);
    this.workloadstate = this.workload.initThread(null);
  }

//------------------------------------------------------------------------------
// DATA LOADING

  public static void loadDataTask() {
    GFXDClient client = new GFXDClient();
    client.initialize();
    client.loadData();
  }

//------------------------------------------------------------------------------
// WORKLOAD

  public static void doWorkloadTask() throws InterruptedException {
    GFXDClient client = new GFXDClient();
    client.initialize();
    client.doWorkload();
  }

//------------------------------------------------------------------------------
// MAJOR COMPACTION

  /**
   * {@inheritDoc}
   * Optionally flushes HDFS AEQs and forces a major compaction. Includes the
   * cost in the workload time.
   */
  protected long completeTask(String trimIntervalName) {
    long timestamp = 0;
    if (this.ttgid == 0) {
      String hdfsStoreConfig = GfxdConfigPrms.getHDFSStoreConfig();
      if (hdfsStoreConfig != null) {
        try {
          if (GFXDPrms.hdfsFlushQueues()) {
            hdfsFlushQueues();
            timestamp = System.currentTimeMillis();
          }
          if (GFXDPrms.hdfsForceCompaction()) {
            hdfsForceCompaction();
            timestamp = System.currentTimeMillis();
          }
        } catch (SQLException e) {
          String s = "Problem completing workload";
          throw new PerfTestException(s, e);
        }
      }
    }
    return timestamp;
  }

  private void hdfsFlushQueues() throws SQLException {
    long start = this.gfxdstats.startHDFSFlushQueues();
    final Connection conn = openTmpConnection();
    List<String> tableNames = getHDFSPartitionedTables();
    try {
      PreparedStatement ps = conn.prepareStatement(HDFS_FLUSH_QUEUE_CALL);
      for (String tableName : tableNames) {
        ps.setString(1, tableName);
        ps.setInt(2, 0); // wait forever
        ps.execute();
      }
      ps.close();
    } finally {
      closeTmpConnection(conn);
    }
    this.gfxdstats.endHDFSFlushQueues(start);
  }

  private void hdfsForceCompaction() throws SQLException {
    long start = this.gfxdstats.startHDFSForceCompaction();
    final Connection conn = openTmpConnection();
    List<String> tableNames = getHDFSPartitionedTables();
    try {
      PreparedStatement ps = conn.prepareStatement(HDFS_FORCE_COMPACTION_CALL);
      for (String tableName : tableNames) {
        ps.setString(1, tableName);
        ps.setInt(2, 0); // wait forever
        ps.execute();
      }
      ps.close();
    } finally {
      closeTmpConnection(conn);
    }
    this.gfxdstats.endHDFSForceCompaction(start);
  }
}
