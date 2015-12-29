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

package hdfs;

import hydra.CacheHelper;
import hydra.ConfigPrms;
import hydra.DistributedSystemHelper;
import hydra.FileUtil;
import hydra.HDFSStoreDescription;
import hydra.HDFSStoreHelper;
import hydra.HadoopDescription;
import hydra.HadoopHelper;
import hydra.HadoopPrms;
import hydra.HostDescription;
import hydra.HydraRuntimeException;
import hydra.HydraTimeoutException;
import hydra.Log;
import hydra.MasterController;
import hydra.PortHelper;
import hydra.Prms;
import hydra.ProcessMgr;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import hydra.blackboard.SharedMap;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import util.SummaryLogListener;
import util.TestException;
import util.TestHelper;
import util.ValueHolder;

import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueStats;
import com.gemstone.gemfire.cache.hdfs.HDFSIOException;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreImpl;
import com.gemstone.gemfire.cache.hdfs.internal.PersistedEventImpl;
import com.gemstone.gemfire.cache.hdfs.internal.UnsortedHoplogPersistedEvent;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.HoplogSetReader.HoplogIterator;
import com.gemstone.gemfire.cache.hdfs.internal.hoplog.SequenceFileHoplog;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerHelper;

/**
 *  Generic utility methods for testing with Hoplogs/HDFS
 */

public class HDFSUtil {

public static final String HDFS_RESULT_REGION = "hdfsResultRegion";

public static void startMapReduceCluster() {
  HadoopHelper.startResourceManager(ConfigPrms.getHadoopConfig());
  HadoopHelper.startNodeManagers(ConfigPrms.getHadoopConfig());
}

public static void stopMapReduceCluster() {
  HadoopHelper.stopNodeManagers(ConfigPrms.getHadoopConfig());
  HadoopHelper.stopResourceManager(ConfigPrms.getHadoopConfig());
}

public static void startCluster() {
  if (HDFSPrms.useExistingCluster()) {
    if (HDFSPrms.manageMapReduceComponents()) {
      startMapReduceCluster();
    }
  } else {
    HadoopHelper.startCluster(ConfigPrms.getHadoopConfig());
  }
}

public static void stopCluster() {
  if (HDFSPrms.useExistingCluster()) {
    if (HDFSPrms.manageMapReduceComponents()) {
      stopMapReduceCluster();
    }
  } else {
    HadoopHelper.stopCluster(ConfigPrms.getHadoopConfig());
  }
}

public static void startHDFSCluster() {
  HadoopHelper.startHDFSCluster(ConfigPrms.getHadoopConfig());
}

public static void stopHDFSCluster() {
  HadoopHelper.stopHDFSCluster(ConfigPrms.getHadoopConfig());
}

public static void configureHDFSTask() {
  HadoopHelper.configureHDFS(ConfigPrms.getHadoopConfig());
}

public static void configureHadoopTask() {
  HadoopHelper.configureHadoop(ConfigPrms.getHadoopConfig());
}

public static void formatNameNodesTask() {
  HadoopHelper.formatNameNodes(ConfigPrms.getHadoopConfig());
}

public static void startNameNodesTask() {
  HadoopHelper.startNameNodes(ConfigPrms.getHadoopConfig());
}

public static void startDataNodesTask() {
  HadoopHelper.startDataNodes(ConfigPrms.getHadoopConfig());
}

public static synchronized void restartCluster() {
  String hadoopConfig = ConfigPrms.getHadoopConfig();
  MasterController.sleepForMs( HDFSPrms.hadoopStartWaitSec() * 1000 );
  HadoopHelper.startNameNodes(hadoopConfig);
  HadoopHelper.startDataNodes(hadoopConfig);
  HadoopHelper.startResourceManager(hadoopConfig);
  HadoopHelper.startNodeManagers(hadoopConfig);
  // give hadoop 30 seconds to get out of SafeMode
  MasterController.sleepForMs( HDFSPrms.hadoopReturnWaitSec() * 1000 );
}

public static void stopNameNodesTask() {
  HadoopHelper.stopNameNodes(ConfigPrms.getHadoopConfig());
}

public static void stopDataNodesTask() {
  HadoopHelper.stopDataNodes(ConfigPrms.getHadoopConfig());
}

public static void configureAndStartHadoop() {
  String hadoopConfig = ConfigPrms.getHadoopConfig();
  HadoopHelper.configureHadoop(hadoopConfig);
  HadoopHelper.startCluster(hadoopConfig);
}

public static void stopStartDataNodes() {
  String hadoopConfig = ConfigPrms.getHadoopConfig();
  MasterController.sleepForMs( HDFSPrms.hadoopStopWaitSec() * 1000 );
  HDFSUtilBB.getBB().getSharedCounters().increment(HDFSUtilBB.recycleInProgress);
  HadoopHelper.stopDataNodes(hadoopConfig);
  MasterController.sleepForMs( HDFSPrms.hadoopStartWaitSec() * 1000 );
  HadoopHelper.startDataNodes(hadoopConfig);
  MasterController.sleepForMs( HDFSPrms.hadoopReturnWaitSec() * 1000 );
  HDFSUtilBB.getBB().getSharedCounters().zero(HDFSUtilBB.recycleInProgress);
}

public static void stopStartNameNodes() {
  String hadoopConfig = ConfigPrms.getHadoopConfig();
  MasterController.sleepForMs( HDFSPrms.hadoopStopWaitSec() * 1000 );
  HDFSUtilBB.getBB().getSharedCounters().increment(HDFSUtilBB.recycleInProgress);
  HadoopHelper.stopNameNodes(hadoopConfig);
  MasterController.sleepForMs( HDFSPrms.hadoopStartWaitSec() * 1000 );
  HadoopHelper.startNameNodes(hadoopConfig);
  MasterController.sleepForMs( HDFSPrms.hadoopReturnWaitSec() * 1000 );
  HDFSUtilBB.getBB().getSharedCounters().zero(HDFSUtilBB.recycleInProgress);
}

public static void recycleDataNode() {
  String hadoopConfig = ConfigPrms.getHadoopConfig();
  HadoopDescription hdd = HadoopHelper.getHadoopDescription(hadoopConfig);
  List<HadoopDescription.DataNodeDescription> dnds = hdd.getDataNodeDescriptions();
  if (dnds.isEmpty()) {
    String s = "Cannot recycle data node, dataNodeDescriptions is empty";
    throw new HydraRuntimeException(s);
  }
  HadoopDescription.DataNodeDescription dnd = dnds.get(0);
  HostDescription hd = dnd.getHostDescription();
  String host = dnd.getHostName();

  String pidfn = dnd.getPIDDir() + "/hadoop-" + System.getProperty("user.name") + "-datanode.pid";
  // override if secure hdfs
  if (hdd.isSecure()) {
    Log.getLogWriter().info("Using secure mode (kerberos) as root");
    pidfn = dnd.getPIDDir() + "/hadoop_secure_dn.pid";
  } 

  Integer pid = getPid(pidfn);
  if (!ProcessMgr.processExists(host, pid)) {
   String s = pid + " for data node is not running on " + host
            + " see " + dnd.getLogDir() + " for output";
   throw new HydraRuntimeException(s);
  }

  Log.getLogWriter().info("Killing data node on host " + host
                           + " pid = " + pid + ", see "
                           + dnd.getLogDir() + " for output");
  MasterController.sleepForMs(HDFSPrms.hadoopStopWaitSec() * 1000);

  Log.getLogWriter().info("Killing data node on host " + host
                           + " pid = " + pid + ", see "
                           + dnd.getLogDir() + " for output");

  HDFSUtilBB.getBB().getSharedCounters().increment(HDFSUtilBB.recycleInProgress);
  if (hdd.isSecure()) {   // do not do a mean kill here or jsvc will spawn another data node
    ProcessMgr.shutdownProcess(host, pid);
  } else {
    ProcessMgr.killProcess(host, pid);
  }

  Log.getLogWriter().info("Killed data node on host " + host
                           + " pid = " + pid + ", see "
                           + dnd.getLogDir() + " for output");

  MasterController.sleepForMs(HDFSPrms.hadoopStartWaitSec() * 1000);
  Log.getLogWriter().info("Restarting data node on host " + host
                          + " see " + dnd.getLogDir() + " for output");
  HadoopHelper.startDataNode(hadoopConfig, host);
  Log.getLogWriter().info("Restarted data node on host " + host
                          + " see " + dnd.getLogDir() + " for output");
  MasterController.sleepForMs(HDFSPrms.hadoopReturnWaitSec() * 1000);
  HDFSUtilBB.getBB().getSharedCounters().zero(HDFSUtilBB.recycleInProgress);
}

public static void recycleNameNode() {
  String hadoopConfig = ConfigPrms.getHadoopConfig();
  HadoopDescription hd = HadoopHelper.getHadoopDescription(hadoopConfig);
  List<HadoopDescription.NameNodeDescription> nnds = hd.getNameNodeDescriptions();
  if (nnds.isEmpty()) {
    String s = "Cannot recycle name node, nameNodeDescriptions is empty";
    throw new HydraRuntimeException(s);
  }
  HadoopDescription.NameNodeDescription nnd = nnds.get(0);
  String host = nnd.getHostName();

  String pidfn = nnd.getPIDDir() + "/hadoop-" + System.getProperty("user.name") + "-namenode.pid";
  Integer pid = getPid(pidfn);
  if (!ProcessMgr.processExists(host, pid)) {
    String s = pid + " for name node is not running on " + host 
             + " see " + nnd.getLogDir() + " for output";
    throw new HydraRuntimeException(s);
  }
   
  Log.getLogWriter().info("Killing name node on host " + host
                          + " pid = " + pid + ", see "
                          + nnd.getLogDir() + " for output");
  MasterController.sleepForMs(HDFSPrms.hadoopStopWaitSec() * 1000);

  HDFSUtilBB.getBB().getSharedCounters().increment(HDFSUtilBB.recycleInProgress);
  ProcessMgr.killProcess(host, pid);
  Log.getLogWriter().info("Killed name node on host " + host
                          + " pid = " + pid + ", see "
                          + nnd.getLogDir() + " for output");

  MasterController.sleepForMs(HDFSPrms.hadoopStartWaitSec() * 1000);
  Log.getLogWriter().info("Restarting name node on host " + host
                          + " see " + nnd.getLogDir() + " for output");
  HadoopHelper.startNameNode(hadoopConfig, host);
  Log.getLogWriter().info("Restarted name node on host " + host
                          + " see " + nnd.getLogDir() + " for output");
  MasterController.sleepForMs(HDFSPrms.hadoopReturnWaitSec() * 1000);
  HDFSUtilBB.getBB().getSharedCounters().zero(HDFSUtilBB.recycleInProgress);
}

private static Integer getPid(String pidfn) {
  Integer pid = null;
  try {
    pid = new Integer(FileUtil.getContents(pidfn));
  } catch (NumberFormatException e) {
    String s = "Cannot read PID from file: " + pidfn;
    throw new HydraRuntimeException(s, e);
  } catch (RemoteException e) {
    String s = "Cannot read PID from file: " + pidfn;
    throw new HydraRuntimeException(s, e);
  } catch (IOException e) {
    String s = "Cannot read PID file: " + pidfn;
    throw new HydraRuntimeException(s, e);
  }
  return pid;
}

/** Utility method invoked by HydraTask_loadDataFromHDFS() for various streaming tests (e.g.  and ParRegTest).
 *  Given the the original region name (so we can access in HDFS), load a region with the operations stored 
 *  in HDFS.  This is done in order (per bucket) ... so we should end up with the same region contents as the main workload vms.
 */
public static void loadDataFromHDFS(Region aRegion, String regionName) {
  Cache aCache = CacheHelper.getCache();
  GemFireCacheImpl cacheImpl = (GemFireCacheImpl)aCache;
  String hdfsStoreName = aRegion.getAttributes().getHDFSStoreName();
  HDFSStoreImpl storeImpl = (HDFSStoreImpl) cacheImpl.findHDFSStore(hdfsStoreName);
  Log.getLogWriter().info("validate() invoked for " + hdfsStoreName);

  FileSystem fs;
  try {
    fs = storeImpl.getFileSystem();
  } catch (IOException e1) {
    throw new HDFSIOException(e1.getMessage(), e1);
  }
  Log.getLogWriter().info("creating region based on files in HDFS directory: " + storeImpl.getHomeDir());
  try {
    Path basePath = new Path(storeImpl.getHomeDir());
    String pathPattern = regionName + "/*/*.*";

    String sep = File.separator;
    String hadoopCmd = " fs -ls " + basePath + sep + pathPattern;

    int vmid = RemoteTestModule.getMyVmid();
    String clientName = RemoteTestModule.getMyClientName();
    String host = RemoteTestModule.getMyHost();
    HostDescription hd = TestConfig.getInstance().getClientDescription( clientName ).getVmDescription().getHostDescription();
    String logfn = hd.getUserDir() + sep + "vm_" + vmid + "_" + clientName + "_" + host + "_loadDataFromHDFS.log";

    HadoopDescription hdd = HadoopHelper.getHadoopDescription(ConfigPrms.getHadoopConfig());
    if (hdd.isSecure()) { // kinit is required for mapreduce ... even with auth.to.local mapping
      Log.getLogWriter().info("HDFSUtil.loadDataFromHDFS()-execute kinit in secure mode");
      String userKinit = HadoopHelper.GFXD_SECURE_KEYTAB_FILE + " gfxd-secure@GEMSTONE.COM";
      HadoopHelper.executeKinitCommand(host, userKinit, 120);
    }

    HadoopHelper.runHadoopCommand(ConfigPrms.getHadoopConfig(), hadoopCmd, logfn);
    Path regionPath = new Path(basePath, pathPattern);
    FileStatus[] sortedListOfHoplogs = fs.globStatus(regionPath);
    
    for (int i = 0; i < sortedListOfHoplogs.length; i++) {
      Path hoplogPath = sortedListOfHoplogs[i].getPath();
      Log.getLogWriter().fine("Processing hoplog " + hoplogPath);
      try {
        loadSequenceFile(aRegion, fs, hoplogPath);
      } catch (FileNotFoundException fnfe) {
        if (hoplogPath.toString().endsWith(".tmp")) {
          Path newHoplogPath = getBaseHoplogPath(hoplogPath);  // returns the hoplogPath without a ".tmp" extension
          Log.getLogWriter().info("loadDataFromHDFS caught FileNotFoundException while processing " + hoplogPath + ", retrying with base filename " + newHoplogPath);
          loadSequenceFile(aRegion, fs, newHoplogPath);
        } else { 
          throw new TestException("loadDataFromHDFS caught unexpected Exception for SequenceHoplog which is not a tmp file" + fnfe + "\n" + TestHelper.getStackTrace(fnfe));
        }
      }
    }
  } catch (TestException e) {
    throw e;
  } catch (Exception e) {
    throw new TestException("loadDataFromHDFS caught unexpected Exception " + e + ": " + TestHelper.getStackTrace(e));
  } finally {
    // if running with auth.to.local mapping, remove kerberos ticket created for running secure hdfs command
    HadoopDescription hdd = HadoopHelper.getHadoopDescription(ConfigPrms.getHadoopConfig());
    if (hdd.getSecurityAuthentication().equals(HadoopPrms.KERBEROS)) {
      HadoopHelper.executeKdestroyCommand(RemoteTestModule.getMyHost(), 120);
    }
  }
}

private static Path getBaseHoplogPath(Path hoplogPath) {
  String originalFilename = hoplogPath.toString();
  int tmpExtIndex = originalFilename.lastIndexOf(".tmp");
  String trimmedFilename = originalFilename.substring(0, tmpExtIndex);
  return new Path(trimmedFilename);
}

public static void loadSequenceFile(Region aRegion, FileSystem inputFS, Path sequenceFileName) throws Exception {
  SequenceFileHoplog hoplog = new SequenceFileHoplog(inputFS, sequenceFileName, null);
  HoplogIterator<byte[], byte[]> iter = hoplog.getReader().scan();
  boolean isSerialExecution = TestConfig.tab().booleanAt(hydra.Prms.serialExecution);

  while (iter.hasNext()) {
    iter.next();
    PersistedEventImpl te = UnsortedHoplogPersistedEvent.fromBytes((iter.getValue()));
    String stringkey = ((String)CacheServerHelper.deserialize(iter.getKey()));

    Operation op = te.getOperation();
    ValueHolder value = null;

    Object displayVal = null;
    if (te != null) {
      displayVal = (ValueHolder)te.getDeserializedValue();
    }
    Log.getLogWriter().fine("loadSequenceFile record: " + op.toString() + ": key = " + stringkey + ", value = " + displayVal);

    try {
      if (op.isCreate()) {
        aRegion.put(stringkey, (ValueHolder)te.getDeserializedValue()); 
      } else if (op.isUpdate()) {
        aRegion.put(stringkey, (ValueHolder)te.getDeserializedValue()); 
      }  else if (op.isDestroy()) {
        aRegion.destroy(stringkey);
      } else {
         Log.getLogWriter().info("StreamingValidator.loadSequenceFile(): Unexpected operation " + op.toString() + " : " + TestHelper.getStackTrace());
      }
    } catch (EntryNotFoundException e) {
      // If the operation is destroy and is a possibleDuplicate, ignore the entry not found exception,
      if (op.isDestroy() && te.isPossibleDuplicate()) {
        Log.getLogWriter().info("loadSequenceFile caught EntryNotFoundException for " + stringkey);
        continue;
      } else if (isSerialExecution) { // shouldn't happen with serial execution
        Log.getLogWriter().info("loadSequenceFile caught " + e + ":" +  TestHelper.getStackTrace(e));
        throw new TestException("loadSequenceFile caught unexpected Exception " + e + ":" + TestHelper.getStackTrace(e));
      }
    }
  }
  iter.close();
  hoplog.close();
}

/** Launch the MapReduce program specified by HDFSPrms.mapReduceClass 
 *  by fgexec'ing a comand of the form:
 *    yarn --config jar <MapReduce>.class libjars <command-seperated-list of jars> <locator-host> <locator-port> <hdfs-home-dir> <test specific args>
 */
public static void execMapReduceJob() {
  String extraArgs = null;
  execMapReduceJob(extraArgs);
}

public static void execMapReduceJob(String extraArgs) {
  String mapReduceClassName = HDFSPrms.getMapReduceClassName();

  try {
    execMapReduceJob(mapReduceClassName, extraArgs);
  } finally {
    // if running without kinit, remove ticket created for mapreduce job
    HadoopDescription hdd = HadoopHelper.getHadoopDescription(ConfigPrms.getHadoopConfig());
    if (hdd.getSecurityAuthentication().equals(HadoopPrms.KERBEROS)) {
      HadoopHelper.executeKdestroyCommand(RemoteTestModule.getMyHost(), 120);
    }
  }
}

public static void execMapReduceJob(String mapReduceClassName, String extraArgs) {
  // Use the first locator for this distributed system
  List<DistributedSystemHelper.Endpoint> endpoints = DistributedSystemHelper.getSystemEndpoints();
  DistributedSystemHelper.Endpoint endpoint = endpoints.get(0);
  String locatorHost = endpoint.getHost();
  int locatorPort = endpoint.getPort();

  String sep = File.separator;
  String jtests = System.getProperty("JTESTS");
  String MRJarPath = jtests + sep + ".." + sep + "extraJars" + sep + "mapreduce.jar";
  String gemfireJarPath = jtests + sep + ".." + sep + ".." + sep + "product" + sep + "lib" + sep + "gemfire.jar";

  String gemfireLibPath = jtests + sep + ".." + sep + ".." + sep + "product" + sep + "lib";
  String hbaseJarPath = getHbaseJar(gemfireLibPath);

  String hdfsStoreConfig = ConfigPrms.getHDFSStoreConfig();
  HDFSStoreDescription hsd = HDFSStoreHelper.getHDFSStoreDescription(hdfsStoreConfig);
  String hdfsStoreName = hsd.getName();
  HadoopDescription hdd = HadoopHelper.getHadoopDescription(ConfigPrms.getHadoopConfig());
  String confDir = hdd.getResourceManagerDescription().getConfDir();
  String hdfsHomeDir = HDFSStoreHelper.getHDFSStore(hdfsStoreName).getHomeDir();

  String cmd = "env CLASSPATH=" + System.getProperty( "java.class.path" ) + " ";
  cmd += "env HADOOP_CLASSPATH=" + System.getProperty( "java.class.path" ) + " ";
  cmd += hdd.getHadoopDist() + sep + "bin" + sep + "yarn ";    
  cmd += "--config " + confDir + " ";
  cmd += "jar " + MRJarPath + " ";
  cmd += mapReduceClassName + " ";
  cmd += " -libjars " + " " + MRJarPath + "," + gemfireJarPath + "," + hbaseJarPath + " ";
  cmd += locatorHost + " " + locatorPort + " " + hdfsHomeDir + " ";
  if (extraArgs != null) {
    cmd += extraArgs;
  }
  Log.getLogWriter().info("Executing " + cmd + "...");
   
  int vmid = RemoteTestModule.getMyVmid();
  String clientName = RemoteTestModule.getMyClientName();
  String host = RemoteTestModule.getMyHost();
  HostDescription hd = TestConfig.getInstance().getClientDescription( clientName ).getVmDescription().getHostDescription();

  String logfn = hd.getUserDir() + sep + "vm_" + vmid + "_" + clientName + "_" + host + "_" + mapReduceClassName + ".log"; 

  if (hdd.isSecure()) {   // execute kinit when running mapreduce, even when using auth.to.local mapping
    String userKinit = HadoopHelper.GFXD_SECURE_KEYTAB_FILE + " gfxd-secure@GEMSTONE.COM";
    HadoopHelper.executeKinitCommand(host, userKinit, 120);
  }

  int pid = ProcessMgr.bgexec(cmd, hd.getUserDir(), logfn);
  try {
    RemoteTestModule.Master.recordHDFSPIDNoDumps(hd, pid, false);
  } catch (RemoteException e) {
    String s = "Failed to record PID: " + pid;
    throw new HydraRuntimeException(s, e);
  }
  int maxWaitSec = (int)TestConfig.tab().longAt( Prms.maxResultWaitSec );
  if (!ProcessMgr.waitForDeath(host, pid, maxWaitSec)) {
    String s = "Waited more than " + maxWaitSec + " seconds for MapReduce Job";
    throw new HydraTimeoutException(s);
  }
  try {
    RemoteTestModule.Master.removeHDFSPIDNoDumps(hd, pid, false);
  } catch (RemoteException e) {
    Log.getLogWriter().info("execMapReduceJob caught " + e + ": " + TestHelper.getStackTrace(e));
    String s = "Failed to remove PID: " + pid;
    throw new HydraRuntimeException(s, e);
  }
  Log.getLogWriter().info("Completed MapReduce job  on host " + host + 
                          " using command: " + cmd + 
                          ", see " + logfn + " for output");
}

/** Returns the current hbase*gemfire*.jar file.
 *  Current file is: 
 *     $GEMFIRE/lib/hbase-0.94.4-gemfire-r43816.jar           
 */
private static String getHbaseJar(String gemfireLibPath) {
  class HbaseJarFilter implements FileFilter {
    public boolean accept(File fn) {
     return fn.getName().startsWith("hbase") && 
            fn.getName().contains("gemfire") &&
            fn.getName().endsWith(".jar");
    }
  };
  List<File> hbaseJars = FileUtil.getFiles(new File(gemfireLibPath), 
                                           new HbaseJarFilter(), 
                                           false);
  if (hbaseJars.size() != 1) {
    throw new TestException("TestException: cannot uniquely identify gemfire/lib/hbase*gemfire*.jar, please check for a change in hbase jar file naming");
  }
  File matchingJarFile = hbaseJars.get(0);
  return matchingJarFile.getAbsolutePath();
}

/** Exec a mapReduce job to find all updates for a particular entry and write 
 *  those entries back out to a GemFire region (HDFS_RESULTS_REGION) as
 *  ("originalKey_EventNumber_Operation", value).  Note that destroyed keys have a value
 *  of "DESTORYED".  Creates the HDFSResultRegion if needed.
 */
public static synchronized void getAllHDFSEventsForKey(String keys) {
  Cache cache = CacheHelper.getCache();
  if (cache.getRegion(HDFS_RESULT_REGION) == null) {
    // make this a replicated region (so it doesn't affect rebalance, waitForRecovery, etc)
    Region hdfsResultRegion = cache.createRegionFactory(RegionShortcut.REPLICATE).
                addCacheListener(new SummaryLogListener()).
                create(HDFS_RESULT_REGION);
    Log.getLogWriter().info("Created hdfsResultRegion " + hdfsResultRegion.getFullPath() + " for dumping HDFS updates for " + keys);
    CacheServer cs = cache.addCacheServer();
    int port = PortHelper.getRandomPort();
    cs.setPort(port);
    try {
      cs.start();
      Log.getLogWriter().info("Started server listenening on port " + port);
    } catch (IOException e) {
      throw new TestException("getAllHDSEventsForKey caught " + e + " while starting a CacheServer " + TestHelper.getStackTrace(e));
    }
  }
  execMapReduceJob("hdfs.mapreduce.GetAllHDFSEventsForKey", keys);
}

/** Dump the HDFSResultRegion, created by the GetAllHDFSEventsForKey() mapreduce job.
 *  Useful for debugging lost events, etc in HDFS
 */
public static void dumpHDFSResultRegion() {
  StringBuffer hdfsOps = new StringBuffer();
  Region hdfsResultRegion = CacheHelper.getCache().getRegion(HDFS_RESULT_REGION);
  // We only create this if we need it, so perhaps there were no validation errors
  if (hdfsResultRegion == null) return;
  Set keySet = new TreeSet(hdfsResultRegion.keySet());
  for (Iterator it = keySet.iterator(); it.hasNext();) {
    String key = (String)it.next();
    Object o = hdfsResultRegion.get(key);
    hdfsOps.append("  " + key + ": " + o.toString() + "\n");
  }
  Log.getLogWriter().info("Contents of HDFSResultRegion = \n" + hdfsOps.toString());
}

public static String getHDFSStoreName(Region aRegion) {
  RegionAttributes attrs = aRegion.getAttributes();
  return attrs.getHDFSStoreName();
}

/**
 *  Utility methods to monitor and wait for HDFS AEQ to drain
 */
public static boolean serverAlive = true;

/**
 * Starts a thread to monitor HDFS AEQ queues (may be active across multiple vms).
 */
  public static synchronized void HydraTask_startQueueMonitor() {
    final StatisticsFactory statFactory = DistributedSystemHelper.getDistributedSystem();
    final String key = "AsyncEventQueueStatistics: eventQueueSize for vm_" + RemoteTestModule.getMyVmid();
    Log.getLogWriter().info("Starting event queue monitor with key: " + key);
    final SharedMap bb = HDFSBlackboard.getInstance().getSharedMap();

    Thread queueMonitor = new Thread(new Runnable() {

      public void run() {
        Statistics[] qStats = statFactory.findStatisticsByType(statFactory.findType(AsyncEventQueueStats.typeName));
        while(serverAlive) {
          SystemFailure.checkFailure(); // GemStoneAddition
          long qSize = 0;
          for(int i=0;i<qStats.length;i++) {
            qSize += qStats[i].getInt(AsyncEventQueueStats.getEventQueueSizeId());
          }
          Log.getLogWriter().fine("Updating " + key + " with eventQueueSize = " + qSize);
          bb.put(key, new Long(qSize));

          if(serverAlive) {
            try {
              Thread.sleep(1000);
            } catch(Exception e) {
              e.printStackTrace();
            }
          }
        }
      }

      protected void finalize() throws Throwable {
        Object o = bb.remove(key);
        Log.getLogWriter().severe("Removing HDFSBlackboard key " + key + " with value (HDFS AEQ size) = " + o);
        super.finalize();
      }

    }, "HDFS AEQ Monitor");

    queueMonitor.setDaemon(true);
    queueMonitor.start();
  }

/**
 * Waits {@link hydra.Prms#maxResultWaitSec} for HDFS AEQs to drain.
 * Suitable for use as a helper method or hydra task.
 */
  public static synchronized void HydraTask_waitForQueuesToDrain() {

    final SharedMap bb = HDFSBlackboard.getInstance().getSharedMap();
    long startTime = System.currentTimeMillis();

    HDFSStoreDescription hsd = HDFSStoreHelper.getHDFSStoreDescription(ConfigPrms.getHDFSStoreConfig());
    long maxWait = hsd.getBatchTimeInterval() + 180000;  // wait for batchTimeInterval(ms) + 3 minutes
    long entriesLeft = 0;
    while((System.currentTimeMillis()-startTime)<(maxWait)) {
      Set keySet = bb.getMap().keySet();
      Iterator kIt = keySet.iterator();
      boolean pass = true;
      while(kIt.hasNext()) {
        String k = (String)kIt.next();
        if(k.startsWith("AsyncEventQueueStatistics: eventQueueSize")) {
          Long value = (Long)bb.get(k);
          Log.getLogWriter().info("Checking event queue key: " + k + " found eventQueueSize = " + value);
          if(value.longValue()>0) {
            pass = false;
            entriesLeft = value.longValue();
            Log.getLogWriter().warning("Still waiting for " + k + " to drain. " + "Current value is " + value);
            break;
          }
        }
      }
      if(pass) {
        entriesLeft = 0;
        break;
      } else {
        try {
          Thread.sleep(1000);
        } catch(Exception e) {
          e.printStackTrace();
        }
      }
    }
    if(entriesLeft>0) {
      //Log.getLogWriter().info("TIMED OUT waiting for HDFS AEQ to drain.  eventQueueSize =  " + entriesLeft);
      throw new TestException("TIMED OUT waiting for HDFS AEQ to drain.  eventQueueSize =  " + entriesLeft);
    } else {
      Log.getLogWriter().info("QUEUES ARE DRAINED");
    }
  }
}
