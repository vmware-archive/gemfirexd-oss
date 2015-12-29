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
package sql.dataextractor;

import hydra.ClientVmInfo;
import hydra.FileUtil;
import hydra.HydraVector;
import hydra.Log;
import hydra.MasterController;
import hydra.RemoteTestModule;
import hydra.StopSchedulingTaskOnClientOrder;
import hydra.TestConfig;
import hydra.VmDescription;
import hydra.blackboard.SharedMap;
import hydra.gemfirexd.HDFSStorePrms;
import hydra.gemfirexd.NetworkServerBlackboard;
import hydra.gemfirexd.NetworkServerHelper.Endpoint;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;

import memscale.OffHeapHelper;
import sql.SQLHelper;
import sql.SQLTest;
import sql.ddlStatements.DDLStmtIF;
import util.StopStartVMs;
import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.internal.offheap.OffHeapMemoryStats;
import com.pivotal.gemfirexd.tools.dataextractor.GemFireXDDataExtractor;
import com.pivotal.gemfirexd.tools.dataextractor.GemFireXDDataExtractorLoader;

public class DataExtractorTest extends SQLTest {
  static HydraVector threadGroupNames = null;
  static HydraVector clientVMsToRestart = null;
  static String[] ddlCreateTableStatements = null;
  static String[] ddlCreateTableExtensions = null;
  static int threadCount = 0;
  static protected DataExtractorTest extractorTest = null; 
  static Random rand = new Random();
  List<ClientVmInfo> restartVMs = new ArrayList<ClientVmInfo>();
  String propFileAbsolutePath = null;
  static boolean performUpdatesWhileShuttingDown = true;
  static boolean simultaneousShutdownVMs = false;
  static final String RANDOM_CORRUPTION_STRING = "THIS IS RANDOM STRING TO CAUSE CORRUPTION";
  static ClientVmInfo lastStandingVmInfo = null;
  static ClientVmInfo secondLastStandingVmInfo = null;
  public static synchronized void HydraTask_initialize() {
    if (extractorTest == null) {
      extractorTest = new DataExtractorTest();
    }
   
    if (threadGroupNames == null) {
      threadGroupNames = TestConfig.tab().vecAt(
          DataExtractorPrms.threadGroupNames);
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
    performUpdatesWhileShuttingDown = TestConfig.tab().booleanAt(DataExtractorPrms.performUpdatesWhileShuttingDown, true);
    
    String clientName = RemoteTestModule.getMyClientName();
    Log.getLogWriter().info("My client name: " + clientName);
    Vector statements = TestConfig.tab().vecAt(DataExtractorPrms.ddlCreateTableStatements, new HydraVector());
    Vector extensions = TestConfig.tab().vecAt(DataExtractorPrms.ddlCreateTableExtensions, new HydraVector());
    ddlCreateTableStatements = new String[statements.size()];
    ddlCreateTableExtensions = new String[extensions.size()];
    for (int i = 0; i < statements.size(); i++) {
      ddlCreateTableStatements[i] = (String)statements.elementAt(i);
      ddlCreateTableExtensions[i] = (String)extensions.elementAt(i);
    }
    HydraTask_initializeFabricServer();
  }
  
  public static void HydraTask_doOperations() {
    extractorTest.doOperations();
  }
  
  private void doOperations() {
    if (DataExtractorBB.getBB().getSharedCounters().read(DataExtractorBB.recyclingStarted) != 0) {
      long recycledAll = DataExtractorBB.getBB().getSharedCounters().read(DataExtractorBB.recycledAllVMs);
      if (recycledAll != 0) {
        // It means this is a restarted VM.
        // We dont want to do any operations in a restarted VM.
        throw new StopSchedulingTaskOnClientOrder("Already Recycled VM, so no DML/DDL operations to be done");
      } else {
        // This is a VM which is not yet scheduled for recycling.
        if (!performUpdatesWhileShuttingDown) {
          // Throw stop scheduling only if perform updates while shutting down is false.
          throw new StopSchedulingTaskOnClientOrder("Recycling started");
        }
      }
    }

    long opsTaskGranularitySec = TestConfig.tab().longAt(DataExtractorPrms.opsTaskGranularitySec);
    long minTaskGranularityMS = opsTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
    performOps(minTaskGranularityMS);
    if (DataExtractorBB.getBB().getSharedCounters().read(DataExtractorBB.recycledAllVMs) != 0) {
      throw new StopSchedulingTaskOnClientOrder("All vms have paused");
    }
  }
  
  private void performOps(long taskTimeMS) {
    long startTime = System.currentTimeMillis();
    boolean performDDLOps = TestConfig.tab().booleanAt(DataExtractorPrms.performDDLOps, false);
    do {
      if (performDDLOps && rand.nextInt(10) > 8) {
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
      Log.getLogWriter().info("RemoteTestModule.getCurrentThread().getThreadId(): " + RemoteTestModule.getCurrentThread().getThreadId());
      Log.getLogWriter().info("RemoteTestModule.getMyVmid(): " + RemoteTestModule.getMyVmid());
      Log.getLogWriter().info("RemoteTestModule.getMyBaseThreadId(): " + RemoteTestModule.getMyBaseThreadId());
      int DDLOpsBatchSize = TestConfig.tab().intAt(DataExtractorPrms.DDLOpsBatchSize, 1);
      for (int i = 0; i < DDLOpsBatchSize; i++) {
        String UniqueId = "_"+ RemoteTestModule.getMyVmid() + "_"+RemoteTestModule.getCurrentThread().getThreadId() + "_op_"+i;
        dropCreateAlterTables(dConn, gConn, UniqueId);
      }
    }
  }
  
  protected void dropCreateAlterTables(Connection dConn, Connection gConn, String UniqueId) {
    try {
      // perform create and drop table operations.
      int i = random.nextInt(ddlCreateTableStatements.length);
      Statement gStmt = gConn.createStatement();
      Statement dStmt = dConn.createStatement();
      Log.getLogWriter().info("Dropping the sql table trade.temp "+UniqueId+" (if exists)");
      gStmt.execute("drop table if exists "+ "trade.temp" + UniqueId);
      
      try {
        dStmt.execute("drop table "+ "trade.temp" + UniqueId);
      } catch (SQLException e) {
        Log.getLogWriter().warning("Derby drop table threw an exception + " + e);
      }
      // 1 of 3 times do only drop table and no create table
      if (random.nextInt(3) > 0) {
        Log.getLogWriter().info("Creating the sql table trade.temp" + UniqueId);
        String sqlCommandGFE = "create table " + "trade.temp"+ UniqueId + " "+ ddlCreateTableStatements[i] +" " + ddlCreateTableExtensions[i];
        String sqlCommandDerby = "create table " + "trade.temp"+ UniqueId + " "+ ddlCreateTableStatements[i];
        Log.getLogWriter().info("Commiting the sql create table");
        Log.getLogWriter().info("sqlCommandGFE: " + sqlCommandGFE);
        Log.getLogWriter().info("sqlCommandDerby: " + sqlCommandDerby);
        gStmt.execute(sqlCommandGFE);
        dStmt.execute(sqlCommandDerby);
        // 1 out of 2 times do alter table.
        if (random.nextInt() == 0) {
          Log.getLogWriter().info("Altering the sql table trade.temp" + UniqueId);
          String sqlAlterCommand = "alter table " + "trade.temp"+ UniqueId + " " + " drop column salary";
          //String sqlCommandDerby = "alter table " + "trade.temp"+ UniqueId + " "+ ddlCreateTableStatements[i];
          Log.getLogWriter().info("Commiting the sql alter table");
          Log.getLogWriter().info("sqlAlterCommand: " + sqlAlterCommand);
          gStmt.execute(sqlAlterCommand);
          dStmt.execute(sqlAlterCommand);
        }
      }
      commit(dConn);
      commit(gConn);
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }
  
  /** Initialize the compatibility test controller
   * 
   */
  public synchronized static void HydraTask_initController() {
    if (extractorTest == null) {
      extractorTest = new DataExtractorTest();  
    }
    
    clientVMsToRestart = TestConfig.tab().vecAt(
        DataExtractorPrms.clientVMNamesForRestart);
    if (clientVMsToRestart == null) {
      throw new TestException ("No client vm names specified for restart sequence");
    }
    simultaneousShutdownVMs = TestConfig.tab().booleanAt(DataExtractorPrms.simultaneousShutdownVMs, false);
    extractorTest.simultaneousShutdownVMs = simultaneousShutdownVMs;
    if (simultaneousShutdownVMs) {
      performUpdatesWhileShuttingDown = true;
    }
    for (int i = 0; i < clientVMsToRestart.size(); i++) {
      extractorTest.restartVMs.addAll(StopStartVMs.getMatchVMs(StopStartVMs.getAllVMs(), clientVMsToRestart.get(i).toString()));
    }
  }
  
  public static void HydraTask_Controller_OnlyStopStartVMs() throws Exception {
    Log.getLogWriter().info("Stopping Vms now");
    extractorTest.shutDownVMs();
    Log.getLogWriter().info("Starting Vms now");
    StopStartVMs.startVMs(extractorTest.restartVMs);
  }
  
  public static void HydraTask_Controller() throws Exception {
    Log.getLogWriter().info("Stopping Vms now");
    extractorTest.shutDownVMs();
    extractorTest.generateProperyFile();
    extractorTest.runExtractorTool();
    extractorTest.clearPersistentDirs();
    Log.getLogWriter().info("Starting Vms now");
    StopStartVMs.startVMs(extractorTest.restartVMs);
    loadUsingRecommendedFile();
  }
  
  public static void HydraTask_Controller_Data_Corrupt() throws Exception {
    Log.getLogWriter().info("Stopping Vms now");
    extractorTest.shutDownVMs();
    extractorTest.generateProperyFile();
    corruptOrDeleteDataFiles();
    extractorTest.runExtractorTool();
    verifySecondLastStandingVM();
  }
  
  public static void HydraTask_Controller_Data_Corrupt_verifyDerby() throws Exception {
    Log.getLogWriter().info("Stopping Vms now");
    extractorTest.shutDownVMs();
    extractorTest.generateProperyFile();
    corruptOrDeleteDataFiles();
    extractorTest.runExtractorTool();
    verifyCorruptedVMNotUsed();
    extractorTest.clearPersistentDirs();
    Log.getLogWriter().info("Starting Vms now");
    StopStartVMs.startVMs(extractorTest.restartVMs);
    loadUsingRecommendedFile();
  }
  
  public static void verifySecondLastStandingVM() throws Exception {
    String userDir = System.getProperty("user.dir");
    String recommendedFile = userDir + File.separator + getExtractedFilesDirName() + File.separator + "Recommended.txt";
    File recFile = new File(recommendedFile);
    int lineCount = 1;
    BufferedReader reader = new BufferedReader(new FileReader(recommendedFile));
    String line = reader.readLine();
    line = reader.readLine();
    String name = "vm_"+secondLastStandingVmInfo.getVmid()+"_"+secondLastStandingVmInfo.getClientName();
    while (line != null) {
      if (!line.contains(name)) {
        throw new TestException("An entry at line#" + lineCount +" in recommended.txt does not contain "
            + name
            + " i.e. second-last standing VM. Possibly a wrong csv was recommended. Line content: " +line);
      }
      line = reader.readLine();
      lineCount++;
    }
  }
  
  public static void verifyCorruptedVMNotUsed() throws Exception {
    String userDir = System.getProperty("user.dir");
    String recommendedFile = userDir + File.separator + getExtractedFilesDirName() + File.separator + "Recommended.txt";
    File recFile = new File(recommendedFile);
    int lineCount = 1;
    BufferedReader reader = new BufferedReader(new FileReader(recommendedFile));
    String line = reader.readLine();
    line = reader.readLine();
    String name = "vm_"+lastStandingVmInfo.getVmid()+"_"+lastStandingVmInfo.getClientName();
    while (line != null) {
      if (line.contains(name)) {
        throw new TestException("An entry at line#" + lineCount +" in recommended.txt does contains "
            + name
            + " i.e. Corrupted VM. Possibly a wrong csv was recommended. Line content: " +line);
      }
      line = reader.readLine();
      lineCount++;
    }
  }
  
  public static void HydraTask_RunExtractor() throws Exception {
    extractorTest.shutDownVMs();
    extractorTest.generateProperyFile();
    extractorTest.runExtractorTool();
  }
  
  public static void HydraTask_StopStartVMs() throws Exception {
    extractorTest.shutDownVMs();
    StopStartVMs.startVMs(extractorTest.restartVMs);
  }
  
  private void shutDownVMs() throws Exception {
    DataExtractorBB.getBB().getSharedCounters().increment(DataExtractorBB.recyclingStarted);
   
    if (extractorTest.simultaneousShutdownVMs) {
      long opsTaskGranularitySec = TestConfig.tab().longAt(DataExtractorPrms.opsTaskGranularitySec);
      long minTaskGranularityMS = opsTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
      Log.getLogWriter().info("Sleeping for " + minTaskGranularityMS *3/2 + " millisecs to allow the current operations task to terminate");
      MasterController.sleepForMs((int)minTaskGranularityMS *3/2);
      List<String> stopModeList = new ArrayList<String>();
      for (ClientVmInfo vmInfo: restartVMs) {
        stopModeList.add("nice_exit");
      }
      StopStartVMs.stopVMs(restartVMs, stopModeList);
    } else {
      for (ClientVmInfo vmInfo: restartVMs) {
        MasterController.sleepForMs(15000);
        StopStartVMs.stopVM(vmInfo, "nice_exit");
        Log.getLogWriter().info("Sleeping for " + 20 + " seconds to allow ops to run...");
        MasterController.sleepForMs(20 * 1000);
      }
    }
    DataExtractorBB.getBB().getSharedCounters().increment(DataExtractorBB.recycledAllVMs);
  }
  
  private void generateProperyFile() throws IOException {
    String userDir = System.getProperty("user.dir");
    File file = new File(userDir, "propertyFile.txt");
    file.createNewFile();
    String hdfsDiskStore = TestConfig.tab().stringAt(HDFSStorePrms.diskStoreName, null);
    Log.getLogWriter().info("hdfsDiskStore= " + hdfsDiskStore);

    PrintWriter pw = new PrintWriter(file);
    for (ClientVmInfo vmInfo: restartVMs) {
      String name = "vm_"+vmInfo.getVmid()+"_"+vmInfo.getClientName();
      String clientName = vmInfo.getClientName();
      Log.getLogWriter().info("vmInfo.getClientName()" + clientName);
      Log.getLogWriter().info("Client description name= " + TestConfig.getInstance().getClientDescription(clientName).getName());
      VmDescription vmDesc = TestConfig.getInstance().getClientDescription(clientName).getVmDescription();
      Log.getLogWriter().info("VM description name= " + vmDesc.getName());
      Log.getLogWriter().info("Host description name= " + vmDesc.getHostDescription().getName());
      Log.getLogWriter().info("Host description host-name= " + vmDesc.getHostDescription().getHostName());
      String dirName = name + "_" + vmDesc.getHostDescription().getHostName() + "_disk"; 
//      pw.write( name + "=" + userDir + File.separator + dirName + "\n");
      String diskDirName =  userDir + File.separator + dirName;
      String hdfsDiskDirName = userDir + File.separator + dirName +  File.separator + hdfsDiskStore;
      if (hdfsDiskStore != null) {
        pw.write(name + "=" + diskDirName + "," + hdfsDiskDirName + "\n");
      } else {
        pw.write(name + "=" + diskDirName + "\n");
      }
    }
    pw.close();
    propFileAbsolutePath = file.getAbsolutePath();
  }
  
  private void runExtractorTool() throws Exception {
    RemoteTestModule.getMyVmid();
    Thread monitorThread = getOffHeapMonitorThread();
    monitorThread.start();
    Log.getLogWriter().info("***************Extracting data*****************");
    GemFireXDDataExtractor.main(new String[] {"property-file="+propFileAbsolutePath});
    Log.getLogWriter().info("***************Done Extracting data***************");
    terminateMonitorThread = true;
    monitorThread.join();
    if (monitorObjectsMax > 0) {
      throw new TestException("Bug 50408 detected; Off-heap memory was used while extractor tool was running, objects stat max: " + monitorObjectsMax);
    }
  }
  
  /** Create a thread that monitor the use of off-heap memory. We expect that the
   *  data extractor will never allocate off-heap memory.
   */
  private static int monitorObjectsMax = 0;
  private static boolean terminateMonitorThread = false;
  private static Thread getOffHeapMonitorThread() {
    Thread monitorThread = new Thread(new Runnable() {
      public void run() {
        if (SQLTest.isOffheap) {
          Log.getLogWriter().info("Starting monitor thread to look for off-heap memory usage");
          while (true) {
            if (terminateMonitorThread) {
              Log.getLogWriter().info("monitor thread is terminating");
              break;
            }
            if (OffHeapHelper.isOffHeapMemoryConfigured()) {
              OffHeapMemoryStats stats = OffHeapHelper.getOffHeapMemoryStats();
              int objects = stats.getObjects();
              if (objects > 0) {
                Log.getLogWriter().info("Detected off-heap memory usage, number of objects in off-heap memory is " + objects);
                monitorObjectsMax = Math.max(monitorObjectsMax, objects);
              }
            }
            MasterController.sleepForMs(1500);
          }
        }
      }
    });
    return monitorThread;
  }
  
  private void clearPersistentDirs()  {
    String userDir = System.getProperty("user.dir");
    Log.getLogWriter().info("Deleting persistence files from all dirs ");
    for (ClientVmInfo vmInfo: restartVMs) {
      String name = "vm_"+vmInfo.getVmid()+"_"+vmInfo.getClientName();
      String clientName = vmInfo.getClientName();
      Log.getLogWriter().info("vmInfo.getClientName()" + clientName);
      Log.getLogWriter().info("Client description name= " + TestConfig.getInstance().getClientDescription(clientName).getName());
      VmDescription vmDesc = TestConfig.getInstance().getClientDescription(clientName).getVmDescription();
      Log.getLogWriter().info("VM description name= " + vmDesc.getName());
      Log.getLogWriter().info("Host description name= " + vmDesc.getHostDescription().getName());
      Log.getLogWriter().info("Host description host-name= " + vmDesc.getHostDescription().getHostName());
      String dirName = name + "_" + vmDesc.getHostDescription().getHostName() + "_disk";
      String fullDirPath = userDir + File.separator + dirName;
      
      List files = FileUtil.getFiles(new File(fullDirPath), null, false);
      Log.getLogWriter().info(dirName +" has total of "+ files.size() + " files/dirs");
      Log.getLogWriter().info("Deleting files from: "+ fullDirPath);
      FileUtil.deleteFilesFromDirRecursive(fullDirPath);
      Log.getLogWriter().info("Deleted files from: "+ fullDirPath);
      files = FileUtil.getFiles(new File(fullDirPath), null, false);
      Log.getLogWriter().info("After deleting " + dirName +" has total of "+ files.size() + " files/dirs");
    }
  }
  
  public static void loadUsingRecommendedFile() throws Exception {
    SharedMap map = NetworkServerBlackboard.getInstance().getSharedMap();
    Log.getLogWriter().info("Printing entire map");
    Endpoint selectedEndpoint = null;
    for (Object key: map.getMap().keySet()) {
      Log.getLogWriter().info("For key: " +key);
      Map<Integer,Endpoint> endpoints = (Map<Integer,Endpoint>)map.get(key);
      for (Endpoint endpoint : endpoints.values()) {
        if (key.toString().contains("server")) {
          selectedEndpoint = endpoint;
        }
        int port = endpoint.getPort();
        Log.getLogWriter().info("Endpoint: " + endpoint.toString());
        Log.getLogWriter().info("Endpoint name: " + endpoint.getName());
        Log.getLogWriter().info("Endpoint host: " + endpoint.getHost());
        Log.getLogWriter().info("Endpoint Id: " + endpoint.getId());
      }
    }
    //Map<Integer,Endpoint> endpoints = (Map<Integer,Endpoint>)map.get(map.getRandomKey());
    //Endpoint endpoint = endpoints.get(0);
    
    int port = selectedEndpoint.getPort();
    Log.getLogWriter().info("Selected Endpoint: " + selectedEndpoint.toString());
    Log.getLogWriter().info("Selected Endpoint name: " + selectedEndpoint.getName());
    
    String userDir = System.getProperty("user.dir");
    String extractDirName = getExtractedFilesDirName();
    String name1 = "recommended=" + userDir + File.separator + extractDirName + File.separator + "Recommended.txt";
    String name2 = "port="+port;
    String name3 = "property-file="+extractorTest.propFileAbsolutePath;
    String name4 = "host="+selectedEndpoint.getAddress();
    sortRecommendedFile(userDir + File.separator + extractDirName + File.separator + "Recommended.txt");
    Thread monitorThread = getOffHeapMonitorThread();
    monitorThread.start();
    Log.getLogWriter().info("***************Loader: Loading data*****************");
    GemFireXDDataExtractorLoader.main(new String[]{name1, name2, name3, name4});
    Log.getLogWriter().info("***************Loader: Loading data complete*****************");
    terminateMonitorThread = true;
    monitorThread.join();
    if (monitorObjectsMax > 0) {
      throw new TestException("Bug 50408 detected; Off-heap memory was used while extractor tool was running, objects stat max: " + monitorObjectsMax);
    }
  }

  static String getExtractedFilesDirName() {
    class ExtractedFilesFolderFilter implements FileFilter {
      public boolean accept(File fn) {
       return fn.getName().startsWith("EXTRACTED_FILES") &&
              fn.getName().contains("EXTRACTED_FILES") &&
              fn.isDirectory();
      }
    };
    
    String userDir = System.getProperty("user.dir");
    List<File> extractDirs = FileUtil.getFiles(new File(userDir),
        new ExtractedFilesFolderFilter(),
        false);
    File extractDir = extractDirs.get(0);
    return extractDir.getName();
  }
  
  public static void corruptOrDeleteDataFiles() throws IOException {
    String userDir = System.getProperty("user.dir");
    ClientVmInfo vmInfo1 = null;
    ClientVmInfo vmInfo2 = null;
    Iterator iter = extractorTest.restartVMs.iterator();
    //Obtain the last VM which was stopped
    do {
      vmInfo2 = vmInfo1;
      vmInfo1 = (ClientVmInfo)iter.next();
    } while (iter.hasNext());
    lastStandingVmInfo = vmInfo1;
    secondLastStandingVmInfo = vmInfo2;
    int vmId = lastStandingVmInfo.getVmid().intValue();
    String name = "vm_"+lastStandingVmInfo.getVmid()+"_"+lastStandingVmInfo.getClientName();
    String clientName = lastStandingVmInfo.getClientName();
    VmDescription vmDesc = TestConfig.getInstance().getClientDescription(clientName).getVmDescription();
    String dirName = name + "_" + vmDesc.getHostDescription().getHostName() + "_disk";
    String fullDirPath = userDir + File.separator + dirName;
    String fileExt = TestConfig.tab().stringAt(DataExtractorPrms.fileExtensionForCorruptOrDelete, null);
    String operation = TestConfig.tab().stringAt(DataExtractorPrms.corruptOrDeleteOP, null);
    List files = FileUtil.getFiles(new File(fullDirPath), null, false);
    if (operation != null && fileExt != null) {
      specificFileCorruptionOrDeletion(files, fileExt, operation);
    } else {
      boolean corruptedOrDeletedOneFile = false;
      for (int i = 0; i < files.size(); i++) {
        File file = (File)files.get(i);
        if (file.getName().contains(".drf") || 
            file.getName().contains(".krf") || 
            file.getName().contains(".crf") || 
            file.getName().contains(".if")) {
          String absoluteFilePath = file.getAbsolutePath();
          if (!corruptedOrDeletedOneFile) {
            if(rand.nextInt(2) == 0) {
              if (file.getName().contains(".drf")) {
                continue;
              }
              corruptOpLogFile(absoluteFilePath);
              Log.getLogWriter().info("Corrupted "+ file.getName() +" files from: "+ fullDirPath);
            } else {
              if (file.getName().contains(".krf")) {
                continue;
              }
              if (!file.delete()) {
                throw new TestException("Could not delete file: " + absoluteFilePath);
              }
              Log.getLogWriter().info("Deleted file: "+ absoluteFilePath);
            }
            corruptedOrDeletedOneFile = true;
          }
        }
      }
    }
  }
  
  public static void specificFileCorruptionOrDeletion(List files, String fileExtension, String operation) throws IOException {
    for (int i = 0; i < files.size(); i++) {
      File file = (File)files.get(i);
      if (file.getName().contains(fileExtension)) {
        String absoluteFilePath = file.getAbsolutePath();
        if(operation.equalsIgnoreCase("corrupt")) {
          corruptOpLogFile(absoluteFilePath);
          Log.getLogWriter().info("Corrupted "+ file.getName() +" file");
        } else {
          if (!file.delete()) {
            throw new TestException("Could not delete file: " + absoluteFilePath);
          }
          Log.getLogWriter().info("Deleted file: "+ absoluteFilePath);
        }
      }
    }
  }
  
  static int countLinesInFile(String fileName) throws IOException {
    int count = 0;
    File file = new File(fileName);
    if (file.isDirectory()) {
      throw new RuntimeException("Cannot count lines in a directory");
    } else {
      BufferedReader reader = new BufferedReader(new FileReader(fileName));
      String line = reader.readLine(); 
      while (line != null) {
        count++;
        line = reader.readLine();
      }
      reader.close();
    }
    return count;
  }
  
  public static void corruptOpLogFile(String dataFile) throws IOException {
    String userDir = System.getProperty("user.dir");
    String tempfile = userDir + File.separator + "temp.txt";
    int numLines =  countLinesInFile(dataFile);
    File file = new File(dataFile);
    PrintWriter pw = new PrintWriter(tempfile);
    BufferedReader reader = new BufferedReader(new FileReader(file));
    int corruptLine = 3 * numLines/4;
    String line = reader.readLine();
    int count = 0;
    while (line != null) {
      count++;
      line = reader.readLine();
      if (count == corruptLine) {
        pw.write(line.substring(0, line.length()/2));
        pw.write(RANDOM_CORRUPTION_STRING);
        pw.write(line.substring(line.length()/2, line.length()));
      } else {
        pw.write(line+ "\n");
      }
    }
    reader.close();
    pw.close();
    file.delete();
    File tempFile = new File(tempfile);
    if(!tempFile.renameTo(file)) {
      throw new RuntimeException("Could not rename: " + tempfile +" to " + dataFile);
    }
  }
  
  /**
   * Sort the recommendation file so that customers, securities, portfolio 
   * and employees table comes at the begnning and does not cause any
   * fareign key constraint problems during import
   */
  public static void sortRecommendedFile(String recommendedFile) throws IOException {
    File recFile = new File(recommendedFile);
    BufferedReader reader = new BufferedReader(new FileReader(recFile));
    String line = reader.readLine();
    String ddl = null;
    
    List<String> list = new ArrayList<String>();
    List<String> customers = new ArrayList<String>();
    List<String> securities = new ArrayList<String>();
    List<String> portfolio = new ArrayList<String>();
    List<String> employees = new ArrayList<String>();
    if (line != null) {
      // First line is ddl sql file
      ddl = line;
    }
    line = reader.readLine();
    while (line != null) {
      if (line.contains("CUSTOMERS")) {
        customers.add(line);
      } else if(line.contains("SECURITIES")) {
        securities.add(line);
      } else if(line.contains("PORTFOLIO")) {
        portfolio.add(line);
      } else if (line.contains("EMPLOYEES")) {
        employees.add(line);
      } else {
        list.add(line);
      }
      line = reader.readLine();
    }
    recFile.delete();
    recFile = new File(recommendedFile);
    PrintWriter pw = new PrintWriter(recFile);
    pw.write(ddl +"\n");
    for (String csvFile : customers) {pw.write( csvFile+"\n");}
    for (String csvFile : securities) {pw.write( csvFile+"\n");}
    for (String csvFile : portfolio) {pw.write( csvFile+"\n");}
    for (String csvFile : employees) {pw.write( csvFile+"\n");}
    for (String csvFile : list) { pw.write(csvFile +"\n");}
    pw.close();
  }  
}
