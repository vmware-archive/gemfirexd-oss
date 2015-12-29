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

package sql.backupAndRestore;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.gemstone.gemfire.distributed.DistributedSystem;

import hydra.ClientDescription;
import hydra.ClientVmInfo;
import hydra.DistributedSystemHelper;
import hydra.FileUtil;
import hydra.HostHelper;
import hydra.HydraRuntimeException;
import hydra.HydraThreadLocal;
import hydra.Log;
import hydra.MasterController;
import hydra.ProcessMgr;
import hydra.RemoteTestModule;
import hydra.StopSchedulingOrder;
import hydra.TestConfig;
import hydra.gemfirexd.FabricServerHelper;
import perffmwk.PerfStatMgr;
import perffmwk.PerfStatValue;
import perffmwk.StatSpecTokens;
import sql.SQLHelper;
import sql.SQLThinClientTest;
import sql.view.ViewTest;
import util.StopStartVMs;
import util.TestException;
import util.TestHelper;
import util.TestHelperPrms;

/**
 * The BackupRestoreTest class... </p>
 *
 * @author mpriest
 * @see ?
 * @since 7.x
 */
public class BackupRestoreTest extends ViewTest {

  private static final String FILE_SEPARATOR = System.getProperty("file.separator");
  private static final String DISK_DIR_SUFFIX = "_disk";
  private static final String RESTORE_LINUX   = "restore.sh";
  private static final String RESTORE_WINDOWS = "restore.bat";
  private static final String BACKUP_PREFIX   = "backup_";
  private static final String BASELINE_OPT    = "-baseline=";
  private static final String INCREMENTAL_TEXT = "Incremental backup.  Restore baseline originals from previous backups.";
  private static BackupRestoreTest backupRestoreTest;
  public static final String DSPROP = "dsProperties";

  private static HydraThreadLocal threadIsPaused = new HydraThreadLocal();

  private String backupDirPath;        // The directory location of the backups

  public void initializeInstance() {
    // The directory where the backup files are to be stored
    backupDirPath = BackupAndRestorePrms.getBackupPath();
    Log.getLogWriter().info("BackupRestoreTest.initializeInstance-backupDirPath=" + backupDirPath);
  }
  
  private static Properties getdsProp() {
    Properties dsProp = null;
    DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
    if (ds != null) dsProp = ds.getProperties();
    else {
      dsProp = (Properties) BackupAndRestoreBB.getBB().getSharedMap().get(DSPROP);
      if (dsProp == null) throw new TestException("Test issue, need to set up " +
      		"ds properties in BackupAndRestoreBB for thin client node");
    }
    
    return dsProp;
  }
  
 
  public static void HydraTask_createIndexs() {
    if (backupRestoreTest == null) {
      backupRestoreTest = new BackupRestoreTest();
      backupRestoreTest.initializeInstance();
    }
    backupRestoreTest.createIndexs();
  }
  private void createIndexs() {
    Connection gConn = getGFEConnection();
    try {
      Statement s = gConn.createStatement();
      s.execute("CREATE INDEX securitiesIdx ON trade.securities (price ASC)");
    } catch (SQLException se) {
      SQLHelper.printSQLException(se);
      throw new TestException("Not able to create indexes\n" + TestHelper.getStackTrace(se));
    }
    commit(gConn);
    closeGFEConnection(gConn);
  }

  public static void HydraTask_doOnlineBackup() {
    // Determine whether this thread needs to perform the backup
    boolean contWithBackup = false;
    if (BackupAndRestorePrms.getDoBackup()) {
      // product does not allow more than one backup to run at a time;
      contWithBackup = (BackupAndRestoreBB.getBB().getSharedCounters().incrementAndRead(BackupAndRestoreBB.BackupInProgress) == 1);
    }
    if (contWithBackup) {
      if (backupRestoreTest == null) {
        backupRestoreTest = new BackupRestoreTest();
        backupRestoreTest.initializeInstance();
      }
      backupRestoreTest.triggerFullOnlineBackup();
    }
  }

  public static void HydraTask_backupLeader() {
    if (backupRestoreTest == null) {
      backupRestoreTest = new BackupRestoreTest();
      backupRestoreTest.initializeInstance();
    }

    // The total number of working client threads in this test
    if (BackupAndRestoreBB.getBB().getSharedCounters().read(BackupAndRestoreBB.NbrOfClientThreads) == 0) {
      int nbrOfClientThreads = TestConfig.getInstance().getThreadGroup("clientThreads").getTotalThreads();
      Log.getLogWriter().info("BackupRestoreTest.HydraTask_initOpsBackupAndStopVM-nbrOfClientThreads=" + nbrOfClientThreads);
      BackupAndRestoreBB.getBB().getSharedCounters().add(BackupAndRestoreBB.NbrOfClientThreads, nbrOfClientThreads);
    }
    backupRestoreTest.backupLeader();
  }
  private void backupLeader() {
    Log.getLogWriter().info("BackupRestoreTest.backupLeader");
    logExecutionNumber();

    // Task setup...
    boolean restartDuringTest = TestConfig.tab().booleanAt(BackupAndRestorePrms.restartDuringTest, false);
    boolean stopVMsDuringTest = TestConfig.tab().booleanAt(BackupAndRestorePrms.stopVmsDuringTest, false);
    Log.getLogWriter().info("BackupRestoreTest.backupLeader-restartDuringTest=" + restartDuringTest +
                            " stopVMsDuringTest=" + stopVMsDuringTest);
    int nbrCurrentTaskThreads = RemoteTestModule.getCurrentThread().getCurrentTask().getTotalThreads();
    if (nbrCurrentTaskThreads > 1) {
      throw new TestException("ERROR! This test was intended to bo 'controlled' with one thread." +
                              " Please reduce the number of threads in threadgroup named:" +
                              RemoteTestModule.getCurrentThread().getThreadGroupName());
    }
    int nbrOfAccessorThreads = (int) BackupAndRestoreBB.getBB().getSharedCounters().read(BackupAndRestoreBB.NbrOfClientThreads);
    int totalThreads = nbrCurrentTaskThreads + nbrOfAccessorThreads;
    Log.getLogWriter().info("BackupRestoreTest.backupLeader-totalThreads=" + totalThreads);

    // Check to see if this is the last task execution to perform
    checkForLastIteration();

    // Give the other vms some time to do random ops
    int minTaskGranularitySec = TestConfig.tab().intAt(TestHelperPrms.minTaskGranularitySec);
    Log.getLogWriter().info("BackupRestoreTest.backupLeader-Thread is going to sleep for " + minTaskGranularitySec +
                            " seconds to allow other vms to do random ops");
    MasterController.sleepForMs(minTaskGranularitySec * (int) TestHelper.SEC_MILLI_FACTOR);

    // This is just for the plain incremental backup testing
    if (!restartDuringTest && !stopVMsDuringTest) {
      // Back up everything while ops are still processing
      triggerBackup();
    }

    // Signal all Threads (even in the other Task) to pause
    BackupAndRestoreBB.getBB().getSharedCounters().increment(BackupAndRestoreBB.PauseOps);
    TestHelper.waitForCounter(BackupAndRestoreBB.getBB(),
      "PauseOps",
      BackupAndRestoreBB.PauseOps,
      totalThreads,
      true,
      -1,
      2000);

    // Verify what we've got so far...
    backupRestoreTest.verifyResultSets();

    // This is for the test that restarts the Fabric Server during the test
    if (restartDuringTest) {
      // Back up everything
      triggerBackup();

      // Signal the Fabric Servers to stop
      backupRestoreTest.stopFabricServer();

      // Delete the old disk dirs
      backupRestoreTest.deleteExistingDiskDirs();
      // Restore the data from the backups
      backupRestoreTest.performRestoreOfBackup();

      backupRestoreTest.startFabricServer();
      SQLThinClientTest.HydraTask_startNetworkServer();

      // Verify what we've got after the restore
      backupRestoreTest.verifyResultSets();
    }

    // This is for the test that stops a VM during the test
    long nbrOfVMsStoped = BackupAndRestoreBB.getBB().getSharedCounters().read(BackupAndRestoreBB.NbrOfVMsStopped);
    Log.getLogWriter().info("BackupRestoreTest.backupLeader-nbrOfVMsStoped=" + nbrOfVMsStoped);
    if (stopVMsDuringTest && nbrOfVMsStoped == 0) {
      // Back up everything
      triggerBackup();
      // Get the info needed to stop a VM and stop it
      Object[] targetVM = StopStartVMs.getOtherVMs(1);
      ClientVmInfo vmToStop = ((List<ClientVmInfo>) targetVM[0]).get(0);
      String stopMode = ((List<String>) targetVM[1]).get(0);
      Log.getLogWriter().info("BackupRestoreTest.backupLeader-about to stop vm:" + vmToStop +
                              " via " + stopMode);
      StopStartVMs.stopVM(vmToStop, stopMode);
      BackupAndRestoreBB.getBB().getSharedMap().put(BackupAndRestoreBB.StoppedVMsKey, vmToStop);

      // Reduce the thread count
      ClientDescription clientDescription = TestConfig.getInstance().getClientDescription(vmToStop.getClientName());
      int stoppedVmThreadCnt = clientDescription.getVmThreads();
      Log.getLogWriter().info("BackupRestoreTest.backupLeader-stoppedVmThreadCnt=" + stoppedVmThreadCnt);
      BackupAndRestoreBB.getBB().getSharedCounters().subtract(BackupAndRestoreBB.NbrOfClientThreads, stoppedVmThreadCnt);

      BackupAndRestoreBB.getBB().getSharedCounters().increment(BackupAndRestoreBB.NbrOfVMsStopped);
    } else if (stopVMsDuringTest && nbrOfVMsStoped == 1) {
      // Restart the stopped VM
      ClientVmInfo stoppedVM = (ClientVmInfo) BackupAndRestoreBB.getBB().getSharedMap().get(BackupAndRestoreBB.StoppedVMsKey);
      StopStartVMs.startVM(stoppedVM);

      // Increase the thread count
      ClientDescription clientDescription = TestConfig.getInstance().getClientDescription(stoppedVM.getClientName());
      int stoppedVmThreadCnt = clientDescription.getVmThreads();
      Log.getLogWriter().info("BackupRestoreTest.backupLeader-stoppedVmThreadCnt=" + stoppedVmThreadCnt);
      BackupAndRestoreBB.getBB().getSharedCounters().add(BackupAndRestoreBB.NbrOfClientThreads, stoppedVmThreadCnt);

      // Set the count so we don't stop or start anymore
      BackupAndRestoreBB.getBB().getSharedCounters().add(BackupAndRestoreBB.NbrOfVMsStopped, 99);
    } else if (stopVMsDuringTest && nbrOfVMsStoped == 99) {
      // Back up everything
      triggerBackup();
    }

    // Check to see if we are all done with the test
    boolean isItTimeToStop = (BackupAndRestoreBB.getBB().getSharedCounters().read(BackupAndRestoreBB.TimeToStop) >= 1);
    Log.getLogWriter().info("BackupRestoreTest.backupLeader-isItTimeToStop=" + isItTimeToStop);
    if (isItTimeToStop) {
      throw new StopSchedulingOrder("Number of executions is " +
                                    BackupAndRestoreBB.getBB().getSharedCounters().read(BackupAndRestoreBB.ExecutionNumber));
    }

    // Unpause all threads (for next round of execution)
    BackupAndRestoreBB.getBB().getSharedCounters().zero(BackupAndRestoreBB.PauseOps);
  }

  public static void HydraTask_doOpsAndWait() {
    if (backupRestoreTest == null) {
      backupRestoreTest = new BackupRestoreTest();
      backupRestoreTest.initializeInstance();
    }
    backupRestoreTest.doOpsAndWait();
  }

  private static void doOpsAndWait() {
    boolean timeToPauseOps = (BackupAndRestoreBB.getBB().getSharedCounters().read(BackupAndRestoreBB.PauseOps) > 0);
    Log.getLogWriter().info("BackupRestoreTest.doOpsAndWait-timeToPauseOps=" + timeToPauseOps);
    if (timeToPauseOps) {
      if ((Boolean) threadIsPaused.get()) {
        Log.getLogWriter().info("BackupRestoreTest.doOpsAndWait-This thread is already paused");
      } else {
        Log.getLogWriter().info("BackupRestoreTest.doOpsAndWait-This thread is now paused");
        threadIsPaused.set(new Boolean(true));
        BackupAndRestoreBB.getBB().getSharedCounters().increment(BackupAndRestoreBB.PauseOps);
      }
      MasterController.sleepForMs(5000);
    } else {
      // Not Pausing, Do Ops and Backups
      threadIsPaused.set(new Boolean(false));
      do {
        Log.getLogWriter().info("BackupRestoreTest.doOpsAndWait-Doing Ops");
        backupRestoreTest.doDMLOp();
        timeToPauseOps = (BackupAndRestoreBB.getBB().getSharedCounters().read(BackupAndRestoreBB.PauseOps) > 0);
        Log.getLogWriter().info("BackupRestoreTest.doOpsAndWait-timeToPauseOps=" + timeToPauseOps);
      } while (!timeToPauseOps);
    }
  }

  public static void HydraTask_performFinalBackup() {
    if (backupRestoreTest == null) {
      backupRestoreTest = new BackupRestoreTest();
      backupRestoreTest.initializeInstance();
    }
    backupRestoreTest.performFinalBackup();
  }
  private static void performFinalBackup() {
    Log.getLogWriter().info("BackupRestoreTest.performFinalBackup");
    // One of the threads will perform a final backup (Full or Incremental) - the product does not allow more than one backup to run at a time;
    boolean leaderToBackup = (BackupAndRestoreBB.getBB().getSharedCounters().incrementAndRead(BackupAndRestoreBB.LeaderForFinalBackup) == 1);
    if (leaderToBackup) {
      Log.getLogWriter().info("BackupRestoreTest.performFinalBackup-It's time to stop, but we must first perform one more backup. I'll do it!");
      triggerBackup();
    }
  }

  /**
   * Log the execution number of this serial task.
   */
  private static void logExecutionNumber() {
    long execNbr = BackupAndRestoreBB.getBB().getSharedCounters().incrementAndRead(BackupAndRestoreBB.ExecutionNumber);
    Log.getLogWriter().info("BackupRestoreTest.logExecutionNumber-Beginning task with execution number: " + execNbr);
  }

  protected static void checkForLastIteration() {
    // determine if this is the last iteration
    long execNbr = BackupAndRestoreBB.getBB().getSharedCounters().read(BackupAndRestoreBB.ExecutionNumber);
    Log.getLogWriter().info("BackupRestoreTest.checkForLastIteration-execNbr=" + execNbr + ", BackupAndRestorePrms.getNbrOfExecutions()=" + BackupAndRestorePrms
      .getNbrOfExecutions());
    if (execNbr >= BackupAndRestorePrms.getNbrOfExecutions()) {
      Log.getLogWriter().info("BackupRestoreTest.checkForLastIteration-This is the last iteration of this task, execNbr=" + execNbr);
      BackupAndRestoreBB.getBB().getSharedCounters().increment(BackupAndRestoreBB.TimeToStop);
    }
  }
  
  //only one concurrent thread should perform backup
  public static void doBackup() {
    if (backupRestoreTest == null) {
      backupRestoreTest = new BackupRestoreTest();
      backupRestoreTest.initializeInstance();
    }
    BackupRestoreTest.triggerBackup();
  }

  private static void triggerBackup() {
    boolean doBackup = BackupAndRestorePrms.getDoBackup();
    Log.getLogWriter().info("BackupRestoreTest.triggerBackup-doBackup=" + doBackup);
    if (doBackup) {
      // Check if we are to perform incremental backups
      boolean incrementalBackups = BackupAndRestorePrms.getIncrementalBackups();
      Log.getLogWriter().info("BackupRestoreTest.triggerBackup-incrementalBackups=" + incrementalBackups);

      // How many backups have we done?
      long BackupCntr = BackupAndRestoreBB.getBB().getSharedCounters().read(BackupAndRestoreBB.BackupCtr);
      Log.getLogWriter().info("BackupRestoreTest.triggerBackup-BackupCntr=" + BackupCntr);

      // If we are doing incremental backups and we have done at least one full backup, then we can do an incremental backup
      if (incrementalBackups && BackupCntr > 0) {
        backupRestoreTest.triggerIncrementalOnlineBackup();
      } else {
        backupRestoreTest.triggerFullOnlineBackup();
      }
    }
  }
  private static void triggerFullOnlineBackup() {
    backupRestoreTest.performOnlineBackup(false);
  }
  private static void triggerIncrementalOnlineBackup() {
    backupRestoreTest.performOnlineBackup(true);
  }
  private static void performOnlineBackup(boolean incremental) {
    Log.getLogWriter().info("BackupRestoreTest.doOnlineBackup-incremental=" + incremental);

    // Get the backup storage location
    File rootBackupDir = new File(backupRestoreTest.backupDirPath);
    Log.getLogWriter().info("BackupRestoreTest.doOnlineBackup-rootBackupDir=" + rootBackupDir);

    long backupCntr = BackupAndRestoreBB.getBB().getSharedCounters().incrementAndRead(BackupAndRestoreBB.BackupCtr);
    Log.getLogWriter().info("BackupRestoreTest.doOnlineBackup-backupCntr=" + backupCntr);

    // If this is to be an incremental backup, we need to build the 'baseline' parameter
    String incrementalClause = "";
    if (incremental) {
      String baselineDir = rootBackupDir + FILE_SEPARATOR + BACKUP_PREFIX + (backupCntr - 1);
      Log.getLogWriter().info("BackupRestoreTest.doOnlineBackup-baselineDir1=" + baselineDir);

      // There should be one directory here, if not throw an error
      File[] baselineDirContents = new File(baselineDir).listFiles();
      if (baselineDirContents.length != 1) {
        throw new TestException("BackupRestoreTest.doOnlineBackup-On line backup cannot continue because we were " +
                                "expecting 1 subdirectory in the '"+ baselineDir +
                                "' directory, but found " + baselineDirContents.length);
      }
      // Get the name of the directory's one subdirectory and use this as the 'baseline' directory
      File backupDateDir = baselineDirContents[0];
      Log.getLogWriter().info("BackupRestoreTest.doOnlineBackup-backupDateDir.getName()=" + backupDateDir.getName());
      baselineDir = baselineDir + FILE_SEPARATOR + backupDateDir.getName();
      Log.getLogWriter().info("BackupRestoreTest.doOnlineBackup-baselineDir2=" + baselineDir);
      incrementalClause = BASELINE_OPT + baselineDir + " ";

    }
    Log.getLogWriter().info("BackupRestoreTest.doOnlineBackup-incrementalClause=" + incrementalClause);

    // Get the path to SQLFire
    String gfxdCommand = FabricServerHelper.getGFXDCommand();
    Properties properties = getdsProp();

    // Set the backup directory
    String backupDir = rootBackupDir + FILE_SEPARATOR + BACKUP_PREFIX + backupCntr;
    Log.getLogWriter().info("BackupRestoreTest.doOnlineBackup-backupDir=" + backupDir);

    // Set the backup command
    String backupCmd = gfxdCommand +
                       " backup " + incrementalClause +
                       backupDir +
                       " -locators=" + properties.get("locators");

    Log.getLogWriter().info("BackupRestoreTest.doOnlineBackup-backupCmd=" + backupCmd);
    String backupResult = null;
    try {
      backupResult = ProcessMgr.fgexec(backupCmd, 0);
    } catch (HydraRuntimeException e) {
      e.printStackTrace();
      Log.getLogWriter().info("BackupRestoreTest.doOnlineBackup-Backup Failed: e.getCause()=" + e.getCause());
      Log.getLogWriter().info("BackupRestoreTest.doOnlineBackup-Backup Failed: e.getMessage()=" + e.getMessage());
    }
    Log.getLogWriter().info("BackupRestoreTest.doOnlineBackup-Done calling online backup tool, backupResult is " + backupResult);
    if (backupResult == null || backupResult.indexOf("Backup successful") < 0) {
      throw new TestException("BackupRestoreTest.doOnlineBackup-On line backup was not successful, backupResult from online backup is " + backupResult);
    }
  }

  public static void HydraTask_snapShotDiskDirContents() {
    if (backupRestoreTest == null) {
      backupRestoreTest = new BackupRestoreTest();
      backupRestoreTest.initializeInstance();
    }
    backupRestoreTest.snapShotDiskDirContents();
  }

  private static void snapShotDiskDirContents() {
    // How many backups did we do (used to skip the last file, because it has a different file size and will throw-off the verify)
    String lastFileName = "_" + (BackupAndRestoreBB.getBB().getSharedCounters().read(BackupAndRestoreBB.BackupCtr) + 1);
    Log.getLogWriter().info("BackupRestoreTest.snapShotDiskDirContents-lastFileName=" + lastFileName);

    // Get the '_disk' directory contents
    List diskDirSnapShot = diskDirContents(System.getProperty("user.dir"), lastFileName);
    Log.getLogWriter().info("BackupRestoreTest.snapShotDiskDirContents-diskDirSnapShot.size()=" + diskDirSnapShot.size());
    // Store the SnapShot to the BlackBoard
    BackupAndRestoreBB.getBB().getSharedMap().put(BackupAndRestoreBB.DiskDirSnapShotKey, diskDirSnapShot);
  }

  public static void HydraTask_verifyDiskDirContents() {
    if (backupRestoreTest == null) {
      backupRestoreTest = new BackupRestoreTest();
      backupRestoreTest.initializeInstance();
    }
    backupRestoreTest.verifyDiskDirContents();
  }
  private static void verifyDiskDirContents() {
    // How many backups did we do (used to skip the last file, because it has a different file size and will throw-off the verify)
    String lastFileName = "_" + (BackupAndRestoreBB.getBB().getSharedCounters().read(BackupAndRestoreBB.BackupCtr));
    Log.getLogWriter().info("BackupRestoreTest.verifyDiskDirContents-lastFileName=" + lastFileName);

    // Get the '_disk' directory contents
    List diskDirNow = diskDirContents(System.getProperty("user.dir"), lastFileName);
    Log.getLogWriter().info("BackupRestoreTest.verifyDiskDirContents-diskDirNow.size()=" + diskDirNow.size());
    // Get the SnapShot from the BlackBoard
    List diskDirSnapShot = (List) (BackupAndRestoreBB.getBB().getSharedMap().get(BackupAndRestoreBB.DiskDirSnapShotKey));
    Log.getLogWriter().info("BackupRestoreTest.verifyDiskDirContents-diskDirSnapShot.size()=" + diskDirSnapShot.size());

    boolean error = false;
    String errorString = "";

    // Remove all of the files found now to what was found before to see what is missing
    List missingFiles = new ArrayList();
    missingFiles.addAll(diskDirSnapShot);
    missingFiles.removeAll(diskDirNow);
    if (missingFiles.size() > 0) {
      error = true;
      errorString = "The following files were missing after the restore:\n" + missingFiles + "\n";
    }

    // Remove all of the files found before from what was found now to see what is extra
    List extraFiles = new ArrayList();
    extraFiles.addAll(diskDirNow);
    extraFiles.removeAll(diskDirSnapShot);
    if (extraFiles.size() > 0) {
      error = true;
      errorString += "The following unexpected files were found after the restore:\n" + extraFiles + "\n";
    }
    if (error) {
      throw new TestException(errorString);
    } else {
      Log.getLogWriter().info("erifyDiskDirContents-No missing or extra files found.");
    }
  }

  // Get the '_disk' directory contents
  private static List diskDirContents(String testDirName, String lastFileName) {
    Log.getLogWriter().info("BackupRestoreTest.diskDirContents-testDirName=" + testDirName +
                            ", lastFileName=" + lastFileName);
    //MasterController.sleepForMs(30000);

    List allDiskDirContents = new ArrayList();

    // Directory Names
    String diskDirName;
    String diskFileName;
    String dataDictDirName;
    String dataDictFileName;

    // Look in the test directory for '_disk' directories
    for (File aTestFile : new File(testDirName).listFiles()) {

      // Look for and process any directories that end in '_disk'
      if (aTestFile.isDirectory() && aTestFile.getName().endsWith(DISK_DIR_SUFFIX)) {
        diskDirName = aTestFile.getName();
        // Only process the 'datastore' directories
        if (!diskDirName.contains("datastore")) {
          continue;
        }

        // Look in the '_disk' directories for the backup files and the 'datadictionary' directory
        for (File aDiskDirFile : aTestFile.listFiles()) {

          if (aDiskDirFile.isDirectory() && aDiskDirFile.getName().equals("datadictionary")) {
            dataDictDirName = aDiskDirFile.getName();
            // Look in the 'datadictionary' directory for the backup files
            for (File aDataDictFile : aDiskDirFile.listFiles()) {
              dataDictFileName = aDataDictFile.getName();
              // Skip the 'lock' files
              if (dataDictFileName.endsWith(".lk") || dataDictFileName.endsWith(".d")) {
                continue;
              } else if (lastFileName != null && dataDictFileName.contains(lastFileName)) {
                // Skip the 'last' files
                continue;
              }
              allDiskDirContents.add(diskDirName + FILE_SEPARATOR +
                                     dataDictDirName + FILE_SEPARATOR +
                                     dataDictFileName + "-" +
                                     aDataDictFile.length());
            }
          } else {
            diskFileName = aDiskDirFile.getName();
            // Skip the 'lock' files
            if (diskFileName.endsWith(".lk") || diskFileName.endsWith(".d")) {
              continue;
            }
            // Skip the 'last' files
            if (lastFileName != null && diskFileName.contains(lastFileName)) {
              continue;
            }
            // Look in the '_disk' directory for the backup files
            allDiskDirContents.add(diskDirName + FILE_SEPARATOR +
                                   diskFileName + "-" +
                                   aDiskDirFile.length());
          }
        }
      }
    }
    return allDiskDirContents;
  }

  public static void HydraTask_doRestoreBackup() {
    // Determine whether this thread needs to perform the restore
    boolean contWithRestore = false;
    if (BackupAndRestorePrms.getDoBackup()) {
      long counter = BackupAndRestoreBB.getBB().getSharedCounters().incrementAndRead(BackupAndRestoreBB.RestoreThreadCnt);
      contWithRestore = (counter == 1); // product does not allow more than one restore to run at a time;
    }
    if (contWithRestore) {
      if (backupRestoreTest == null) {
        backupRestoreTest = new BackupRestoreTest();
        backupRestoreTest.initializeInstance();
      }
      backupRestoreTest.deleteExistingDiskDirs();
      backupRestoreTest.performRestoreOfBackup();
      
      BackupAndRestoreBB.getBB().getSharedCounters().zero(BackupAndRestoreBB.RestoreThreadCnt);
    }
  }

  private void performRestoreOfBackup() {
    String restoreFile = RESTORE_LINUX;
    if (HostHelper.isWindows()) {
      restoreFile = RESTORE_WINDOWS;
    }

    // Find the latest backup directory
    File latestBackupDir = findLatestBackupDir();
    if (latestBackupDir == null) {
      throw new TestException("Expecting the test directory to contain at least 1 backup directory, but none were found.");
    }

    // Check the contents of the backup directory
    for (File hostVMDir : latestBackupDir.listFiles()) {
      // Scipt the restoring of the locator files
      Log.getLogWriter().info("BackupRestoreTest.performRestoreOfBackup-hostVMDir.getName()=" + hostVMDir.getName());
      if (hostVMDir.getName().contains("locator")) {
        continue;
      }
      // Check the contents of the host's backup directories
      for (File aFile : hostVMDir.listFiles()) {
        Log.getLogWriter().info("BackupRestoreTest.performRestoreOfBackup-aFile.getName()=" + aFile.getName());
        // Find the restore script
        if (aFile.getName().equals(restoreFile)) {
          // Run the script
          try {
            String command = aFile.getCanonicalPath();
            Log.getLogWriter().info("BackupRestoreTest.performRestoreOfBackup-Running restore scripts, command=" + command);
            String cmdResult = ProcessMgr.fgexec(command, 5000);
            Log.getLogWriter().info("BackupRestoreTest.performRestoreOfBackup-Restore script executed successfully. cmdResult=" + cmdResult);
          } catch (HydraRuntimeException e) {
            throw e;
          } catch (IOException e) {
            throw new TestException(TestHelper.getStackTrace(e));
          }
          break;
        }
      }
    }
  }

  /**
   * Delete existing disk directories if they contain disk files.
   */
  private void deleteExistingDiskDirs() {
    String currDirName = System.getProperty("user.dir");
    Log.getLogWriter().info("BackupRestoreTest.deleteExistingDiskDirs-currDirName=" + currDirName);

    String dirName;
    List<String> dirsToDelete = new ArrayList<String>();
    File currDir = new File(currDirName);
    File[] contents = currDir.listFiles();
    for (File aDir: contents) {
      dirName = aDir.getName();
      // Must be a '_disk' directory that is NOT from the locator
      if (aDir.isDirectory() && (dirName.endsWith(DISK_DIR_SUFFIX)) && (!dirName.contains("locator"))) {
        Log.getLogWriter().info("BackupRestoreTest.deleteExistingDiskDirs-aDir.getName()=" + dirName);
        dirsToDelete.add(dirName);
      }
    }
    int deletedCnt;
    //MasterController.sleepForMs(30000);
    for (String dirToDelete : dirsToDelete) {
      deletedCnt = deleteDir(new File(dirToDelete));
      Log.getLogWriter().info("BackupRestoreTest.deleteExistingDiskDirs-dirToDelete=" + dirToDelete +
                              " deletedCnt=" + deletedCnt);
    }
  }

  /**
   * Recursively delete a directory and its contents
   *
   * @param aDir The directory to delete.
   */
  private int deleteDir(File aDir) {
    int deletedCnt = 0;
    List<String> deleteExceptions = new ArrayList<String>();

    File[] contents = aDir.listFiles();
    for (File aFile : contents) {
      if (aFile.isDirectory()) {
        deletedCnt += deleteDir(aFile);
      } else {
        if (aFile.delete()) {
          Log.getLogWriter().info("BackupRestoreTest.deleteDir-Deleted file: " + aFile.getAbsolutePath());
          deletedCnt++;
        } else {
          deleteExceptions.add("Could not delete file: " + aFile.getAbsolutePath());
        }
      }
    }

    if (aDir.delete()) {
      Log.getLogWriter().info("BackupRestoreTest.deleteDir-Deleted directory: " + aDir.getAbsolutePath());
      deletedCnt++;
    } else {
      Log.getLogWriter().info("BackupRestoreTest.deleteDir-About to delete aDir.canWrite()=" + aDir.canWrite());
      deleteExceptions.add("Could not delete directory: " + aDir.getAbsolutePath());
    }

    if (deleteExceptions.size() > 0) {
      throw new TestException("BackupRestoreTest.deleteDir-Could not delete the following direcories / files: " + deleteExceptions.toString());
    }

    return deletedCnt;
  }

  public static void HydraTask_verifyIncrementalBackupPerformed() {
    if (backupRestoreTest == null) {
      backupRestoreTest = new BackupRestoreTest();
      backupRestoreTest.initializeInstance();
    }
    backupRestoreTest.verifyIncrementalBackupPerformed();
  }

  private void verifyIncrementalBackupPerformed() {
    Log.getLogWriter().info("BackupRestoreTest.verifyIncrementalBackupPerformed");

    String restoreFile = RESTORE_LINUX;
    if (HostHelper.isWindows()) {
      restoreFile = RESTORE_WINDOWS;
    }

    boolean doBackup = BackupAndRestorePrms.getDoBackup();
    Log.getLogWriter().info("BackupRestoreTest.verifyIncrementalBackupPerformed-doBackup=" + doBackup);
    // Check if we are to perform backups
    if (doBackup) {
      // Check if we are to perform incremental backups
      boolean incrementalBackups = BackupAndRestorePrms.getIncrementalBackups();
      Log.getLogWriter()
         .info("BackupRestoreTest.verifyIncrementalBackupPerformed-incrementalBackups=" + incrementalBackups);

      // How many backups have we done?
      long BackupCntr = BackupAndRestoreBB.getBB().getSharedCounters().read(BackupAndRestoreBB.BackupCtr);
      Log.getLogWriter().info("BackupRestoreTest.verifyIncrementalBackupPerformed-BackupCntr=" + BackupCntr);

      // If we are doing incremental backups and we have done at least two backups, then we should have done an incremental backup
      if (incrementalBackups && BackupCntr > 1) {
        File latestBackupDir = findLatestBackupDir();
        if (latestBackupDir == null) {
          throw new TestException("Expecting the test directory to contain at least 1 backup directory, but none were found.");
        }
        // Check the contents of the backup directory
        for (File hostVMDir : latestBackupDir.listFiles()) {
          // Check the contents of the host's backup directories
          for (File aFile : hostVMDir.listFiles()) {
            // Find the restore script
            if (aFile.getName().equals(restoreFile)) {
              // Check the script file for incremental
              try {
                String restoreScriptFullFilename = aFile.getCanonicalPath();
                boolean isBackupIncremental = FileUtil.hasLinesContaining(restoreScriptFullFilename, INCREMENTAL_TEXT);
                Log.getLogWriter()
                   .info("BackupRestoreTest.verifyIncrementalBackupPerformed-isBackupIncremental=" + isBackupIncremental);
                if (!isBackupIncremental) {
                  throw new TestException("Expecting the restore script to be of type incremental, but it wasn't.");
                }
              } catch (IOException e) {
                throw new TestException(TestHelper.getStackTrace(e));
              }
              break;
            }
          }
        }
      }
    }
  }

  private File findLatestBackupDir() {
    File backupDir = null;
    // Get the latest backup number
    long lastBackupNbr = BackupAndRestoreBB.getBB().getSharedCounters().read(BackupAndRestoreBB.BackupCtr);
    Log.getLogWriter().info("BackupRestoreTest.findLatestBackupDir-lastBackupNbr=" + lastBackupNbr);

    // Find the backup directories
    File rootBackupDir = new File(backupDirPath);
    Log.getLogWriter().info("BackupRestoreTest.findBackupDir-rootBackupDir=" + rootBackupDir);

    // Check the contents of the current test directory
    for (File aDir : rootBackupDir.listFiles()) {
      // Find the last backup directory
      if (aDir.getName().equalsIgnoreCase(BACKUP_PREFIX + lastBackupNbr)) {
        // Get the contents of this backup directory
        File[] backupContents = aDir.listFiles();
        // We should have exactly one directory
        if (backupContents.length != 1) {
          throw new TestException("Expecting backup directory to contain 1 directory, but it contains " + backupContents.length);
        }
        // Use this as the latest backup directory
        backupDir = backupContents[0];
        break;
      }
    }
    Log.getLogWriter().info("BackupRestoreTest.findLatestBackupDir-Backup dir is " + backupDir);
    return backupDir;
  }

  public static void HydraTask_checkDataStoreBytesInUse() {
    backupRestoreTest.checkDataStoreBytesInUse();
  }
  private static void checkDataStoreBytesInUse() {
    String archiveName = BackupAndRestorePrms.getArchiveName();
    Log.getLogWriter().info("BackupRestoreTest.checkDataStoreBytesInUse-archiveName=" + archiveName);
    String spec = "*" + archiveName + "* " // search all BridgeServer archives
                  + "PartitionedRegionStats "
                  + "* " // match all instances
                  + "dataStoreBytesInUse "
                  + StatSpecTokens.FILTER_TYPE + "=" + StatSpecTokens.FILTER_NONE + " "
                  + StatSpecTokens.COMBINE_TYPE + "=" + StatSpecTokens.COMBINE_ACROSS_ARCHIVES + " "
                  + StatSpecTokens.OP_TYPES + "=" + StatSpecTokens.MAX;
    List aList = PerfStatMgr.getInstance().readStatistics(spec);
    if (aList == null) {
      Log.getLogWriter().info("BackupRestoreTest.checkDataStoreBytesInUse-No Stats Found for spec:" + spec);
    } else {
      Log.getLogWriter().info("BackupRestoreTest.checkDataStoreBytesInUse-Getting Stats for spec:" + spec);
      double dataStoreBytesInUse = 0;
      for (int i = 0;i < aList.size();i++) {
        PerfStatValue stat = (PerfStatValue) aList.get(i);
        double statMax = stat.getMax();
        Log.getLogWriter().info("BackupRestoreTest.checkDataStoreBytesInUse-statMax=" + statMax);
        dataStoreBytesInUse += statMax;
      }
      Log.getLogWriter().info("BackupRestoreTest.checkDataStoreBytesInUse-dataStoreBytesInUse=" + dataStoreBytesInUse);
      Log.getLogWriter().info("BackupRestoreTest.checkDataStoreBytesInUse-dataStoreBytesInUse(MB)=" + dataStoreBytesInUse / (1024*1024));
    }
  }
}
