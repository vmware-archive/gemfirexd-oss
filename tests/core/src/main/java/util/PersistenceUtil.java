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
package util;

import hydra.DiskStorePrms;
import hydra.DistributedSystemHelper;
import hydra.HostHelper;
import hydra.HydraVector;
import hydra.Log;
import hydra.ProcessMgr;
import hydra.TestConfig;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;
import java.util.regex.Pattern;

import management.test.cli.CommandTest;

import parReg.ParRegBB;
import parReg.ParRegPrms;

import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.BackupStatus;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.persistence.PersistentID;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.util.IOUtils;

/**
 * @author lynn
 *
 */
public class PersistenceUtil {
  
  /**
   * Matches a disk store id, as printed out from list-missing disk stores.
   * The first group is the disk store id, the second group is the host name and the
   * thirds is the port. Here's an example of a disk store id:
   * 8ba7780c-dab8-4f9a-a1cb-fe66ea256650 [dasmith-e6410:/home/dsmith/data/work/manual_test_4/server2/.]
   * 
   */
  public static final Pattern DISK_STORE_ID = Pattern.compile("^([0-9a-f\\-]+) \\[(.*):(.*)\\]$");

  /** Run the offline validator and compaction tool. This can be called by
   *  concurrent threads across multiple jvms, and only 1 thread will do
   *  the validate and compaction. If this task is used more than once
   *  per test, then prior to invoking this task for the 2nd time (or more)
   *  you must first call PersistenceUtil.HydraTask_initialize.
   */
  public static void HydraTask_doOfflineValAndCompactionOnce() {

    long leader = PersistenceBB.getBB().getSharedCounters().incrementAndRead(PersistenceBB.offlineToolLeader);
    Log.getLogWriter().info("Leader is " + leader);
    if (leader != 1) {
      return; // only the vm that gets a value of 1 can continue
    }
    // workaround for bug 42432; cannot run offline tools on windows; when this is fixed remove the following if
    if (HostHelper.isWindows()) {
        if (!CliHelperPrms.getUseCli()) {
          Log.getLogWriter().info("Not running off line val and compaction on windows to avoid bug 42432");
          return;
        }
    }
    doOfflineValAndCompaction();
  }
  
  /** Run the offline export command. This can be called by
   *  concurrent threads across multiple jvms, and only 1 thread will do
   *  the export. If this task is used more than once
   *  per test, then prior to invoking this task for the 2nd time (or more)
   *  you must first call PersistenceUtil.HydraTask_initialize.
   */
  public static void HydraTask_doOfflineExportOnce() {

    long leader = PersistenceBB.getBB().getSharedCounters().incrementAndRead(PersistenceBB.offlineToolLeader);
    if (leader != 1) {
      return; // only the vm that gets a value of 1 can continue
    }
    // workaround for bug 42432; cannot run offline tools on windows; when this is fixed remove the following if
    if (HostHelper.isWindows()) {
      Log.getLogWriter().info("Not running off line val and compaction on windows to avoid bug 42432");
      return;
    }
    doOfflineExport();
  }
  
  /** Initialize blackboard. This is used if a test does more than one
   *  invocation of HydraTask_doOfflineValAndCompaction
   */
  public static void HydraTask_initialize() {
    PersistenceBB.getBB().getSharedCounters().zero(PersistenceBB.offlineToolLeader);
  }
  
  /** Run the offline validator and compaction tool. This should be called by
   *  only 1 thread at a time. If you need to invoke this task with multiple
   *  threads (perhaps you don't have a threadGroup with only one thread)
   *  see HydraTask_doOfflineValAndCompactionOnce().
   */
  public static void doOfflineValAndCompaction() {
    Log.getLogWriter().info("Running offline tools...");

    Map<Integer, List<File>> diskDirMap = getDiskDirMap();
    Log.getLogWriter().info("diskDirMap is " + diskDirMap);

    // call the offline tools
    for (Integer vmID: diskDirMap.keySet()) {
      List<File> diskDirList = diskDirMap.get(vmID);
      File[] diskDirArr = diskDirList.toArray(new File[]{});
      scramble(diskDirArr);
      for (File diskDir: diskDirArr) {
        String[] diskDirFileNames = diskDir.list();
        for (String diskFileName: diskDirFileNames) {
          String searchStr1 = "BACKUP";
          String searchStr2 = ".if";
          if (diskFileName.startsWith(searchStr1) && diskFileName.endsWith(searchStr2)) {
            int index1 = diskFileName.indexOf(searchStr1);
            int index2 = diskFileName.indexOf(searchStr2);
            String diskStoreName = diskFileName.substring(
                index1 + searchStr1.length(), index2);
            runOfflineValAndCompaction(diskStoreName, diskDirArr);
          }
        }
      }
    }
  }

  /** Return a Set of diskStore names in use by the test
   */
  public static Set<String> getDiskStores() {
    Log.getLogWriter().info("Getting list of diskStores...");
    Map<Integer, List<File>> diskDirMap = getDiskDirMap();
    Log.getLogWriter().info("diskDirMap is " + diskDirMap);
    Set<String> diskStores = new HashSet();

    for (Integer vmID: diskDirMap.keySet()) {
      List<File> diskDirList = diskDirMap.get(vmID);
      File[] diskDirArr = diskDirList.toArray(new File[]{});
      for (File diskDir: diskDirArr) {
        String[] diskDirFileNames = diskDir.list();
        for (String diskFileName: diskDirFileNames) {
          String searchStr1 = "BACKUP";
          String searchStr2 = ".if";
          if (diskFileName.startsWith(searchStr1) && diskFileName.endsWith(searchStr2)) {
            int index1 = diskFileName.indexOf(searchStr1);
            int index2 = diskFileName.indexOf(searchStr2);
            String diskStoreName = diskFileName.substring(
                index1 + searchStr1.length(), index2);
            diskStores.add(diskStoreName);
          }
        }
      }
    }
    Log.getLogWriter().info("Returning list of diskStores: " + diskStores);
    return diskStores;
  }

  /** Run the offline export tool. This should be called by
   *  only 1 thread at a time. If you need to invoke this task with multiple
   *  threads (perhaps you don't have a threadGroup with only one thread)
   *  see HydraTask_doOfflineExport().
   */
  public static void doOfflineExport() {
    Log.getLogWriter().info("Running offline export ...");

    Map<Integer, List<File>> diskDirMap = getDiskDirMap();
    Log.getLogWriter().info("diskDirMap is " + diskDirMap);

    // call the offline tools
    Object[] vmIds = diskDirMap.keySet().toArray();
    for (int i=0; i < vmIds.length; i++) {
      Integer vmid = (Integer)vmIds[i];
      List<File> diskDirList = diskDirMap.get(vmid);
      File[] diskDirArr = diskDirList.toArray(new File[]{});
      for (File diskDir: diskDirArr) {
        String[] diskDirFileNames = diskDir.list();
        for (String diskFileName: diskDirFileNames) {
          String searchStr1 = "BACKUP";
          String searchStr2 = ".if";
          if (diskFileName.startsWith(searchStr1) && diskFileName.endsWith(searchStr2)) {
            int index1 = diskFileName.indexOf(searchStr1);
            int index2 = diskFileName.indexOf(searchStr2);
            String diskStoreName = diskFileName.substring(index1 + searchStr1.length(), index2);
            runOfflineExport(vmid, diskStoreName, diskDirArr);
          }
        }
      }
    }
  }

  /** Run the offline export tool.
   * @param diskStoreName The name of the diskStore to export.
   * @param diskDirArr An Array of disk directories, one of which contains the .if
   *                   (info) file for diskStoreName.
   */
  public static void runOfflineExport(Integer vmid, String diskStoreName, File[] diskDirArr) {
    String diskDirStr = "";
    for (File diskDir: diskDirArr) {
      diskDirStr = diskDirStr + diskDir.getAbsolutePath() + " ";
    }

    String exeName = "gemfire";
    String command = "env GF_JAVA=" + System.getProperty("java.home") + "/bin/java ";
    if (HostHelper.isWindows()) {
      exeName = "gemfire.bat";
      command = "cmd /c set GF_JAVA=" + System.getProperty("java.home") + "/bin/java.exe && cmd /c ";
    }
    command = command + System.getProperty("gemfire.home") + 
                        File.separator + "bin" + File.separator + exeName + 
                        " export-disk-store " + diskStoreName + " " + diskDirStr + 
                        " -outputDir=snapshotDir_" + vmid;
    Log.getLogWriter().info("Calling offline export with command " + command);
    String result = ProcessMgr.fgexec(command, 0);
    Log.getLogWriter().info("Done calling offline export, result is " + result);
  }

  /** Return a Map of vmIDs (keys) and disk directories (values)
   * 
   * @return A Map where the key is a Integer (vmID) and the value is a List
   *                 of Files where each File is a directory of disk files for that vmID
   */
  public static Map<Integer, List<File>> getDiskDirMap() {
    Map<Integer, List<File>> aMap = new HashMap();
    String currDirName = System.getProperty("user.dir");
    File currDir = new File(currDirName);
    File[] currDirContents = currDir.listFiles();
    for (File fileInCurrDir: currDirContents) {
      String fileName = fileInCurrDir.getName();
      if (fileInCurrDir.isDirectory() && (fileName.indexOf("_disk_") >= 0)) {
        String searchStr = "vm_";
        int index1 = fileName.indexOf(searchStr);
        if (index1 >= 0) {
          int index2 = fileName.indexOf("_", searchStr.length());
          if (index2 >= 0) {
            Integer vmID = Integer.valueOf(fileName.substring(searchStr.length(), index2));
            List<File> aList = aMap.get(vmID);
            if (aList == null) {
              aList = new ArrayList();
              aMap.put(vmID, aList);
            }
            aList.add(fileInCurrDir);
          } else {
            throw new TestException("Test error; unexpected disk dir name format " + fileName);
          }
        } else {
          throw new TestException("Test error; unexpected disk dir name format " + fileName);
        }
      }
    }
    return aMap;
  }

  /** Change the order of the files in the array
   * 
   * @param fileArr An Array of Files to change order of.
   */
  protected static void scramble(File[] fileArr) {
    if (fileArr.length == 1) {
      return; // nothing to scramble
    }
    for (int i = 0; i < fileArr.length; i++) {
      int randInt = TestConfig.tab().getRandGen().nextInt(0, fileArr.length-1);
      File tmp = fileArr[i];
      fileArr[i] = fileArr[randInt];
      fileArr[randInt] = tmp;
    }
  }

  /** Return true if the list of disk directories contains an init file (*.if) 
   *  for the given diskStoreName,return false otherwise. 
   *   
   * @param diskDirList A list of Files, each one is a directory containing disk files. 
   * @param diskStoreName The diskStoreName to check for in the list of directories. 
   * @return true of the list of disk directories contains an init file (*.if) for the given diskStoreName,
   *                 false otherwise.
   */
  public static boolean diskDirsContainInfoFile(List<File> diskDirList, String diskStoreName) {
    for (File diskDir: diskDirList) {
      File[] contents = diskDir.listFiles();
      for (File diskFile: contents) {
        String diskFileName = diskFile.getName();
        if (diskFileName.endsWith(".if") && (diskFileName.indexOf(diskStoreName) >= 0)) {
          return true;
        }
      }
    }
    return false;
  }

  /** Given a diskStoreName and an array of disk directories:
   *     1) run the offline validator
   *     2) run the offline compactor
   *     3) run the offline validator again
   *  then check that the validator returns the same results before and
   *  after the compactor.
   *  
   * @param diskStoreName The name of a diskStore.
   * @param diskDirArr An array of Files, each element being a disk directory.
   */
  public static void runOfflineValAndCompaction(String diskStoreName,
      File[] diskDirArr) {
    Vector<String> diskStoreNames = TestConfig.tab().vecAt(DiskStorePrms.names);
    int i = diskStoreNames.indexOf(diskStoreName);
    Vector<String> maxOpLogSizes = TestConfig.tab().vecAt(DiskStorePrms.maxOplogSize, new HydraVector());
    long opLogSize = 0;
    if ((maxOpLogSizes.size() == 0) || (i < 0) || (i >= maxOpLogSizes.size())) {
      opLogSize = DiskStoreFactory.DEFAULT_MAX_OPLOG_SIZE;
    } else {
      opLogSize = Long.valueOf(maxOpLogSizes.get(i));
    }

    try {
      // run and save validator results before compaction
      Object[] tmp = runOfflineValidate(diskStoreName, diskDirArr);
      String beforeCompactionStr = (String)tmp[0];
      Map<String, Map<String, Integer>> beforeCompactionMap = (Map<String, Map<String, Integer>>)tmp[1];
      Log.getLogWriter().info("Before compaction, results map is " + beforeCompactionMap);

      // run compaction
      runOfflineCompaction(diskStoreName, diskDirArr, opLogSize);

      // run and save validator results after compaction
      tmp = runOfflineValidate(diskStoreName, diskDirArr);
      String afterCompactionStr = (String)tmp[0];
      Map<String, Map<String, Integer>> afterCompactionMap = (Map<String, Map<String, Integer>>)tmp[1];
      Log.getLogWriter().info("After compaction, results map is " + afterCompactionMap);

      // check that before results is the same as after results; this compares the entryCount and
      // bucketCount for each region in the disk files
      if (!beforeCompactionMap.equals(afterCompactionMap)) {
        throw new TestException("Before compaction, validator results was " + beforeCompactionStr +
            " and after compaction, validator results is " + afterCompactionStr + 
        ", but expect validator results for entryCount/bucketCount to be equal");
      }
    } catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }
  /** Run the offline compaction tool.
   * @param diskStoreName The diskStore name to use for compaction.
   * @param diskDirArr An Array of Files, each file is a disk dir.
   * @param opLogSize The opLog size for the diskStore.
   */
  private static void runOfflineCompaction(String diskStoreName,
      File[] diskDirArr, long opLogSize) {
    String diskDirStr = "";
    for (File diskDir: diskDirArr) {
      diskDirStr = diskDirStr + diskDir.getAbsolutePath() + " ";
    }
    String result = null;
    if (CliHelperPrms.getUseCli()) { // run offline compaction with gfsh
      // offline compaction is an offline command but can be run with gfsh connected or not

      // get a new cli instance; it is not connected
      boolean connected = false;
      CliHelper.execCommand(CliHelper.START_NEW_CLI, true);
      if (TestConfig.tab().getRandGen().nextBoolean()) { // randomly choose to connect
        CliHelper.execCommand(CliHelper.CONNECT_CLI, true);
        connected = true;
      }
      String command = "compact offline-disk-store --name=" + diskStoreName + " --disk-dirs=" + diskDirStr + " --max-oplog-size=" + opLogSize;
      result = CliHelper.execCommand(command, true)[1];
      if (!connected) { // leave here in a connected state
        CliHelper.execCommand(CliHelper.CONNECT_CLI, true);
      }
    } else {

      //DiskStoreImpl.offlineCompact(diskStoreName, diskDirArr, opLogSize);
      String exeName = "gemfire";
      String command = "env GF_JAVA=" + System.getProperty("java.home") + "/bin/java ";
      if (HostHelper.isWindows()) {
        exeName = "gemfire.bat";
        command = "cmd /c set GF_JAVA=" + System.getProperty("java.home") + "/bin/java.exe && cmd /c ";
      }
      command = command + System.getProperty("gemfire.home") + File.separator + "bin" +
          File.separator + exeName + " compact-disk-store " + 
          diskStoreName + " " + diskDirStr +
          "-maxOplogSize=" + opLogSize;
      Log.getLogWriter().info("Calling offline compaction tool with diskStoreName " + diskStoreName + 
          ", disk dirs " + diskDirStr + " and opLogSize " + opLogSize);
      result = ProcessMgr.fgexec(command, 0);
      Log.getLogWriter().info("Done calling offline compaction tool, result is " + result);
    }
    if (result.indexOf("Offline compaction") < 0) { // did not return normal result
      throw new TestException("Offline compaction tool returned " + result);
    }
  }

  /** Run the offline validate tool.
   * @param diskStoreName The name of the diskStore to validate.
   * @param diskDirArr An Array of disk directories, one of which contains the .if
   *                   (info) file for diskStoreName.
   * @return [0] The String results from the validator
   *         [1] Map with key being a regionName, and value being a Map with
   *         key "entryCount" and "bucketCount", values being Integers
   *         of the count for this dataStore.
   */
  public static Object[] runOfflineValidate(String diskStoreName, File[] diskDirArr) {
    String diskDirStr = "";
    for (File diskDir: diskDirArr) {
      diskDirStr = diskDirStr + diskDir.getAbsolutePath() + " ";
    }
    if (CliHelperPrms.getUseCli()) { // run validate with gfsh
      // validate is an offline command but can be run online as well; run it both ways
      // and make sure the output is identical
      
      // offline first
      CliHelper.execCommand(CliHelper.START_NEW_CLI, true);  // not connected however
      String command = "validate offline-disk-store --name=" + diskStoreName + " --disk-dirs=" + diskDirStr;
      String offlineResult = CliHelper.execCommand(command, true)[1];
      
      // now connect and try it again
      CliHelper.execCommand(CliHelper.CONNECT_CLI, true);
      String onlineResult = CliHelper.execCommand(command, true)[1];
      
      // compare the offline result with the online result
      if (!offlineResult.equals(onlineResult)) {
        throw new TestException("Expected " + command + " output when offline to be the same as " +
           " when online, but they are different. Offline results: " + offlineResult + ", online results: " +
           onlineResult);
      }
      return new Object[] {offlineResult, getValidateResultsMap(offlineResult)};
    } else {

      String exeName = "gemfire";
      String command = "env GF_JAVA=" + System.getProperty("java.home") + "/bin/java ";
      if (HostHelper.isWindows()) {
        exeName = "gemfire.bat";
        command = "cmd /c set GF_JAVA=" + System.getProperty("java.home") + "/bin/java.exe && cmd /c ";
      }
      command = command + System.getProperty("gemfire.home") + File.separator + "bin" +
          File.separator + exeName + " validate-disk-store " + 
          diskStoreName + " " + diskDirStr;
      Log.getLogWriter().info("Calling offline validate tool with diskStoreName " + diskStoreName + 
          " and disk dirs " + diskDirStr);
      //DiskStoreImpl.validate(diskStoreName, diskDirArr);
      String result = ProcessMgr.fgexec(command, 0);
      Log.getLogWriter().info("Done calling offline validate tool, result is " + result);
      return new Object[] {result, getValidateResultsMap(result)};
    }
  }

  /** Given a String which is the result from the offline validator tool,
   *  return a map where the key (String) is the name of a region and the
   *  value is a Map
   * @param resultsStr The result of an offline validation
   * @return Map with key being a regionName, and value being a Map with
   *         key "entryCount" and "bucketCount", values being Integers
   *         of the count for this dataStore.
   */
  private static Map<String, Map<String, Integer>> getValidateResultsMap(String resultsStr) {
    Map<String, Map<String, Integer>> resultMap = new TreeMap();
    String[] tokens = resultsStr.split("[\\s]+"); // split on white space
    int tokensIndex = 0;
    int calculatedEntryCountTotal = 0;
    while (tokensIndex < tokens.length) {
      String token = tokens[tokensIndex];
      if (token.startsWith("/")) { // token is a region name
        token = token.split(":")[0]; // strip off ending :
        if (tokensIndex+2 < tokens.length) { // next 2 tokens are entryCount and bucketCount
          String entryCountToken = tokens[tokensIndex+1];
          String bucketCountToken = tokens[tokensIndex+2];
          try {
            int entryCount = -1;
            int bucketCount = 0;
            int i = entryCountToken.indexOf("=");
            if (i > 0) {
              entryCount = Integer.valueOf(entryCountToken.substring(i+1, entryCountToken.length()));
            }
            i = bucketCountToken.indexOf("=");
            if (i > 0) {
              bucketCount = Integer.valueOf(bucketCountToken.substring(i+1), bucketCountToken.length(
              ));
            }
            if ((entryCount < 0) || (bucketCount < 0)) { // not in the format we expected
              throw new TestException("Results from offline validator not in expected form " +
                  " or had negative entryCount or negative bucketCount: " +  resultsStr);
            }
            Map countMap = new TreeMap();
            countMap.put("entryCount", entryCount);
            countMap.put("bucketCount", bucketCount);
            resultMap.put(token /* regionName */, countMap);
            calculatedEntryCountTotal += entryCount;
          } catch (NumberFormatException e) {
            throw new TestException("entryCount/bucketCount results from offline validator " +
                " not in expected format: " + resultsStr);
          }
        }
      } else if (token.equals("Total")) { // look for totals line "Total number of region entries in this disk store is:"
        if (tokensIndex + 10 < tokens.length) {
          String line = token;
          for (int i = 1; i <= 9; i++) {
            line = line + " " + tokens[tokensIndex+i];
          }
          if (line.equals("Total number of region entries in this disk store is:")) {
            try {
              int totalFromResults = Integer.valueOf(tokens[tokensIndex+10]);
              if (totalFromResults != calculatedEntryCountTotal) {
                throw new TestException("Total of entryCounts for each region (" + calculatedEntryCountTotal +
                    ") is not equal to the reported total " + totalFromResults +
                    "\n" + resultsStr);
              }
            } catch (NumberFormatException e) {
              throw new TestException("Totals reported by validator not in expected format: " + resultsStr);
            }
          }
        }
      }
      tokensIndex++;
    }

    // look for final line in output; verify the total entries reported by the tool
    // to make sure the final results are there (rather than an exception)
    int index = resultsStr.indexOf("Total number of region entries in this disk store is: ");
    if (index < 0) {
      throw new TestException("Result from offline validator did not include expected totals line: " + resultsStr);
    }
    return resultMap;
  }

  /**
   * Locates a previously created backup directory associated with an online backup counter.
   * @param counter a ParRegBB.onlineBackupNumber
   * @throws TestException a backup directory could not be located or was malformed (not a directory or contains no children or too many children).
   */
  private static String getBackupDir(long counter) {
    File backupDir = new File("backup_" + counter);

    if(!backupDir.exists()) {
      throw new TestException("PersistenceUtil#getBackupDir: Backup directory " + backupDir.getName() + " does not exist.");
    }
    
    File[] files = backupDir.listFiles();
    
    if((null == files) || (files.length != 1)) {
      throw new TestException("PersistenceUtil#getBackupDir: Backup directory " + backupDir.getName() + " is malformed or is not a directory");      
    }
    
    return IOUtils.tryGetCanonicalFileElseGetAbsoluteFile(files[0]).getAbsolutePath();
  }
  
  /** 
   * Run the online backup command
   * @param incremental set to true to perform an incremental backup.
   * @param performCompactionOnBackup validation and compaction are peformed on the backup directory when true.
   */
  public static void doOnlineBackup(boolean incremental) {
    // run the online back tool
    long counter = ParRegBB.getBB().getSharedCounters().incrementAndRead(ParRegBB.onlineBackupNumber);
    String dirName = "backup_" + counter;
    File dir = new File(dirName);
    String baselineDir = (incremental ? getBackupDir(counter - 1) : null);
    dir.mkdir();
    // workaround for bug 42432; when this bug is fixed, remove the following line and uncomment the line after
    if (CliHelperPrms.getUseCli()) { // run backup with gfsh
      if (incremental) {
        CliHelper.execCommand("backup disk-store --dir=" + dirName + " --baseline-dir=" + baselineDir, true);
      } else {
        CliHelper.execCommand("backup disk-store --dir=" + dirName, true);
      }
    } else {
      if ((counter % 2 == 1) && (!HostHelper.isWindows())) { // run online backup via the command line tool
        //    if (counter % 2 == 1) { // run online backup via the command line tool
        String exeName = "gemfire";
        String command = "env GF_JAVA=" + System.getProperty("java.home") + "/bin/java ";
        if (HostHelper.isWindows()) {
          exeName = "gemfire.bat";
          command = "cmd /c set GF_JAVA=" + System.getProperty("java.home") + "/bin/java.exe && cmd /c ";
        }
        Properties p = DistributedSystemHelper.getDistributedSystem().getProperties();
        Object mcastPort = p.get("mcast-port");
        Object locators = p.get("locators");
        command = command + System.getProperty("gemfire.home") + File.separator + "bin" +
            File.separator + exeName + " -J-Dgemfire.mcast-port=" + mcastPort +
            " -J-Dgemfire.locators=" + locators + " backup ";

        /*
         * Build the incremental command line option if performing an incremental backup.
         */
        String incrementalOption = (incremental ? ("-baseline=" + baselineDir + " ") : "");

        command += (incrementalOption + dirName);

        Log.getLogWriter().info("Calling online backup tool from command line tool...");
        String result = ProcessMgr.fgexec(command, 0);
        Log.getLogWriter().info("Done calling online backup tool, result is " + result);
        if (result.indexOf("Backup successful") < 0) {
          throw new TestException("On line backup was not successful, result from online backup is " + result);
        }
      } else { // run online backup with the admin API
        try {
          Log.getLogWriter().info("Calling online backup tool from admin API...");

          BackupStatus bStatus =null;

          /*
           * Call appropriate backupAllMembers based on incremental flag.
           */
          if(incremental) {
            bStatus = AdminHelper.getAdminDistributedSystem().backupAllMembers(dir,new File(baselineDir));                    
          } else {
            bStatus = AdminHelper.getAdminDistributedSystem().backupAllMembers(dir);          
          }

          Map<DistributedMember, Set<PersistentID>> backedUp = bStatus.getBackedUpDiskStores();
          Set<PersistentID> offline = bStatus.getOfflineDiskStores();
          Log.getLogWriter().info("Done calling online backup tool from admin API, backed up disk stores " + backedUp);
          Log.getLogWriter().info("Done calling online backup tool from admin API, offline disk stores: " + offline);
          Set keySet = backedUp.keySet();
          if (keySet.contains(null)) {
            throw new TestException("BackupStatus.getBackedUpDiskStores contains a null key: " + backedUp);
          }
          if (offline.size() != 0) {
            throw new TestException("Expected BackupStatus.getOfflineDiskStores set to be empty, but it is " + offline);
          }
        } catch (AdminException e) {
          throw new TestException(TestHelper.getStackTrace(e));
        }
      }

      // run the validator and compaction on the backup files
      // workaround for bug 42432; when this bug is fixed, remove the following if
      if (HostHelper.isWindows()) {
        Log.getLogWriter().info("Not running offline val and compaction on windows to avoid bug 42432");
        return;
      }
    }
    
    /*
     * Don't compact if performing incremental backups.
     */
    if(!ParRegPrms.getDoIncrementalBackup()) {
      runOfflineValAndCompactionOnBackup(dir);      
    }
  }

  /** Run the offline validator and compaction tool on a backup.
   * 
   * @param backupDir The directory containing a backup.
   */
  private static void runOfflineValAndCompactionOnBackup(File backupDir) {
    // Get a map of key: diskStoreName, and value (list of diskDirs (Files))
    // backup directories are in the form:
    // <backupDir>/<dateDir>/<host & pid dir>/diskstores/<diskstore name>/dir<N>/<disk files *.crf *.drf *.if etc>
    // such as myBackup/2010-10-12-09-54/bilbo_7029_v1_21117_33277/diskstores/diskStore1/dir0
    File[] dirContents = backupDir.listFiles(); // should contain 1 thing; directory date name
    if (dirContents.length != 1) {
      throw new TestException("Expected dirContents to be size 1, but it is size " + dirContents.length);
    }
    dirContents = dirContents[0].listFiles(); // contents contains 1 or more <host & pid dir> directories
    for (File hostAndPidDir: dirContents) {
      File[] hostAndPidDirContents = hostAndPidDir.listFiles(); // contents contains directory "diskStores"
      for (File aFile: hostAndPidDirContents) { // look for directory called diskStores
        if (aFile.getName().equals("diskstores")) { // found directory called diskStores
          File[] diskStoresContents = aFile.listFiles(); // contents is directories named for each diskStore
          for (File diskStoreDir: diskStoresContents) { // for each directory named after a diskStore
            String diskStoreName = diskStoreDir.getName(); // find diskStore name
            int index = diskStoreName.indexOf("_");
            if (index >= 0) {
               diskStoreName = diskStoreName.substring(0, index);
            } else {
               throw new TestException("Test problem, unable to obtain disk store name from " + diskStoreName);
            }
            File[] diskDirs = diskStoreDir.listFiles(); // array of dir<N> directories
            scramble(diskDirs);
            runOfflineValAndCompaction(diskStoreName, diskDirs);
          }
        }
      }
    }
  }

  /** Using the command line tool, run list-missing-disk-stores
   * 
   */
  public static String runListMissingDiskStores() {
    if (CliHelperPrms.getUseCli()) {
      String command = "show missing-disk-stores";
      String result = (CommandTest.testInstance.execCommand(command, true)[1]);
      return result;
    } else {
      String exeName = "gemfire";
      String command = "env GF_JAVA=" + System.getProperty("java.home") + "/bin/java ";
      if (HostHelper.isWindows()) {
        exeName = "gemfire.bat";
        command = "cmd /c set GF_JAVA=" + System.getProperty("java.home") + "/bin/java.exe && cmd /c ";
      }
      Properties p = DistributedSystemHelper.getDistributedSystem().getProperties();
      Object mcastPort = p.get("mcast-port");
      Object locators = p.get("locators");
      command = command + System.getProperty("gemfire.home") + File.separator + "bin" +
          File.separator + exeName + " -J-Dgemfire.mcast-port=" + mcastPort +
          " -J-Dgemfire.locators=" + locators + " list-missing-disk-stores";
      Log.getLogWriter().info("Calling list-missing-disk-stores from command line tool...");
      String result = ProcessMgr.fgexec(command, 0);
      Log.getLogWriter().info("Done calling list-missing-disk-stores tool, result is " + result);

      // verify result
      String[] elements = result.split("\n");
      for (String line: elements) {
        if ((line.startsWith("Connecting to distributed system")) ||
            (DISK_STORE_ID.matcher(line).matches())) {
          // ok
        } else {
          throw new TestException("Unexpected output from list-missing-disk-stores: " +
              result);
        }
      }
      return result;
    }
  }
  
  /** Using the command line too, run the revokeMissingDiskStore tool.
   * 
   *  @param id The persistent member to revoke. 
   * 
   */
  public static void runRevokeMissingDiskStore(PersistentID id) {
    String result = null;
    if (CliHelperPrms.getUseCli()) {
      String command = "revoke missing-disk-store --id=" + id.getUUID().toString();
      result = (CommandTest.testInstance.execCommand(command, true)[1]);
      if (result.indexOf("Missing disk store successfully revoked") < 0) {
        throw new TestException("Unexpected result: " + result + ", from command " + command);
      }
    } else {
      String exeName = "gemfire";
      String command = "env GF_JAVA=" + System.getProperty("java.home") + "/bin/java ";
      if (HostHelper.isWindows()) {
        exeName = "gemfire.bat";
        command = "cmd /c set GF_JAVA=" + System.getProperty("java.home") + "/bin/java.exe && cmd /c ";
      }
      Properties p = DistributedSystemHelper.getDistributedSystem().getProperties();
      Object mcastPort = p.get("mcast-port");
      Object locators = p.get("locators");
      String uuid = id.getUUID().toString();
      command = command + System.getProperty("gemfire.home") + File.separator + "bin" +
          File.separator + exeName + " -J-Dgemfire.mcast-port=" + mcastPort +
          " -J-Dgemfire.locators=" + locators + " revoke-missing-disk-store " +
          uuid;
      Log.getLogWriter().info("Calling revoke-missing-disk-store from command line tool...");
      result = ProcessMgr.fgexec(command, 0);
      Log.getLogWriter().info("Done calling revoke-missing-disk-store tool, result is " + result);

      // verify result
      int index = result.indexOf("The following disk stores are still missing:");
      if (result.indexOf(uuid, index) >= 0) {
        throw new TestException("revoke-missing-disk-store command line tool revoked " +
            uuid + ", but command results shows it is still missing: " +
            result);
      }
      String[] elements = result.split("\n");
      for (String line: elements) {
        if ((line.startsWith("Connecting to distributed system")) ||
            (line.startsWith("The following disk stores are still missing:")) ||
            (DISK_STORE_ID.matcher(line).matches()) ||
            (line.startsWith("revocation was successful and no disk stores are now missing"))) {
          // ok
        } else {
          throw new TestException("Unexpected output from revoke-missing-disk-stores: " +
              result);
        }
      }
    }
    if ((result.indexOf("The following disk stores are still missing:") >= 0) &&
        (result.indexOf("revocation was successful and no disk stores are now missing") >= 0)) {
      throw new TestException("Result of revoke-missing-disk-stores reports disk stores " +
          "are still missing AND no disk stores are missing, result is " + result);
    }
  }
}
