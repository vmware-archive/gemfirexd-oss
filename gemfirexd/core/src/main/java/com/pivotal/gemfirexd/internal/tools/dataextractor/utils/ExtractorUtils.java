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
package com.pivotal.gemfirexd.internal.tools.dataextractor.utils;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Map.Entry;

import org.apache.commons.io.FileSystemUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.gemstone.gemfire.internal.cache.Oplog;
import com.pivotal.gemfirexd.internal.tools.dataextractor.comparators.ServerNameComparator;
import com.pivotal.gemfirexd.internal.tools.dataextractor.comparators.RegionViewSortComparator;
import com.pivotal.gemfirexd.internal.tools.dataextractor.diskstore.GFXDDiskStoreImpl;
import com.pivotal.gemfirexd.internal.tools.dataextractor.domain.ServerInfo;
import com.pivotal.gemfirexd.internal.tools.dataextractor.extractor.GemFireXDDataExtractorImpl;
import com.pivotal.gemfirexd.internal.tools.dataextractor.report.views.PersistentView;
import com.pivotal.gemfirexd.internal.tools.dataextractor.snapshot.GFXDSnapshotExportStat;
/****
 * Contains utility methods used across by extractor tools and its tests. 
 * @author bansods
 * @author jhuynh
 *
 */
public final class ExtractorUtils {

  //File Extensions
  private static final String LOCK_FILE_EXTENSION = ".lk";
  private static final String CRF_EXTENSION = Oplog.CRF_FILE_EXT;
  private static final String KRF_EXTENSION = Oplog.KRF_FILE_EXT;
  private static final String IF_EXTENSION = ".if";
  private static final String DRF_EXTENSION = Oplog.DRF_FILE_EXT;
  private static final String INDEX_KRF_EXTENSION = ".idxkrf";
  static ExtensionFilter extensionFilter = new ExtensionFilter();

  static {
    extensionFilter.addExtension(LOCK_FILE_EXTENSION);
    extensionFilter.addExtension(CRF_EXTENSION);
    extensionFilter.addExtension(KRF_EXTENSION);
    extensionFilter.addExtension(IF_EXTENSION);
    extensionFilter.addExtension(DRF_EXTENSION);
    extensionFilter.addExtension(INDEX_KRF_EXTENSION);
  }
  
  
  private static final class ExtensionFilter implements FilenameFilter {
    public final List<String> extensionList = new ArrayList<String>();
    
    @Override
    public boolean accept(File dir, String name) {
      boolean isMatched = false;
      for (String extention : extensionList) {
        if (name.endsWith(extention)) {
          isMatched = true;
          break;
        }
      }
      return isMatched;
    }

    public void addExtension(String extention) {
      extensionList.add(extention);
    }
  }
  
  private static final class DiskStoreNameFilter implements FilenameFilter {
    final String diskStoreName; 

    public DiskStoreNameFilter(String diskStoreName) {
      this.diskStoreName = diskStoreName;
    }
    @Override
    public boolean accept(File file, String name) {
      return name.contains(diskStoreName);
    }
  }
  
  /*****
   * Ranks the DDL by Max Seq Id, Persistent View and then groups ddl stats by the file contents.
   * @param hostToDdlMap
   * @return rankedDDLs
   */
  public static List<List<GFXDSnapshotExportStat>> rankAndGroupDdlStats(Map<ServerInfo, List<GFXDSnapshotExportStat>> hostToDdlMap) {
    //Create the persistent view for the exported DDL stats.
    PersistentView pv = PersistentView.getPersistentViewForDdlStats(hostToDdlMap);
    List<GFXDSnapshotExportStat> ddlStats = new ArrayList<GFXDSnapshotExportStat>();
    
    for (Map.Entry<ServerInfo, List<GFXDSnapshotExportStat>> entry : hostToDdlMap.entrySet()) {
      ServerInfo member = entry.getKey();
      List<GFXDSnapshotExportStat> statList =entry.getValue();
      GFXDSnapshotExportStat stat =  statList.get(0);
      stat.setServerName(member.getServerName());
      ddlStats.add(stat);
    }
    
    final ServerNameComparator serverNameComparator = new ServerNameComparator();
    final RegionViewSortComparator rvSortComparator = new RegionViewSortComparator(serverNameComparator);
    
    Collections.sort(ddlStats, rvSortComparator);
    
    return groupByContent(ddlStats);
  }
  
  /****
   * Groups the DDL stats by the actual content of the extracted DDL's
   * @param rankedDdlStats
   * @return List of grouped DDL stats
   */
  public static List<List<GFXDSnapshotExportStat>> groupByContent(List<GFXDSnapshotExportStat> rankedDdlStats) {
    int length = rankedDdlStats.size();
    int i=0;
    List<List<GFXDSnapshotExportStat>> rankedAndGroupedDdlStats = new ArrayList<List<GFXDSnapshotExportStat>>();
    
    while (i<length) {
      GFXDSnapshotExportStat stat1 = rankedDdlStats.get(i);
      List<GFXDSnapshotExportStat> statGroup = new ArrayList<GFXDSnapshotExportStat>();
      statGroup.add(stat1);
      int j=i+1;
      while (j<length) {
        GFXDSnapshotExportStat stat2 = rankedDdlStats.get(j);
        if (isDDLFileSame(stat1, stat2)) {
          //GemFireXDDataExtractor.logInfo("DDL is SAME for " + stat1.getServerName() + " " + stat2.getServerName());
          statGroup.add(stat2);
          j++;
        } else {
          break;
        }
      }
      i=j;
      rankedAndGroupedDdlStats.add(statGroup);
    }
    return rankedAndGroupedDdlStats;
  }
  
  /****
   * Checks if the DDL file is same for stat1 and stat2
   * @param stat1
   * @param stat2
   * @return true if the files are same
   */
  public static boolean isDDLFileSame(GFXDSnapshotExportStat stat1, GFXDSnapshotExportStat stat2)  {
    File ddlFile1  = new File(stat1.getFileName());
    File ddlFile2 = new File(stat2.getFileName());
    try {
      return FileUtils.contentEquals(ddlFile1, ddlFile2);
    }catch (Exception e) {
      GemFireXDDataExtractorImpl.logInfo("Exception occurred while comparing the DDL", e);
    }
    return false;
  }
  
  /***
   * Reads the sql statements from a SQL file 
   * @param ddlFilePath Path of the sqlFile
   * @return SQL statements
   * @throws IOException
   */
  public static List<String> readSqlStatements(String ddlFilePath) throws IOException {
    List<String> lines = FileUtils.readLines(new File(ddlFilePath));
    List<String> ddlStatements = new ArrayList<String>();

    StringBuilder sb = new StringBuilder();

    for (String line : lines) {
      line = line.trim();

      if (!line.startsWith("--")) {
        int scIndex = line.indexOf(";");
        int commentStartIndex = line.indexOf("--");

        if (commentStartIndex != -1) {
          line = line.substring(0, commentStartIndex);
        } 

        //Ignore everything after the first semi-colon
        if (scIndex != -1) {
          line = line.substring(0, scIndex);
          sb.append(line);
          ddlStatements.add(sb.toString());
          sb = new StringBuilder();
        } else {
          sb.append(line).append(" ");

        }
      }
    }
    return ddlStatements;
  }
  
  /*** DDL read and replay methods ***/
  public static void executeDdlFromSqlFile(Connection conn, List<String> ddlStatements) throws IOException, SQLException {
    Statement statement = null;
    try {
      statement = conn.createStatement();
      //Progess as much as you can 

      for (String ddlStatement : ddlStatements) {
        if (!ddlStatement.startsWith("--")) {
          try {
            statement.execute(ddlStatement);
          } catch (Exception e) {
            GemFireXDDataExtractorImpl.logSevere("Exception occurred while executing : " + ddlStatement, e, GemFireXDDataExtractorImpl.getReportableErrors());
          }
        }
      }
    } finally {
      if (statement != null) {
        statement.close();
      }
    }
  }

  /*** File Manipulation Methods for tests***/
  public static void cleanWorkingDirectory()  {    
    try {
      File workingDirectory = new File(System.getProperty("user.dir"));
      cleanFiles(workingDirectory, extensionFilter);
      File ddDir = new File(workingDirectory, "datadictionary");
      if (ddDir.exists() && ddDir.isDirectory()) {
        FileUtils.deleteDirectory(ddDir);
      }
    } catch (Exception e) {
      GemFireXDDataExtractorImpl.logInfo("Error occured while cleaning the working directory", e);
    }
  }
  
  /**
   * Cleans file in a directory that have the extenstion specified by the extension filter
   * @param dir target directory
   * @param filter FileFilter for extensions
   */
  private static void cleanFiles(File dir, ExtensionFilter filter)  {
    File [] files = dir.listFiles(filter);
    if (files != null && files.length > 0) {
      for (File file : files) {
        try {
          FileUtils.forceDelete(file);
        } catch (IOException e) {
          GemFireXDDataExtractorImpl.logInfo("Error occured while cleaning up the files", e);
        }
      }
    }
  }
  
  public static void cleanDiskStores(List<DiskStoreImpl> diskStores, boolean deleteDiskStoreFiles) {
    for (DiskStoreImpl diskStore : diskStores) {
      cleanDiskStore(diskStore, deleteDiskStoreFiles);
    }
  }

  private static void cleanDiskStore(DiskStoreImpl diskStore, boolean deleteDiskStoreFiles) {
    GFXDDiskStoreImpl.closeDiskStoreFiles(diskStore);
    if (deleteDiskStoreFiles) {
      File [] diskDirs = diskStore.getDiskDirs();
      for (File diskDir : diskDirs) {
        if (diskDir.exists()) {
          cleanFiles(diskDir, extensionFilter);
        }
      }
    }
  }
  
  /***
   * Determines if the target directory for extraction has sufficient space to store the output of data extraction
   * @param serverInfoMap
   * @param targetDirectory
   * @return True if sufficient space , false otherwise
   * @throws IOException
   */
  public static boolean checkDiskSpaceInTargetDirectory(Map<String, ServerInfo> serverInfoMap, String targetDirectory) throws IOException {
    long totalDiskDirSize = getTotalSize(serverInfoMap);
    long totalSpaceAvailable = FileSystemUtils.freeSpaceKb(targetDirectory);

    GemFireXDDataExtractorImpl.logInfo("Total size of data to be extracted : " +  (double)totalDiskDirSize/1024d +  "MB");
    GemFireXDDataExtractorImpl.logInfo("Disk space available in the output directory : " + (double)totalSpaceAvailable/1024d + "MB");

    if (totalSpaceAvailable < totalDiskDirSize) {
      if ("n".equalsIgnoreCase(getUserInput())) {
        return false;
      }
    } else {
      GemFireXDDataExtractorImpl.logInfo("Sufficient disk space to carry out data extraction");
    }
    return true;
  }

  protected static boolean checkDiskSpaceInTargetDirectory(String targetDirectory) throws IOException {
    long totalDiskDirSize = getTotalSize(targetDirectory);
    long totalSpaceAvailable = FileSystemUtils.freeSpaceKb(targetDirectory);
    GemFireXDDataExtractorImpl.logInfo("Estimated data to be extracted : " +  (double)totalDiskDirSize/1024d +  "MB");
    GemFireXDDataExtractorImpl.logInfo ("Disk space available in the output directory : " + (double)totalSpaceAvailable/1024d + "MB");
    if (totalSpaceAvailable < totalDiskDirSize) {
      if ("n".equalsIgnoreCase(getUserInput())) {
        return false;
      }
    } else {
      GemFireXDDataExtractorImpl.logInfo("Sufficient disk space to carry out data extraction");
    }
    return true;
  }
  
  protected static String getUserInput() {
    GemFireXDDataExtractorImpl.logInfo("Possibly insufficient disk space to carry out data extraction");
    String userInput = null;
    do {
      System.out.println("Do you wish to continue [y\n] ?");
      Scanner in = new Scanner(System.in);
      userInput = in.next();
    } while(!("y".equalsIgnoreCase(userInput) || "n".equalsIgnoreCase(userInput)));

    return userInput;
  }
  
  public static int getNumberOfThreads (List<ServerInfo> serverInfoList, List<DiskStoreImpl> diskStores) {
    /****
     * As we extract one diskstore at a time for a given server. 
     * Here the logic is we find the largest disk-store and use its size as a basis for worst case heap memory required 
     * for salvaging a server at a given time.
     * This helps in determining how may servers can we salvage in parallel for given heap memory.
     * This is a very conservative estimate. 
     */
    long maxDiskStoreSizeOnDisk = 0;
    int maxNumberOfServersInParallel = 1;
    final double mbDiv = Math.pow(1024, 2);

    for (ServerInfo serverInfo : serverInfoList) {
      long maxDiskStoreSizeForServer = ExtractorUtils.getMaxDiskStoreSizeForServer(serverInfo, diskStores);
      if (maxDiskStoreSizeOnDisk < maxDiskStoreSizeForServer) {
        maxDiskStoreSizeOnDisk = maxDiskStoreSizeForServer;
      }
    }

    GemFireXDDataExtractorImpl.logInfo("Maximum disk-store size on disk " + maxDiskStoreSizeOnDisk/mbDiv + " MB");
    MemoryMXBean memBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage heapMemUsage = memBean.getHeapMemoryUsage();
    long usedMemory = heapMemUsage.getUsed();
    long committedMemory = heapMemUsage.getCommitted();

    //For safety using the committedMemory for calculation, as it is the memory that is guaranteed to be available for the VM. 
    long availableMemory = (committedMemory - usedMemory);
    GemFireXDDataExtractorImpl.logInfo("Available memory : " + availableMemory/mbDiv + " MB");

    double maxMemoryPerServer = (1.2)*maxDiskStoreSizeOnDisk;

    //Setting the lower limit
    if (maxMemoryPerServer < 1) {
      maxMemoryPerServer = 1;
    }

    GemFireXDDataExtractorImpl.logInfo("Estimated memory needed per server : " + maxMemoryPerServer/mbDiv + " MB");

    if (availableMemory < maxMemoryPerServer) {
      GemFireXDDataExtractorImpl.logWarning("Not enough memory to extract the server, extractor could possibly run out of memory");
    }

    maxNumberOfServersInParallel =  (int) (availableMemory/maxMemoryPerServer);

    if (maxNumberOfServersInParallel < 1) {
      maxNumberOfServersInParallel = 1;
    }

    GemFireXDDataExtractorImpl.logInfo("Recommended number of threads to extract server(s) in parallel : " + maxNumberOfServersInParallel);
    return maxNumberOfServersInParallel;
  }
  
  private static long getTotalSize(Map<String, ServerInfo> serverInfoMap) {
    long size = 0;
    Iterator<Entry<String, ServerInfo>> serverInfoIterator = serverInfoMap.entrySet().iterator();

    while (serverInfoIterator.hasNext()) {
      Entry<String, ServerInfo> entry = serverInfoIterator.next();
      ServerInfo serverInfo = entry.getValue();
      List<String> diskDirPaths = serverInfo.getDiskStoreDirectories();
      for (String diskDirPath : diskDirPaths) {
        size += getTotalSize(diskDirPath);
      }
    }
    return size;
  }

  private static long getTotalSize(String directory) {
    File diskDirFile = new File(directory);
    return FileUtils.sizeOfDirectory(diskDirFile)/1024;
  }
  
  /****
   * Gets the size of the largest disk-store in a directory 
   * @param diskStores List of diskstores
   * @param diskStoreDirPath path of the directory where the disk-store are created
   * @return size of the largest disk-store
   */
  public static long getMaxDiskStoreSizeInDir(List<DiskStoreImpl> diskStores, String diskStoreDirPath) {
    File diskStoreDir = new File(diskStoreDirPath);
    long maxDiskStoreSizeOnDisk =0;
    for (DiskStoreImpl diskStore : diskStores) {
      String[] fileNames = diskStoreDir.list(new DiskStoreNameFilter(diskStore.getName()));
      long diskStoreSize = 0;

      if (fileNames != null && fileNames.length > 0) {
        for (String fileName : fileNames) {
          File file = new File(FilenameUtils.concat(diskStoreDirPath, fileName));
          if (file.exists()) {
            diskStoreSize += FileUtils.sizeOf(file); 
          }
        }
      }

      if (maxDiskStoreSizeOnDisk < diskStoreSize) {
        maxDiskStoreSizeOnDisk = diskStoreSize;
      }
    }
    return maxDiskStoreSizeOnDisk;
  }
  
  /*****
   * Determines the size of the largest disk-store for a server
   * @param serverInfo 
   * @param diskStores
   * @return size of the largest disk store 
   */
  public static long getMaxDiskStoreSizeForServer(ServerInfo serverInfo, List<DiskStoreImpl> diskStores) {
    long maxDiskStoreSizeInServer = 0;
    List<String> diskStoreDirPaths = serverInfo.getDiskStoreDirectories();

    for (String diskStoreDirPath : diskStoreDirPaths) {
      long maxDiskStoreSizeInDir = getMaxDiskStoreSizeInDir(diskStores, diskStoreDirPath);

      if (maxDiskStoreSizeInServer < maxDiskStoreSizeInDir) {
        maxDiskStoreSizeInServer = maxDiskStoreSizeInDir;
      }
    }
    return maxDiskStoreSizeInServer;
  }

}
