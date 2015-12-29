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
package util;

import hydra.Log;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.management.ManagementException;

import resultsUtil.FileLineReader;

/**
 * Class to check the gemfire logs for suspicious strings. This provides early
 * detection of suspect strings.
 * 
 * @author lynn
 * 
 */
public class LogChecker {

  private static List<String> suspectStrings = null;
  private static List<String> excludeStrings = null;

  static {
    suspectStrings = new ArrayList<String>();
    suspectStrings.add(java.lang.ClassCastException.class.getName());
    suspectStrings.add(java.lang.NullPointerException.class.getName());
    suspectStrings.add("java.io.IOException: Resource deadlock avoided");
    suspectStrings.add("java.nio.channels.OverlappingFileLockException");
    suspectStrings.add("java.io.IOException: Bad file number");
    suspectStrings.add(InternalGemFireError.class.getName());
    suspectStrings.add(ManagementException.class.getName());
    suspectStrings.add("Could not start jmx manager");
    suspectStrings.add("java.lang.Exception");
    
    excludeStrings = new ArrayList<String>();
    excludeStrings.add("[fine");
    // work around bug 48904
    excludeStrings.add("Cannot have overlapping ranges");
  }

  /**
   * Check both system logs and bgExec logs for suspect strings.
   */
  public static void HydraTask_checkLogs() {
    long leader = LogCheckerBB.getBB().getSharedCounters().incrementAndRead(LogCheckerBB.leader);
    if (leader != 1) {
      return;
    }
    Log.getLogWriter().info("Checking the logs...");
    List<File> logsToCheck = getLogs();
    for (File aFile : logsToCheck) {
      Log.getLogWriter().info("Checking " + aFile.getAbsolutePath());
      FileLineReader reader;
      try {
        reader = new FileLineReader(aFile);
      } catch (FileNotFoundException e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }
      String line = reader.readNextLine();
      while (line != null) {
        if (contains(suspectStrings, line) && !contains(excludeStrings, line)) {
          throw new TestException(aFile.getAbsolutePath() + " contains " + line);
        }
        line = reader.readNextLine();
      }
    }
  }

  /** Return true if aStr contains any of the strings in targetStrs, false otherwise.
   *
   *  @param targetStrs - A List of Strings containing text of interest.
   *  @param aStr - The string to test.
   */
  private static boolean contains(List<String> targetStrs, String aStr) {
    for (String target: targetStrs) {
      if (aStr.contains(target)) {
        return true;
      }
    }
    return false;
  }

  private static List<File> getLogs() {
    String baseTestDir = getBaseTestDir();
    if (baseTestDir == null) {
      throw new TestException("Test problem: Could not find base test directory");
    }
    File currentDir = new File(baseTestDir);
    List<File> logList = new ArrayList();
    getLogs(currentDir, logList);
    return logList;
  }

  /** Find the directory containing the Master.log; it is the base test dir.
   *  Typically that directory will be the user.dir but if hydra.HostPrms-userDirs
   *  is specified, it might be in a parent of user.dir.
   * @return
   */
  private static String getBaseTestDir() {
    String currentDirPath = System.getProperty("user.dir");
    return _getBaseTestDir(currentDirPath);
  }
  
  private static String _getBaseTestDir(String fileName) {
    if (fileName == null) {
      return null;
    }
    File currentDir = new File(fileName);
    if (currentDir.isDirectory()) {
      String[] dirContents = currentDir.list();
      for (String name: dirContents) {
        if (name.startsWith("Master") && name.endsWith(".log")) {
          return currentDir.getAbsolutePath();
        }
      }
    }
    return _getBaseTestDir(currentDir.getParent());    
  }

  private static void getLogs(File currentDir, List<File> logs) {
    File[] dirContents = currentDir.listFiles();
    for (File aFile: dirContents) {
      if (aFile.isDirectory()) {
        getLogs(aFile, logs);
      } else {
        String fileName = aFile.getName();
        if (fileName.startsWith("system") && fileName.endsWith(".log")) {
          logs.add(aFile);
        } else if (fileName.startsWith("bgexec") && fileName.endsWith(".log")) {
          logs.add(aFile);
        } else if (fileName.startsWith("gfsh") && (fileName.indexOf(".log") >= 0)) {
          logs.add(aFile);
        }
      }
    }
  }

}
