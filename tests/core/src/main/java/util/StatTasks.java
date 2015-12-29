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

import hydra.ClientDescription;
import hydra.ClientPrms;
import hydra.Log;
import hydra.TestConfig;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import perffmwk.PerfStatMgr;
import perffmwk.PerfStatValue;
import perffmwk.StatSpecTokens;

import com.gemstone.gemfire.LogWriter;

public class StatTasks {

  /**
   *  ENDTASK to throw a fit if no transactions occurred.
   */
  public static void ensureTxOccurredTask() {
    int numCommitSuccesses = 0;
    int numCommitFailures = 0;
    int numRollbacks = 0;

    // see how many commit successes
    String spec = "* CachePerfStats cachePerfStats txCommits "
                + "filter=none combine=combineAcrossArchives ops=max trimspec=untrimmed";
    List psvs = PerfStatMgr.getInstance().readStatistics( spec );
    if ( psvs == null ) {
      log().info( "There was no statistic for txCommits" );
    } else {
      PerfStatValue psv = (PerfStatValue) psvs.get(0);
      numCommitSuccesses = (int) psv.getMax();
    }

    // see how many commit failures
    spec = "* CachePerfStats cachePerfStats txFailures "
           + "filter=none combine=combineAcrossArchives ops=max trimspec=untrimmed";
    psvs = PerfStatMgr.getInstance().readStatistics( spec );
    if ( psvs == null ) {
      log().info( "There was no statistic for txFailures" );
    } else {
      PerfStatValue psv = (PerfStatValue) psvs.get(0);
      numCommitFailures = (int) psv.getMax();
    }

    // see how many rollbacks
    spec = "* CachePerfStats cachePerfStats txRollbacks "
           + "filter=none combine=combineAcrossArchives ops=max trimspec=untrimmed";
    psvs = PerfStatMgr.getInstance().readStatistics( spec );
    if ( psvs == null ) {
      log().info( "There was no statistic for txRollbacks" );
    } else {
      PerfStatValue psv = (PerfStatValue) psvs.get(0);
      numRollbacks = (int) psv.getMax();
    }

    // taken together, numCommitSuccesses, numCommitFailures and numRollbacks
    // are the total number of transactions.
    int totalTx = numCommitSuccesses + numCommitFailures + numRollbacks;
    String aStr = "Number of commit successes: " + numCommitSuccesses + ", " +
                  "Number of commit failures: " + numCommitFailures + ", " +
                  "Number of rollbacks: " + numRollbacks;
    Log.getLogWriter().info(aStr);
    if (totalTx <= 0)
       throw new TestException("Expected transactions, but none occurred; " + aStr);
  }

  private static LogWriter log() {
    return Log.getLogWriter();
  }
  
  /** Retrieve a stat from the archive, for example this is useful for getting min 
   *  or max stat values.
   *  
   *  Examples:
   *    getStatFromArchive("CachePerfStats", "*", "regions", StatSpecTokens.MAX);
   *    getStatFromArchive("OffHeapMemoryStats", "*", "objects", StatSpecTokens.MIN);
   *  
   * @param archiveID The identifying name for the archive (currently you cannot specify
   *        a specific archive file); this ID is typically the hydra client name.
   * @param statType A stat "Type" from vsd
   * @param statName A stat "Name" from vsd
   * @param individStatName The name of the stat to be obtained from statType.statName
   * @param statKind A value from StatSpecTokens, such as StatSpecTokens.MIN or StatExpecTokens.MAX
   * @return A List of PerfStatValue instances obtained from the stat archive of the current
   *         jvm, using a stat spec formed from the previous arguments.
   *         
   */
  public static List<PerfStatValue> getStatFromArchive(String archiveID, String statType, String statName, String individStatName, String statKind) {
    String statId = getStatId(statType, statName, individStatName);
    String clientName = System.getProperty(ClientPrms.CLIENT_NAME_PROPERTY);
    ClientDescription cd = TestConfig.getInstance().getClientDescription(clientName);
    //String archive = cd.getGemFireDescription().getName() + "*";

    String spec = archiveID + 
                  " " + statType +
                  " " + statName +
                  " " + individStatName +
                  " " + StatSpecTokens.FILTER_TYPE + "=" + StatSpecTokens.FILTER_NONE +
                  " " + StatSpecTokens.COMBINE_TYPE + "=" + StatSpecTokens.RAW +
                  " " + StatSpecTokens.OP_TYPES + "=" + statKind;
    Log.getLogWriter().info("Stat spec: " + spec );
    List<PerfStatValue> statList = PerfStatMgr.getInstance().readStatistics(spec);
    Log.getLogWriter().info("Stat list for " + statId + ": " + statList);
    return statList;
  }
  
  /** Return a String which identifies a stat
   * 
   * @param statType A stat "Type" from vsd
   * @param statName A stat "Name" from vsd
   * @param individStatName The name of the stat to be obtained from statType.statName
   * @return A String describing the stat specified with the previous arguments.
   */
  private static String getStatId(String statType, String statName, String individStatName) {
    return statType + "." + statName + "." + individStatName;
  }

  /** Check that the given stat is not negative. Throws TestException if it is.
   * 
   * @param archiveID The identifying name for the archive (currently you cannot specify
   *        a specific archive file); this ID is typically the hydra client name.
   * @param statType A stat "Type" from vsd
   * @param statName A stat "Name" from vsd
   * @param individStatName The name of the stat to be obtained from statType.statName
   * @param statKind A value from StatSpecTokens, such as StatSpecTokens.MIN or StatExpecTokens.MAX
   */
  public static void ensureStatIsNonNegative(String archiveID, String statType, String statName, String individStatName, String statKind) {
    List<PerfStatValue> aList = getStatFromArchive(archiveID, statType, statName, individStatName, statKind);
    String statID = getStatId(statType, statName, individStatName);
    if (aList != null) {
      for (PerfStatValue psv: aList) {
        Log.getLogWriter().info("Retrieved " + psv);
        double value = 0;
        if (statKind.equals(StatSpecTokens.MIN)) {
          value = psv.getMin();
        } else if (statKind.equals(StatSpecTokens.MAX)) {
          value = psv.getMax();
        } else {
          throw new TestException("Test problem: unable to process statKind " + statKind);
        }
        if (value < 0) {
          throw new TestException("Expected " + statID + " " + statKind + " to be >= 0, but it is " + value + ", stat retrieved from archive: " + psv);
        }
      }
    }
  }
  
  /** Validate stats in each archive in the current test run. This work is done only once in multiple threads/jvms.
   *  If this needs to execute more then onece, then StatTasksBB.leader should be reset to 0 first.
   *  
   *  This is suitable for invoking in and ENDTASK. It will be done once and check all stats for all members
   *  that ran throughout the test.
   * 
   */
  public static void validateStats() {
    long leader = StatTasksBB.getBB().getSharedCounters().incrementAndRead(StatTasksBB.leader);
    if (leader == 1) {
      // Create a set of archive id
      String currDirPath = System.getProperty("user.dir");
      File currDir = new File(currDirPath);
      File[] contents = currDir.listFiles();
      Set<String> archiveIdSet = new HashSet<String>();
      for (File aFile: contents) {
        if (aFile.isDirectory()) {
          String[] subDirContents = aFile.list();
          List<String> aList = Arrays.asList(subDirContents);
          if (aList.toString().contains(".gfs")) { // this directory has a stat file
            String dirName = aFile.getName();
            int index = dirName.indexOf("_");
            if (index < 0) {
              throw new TestException("Unable to get archive base name from " + dirName);
            }
            String archiveID = dirName.substring(0, index);
            archiveIdSet.add(archiveID);
          }
        }
      }
      
      // validate the stats for each archiveID
      for (String archiveID: archiveIdSet) {
        ensureStatIsNonNegative(archiveID, "MemLRUStatistics", "*", "byteCount", StatSpecTokens.MIN);
        ensureStatIsNonNegative(archiveID, "LRUStatistics", "*", "entryCount", StatSpecTokens.MIN);
      }
    }
  }
}
