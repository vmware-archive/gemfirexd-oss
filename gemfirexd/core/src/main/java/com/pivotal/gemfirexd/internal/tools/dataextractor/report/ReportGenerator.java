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
package com.pivotal.gemfirexd.internal.tools.dataextractor.report;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.pivotal.gemfirexd.internal.tools.dataextractor.comparators.RegionViewGroupSortComparator;
import com.pivotal.gemfirexd.internal.tools.dataextractor.comparators.RegionViewSortComparator;
import com.pivotal.gemfirexd.internal.tools.dataextractor.comparators.ServerNameComparator;
import com.pivotal.gemfirexd.internal.tools.dataextractor.comparators.TimestampSortComparator;
import com.pivotal.gemfirexd.internal.tools.dataextractor.domain.ServerInfo;
import com.pivotal.gemfirexd.internal.tools.dataextractor.report.views.PersistentView;
import com.pivotal.gemfirexd.internal.tools.dataextractor.snapshot.GFXDSnapshotExportStat;
import com.pivotal.gemfirexd.internal.tools.dataextractor.utils.ExtractorUtils;
/*****
 * Post extraction this class is used to generate  report summary and recommendations.
 * @author jhuynh
 * @author bansods
 *
 */
public class ReportGenerator {
  
  private static String LINE_SEPARATOR = System.getProperty("line.separator");
  private static String SECTION_SEPARATOR = LINE_SEPARATOR;
  Map<String, List<GFXDSnapshotExportStat>> hostToStatsMap;
  Map<String, List<GFXDSnapshotExportStat>> hostToQueueStatsMap;
  
  private List<String> reportableErrors;
  private String recommendedDDLFile;
  List<List<GFXDSnapshotExportStat>> rankedAndGroupedDdlStats;
  
  public void setRankedAndGroupDdlsStat(List<List<GFXDSnapshotExportStat>> rankedAndGroupdDDlStats) {
    this.rankedAndGroupedDdlStats = rankedAndGroupdDDlStats;
  }

  public ReportGenerator(List<List<GFXDSnapshotExportStat>> rankedAndGroupedDdlStats, Map<String, List<GFXDSnapshotExportStat>> hostToStatsMap, List<String> reportableErrors) {
    this.rankedAndGroupedDdlStats = rankedAndGroupedDdlStats;
    this.hostToStatsMap = hostToStatsMap;
    this.reportableErrors = reportableErrors;
    this.recommendedDDLFile = rankedAndGroupedDdlStats.get(0).get(0).getFileName();
    this.hostToQueueStatsMap = new HashMap<String, List<GFXDSnapshotExportStat>>();
  }
  
  public void filterQueueStats() {
    if (!hostToStatsMap.isEmpty()) {
      Iterator<Entry<String, List<GFXDSnapshotExportStat>>> iter = hostToStatsMap.entrySet().iterator();
      
      while (iter.hasNext()) {
        Entry<String, List<GFXDSnapshotExportStat>> entry = iter.next();
        String serverName = entry.getKey();
        
        List<GFXDSnapshotExportStat> stats = entry.getValue();
        List<GFXDSnapshotExportStat> qstats = new ArrayList<GFXDSnapshotExportStat>();
        
        Iterator<GFXDSnapshotExportStat> statsIterator = stats.iterator();
        
        while (statsIterator.hasNext()) {
          GFXDSnapshotExportStat stat =  statsIterator.next();
          if (stat.isQueueExport()) {
            qstats.add(stat);
            statsIterator.remove();
          }
        }
        
        if (!qstats.isEmpty()) {
          hostToQueueStatsMap.put(serverName, qstats);
        }
      }
    } 
  }
  
  public void printReport (String outputFileName, String recommendedFileName) throws IOException {
    PersistentView pv = PersistentView.getPersistentView(hostToStatsMap);
    File file = new File(outputFileName);
    if (!file.exists()) {
      if (!file.createNewFile()) {
        throw new IOException("Could not create file:" + file.getAbsolutePath());
      }
    }
   
    File recommendedFile = new File(recommendedFileName);
    if (!recommendedFile.exists()) {
      if (!recommendedFile.createNewFile()) {
        throw new IOException("Could not create file:" + recommendedFile.getAbsolutePath());
      }
    }
    PrintWriter pw = new PrintWriter(file);
    PrintWriter recommendedPW = new PrintWriter(recommendedFile);
    try {
      printReportableErrors(pw, reportableErrors);
      printExportDdlSummary(pw);
      printExportHdfsQueueSummary(pw);
      Map<String, StatGroup> schemaTableToStatGroup = schemaTableNameToStatGroup(hostToStatsMap);//analyzeHostToStatsMap(hostToStatsMap, new TextFileCloseEnoughMatchComparator());
      sortCSV(pw, schemaTableToStatGroup, new RegionViewGroupSortComparator(new RegionViewSortComparator(new TimestampSortComparator(new ServerNameComparator()))));
      printCSVSummary(pw, schemaTableToStatGroup);
      printRecommended(recommendedPW, recommendedDDLFile, schemaTableToStatGroup);
    } finally {
      recommendedPW.close();
      pw.close();
    }
  }

  
  /*****
   * The following are static methods for the DDL , they should be in a  different Utils class
   */
  
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
    //final SequenceIdComparator seqIdComparator = new SequenceIdComparator(rvSortComparator);
    
    Collections.sort(ddlStats, rvSortComparator);
    
    return ExtractorUtils.groupByContent(ddlStats);
  }
  
  private void printReportableErrors(PrintWriter writer, List<String> reportableErrors) throws IOException {
    if (reportableErrors.size() > 0) {
      writer.write(LINE_SEPARATOR);
      writer.write("[ERRORS AND/OR CONCERNS]" + LINE_SEPARATOR);
      Iterator<String> reportableErrorsIterator = reportableErrors.iterator();
      while (reportableErrorsIterator.hasNext()) {
        writer.write(reportableErrorsIterator.next() + LINE_SEPARATOR);
      }
      writer.write(LINE_SEPARATOR);
      writer.println(SECTION_SEPARATOR);
    }
  }
  
  public void printExportDdlSummary(PrintWriter writer)  throws IOException {
    if (this.rankedAndGroupedDdlStats == null || this.rankedAndGroupedDdlStats.isEmpty()) {
      writer.write("No DDL's to export");
      return;
    }
    writer.write("[DDL EXPORT INFORMATION]");
    writer.println();
    //writer.println("The exported ddl file with the largest max sequence id is usually the most current in the distributed system.");
    writer.println();
    
    Iterator<List<GFXDSnapshotExportStat>> rankedDDLIterator = rankedAndGroupedDdlStats.iterator();
    int count = 0;
    while (rankedDDLIterator.hasNext()) {
      List<GFXDSnapshotExportStat> ddlStatGroup = rankedDDLIterator.next();
      for (GFXDSnapshotExportStat stat : ddlStatGroup) {
        writer.println("  " + ++count + ". " + stat.getServerName() + " , file : " + stat.getFileName() + " " + "Number of ddl statements : " + stat.getNumDdlStmts());
      }
    }
    writer.println(SECTION_SEPARATOR);
  }
  
  public void printExportHdfsQueueSummary(PrintWriter writer) throws IOException {
    filterQueueStats();
    
    if (!this.hostToQueueStatsMap.isEmpty()) {
      writer.write("[HDFS QUEUE EXPORT INFORMATION]");
      writer.println();
      
      Iterator<Entry<String, List<GFXDSnapshotExportStat>>> iter = hostToQueueStatsMap.entrySet().iterator();
      
      while (iter.hasNext()) {
        Entry<String, List<GFXDSnapshotExportStat>> entry = iter.next();
        String serverName = entry.getKey();
        List<GFXDSnapshotExportStat> qStats = entry.getValue();
        int count = 0;
        writer.println("Server : " + serverName);
        for (GFXDSnapshotExportStat stat : qStats) {
          writer.println("  " + ++count + ". " + " file : " + stat.getFileName() + " " + "Number of events : " + stat.getNumValuesDecoded());
        }
        writer.println();
      }
    }
  }

  private void sortCSV(PrintWriter writer, Map<String, StatGroup> schemaTableNameToStatGroup, Comparator<GFXDSnapshotExportStat> fileSortComparator) {
    Collection<StatGroup> statGroups = schemaTableNameToStatGroup.values();

    //print out the results
    Iterator<StatGroup> statGroupIterator = statGroups.iterator();
    while (statGroupIterator.hasNext()) {
      StatGroup statGroup = statGroupIterator.next();
      statGroup.sortTally(fileSortComparator);
    } 
  }

  private void printCSVSummary(PrintWriter writer, Map<String, StatGroup> schemaTableNameToStatGroup) {
    writer.write("[EXPORT INFORMATION FOR TABLES]");
    writer.println();
    Collection<StatGroup> statGroups = schemaTableNameToStatGroup.values();

    //print out the results
    Iterator<StatGroup> statGroupIterator = statGroups.iterator();
    while (statGroupIterator.hasNext()) {
      StatGroup statGroup = statGroupIterator.next();
      statGroup.printTally(writer);
    } 
    writer.println(SECTION_SEPARATOR);
  }

  private void printRecommended(PrintWriter writer, String ddlFileName, Map<String, StatGroup> schemaTableNameToStatGroup) {
    writer.write(ddlFileName + LINE_SEPARATOR);
    Collection<StatGroup> statGroups = schemaTableNameToStatGroup.values();

    //print out the results
    Iterator<StatGroup> statGroupIterator = statGroups.iterator();
    while (statGroupIterator.hasNext()) {
      StatGroup statGroup = statGroupIterator.next();
      statGroup.printRecommended(writer);
    } 
  }

  //Groups stat objects from all nodes by schema/table name
  private Map<String, StatGroup> schemaTableNameToStatGroup(Map<String, List<GFXDSnapshotExportStat>> map) {
    Map<String, StatGroup> schemaTableNameToStatGroup = new HashMap<String, StatGroup>();
    //group each stat into a group based on schema_table name
    Iterator<Entry<String, List<GFXDSnapshotExportStat>>> entrySetIterator = map.entrySet().iterator();
    while (entrySetIterator.hasNext()) {
      Entry<String, List<GFXDSnapshotExportStat>> entrySet = entrySetIterator.next();
      List<GFXDSnapshotExportStat> listOfStatsForServer = entrySet.getValue();
      organizeListOfStatsToStatGroup(schemaTableNameToStatGroup, listOfStatsForServer);
    }
    return schemaTableNameToStatGroup;
  }

  private void organizeListOfStatsToStatGroup (Map<String, StatGroup> schemaTableNameToStatGroup, List< GFXDSnapshotExportStat> listOfStats) {
    for (GFXDSnapshotExportStat stat: listOfStats) {
      String schemaAndTableName = stat.getSchemaTableName();
      StatGroup statGroup = schemaTableNameToStatGroup.get(schemaAndTableName);
      if (statGroup == null) {
        statGroup = new StatGroup(schemaAndTableName);
        schemaTableNameToStatGroup.put(schemaAndTableName, statGroup);
      }
      statGroup.addStat(stat);
    }
  }
  
  public static class StatGroup {
    private String schemaAndTableName; //also includes bucketName
    private ArrayList<GFXDSnapshotExportStat> statsOfRelatedTableFiles = new ArrayList<GFXDSnapshotExportStat>();
    private Map<GFXDSnapshotExportStat, List<GFXDSnapshotExportStat>> tally = new HashMap<GFXDSnapshotExportStat, List<GFXDSnapshotExportStat>>(); //full path, List of same files
    private List<GFXDSnapshotExportStat> sortedTally;

    private StatGroup(String schemaAndTableName) {
      this.schemaAndTableName = schemaAndTableName;
    }

    public String getSchemaAndTableName() {
      return schemaAndTableName;
    }

    public void addStat(GFXDSnapshotExportStat stat) {
      statsOfRelatedTableFiles.add(stat);
    }

    public Map<GFXDSnapshotExportStat, List<GFXDSnapshotExportStat>> getTalliedStats() {
      return tally;
    }

    public void printTally(PrintWriter writer) {
      writer.write("Table:" + schemaAndTableName + LINE_SEPARATOR);
      //write down info about each combined stat file.  
      //If a stat file is considered the same as others, we can write out the other file locations that are considered the same
      //Iterator<Map.Entry<GFXDSnapshotExportStat, List<GFXDSnapshotExportStat>>> iterator = tally.entrySet().iterator();
      List<GFXDSnapshotExportStat> sortedTally = getSortedTally();
      Iterator<GFXDSnapshotExportStat> sortedIterator = sortedTally.iterator();
      int index = 1;
      while (sortedIterator.hasNext()) {
        GFXDSnapshotExportStat stat = sortedIterator.next();
        writer.write("  " + index++ + ". " + stat.getFileName() + " . Number of rows extracted : " + stat.getNumValuesDecoded());

        writer.write(LINE_SEPARATOR);
      }
      writer.write(LINE_SEPARATOR);
    }

    public void printRecommended(PrintWriter writer) {
      List<GFXDSnapshotExportStat> sortedTally = getSortedTally();
      Iterator<GFXDSnapshotExportStat> sortedIterator = sortedTally.iterator();
      if (sortedIterator.hasNext()) {
        GFXDSnapshotExportStat stat = sortedIterator.next();
        writer.write(stat.getFileName() + LINE_SEPARATOR);
      }
    }

    public List<GFXDSnapshotExportStat> getSortedTally() {
      return sortedTally;
    }

    public List<GFXDSnapshotExportStat> sortTally(
        Comparator<GFXDSnapshotExportStat> fileSortComparator) {
      // sort the tally
      sortedTally = new ArrayList<GFXDSnapshotExportStat>();
      Iterator<GFXDSnapshotExportStat> iterator = statsOfRelatedTableFiles.iterator();
      while (iterator.hasNext()) {
        sortedTally.add(iterator.next());
      }
      Collections.sort(sortedTally, fileSortComparator);
      return sortedTally;
    }

  }
}
