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
package s2qa;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.TreeMap;
import java.util.TreeSet;

public class GFXDReport {

  // IMPROVE_ME: don't hard-code the junit output file extension
  static final String JUNIT_OUT_FILE_EXT = ".out";
  static final String JUNIT_XML_FILE_EXT = ".xml";
  static final String JUNIT_XML_FILE_PFX = "TEST-";
  static final String KF_DIR_PROP = "gfxdKFDir";
  static final String KF_DIR_PROP_HELP = "directory containing known failure (kf) files";
  static final String KF_CSVFILE_PROP = "gfxdKFCSVFile";
  static final String KF_CSVFILE_PROP_HELP = "Comma-separated value index file of gemfirexd known failures (e.g. /some/path/knownfailures.csv)";
  static final String TRAC_BUG_FILE_PROP = "gfxdBugFile";
  static final String TRAC_BUG_FILE_PROP_HELP = "CSV File containing gemfirexd TRAC tickets (e.g. /some/path/report_118.csv)";
  static final String REPORT_DIR_PROP = "gfxdReportDir";
  static final String REPORT_DIR_PROP_HELP = "Directory where this app will create report files";
  static final String JUNIT_OUT_PROP = "junitOutDir";
  static final String JUNIT_OUT_PROP_HELP = "Directory where output from the junit test executions lives...";
  static final String KF_FILE_DEFAULT = "knownFailures.csv";
  static final String TRAC_BUG_FILE_DEFAULT = "report_118.csv";
  static final String SUMMARY_CSV_PROP = "summaryFile";
  static final String SUMMARY_CSV_PROP_HELP = "CSV File name for overall test pass/fail summary";
  static final String SUMMARY_CSV_DEFAULT = "summary.csv";
  static final String UNKNOWN_FAILURES_REPORT_FILE = "unknownFailures.txt";
  static final String TICKET_URL="https://svn.gemstone.com/trac/gemfire/ticket/";
  static final String KF_DIR_DEFAULT = "./knownFailures";
  static final String REPORT_DIR_DEFAULT = ".";
  static final String[] CMD_LINE_OPTS = { KF_DIR_PROP, KF_CSVFILE_PROP,
      TRAC_BUG_FILE_PROP, REPORT_DIR_PROP, JUNIT_OUT_PROP, SUMMARY_CSV_PROP };
  static final String[] CMD_LINE_HELP = { KF_DIR_PROP_HELP,
      KF_CSVFILE_PROP_HELP, TRAC_BUG_FILE_PROP_HELP, REPORT_DIR_PROP_HELP,
      JUNIT_OUT_PROP_HELP, SUMMARY_CSV_PROP_HELP };
  static final String[] CMD_LINE_DEFAULTS_HELP = { KF_DIR_DEFAULT, "<"+KF_DIR_PROP+">/"+KF_FILE_DEFAULT,
    "<"+KF_DIR_PROP+">/"+TRAC_BUG_FILE_DEFAULT, REPORT_DIR_DEFAULT, null,
    "<"+REPORT_DIR_PROP+">/"+SUMMARY_CSV_DEFAULT };

  private String gfxdKnownFailureDirRoot = System.getProperty(KF_DIR_PROP,
      KF_DIR_DEFAULT);
  private String gfxdKnownFailureFileName = System.getProperty(KF_CSVFILE_PROP,
      gfxdKnownFailureDirRoot + "/" + KF_FILE_DEFAULT);
  private String gfxdBugSnapshotFileName = System.getProperty(
      TRAC_BUG_FILE_PROP, gfxdKnownFailureDirRoot+"/"+TRAC_BUG_FILE_DEFAULT);
  //private SimpleDateFormat reportFileDateFormat = new SimpleDateFormat(
  //    "yy-MMM-dd-hh-mm");
  //private String reportFileDate = reportFileDateFormat.format(new Date());
  private String gfxdReportDir = System.getProperty(REPORT_DIR_PROP, REPORT_DIR_DEFAULT);
  private String junitOutDir = System.getProperty(JUNIT_OUT_PROP);
  private String summaryCSVPath = System.getProperty(SUMMARY_CSV_PROP,
      gfxdReportDir + "/" + SUMMARY_CSV_DEFAULT);
  private GFXDBugs gfxdBugs = null;
  private GFXDKnownFailures gfxdKnownFailures;

  public GFXDReport() {

  }

  public String getGfxdKnownFailureDirRoot() {
    return gfxdKnownFailureDirRoot;
  }

  public void setGfxdKnownFailureDirRoot(String gfxdKnownFailureDirRoot) {
    this.gfxdKnownFailureDirRoot = gfxdKnownFailureDirRoot;
  }

  public String getGfxdKnownFailureFileName() {
    return gfxdKnownFailureFileName;
  }

  public void setGfxdKnownFailureFileName(String gfxdKnownFailureFileName) {
    this.gfxdKnownFailureFileName = gfxdKnownFailureFileName;
  }

  public String getGfxdBugSnapshotFileName() {
    return gfxdBugSnapshotFileName;
  }

  public void setGfxdBugSnapshotFileName(String gfxdBugSnapshotFileName) {
    this.gfxdBugSnapshotFileName = gfxdBugSnapshotFileName;
  }

  public String getGfxdReportDir() {
    return gfxdReportDir;
  }

  public void setGfxdReportDir(String gfxdReportDir) {
    this.gfxdReportDir = gfxdReportDir;
  }

  public String getJunitOutDir() {
    return junitOutDir;
  }

  public void setJunitOutDir(String junitOutArg) {
    this.junitOutDir = junitOutArg;
  }

  public String getSummaryCSVPath() {
    return summaryCSVPath;
  }

  public void setSummaryCSVPath(String summaryCSVPath) {
    this.summaryCSVPath = summaryCSVPath;
  }

  public ArrayList<JUnitResultData> gfxdJUnitReport(String path)
      throws IOException {
    ArrayList<JUnitResultData> retData = new ArrayList<JUnitResultData>();
    File f = new File(path);
    if (f.isDirectory()) {
      ArrayList<JUnitResultData> summaryOfFiles = gfxdJUnitReportOnDir(path);
      retData.addAll(summaryOfFiles);
    } else {
      JUnitResultData summaryOfFile = gfxdJUnitReportOnFile(path);
      retData.add(summaryOfFile);
    }
    return retData;
  }

  public JUnitResultData gfxdJUnitReportOnFile(String junitOutFile)
      throws IOException {
    File f = new File(junitOutFile);
    JUnitResultData resultsFromFile = null;
    if (junitOutFile.endsWith(".out")) {
      JUnitOutParser outParser = new JUnitOutParser(f.getAbsolutePath());
      resultsFromFile = outParser.getErrorsAndFailures();
    } else if (junitOutFile.endsWith(".xml")) {
      JUnitXMLParser xmlParser = new JUnitXMLParser();
      resultsFromFile = xmlParser.parseXmlFile(f);
    }
    ArrayList<JUnitResult> junitFailuresFromOutFile = resultsFromFile.getFailures();
    int knownFailureCount = 0;
    int testIssueCount = 0;
    for (JUnitResult junitFailure : junitFailuresFromOutFile) {
      // See if this junit failure is a known failure
      GFXDKnownFailure kf = gfxdKnownFailures.matchKnownFailure(junitFailure,
          junitFailure.getScopedTestcaseName());
      if (kf != null) {
        junitFailure.setAssociatedKnownFailure(kf);
        knownFailureCount++;
        if (kf.getKnownFailureType().equals(GFXDKnownFailure.KF_TEST_ISSUE)) {
          testIssueCount++;
        }
      } else {
        // This is an unknown failure
        //juOutData.getUnknownFailures().add(junitFailure);
        String trimmedClass = junitFailure.getTestclassName().replaceFirst(
            "org.apache.derbyTesting.functionTests.tests.", "");
        System.out.println("DEBUG: " + trimmedClass + " "
            + junitFailure.getTestcaseName() + " is an unknown failure");
      }
    }
    resultsFromFile.setTestIssueCount(testIssueCount);
    resultsFromFile.setKnownFailureCount(knownFailureCount);
    return resultsFromFile;

  }

  public ArrayList<JUnitResultData> gfxdJUnitReportOnDir(
      String dirContainingJUnitOutFiles) throws IOException {
    ArrayList<JUnitResultData> retFileSummaries = new ArrayList<JUnitResultData>();
    File junitOutDir = new File(dirContainingJUnitOutFiles);
    String[] files = junitOutDir.list();
    TreeSet<String> sortedFiles = new TreeSet<String>(Arrays.asList(files));
    for (String fName : sortedFiles) {
      // Might want to allow recursion here
      File f = new File(dirContainingJUnitOutFiles + "/" + fName);
      // filter for junit
      if (fName.endsWith(JUNIT_OUT_FILE_EXT)) {
        JUnitResultData fileSummary = gfxdJUnitReportOnFile(f.getAbsolutePath());
        retFileSummaries.add(fileSummary);
      }
      if (fName.startsWith(JUNIT_XML_FILE_PFX) && fName.endsWith(JUNIT_XML_FILE_EXT)) {
        JUnitResultData fileSummary = gfxdJUnitReportOnFile(f.getAbsolutePath());
        retFileSummaries.add(fileSummary);
      }
    }
    return retFileSummaries;
  }

  private TreeMap<String, ArrayList<FailureReportingRow>> flattenReportingData(ArrayList<JUnitResultData> junitOutFileSummaries) {
    TreeMap<String, ArrayList<FailureReportingRow>> reportingRowsAllFiles =
      new TreeMap<String, ArrayList<FailureReportingRow>>();
    for (JUnitResultData fileSummary : junitOutFileSummaries) {
      if (fileSummary.getFailures().size() > 0) {
        ArrayList<FailureReportingRow> reportingRowsForFile = new ArrayList<FailureReportingRow>();
        for (JUnitResult testFailure : fileSummary.getFailures()) {
          String bugNum = " ";
          String bugState = " ";
          String bugPriority = " ";
          String failureDescription = "unknown problem needs research";
          String testDescription = " ";
          GFXDKnownFailure kf = testFailure.getAssociatedKnownFailure();
          if (kf != null) {
            if (kf.getBug() == null) {
              System.err
                  .println("DATA_ERROR: bug snapshot file needs a refresh to include "
                      + kf.getBug() + "?");
            } else {
              bugNum = kf.getBug();
              GFXDBug bugData = gfxdBugs.getBug(bugNum);
              if (bugData != null) {
                bugState = bugData.getStatus();
                bugPriority = bugData.getPriority();
                failureDescription = bugData.getSummary();
              }
            }
            // Note: if the known failure data provides a failure description,
            //       it supercedes the bug summary string
            if (kf.getFailureDescription() != null
                && kf.getFailureDescription().length() > 0) {
              failureDescription = kf.getFailureDescription();
            }
            if (kf.getTestDescription() != null) {
              testDescription = kf.getTestDescription();
            }
          }
          reportingRowsForFile.add(new FailureReportingRow(testFailure
              .getScopedTestcaseName(), bugNum, bugState, bugPriority,
              failureDescription));

        }
        reportingRowsAllFiles.put(fileSummary.getJunitResultfile().getName(),
            reportingRowsForFile);
      }
    }
    return reportingRowsAllFiles;
  }


  public void makeReports(String junitOutArg) throws Exception {
    gfxdKnownFailures = new GFXDKnownFailures(
        new File(gfxdKnownFailureFileName), gfxdKnownFailureDirRoot);
    if (gfxdBugs == null) {
      gfxdBugs = new GFXDBugs();
      gfxdBugs.loadFromFile(new File(gfxdBugSnapshotFileName));
    }

    // Convert all the junit output files into an array of parsed-out
    //   structures.
    ArrayList<JUnitResultData> junitOutFileSummaries = gfxdJUnitReport(junitOutArg);

    // Flatten out the merge of the test failures + known problems + bugData
    // so the reporting methods can be simplified.
    TreeMap<String, ArrayList<FailureReportingRow>> reportingRowsAllFiles =
      flattenReportingData(junitOutFileSummaries);

    // Generate comma-separated-value reports
    makeCSVSummaryReport(junitOutFileSummaries);
    makeCSVFailureReports(reportingRowsAllFiles);
    // Generate a text file of the unknown failures
    makeUnknownFailuresReport(junitOutFileSummaries);
    // Generate an html report
    makeHTMLReport(junitOutFileSummaries, reportingRowsAllFiles);
    System.out.println("ALL DONE GFXDReports... output to " + gfxdReportDir);

  }

  public void makeUnknownFailuresReport(
      ArrayList<JUnitResultData> junitOutFileSummaries) throws Exception {
    PrintWriter ukfPW = new PrintWriter(new BufferedWriter(new FileWriter(
        getGfxdReportDir() + "/" + UNKNOWN_FAILURES_REPORT_FILE)));
    ukfPW.println("UNKNOWN FAILURES");
    for (JUnitResultData fileSummary : junitOutFileSummaries) {
      for (JUnitResult testFailure : fileSummary.getFailures()) {
        if (testFailure.getAssociatedKnownFailure() == null) {
          ukfPW.println("From file "
              + fileSummary.getJunitResultfile().getAbsolutePath());
          ukfPW.println(testFailure.getOriginalIndexNumber() + ") "
              + testFailure.getTestclassName() + " "
              + testFailure.getTestcaseName());
          ukfPW.println(testFailure.getTraceback());
          ukfPW.println();
        }
      }
    }

      /*
      if (fileSummary.getUnknownFailures() != null
          && fileSummary.getUnknownFailures().size() > 0) {
        ukfPW.println("From file "
            + fileSummary.getJunitOutfile().getAbsolutePath());
        for (JUnitResult unknownFailure : fileSummary.getUnknownFailures()) {
          ukfPW.println(unknownFailure.getOriginalIndexNumber() + ") "
              + unknownFailure.getTestclassName() + " "
              + unknownFailure.getTestcaseName());
          ukfPW.println(unknownFailure.getTraceback());
          ukfPW.println();
        }
      }
    }
        */
    ukfPW.close();
  }

  private String htmlBoldStatus(String bugStatus) {
    if (bugStatus.equals("verifying") ||
        bugStatus.equals("closed")) {
      return "<b>"+bugStatus+"<b>";
    } else {
      return bugStatus;
    }
  }

  public String urlifyTicket(String ticketNum) {
    if (ticketNum == null || ticketNum.trim().length() == 0) {
      return ticketNum;
    } else {
      return "<a href=\""+TICKET_URL+ticketNum+"\">"+ticketNum+"</a>";
    }
  }

  public TreeMap<String, Integer> getWorstBugs(ArrayList<JUnitResultData> junitOutFileSummaries, int minimumInstances) {
    // Build a map of the bug to its frequency
    TreeMap<String, Integer> bugToFailCount = new TreeMap<String, Integer>();
    for (JUnitResultData fileSummary : junitOutFileSummaries) {
      for (JUnitResult testFailure : fileSummary.getFailures()) {
        if (testFailure.getAssociatedKnownFailure() != null &&
            testFailure.getAssociatedKnownFailure().getBug() != null) {
          String bugNumber = testFailure.getAssociatedKnownFailure().getBug();
          if (bugNumber.trim().length() > 0) {
            Integer priorCount = bugToFailCount.get(bugNumber) == null ? 0: bugToFailCount.get(bugNumber);
            bugToFailCount.put(bugNumber, priorCount+1);
          }
        }
      }
    }
    // Filter the bugmap by minimum count
    TreeMap<String, Integer> returnMap = new TreeMap<String, Integer>();
    for (String bugNum : bugToFailCount.keySet()) {
      if (bugToFailCount.get(bugNum) >= minimumInstances) {
        returnMap.put(bugNum, bugToFailCount.get(bugNum));
      }
    }
    return returnMap;
  }

  public void makeHTMLReport(ArrayList<JUnitResultData> junitOutFileSummaries,
      TreeMap<String,ArrayList<FailureReportingRow>> allFailuresMap)
      throws Exception {
    String htmlBaseFileName = new File(junitOutDir).getName();
    String htmlFileName = getGfxdReportDir() + "/" + htmlBaseFileName+"derbyTestsReport.html";
    PrintWriter htmlPW = new PrintWriter(new BufferedWriter(new FileWriter(
        htmlFileName)));
    htmlPW.println("<html><title>GemFireXD Derby test results</title>");
    htmlPW.println("<br>Report on "+junitOutDir+"<br>");
    String changenumber = System.getProperty("changenumber");
    if (changenumber != null) {
      htmlPW.println("changenumber "+changenumber+"<br>");
    } else {
      changenumber="&nbsp;";
    }
    // Start with the overall summary of all junit output files
    htmlPW.println("<br><table border=\"2\">");
    htmlPW
        .println("<tr><th>filename</th><th>svnChangeNum</th>"
            + "<th>Tests run</th><th>Fails+Errs</th><th>known failures</th><th>unknown failures</th><th>test issues</th></tr>");
    int totRuns = 0;
    int totErrFails = 0;
    int totKnownFailures = 0;
    int totTestIssues = 0;
    int totUnknownFailures = 0;
    for (JUnitResultData fileSummary : junitOutFileSummaries) {
      totRuns += fileSummary.getRunCount();
      // Note: for the purposes of a summary report, a junit error is
      //       the same as a junit failure.
      //       Can a test be both a err and a fail?  Don't want to mess up accounting...
      totErrFails += (fileSummary.getFailCount() + fileSummary.getErrorCount());
      totKnownFailures += fileSummary.getKnownFailureCount();
      totTestIssues += fileSummary.getTestIssueCount();
      String changeNumForFile = fileSummary.getSvnChangeNumber() == null ? changenumber
          : fileSummary.getSvnChangeNumber();
      int unknownFailureCount = fileSummary.getFailCount() + fileSummary.getErrorCount()
                             - fileSummary.getKnownFailureCount();
      totUnknownFailures += unknownFailureCount;
      String relPath="../"+fileSummary.getJunitResultfile().getParentFile().getName()+
                      "/"+fileSummary.getJunitResultfile().getName();
      htmlPW.println("<tr><td><a href=\"" + relPath+"\">"+
          fileSummary.getJunitResultfile().getName()+"</a>"
          + "</td><td>" + changeNumForFile + "</td><td align=\"right\">"
          + fileSummary.getRunCount() + "</td><td align=\"right\">"
          + (fileSummary.getFailCount() + fileSummary.getErrorCount())
          + "</td><td align=\"right\">" + fileSummary.getKnownFailureCount()
          + "</td><td align=\"right\">" + unknownFailureCount
          + "</td><td align=\"right\">" + fileSummary.getTestIssueCount()
          + "</td></tr>");
    }
    int passRatio = 0;
    if (totRuns > 0) passRatio = 100 * (totRuns - totErrFails) / totRuns;
    htmlPW.println("<tr><td>TOTALS</td><td>" + passRatio
        + "% passed</td><td align=\"right\">" + totRuns
        + "</td><td align=\"right\">" + totErrFails
        + "</td><td align=\"right\">" + totKnownFailures
        + "</td><td align=\"right\">" + totUnknownFailures
        + "</td><td align=\"right\">" + totTestIssues + "</td></tr>");
    htmlPW.println("</table><br>");

    // Next, list the testcase names of the unknown failures
    htmlPW.println("<table border=\"2\">");
    htmlPW.println("<caption>Unknown failures: need investigation to associate with a ticket</caption>");
    htmlPW.println("<tr><th>file</th><th>testcase</th></tr>");
    for (JUnitResultData fileSummary : junitOutFileSummaries) {
        for (JUnitResult testFailure : fileSummary.getFailures()) {
          if (testFailure.getAssociatedKnownFailure() == null) {
            htmlPW.println("<tr><td>"+fileSummary.getJunitResultfile().getName()+"</td>"+
                           "<td>"+testFailure.getScopedTestcaseName()+"</td></tr>");
          }
        }
    }
    htmlPW.println("</table>");

    // Create a bug impact table
    // Fir
    final int MINBUGCOUNT=5;
    htmlPW.println("<br><table border=\"1\"");
    htmlPW.println("<caption>Bugs affecting at least "+MINBUGCOUNT+" tests</caption>");
    htmlPW.println("<tr><th>ticket</th><th>state</th><th>priority</th><th>summary</th><th>tests affected</th></tr>");
    TreeMap<String, Integer> worstBugs =  getWorstBugs(junitOutFileSummaries,  MINBUGCOUNT);
    for (String bug : worstBugs.keySet()) {
      htmlPW.println("<tr><td>"+urlifyTicket(bug)+"</td>");
      if (gfxdBugs.getBug(bug) != null) {
        htmlPW.println("<td>"+htmlBoldStatus(gfxdBugs.getBug(bug).getStatus())+"</td>");
        htmlPW.println("<td>"+gfxdBugs.getBug(bug).getPriority()+"</td>");
        htmlPW.println("<td>"+gfxdBugs.getBug(bug).getSummary()+"</td>");
      } else {
        htmlPW.println("<td>&nbsp;</td><td>&nbsp;</td><td>&nbsp;</td>");
      }
      htmlPW.println("<td align=\"right\">"+worstBugs.get(bug)+"</td></tr>");
    }
    htmlPW.println("</table>");

    //  Now provide some details on each of the failures.
    //  We create a separate html table per junit output file.
    for (String outFileName : allFailuresMap.keySet()) {
      htmlPW.println("<br><table border=\"2\">");
      htmlPW.println("<caption>" + outFileName
          + "</caption>");
      htmlPW
          .println("<tr><th>Test</th>"
              + "<th>Bug#</th><th>Bug State</th><th>Bug Priority</th><th>Failure description</th></tr>");
      for (FailureReportingRow rptRow : allFailuresMap.get(outFileName)) {
        htmlPW.println("<tr><td>" + rptRow.getTestName()
            + "</td><td>&nbsp;" + urlifyTicket(rptRow.getBugNum()) +
            "</td><td>&nbsp;" + htmlBoldStatus(rptRow.getBugState()) + "</td><td>&nbsp;"
            + rptRow.getBugPriority() + "</td><td>&nbsp;" +
            rptRow.getFailureDescription() + "</td></tr>");

      }
      htmlPW.println("</table><br>");
    }
    htmlPW.close();
    System.out.println("HTML report written to "+htmlFileName);
  }

  public void makeCSVSummaryReport(ArrayList<JUnitResultData> junitOutFileSummaries)
      throws Exception {
    PrintWriter summaryPW = new PrintWriter(new BufferedWriter(new FileWriter(
        summaryCSVPath)));
    summaryPW
        .println("testSet,svnChangeNum,runCount,errFailCount,knownProblemCount,testIssueCount");
    int totalRuns=0;
    int totalErrFails=0;
    int totalKnownProblems=0;
    int totalTestIssues=0;
    for (JUnitResultData fileSummary : junitOutFileSummaries) {
      totalRuns += fileSummary.getRunCount();
      // Note: for the purposes of a summary report, a junit error is
      //       the same as a junit failure.
      //       Can a test be both a err and a fail?  Don't want to mess up accounting...
      totalErrFails += (fileSummary.getFailCount() + fileSummary
          .getErrorCount());
      totalKnownProblems += fileSummary.getKnownFailureCount();
      totalTestIssues += fileSummary.getTestIssueCount();
      String changeNum = fileSummary.getSvnChangeNumber() == null ? ""
          : fileSummary.getSvnChangeNumber();
      summaryPW.println(fileSummary.getJunitResultfile().getName() + ","
          + changeNum + "," + fileSummary.getRunCount() + ","
          + (fileSummary.getFailCount() + fileSummary.getErrorCount()) + ","
          + fileSummary.getKnownFailureCount() + ","
          + fileSummary.getTestIssueCount());
    }
    summaryPW.println("TOTALS,," + totalRuns + "," + totalErrFails + ","
        + totalKnownProblems + "," + totalTestIssues);
    summaryPW.close();

  }

  /*
   * Write a CSV file detailing the failures in a junit output file
   */
  public void makeCSVFailureReports(
      TreeMap<String, ArrayList<FailureReportingRow>> failureReportingMap)
      throws Exception {
    for (String outFileName : failureReportingMap.keySet()) {
      PrintWriter csvPW = new PrintWriter(new BufferedWriter(new FileWriter(
          getGfxdReportDir() + "/" + outFileName + ".csv")));
      csvPW.println("Test,Bug#,Bug State,Bug Priority,Failure description");
      for (FailureReportingRow rptRow : failureReportingMap.get(outFileName)) {
        csvPW.println(rptRow.getTestName() + "," + rptRow.getBugNum() + ","
            + rptRow.getBugState() + "," + rptRow.getBugPriority() + ","
            + rptRow.getFailureDescription());
      }
      csvPW.close();
    }
  }

  public static void usage() {
    System.out.println("GFXDReport");
    System.out
        .println("  creates a .csv file of test run status, merging in known failures and trac bug data");
    System.out
        .println("Properties that can be set on either the command line or as system properties:");
    for (int i = 0; i < CMD_LINE_OPTS.length; i++) {
      System.out.println("  --" + CMD_LINE_OPTS[i] + "=value      ");
      System.out.println("     " + CMD_LINE_HELP[i]);
      System.out.println("     default value: " + CMD_LINE_DEFAULTS_HELP[i]);
    }

  }

  private boolean parseCmdLine(String[] args) {
    boolean didSetKFDir = false;
    boolean didSetKFFile = false;
    boolean didSetTracFile = false;
    for (int i = 0; i < args.length; i++) {
      String thisArg = args[i];
      String argValue = null;
      if (args[i].indexOf("=") > 0) {
        thisArg = args[i].substring(0, args[i].indexOf("="));
        argValue = args[i].substring(args[i].indexOf("=") + 1);
      } else {
        // all our args currently require an =value
        usage();
        return false;
      }
      if (thisArg.equals("--" + JUNIT_OUT_PROP)) {
        setJunitOutDir(argValue);
      } else if (thisArg.equals("--" + KF_CSVFILE_PROP)) {
        setGfxdKnownFailureFileName(argValue);
        didSetKFFile = true;
      } else if (thisArg.equals("--" + KF_DIR_PROP)) {
        setGfxdKnownFailureDirRoot(argValue);
        didSetKFDir = true;
      } else if (thisArg.equals("--" + REPORT_DIR_PROP)) {
        setGfxdReportDir(argValue);
      } else if (thisArg.equals("--" + TRAC_BUG_FILE_PROP)) {
        setGfxdBugSnapshotFileName(argValue);
        didSetTracFile=true;
      } else if (thisArg.equals("--" + SUMMARY_CSV_PROP)) {
        setSummaryCSVPath(argValue);
      } else {
        usage();
        return false;
      }
    }
    if (didSetKFDir == true && didSetKFFile == false) {
      setGfxdKnownFailureFileName(getGfxdKnownFailureDirRoot()
          + KF_FILE_DEFAULT);
    }
    if (didSetKFDir == true && didSetTracFile == false) {
      setGfxdBugSnapshotFileName(getGfxdKnownFailureDirRoot()
          + TRAC_BUG_FILE_DEFAULT);
    }
    if (getJunitOutDir() == null || getGfxdKnownFailureFileName() == null
        || getGfxdKnownFailureDirRoot() == null || getGfxdReportDir() == null
        || getGfxdBugSnapshotFileName() == null || getSummaryCSVPath() == null) {
      usage();
      return false;
    }
    return true;
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    GFXDReport gfxdReport = new GFXDReport();
    if (!gfxdReport.parseCmdLine(args)) {
      System.exit(1);
    }
    try {
      gfxdReport.makeReports(gfxdReport.getJunitOutDir());
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

}
