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
/** Class to create a bug report template for a failed run.
 * 
 */
package resultsUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.PrintWriter;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import util.TestHelper;

/**Generate a bug report template for a given hydra run.
 * @author lynn 
 */
public class BugReportTemplate {

  // the maximum time to allow to create bug report template file; since this
  // tries to analyze log files which might be really big, we limit the
  // amount of time we spend on this and just report what we can in the
  // time allowed
  private static final long MAX_MS_TO_RUN = 300000; // 5 minutes

  // the file name to be generated
  final static String templateFileName = "bugReportTemplate.txt";

  // names of files of interest in the hydra run to be used for gathering
  // information for the bug report template
  private List<String> bgExecFileList = new ArrayList();
  private List<String> vmLogFileList = new ArrayList();
  private String taskMasterLogFileName = null;
  private String errorsDotTextFileName = null;
  private List<String> hprofFileList = new ArrayList();
  private List<String> hotspotFileList = new ArrayList();
  private String masterLogFileName = null;
  private String testPropFileName = null;
  private String latestPropFileName = null;
  private String localConfFileName = null;

  // The string containing the text of the bug report template
  private StringBuffer templateStr = new StringBuffer();

  // The directory path to a hydra run
  private String hydraRunDirPath = null;
  private boolean writeToStdOut = false;

  /**
   * Generate a bug report template with information from the run to be 
   * included in a bug report. This information is basic test run information 
   * to be included for all bugreports.
   * 
   * @param dirPath A String indicating a directory path to a hydra run.
   * @param writeToStdOut If true, then write the template to standard out,
   *        otherwise write to a file in dirPath.
   */
  public static void createTemplateFile(String dirPath, boolean toStdOut) {
    try {
      BugReportTemplate brt = new BugReportTemplate();
      brt.hydraRunDirPath = dirPath;
      brt.writeToStdOut = toStdOut;
      brt._createTemplateFile();
    } catch (Exception e) { // guard against any exceptions that might keep
      // hydra from continuing
      System.out.println(
          "Bug report template generation failed due to "
          + TestHelper.getStackTrace(e));
    }
  }

  /** Create the template file but don't allow it to run longer than
   *  MAX_MS_TO_RUN ms in case the log files are very large and creating
   *  this file takes too long.
   *  
   * @throws InterruptedException
   */
  protected void _createTemplateFile() throws InterruptedException {
    // do the work of creating the file text in a thread so it can be
    // interrupted
    // if it takes too long to complete (probably due to large log files); this
    // must not allow hydra itself to hang
    Thread createTemplateThread = new Thread(new Runnable() {
      public void run() {
        try {
          buildTemplateFileText();
        } catch (Exception e) { // guard against any exceptions that might keep
          // hydra from continuing
          System.out.println(
              "Bug report template generation failed due to "
              + TestHelper.getStackTrace(e));
        }
      }
    });
    createTemplateThread.start();
    createTemplateThread.join(MAX_MS_TO_RUN); // wait only so long
    if (createTemplateThread.getState() != Thread.State.TERMINATED) { // still
      // running
      createTemplateThread.interrupt();
      templateStr.insert(0,
          "****************************************************************\n" +
          "   Run analysis exceeded time limit of " + MAX_MS_TO_RUN + " ms\n" +
          "   and is incomplete (might be due to large log files)\n" +
      "****************************************************************\n\n");
    }
    writeTemplateFile(); // writes the entire thing, or what we were able to do
    // until interrupted
  }

  /**
   * Creates the contents of the bug report template file and stores it in
   * templateStr.
   * 
   */
  protected void buildTemplateFileText() {
    // initialize
    categorizeFiles();

    // write the header information
    templateStr.append(getHeaderInfo());

    // get the bt file this test was run from, if any
    String batteryTestName = getBatteryTestName();
    if (batteryTestName != null) {
      templateStr.append("\n\nTest was run from " + batteryTestName + "\n");
    }

    // write the test specification
    templateStr.append("\nTest:\n" + getTestSpecification());

    // write the local.conf
    try {
      FileLineReader reader = new FileLineReader(localConfFileName);
      templateStr.append("\n\nRun with local.conf:");
      String line = reader.readNextLine();
      while (line != null) {
        templateStr.append("\n" + line);
        line = reader.readNextLine();
      }
    } catch (FileNotFoundException e) { // no local.conf
      templateStr.append("\n\nNo local.conf for this run");
    }

    // write the randomSeed
    String randomSeed = getRandomSeed();
    if (randomSeed == null) {
      templateStr.append("\n\nUnable to find randomSeed\n");
    } else {
      templateStr.append("\n\n//randomSeed extracted from test:\n");
      templateStr.append(getRandomSeed());
    }

    // look for errors
    templateStr.append("\n\n" + doErrorAnalysis() + "\n");
  }

  /** Return a String containing the test specification (how it was run
   *  from a battery test .bt file)
   * 
   */
  private String getTestSpecification() {
    StringBuffer returnStr = new StringBuffer();
    String prefix = null;
    String fileName = null;
    boolean isMultiLineTestSpec = false;
    if (testPropFileName == null) { // look in the latest.prop
      if (latestPropFileName == null) {
        returnStr.append("  Unable to determine test that was run\n");
      } else {
        fileName = latestPropFileName;
        prefix = "TestName=";
        isMultiLineTestSpec = false;
      }
    } else {
      fileName = testPropFileName;
      prefix = "testName=";
      isMultiLineTestSpec = true;
    }
    try {
      FileLineReader reader = new FileLineReader(fileName);
      String line = reader.readNextLine();
      if (line != null) {
        if (line.startsWith(prefix)) {
          returnStr.append(line.substring(prefix.length(), line.length()));
        }
      }
      if (isMultiLineTestSpec) {
        line = reader.readNextLine();
        while (line != null) {
          returnStr.append("\n   " + line);
          line = reader.readNextLine();
        }
      }
    } catch (FileNotFoundException e) { // did not find file to get test name from
      returnStr.append("  Unable to determine test that was run\n");
    }
    return returnStr.toString();
  }
  
  /** Determine if this test was run as part of a BatteryTest .bt file. If so,
   *  return the name of the battery test file, otherwise return null.
   * @return The name of the battery test this test was run from, or null if
   *         that cannot be determined.
   */
  private String getBatteryTestName() {
    File runDir = new File(hydraRunDirPath);
    String parent = runDir.getParent();
    if (parent != null) {
      File parentDir = new File(parent); 
        final String batteryTestLogFileName = "batterytest.log";
        FilenameFilter filter = new FilenameFilter() {
          public boolean accept(File dir, String name) {
            return name.equals(batteryTestLogFileName);
          }
        };
        File[] logFiles = parentDir.listFiles(filter);
        if (logFiles.length > 0) {
          for (File aFile: logFiles) { // should only be one file, but loop anyway
            FileLineReader reader;
            try {
              reader = new FileLineReader(aFile);
            } catch (FileNotFoundException e) {
              return null; // could not get battery test info
            }
            String line = reader.readNextLine();
            while (line != null) {
              String searchStr = "  testFileName = ";
              if (line.startsWith(searchStr)) {
                return line.substring(searchStr.length());
              }
              line = reader.readNextLine();
            }
          }
      }
    }
    return null;
  }

  /** Get the randomSeed out of the latest.prop file. 
   * @return
   */
  private String getRandomSeed() {
    FileLineReader reader;
    try {
      reader = new FileLineReader(latestPropFileName);
    } catch (FileNotFoundException e) {
      return null;
    }
    String searchStr = "hydra.Prms-randomSeed=";
    String line = reader.readNextLine();
    while (line != null) {
      if (line.startsWith(searchStr)) {
        return line + ";";
      }
      line = reader.readNextLine();
    }
    return null;
  }

  /** Look for header information in the Master*.log file. 
   * @return A String containing information from the hydra run. 
   */
  private String getHeaderInfo() {
    // check if Master*.log file exists
    if (masterLogFileName == null) { // cannot get header information
      return "";
    }

    // initialize
    StringBuffer returnStr = new StringBuffer();
    String searchStr = "####################################################";
    int searchStrFoundCntr = 0; 
    FileLineReader reader;
    try {
      reader = new FileLineReader(masterLogFileName);
    } catch (FileNotFoundException e) {
      return "";
    }
    String line = reader.readNextLine();
    while (line != null) {
      returnStr.append(extractLine("Host name:", line));
      returnStr.append(extractLine("OS name:", line));
      returnStr.append(extractLine("Architecture:", line));
      returnStr.append(extractLine("OS version:", line));
      returnStr.append(extractLine("Java version:", line));
      returnStr.append(extractLine("Java vm name:", line));
      returnStr.append(extractLine("Java vendor:", line));
      returnStr.append(extractLine("Java home:", line));
      int index = line.indexOf(searchStr);
      if (index > 0) { // found the searchStr
        searchStrFoundCntr++;
        if (searchStrFoundCntr == 2) { // found searchStr for 2nd time
          returnStr.append("\n" + line + "\n");
          line = reader.readNextLine();
          while (line != null) { // take all lines until searchStr found again
            returnStr.append(line + "\n");
            if (line.indexOf(searchStr) >= 0) {
              return returnStr.toString();
            }
            line = reader.readNextLine();
          }
        }
      }
      line = reader.readNextLine();
    }
    return returnStr.toString();
  }

  /** Look in baseStr for the searchStr, and if found, return the full line
   *  containing searchStr.
   * @param searchStr The String to search for, then extract the line containing this.
   * @param baseStr The String to extract from. 
   * @return
   */
  private Object extractLine(String searchStr, String baseStr) {
    int index = baseStr.indexOf(searchStr);
    if (index >= 0) {
      int index2 = baseStr.indexOf("\n", index);
      if (index2 >= 0) {
        return baseStr.substring(index, index2+1) + "\n";
      } else {
        return baseStr.substring(index, baseStr.length()) + "\n";
      }
    }
    return ""; // not found
  }

  /**
   * Write the contents of the templateStr.
   * 
   */
  private void writeTemplateFile() {
    if (writeToStdOut) {
      System.out.println(templateStr);
    } else { // write to a file
      PrintWriter aFile;
      try {
        String resultsFileName = hydraRunDirPath + File.separator + templateFileName;
        aFile = new PrintWriter(new FileOutputStream(new File(resultsFileName)));
        aFile.print(templateStr.toString());
        aFile.flush();
        aFile.close();
      } catch (FileNotFoundException e) {
        System.out.println("Unable to create " + templateFileName + " due to " +
            e.toString());
      }
    }
  }

  /**
   * Categorize files of the run being analyzed into Lists for more efficient
   * access while creating the bug report template.
   */
  protected void categorizeFiles() {
    File hydraRunDir = new File(hydraRunDirPath);
    String[] dirContents = hydraRunDir.list();
    String parent = hydraRunDir.getAbsolutePath() + File.separator;
    for (String fileName : dirContents) {
      if (fileName.endsWith(".log")) {
        if (fileName.startsWith("bgexec")) {
          bgExecFileList.add(parent + fileName);
        } else if (fileName.startsWith("vm_")) {
          vmLogFileList.add(parent + fileName);
        } else if (fileName.startsWith("taskmaster")) {
          taskMasterLogFileName = parent + fileName;
        } else if (fileName.startsWith("hs_")) {
          hotspotFileList.add(parent + fileName);
        } else if (fileName.startsWith("Master")) {
          masterLogFileName = parent + fileName;
        }
      } else if (fileName.endsWith(".hprof")) {
        hprofFileList.add(parent + fileName);
      } else if (fileName.endsWith(".prop")) {
        String prefix = fileName.substring(0, fileName.indexOf(".prop"));
        if (hydraRunDirPath.indexOf(prefix) >= 0) { // this is the test prop file
          testPropFileName = parent + fileName;
        }
      }
    }
    errorsDotTextFileName = parent + "errors.txt";
    latestPropFileName = parent + "latest.prop";
    localConfFileName = parent + "local.conf";
  }

  /**
   * Extract information from this run about the error that caused the run to
   * fail (if this run failed).
   * 
   * @return A String containing information about the error that caused this
   *         run to fail, or null if the run passed.
   */
  private String doErrorAnalysis() {
    FileLineReader reader = null;
    try {
      reader = new FileLineReader(errorsDotTextFileName);
    } catch (FileNotFoundException e) {
      return "No errors reported in this run"; // no errors to analyze
    }

    // there are errors in this run; extract the first error in the errors.txt
    // file and report it
    StringBuffer errorAnalysisStr = new StringBuffer();
    String firstErrStr = getFirstError(reader);
    errorAnalysisStr.append("*** Test failed with this error:\n" + firstErrStr);

    // look for files indicating a problem such as hprof or hotspot files
    reportProblemFiles(errorAnalysisStr);

    // do more analysis if the failure is a hang
    if ((firstErrStr != null) && (firstErrStr.indexOf("\nHANG") >= 0)) {
      String hangAnalysisStr = doHangAnalysis(firstErrStr);
      errorAnalysisStr.append(hangAnalysisStr);
    } else { // not a hang
      // look for java-level deadlocks but not a hang
      Set<String> deadlockSet = new HashSet();
      for (String fileName : bgExecFileList) {
        searchBgExecFile(null, fileName, null, deadlockSet);
      }
      reportJavaLevelDeadlocks(deadlockSet, errorAnalysisStr);
    }

    return errorAnalysisStr.toString();
  }

  /**
   * If there were suspicious files present in the run directory, report them to
   * reportStr
   * 
   * @param reportStr
   *          Append any problem files found in this StringBuffer.
   * 
   */
  private void reportProblemFiles(StringBuffer reportStr) {
    if (hotspotFileList.size() > 0) {
      reportStr.append("\nFound the following files:\n");
      for (String fileName : hotspotFileList) {
        reportStr.append("   " + fileName + "\n");
      }
    } else if (hprofFileList.size() > 0) {
      reportStr.append("\nFound the following files:\n");
      for (String fileName : hprofFileList) {
        reportStr.append("   " + fileName + "\n");
      }
    }

  }

  /**
   * This is called when the first error in the errors.txt file indicates we
   * have a hang. This extracts information about the hang and returns it in a
   * String.
   * 
   * @param errStr
   *          A String containing the text of an error in errors.txt.
   * @return A String containing information about the hang.
   */
  private String doHangAnalysis(String errStr) {
    // look for a hang in MasterController doTaskLoop; this is not a product
    // problem but typically fails due to exception with RMI while assigning
    // a hydra task; this is not a product problem and there is nothing to
    // analyze; note the failed stack will still appear in the template file
    final String MCTaskLoopHangStr = "HANG MasterController.doTaskLoop -- treating as hang";
    if (errStr.indexOf(MCTaskLoopHangStr) >= 0) { // this is not a product
      // problem, so nothing to analyze
      return "";
    }

    // look for a hang due to vms not stopping (either at hydra shutdown or
    // with dynamic stop/start
    final String failedToStopStr = "hydra.HydraTimeoutException: Failed to stop client vms within";
    if (errStr.indexOf(failedToStopStr) >= 0) { // look for the stack
      // currently not handled any further
    }

    // look for hang due to hydra task timeout
    final String normalHangStr = "HANG a client exceeded max result wait sec";
    if (errStr.indexOf(normalHangStr) >= 0) {
      String clientIDStr = getClientIdentifierStr(errStr);
      if (clientIDStr == null) return "";
      String hangLoggingStr = getHangLoggingStr();
      if (hangLoggingStr == null) return "";
      Date hangDateTime = getDateFromLoggingLine(hangLoggingStr);
      if (hangDateTime == null) return "";
      String[] anArr = getClientLoggingSurroundingHang(clientIDStr, hangDateTime);
      String lastLineBeforeHang = anArr[0];
      String firstLoggingAfterHang = anArr[1];
      String taskResultLogging = anArr[2];
      String hangAnalysisStr = getStackAnalysis(clientIDStr);
      if (lastLineBeforeHang == null) return "";
      if (hangAnalysisStr ==  null) return "";
      Date lastLoggingDate = getDateFromLoggingLine(lastLineBeforeHang);
      if (lastLoggingDate == null) return "";
      String waitForRepliesLogging = getWaitForRepliesLogging(lastLoggingDate,
          hangDateTime, clientIDStr);

      // create the return string
      StringBuffer returnStr = new StringBuffer();
      returnStr.append("\n*** Last client logging by hung thread\n" + lastLineBeforeHang);
      if (waitForRepliesLogging != null) {
        returnStr.append("\n\n*** From system.log of hung thread\n" + waitForRepliesLogging);
      }
      long timeDiff = hangDateTime.getTime() - lastLoggingDate.getTime();
      returnStr.append("\n\n*** Test declared hung " + timeDiff +
          " ms after last client logging\n" + hangLoggingStr);
      returnStr.append("\n\n" + hangAnalysisStr);
      if (firstLoggingAfterHang != null) {
        Date loggingDate = getDateFromLoggingLine(firstLoggingAfterHang);
        if (loggingDate != null) {
          timeDiff = loggingDate.getTime() - hangDateTime.getTime();
          returnStr.append("\n\n*** Hung thread logged " + timeDiff + 
              " ms after hang declared\n" + firstLoggingAfterHang);
        }
      }
      if (taskResultLogging != null) {
        Date loggingDate = getDateFromLoggingLine(taskResultLogging);
        if (loggingDate != null) {
          timeDiff = loggingDate.getTime() - hangDateTime.getTime();
          returnStr.append("\n\n*** Task result for hung thread logged " +
              timeDiff + " ms after hang declared\n" + taskResultLogging);
        }
      }
      return returnStr.toString();
    }

    // unknown hang not processed by this tool
    return "";
  }

  /** Given a hung test with the date/time of the last client logging and the
   *  date/time of the hang, find any  messages in the system.log which 
   *  involve waiting for replies for the hung client thread.
   * @param lastLoggingDate Date of the last client logging before the hang.
   * @param hangDateTime Date the test was declared hung. 
   * @param clientIDStr Identifier of the client that was hung. 
   * @return String containing any waitForReplies messages from the hung
   *         thread's system.log file. 
   */
  private String getWaitForRepliesLogging(Date lastLoggingDate,
      Date hangDateTime, 
      String clientIDStr) {
    String pid = getPidStr(clientIDStr);
    File hydraDir = new File(hydraRunDirPath);
    String[] contents = hydraDir.list();
    for (String fileName: contents) {
      if (fileName.endsWith("_" + pid)) { // found dir containing system.logs
        File systemDir = new File(hydraRunDirPath + File.separator + fileName);
        String[] systemDirContents = systemDir.list();
        for (String systemLogFileName: systemDirContents) { // iterate system dir contents
          if (systemLogFileName.startsWith("system") && systemLogFileName.endsWith(".log")) {
            StringBuffer returnStr = new StringBuffer();
            FileLineReader reader;
            try {
              reader = new FileLineReader(hydraRunDirPath + File.separator + 
                  fileName + File.separator + systemLogFileName);
            } catch (FileNotFoundException e) {
              return null; // could not read system log we were after
            }
            String line = reader.readNextLine();
            while (line != null) {
              if (line.indexOf(clientIDStr) >= 0) {
                if (line.indexOf("wait for replies completed") >= 0 ||
                    line.indexOf("waiting for replies") >= 0) {
                  Date thisLoggingDate = getDateFromLoggingLine(line);
                  if (thisLoggingDate.after(lastLoggingDate) && thisLoggingDate.before(hangDateTime)) {
                    if (returnStr.length() > 0) {
                      returnStr.append("\n\n");
                    }
                    returnStr.append(line);
                  }
                }
              }
              line = reader.readNextLine();
            }
            if (returnStr.length() > 0) { // found lines of interest
              return returnStr.toString();
            } else { // did not find lines of interest
              return null;
            }
          }
        }
      }
    }
    return null; // found nothing of interest
  }

  /**
   * Given a String containing a hydra client vm, id and pid, find the last
   * logging by that client prior to the hang, the first client logging
   * after the hang, and determine if the hung thread returned a result.
   * 
   * @param clientIDStr
   *          A String containing the jvm id, thread id and pid typically of the
   *          form vm_XX_thr_YY_<clientName>_<hostName>_<pid>
   * @param hangDateTime
   *          The Date for when the test was declared hung.
   * @return [0] A String containing the last logging line of the client indicated
   *             in clientIDStr prior to hanging, or null if the last logging line
   *             could not be found.
   *         [1] A String containing the first logging line of the client indicated
   *             in clientIDStr after the hang, or null if this line could not be
   *             determined.
   *         [2] A String containing the logging for the hung thread when it
   *             returned a result, or null if this line could not be determined.
   */
  private String[] getClientLoggingSurroundingHang(String clientIDStr,
      Date hangDateTime) {
    if (hangDateTime != null) {
      // include <> brackets to identify test logging lines we want to find vs. 
      // automatic deadlock detection
      String loggingTargetStr = "<" + clientIDStr + ">";

      // Look for the last logging prior to the hang made by the jvm and
      // thread indicated in clientIDStr.
      // First, remove the thread ID from the clientIDStr to obtain a client
      // log file name to search.
      String searchStr = "thr_";
      int index = clientIDStr.indexOf(searchStr);
      if (index <= 0) {
        return null;
      }
      int index2 = clientIDStr.indexOf("_", index + searchStr.length());
      String targetLogFileSubstr = clientIDStr.substring(0, index)
      + clientIDStr.substring(index2 + 1, clientIDStr.length());
      for (String fileName : vmLogFileList) { // search the directory the log
        // for clientIDStr
        if (fileName.indexOf(targetLogFileSubstr) >= 0) { // found the correct
          // log file
          FileLineReader reader;
          try {
            reader = new FileLineReader(fileName);
          } catch (FileNotFoundException e) {
            return null;
          }
          String line = reader.readNextLine();
          Date dateTimeOfLastLineBeforeHang = null;
          String lastLineBeforeHang = null;
          String firstLineAfterHang = null;
          while (line != null) { // loop through lines of the log file looking
            // for the lines of interest
            if (line.indexOf(loggingTargetStr) >= 0) { // found a logging line from
                     // the desired client/thread
              Date logLineDateTime = getDateFromLoggingLine(line);
              if (logLineDateTime != null) { // found a date in the logging
                // line of interest
                if (logLineDateTime.before(hangDateTime)) { // this loggging
                  // line is before the hang
                  if ((dateTimeOfLastLineBeforeHang == null) || 
                      logLineDateTime.after(dateTimeOfLastLineBeforeHang) ||
                      logLineDateTime.equals(dateTimeOfLastLineBeforeHang)) {
                    dateTimeOfLastLineBeforeHang = logLineDateTime;
                    lastLineBeforeHang = line;
                  }
                } else { // logging line is after the hang
                  if (line.indexOf("Task result:") >= 0) { // line is task result
                    return new String[] {lastLineBeforeHang, firstLineAfterHang, line};
                  } else {
                    if (firstLineAfterHang == null) {
                      firstLineAfterHang = line;
                    }
                  }
                }
              }
            }
            line = reader.readNextLine();
          }
          return new String[] {lastLineBeforeHang, firstLineAfterHang, null};
        }
      }
    }
    return null;
  }

  /**
   * Look for a hang in the log files and return the logging line that declares
   * the run hung.
   * 
   * @return The logging line that declares the run hung
   */
  private String getHangLoggingStr() {
    FileLineReader reader;
    try {
      reader = new FileLineReader(taskMasterLogFileName);
      String line = reader.readNextLine();
      while (line != null) {
        if (line.indexOf("HANG") >= 0) {
          return line;
        }
        line = reader.readNextLine();
      }
    } catch (FileNotFoundException e) {
      return null;
    }
    return null;
  }

  /**
   * Given a line from a hydra log, return a Date from it or null if no date can
   * be found
   * 
   * @param loggingLine
   *          A line from a hydra log.
   * @return Date The date/time from loggingLine.
   */
  private Date getDateFromLoggingLine(String loggingLine) {
    SimpleDateFormat dateFormat = new SimpleDateFormat(
    "yyyy/MM/dd kk:mm:ss.SSS");

    /* any number of chars followed by dddd/dd/dd dd:dd:dd.ddd */
    String dateRegex = ".*(\\d\\d\\d\\d)/(\\d\\d)/(\\d\\d) (\\d\\d):(\\d\\d):(\\d\\d).(\\d\\d\\d).*";
    if (loggingLine.matches(dateRegex)) { // this line contains a date
      int index = loggingLine.indexOf(" ") + 1;
      return dateFormat.parse(loggingLine, new ParsePosition(index));
    }
    return null;
  }

  /**
   * Given a String indicating a jvm, thread an pid, return a String containing
   * the stack analysis from any thread dumps.
   * 
   * @param clientIDStr
   *          A String from hydra showing a jvm, thread id and pid typically of
   *          the form vm_XX_thr_YY_<clientName>_<hostName>_<pid>
   * @return A String containing stack analysis for stacks found the thread
   *         referenced by clientIDStr.
   */
  private String getStackAnalysis(String clientIDStr) {
    // create a List of stacks for vmID_tid_str
    List<String> stackList = new ArrayList();
    Set<String> deadlockSet = new HashSet();
    // this searches all bgexec*.log files; we do this because we are looking
    // both for stacks and java-level deadlocks
    for (String fileName : bgExecFileList) {
      searchBgExecFile(clientIDStr, fileName, stackList, deadlockSet);
    }

    // build the analysis string
    // first consider stacks
    StringBuffer stackAnalysisStr = new StringBuffer();
    if (stackList.size() == 0) {
      stackAnalysisStr.append("Stacks not found for " + clientIDStr + "\n");
    } else { // found stacks
      // see if the stacks are changing
      List<Integer> changingStacks = new ArrayList(); // List stores indexes
      // into stackList; indicates which stacks differ from stack at index 
      // before it
      int numDifferentStacks = 0;
      for (int i = 1; i < stackList.size(); i++) {
        boolean isEqual = stackList.get(i-1).equals(stackList.get(i));
        if (!isEqual) {
          numDifferentStacks++;
          changingStacks.add(i);
        }
      }

      // build the analysis string
      stackAnalysisStr.append("*** Hung thread\n" + stackList.get(0) + "\n");
      if (numDifferentStacks == 0) { // stack was unchanging
        stackAnalysisStr.append("Stack for hung thread " + clientIDStr
            + " was found " + stackList.size() + " times and was unchanging.");
      } else { // stack changed between stack dumps
        for (int stackListIndex : changingStacks) {
          stackAnalysisStr.append("*** Stack changed\n"
              + stackList.get(stackListIndex) + "\n");
        }
        stackAnalysisStr.append("Stack for hung thread " + clientIDStr
            + " was found " + stackList.size() + " times and changed "
            + numDifferentStacks + " times.");
      }
    }

    // now consider deadlocks
    reportJavaLevelDeadlocks(deadlockSet, stackAnalysisStr);
    return stackAnalysisStr.toString();
  }

  /**
   * Give a list of detected java-level deadlocks (Strings), report those that
   * are unique in the reportStr.
   * 
   * @param deadlockList
   *          A Set where each element is a java-level deadlock
   * @param reportStr
   *          Append a report of the java-level deadlocks to this StringBuffer.
   */
  private static void reportJavaLevelDeadlocks(Set<String> deadlockSet,
      StringBuffer reportStr) {
    if (deadlockSet.size() == 0) {
      return;
    }
    for (String deadlockStr: deadlockSet) {
      reportStr.append("\n\n" + deadlockStr);
    }
  }

  /**
   * Return the pid String from the given clientIDStr.
   * 
   * @param clientIDStr
   *          A String of the form vm_XX_thr_YY_<clientName>_<hostName>_<pid>
   * @return The pid as a String, or null if not found.
   */
  private static String getPidStr(String clientIDStr) {
    int index = clientIDStr.lastIndexOf("_");
    String pidStr = clientIDStr.substring(index + 1, clientIDStr.length());
    String regEx = "(\\d)+"; // at least one digit
    if (pidStr.matches(regEx)) { // have a string of all digits
      return pidStr;
    } else {
      return null;
    }
  }

  /**
   * Given a fileName representing a bgexec*.log file, looks for 1) any stacks
   * for the given vmID and thread ID 2) java-level deadlocks.
   * 
   * @param clientIDStr
   *          A String from hydra showing a jvm, thread id and pid typically of
   *          the form vm_XX_thr_YY_<clientName>_<hostName>_<pid>; this is used
   *          to look for stacks for the given vm and thread, or null if there
   *          are no stacks to look for
   * @param bgExecFileName
   *          The name of a bgexec*.log file.
   * @param stackList
   *          List of stack Strings to append any found stacks to.
   * @param deadlockSet
   *          Set of deadlock Strings to add deadlocks to; since this is a Set
   *          we only record unique deadlock Strings.
   */
  private static void searchBgExecFile(String clientIDStr,
      String bgExecFileName, List<String> stackList, Set<String> deadlockSet) {
    try {
      String stackIDStr = null;
      if (clientIDStr != null) {
        stackIDStr = "\"" + clientIDStr + "\"";
      }
      FileLineReader reader = new FileLineReader(bgExecFileName);
      String line = reader.readNextLine();
      StringBuffer stackStr = new StringBuffer();
      StringBuffer deadlockStr = new StringBuffer();
      boolean inStack = false;
      boolean inDeadlock = false;
      while (line != null) {
        if (inDeadlock) {
          deadlockStr.append(line + "\n");
          if (line.startsWith("Found ") && (line.indexOf("deadlock") > 0)) {
            // end of deadlock
            inDeadlock = false;
            deadlockSet.add(deadlockStr.toString());
            deadlockStr = new StringBuffer();
          }
        } else if (inStack) {
          if (line.length() == 0) { // end of stack
            inStack = false;
            stackList.add(stackStr.toString());
            stackStr = new StringBuffer();
          } else { // in the middle of the stack; just append the line
            stackStr.append(line + "\n");
          }
        } else if ((stackIDStr != null) && (line.indexOf(stackIDStr) >= 0)) { // found
          // a
          // stack
          stackStr.append(line + "\n");
          inStack = true;
        } else if (line.indexOf("Java-level deadlock") >= 0) {
          inDeadlock = true;
          deadlockStr.append("*** From " + bgExecFileName + "\n");
          deadlockStr.append(line + "\n");
        }
        line = reader.readNextLine();
      }
    } catch (FileNotFoundException e) {
      return; // the file does not exist
    }
  }

  /**
   * Find a string in a failure string that shows the client identifier.
   * Typically this would be of the form
   * vm_XX_thr_YY_<clientName>_<hostName>_<pid>.
   * 
   * @param errStr
   *          A String containing an error from an errors.txt file.
   * @return A client identifier String [1] a String containing the pid from [0]
   */
  private static String getClientIdentifierStr(String errStr) {
    String searchStr = "CLIENT ";
    int index = errStr.indexOf(searchStr);
    if (index >= 0) {
      // extract from the errStr the client idenifier string
      int firstIndex = index + searchStr.length();
      int lastIndex = firstIndex;
      while ((lastIndex < errStr.length())
          && Character.isWhitespace(errStr.charAt(lastIndex))) {
        lastIndex++;
      }
      while ((lastIndex < errStr.length())
          && !Character.isWhitespace(errStr.charAt(lastIndex))) {
        lastIndex++;
      }
      String clientIdentifierStr = errStr.substring(firstIndex, lastIndex);
      return clientIdentifierStr;
    } else {
      return null;
    }
  }

  /**
   * Return a String containing the first error reported in the errors.txt file
   * for this run
   * 
   * @param reader
   *          The reader for the errors.txt file.
   * @return String A String containing the text of the first error in
   *         errors.txt.
   */
  private static String getFirstError(FileLineReader reader) {
    StringBuffer firstErrStr = new StringBuffer();
    String line = reader.readNextLine();
    while (line != null) {
      if (isErrorDelimiter(line)) { // done finding first error
        return firstErrStr.toString();
      }
      firstErrStr.append(line + "\n");
      line = reader.readNextLine();
    }
    return firstErrStr.toString();
  }

  /** An error is delimited in errors.txt by a line containing only dashes (-).
   *  Return true if this line contains only dashes, false otherwise.
   *  
   * @param line The line to check
   * @return true if this line is an error delimiter, false otherwise
   */
  private static boolean isErrorDelimiter(String line) {
    if (line.length() == 0) {
      return false;
    }
    for (int i = 0; i < line.length(); i++) {
      if (line.charAt(i) != '-') {
        return false;
      }
    }
    return true;
  }
  //================================================================================
  //Main method

  /** Create a bug report template for a hydra run.
   */
  public static void main(String args[]) throws Exception {
    if (args.length != 0) {
      System.out.println(
          "Usage: java BugReportTemplate\n" +
          "       -DhydraRunDir=<dir path>\n" +
          "         Directory of a hydra run. This is optional. If not \n" +
          "         specified then the current directory is used.\n" +
          "       -DwriteToStdOut=<true|false>\n" +
          "         If true, then write the bug report template to standard\n" +
          "         out, if false then write to a file named bugReportTemplate.txt\n" +
      "         in the hydra run directory. Defaults to false\n");
      System.exit(0);
    }
    String runDirName = System.getProperty("hydraRunDir"); 
    if (runDirName == null) {
      runDirName = System.getProperty("user.dir");
    }
    boolean writeToStdOut = Boolean.valueOf(System.getProperty("writeToStdOut"));
    BugReportTemplate.createTemplateFile(runDirName, writeToStdOut);
  }
}
