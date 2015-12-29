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
package resultsUtil;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import util.TestHelper;

/** Class to summarize a regression suite into a file, whose contents can be
 *  pasted into a spreadsheet.
 *  Note this is called from runBT.sh, so logging goes to System.out rather
 *  than the hydra logger.
 */
public class RegressionSummary {

public static final int CreateNew = 1;
public static final int ContinueFromExisting = 2;

private BatteryTestLogInfo btInfo = null;
private String btDir = null;
private String logDir = null;
private int howToCreateSummary = -1;
private SummaryInfo summInfo = null;

//================================================================================
// initializers

/** Generate summary results.
 *
 *  @param howToCreateSummary If this is 
 *            RegressionSummary.CreateNew: the entire summary file is created from 
 *               whatever results are currently existing. 
 *            RegressionSummary.ContinueFromExisting: any existing summary file 
 *               found in logDir is appended to with any new results. If an 
 *               existing summary file is not found in logDir, then this has the
 *               same effect as RegressionSummary.CreateNew.
 *  @param btDir The root directory of a batterytest run, which contains
 *            a oneliner.txt and a batterytest.log file. 
 *  @param logDir The directory to write the summary file to.
 */
public RegressionSummary(int howToCreateSummary,
                         String btDir,
                         String logDir) {
   this.howToCreateSummary = howToCreateSummary;
   this.btDir = btDir;
   this.logDir = logDir;
}

//================================================================================
// Methods to summarize the results

/** Process tests in the oneliner.txt file. The summary of the results are stored
 *  in summInfo.
 *
 *  @param oneLinerFile The oneliner.txt file.
 */
private void doSummaryFromOneLiner(File oneLinerFile) throws FileNotFoundException, Exception {
   String spaceStr = "                    ";

   // read every line in the oneliner.txt
   FileLineReader flr = null;
   try {
     flr = new FileLineReader(oneLinerFile); 
     String line = flr.readNextLine();
     int lineCount = 1;
     while (line != null) { 
        if (lineCount > summInfo.getNumTests()) { // we don't yet have this test in the summary
           StringBuffer testSummary = new StringBuffer();
           String[] components = line.split("\\s+");
           String testName = components[0];
           String status = components[1];
           String testTime = components[2];
           String resultDir = components[3];
           String resultDirName = getDirName(resultDir);
           // testDir is a subdirectory of btDir, in case the results
           // were copied to another location 
           String testDir = btDir + File.separator + resultDirName;
           MasterLogInfo.saveMasterLogInfo(testDir);
           if (status.equals("F") || status.equals("H")) {
              String failure = getTestFailure(testDir);
              testSummary.append(status + " " + failure);
           } else {
              // on a passing run, spaces give a cleaner look to the
              // spreadsheet, so it's easier to see at a glance tests
              // that pass vs. tests that fail
              testSummary.append(status);
           }
           testSummary.append(spaceStr);
           testSummary.append("(elapsed: " + testTime + ") ");
           String diskUsage = getDiskUsage(testDir);
           summInfo.addToDiskUsage(diskUsage);
           testSummary.append("(disk: " + diskUsage + ") ");
           // storing memory usage in spreadsheets is not useful and makes the spreadsheet larger
           //testSummary.append("(memory usage: " + getMemoryUsage(testDir) + ")");
           summInfo.addTest(testSummary.toString());
        }
        line = flr.readNextLine();
        lineCount++;
     }
   } finally {
     if(flr != null) flr.close();
   }
} 

/** Get a summary of the given battery test directory (a directory containing
 *  a oneliner.txt file).
 *
 *  @param dir The directory that contains a oneliner.txt file.
 */
private void doSummary(File dir) throws Exception, FileNotFoundException {
   File[] fileList = dir.listFiles();
   for (int i = 0; i < fileList.length; i++) {
      File aFile = fileList[i];
      String name = aFile.getName();
      if (name.equals("oneliner.txt")) { 
         doSummaryFromOneLiner(aFile); 
         summInfo.updateAndCombine(btInfo);
         return;
      } 
   }
} 

/** Create a summary of a battery test run, suitable for pasting into a spreadsheet.
 *
 */
public void doSummary() throws Exception {
   File dir = new File(btDir);
   if (!dir.exists()) {
      throw new Exception(dir.getAbsolutePath() + " does not exist; expected directory containing oneliner.txt");
   }
   if (!isBtDir(dir)) {
      throw new Exception(dir.getAbsolutePath() + " does not contain oneliner.txt");
   }
   btInfo = new BatteryTestLogInfo(dir.getAbsolutePath());
   if (howToCreateSummary == ContinueFromExisting) {
      summInfo = new SummaryInfo(btDir + File.separator + getAutoGeneratedSummFileName());
   } else if (howToCreateSummary == CreateNew) {
      summInfo = new SummaryInfo();
   } else {
      throw new Exception("Unknown howToCreateSummary " + howToCreateSummary);
   }
   doSummary(dir);
   if (logDir == null) {
      System.out.println(summInfo);
   } else {
      logStringToFile(logDir, summInfo.toString());
   }
}

//================================================================================
// Other methods

/** Log the string to a file of the given name.
 *
 *  @param logDirStr The name of the directory to log to.
 *  @param aStr The string to log to the file.
 */
private void logStringToFile(String logDirStr, String aStr) throws FileNotFoundException {
   File logDir = new File(logDirStr);
   if (!logDir.exists()) {
      System.out.println("logDir " + logDirStr + " does not exist");
      return;
   }
   String fileName = getAutoGeneratedSummFileName();
   StringBuffer tmp = new StringBuffer(fileName);
   for (int i = 0; i < tmp.length(); i++) {
      if (tmp.charAt(i) == ' ') {
         tmp.setCharAt(i, '-');
      }
   }
   fileName = tmp.toString();
   PrintWriter aFile = new PrintWriter(new FileOutputStream(new File(fileName)));
   aFile.print(aStr);
   aFile.flush();
   aFile.close();
}

/** Get the memory usage of a test, if possible.
 *  Stats are different per platform:
 *    linux:   want to return rssSize in LinuxProcessStats
 *    solaris: want to return rssSize in SolarisProcessStats
 *    windows: want to return freeMemory, maxMemory and totalMemory
 *             in VMStats.
 *
 *  @param dirPath The directoryPath of a test results directory.
 *
 *  @returns A String describing memory usage for the test.
 */
private String getMemoryUsage(String dirPath) {
   File perfReportfile = getFileInDir(dirPath, "perfreport.txt");
   if (perfReportfile == null) {
      return "Unable to get memory usage for this test; perfreport.txt does not exist";
   }
   
   // read every line in the perfreport.txt looking for appropriate stat
   FileLineReader flr = null;
   try {
     flr = new FileLineReader(perfReportfile); 
     String line = flr.readNextLine();
     StringBuffer vmStats = new StringBuffer();
     while (line != null) { 
        boolean isRssStat = line.indexOf("rssSize") >= 0;
        boolean isVMStat = line.indexOf("VMStats") >= 0;
        if (isRssStat || isVMStat) {
           // grab up all lines for this stat; stat ends with line of "=====..."
           StringBuffer statsStr = new StringBuffer();
           do {
              if (line.indexOf("--------") < 0) { // eliminate separator lines containing "----..."
                 statsStr.append(line.trim() + " ");  // remove line endings
              }
              line = flr.readNextLine(); 
           } while ((line != null) && (line.indexOf("=====") < 0));
  
           if (isRssStat && statsStr.indexOf("min=") >=0) { // rssSize stats were found
              return statsStr.toString();
           } else if (isVMStat) { // save the VMstats; if we don't find rssSize stats, we will use VMstats
              vmStats.append(statsStr.toString());
           }
        }
        line = flr.readNextLine();
     }

     // if we had found any rssSize stats, it would have been returned already;
     // make sure we have VMStats present to return 
     if (vmStats.indexOf("min=") >= 0) { // successful in getting VMStats
        return vmStats.toString();
     } else {
        return "Unable to get memory usage for this test; unable to find *ProcessStats or VMStats";
     }
   } catch (FileNotFoundException e) {
      return "Unable to get memory usage for this test; perfreport.txt does not exist";
   } finally {
     if(flr != null) flr.close();
   }
}

/** Return a string showing the disk usage of a directory.
 *
 *  @param dirPath The directory to get disk usage for.
 *
 *  @returns A string showing the disk usage of dirPath.
 */
private String getDiskUsage(String dirPath) throws FileNotFoundException, InterruptedException, IOException {
   if (hydra.HostHelper.isWindows()) { // du -sh below not supported on windows without cygwin
      return "unknown";
   }
   File aDir = new File(dirPath);
   if (!aDir.exists()) {
      return "unknown";
   }
   String dirName = getDirName(dirPath);
   String cmd = "du -sh " + dirPath;
   String result = execute(cmd, 10000);
   if (result.indexOf(dirName) > 0) { // we were successful
      String[] components = result.trim().split("\\s+");
      if (components.length >= 1) {
         return components[0];
      }
   }
   return "unknown";
}

/** Given a directory path, see if it contains a file named fileName, and
 *  return a File for it.
 *
 *  @param dirPath The fully qualified path for the directory
 *
 *  @returns A File for the fileName in dirPath, or null if it could
 *           not be located.
 */
public static File getFileInDir(String dirPath, String fileName) {
   File aDir = new File(dirPath);
   if (!aDir.exists()) { 
      return null;
   }
   File[] fileList = aDir.listFiles();
   boolean found = false;
   for (int i = 0; i < fileList.length; i++) {
      File aFile = fileList[i];
      String name = aFile.getName();
      if (name.matches(fileName)) {
         return aFile;
      } 
   }
   return null;
} 

/** Given a path to a directory, return the name of the directory without
 *  the path. 
 *
 *  @param fullPath A string containing a full directory path.
 *
 *  @returns The name of the directory without the path.
 */
private String getDirName(String fullPath) {
   int index1 = fullPath.lastIndexOf("/");
   int index2 = fullPath.lastIndexOf("\\");
   int index = Math.max(index1, index2);
   String dirName = fullPath.substring(index+1, fullPath.length());
   return dirName;
}

/** Determine if the given dir is a battery test directory. A directory
 *  is a battery test directory if it contains a oneliner.txt file.
 */
private boolean isBtDir(File aDir) {
   if (aDir == null)
      return false;
   if (!aDir.exists())
      return false;
   if (!aDir.isDirectory())
      return false;
   File[] fileList = aDir.listFiles();
   for (int i = 0; i < fileList.length; i++) {
      if (fileList[i].getName().equals("oneliner.txt")) {
         return true;
      }
   }
   return false;
}

/** Given the dir name of a test run, return the first error detected in
 *  the errors.txt file. 
 *
 *  @param dirPath The directoryPath of a test results directory.
 *
 *  @returns A String showing the first error in the errors.txt file.
 */
private String getTestFailure(String dirPath) {
   final int MAX_ERROR_LENGTH = 1000;
   // find the results directory
   File aDir = new File(dirPath);
   if (!aDir.exists()) { 
      return "unable to locate results directory " + dirPath;
   }

   // find the errors.txt file 
   File[] fileList = aDir.listFiles();
   for (int i = 0; i < fileList.length; i++) {
      File aFile = fileList[i];
      String name = aFile.getName();
      if (name.equals("errors.txt")) {
         File errorsTxtFile = aFile;
         FileLineReader flr = null;
         try {
           flr = new FileLineReader(errorsTxtFile); 
           String line = flr.readNextLine();
           while (line != null) {
              if (line.indexOf("HANG") == 0) {
                 return line.substring(0, Math.min(line.length(), MAX_ERROR_LENGTH));
              } else if (line.indexOf("ERROR") == 0) {
                 return line.substring(0, Math.min(line.length(), MAX_ERROR_LENGTH));
              } else if (line.indexOf("-----------------") == 0) {
                 return "Unable to determine test failure";
              }
              line = flr.readNextLine();
           }
         } catch (FileNotFoundException e) {
            return "Unable to read " + errorsTxtFile.getName();
         } finally {
           if(flr != null) flr.close();
         }
      }
   }
   return "Unable to find test failure";
}

/** Execute the given command on the OS.
 *
 *  @param cmd The command string to execute.
 *  @param waitMillis The number of millis to wait for the command
 *         subprocess to return, otherwise kill the subprocess.
 *
 *  @returns The output from running cmd.
 */
protected String execute(String cmd, final int waitMillis) throws InterruptedException, IOException {
//   System.out.println("xxxExecuting cmd: %" + cmd + "%");
   final long startTime = System.currentTimeMillis();
   final Process aProcess = Runtime.getRuntime().exec(cmd);

   // thread to wait for the process to complete
   Thread waitForProcessThread = new Thread(new Runnable() {
      public void run() {
         try {
            aProcess.waitFor();
         } catch (InterruptedException e) {
         }
      }
   });
   waitForProcessThread.start();
   waitForProcessThread.join(waitMillis);
   if (waitForProcessThread.isAlive()) {
      aProcess.destroy();
      waitForProcessThread.join();
      return "";
   }
   String aStr = getOutputFrom(aProcess);
   return aStr;
}

/** Given a process, wait for it to complete and return a string
 *  of its output.
 */
private static String getOutputFrom(Process aProcess) throws InterruptedException, IOException {
   BufferedReader bfOut = null;
   BufferedReader bfErr = null;
   InputStream in = aProcess.getInputStream();
   bfOut = new BufferedReader(new InputStreamReader(in));
   InputStream err = aProcess.getErrorStream();
   bfErr = new BufferedReader(new InputStreamReader(err));

   aProcess.waitFor();
   StringBuffer resultBuff = new StringBuffer();
   String line;
   while ((line = bfOut.readLine()) != null) {
      resultBuff.append(line);
   resultBuff.append("\n");
   }
   while ((line = bfErr.readLine()) != null) {
      resultBuff.append(line);
      resultBuff.append("\n");
   }
   return resultBuff.toString();
}

/** Return the auto generated file name of a summary file
 */
protected String getAutoGeneratedSummFileName() {
   String fileName = null;
   if (btInfo.getRegressionSuiteName() == null) {
      fileName = "summ-B" + btInfo.getBuildNumber() + "-" +
                  btInfo.getHostName() + "-" +
                  btInfo.getRegressionSuiteDate() + ".txt";
   } else {
      fileName = "summ-B" + btInfo.getBuildNumber() + "-" +
                 btInfo.getRegressionSuiteName() + "-" +
                 btInfo.getHostName() + "-" +
                 btInfo.getRegressionSuiteDate() + ".txt";
   }
   StringBuffer tmp = new StringBuffer(fileName);
   for (int i = 0; i < tmp.length(); i++) {
      if (tmp.charAt(i) == ' ') {
         tmp.setCharAt(i, '-');
      }
   }
   fileName = tmp.toString();
   return fileName;
}

//================================================================================
// Main method

/** Summarize the results of a batterytest in a format that can be pasted into
 *  a spreadsheet. See usage text below.
 */
public static void main(String args[]) throws Exception {
   if (args.length != 0) {
      System.out.println(
         "Usage: java RegressionSummary\n" +
         "       -DbtDir=<dir path>\n" +
         "          Directory that contains at a minimum oneliner.txt.\n" +
         "          In addition, this can generate a better summary if this\n" +
         "          directory also contains a batterytest.log file and test\n" +
         "          result directories. If not specified, use the current directory.\n" +
         "       -DlogDir=<dir path>\n" +
         "          Log the summary to this directory to an automatically generated\n" +
         "          filename. If not specified, then log the summary to stdout\n" +
         "       -DhowToCreateSummary=<createNew|continueFromExisting>\n" +
         "          If createNew (the default if not specified), then create a new\n" +
         "          summary file. If continueFromExisting, any existing summary\n" +
         "          file in logDir will be modified in order to update it if it\n" +
         "          is not complete.\n");
      System.exit(0);
   }
   String btDir = System.getProperty("btDir"); 
   if (btDir == null)
      btDir = System.getProperty("user.dir");
   String logDir = System.getProperty("logDir"); 
   String how = System.getProperty("howToCreateSummary"); 
   RegressionSummary regrSumm = null;
   if ((how == null) || how.equalsIgnoreCase("createNew")) {
      regrSumm = new RegressionSummary(CreateNew, btDir, logDir);
   } else {
      regrSumm = new RegressionSummary(ContinueFromExisting, btDir, logDir);
   }
   regrSumm.doSummary();
}

}
