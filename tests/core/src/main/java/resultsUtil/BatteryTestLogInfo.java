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
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;

public class BatteryTestLogInfo {

private Date firstDate = null;             /* the first date occurence in the batterytest.log file */
private Date lastDate = null;              /* the last date occurence in the batterytest.log file */
private String buildNumber = null;         /* build number from the batterytest.log file */
private String hostName = null;            /* host name from the batterytest.log file */
private String regressionSuiteDate = null; /* the date of the regression suite */
private String regressionSuiteName = null; /* the full name of the regression suite, such as parRegBridge */
private String productVersion = null;      /* the product version */
   
/** Constructor to create an instance based on the given directory name.
 *
 *  @param batteryTestLogDir The directory of a battery test run, which contains
 *         a batterytest.log file.
 */
public BatteryTestLogInfo(String batteryTestLogDir) {
   File btLogFile = getBatteryTestLogFile(batteryTestLogDir);
   if (btLogFile != null) 
      initFromFile(btLogFile);
}
   
/** Initialize the instance with information taken from the batterytest.log
 *  file in the given directory.
 *
 *  @param batteryTestLogDirName The name of the directory containing a 
 *         batterytest.log file.
 *
 *  @returns A File for the batterytest.log file, or null if it could not be found.
 */
private File getBatteryTestLogFile(String batteryTestLogDirName) {
   File batteryTestLogDir = new File(batteryTestLogDirName);
   if (!batteryTestLogDir.exists()) {
      System.out.println(batteryTestLogDir + " does not exist; expected directory containing batterytest.log");
      return null;
   }
   File[] fileList = batteryTestLogDir.listFiles();
   boolean found = false;
   for (int i = 0; i < fileList.length; i++) {
      File aFile = fileList[i];
      String name = aFile.getName();
      if (name.equals("batterytest.log")) { 
         return aFile;
      } 
   }
//   System.out.println("Could not find a batterytest.log file in " + batteryTestLogDir.getName());
   return null;
} 

/** Read the given batterytest.log file and extract useful information from it,
 *  saving the information in this instance.
 *
 *  btLogFile The file for the batterytest.log.
 */
private void initFromFile(File btLogFile) {
   SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd kk:mm:ss.SSS");
   FileLineReader flr = null;
   try {
     flr = new FileLineReader(btLogFile); 
     String firstDateLine = null;
     String lastDateLine = null;
     final String hostKey = "Host name: ";
     final String regressionNameKey = "testFileName = ";
     final String regressionDateKey = "resultDir = ";
     final String productVersionKey = "Product Version:";
     final String buildNumberKey = "Build Id: ";
     String line = flr.readNextLine();
     String dateRegex = ".*(\\d\\d\\d\\d)/(\\d\\d)/(\\d\\d) (\\d\\d):(\\d\\d):(\\d\\d).(\\d\\d\\d).*";
            /* any number of chars followed by dddd/dd/dd dd:dd:dd.ddd */
     while (line != null) { 
        if (line.matches(dateRegex)) { // this line contains a date
           if (firstDateLine == null)
              firstDateLine = line;
           lastDateLine = line;
        }
        if (buildNumber == null) {
           final String snapshotsStr = "snapshots.";
           int index = line.indexOf(snapshotsStr);
           if (index >= 0) {
              buildNumber = line.substring(index + snapshotsStr.length(), 
                                           line.indexOf(File.separator, index));
           }
        } 
        if (hostName == null) {
           int index = line.indexOf(hostKey);
           if (index >= 0) {
              hostName = line.substring(index + hostKey.length(), 
                                        line.length());
           }
        }
        if (regressionSuiteName == null) {
           String alteredLine = line.replaceAll("\\\\","/");
           int index = line.indexOf(regressionNameKey);
           if (index >= 0) {
              String searchStr = "tests/classes/";
              index = alteredLine.indexOf(searchStr);
              if (index < 0) {
                 searchStr = "tests/";
                 index = alteredLine.indexOf(searchStr);
              }
              if (index >= 0) {
                 String subStr = alteredLine.substring(index + searchStr.length(), alteredLine.length());
                 String[] tokens = subStr.split("[/\\\\]"); 
                 regressionSuiteName = "";
                 for (int i = 0; i < tokens.length; i++) {
                    regressionSuiteName = regressionSuiteName + tokens[i];
                    if (i < tokens.length-1) { // not last one
                       regressionSuiteName = regressionSuiteName + ".";
                    } else { // not last
                    }
                 }
              }
           }
        }
        if (regressionSuiteDate == null) {
           int index = line.indexOf(regressionDateKey);
           if (index >= 0) {
              String[] tokens = line.split("[/\\\\]");
              regressionSuiteDate = tokens[tokens.length-1];
           }
        }
        if (productVersion == null) {
           int index = line.indexOf(productVersionKey);
           if (index >= 0) {
              productVersion = line.substring(index + productVersionKey.length(), line.length()).trim();
           }
        }
        if (buildNumber == null) {
           int index = line.indexOf(buildNumberKey);
           if (index >= 0) {
              String[] tokens = line.split("[\\s]+"); // split on white space
              buildNumber = tokens[tokens.length-2] + "-" + tokens[tokens.length-1];
           }
        }
        line = flr.readNextLine();
     }
     if (firstDateLine != null) {
        int index = firstDateLine.indexOf(" ") + 1;
        firstDate = dateFormat.parse(firstDateLine, new ParsePosition(index)); 
     }
     if (lastDateLine != null) {
        int index = lastDateLine.indexOf(" ") + 1;
        lastDate = dateFormat.parse(lastDateLine, new ParsePosition(lastDateLine.indexOf("20"))); 
     }
   } catch (FileNotFoundException e) {
//      System.out.println("Could not find batterytest.log file in " + btLogFile.getName());
      return;
   } finally {
     if(flr != null) flr.close();
   }
} 

//==============================================================================
// string methods
public String toString() {
   return "BatteryTestInfo with firstDate " + firstDate + "\n" +
          "                     lastDate " + lastDate + "\n" +
          "                     hostName " + hostName + "\n" +
          "                     buildNumber " + buildNumber + "\n" +
          "                     regressionSuiteName " + regressionSuiteName + "\n" +
          "                     regressionSuiteDate " + regressionSuiteDate + "\n" +
          "                     productVersion " + productVersion;
}

//==============================================================================
// getters
public Date getFirstDate() {
   return firstDate;
}
public Date getLastDate() {
   return lastDate;
}
public String getBuildNumber() {
   return buildNumber;
}
public String getHostName() {
   return hostName;
}
public String getRegressionSuiteName() {
   return regressionSuiteName;
}
public String getRegressionSuiteDate() {
   return regressionSuiteDate;
}
public String getProductVersion() {
   return productVersion;
}
}
