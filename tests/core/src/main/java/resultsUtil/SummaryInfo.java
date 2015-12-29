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
import util.*;

/** Class that contains the currently known summary information about a regression.
 */
public class SummaryInfo {

// the state of the summary information
private String buildInfo = null;
private String svnRevision = null;
private String svnRepository = null;
private String hostName = null;
private String osInfo = null;
private String javaHome = null;
private String javaVersion = null;
private String architecture = null;
private String javaVendor = null;
private String productVersion = null;
private String elapsedTime = null;
private float diskUsage = 0;
private String date = null;
private List testResultList = new ArrayList();

private static final String buildInfoLabel      = "Build: ";
private static final String svnRevisionLabel    = "SVN revision: ";
private static final String svnRepositoryLabel  = "Source repository: ";
private static final String hostNameLabel       = "Host: ";
private static final String osLabel             = "OS: ";
private static final String javaHomeLabel       = "Java home: ";
private static final String javaVersionLabel    = "Java version: ";
private static final String architectureLabel   = "Architecture: ";
private static final String javaVendorLabel     = "Java vendor: ";
private static final String productVersionLabel = "Product version: ";
private static final String elapsedTimeLabel    = "Elapsed time: ";
private static final String diskUsageLabel      = "Disk usage: ";

//================================================================================
// initializers

/** Create a new SummaryInfo from the given summary file path.
 *
 *  @param filePath The full directory path and file name of an existing
 *         summary file.
 */
public SummaryInfo(String filePath) throws Exception {
   importFromFile(filePath);
}

public SummaryInfo() {
}

//================================================================================
// public methods

/** Return the current number of tests in this summary
 */
public int getNumTests() {
   return testResultList.size();
}

/** Add a test summary string
 */
public void addTest(String testSummary) {
   testResultList.add(testSummary);
}

/** Add to the disk usage for this regression summary
 */
public void addToDiskUsage(String diskUsageArg) throws Exception {
   if (diskUsageArg.equalsIgnoreCase("unknown")) {
      // cannot add to total
   } else {
      char lastChar = diskUsageArg.charAt(diskUsageArg.length()-1);
      float diskValue = 0;
      String numberStr = diskUsageArg;
      if (!Character.isDigit(lastChar)) {
         numberStr = diskUsageArg.substring(0, diskUsageArg.length()-1);
      }
      try {
         diskValue = Float.valueOf(numberStr).floatValue(); 
      } catch (NumberFormatException e) {
         throw new Exception("Cannot convert " + numberStr + " to float");
      }
      if (lastChar == 'K') {
         diskValue = diskValue * 1024;
      } else if (lastChar == 'M') {
         diskValue = diskValue * 1024 * 1024;
      } else if (lastChar == 'G') {
         diskValue = diskValue * 1024 * 1024 * 1024;
      }
      diskUsage += diskValue;
   }
}

/** Return a string represenation of this instance, suitable for
 *  pasting into a regression spreadsheet.
 */
public String toString() {
   StringBuffer aStr = getHeadingString();
   for (int i = 0; i < testResultList.size(); i++) {
      aStr.append(testResultList.get(i) + "\n");   
   }
   return aStr.toString();
}

/** Using information in btInfo and MasterLogInfo, combine information
 *  with the current state of this SummaryInfo.
 */
public void updateAndCombine(BatteryTestLogInfo btInfo) {
   // build number
   if (MasterLogInfo.BuildIdSet.size() == 0) {
      if (btInfo.getBuildNumber() != null) {
         buildInfo = combine(buildInfo, btInfo.getBuildNumber());
      }
   } else {
      buildInfo = combine(buildInfo, MasterLogInfo.BuildIdSet);
   } 
   svnRevision = combine(svnRevision, MasterLogInfo.RevisionSet);
   svnRepository = combine(svnRepository, MasterLogInfo.SourceRepositorySet);
   hostName = combine(hostName, btInfo.getHostName());
   String osType = getOSType();
   if (osType.length() > 0) {
      osType = osType + " ";
   }
   osInfo = osType + System.getProperty("os.name") + " " +
                     System.getProperty("os.arch") + " " +
                     System.getProperty("os.version");
   javaHome = combine(javaHome, MasterLogInfo.JavaHomeSet);
   javaVersion = combine(javaVersion, MasterLogInfo.JavaVersionSet);
   architecture = combine(architecture, MasterLogInfo.ArchitectureSet);
   javaVendor = combine(javaVendor, MasterLogInfo.JavaVendorSet);
   productVersion = combine(productVersion, MasterLogInfo.ProductVersionSet);
   productVersion = combine(productVersion, btInfo.getProductVersion());
   if ((btInfo.getFirstDate() == null) || (btInfo.getLastDate() == null)) {
      elapsedTime = "Elapsed time not available";
   } else {
      long elapsed = btInfo.getLastDate().getTime() -
                     btInfo.getFirstDate().getTime();
      long hours, minutes, seconds, millis;
      millis = elapsed % 1000;
      elapsed = elapsed / 1000;
      hours = elapsed / 3600;
      elapsed = elapsed - (hours * 3600);
      minutes = elapsed / 60;
      seconds = elapsed - (minutes * 60);
      String elapsedStr = expandNum(hours, 2) + ":" + expandNum(minutes, 2) + 
             ":" + expandNum(seconds, 2) + "." + expandNum(millis, 3);
      elapsedTime = elapsedStr;
   }
   // (read in) or none, and subsequent tests append to it
   if (btInfo.getFirstDate() != null) {
      SimpleDateFormat format = new SimpleDateFormat("MMM dd yyyy");
      date = format.format(btInfo.getFirstDate());
   }
}

//================================================================================
// non-public methods

/** Import information from the given summary file into this instance of
 *  SummaryInfo.
 *
 *  @param filePath The full directory path and file name of an existing
 *         summary file.
 */
protected void importFromFile(String filePath) throws Exception {
   File aFile = new File(filePath);
   FileLineReader flr = null;
   try {
     flr = new FileLineReader(filePath); 
     // read the heading information
     String line = flr.readNextLine();
     buildInfo = getStringDelimitedBy(line, buildInfoLabel, null);
     line = flr.readNextLine();
     svnRevision = getStringDelimitedBy(line, svnRevisionLabel, ", ");
     svnRepository = getStringDelimitedBy(line, svnRepositoryLabel, null);
     line = flr.readNextLine();
     hostName = getStringDelimitedBy(line, hostNameLabel, ", ");
     architecture = getStringDelimitedBy(line, architectureLabel, null);
     line = flr.readNextLine();
     osInfo = getStringDelimitedBy(line, osLabel, null);
     line = flr.readNextLine();
     javaVersion = getStringDelimitedBy(line, javaVersionLabel, ",");
     javaHome = getStringDelimitedBy(line, javaHomeLabel, ",");
     line = flr.readNextLine();
     javaVendor = getStringDelimitedBy(line, javaVendorLabel, null);
     line = flr.readNextLine();
     productVersion = getStringDelimitedBy(line, productVersionLabel, null);
     line = flr.readNextLine(); 
     elapsedTime = getStringDelimitedBy(line, elapsedTimeLabel, null);
     line = flr.readNextLine();
     String diskStr = getStringDelimitedBy(line, diskUsageLabel, null);
     addToDiskUsage(diskStr);
     date = flr.readNextLine();
   
     // read each test line
     line = flr.readNextLine();
     while (line != null) {
        testResultList.add(line);
        line = flr.readNextLine();
     }
   } catch (FileNotFoundException e) {
      return; // the file does not exist; no information to import
   } finally { 
     if(flr != null) flr.close();
   }
}

/** Given a string, return the substring delimited by the given prefix and suffix,
 *  or null if a substring cannot be deteremined.
 *
 *  @param aStr The string to get a substring of.
 *  @param prefixDelim The beginning delimeter to the desired substring.
 *  @param suffisDelim The ending delimeter to the desired substring.
 *
 *  @return The substring delimited by prefixDelim and suffixDeli, or null.
 */
protected String getStringDelimitedBy(String aStr, String prefixDelim, String suffixDelim) {
   int prefixIndex = aStr.indexOf(prefixDelim);
   if (prefixIndex >= 0) {
      if (suffixDelim == null) {
         return aStr.substring(prefixIndex + prefixDelim.length(), aStr.length());
      } else {
         int suffixIndex = aStr.indexOf(suffixDelim, prefixIndex);
         if (suffixIndex >= 0) {
            return aStr.substring(prefixIndex + prefixDelim.length(), suffixIndex);
         } else {
            return null;
         }
      }
   } else {
      return null;
   }
}

/** Return the heading string for the summary file. The heading string
 *  is the text that specifies the build number, java version, etc.
 */
protected StringBuffer getHeadingString() {
   StringBuffer headingStr = new StringBuffer();
   headingStr.append(formatHeaderLine(buildInfoLabel, buildInfo) + "\n");
   headingStr.append(formatHeaderLine(svnRevisionLabel, svnRevision) + ", " + 
                     formatHeaderLine(svnRepositoryLabel, svnRepository) + "\n");
   headingStr.append(formatHeaderLine(hostNameLabel, hostName) + ", " +
                     formatHeaderLine(architectureLabel, architecture) + "\n");
   headingStr.append(formatHeaderLine(osLabel, osInfo) + "\n");
   headingStr.append(formatHeaderLine(javaVersionLabel, javaVersion) + ", " + 
                     formatHeaderLine(javaHomeLabel, javaHome) + "\n");
   headingStr.append(formatHeaderLine(javaVendorLabel, javaVendor) + "\n");
   headingStr.append(formatHeaderLine(productVersionLabel, productVersion) + "\n");
   headingStr.append(formatHeaderLine(elapsedTimeLabel, elapsedTime) + "\n");
   headingStr.append(formatHeaderLine(diskUsageLabel, getReadableBytes(diskUsage) + "\n"));
   if (date == null) {
      headingStr.append("Date unknown\n");
   } else {
      headingStr.append(date + "\n");
   }
   return headingStr;
}

/** Return a formatted string for the heading section of summary file text.
 *
 *  @param label The label for the heading information.
 *  @param info The actual information for the heading string.
 */
private String formatHeaderLine(String label, String info) {
   if (info == null) {
      return label + "unknown";
   } else {
      return label + info;
   }
}

/** Given existing information and new information, combine them if
 *  the new information is not already included in the existing information.
 *
 *  @param existingInfo Existing state.
 *  @param newInfo New state to combine with existingInfo.
 *
 *  @return The new combined information.
 */
protected String combine(String existingInfo, String newInfo) {
   if (existingInfo == null) { // not here
      return newInfo;
   } else { // combine
      if (existingInfo.indexOf(newInfo) < 0) { // not currently here in existing info
         return existingInfo + " " + newInfo; // append new to old
      }
   }
   return existingInfo;
}

/** Given existing information and a Set of new information, combine them if
 *  the new information is not already included in the existing information.
 *
 *  @param existingInfo Existing state.
 *  @param newInfo New state to combine with existingInfo.
 *
 *  @return The new combined information.
 */
protected String combine(String existingInfo, Set newInfo) {
   String returnInfo = existingInfo;
   Iterator it = newInfo.iterator();
   while (it.hasNext()) {
      String element = (String)(it.next());
      if (returnInfo == null) { // not here
         returnInfo = element;
      } else { // combine
         if (returnInfo.indexOf(element) < 0) { // not currently here
            returnInfo = returnInfo + " " + element;
         }
      }
   }
   return returnInfo;
}


/** Return a String representing anInt using numDigist number of digits. This
 *  will pad a number with zeroes to give it the proper number of digits.
 */
private String expandNum(long aNum, int numDigits) {
   String aStr = "" + aNum;
   while (aStr.length() < numDigits) {
      aStr = "0" + aStr;
   }
   return aStr;
}
 
/** Try to get the OS type from the motd file, if it exists
 */
private String getOSType() {
   File aFile = new File("/etc/motd");
   FileLineReader flr = null;
   try {
     flr = new FileLineReader(aFile); 
     String line = flr.readNextLine();
     while (line != null) {
        if (line.indexOf("Red Hat") >= 0)
           return "RedHat";
        else if (line.indexOf("SLES") >= 0)
           return "SLES";
        else if (line.indexOf("SUSE") >= 0)
           return "SUSE";
        else if (line.indexOf("SunOS") >= 0) {
           int index = line.indexOf("SunOS") + "SunOS".length();
           while (Character.isWhitespace(line.charAt(index))) {
              index++;
           }
           StringBuffer osNum = new StringBuffer();
           while (!Character.isWhitespace(line.charAt(index))) {
              osNum.append(line.charAt(index));
              index++;
           }
           return "SunOS-" + osNum;
        }
        line = flr.readNextLine();
     }
     return "";
   } catch (FileNotFoundException e) {
      return "";
   } finally {
     if(flr != null) flr.close();
   }
}

/** Convert a number of bytes to a readable string (for example "874M")
 */
protected static String getReadableBytes(float numBytes) {
   final float M = 1024 * 1024;
   final float G = M * 1024;
   if (numBytes > G) {
      float readable = numBytes / G;
      return "" + (Math.round(readable*10.0))/10.0 + "G";
   } else if (numBytes > M) {
      float readable = numBytes / M;
      return "" + (Math.round(readable*10.0))/10.0 + "M";
   } else if (numBytes <= 0) {
      return "unknown";
   } else {
      return "" + numBytes + "B";
   }
}

}
