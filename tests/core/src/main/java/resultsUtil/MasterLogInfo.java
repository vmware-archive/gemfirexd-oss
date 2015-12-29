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
import java.util.*;

public class MasterLogInfo {

private static MasterLogInfo info = new MasterLogInfo();

public static Set RevisionSet = new HashSet();
public static Set JavaHomeSet = new HashSet();
public static Set JavaVersionSet = new HashSet();
public static Set JavaVendorSet = new HashSet();
public static Set BuildIdSet = new HashSet();
public static Set ProductVersionSet = new HashSet();
public static Set ArchitectureSet = new HashSet();
public static Set SourceRepositorySet = new HashSet();

/*  Get information from the given Master*.log file and
 *  save it.
 *
 *  @param dirPath The directoryPath of a test results directory.
 */
public static void saveMasterLogInfo(String dirPath) {
   info._saveMasterLogInfo(dirPath);
}

private void _saveMasterLogInfo(String dirPath) {   
   File masterLogFile = RegressionSummary.getFileInDir(dirPath, "Master(.*).log");
   if (masterLogFile == null) {
      return;
   }
   
   // read every line in the Master*.log looking for needed information
   FileLineReader flr = null;
   try {
     flr = new FileLineReader(masterLogFile); 
     String line = flr.readNextLine();
     while (line != null) { 
        if (line.indexOf("Source Revision:") >= 0) {
           String revNum = line.substring(line.indexOf(":")+1, line.length());
           revNum = revNum.trim();
           RevisionSet.add(revNum);
        } else if (line.indexOf("Java version:") >= 0) {
           String javaVersion = line.substring(line.indexOf(":")+1, line.length());
           javaVersion = javaVersion.trim();
           JavaVersionSet.add(javaVersion);
        } else if (line.indexOf("Java home:") >= 0) {
           String javaHome = line.substring(line.indexOf(":")+1, line.length());
           javaHome = javaHome.trim();
           JavaHomeSet.add(javaHome);
        } else if (line.indexOf("Java vendor:") >= 0) {
           String javaVendor = line.substring(line.indexOf(":")+1, line.length());
           javaVendor = javaVendor.trim();
           JavaVendorSet.add(javaVendor);
        } else if (line.indexOf("Build Id:") >= 0) {
           String buildId = line.substring(line.indexOf(":")+1, line.length());
           buildId = buildId.trim();
           BuildIdSet.add(buildId);
        } else if (line.indexOf("Architecture:") >= 0) {
           String archit = line.substring(line.indexOf(":")+1, line.length());
           archit = archit.trim();
           ArchitectureSet.add(archit);
        } else if (line.indexOf("Source Repository:") >= 0) {
           String source = line.substring(line.indexOf(":")+1, line.length());
           source = source.trim();
           SourceRepositorySet.add(source);
        } else {
           String aStr = "Product Version:";
           int index = line.indexOf(aStr);
           if (index >= 0) {
              String versionStr = line.substring(index + aStr.length(), line.length());
              versionStr = versionStr.trim();
              ProductVersionSet.add(versionStr);
           }
        }
        line = flr.readNextLine();
     }
   } catch (FileNotFoundException e) {
      return;
   } finally {
     if(flr != null) flr.close(); 
   } 
}

}
