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

package hydratest.hydracheck;

import hydra.*;
import java.io.*;
import util.*;

/**
 * Client to check various things hydra before running on set of hardware.
 */

public class HydraCheckClient {
  public static void validateJDKVersion() { 
    Log.getLogWriter().info("Checking JDK version");
    String expectedJDK = TestConfig.tab().stringAt(HydraCheckPrms.jdkVersion);
    String actualJDK = System.getProperty("java.version");
    if (!actualJDK.equals(expectedJDK)) {
      String s = "Expected JDK " + expectedJDK + ", found " + actualJDK;
      throw new HydraRuntimeException(s);
    }
    Log.getLogWriter().info("Checked JDK version is " + actualJDK);
  }
  public static void openCache() { 
    CacheUtil.createCache();
    CacheUtil.createRegion("TestRegion");
    CacheUtil.closeCache();
  }
  public static void moveSystemDirectories()
  throws IOException { 
    Log.getLogWriter().info("Moving system directories");
    String cmd = System.getProperty("java.home") + "/bin/java "
               + "-classpath " + System.getProperty("java.class.path")
               + " hydra.TestFileUtil move sysdirs";
    String result = execute(cmd);
    BufferedReader reader = new BufferedReader(new StringReader(result));
    String line;
    while ((line = reader.readLine()) != null) {
      if (!result.startsWith("Moving")) {
        String s = "Problem moving system directory: " + line;
        throw new HydraRuntimeException(s);
      }
    }
    reader.close();
    Log.getLogWriter().info("Moved system directories:\n" + result);
  }
  public static void grepForIssues()
  throws FileNotFoundException, IOException {
    Log.getLogWriter().info("Grepping for issues");
    String jtests = System.getProperty("JTESTS");
    String errs = null;
    try {
      errs = execute(jtests + "/hydratest/hydracheck/grep.sh");
    } catch (HydraRuntimeException e) {
      if (e.getMessage().indexOf("Command failed with exit code: 1") == -1) {
        throw e;
      } else {
        Log.getLogWriter().info(e.getMessage());
      }
    }
    String errlog = FileUtil.getText("errlog");
    if (errlog.length() > 0) {
      String s = "Found errors:\n" + errlog;
      throw new HydraRuntimeException(s);
    }
    Log.getLogWriter().info("Grepped for issues, found none");
  }
  private static String execute(String cmd) {
    Log.getLogWriter().info("Executing " + cmd);
    String result = ProcessMgr.fgexec(cmd, 300);
    Log.getLogWriter().info("Executed " + cmd);
    return result;
  }
}
