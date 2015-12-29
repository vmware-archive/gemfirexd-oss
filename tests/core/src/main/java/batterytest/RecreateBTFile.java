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
package batterytest;

import java.io.*;
import java.util.*;

/**
 * This program re-creates a <code>.bt</code> file based on the
 * configuration information (<code>latest.prop</code>, etc.) found in
 * the output directory of a previous test run.  This allows us to
 * easily re-run a Hydra test that was configured using Hydra system
 * properties in a battery test configuration file
 *
 * @author David Whitlock
 *
 * @since 3.2.1
 */
public class RecreateBTFile {
  private static final PrintStream out = System.out;
  private static final PrintStream err = System.err;

  /** 
   * Prints usage information about this program
   */
  private static void usage(String s) {
    err.println("\n** " + s + "\n");
    err.println("usage: batterytest.RecreateBTFile (dir)+");
    err.println("  dir     Directory containing battery test run");
    err.println("");
    err.println("Re-creates a .bt file based on the configutation " +
                "information found in a directory containing a " +
                "test run.  The generated .bt is printed to standard out.");
    err.println("");

    System.exit(1);
  }

  public static void main(String[] args) throws IOException {
    List dirs = new ArrayList();

    for (int i = 0; i < args.length; i++) {
      dirs.add(new File(args[i]));
    }

    if (dirs.isEmpty()) {
      usage("Missing directory");
    }

    for (Iterator iter = dirs.iterator(); iter.hasNext(); ) {
      File dir = (File) iter.next();
      if (!dir.isDirectory()) {
        usage("File " + dir + " is not a directory");
      }

      File latest = new File(dir, "latest.prop");
      if (!latest.exists()) {
        usage("Directory " + dir + " does not contain a test run");
      }

      Properties props = new Properties();
      props.load(new FileInputStream(latest));

      String testName = props.getProperty("TestName");
      if (testName == null) {
        err.println("No \"TestName\" property in " + latest);
        continue;
      }

      out.print(testName);
      out.print(" ");
      out.flush();

      for (Enumeration theEnum = props.propertyNames();
           theEnum.hasMoreElements(); ) {
        String key = (String) theEnum.nextElement();
        String search = "hydra.SystemProperties-";
        if (key.startsWith(search)) {
          out.print(key.substring(search.length()));
          out.print("=");
          out.print(props.getProperty(key));
          out.print(" ");
          out.flush();
        }
      }

      out.println("");
    }
  }

}
