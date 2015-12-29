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

import java.io.FilenameFilter;
import java.io.File;

/**
Filter to only accept "known failure" files
Files should be either myTestName.tmp or myTestName.tmp.1, myTestName.tmp.2, etc.,
or myTestName.kf or myTestName.kf.1 or myTestName.kf.2 ...
*/
class GFXDKnownFailureFileFilter implements FilenameFilter {

  private String testNameMinusExt = null;

  public boolean accept(File dir, String name) {
    System.out.println("TMP_DEBUG: entered accept with testName=" + testNameMinusExt + ", dir=" + dir.getAbsolutePath()
        + ", name=" + name);
    // FIX_ME: restore this later after debuggging .kf files
    //if (name.matches(".*\\.tmp[0-9]*"))
    //  return true;
    // If the known failure file is testnameMinusExt.kf###, accept
    if (name.endsWith(".kf")) {
      System.out.println("TMP_DEBUG: temporary break, testNameMinusExt is: "+testNameMinusExt);
    }
    //if (testNameMinusExt == null || name.matches(testNameMinusExt+"\\.kf[0-9]*"))
    if (name.matches(".*\\.kf[0-9]*"))
      return true;
    // If the known failure file is sometext.all.kf###, accept
    //    e.g. bug1234.all.kf, meaning bug1234 known failure pattern
    //         can be applied to all tests
    if (name.matches(".*\\.all\\.kf[0-9]*"))
      return true;
    File f = new File(dir, name);
    if (f.isDirectory())
      return true;

    return false;
  }

  GFXDKnownFailureFileFilter(String testName) {
    // Strip directory and extension off the testname
    testNameMinusExt = testName;
    if (testName.lastIndexOf("/") > 0) {
      testNameMinusExt = testName.substring(testName.lastIndexOf("/"));
    }
    if (testNameMinusExt.lastIndexOf(".") > 0) {
      testNameMinusExt = testNameMinusExt.substring(0, testNameMinusExt.lastIndexOf("."));
    }
  }
  GFXDKnownFailureFileFilter() {
  }
}
