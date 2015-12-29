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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Vector;

public class GFXDKnownFailureIndexFile {
  private static GFXDKnownFailure parseLineIntoKF(String csvLine, File detailDirRoot) {
    GFXDKnownFailure kf = new GFXDKnownFailure();
    // IMPROVE_ME: dumb implementation that won't handle
    // commas inside quotes.
    // Expected format:
    // testname,testDescription,ticket,failureDescription,knownFailureType,detailRelPath
    String line[] = csvLine.split(",",6);
    if (line.length != 6) {
      System.err.println("Error parsing GFXDKnownFailure line: "+csvLine+", IGNORING...");
      return null;   // indicates parsing error
    }
    kf.setTestName(line[0].trim());
    kf.setTestDescription(line[1].trim());
    kf.setBug(line[2].trim());
    kf.setFailureDescription(line[3].trim());
    kf.setKnownFailureType(line[4].trim());
    kf.setDetailFile(new File(detailDirRoot.getAbsolutePath()+"/"+line[5].trim()));
    kf.dump();
    return kf;
  }

  public static Vector<GFXDKnownFailure> loadFromFile(File kfCsvFile, File detailDirRoot) {
    Vector<GFXDKnownFailure> retVec = new Vector<GFXDKnownFailure>();
    BufferedReader readFile = null;
    try {
      readFile = new BufferedReader(new FileReader(kfCsvFile));

      String aLine = readFile.readLine();
      // First line is header line, so skip...
      if (aLine != null && aLine.toLowerCase().startsWith("testname")) {
        aLine = readFile.readLine();
      }
      while (aLine!=null) {
        GFXDKnownFailure kf = parseLineIntoKF(aLine, detailDirRoot);
        if (kf != null) {
          retVec.add(kf);
        }
        aLine = readFile.readLine();
      }
      readFile.close();
    } catch (IOException ioe) {
      ioe.printStackTrace(System.out);
    }
    return retVec;
  }
}
