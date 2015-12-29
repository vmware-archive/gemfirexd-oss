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
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Vector;
import java.util.regex.Pattern;

public class GFXDKnownFailures {

  Vector<GFXDKnownFailure> knownFailures;
  FilenameFilter kfDetailFileFilter = new GFXDKnownFailureFileFilter();
  File kfCsvFile = null;
  String dirContainingKnownFailureDetailFiles = null;
  //HashMap<String, String> cachedFileContents = new HashMap<String, String>();
  static final String UNIX_EOL = "\n";

  public GFXDKnownFailures(File kfCsvFile, String dirContainingKnownFailureDetailFiles) {
    this.kfCsvFile = kfCsvFile;
    this.dirContainingKnownFailureDetailFiles = dirContainingKnownFailureDetailFiles;
    File kfDetailDir = new File(dirContainingKnownFailureDetailFiles);
    knownFailures = GFXDKnownFailureIndexFile.loadFromFile(kfCsvFile, kfDetailDir);
    verifyKnownFailureDetailFiles();
  }

  /*
   * Data integrity checks on known failures CSV file
   */
  private void verifyKnownFailureDetailFiles() {
    for (GFXDKnownFailure kf : knownFailures) {

      // Make sure the type string is valid
      int kfTypeI = 0;
      for (kfTypeI=0; kfTypeI<GFXDKnownFailure.KF_VALID_TYPES.length; kfTypeI++) {
        if (kf.getKnownFailureType().equals(GFXDKnownFailure.KF_VALID_TYPES[kfTypeI])) {
          break;
        }
      }
      if (kfTypeI == GFXDKnownFailure.KF_VALID_TYPES.length) {
        System.err.println("Unknown failure type "+kf.getKnownFailureType()+" on "+kf.getTestName());
      }

      File dF = kf.getDetailFile();
      if (dF == null) {
        String kfType = kf.getKnownFailureType();
        if (kfType.equals(GFXDKnownFailure.KF_TRACEBACK_CONTAINS_TEXT)
            || kfType.equals(GFXDKnownFailure.KF_TRACEBACK_MATCHES_REGEX)
            || kfType.equals(GFXDKnownFailure.KF_ERRMSG_CONTAINS_TEXT)
            || kfType.equals(GFXDKnownFailure.KF_ERRMSG_MATCHES_REGEX)) {
          // The known failure should have a failure file associated with it.
          System.err.println("Data error: knownFailure " + kf.getTestName() + " in "
              + kfCsvFile.getAbsolutePath() + " does not have a corresponding failure file");
        }
      } else {
        if (!dF.exists()) {
          System.err.println("Cannot find " + dF.getAbsolutePath() + " for " + kf.getTestName()
              + " cited in " + kfCsvFile.getAbsolutePath());
        } else {
          if (!kf.getDetailFile().canRead()) {
            System.err.println("Cannot read " + dF.getAbsolutePath() + " for " + kf.getTestName()
                + " cited in " + kfCsvFile.getAbsolutePath());
          }
        }
      }
    }
  }

  public String stripExt(String fileName) {
    String nameMinusExt = fileName;
    if (fileName.lastIndexOf(".") > 0) {
      nameMinusExt = fileName.substring(0, fileName.lastIndexOf("."));
    }
    return nameMinusExt;
  }

  // Return a list of the known failures that COULD match a test failure
  public Vector<GFXDKnownFailure> getEligibleKnownFailures(String testName) {
    Vector<GFXDKnownFailure> retVec = new Vector<GFXDKnownFailure>();
    for (GFXDKnownFailure kf : knownFailures) {
      if ((kf.getTestName().equals("*") || Pattern.matches(kf.getTestName(), testName))) {
        retVec.add(kf);
      }
    }
    return retVec;
  }

  // Might want a version of this that converts line endings to unix eol?
  public String readFileIntoString(File f) throws FileNotFoundException, IOException {
    StringBuffer sb = new StringBuffer();
    BufferedReader reader = new BufferedReader(new FileReader(f));
    char[] buf = new char[1024];
    int numRead = 0;
    while ((numRead = reader.read(buf)) != -1) {
      String readData = String.valueOf(buf, 0, numRead);
      sb.append(readData);
      buf = new char[1024];
    }
    reader.close();
    return sb.toString();

  }

  public String readFileIntoString(File f, boolean trimSpacing, String eol)
      throws FileNotFoundException, IOException {
    StringBuffer sb = new StringBuffer();
    BufferedReader reader = new BufferedReader(new FileReader(f));
    String inpLine;
    while ((inpLine = reader.readLine()) != null) {
      if (trimSpacing) {
        sb.append(inpLine.trim() + eol);
      } else {
        sb.append(inpLine + eol);
      }
    }
    reader.close();
    return sb.toString();

  }

  public boolean fileContains(String searchString, File f) throws IOException {
    String fileContents = readFileIntoString(f);
    if (fileContents.indexOf(searchString) >= 0) {
      return true;
    }
    return false;
  }

  public GFXDKnownFailure matchKnownFailure(JUnitResult junitFailure, String testName)
      throws IOException, FileNotFoundException {
    Vector<GFXDKnownFailure> eligibleKFs = getEligibleKnownFailures(testName);
    for (GFXDKnownFailure kf : eligibleKFs) {
      String kfType = kf.getKnownFailureType();
      if (kfType.equals(GFXDKnownFailure.KF_NO_COMPARE)) {
        // The match on the known failure testcase name is considered good
        // enough for correlation of a test failure to a known failure.
        return kf;
      } else if (kfType.equals(GFXDKnownFailure.KF_TEST_ISSUE)){
        return kf;
      } else if (kfType.equals(GFXDKnownFailure.KF_TRACEBACK_CONTAINS_TEXT)
          || kfType.equals(GFXDKnownFailure.KF_TRACEBACK_MATCHES_REGEX)
          || kfType.equals(GFXDKnownFailure.KF_ERRMSG_CONTAINS_TEXT)
          || kfType.equals(GFXDKnownFailure.KF_ERRMSG_MATCHES_REGEX)) {
        // The known failure should have a failure file associated with it.
        if (kf.getDetailFile() != null) {
          // IMPROVE_ME: some of the detail files should be cached...
          String kfContents;
          try {
            kfContents = readFileIntoString(kf.getDetailFile(), true, UNIX_EOL);
          } catch (IOException ioe) {
            System.err.println("Error reading file "+kf.getDetailFile());
            throw ioe;
          }
          // if last char is an eol, remove it for looser comparisons
          if (kfContents.endsWith(UNIX_EOL)) {
            kfContents = kfContents.substring(0, kfContents.length() - 1);
          }

          boolean kfMatch = false;
          if (kfType.equals(GFXDKnownFailure.KF_TRACEBACK_CONTAINS_TEXT)) {
            kfMatch = (junitFailure.getTraceback().indexOf(kfContents) >= 0);
          } else if (kfType.equals(GFXDKnownFailure.KF_TRACEBACK_MATCHES_REGEX)) {
            kfMatch = Pattern.matches(kfContents, junitFailure.getTraceback());
          } else if (kfType.equals(GFXDKnownFailure.KF_ERRMSG_CONTAINS_TEXT)){
            kfMatch = (junitFailure.getErrString().indexOf(kfContents) >= 0);
          } else if (kfType.equals(GFXDKnownFailure.KF_ERRMSG_MATCHES_REGEX)){
            kfMatch = Pattern.matches(kfContents, junitFailure.getErrString());
          }
          if (kfMatch) {
            //System.out.println("junit failure " + junitFailure.getScopedTestcaseName()
            //    + " matches known failure " + kf.getDetailFile().getAbsolutePath());
            return kf;
          }
        }
      }
    }
    return null;
  }

}
