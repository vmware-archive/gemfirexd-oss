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

import java.io.File;
import java.util.ArrayList;

/*
 * Data structure for holding parsed content of a junit output file.
 * Includes summary pass/fail, error details.
 */
public class JUnitResultData {
  private File junitResultfile;
  private ArrayList<JUnitResult> results = new ArrayList<JUnitResult>();
//  private ArrayList<JUnitResult> unknownFailures = new ArrayList<JUnitResult>();
  private Integer runCount;
  private Integer failCount;
  private Integer errorCount;
  private Integer passCount;
  private String svnChangeNumber;
  private int knownFailureCount;
  private int testIssueCount;

  public ArrayList<JUnitResult> getResults() {
    return results;
  }
  public void setResults(ArrayList<JUnitResult> results) {
    this.results = results;
  }
  public ArrayList<JUnitResult> getFailures() {
    ArrayList<JUnitResult> failedResults = new ArrayList<JUnitResult>();
    for (JUnitResult result : results) {
      if (result.isFailed()) {
        failedResults.add(result);
      }
    }
    return failedResults;
  }

  public void setFailures(ArrayList<JUnitResult> failures) {
    for (JUnitResult failure : failures) {
      // Make sure every result in the result list is marked as failed
      failure.setFailed(true);
    }
    this.results = failures;
  }

  public int getRunCount() {
    if (runCount != null) return runCount;
    if (results == null) return 0;
    if (failCount != null && errorCount != null && passCount != null) {
      return failCount+errorCount+passCount;
    } else {
      return results.size();
    }
  }
  public void setRunCount(int runCount) {
    this.runCount = runCount;
  }
  public void setRunCount(Integer runCount) {
    this.runCount = runCount;
  }
  public int getFailPlusErrorCount() {
    if (failCount != null && errorCount != null) return failCount + errorCount;
    if (results ==  null) return 0;
    return getFailures().size();
  }

  public int getFailCount() {
    if (failCount != null) return failCount;
    if (results == null) return 0;
    if (runCount != null && errorCount != null && passCount != null) {
      return runCount-(errorCount+passCount);
    } else {
      int numFails = 0;
      for (JUnitResult failedTest : getFailures()) {
        if (failedTest.isInFailureContext()) numFails++;
      }
      return numFails;
    }
  }
  public void setFailCount(int failCount) {
    this.failCount = failCount;
  }
  public void setFailCount(Integer failCount) {
    this.failCount = failCount;
  }
  public int getErrorCount() {
    if (errorCount != null) return errorCount;
    if (results == null) return 0;
    if (runCount != null && failCount != null && passCount != null) {
      return runCount-(failCount+passCount);
    } else {
      int numErrors = 0;
      for (JUnitResult failedTest : getFailures()) {
        if (!failedTest.isInFailureContext()) numErrors++;
      }
      return numErrors;
    }
  }
  public void setErrorCount(int errorCount) {
    this.errorCount = errorCount;
  }
  public void setErrorCount(Integer errorCount) {
    this.errorCount = errorCount;
  }
  public String getSvnChangeNumber() {
    return svnChangeNumber;
  }
  public void setSvnChangeNumber(String svnChangeNumber) {
    this.svnChangeNumber = svnChangeNumber;

  }
  public void setJunitResultfile(File junitOutfile) {
    this.junitResultfile = junitOutfile;
  }
  public File getJunitResultfile() {
    return junitResultfile;
  }
  public int getKnownFailureCount() {
    return knownFailureCount;
  }
  public void setKnownFailureCount(int knownFailureCount) {
    this.knownFailureCount = knownFailureCount;
  }
  public int getTestIssueCount() {
    return testIssueCount;
  }
  public void setTestIssueCount(int testIssueCount) {
    this.testIssueCount = testIssueCount;
  }
  public int getPassCount() {
    if (passCount != null) return passCount;
    if (results == null) return 0;
    if (runCount != null && failCount != null && errorCount != null) {
      return runCount-(failCount+errorCount);
    } else {
      return results.size() - getFailures().size();
    }
  }
  public void setPassCount(Integer passCount) {
    this.passCount = passCount;
  }
  public void setPassCount(int passCount) {
    this.passCount = passCount;
  }

}
