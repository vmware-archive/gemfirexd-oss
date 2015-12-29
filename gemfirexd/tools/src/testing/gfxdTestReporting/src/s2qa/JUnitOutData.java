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
public class JUnitOutData {
  private File junitOutfile;
  private ArrayList<JUnitFailure> failures = new ArrayList<JUnitFailure>();
//  private ArrayList<JUnitFailure> unknownFailures = new ArrayList<JUnitFailure>();
  private int runCount;
  private int failCount;
  private int errorCount;
  private String svnChangeNumber;
  private int knownFailureCount;
  private int testIssueCount;

  public ArrayList<JUnitFailure> getFailures() {
    return failures;
  }
  public void setFailures(ArrayList<JUnitFailure> failures) {
    this.failures = failures;
  }
  public int getRunCount() {
    return runCount;
  }
  public void setRunCount(int runCount) {
    this.runCount = runCount;
  }
  public int getFailCount() {
    return failCount;
  }
  public void setFailCount(int failCount) {
    this.failCount = failCount;
  }
  public int getErrorCount() {
    return errorCount;
  }
  public void setErrorCount(int errorCount) {
    this.errorCount = errorCount;
  }
  public String getSvnChangeNumber() {
    return svnChangeNumber;
  }
  public void setSvnChangeNumber(String svnChangeNumber) {
    this.svnChangeNumber = svnChangeNumber;

  }
  public void setJunitOutfile(File junitOutfile) {
    this.junitOutfile = junitOutfile;
  }
  public File getJunitOutfile() {
    return junitOutfile;
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
}
