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

public class GFXDKnownFailure {
  public static final String KF_TRACEBACK_CONTAINS_TEXT = "TRACEBACK_CONTAINS_TEXT"; // known failure matches .tmp file
  public static final String KF_TRACEBACK_MATCHES_REGEX = "TRACEBACK_MATCHES_REGEX";
  public static final String KF_ERRMSG_CONTAINS_TEXT = "ERRMSG_CONTAINS_TEXT";
  public static final String KF_ERRMSG_MATCHES_REGEX = "ERRMSG_MATCHES_REGEX";
  public static final String KF_UNSUPPORTED = "UNSUPPORTED"; // known failure due to unsupported
  public static final String KF_TEST_ISSUE = "TEST_ISSUE"; // known failure due to test issue
  public static final String KF_HANGS = "HANGS"; // known failure the test hangs (so skip)
  public static final String KF_NO_COMPARE = "NO_COMPARISON"; // just match to known failure, without any search of tracebacks
  public static final String[] KF_VALID_TYPES = { KF_TRACEBACK_CONTAINS_TEXT,
      KF_TRACEBACK_MATCHES_REGEX, KF_ERRMSG_CONTAINS_TEXT,
      KF_ERRMSG_MATCHES_REGEX,
      KF_UNSUPPORTED, KF_TEST_ISSUE, KF_HANGS, KF_NO_COMPARE };

  private String suiteName;
  private String testName;
  private String harness;
  private String gfxdRelease;
  private String testDescription;
  private String bug;
  private String failureDescription;
  private String knownFailureType;
  private String fileExt;
  private File detailFile; // for additional data on known failure

  public String getSuiteName() {
    return suiteName;
  }

  public void setSuiteName(String suiteName) {
    this.suiteName = suiteName;
  }

  public String getTestName() {
    return testName;
  }

  public void setTestName(String testName) {
    this.testName = testName;
  }

  public String getHarness() {
    return harness;
  }

  public void setHarness(String harness) {
    this.harness = harness;
  }

  public String getGfxdRelease() {
    return gfxdRelease;
  }

  public void setGfxdRelease(String gfxdRelease) {
    this.gfxdRelease = gfxdRelease;
  }

  public String getTestDescription() {
    return testDescription;
  }

  public void setTestDescription(String testDescription) {
    this.testDescription = testDescription;
  }

  public String getBug() {
    return bug;
  }

  public void setBug(String bug) {
    this.bug = bug;
  }

  public String getFailureDescription() {
    return failureDescription;
  }

  public void setFailureDescription(String failureDescription) {
    this.failureDescription = failureDescription;
  }

  public String getKnownFailureType() {
    return knownFailureType.toString();
  }

  public void setKnownFailureType(String knownFailureType) {
    this.knownFailureType = knownFailureType;
  }

  public void setFileExt(String fileExt) {
    this.fileExt = fileExt;
  }

  public String getFileExt() {
    return fileExt;
  }

  public File getDetailFile() {
    return detailFile;
  }

  public void setDetailFile(File detailFile) {
    this.detailFile = detailFile;
  }

  public void dump() {
    if (System.getProperty("verbose") != null) {
      System.out.println("testName: " + testName + ", testDescription: " + testDescription);
      System.out.println("  bug: " + bug + ", knownFailureType: " + knownFailureType);
      System.out.println("  detailFile: "
        + (detailFile == null ? "null" : detailFile.getAbsolutePath()));
      System.out.println("  failureDescription: " + failureDescription);
    }
  }

}
