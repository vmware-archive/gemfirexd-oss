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

/*
 * This class flattens out other data structures into an easily
 * processed container for data we want in detailed test
 * failure reports.
 */
public class FailureReportingRow {
  private String testName;
  private String bugNum;
  private String bugState;
  private String bugPriority;
  private String failureDescription;

  public FailureReportingRow() {

  }
  public FailureReportingRow(String testName, String bugNum,
      String bugState, String bugPriority, String failureDescription) {
    super();
    this.testName = testName;
    this.bugNum = bugNum;
    this.bugState = bugState;
    this.bugPriority = bugPriority;
    this.failureDescription = failureDescription;
  }
  public String getTestName() {
    return testName;
  }
  public void setTestName(String testName) {
    this.testName = testName;
  }
  public String getBugNum() {
    return bugNum;
  }
  public void setBugNum(String bugNum) {
    this.bugNum = bugNum;
  }
  public String getBugState() {
    return bugState;
  }
  public void setBugState(String bugState) {
    this.bugState = bugState;
  }
  public String getBugPriority() {
    return bugPriority;
  }
  public void setBugPriority(String bugPriority) {
    this.bugPriority = bugPriority;
  }
  public String getFailureDescription() {
    return failureDescription;
  }
  public void setFailureDescription(String failureDescription) {
    this.failureDescription = failureDescription;
  }

}
