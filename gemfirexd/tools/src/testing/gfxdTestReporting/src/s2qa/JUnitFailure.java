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

public class JUnitFailure {
  private String testcaseName;
  private String scopedTestcaseName;
  private String testclassName;
  private String errString;
  private StringBuffer traceback;
  private boolean inFailureContext;
  private int originalIndexNumber;
  private File fromFile;
  private GFXDKnownFailure associatedKnownFailure;
  public JUnitFailure() {

  }
  public JUnitFailure(String testcaseName, String scopedTestcaseName,
      String testclassName, String errString, StringBuffer traceback,
      boolean inFailureContext, int originalIndexNumber, File fromFile) {
    super();
    this.testcaseName = testcaseName;
    this.scopedTestcaseName = scopedTestcaseName;
    this.testclassName = testclassName;
    this.errString = errString;
    this.traceback = traceback;
    this.inFailureContext = inFailureContext;
    this.originalIndexNumber = originalIndexNumber;
    this.fromFile = fromFile;
  }
  public String getTestcaseName() {
    return testcaseName;
  }
  public void setTestcaseName(String testcaseName) {
    this.testcaseName = testcaseName;
  }
  public String getScopedTestcaseName() {
    return scopedTestcaseName;
  }
  public void setScopedTestcaseName(String scopedTestcaseName) {
    this.scopedTestcaseName = scopedTestcaseName;
  }
  public String getTestclassName() {
    return testclassName;
  }
  public void setTestclassName(String testclassName) {
    this.testclassName = testclassName;
  }
  public String getErrString() {
    return errString;
  }
  public void setErrString(String errString) {
    this.errString = errString;
  }
  public StringBuffer getTraceback() {
    return traceback;
  }
  public void setTraceback(StringBuffer traceback) {
    this.traceback = traceback;
  }
  public void setInFailureContext(boolean inFailureContext) {
    this.inFailureContext = inFailureContext;
  }
  public boolean isInFailureContext() {
    return inFailureContext;
  }
  public int getOriginalIndexNumber() {
    return originalIndexNumber;
  }
  public void setOriginalIndexNumber(int originalIndexNumber) {
    this.originalIndexNumber = originalIndexNumber;
  }
  public void setFromFile(File fromFile) {
    this.fromFile = fromFile;
  }
  public File getFromFile() {
    return fromFile;
  }
  public GFXDKnownFailure getAssociatedKnownFailure() {
    return associatedKnownFailure;
  }
  public void setAssociatedKnownFailure(GFXDKnownFailure associatedKnownFailure) {
    this.associatedKnownFailure = associatedKnownFailure;
  }

}
