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

package perffmwk;

import hydra.*;
import java.util.*;

/**
 * Represents a test directory.
 */
public class Test implements ComparisonConstants, Comparable<Test> {

  private String testDir;
  private SortedMap testProps;
  private String testId;
  private String testDescription;
  private String testResult;
  private String statSpecFile;
  private StatConfig statConfig;
  private String sourceVersion;
  private String buildVersion;
  private String nativeClientSourceVersion;
  private String nativeClientBuildVersion;
  private String buildJDK;
  private String runtimeJDK;
  private String javaVMName;

  public Test(String testDir, boolean compareByKey) {
    this.testDir = testDir;
    this.statSpecFile = testDir + "/statistics.spec";
    this.statConfig = PerfReporter.getStatConfig(this.testDir,
                                                 this.statSpecFile);
    this.testResult = computeTestResult(this.testDir);
    if (this.statConfig != null) {
      SortedMap distilled = this.statConfig.distillStatSpecsForComparison();
      this.statConfig.setStatSpecs(distilled);
      this.testProps = computeTestProperties(this.statConfig);
      this.testId = computeTestId(this.testProps, compareByKey);
      this.testDescription = this.statConfig.getTestDescription();
      String svn = this.statConfig.getSourceRepository();
      if (svn == null) {
        this.sourceVersion = "not available";
      } else {
        this.sourceVersion = this.statConfig.getSourceRepository() + ":"
                           + this.statConfig.getSourceRevision()
                           + " (" + this.statConfig.getSourceDate() + ")";
      }
      this.buildVersion = this.statConfig.getBuildVersion();
      this.buildJDK = this.statConfig.getBuildJDK();
      this.runtimeJDK = this.statConfig.getRuntimeJDK();
      this.javaVMName = this.statConfig.getJavaVMName();
      if (this.statConfig.isNativeClient()) {
        svn = this.statConfig.getNativeClientSourceRepository();
        if (svn == null) {
          this.nativeClientSourceVersion = "not available";
        } else {
          this.nativeClientSourceVersion =
              this.statConfig.getNativeClientSourceRepository() + ":"
              + this.statConfig.getNativeClientSourceRevision();
        }
        this.nativeClientBuildVersion =
             this.statConfig.getNativeClientBuildVersion();
      }
    }
  }
  public String getTestDir() {
    return this.testDir;
  }
  public SortedMap getTestProperties() {
    return this.testProps;
  }
  public String getTestId() {
    return this.testId;
  }
  public String getTestDescription() {
    return this.testDescription;
  }
  public String getTestResult() {
    return this.testResult;
  }
  public String getStatSpecFile() {
    return this.statSpecFile;
  }
  public StatConfig getStatConfig() {
    return this.statConfig;
  }
  public void resetStatSpecs(String statSpecFile) {
    this.statSpecFile = statSpecFile;
    this.statConfig = PerfReporter.getStatConfig(this.testDir,
                                                 this.statSpecFile);
    SortedMap distilled = this.statConfig.distillStatSpecsForComparison();
    this.statConfig.setStatSpecs(distilled);
  }
  public SortedMap getStatSpecs() {
    return this.statConfig.getStatSpecs();
  }
  public String getSourceVersion() {
    return this.sourceVersion;
  }
  public String getBuildVersion() {
    return this.buildVersion;
  }
  public String getNativeClientSourceVersion() {
    return this.nativeClientSourceVersion;
  }
  public String getNativeClientBuildVersion() {
    return this.nativeClientBuildVersion;
  }
  public String getBuildJDK() {
    return this.buildJDK;
  }
  public String getRuntimeJDK() {
    return this.runtimeJDK;
  }
  public String getJavaVMName() {
    return this.javaVMName;
  }
  public String toString() {
    String s =
           "\ntestDir: " + this.testDir
         + "\ntestId: " + this.testId
         + "\ntestDescription: " + this.testDescription
         + "\ntestResult: " + this.testResult
         + "\nstatSpecFile: " + this.statSpecFile
         + "\nsourceVersion: " + this.sourceVersion
         + "\nbuildVersion: " + this.buildVersion
         + "\nbuildJDK: " + this.buildJDK
         + "\nruntimeJDK: " + this.runtimeJDK
         + "\njavaVMName: " + this.javaVMName;
    if (this.statConfig.isNativeClient()) {
      s += "\nnativeClientSourceVersion: " + this.nativeClientSourceVersion
         + "\nnativeClientBuildVersion: " + this.nativeClientBuildVersion;
    }
    return s;
  }

  /**
   * Returns a possibly empty sorted map of the test properties.
   */
  private SortedMap computeTestProperties(StatConfig statConfig) { 
    SortedMap sortedProps = new TreeMap();
    Properties testProps = statConfig.getTestProperties();
    if (testProps != null) {
      return new TreeMap(testProps);
    }
    return new TreeMap();
  }

  /**
   * Computes the test id consisting of the hydra configuration test name
   * followed by a sorted list of hydra configuration properties, if any.
   * Omits {@link PerfSorter#COMPARISON_KEY_PROP}.
   */
  private String computeTestId(SortedMap testProps, boolean compareByKey) {
    String testId = statConfig.getTestName();
    if (testProps != null) {
      if (compareByKey) {
        String key = (String)testProps.get(PerfSorter.COMPARISON_KEY_PROP);
        if (key == null) {
          String s = PerfSorter.COMPARISON_KEY_PROP + " not found";
          throw new PerfComparisonException(s);
        } else {
          testId = key;
        }
      } else {
        for (Iterator i = testProps.keySet().iterator(); i.hasNext();) {
           String key = (String)i.next();
           if (!key.equals("testName") &&
               !key.equals(PerfSorter.COMPARISON_KEY_PROP)) {
             testId += " " + key + "=" + testProps.get(key);
           }
        }
      }
    }
    return testId;
  }

  /**
   * Computes the test result, based on the presence tell-tale files.
   */
  private String computeTestResult(String testDir) {
    if (!FileUtil.exists(testDir + "/latest.prop")) {
      return BOGUS;
    } else if (FileUtil.exists(testDir + "/hang.txt")) {
      return HANG;
    } else if (FileUtil.exists(testDir + "/errors.txt")) {
      return FAIL;
    } else if (this.statConfig.isNativeClient()
           && FileUtil.exists(testDir + "/failure.txt")) {
      return FAIL;
    } else {
      return PASS;
    }
  }
  
  /* (non-Javadoc)
   * @see java.lang.Object#equals(java.lang.Object)
   */
  @Override
  public boolean equals(Object obj) {
    if (obj == null || !(obj instanceof Test)) {
      return false;
    }
    Test other = (Test)obj;
    return this.testDir.equals(other.testDir);
  }
  
  /* (non-Javadoc)
   * @see java.lang.Object#hashCode()
   */
  @Override
  public int hashCode() {
    return this.testDir.hashCode();
  }
  
  /* (non-Javadoc)
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  public int compareTo(Test o) {
    return this.testDir.compareTo(o.testDir);
  }
}
