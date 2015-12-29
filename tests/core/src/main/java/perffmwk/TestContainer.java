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
import java.io.File;
import java.util.*;

/**
 * Represents a test container directory.
 */
public class TestContainer implements ComparisonConstants {

  /** Directory name of this container. */
  private String testContainerDir;

  /** Tests in this container. */
  private List tests;

  /**
   * Creates a test container for the directory, omitting failed tests upon
   * request.
   */
  public TestContainer(String testContainerDir, boolean omitFailedTests,
                                                boolean compareByKey,
                                                boolean enforceConsistency) {

    // find the container directory
    if (!FileUtil.exists(testContainerDir)) {
      String s = "Directory not found: " + testContainerDir;
      throw new PerfComparisonException(s);
    }
    this.testContainerDir = testContainerDir;

    // find the tests in the container
    this.tests = new ArrayList();
    List testDirs = TestFileUtil.getTestDirectories(testContainerDir, false);
    if (testDirs.size() == 0) {
      String s = "Directory contains no test results: " + testContainerDir;
      throw new PerfComparisonException(s);
    }
    for (Iterator i = testDirs.iterator(); i.hasNext();) {
      String testDir = ((File)i.next()).toString();
      Test test = new Test(testDir, compareByKey);
      String testResult = test.getTestResult();
      if (testResult.equals(BOGUS)) {
        String s = "Omitting " + testResult + " test: " + testDir;
        Log.getLogWriter().warning(s);
      } else if (test.getTestResult().equals(PASS) || !omitFailedTests) {
        this.tests.add(test);
      } else {
        String s = "Omitting " + testResult + " test: " + testDir;
        Log.getLogWriter().warning(s);
      }
    }
    Collections.sort(this.tests);
    // complain if there are no tests
    if (this.tests.size() == 0) {
      String s = "Directory contains no passing tests: " + testContainerDir;
      throw new PerfComparisonException(s);
    }

    // make sure all of the tests use the same software versions
    if (enforceConsistency) {
      enforceBuildVersion();
      enforceNativeClientBuildVersion();
      enforceNativeClientSourceVersion();
      enforceBuildJDK();
      enforceRuntimeJDK();
    }
  }

  /**
   * Returns the directory name for this container.
   */
  public String getTestContainerDir() {
    return this.testContainerDir;
  }

  /** Returns the tests in this container. */
  public List getTests() {
    return this.tests;
  }

  /**
   * Returns a sorted set of property keys used by the tests in this container.
   */
  public SortedSet getTestPropertyKeys() {
    SortedSet keys = new TreeSet();
    for (Iterator i = this.tests.iterator(); i.hasNext();) {
      Test test = (Test)i.next();
      keys.addAll(test.getTestProperties().keySet());
    }
    return keys;
  }

  /**
   * Returns ids for the tests in this container.
   */
  public List getTestIds() {
    List testIds = new ArrayList();
    for (Iterator i = this.tests.iterator(); i.hasNext();) {
      Test test = (Test)i.next();
      String testId = test.getTestId();
      if (testId != null) {
        testIds.add(testId);
      }
    }
    return testIds;
  }

  /** Returns the test dirs in this container with the given test id. */
  public List getTestDirsWithId(String testId) {
    List testDirs = new ArrayList();
    for (Iterator i = this.tests.iterator(); i.hasNext();) {
      Test test = (Test)i.next();
      if (test.getTestId().equals(testId)) {
        testDirs.add(test.getTestDir());
      }
    }
    return testDirs;
  }

  /** Returns the tests in this container with the given test id. */
  public List getTestsWithId(String testId) {
    List testsWithId = new ArrayList();
    for (Iterator i = this.tests.iterator(); i.hasNext();) {
      Test test = (Test)i.next();
      if (test.getTestId().equals(testId)) {
        testsWithId.add(test);
      }
    }
    return testsWithId;
  }

  /**
   * Returns whether this container holds native client tests.
   */
  public boolean isNativeClient() {
    // just return the first one since they're all the same
    Test test = (Test)this.tests.iterator().next();
    return test.getStatConfig().isNativeClient();
  }

  /**
   * Throws an exception if the tests in this container do not all use the
   * same build version.
   */
  private void enforceBuildVersion() {
    String lastBuildVersion = null;
    for (Iterator i = this.tests.iterator(); i.hasNext();) {
      Test test = (Test)i.next();
      String currentBuildVersion = test.getBuildVersion();
      if (currentBuildVersion != null) {
        if (lastBuildVersion == null) {
          lastBuildVersion = currentBuildVersion;
        } else if (!same(currentBuildVersion, lastBuildVersion)) {
          String s = "Directory " + this.testContainerDir
                   + " contains tests that use different build versions: "
                   + lastBuildVersion + " and " + currentBuildVersion;
          throw new PerfComparisonException(s);
        }
      }
    }
  }

  /**
   * Throws an exception if the tests in this container do not all use the
   * same native client build version.
   */
  private void enforceNativeClientBuildVersion() {
    String lastNativeClientBuildVersion = null;
    for (Iterator i = this.tests.iterator(); i.hasNext();) {
      Test test = (Test)i.next();
      String currentNativeClientBuildVersion =
                                test.getNativeClientBuildVersion();
      if (currentNativeClientBuildVersion != null) {
        if (lastNativeClientBuildVersion == null) {
          lastNativeClientBuildVersion = currentNativeClientBuildVersion;
        } else if (!same(currentNativeClientBuildVersion,
                         lastNativeClientBuildVersion)) {
          String s = "Directory " + this.testContainerDir
                   + " contains tests that use different build versions: "
                   + lastNativeClientBuildVersion + " and "
                   + currentNativeClientBuildVersion;
          throw new PerfComparisonException(s);
        }
      }
    }
  }

  /**
   * Throws an exception if the tests in this container do not all use the
   * same native client source version.
   */
  private void enforceNativeClientSourceVersion() {
    String lastNativeClientSourceVersion = null;
    for (Iterator i = this.tests.iterator(); i.hasNext();) {
      Test test = (Test)i.next();
      String currentNativeClientSourceVersion =
                                test.getNativeClientSourceVersion();
      if (currentNativeClientSourceVersion != null) {
        if (lastNativeClientSourceVersion == null) {
          lastNativeClientSourceVersion = currentNativeClientSourceVersion;
        } else if (!same(currentNativeClientSourceVersion,
                         lastNativeClientSourceVersion)) {
          String s = "Directory " + this.testContainerDir
                   + " contains tests that use different source versions: "
                   + lastNativeClientSourceVersion + " and "
                   + currentNativeClientSourceVersion;
          throw new PerfComparisonException(s);
        }
      }
    }
  }

  /**
   * Returns the source version used by all tests in this container.
   */
  public String getSourceVersion() {
    // just return the first one since they're all the same
    Test test = (Test)this.tests.iterator().next();
    return test.getSourceVersion();
  }

  /**
   * Returns the native client source version used by all tests in this
   * container, null if no native client was used.
   */
  public String getNativeClientSourceVersion() {
    // just return the first one since they're all the same
    Test test = (Test)this.tests.iterator().next();
    return test.getNativeClientSourceVersion();
  }

  /**
   * Returns the build version used by all tests in this container.
   */
  public String getBuildVersion() {
    // just return the first one since they're all the same
    for (Iterator i = this.tests.iterator(); i.hasNext();) {
      Test test = (Test)i.next();
      String firstBuildVersion = test.getBuildVersion();
      if (firstBuildVersion != null) {
        return firstBuildVersion;
      }
    }
    String s = "No build version available in " + this.testContainerDir;
    throw new PerfComparisonException(s);
  }

  /**
   * Returns the native client build version used by all tests in this
   * container, or null if no native client was used.
   */
  public String getNativeClientBuildVersion() {
    // just return the first one since they're all the same
    for (Iterator i = this.tests.iterator(); i.hasNext();) {
      Test test = (Test)i.next();
      String firstNativeClientBuildVersion = test.getNativeClientBuildVersion();
      if (firstNativeClientBuildVersion != null) {
        return firstNativeClientBuildVersion;
      }
    }
    String s = "No build version available in " + this.testContainerDir;
    throw new PerfComparisonException(s);
  }

  /**
   * Throws an exception if the tests in this container do not all use the
   * same build JDK.
   */
  private void enforceBuildJDK() {
    String lastBuildJDK = null;
    for (Iterator i = this.tests.iterator(); i.hasNext();) {
      Test test = (Test)i.next();
      String currentBuildJDK = test.getBuildJDK();
      if (currentBuildJDK != null) {
        if (lastBuildJDK == null) {
          lastBuildJDK = currentBuildJDK;
        } else if (!same(currentBuildJDK, lastBuildJDK)) {
          String s = "Directory " + this.testContainerDir
                   + " contains tests that use different build JDKs: "
                   + lastBuildJDK + " and " + currentBuildJDK;
          throw new PerfComparisonException(s);
        }
      }
    }
  }

  /**
   * Returns the build JDK version used by all tests in this container.
   */
  public String getBuildJDK() {
    // just return the first one since they're all the same
    for (Iterator i = this.tests.iterator(); i.hasNext();) {
      Test test = (Test)i.next();
      String buildJDK = test.getBuildJDK();
      if (buildJDK != null) {
        return buildJDK;
      }
    }
    String s = "No build JDK available in " + this.testContainerDir;
    throw new PerfComparisonException(s);
  }

  /**
   * Throws an exception if the tests in this container do not all use the
   * same runtime JDK (or Java VM name).
   */
  private void enforceRuntimeJDK() {
    String lastRuntimeJDK = null;
    String lastJavaVMName = null;
    for (Iterator i = this.tests.iterator(); i.hasNext();) {
      Test test = (Test)i.next();
      String currentRuntimeJDK = test.getRuntimeJDK();
      String currentJavaVMName = test.getJavaVMName();
      if (currentRuntimeJDK != null) {
        if (lastRuntimeJDK == null) {
          lastRuntimeJDK = currentRuntimeJDK;
          lastJavaVMName = currentJavaVMName;
        } else {
          if (!same(currentRuntimeJDK, lastRuntimeJDK)) {
            String s = "Directory " + this.testContainerDir
                     + " contains tests that use different runtime JDKs: "
                     + lastRuntimeJDK + " and " + currentRuntimeJDK;
            throw new PerfComparisonException(s);
          }
          if (!same(currentJavaVMName, lastJavaVMName)) {
            String s = "Directory " + this.testContainerDir
                     + " contains tests that use different Java VM names: "
                     + lastJavaVMName + " and " + currentJavaVMName;
            throw new PerfComparisonException(s);
          }
        }
      }
    }
  }

  /**
   * Returns the runtime JDK version used by all tests in this container.
   */
  public String getRuntimeJDK() {
    // just return the first one since they're all the same
    for (Iterator i = this.tests.iterator(); i.hasNext();) {
      Test test = (Test)i.next();
      String runtimeJDK = test.getRuntimeJDK();
      if (runtimeJDK != null) {
        return runtimeJDK;
      }
    }
    String s = "No runtime JDK available in " + this.testContainerDir;
    throw new PerfComparisonException(s);
  }

  /**
   * Returns the Java VM name used by all tests in this container.
   */
  public String getJavaVMName() {
    // just return the first one since they're all the same
    for (Iterator i = this.tests.iterator(); i.hasNext();) {
      Test test = (Test)i.next();
      String javaVMName = test.getJavaVMName();
      if (javaVMName != null) {
        return javaVMName;
      }
    }
    return null;
  }

  private boolean same(String s, String t) {
    if (s == null && t == null) return true;
    if (s == null && t != null) return false;
    if (s != null && t == null) return false;
    return s.equals(t);
  }

  /**
   * Returns some info about this container.
   */
  public String toString() {
    return "\n" + this.testContainerDir + "\nTests: " + this.tests;
  }
}
