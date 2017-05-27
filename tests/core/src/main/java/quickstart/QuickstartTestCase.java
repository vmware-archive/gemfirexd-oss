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
package quickstart;

import java.io.IOException;
import java.util.Properties;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.internal.AvailablePort;
import junit.framework.TestCase;

/**
 * The abstract superclass of tests that need to process output from the
 * quickstart examples.
 *
 * @author Kirk Lund
 * @since 4.1.1
 */
public abstract class QuickstartTestCase extends TestCase {

  private final int mcastPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);

  public QuickstartTestCase(String name) {
    super(name);
    if (ProcessWrapper.ENABLE_TRACING) {
      getLogWriter().info("quickstart.test.ENABLE_TRACING is " + ProcessWrapper.ENABLE_TRACING);
    }
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }
  
  /** 
   * Creates and returns a new GoldenComparator instance. Default implementation
   * is RegexGoldenComparator. Override if you need a different implementation
   * such as StringGoldenComparator.
   */
  protected GoldenComparator createGoldenComparator() {
    return new RegexGoldenComparator(expectedProblemLines());
  }
  
  /**
   * Returns an array of expected problem strings. Without overriding this,
   * any line with warning, error or severe will cause the test to fail. By
   * default, null is returned.
   * 
   *(see PartitionedRegionTest which expects a WARNING log message) 
   */
  protected String[] expectedProblemLines() {
    return null;
  }
  
  protected void assertOutputMatchesGoldenFile(String actualOutput, String goldenFileName) throws IOException {
    GoldenComparator comparator = createGoldenComparator();
    comparator.assertOutputMatchesGoldenFile(actualOutput, goldenFileName);
  }

  protected final void assertOutputMatchesGoldenFile(ProcessWrapper process, String goldenFileName) throws IOException {
    GoldenComparator comparator = createGoldenComparator();
    comparator.assertOutputMatchesGoldenFile(process.getOutput(), goldenFileName);
  }

  protected static LogWriter getLogWriter() {
    return ProcessWrapper.getLogWriter();
  }
  
  protected static void trace(String message) {
    ProcessWrapper.trace(message);
  }
  
  protected Properties createProperties() {
    Properties properties = new Properties();
    properties.setProperty("gemfire.mcast-port", String.valueOf(this.mcastPort));
    properties.setProperty("gemfire.log-level", "warning");
    properties.setProperty("file.encoding", "UTF-8");
    return properties;
  }
  
  protected final int getMcastPort() {
    return this.mcastPort;
  }
  
  // TODO: get rid of this to tighten up tests
  protected final void sleep(long millis) throws InterruptedException {
    Thread.sleep(millis);
  }
  
  protected final void printProcessOutput(ProcessWrapper process) {
    innerPrintOutput(process.getOutput(), "OUTPUT");
  }
  
  protected final void printProcessOutput(ProcessWrapper process, String banner) {
    innerPrintOutput(process.getOutput(), banner);
  }
  
  protected final void innerPrintOutput(String output, String title) {
    System.out.println("------------------ BEGIN " + title + " ------------------");
    System.out.println(output);
    System.out.println("------------------- END " + title + " -------------------");
  }
}
