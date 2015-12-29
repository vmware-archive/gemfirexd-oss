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

import java.io.File;

/**
 * Tests the functionality of the DataPersistence quickstart example.
 *
 * @author Kirk Lund
 * @since 4.1.1
 */
public class DataPersistenceTest extends QuickstartTestCase {

  protected ProcessWrapper process;
  
  public DataPersistenceTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    if (this.process != null) this.process.destroy();
  }
  
  public void testDataPersistence1() throws Exception {
    File persistDir = new File("persistData1");
    persistDir.mkdir();
    
    // start up DataPersistence
    getLogWriter().info("[testDataPersistence1] start up DataPersistence");
    this.process = new ProcessWrapper(DataPersistence.class);
    this.process.execute(createProperties());

    getLogWriter().info("[testDataPersistence1] joining to DataPersistence");
    this.process.waitFor();
    printProcessOutput(this.process, "testDataPersistence1");
    
    // validate output from process
    assertOutputMatchesGoldenFile(this.process.getOutput(), "DataPersistence1.txt");
  }
 
  public void testDataPersistence2() throws Exception {
    // start up DataPersistence
    getLogWriter().info("[testDataPersistence2] start up DataPersistence");
    this.process = new ProcessWrapper(DataPersistence.class);
    this.process.execute(createProperties());

    getLogWriter().info("[testDataPersistence2] joining to DataPersistence");
    this.process.waitFor();
    printProcessOutput(this.process, "testDataPersistence2");
    
    // validate output from process
    assertOutputMatchesGoldenFile(this.process.getOutput(), "DataPersistence2.txt");
  }
}
