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
package com.pivotal.gemfirexd.dbsync;


public class ParallelDBSynchronizerDUnit extends DBSynchronizerTestBase {

  public ParallelDBSynchronizerDUnit(String name) {
    super(name);
  }

  /**
   * Create configuration for Parallel AsyncEventListener
   */
  protected Runnable createAsyncQueueConfigurationForBasicTests(String derbyUrl) {
    return getExecutorForWBCLConfiguration("SG1", "WBCL1",
        "com.pivotal.gemfirexd.callbacks.DBSynchronizer",
        "org.apache.derby.jdbc.ClientDriver", derbyUrl, true,
        Integer.valueOf(1), null, Boolean.FALSE, null, null, null, 100000,
        "org.apache.derby.jdbc.ClientDriver," + derbyUrl, true);
  }

  
  public void testDUMMY() throws Exception {
    
  }
}
