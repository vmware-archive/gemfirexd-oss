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
package com.pivotal.gemfirexd.query;

import io.snappydata.test.dunit.SerializableRunnable;

@SuppressWarnings("serial")
/*
 * Run all cases of SingleTablePredicatesCheckDUnit, for replicated tables.
 */
public class SingleTablePredicatesCheck4ReplicatedDUnit extends
    SingleTablePredicatesCheckDUnit {

  public SingleTablePredicatesCheck4ReplicatedDUnit(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    testRunForReplicatedTablesOnly = true;
    invokeInEveryVM(new SerializableRunnable() {

      @Override
      public void run() {
        testRunForReplicatedTablesOnly = true;
      }
    });
  }

  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
    testRunForReplicatedTablesOnly = false;
    invokeInEveryVM(new SerializableRunnable() {

      @Override
      public void run() {
        testRunForReplicatedTablesOnly = false;
      }
    });
  }

}
