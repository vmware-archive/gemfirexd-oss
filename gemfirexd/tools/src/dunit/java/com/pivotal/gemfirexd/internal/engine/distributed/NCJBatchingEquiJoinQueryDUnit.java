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
/**
 * 
 */
package com.pivotal.gemfirexd.internal.engine.distributed;

import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import io.snappydata.test.dunit.SerializableRunnable;

/**
 * Tests whether the statementID , connectionID etc are being passed correctly
 * or not
 * 
 * Copy of com.pivotal.gemfirexd.internal.engine.distributed.EquiJoinQueryDUnit
 * and adapted for NCJ.
 * 
 * Set Batching Size of 5 for every test. Using either
 * 
 * a) System.setProperty("ncj-batch-size", String.valueOf(5));,
 * 
 * OR
 * 
 * b) props.put("gemfirexd.ncj-batch-size", String.valueOf(5)); over connection
 * 
 * @author vivekb
 * @since 2.0
 * 
 */
@SuppressWarnings("serial")
public class NCJBatchingEquiJoinQueryDUnit extends NCJoinEquiJoinQueryDUnit {

  public NCJBatchingEquiJoinQueryDUnit(String name) {
    super(name);
  }
  
  @Override
  public void setUp() throws Exception {
    System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "true");
    System.setProperty("ncj-batch-size", String.valueOf(5));
    invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "true");
        System.setProperty("ncj-batch-size", String.valueOf(5));
      }
    });
    super.setUp();
  }

  @Override
  public void tearDown2() throws java.lang.Exception {
    System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "false");
    System.setProperty("ncj-batch-size", String.valueOf(0));
    invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "false");
        System.setProperty("ncj-batch-size", String.valueOf(0));
      }
    });
    super.tearDown2();
  }
}
