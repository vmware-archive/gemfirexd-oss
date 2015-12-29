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
package com.gemstone.gemfire.admin.internal;

import com.gemstone.gemfire.internal.HostStatHelper;
import com.gemstone.gemfire.internal.GemFireStatSampler;
import com.gemstone.gemfire.internal.ProcessStats;
import com.gemstone.gemfire.internal.PureJavaMode;
import com.gemstone.gemfire.admin.*;
import java.util.*;
//import junit.framework.*;

/**
 * Contains simple tests for the {@link MemberHealthEvaluator}.
 *
 * @author David Whitlock
 *
 * @since 3.5
 */
public class MemberHealthEvaluatorTest
  extends HealthEvaluatorTestCase {

  ////////  Constructors

  /**
   * Creates a new <code>MemberHealthEvaluatorTest</code>
   */
  public MemberHealthEvaluatorTest(String name) {
    super(name);
  }

  ////////  Test Methods

  /**
   * Tests that we are in {@link GemFireHealth#OKAY_HEALTH okay}
   * health if the VM's process size is too big.
   *
   * @see MemberHealthEvaluator#checkVMProcessSize
   */
  public void testCheckVMProcessSize() throws InterruptedException {
    if(PureJavaMode.osStatsAreAvailable()) {
      GemFireStatSampler sampler = system.getStatSampler();
      assertNotNull(sampler);

      sampler.waitForInitialization(10000); // fix: remove infinite wait

      ProcessStats stats = sampler.getProcessStats();
      assertNotNull(stats);

      List status = new ArrayList();
      long threshold = stats.getProcessSize() * 2;

      if (threshold <= 0) {
        // The process size is zero on some Linux versions
        return;
      }

      GemFireHealthConfig config = new GemFireHealthConfigImpl(null);
      config.setMaxVMProcessSize(threshold);

      MemberHealthEvaluator eval =
        new MemberHealthEvaluator(config,
                                this.system.getDistributionManager());
      eval.evaluate(status);
      assertTrue(status.isEmpty());

      status = new ArrayList();
      long processSize = stats.getProcessSize();
      threshold = processSize / 2;
      assertTrue("Threshold (" + threshold + ") is > 0.  " +
                 "Process size is " + processSize,
                 threshold > 0);

      config = new GemFireHealthConfigImpl(null);
      config.setMaxVMProcessSize(threshold);

      eval = new MemberHealthEvaluator(config,
                                     this.system.getDistributionManager());
      eval.evaluate(status);
      assertEquals(1, status.size());

      AbstractHealthEvaluator.HealthStatus ill =
        (AbstractHealthEvaluator.HealthStatus) status.get(0);
      assertEquals(GemFireHealth.OKAY_HEALTH, ill.getHealthCode());
      assertTrue(ill.getDiagnosis().indexOf("The size of this VM") != -1);
    }
  }
}

