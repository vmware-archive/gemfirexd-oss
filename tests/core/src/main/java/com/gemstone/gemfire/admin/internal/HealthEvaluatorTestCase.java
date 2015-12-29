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

import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import java.util.Properties;
import junit.framework.*;

/**
 * Superclass of tests for the {@linkplain
 * com.gemstone.gemfire.admin.internal.AbstractHealthEvaluator health
 * evaluator} classes.
 *
 * @author David Whitlock
 *
 * @since 3.5
 */
public abstract class HealthEvaluatorTestCase extends TestCase {

  /** The DistributedSystem used for this test */
  protected InternalDistributedSystem system;

  ////////  Constructors

  /**
   * Creates a new <code>HealthEvaluatorTestCase</code>
   */
  public HealthEvaluatorTestCase(String name) {
    super(name);
  }

  ////////  Test lifecycle methods

  /**
   * Creates a "loner" <code>DistributedSystem</code> for this test.
   */
  public void setUp() {
    Properties props = getProperties();
    system = (InternalDistributedSystem)
      DistributedSystem.connect(props);
  }

  /**
   * Closes the "loner" <code>DistributedSystem</code>
   */
  public void tearDown() {
    if (this.system != null) {
      this.system.disconnect();
    }

    this.system = null;
  }

  /**
   * Creates the <code>Properties</code> objects used to connect to
   * the distributed system.
   */
  protected Properties getProperties() {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.setProperty("statistic-sampling-enabled", "true");

    return props;
  }

}
