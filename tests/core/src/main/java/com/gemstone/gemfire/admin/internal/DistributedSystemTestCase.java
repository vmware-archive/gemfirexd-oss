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

//import com.gemstone.gemfire.distributed.internal.DistributionConfig;
//import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.distributed.DistributedSystem;
//import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
//import com.gemstone.gemfire.admin.*;

import java.util.*;
import junit.framework.*;

/**
 * Provides common setUp and tearDown for testing the Admin API.
 *
 * @author Kirk Lund
 * @since 3.5
 */
public abstract class DistributedSystemTestCase extends TestCase {

  /** The DistributedSystem used for this test */
  //protected InternalDistributedSystem system;
  protected DistributedSystem system;

//  /** The Admin DistributedSystem used for this test */
//  private DistributedSystem distSystem;
  
  /**
   * Creates a new <code>DistributedSystemTestCase</code>
   */
  public DistributedSystemTestCase(String name) {
    super(name);
  }

  /**
   * Creates a "loner" <code>DistributedSystem</code> for this test.
   */
  public void setUp() throws Exception {
    super.setUp();
//     Properties props = new Properties();
//    this.system = (InternalDistributedSystem) 
//        InternalDistributedSystem.connect(props);
//     props.setProperty(DistributionConfig.MCAST_PORT_NAME, 
//         String.valueOf(AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS)));
    this.system = DistributedSystem.connect(defineProperties());
  }

  /**
   * Closes the "loner" <code>DistributedSystem</code>
   */
  public void tearDown() throws Exception {
    super.tearDown();
    if (this.system != null) {
      this.system.disconnect();
    }
    this.system = null;
  }

  /**
   * Defines the <code>Properties</code> used to connect to the distributed 
   * system.
   */
  protected Properties defineProperties() {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.setProperty("conserve-sockets", "true");
    return props;
  }
 
} 

