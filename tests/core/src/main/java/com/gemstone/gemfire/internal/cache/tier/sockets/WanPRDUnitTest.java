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
package com.gemstone.gemfire.internal.cache.tier.sockets;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;

import dunit.Host;

/** this class tests WAN functionality with a PartitionedRegion */
public class WanPRDUnitTest extends WanDUnitTest
{

  public static void createImpl() {
    impl = new WanPRDUnitTest("temp");
  }

  public WanPRDUnitTest(String name) {
    super(name);
  }
  
  public void setUp() throws Exception
  {
    disconnectAllFromDS();
    pause(5000);
    final Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
    vm3 = host.getVM(3);
    // use an instance to control inheritable aspects of the test
    vm0.invoke(getClass(), "createImpl", null);
    vm1.invoke(getClass(), "createImpl", null);
    vm2.invoke(getClass(), "createImpl", null);
    vm3.invoke(getClass(), "createImpl", null);
  }

  protected AttributesFactory getServerCacheAttributesFactory()
  {
    AttributesFactory factory = new AttributesFactory();
    factory.setPartitionAttributes((new PartitionAttributesFactory()).setTotalNumBuckets(1).create());
    return factory;
  }
}
