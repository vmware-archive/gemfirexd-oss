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
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.GatewayHub;
import com.gemstone.gemfire.cache30.GatewayDUnitTest;

@SuppressWarnings("serial")
public class GatewayParallelThreadDUnitTest extends GatewayDUnitTest {

  public GatewayParallelThreadDUnitTest(String name) {
    super(name);
  }
  
  protected Gateway addGateway(GatewayHub hub, String gatewayName) {
    Gateway gateway = hub.addGateway(gatewayName, 10);
    gateway.setOrderPolicy(Gateway.OrderPolicy.THREAD);
    return gateway;
  }
}
