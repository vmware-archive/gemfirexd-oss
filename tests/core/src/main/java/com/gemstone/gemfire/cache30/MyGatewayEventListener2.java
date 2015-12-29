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
package com.gemstone.gemfire.cache30;

import java.util.List;
import java.util.Properties;

import com.gemstone.gemfire.cache.util.GatewayEvent;
import com.gemstone.gemfire.cache.util.GatewayEventListener;
import com.gemstone.gemfire.internal.cache.xmlcache.Declarable2;

public class MyGatewayEventListener2 implements GatewayEventListener, Declarable2{

  public boolean processEvents(List<GatewayEvent> events) {
    // TODO Auto-generated method stub
    return false;
  }

  public void close() {
    // TODO Auto-generated method stub
  }

  public Properties getConfig() {
    // TODO Auto-generated method stub
    return null;
  }

  public void init(Properties props) {
    // TODO Auto-generated method stub
    
  }
}
