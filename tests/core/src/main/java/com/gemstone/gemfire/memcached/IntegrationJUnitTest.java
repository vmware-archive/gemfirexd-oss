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
package com.gemstone.gemfire.memcached;

import java.net.InetSocketAddress;
import java.util.Properties;
import java.util.concurrent.Future;

import net.spy.memcached.MemcachedClient;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.SocketCreator;

import junit.framework.TestCase;

/**
 * 
 * @author sbawaska
 */
public class IntegrationJUnitTest extends TestCase {

  public void testGemFireProperty() throws Exception {
    Properties props = new Properties();
    final int port = AvailablePortHelper.getRandomAvailableTCPPort();
    props.setProperty("memcached-port", port+"");
    CacheFactory cf = new CacheFactory(props);
    Cache cache = cf.create();
    
    MemcachedClient client = new MemcachedClient(
        new InetSocketAddress(SocketCreator.getLocalHost(), port));
    Future<Boolean> f = client.add("key", 10, "myStringValue");
    assertTrue(f.get());
    Future<Boolean> f1 = client.add("key1", 10, "myStringValue1");
    assertTrue(f1.get());
    
    assertEquals("myStringValue", client.get("key"));
    assertEquals("myStringValue1", client.get("key1"));
    assertNull(client.get("nonExistentkey"));
    cache.close();
  }
}
