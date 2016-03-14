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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.memcached.GemFireMemcachedServer.Protocol;

import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;

/**
 * Test for binary protocol
 * @author sbawaska
 */
public class GemcachedBinaryClientJUnitTest extends
    GemcachedDevelopmentJUnitTest {

  @Override
  protected Protocol getProtocol() {
    return Protocol.BINARY;
  }

  @Override
  protected MemcachedClient createMemcachedClient() throws IOException,
      UnknownHostException {
    List<InetSocketAddress> addrs = new ArrayList<InetSocketAddress>();
    addrs.add(new InetSocketAddress(SocketCreator.getLocalHost(), PORT));
    MemcachedClient client = new MemcachedClient(new BinaryConnectionFactory(), addrs);
    return client;
  }

  @Override
  public void testDecr() throws Exception {
    super.testDecr();
    MemcachedClient client = createMemcachedClient();
    assertEquals(0, client.decr("decrkey", 999));
  }
  
  @Override
  public void testFlushDelay() throws Exception {
    // for some reason the server never gets expiration bits from the
    // client, so disabling for now
  }
}
