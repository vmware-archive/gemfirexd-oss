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
package com.gemstone.gemfire.cache.client.internal;

import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.ServerConnectivityException;
import com.gemstone.gemfire.cache.util.BridgeServer;
import com.gemstone.gemfire.cache.util.EndpointDoesNotExistException;
import com.gemstone.gemfire.cache.util.EndpointExistsException;
import com.gemstone.gemfire.cache.util.EndpointInUseException;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import io.snappydata.test.dunit.AvailablePortHelper;
import junit.framework.Assert;
import junit.framework.TestCase;

/**
 * @author dsmith
 *
 */
public class ConnectionPoolImplJUnitTest extends TestCase {
  static Properties props = new Properties();
  private Cache cache;
  static {
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
  }
  
  public void tearDown() {
    try {
      cache.close();
    } catch (Exception e) {
      // do nothing
    }
  }

  public void testDefaults() throws EndpointExistsException, EndpointDoesNotExistException, EndpointInUseException {
    cache = CacheFactory.create(DistributedSystem.connect(props));
    PoolFactory cpf = PoolManager.createFactory();
    cpf.addServer("localhost", 40907);

    PoolImpl pool = (PoolImpl) cpf.create("myfriendlypool");
    
    // check defaults
    Assert.assertEquals(PoolFactory.DEFAULT_FREE_CONNECTION_TIMEOUT, pool.getFreeConnectionTimeout());
    Assert.assertEquals(PoolFactory.DEFAULT_SOCKET_BUFFER_SIZE, pool.getSocketBufferSize());
    Assert.assertEquals(PoolFactory.DEFAULT_READ_TIMEOUT, pool.getReadTimeout());
    Assert.assertEquals(PoolFactory.DEFAULT_MIN_CONNECTIONS, pool.getMinConnections());
    Assert.assertEquals(PoolFactory.DEFAULT_MAX_CONNECTIONS, pool.getMaxConnections());
    Assert.assertEquals(PoolFactory.DEFAULT_RETRY_ATTEMPTS, pool.getRetryAttempts());
    Assert.assertEquals(PoolFactory.DEFAULT_IDLE_TIMEOUT, pool.getIdleTimeout());
    Assert.assertEquals(PoolFactory.DEFAULT_PING_INTERVAL, pool.getPingInterval());
    Assert.assertEquals(PoolFactory.DEFAULT_THREAD_LOCAL_CONNECTIONS, pool.getThreadLocalConnections());
    Assert.assertEquals(PoolFactory.DEFAULT_SUBSCRIPTION_ENABLED, pool.getSubscriptionEnabled());
    Assert.assertEquals(PoolFactory.DEFAULT_SUBSCRIPTION_REDUNDANCY, pool.getSubscriptionRedundancy());
    Assert.assertEquals(PoolFactory.DEFAULT_SUBSCRIPTION_MESSAGE_TRACKING_TIMEOUT, pool.getSubscriptionMessageTrackingTimeout());
    Assert.assertEquals(PoolFactory.DEFAULT_SUBSCRIPTION_ACK_INTERVAL, pool.getSubscriptionAckInterval());
    Assert.assertEquals(PoolFactory.DEFAULT_SERVER_GROUP, pool.getServerGroup());
    // check non default
    Assert.assertEquals("myfriendlypool", pool.getName());
    Assert.assertEquals(1, pool.getServers().size());
    Assert.assertEquals(0, pool.getLocators().size());
    {
      InetSocketAddress addr = (InetSocketAddress)pool.getServers().get(0);
      Assert.assertEquals(40907, addr.getPort());
      Assert.assertEquals("localhost", addr.getHostName());
    }
    
  }
  
  public void testProperties() throws EndpointExistsException, EndpointDoesNotExistException, EndpointInUseException {
    int readTimeout = 234234;
    cache = CacheFactory.create(DistributedSystem.connect(props));
    PoolFactory cpf = PoolManager.createFactory();
    cpf.addServer("localhost", 40907)
      .setReadTimeout(readTimeout)
      .setThreadLocalConnections(true);

    PoolImpl pool = (PoolImpl) cpf.create("myfriendlypool");

    // check non default
    Assert.assertEquals("myfriendlypool", pool.getName());
    Assert.assertEquals(readTimeout, pool.getReadTimeout());
    Assert.assertEquals(true, pool.getThreadLocalConnections());
    Assert.assertEquals(1, pool.getServers().size());
    Assert.assertEquals(0, pool.getLocators().size());
    {
      InetSocketAddress addr = (InetSocketAddress)pool.getServers().get(0);
      Assert.assertEquals(40907, addr.getPort());
      Assert.assertEquals("localhost", addr.getHostName());
    }
  }
  
  public void testCacheClose() throws Exception {
    cache = CacheFactory.create(DistributedSystem.connect(props));
    PoolFactory cpf = PoolManager.createFactory();
    cpf.addLocator("localhost", AvailablePortHelper.getRandomAvailableTCPPort());
    Pool pool1 = cpf.create("pool1");
    Pool pool2 = cpf.create("pool2");
    cache.close();
    
    Assert.assertTrue(pool1.isDestroyed());
    Assert.assertTrue(pool2.isDestroyed());
  }
  
  public void testExecuteOp() throws Exception {
    cache = CacheFactory.create(DistributedSystem.connect(props));
    BridgeServer server1 = cache.addBridgeServer();
    BridgeServer server2 = cache.addBridgeServer();
    int port1 = AvailablePortHelper.getRandomAvailableTCPPort();
    int port2 = AvailablePortHelper.getRandomAvailableTCPPort();
    server1.setPort(port1);
    server2.setPort(port2);
    
    server1.start();
    server2.start();
    
    PoolFactory cpf = PoolManager.createFactory();
    cpf.addServer("localhost", port2);
    cpf.addServer("localhost", port1);
    PoolImpl pool = (PoolImpl) cpf.create("pool1");
    
    ServerLocation location1 = new ServerLocation("localhost", port1);
    ServerLocation location2 = new ServerLocation("localhost", port2);
    
    Op testOp = new Op() {
      int attempts = 0;

      public Object attempt(Connection cnx) throws Exception {
        if(attempts == 0) {
          attempts++;
          throw new SocketTimeoutException();
        }
        else {
          return cnx.getServer();
        }
          
      }
      @Override
      public boolean useThreadLocalConnection() {
        return true;
      }
    };
    
    //TODO - set retry attempts, and throw in some assertions
    //about how many times we retry
    
    ServerLocation usedServer = (ServerLocation) pool.execute(testOp);
    Assert.assertTrue("expected " + location1 + " or " + location2 + ", got " + usedServer,
        location1.equals(usedServer) || location2.equals(usedServer));
    
    testOp = new Op() {
      public Object attempt(Connection cnx) throws Exception {
          throw new SocketTimeoutException();
      }
      @Override
      public boolean useThreadLocalConnection() {
        return true;
      }
    };
    
    try {
      usedServer = (ServerLocation) pool.execute(testOp);
      Assert.fail("Should have failed");
    } catch(ServerConnectivityException expected) {
      //do nothing
    }
  }
  
  public void testCreatePool() throws Exception {
    cache = CacheFactory.create(DistributedSystem.connect(props));
    BridgeServer server1 = cache.addBridgeServer();
    int port1 = AvailablePortHelper.getRandomAvailableTCPPort();
    server1.setPort(port1);
    
    server1.start();
    
    PoolFactory cpf = PoolManager.createFactory();
    cpf.addServer("localhost", port1);
    cpf.setSubscriptionEnabled(true);
    cpf.setSubscriptionRedundancy(0);
    PoolImpl pool = (PoolImpl) cpf.create("pool1");
    
    ServerLocation location1 = new ServerLocation("localhost", port1);
    
    Op testOp = new Op() {
      public Object attempt(Connection cnx) throws Exception {
          return cnx.getServer();
      }
      @Override
      public boolean useThreadLocalConnection() {
        return true;
      }
    };
    
    Assert.assertEquals(location1, pool.executeOnPrimary(testOp));
    Assert.assertEquals(location1, pool.executeOnQueuesAndReturnPrimaryResult(testOp));
  }
  
}
