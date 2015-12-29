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
/*
 * IndexTest.java
 * JUnit based test
 *
 * Created on March 9, 2005, 3:30 PM
 */

package com.gemstone.gemfire.cache.query.internal.index;

import java.util.HashMap;
import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.QueryTestUtils;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.data.Position;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerTestUtil;

import dunit.Host;
import dunit.SerializableCallable;
import dunit.SerializableRunnable;
import dunit.VM;

/**
 * 
 * @author jhuynh
 *
 */
public class CopyOnReadIndexDUnitTest extends CacheTestCase {

  VM vm0;
  VM vm1;
  VM vm2;
  
  public CopyOnReadIndexDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    getSystem();
    invokeInEveryVM(new SerializableRunnable("getSystem") {
      public void run() {
        getSystem();
      }
    });
    Host host = Host.getHost(0);
    vm0 = host.getVM(0);
    vm1 = host.getVM(1);
    vm2 = host.getVM(2);
  }
  
  public void tearDown2() throws Exception {
    invokeInEveryVM(new SerializableRunnable("resetCopyOnRead") {
      public void run() {
        getCache().setCopyOnRead(false);
      }
    });
    vm0.invoke(CacheServerTestUtil.class, "closeCache");
    vm1.invoke(CacheServerTestUtil.class, "closeCache");
    vm2.invoke(CacheServerTestUtil.class, "closeCache");
  }
  
  
  public void testPRQueryOnLocalNode() throws Exception {
    QueryTestUtils utils = new QueryTestUtils();
    helpTestPRQueryOnLocalNode(utils.queries.get("545"), 100, 100);
    helpTestPRQueryOnLocalNode(utils.queries.get("546"), 100, 100);
    helpTestPRQueryOnLocalNode(utils.queries.get("543"), 100, 100);
    helpTestPRQueryOnLocalNode(utils.queries.get("544"), 100, 100);
  }
  
  public void helpTestPRQueryOnLocalNode(final String queryString, final int numPortfolios, final int numExpectedResults) throws Exception {
    final int[] port = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    final int mcastPort = AvailablePortHelper.getRandomAvailableUDPPort();
    
    startCacheServer(vm0, port[0], mcastPort);
    startCacheServer(vm1, port[1], mcastPort);
    
    createPartitionRegion(vm0, "portfolios");
    
    vm0.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        QueryTestUtils utils = new QueryTestUtils();
        utils.createIndex("idIndex", "p.ID", "/portfolios p");
        return null;
      }
    });
    
    createPartitionRegion(vm1, "portfolios");

    
    vm0.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region region = getCache().getRegion("/portfolios");
        for (int i = 0 ; i < 50; i++) {
          Portfolio p = new Portfolio(i);
          p.status = "testStatus";
          p.positions = new HashMap();
          p.positions.put("" + i, new Position("" + i, 20));
          region.put("key " + i, p);
        }
        return null;
      }
    });
    
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region region = getCache().getRegion("/portfolios");
        for (int i = 50 ; i < 100; i++) {
          Portfolio p = new Portfolio(i);
          p.status = "testStatus";
          p.positions = new HashMap();
          p.positions.put("" + i, new Position("" + i, 20));
          region.put("key " + i, p);
        }
        return null;
      }
    });
    
    vm0.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        QueryService qs = getCache().getQueryService();
        Query query = qs.newQuery(queryString);
        SelectResults results = (SelectResults) query.execute();
        assertEquals(100, results.size());
        for (Object o: results) {
          if (o instanceof Portfolio) {
            Portfolio p = (Portfolio) o;
            p.status = "discardStatus";
          }
          else {
            Struct struct = (Struct) o;
            Portfolio p = (Portfolio) struct.getFieldValues()[0];
            p.status = "discardStatus";
          }
        }
        return null;
      }
    });
    
    vm0.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        QueryService qs = getCache().getQueryService();
        Query query = qs.newQuery(queryString);
        SelectResults results = (SelectResults) query.execute();
        assertEquals(100, results.size());
        for (Object o: results) {
          if (o instanceof Portfolio) {
            Portfolio p = (Portfolio) o;
            assertEquals("status should not have been changed", "testStatus", p.status);
          }
          else {
            Struct struct = (Struct)o;
            Portfolio p = (Portfolio) struct.getFieldValues()[0];
            assertEquals("status should not have been changed", "testStatus", p.status);
          }
        }
        return null;
      }
    });
  }

  
  private void createPartitionRegion(VM vm, String regionName) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        QueryTestUtils utils = new QueryTestUtils();
        utils.createPartitionRegion("portfolios", Portfolio.class);
        return null;
      }
    });
  }
  private void startCacheServer(VM server, final int port, final int mcastPort) throws Exception {
    server.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        getSystem(getServerProperties(mcastPort));
        
        GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
        cache.setCopyOnRead(true);
        AttributesFactory factory = new AttributesFactory();        
        
        CacheServer cacheServer = getCache().addCacheServer();
        cacheServer.setPort(port);
        cacheServer.start();  
        
        QueryTestUtils.setCache(cache);
        return null;
      }
    });
  }
  
  private void startClient(VM client, final VM server, final int port) {
    client.invoke(new CacheSerializableRunnable("Start client") {
      public void run2() throws CacheException {
        Properties props = getClientProps();
        getSystem(props);
        
        final ClientCacheFactory ccf = new ClientCacheFactory(props);
        ccf.addPoolServer(getServerHostName(server.getHost()), port);
        ccf.setPoolSubscriptionEnabled(true);
        
        ClientCache cache = (ClientCache)getClientCache(ccf);
      }
    });
  }
 
  protected Properties getClientProps() {
    Properties p = new Properties();
    p.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    p.setProperty(DistributionConfig.LOCATORS_NAME, "");
    return p;
  }

  protected Properties getServerProperties(int mcastPort) {
    Properties p = new Properties();
    p.setProperty(DistributionConfig.MCAST_PORT_NAME, mcastPort+"");
    p.setProperty(DistributionConfig.LOCATORS_NAME, "");
    return p;
  }
  
  

}
