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

import java.util.Properties;

import dunit.*;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.GatewayHub;
import com.gemstone.gemfire.cache.util.GatewayQueueAttributes;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;

import java.io.Serializable;

/**
 * Currently this test is broken, causing other DUnit tests to fail. Please run
 * the testWanQueueSize method with a set of other DUnit tests (I found that
 * just running this and
 * com.gemstone.gemfire.internal.jta.dunit.BlockingTimeOutDUnitTest.txt caused
 * problems) to prove that it is in working order.
 * 
 * In general, this test should be a CacheTestCase. CacheTestCase sub-classes
 * already have a static cache instance managed by tried and true methods. No
 * need to re-invent the wheel. Because this DUnit test creates its own cache, I
 * had to add a hacks to clean up the cache instance (closeStaticCache())...
 * which was left lingering for other tests to trip over... unfortunately there
 * are more failures.
 * 
 * Mitch Thomas 8/25/2006
 *  
 * 
 * 
 * 
 * This is Dunit test for testing Queue size property of Wan gateway. The test
 * creates wan connected system with 2 WAN sites. One of the sites having a
 * single node WAN gateway act as a server and other as client. The server
 * creates a wan enabled region and does puts on the region. The client does a
 * get on the same region. The {@link WanQueueSizeDUnitTest#CACHE_SIZE }feild
 * can be changed to put different size objects on the region.
 * 
 * @author tnegi, rreja
 */

public class WanQueueSizeDUnitTest extends CacheTestCase {
	public static String CLASS_NAME = "WanQueueSizeDUnitTest ";

	VM vm0 = null;

	VM vm1 = null;

	public static final int SERVER_PORT = 22224;

	public static final int CLIENT_PORT = 22225;

	public static final int SOCKET_BUFF_SIZE = 256000;

	public static final String REGION_NAME = "WanQueueSizeDUnitTest";

	public static volatile Cache cache = null;

	public static final int MAX_PUT = 5;

	static final int CACHE_SIZE = 100 * (1024); // in bytes

	/**
	 * Constructs a test case with the given name.
	 *
	 * @param name
	 */
	public WanQueueSizeDUnitTest(String name) {
		super(name);
	}

	public void setUp() {
          disconnectAllFromDS();
          pause(5000);
//		Host host = Host.getHost(0);
//		vm0 = host.getVM(0);
//		vm1 = host.getVM(1);
	}

	/**
	 * Tear down for test
	 *
	 * @throws Exception
	 */
	public void tearDown2() throws Exception {
          super.tearDown2();
	  invokeInEveryVM(WanQueueSizeDUnitTest.class, "closeStaticCache");
          vm0 = null;
          vm1 = null;
	}

	/**
	 * This Dunit test does the following: <br>
	 * 1) Creates a wan site-1 with a Gateway and a wan enabled region to act as
	 * a server. <br>
	 * 2) Puts are performed at the region on site-2. <br>
	 * 3) Creates a wan site-2 with Gateway and a wan enabled region to act as a
	 * client. <br>
	 * 4) gets are performed at the wan region on site-1.
	 */

        public void testThisDUnitIsBroken() throws Exception {}
        
        public void _testWanQueueSize() throws Exception {
          vm0.invoke(WanQueueSizeDUnitTest.class, "createWanServer",
              new Object[]{ getServerHostName(vm0.getHost()) });
          vm0.invoke(WanQueueSizeDUnitTest.class, "doPutAtServer");
          vm1.invoke(WanQueueSizeDUnitTest.class, "createWanClient");
          try {
            Thread.sleep(30000);
          } catch (InterruptedException e) {
            fail("interrupted sleeping", e);
          }
          vm1.invoke(WanQueueSizeDUnitTest.class, "getAtClient");
	}

	/**
	 * This function creates single node wan site to act as a server. The
	 * gateway connects to the other wan site end point. A Wan enabled region is
	 * created at the node.
	 *
	 * @throws Exception
	 */

        public static void createWanServer(String host) throws Exception {
          WanQueueSizeDUnitTest test = new WanQueueSizeDUnitTest("name");
          Properties props = new Properties();
          int port = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
          props.setProperty(DistributionConfig.MCAST_PORT_NAME, Integer.toString(port));
          cache = test.createCache(props);

          // set for WAN
          GatewayHub severHub = cache.setGatewayHub("SERVER_HUB", SERVER_PORT);
          Gateway serverGateway = severHub.addGateway("SERVER_GATEWAY");
          serverGateway.addEndpoint("SERVER_GATEWAY", host, CLIENT_PORT);
          serverGateway.setSocketBufferSize(SOCKET_BUFF_SIZE);

          GatewayQueueAttributes queueAttributes = serverGateway
          .getQueueAttributes();
          queueAttributes.setMaximumQueueMemory(1);
          queueAttributes.setBatchSize(1);

          severHub.start();
          serverGateway.start();

          // create the region for the wan
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          // factory.setDataPolicy(DataPolicy.REPLICATE);
          factory.setEnableWAN(true);
          RegionAttributes attrs = factory.create();
          cache.createRegion(REGION_NAME, attrs);
          //		test the socket buffer size seting
          int buffSize = serverGateway.getSocketBufferSize();
          if (buffSize != SOCKET_BUFF_SIZE)
            throw new Exception(
            "Socket Buffer size not configured correctly for server gateway");

        }

	/**
	 * This function creates single node wan site to act as a client. This wan
	 * hub acts as a client to the server. A Wan enabled region is created at
	 * the node.
	 *
	 * @throws Exception
	 */
	public static void createWanClient() throws Exception {
	  WanQueueSizeDUnitTest test = new WanQueueSizeDUnitTest("name");
	  Properties props = new Properties();
	  int port = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
	  props.setProperty(DistributionConfig.MCAST_PORT_NAME, Integer.toString(port));
	  cache = test.createCache(props);

	  // set for WAN
	  GatewayHub clientHub = cache.setGatewayHub("CLIENT_HUB", CLIENT_PORT);
	  Gateway clientGateway = clientHub.addGateway("CLIENT_GATEWAY");
	  clientGateway.setSocketBufferSize(SOCKET_BUFF_SIZE);
	  GatewayQueueAttributes queueAttributes = clientGateway
	  .getQueueAttributes();
	  queueAttributes.setMaximumQueueMemory(1);
	  queueAttributes.setBatchSize(1);

	  clientHub.start();
	  clientGateway.start();

	  // create the region for the wan
	  AttributesFactory factory = new AttributesFactory();
	  factory.setScope(Scope.DISTRIBUTED_ACK);
	  // factory.setDataPolicy(DataPolicy.REPLICATE);
	  factory.setEnableWAN(true);
	  RegionAttributes attrs = factory.create();
	  cache.createRegion(REGION_NAME, attrs);

	  //test the socket buffer size seting
	  int buffSize = clientGateway.getSocketBufferSize();
	  if (buffSize != SOCKET_BUFF_SIZE)
	    throw new Exception(
	    "Socket Buffer size not configured correctly for the client gateway");
	}

	/**
	 * Function for populating the wan eanbled region at the server.
	 *
	 * @throws Exception
	 */
	public static void doPutAtServer() throws Exception {
	  Region region = cache.getRegion(REGION_NAME);
	  byte[] val = new byte[CACHE_SIZE] ;

	  for (int i= 0; i<CACHE_SIZE ; i++){
	    val[i] = 0;
	  }	

	  for (int count = 0; count < MAX_PUT; count++) {			
	    region.put("key" + count,val);
	    cache.getLogger().fine("Put the key  ===== " + "key" + count);
	  }
	}

             
        /**
         * This test really should be convereted to using the cache managed by CacheTestCase 
         * and this method would then not be needed CacheTestCase.cacheClose()
         */
        public static void closeStaticCache()
        {
          if (cache != null) {
            if (!cache.isClosed()) {
              cache.close();
            }
            cache = null;
          }
        }


	/**
	 * Function for pgetting the data from wan eanbled region at the server.
	 *
	 * @throws Exception
	 */
        public static void getAtClient() throws Exception {
          Region region = cache.getRegion(REGION_NAME);
          for (int count = 0; count < MAX_PUT; count++) {
            Object val = region.get("key" + count);
            cache.getLogger().fine(
                " The key  ===== " + "key" + count + "value = " + val);
            if (val != null) {
              cache.getLogger().fine(
                  "Got the  value for the key  ===== " + "key" + count);
            } else {
              fail("Didn't received value at the client side. val =" + val);
            }
            Thread.sleep(100);
          }
        }

        private Cache createCache(Properties props) throws Exception {
          DistributedSystem ds = getSystem(props);
          Cache cache = null;
          cache = CacheFactory.create(ds);
          if (cache == null) {
            throw new Exception("CacheFactory.create() returned null ");
          }
          return cache;
        }

        static class LargeObject implements Serializable {
          byte[] val;

          LargeObject(int size) {
            val = new byte[size];
          }
        }

}
