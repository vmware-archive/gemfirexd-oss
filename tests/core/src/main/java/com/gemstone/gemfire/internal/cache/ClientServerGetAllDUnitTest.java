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

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.tools.gfsh.app.commands.key;

import com.gemstone.gemfire.cache30.BridgeTestCase;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;

import com.gemstone.gemfire.cache.client.*;

import dunit.*;

import java.util.*;

/**
 * Class <code>ClientServerGetAllDUnitTest</code> test client/server getAll.
 *
 * @author Barry Oglesby
 * @since 5.7
 */
 public class ClientServerGetAllDUnitTest extends BridgeTestCase {

  public ClientServerGetAllDUnitTest(String name) {
    super(name);
  }

  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
    disconnectAllFromDS();
  }

  public void testGetAllFromServer() throws Exception {
    final Host host = Host.getHost(0);
    final VM server = host.getVM(0);
    final VM client = host.getVM(1);
    final String regionName = getUniqueName();
    final int mcastPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    final int serverPort = AvailablePortHelper.getRandomAvailableTCPPortOnVM(server);
    final String serverHost = getServerHostName(server.getHost());

    createBridgeServer(server, mcastPort, regionName, serverPort, false);

    createBridgeClient(client, regionName, serverHost, new int[] {serverPort});

    // Run getAll
    client.invoke(new CacheSerializableRunnable("Get all entries from server") {
      @Override
      public void run2() throws CacheException {
        // Build collection of keys
        Collection keys = new ArrayList();
        for (int i=0; i<5; i++) {
          keys.add("key-"+i);
        }
        
        keys.add(BridgeTestCase.NON_EXISTENT_KEY); // this will not be load CacheLoader
        
        // Invoke getAll
        Region region = getRootRegion(regionName);
        Map result = region.getAll(keys);

        // Verify result size is correct
        assertEquals(6, result.size());

        // Verify the result contains each key,
        // and the value for each key is correct
        // (the server has a loader that returns the key as the value)
        for (Iterator i = keys.iterator(); i.hasNext();) {
          String key = (String) i.next();
          assertTrue(result.containsKey(key));
          Object value = result.get(key);
          if(!key.equals(BridgeTestCase.NON_EXISTENT_KEY))
            assertEquals(key, value);
          else
            assertEquals(null, value);
        }
        
        assertEquals(null, region.get(BridgeTestCase.NON_EXISTENT_KEY));
      }
    });

    stopBridgeServer(server);
  }

  public void testGetSomeFromServer() throws Exception {
    testGetFromServer(2);
  }

  public void testGetAllFromClient() throws Exception {
    testGetFromServer(5);
  }

  public void testGetAllFromServerWithPR() throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM server2 = host.getVM(1);
    final VM client = host.getVM(2);
    final String regionName = getUniqueName();
    final int mcastPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    final int server1Port = AvailablePortHelper.getRandomAvailableTCPPortOnVM(server1);
    final int server2Port = AvailablePortHelper.getRandomAvailableTCPPortOnVM(server2);
    final String serverHost = getServerHostName(server1.getHost());

    createBridgeServer(server1, mcastPort, regionName, server1Port, true);

    createBridgeServer(server2, mcastPort, regionName, server2Port, true);

    createBridgeClient(client, regionName, serverHost, new int[] {server1Port, server2Port});

    // Run getAll
    client.invoke(new CacheSerializableRunnable("Get all entries from server") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion(regionName);
        for (int i=0; i<200; i++) {
          region.put(i, i);
        }
        
        try {
          Thread.currentThread().sleep(1000);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();          
        }
        // Build collection of keys
        Collection keys = new ArrayList();
        for (int i=0; i<5; i++) {
          keys.add("key-"+i);
        }
        keys.add(BridgeTestCase.NON_EXISTENT_KEY); // this will not be load CacheLoader
        
        // Invoke getAll
        
        Map result = region.getAll(keys);
        

        // Verify result size is correct
        assertEquals(6, result.size());

        // Verify the result contains each key,
        // and the value for each key is correct
        // (the server has a loader that returns the key as the value)
        for (Iterator i = keys.iterator(); i.hasNext();) {
          String key = (String) i.next();
          assertTrue(result.containsKey(key));
          Object value = result.get(key);
          if(!key.equals(BridgeTestCase.NON_EXISTENT_KEY))
            assertEquals(key, value);
          else
            assertEquals(null, value);
        }
        assertEquals(null, region.get(BridgeTestCase.NON_EXISTENT_KEY));
      }
    });

    stopBridgeServer(server1);

    stopBridgeServer(server2);
  }

  private void testGetFromServer(final int numLocalValues) {
    final Host host = Host.getHost(0);
    final VM server = host.getVM(0);
    final VM client = host.getVM(1);
    final String regionName = getUniqueName();
    final int mcastPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    final int serverPort = AvailablePortHelper.getRandomAvailableTCPPortOnVM(server);
    final String serverHost = getServerHostName(server.getHost());

    createBridgeServer(server, mcastPort, regionName, serverPort, false);

    createBridgeClient(client, regionName, serverHost, new int[] {serverPort});

    // Put some entries from the client
    client.invoke(new CacheSerializableRunnable("Put entries from client") {
      @Override
      public void run2() throws CacheException {
        Region region = getRootRegion(regionName);
        for (int i=0; i<numLocalValues; i++) {
          region.put("key-"+i, "value-from-client-"+i);
        }
      }
    });

    // Run getAll
    client.invoke(new CacheSerializableRunnable("Get all entries from server") {
      @Override
      public void run2() throws CacheException {
        // Build collection of keys
        Collection keys = new ArrayList();
        for (int i=0; i<5; i++) {
          keys.add("key-"+i);
        }

        // Invoke getAll
        Region region = getRootRegion(regionName);
        Map result = region.getAll(keys);

        // Verify result size is correct
        assertEquals(5, result.size());

        // Verify the result contains each key,
        // and the value for each key is correct
        // (the server has a loader that returns the key as the value)
        // (the local value contains the phrase 'from client')
        int i = 0;
        for (Iterator it = keys.iterator(); it.hasNext(); i++) {
          String key = (String) it.next();
          assertTrue(result.containsKey(key));
          Object value = result.get(key);
          if (i < numLocalValues) {
            assertEquals("value-from-client-"+i, value);
          } else {
            assertEquals(key, value);
          }
        }
      }
    });

    // client may see "server unreachable" exceptions after this
    addExpectedException("Server unreachable", client);
    stopBridgeServer(server);
  }
  
  public void testGetAllWithExtraKeyFromServer() throws Exception {
    final Host host = Host.getHost(0);
    final VM server = host.getVM(0);
    final VM client = host.getVM(1);
    final String regionName = getUniqueName();
    final int mcastPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    final int serverPort = AvailablePortHelper.getRandomAvailableTCPPortOnVM(server);
    final String serverHost = getServerHostName(server.getHost());
    final int numLocalValues = 101;
    
    createBridgeServerWithoutLoader(server, mcastPort, regionName, serverPort, false);

    createBridgeClient(client, regionName, serverHost, new int[] {serverPort});

    // Put some entries from the client
    client.invoke(new CacheSerializableRunnable("Put entries from client") {
      public void run2() throws CacheException {
        Region region = getRootRegion(regionName);
        for (int i = 0; i < numLocalValues; i++) {
          region.put("key-" + i, "value-from-client-" + i);
        }
      }
    });
    
    server.invoke(new CacheSerializableRunnable("Put entries from server") {
      public void run2() throws CacheException {
        Region region = getRootRegion(regionName);
        for (int i = numLocalValues; i < numLocalValues*2; i++) {
          region.put("key-" + i, "value-from-server-" + i);
        }
        region.getCache().getLogger().fine("The region entries in server " + region.entrySet());
      }
    });
    
    
    // Run getAll
    client.invoke(new CacheSerializableRunnable("Get all entries from server") {
      public void run2() throws CacheException {
        // Build collection of keys
        Collection keys = new ArrayList();
        for (int i=0; i<numLocalValues*3; i++) {
          keys.add("key-"+i);
        }

        // Invoke getAll
        Region region = getRootRegion(regionName);
        region.getCache().getLogger().fine("The region entries in client before getAll " + region.entrySet());
        assertEquals(region.entrySet().size(),numLocalValues);
        Map result = region.getAll(keys);
        assertEquals(region.entrySet().size(),2*numLocalValues);
        region.getCache().getLogger().fine("The region entries in client after getAll " + region.entrySet());
        
        // Verify result size is correct
        assertEquals(3*numLocalValues, result.size());

        // Verify the result contains each key,
        // and the value for each key is correct
        int i = 0;
        for (Iterator it = keys.iterator(); it.hasNext(); i++) {
          String key = (String) it.next();
          assertTrue(result.containsKey(key));
          Object value = result.get(key);
          if (i < numLocalValues) {
            assertEquals("value-from-client-"+i, value);
          }else if(i < 2*numLocalValues) {
            assertEquals("value-from-server-"+i, value);
          }
          else {
            assertEquals(null, value);
          }
        }
      }
    });

    stopBridgeServer(server);
  }

  

  private void createBridgeServer(VM server, final int mcastPort, final String regionName, final int serverPort, final boolean createPR) {
    server.invoke(new CacheSerializableRunnable("Create server") {
      @Override
      public void run2() throws CacheException {
        // Create DS
        Properties config = new Properties();
        config.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(mcastPort));
        config.setProperty(DistributionConfig.LOCATORS_NAME, "");
        getSystem(config);

        // Create Region
        AttributesFactory factory = new AttributesFactory();
        factory.setCacheLoader(new BridgeServerCacheLoader());
        if (createPR) {
          factory.setDataPolicy(DataPolicy.PARTITION);
          factory.setPartitionAttributes((new PartitionAttributesFactory()).create());
        } else {
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.REPLICATE);
        }
        Region region = createRootRegion(regionName, factory.create());
        if (createPR) {
          assertTrue(region instanceof PartitionedRegion);
        }
        try {
          startBridgeServer(serverPort);
        } catch (Exception e) {
          fail("While starting CacheServer", e);
        }
      }
    });
  }

  private void createBridgeServerWithoutLoader(VM server, final int mcastPort, final String regionName, final int serverPort, final boolean createPR) {
    server.invoke(new CacheSerializableRunnable("Create server") {
      public void run2() throws CacheException {
        // Create DS
        Properties config = new Properties();
        config.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(mcastPort));
        config.setProperty(DistributionConfig.LOCATORS_NAME, "");
        getSystem(config);

        // Create Region
        AttributesFactory factory = new AttributesFactory();
        if (createPR) {
          factory.setDataPolicy(DataPolicy.PARTITION);
          factory.setPartitionAttributes((new PartitionAttributesFactory()).create());
        } else {
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.REPLICATE);
        }
        Region region = createRootRegion(regionName, factory.create());
        if (createPR) {
          assertTrue(region instanceof PartitionedRegion);
        }
        try {
          startBridgeServer(serverPort);
          System.out.println("Started bridger server ");
        } catch (Exception e) {
          fail("While starting CacheServer", e);
        }
      }
    });
  }
  
  private void createBridgeClient(VM client, final String regionName, final String serverHost, final int[] serverPorts) {
    client.invoke(new CacheSerializableRunnable("Create client") {
      @Override
      public void run2() throws CacheException {
        // Create DS
        Properties config = new Properties();
        config.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
        config.setProperty(DistributionConfig.LOCATORS_NAME, "");
        getSystem(config);

        // Create Region
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        {
          PoolFactory pf = PoolManager.createFactory();
          for (int i=0; i < serverPorts.length; i++) {
            pf.addServer(serverHost, serverPorts[i]);
          }
          pf.create("myPool");
        }
        factory.setPoolName("myPool");
        createRootRegion(regionName, factory.create());
      }
    });
  }

  private void stopBridgeServer(VM server) {
    server.invoke(new CacheSerializableRunnable("Stop Server") {
      @Override
      public void run2() throws CacheException {
        stopBridgeServers(getCache());
      }
    });
  }
}

