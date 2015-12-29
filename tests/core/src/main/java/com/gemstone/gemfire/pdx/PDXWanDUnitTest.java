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
package com.gemstone.gemfire.pdx;

import java.util.Properties;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.GatewayConfigurationException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.GatewayHub;
import com.gemstone.gemfire.cache.util.GatewayQueueAttributes;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.shared.Version;

import dunit.Host;
import dunit.RMIException;
import dunit.SerializableCallable;
import dunit.VM;

public class PDXWanDUnitTest extends CacheTestCase {

  public PDXWanDUnitTest(String name) {
    super(name);
  }
  
  /**
   * Test that we get an exception trying to connect
   * two WAN sites that have the same distributed system id.
   */
  public void testWANMatchingIds() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    createSystem(vm0, "1");
    int port0 = AvailablePortHelper.getRandomAvailableTCPPortOnVM(vm0);
    createRegion(vm0);
    createWANReceiver(vm0,  port0);
    try {
      createSystem(vm1, "1");
      createWANSender(vm1, port0);
      createRegion(vm1);
      putInRegion(vm1, "ping", 1);
      fail("Should have received an exception");
    } catch(RMIException e) {
      if(!(e.getCause() instanceof GatewayConfigurationException)) {
        throw e;
      }
    }
  }
  
  /**
   * Test that we can connection two wan sites
   * with the different distributed system ids.
   */
  public void testWANDifferentIds() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    createSystem(vm0, "1");
    int port0 = AvailablePortHelper.getRandomAvailableTCPPortOnVM(vm0);
    createRegion(vm0);
    createWANReceiver(vm0,  port0);
    
    //this should work.
    createSystem(vm1, "2");
    createWANSender(vm1, port0);
    createRegion(vm1);
    putInRegion(vm1, "ping", 1);
    
    //And we should be able to receive through the gateway.
    waitForValue(vm0, "ping", 1);
  }
  
  /**
   * Test that we still send types over the WAN,
   * even if the type registry was created before the 
   * WAN gateway.
   */
  public void testWANProgationTypeDefinedFirst() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    createSystem(vm0, "1");
    int port0 = AvailablePortHelper.getRandomAvailableTCPPortOnVM(vm0);
    createRegion(vm0);
    createWANReceiver(vm0,  port0);
    
    createSystem(vm1, "2");
    createRegion(vm1);
    //define a type;
    serializeOnVM(vm1, 1);
    createWANSender(vm1, port0);
    putInRegion(vm1, "ping", 1);
    
    //And we should be able to receive through the gateway.
    waitForValue(vm0, "ping", 1);
  }
  
  /**
   * Test that we still send types over the WAN,
   * even if the type registry is recovered from disk. 
   * WAN gateway.
   */
  public void testWANProgationTypeWasPersisted() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    createSystem(vm1, "2");
    createRegion(vm1);
    //define a type;
    serializeOnVM(vm1, 1);
    closeCache(vm1);
    
    createSystem(vm0, "1");
    int port0 = AvailablePortHelper.getRandomAvailableTCPPortOnVM(vm0);
    createRegion(vm0);
    createWANReceiver(vm0,  port0);
    
    //this should work. This will recover the type
    //from disk.
    createSystem(vm1, "2");
    createRegion(vm1);
    createWANSender(vm1, port0);
    putInRegion(vm1, "ping", 1);
    
    //And we should be able to receive through the gateway.
    waitForValue(vm0, "ping", 1);
  }
  
  /**
   * Test that we get an exception using PDX and WAN gateways, but
   * not specifying a distributed system id.
   */
  public void testWANUnspecifiedIds() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    
    //Try creating the gateway first
    createSystem(vm0, "-1");
    int port0 = AvailablePortHelper.getRandomAvailableTCPPortOnVM(vm0);
    createWANReceiver(vm0,  port0);
    createRegion(vm0);
    try {
      serializeOnVM(vm0, 1, null);
      fail("should have received an exception");
    } catch(RMIException e) {
      if(!(e.getCause() instanceof PdxInitializationException)) {
        throw e;
      }
    }
    
    //Try creating the type registry first
    closeCache(vm0);
    createRegion(vm0);
    serializeOnVM(vm0, 1, null);
    try {
      port0 = AvailablePortHelper.getRandomAvailableTCPPortOnVM(vm0);
      createWANReceiver(vm0,  port0);
      fail("should have received an exception");
    } catch(RMIException e) {
      if(!(e.getCause() instanceof PdxInitializationException)) {
        throw e;
      }
    }
  }
  
  private void closeCache(VM vm) {
    SerializableCallable createSystem = new SerializableCallable() {
      public Object call() throws Exception {
        closeCache();
        return null;
      }
    }; 
    vm.invoke(createSystem);
  }

  private void createSystem(VM vm, final String dsId) {
    SerializableCallable createSystem = new SerializableCallable() {
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, dsId);
        props.setProperty(DistributionConfig.LOCATORS_NAME, "");
        getSystem(props);
        CacheFactory cf = new CacheFactory();
        cf.setPdxPersistent(true);
        getCache(cf);
        return null;
      }
    }; 
    vm.invoke(createSystem);
  }
  
  private void createRegion(VM vm) {
    SerializableCallable createSystem = new SerializableCallable() {
      public Object call() throws Exception {
        Cache cache = getCache();
        Region region1 = cache.createRegionFactory(RegionShortcut.REPLICATE)
        .setEnableGateway(true)
        .create("region");
        return null;
      }
    };
    vm.invoke(createSystem);
  }
  
  private void createWANReceiver(VM vm, final int wanPort) {
    SerializableCallable createSystem = new SerializableCallable() {
      public Object call() throws Exception {
        Cache cache = getCache();
        //Force the creation of the PDX registry
//        region1.put("hello", new SimpleClass(1, (byte)1));
        GatewayHub hub = cache.addGatewayHub("myId", wanPort);
        hub.start();
        return null;
      }
    };
    vm.invoke(createSystem);
  }
  
  private void createWANSender(VM vm, final int wanPort) {
    SerializableCallable createSystem = new SerializableCallable() {
      public Object call() throws Exception {
        Cache cache = getCache();
        GatewayHub hub = cache.addGatewayHub("myId", -1);
        Gateway gateway = hub.addGateway("g1");
        gateway.setQueueAttributes(new GatewayQueueAttributes(null, 10, 2, 1000, false, false, 5));
        gateway.addEndpoint("myid", "localhost", wanPort);
        hub.start();
        gateway.start();
        return null;
      }
    };
    vm.invoke(createSystem);
  }
  
  private void serializeOnVM(VM vm, final int value) {
    SerializableCallable createSystem = new SerializableCallable() {
      public Object call() throws Exception {
        //Make sure the cache exists
        getCache();
        DataSerializer.writeObject(new SimpleClass(value, (byte) value), new HeapDataOutputStream(Version.CURRENT));
        return null;
      }
    };
    vm.invoke(createSystem);
  }
  private void serializeOnVM(VM vm, final int value, final SimpleClass.SimpleEnum enumVal) {
    SerializableCallable createSystem = new SerializableCallable() {
      public Object call() throws Exception {
        //Make sure the cache exists
        getCache();
        DataSerializer.writeObject(new SimpleClass(value, (byte) value, enumVal), new HeapDataOutputStream(Version.CURRENT));
        return null;
      }
    };
    vm.invoke(createSystem);
  }
  
  private void putInRegion(VM vm, final Object key, final int value) {
    SerializableCallable createSystem = new SerializableCallable() {
      public Object call() throws Exception {
        Cache cache = getCache();
        Region region1 = cache.getRegion("region");
        region1.put(key, new SimpleClass(value, (byte) value));
        return null;
      }
    };
    vm.invoke(createSystem);
  }

  public void waitForValue(VM vm, final String key, final int value) {
    SerializableCallable createSystem = new SerializableCallable() {
      public Object call() throws Exception {
        Cache cache = getCache();
        final Region region1 = cache.getRegion("region");
        waitForCriterion(new WaitCriterion() {

          public String description() {
            return "Didn't receive update over the WAN";
          }

          public boolean done() {
            return region1.get(key) != null;
          }
          
        }, 30000, 100, true);
        assertEquals(new SimpleClass(value, (byte) value), region1.get(key));
        return null;
      }
      
    };
    vm.invoke(createSystem);
  }
}
