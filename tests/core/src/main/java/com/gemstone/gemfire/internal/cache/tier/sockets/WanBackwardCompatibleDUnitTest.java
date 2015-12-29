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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.tier.sockets;

import java.util.Properties;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.util.CacheWriterAdapter;
import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.GatewayHub;
import com.gemstone.gemfire.cache.util.GatewayQueueAttributes;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.tier.ConnectionProxy;
import com.gemstone.gemfire.internal.shared.Version;

import dunit.DistributedTestCase;
import dunit.Host;
import dunit.VM;

/**
 * This tests for wan to wan backward compatiblity with different version
 * and also with unversion wan
 * 
 * @author aingle
 */
public class WanBackwardCompatibleDUnitTest extends DistributedTestCase {
  
  private static final String REGION_NAME = "WanBackwardCompatibleDUnitTest_region";
  private static final String key = "WAN_KEY";
  protected static Cache cache = null;
  VM wan1 = null; // gatewayhub 1
  VM wan2 = null; // gatewayhub 2
  
  private static Integer ePort1;
  private static Integer ePort2;
  
  static WanBackwardCompatibleDUnitTest impl;

  public static void createImpl() {
    impl = new WanBackwardCompatibleDUnitTest("temp");
  }
  /** constructor */
  public WanBackwardCompatibleDUnitTest(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception
  {
    disconnectAllFromDS();
    pause(5000);
    final Host host = Host.getHost(0);
    wan1 = host.getVM(0);
    wan2 = host.getVM(1);
    // use an instance to control inheritable aspects of the test
    wan1.invoke(getClass(), "createImpl", null);
    wan2.invoke(getClass(), "createImpl", null);
  }

  /**
   * This test does the following :<br>
   * 1)Create two wan site with different version<br>
   * 2)Perform put operation on each site <br>
   * 3)Verifies events recieved on each wan site<br>
   */
  public void testGatewayConnectivityWithDifferentVersionedWan() {
    int totalPut = 30;
    ePort2 = Integer.valueOf(AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET));
    ePort1 = Integer.valueOf(AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET));

    //  create server cache - gateways configure
    wan1.invoke(getClass(), "setHandshakeVersionForTesting",
        new Object[] { Version.GFE_57.ordinal() });
    wan1.invoke(getClass(), "createServerCache",
        new Object[] {"EU", "1", ePort1, ePort2, getServerHostName(wan1.getHost()) });

    wan2.invoke(getClass(), "createServerCache",
        new Object[] {"NY", "2", ePort2, ePort1, getServerHostName(wan2.getHost())});

    try {
      Thread.sleep(5000);
      // perform 10 puts
      wan1.invoke(getClass(), "doPuts", new Object[] {
          Integer.valueOf(totalPut - 30)/* minRange = 0 */,
          Integer.valueOf(totalPut - 20) /* maxRange = 10 */});
      // perform 20 puts
      wan2.invoke(getClass(), "doPuts", new Object[] {
          Integer.valueOf(totalPut - 20)/* minRange = 10 */,
          Integer.valueOf(totalPut) /* maxRange = 30 */}); 
    }
    catch (InterruptedException e) {
      fail("Exception thrown ", e);
    }
    // perfrom get operation
    wan2.invoke(getClass(), "doGet",
        new Object[] { Integer.valueOf(totalPut) /* aggregate should be 30 */});
    wan1.invoke(getClass(), "doGet",
        new Object[] { Integer.valueOf(totalPut) /* aggregate should be 30 */});
    wan1.invoke(getClass(), "setHandshakeVersionForTesting",
        new Object[] { ConnectionProxy.VERSION.ordinal() });
  }

  /**
   * This test does the following :<br>
   * 1)Create two wan site one is version and other is unversion<br>
   * 2)Perform put operation on each site <br>
   * 3)Verifies events recieved on each wan site<br>
   */
  public void testGatewayConnectivityWithOneUnsupportedVersionedWan() {
    int totalPut = 30;
    ePort2 = Integer.valueOf(AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET));
    ePort1 = Integer.valueOf(AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET));

    //  create server cache - gateways configure
    wan1.invoke(getClass(), "createServerCache",
        new Object[] {"EU", "1", ePort1, null, getServerHostName(wan1.getHost()) });

    // make this vm unsupported wan
    wan2.invoke(getClass(), "setHandshakeVersionForTesting",
        new Object[] { Version.NOT_SUPPORTED_ORDINAL });
    wan2.invoke(getClass(), "createServerCache",
        new Object[] {"NY", "2", ePort2, ePort1, getServerHostName(wan2.getHost()) });

    try {
      Thread.sleep(5000);
      // perform 10 puts
      wan1.invoke(getClass(), "doPuts",
          new Object[] { Integer.valueOf(0)/* minRange = 0 */,
              Integer.valueOf(10) /* maxRange = 10 */});
      // perform 20 puts
      wan2.invoke(getClass(), "doPuts",
          new Object[] { Integer.valueOf(10)/* minRange = 10 */,
              Integer.valueOf(totalPut)/* maxRange = 30 */});
    }
    catch (InterruptedException e) {
      fail("Exception thrown ", e);
    }

    // perfrom get operation
    wan1.invoke(getClass(), "doGet",
        new Object[] { Integer.valueOf(10)/* aggregate should be 10 */});
    wan2.invoke(getClass(), "doGet",
        new Object[] { Integer.valueOf(20)/* aggregate should be 20 */});
    wan2.invoke(getClass(), "setHandshakeVersionForTesting",
        new Object[] { ConnectionProxy.VERSION.ordinal() });
  }

  /**
   * Creates the server cache, and configure wan
   * 
   * @param site - used to configure gateway endpoint
   * @param name - used to generate name
   * @param ePort1 -
   *          GatewayHub port
   * @param ePort2 -
   *          other GatewayHub port
   * @throws Exception -
   *           thrown if any problem occurs in setting up the server
   */
  public static Object createServerCache(String site, String name,
      Integer ePort1, Integer ePort2, String host) throws Exception {
    Properties props = new Properties();
    int mcast_port = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS) ;
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, mcast_port+"");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");

    cache = impl.createCache(props);
    addExceptions();
    AttributesFactory factory = impl.getServerCacheAttributesFactory();
    factory.setEnableGateway(true);

    factory.setCacheWriter(new CacheWriterAdapter() {
      @Override
      public void beforeCreate(EntryEvent event) throws CacheWriterException {
        cache.getLogger().fine("beforeCreate(): " + event);
      }

      @Override
      public void beforeUpdate(EntryEvent event) throws CacheWriterException {
        cache.getLogger().fine("beforeUpdate(): " + event);
      }
    });
    RegionAttributes attrs = factory.create();
    cache.createRegion(REGION_NAME, attrs);
    GatewayHub hub1 = cache.addGatewayHub("GatewayHub " + name, ePort1);
    Gateway gateway1 = hub1.addGateway("Gateway_" + name + ePort1);

    if (ePort2 != null) {
      gateway1.addEndpoint(site, host, ePort2);
      cache.getLogger().info("site: " + site + ", endpoint port: " + ePort2);
    }
    else {
      gateway1.addEndpoint(site, host, AvailablePort
          .getRandomAvailablePort(AvailablePort.SOCKET));
    }
    GatewayQueueAttributes queueAttributes = gateway1.getQueueAttributes();
    queueAttributes.setMaximumQueueMemory(1);
    queueAttributes.setBatchSize(1);
    setDiskStoreForGateway(cache, gateway1.getId(), queueAttributes);
    hub1.start();
    gateway1.start();
    return Integer.valueOf(0);
  }

  protected AttributesFactory getServerCacheAttributesFactory()
  {
    AttributesFactory factory = new AttributesFactory();
    factory.setScope(Scope.DISTRIBUTED_ACK);
    factory.setDataPolicy(DataPolicy.REPLICATE);
    return factory;
  }
  
  /**
   * creates the cache
   * @param props
   * @return Cache
   * @throws Exception
   */
  private Cache createCache(Properties props)
      throws Exception {
    DistributedSystem ds = getSystem(props);
    Cache cache = null;
    cache = CacheFactory.create(ds);
    if (cache == null) {
      throw new Exception("CacheFactory.create() returned null ");
    }
    return cache;
  }
  
  /**
   * close the clients and the servers
   */
  @Override
  public void tearDown2() throws Exception
  {
    /* close the cache */
    wan1.invoke(getClass(), "closeCache");
    wan2.invoke(getClass(), "closeCache");
  }

  /**
   * close the cache
   *
   */
  public static void closeCache()
  {
    if (cache != null && !cache.isClosed()) {
      removeExceptions(); 
      cache.close();
      cache.getDistributedSystem().disconnect();
    }
  }
  
  /**
   * perform put operation
   *
   */
  public static void doPuts(Integer min, Integer max) throws Exception
  {
    Region region = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    for(int i= min.intValue(); i< max.intValue(); i++){
      region.put(key + i, "newValue" + i);
    }
    cache.getLogger().fine(
        "Completed puts from " + key + min + " to " + key + (max - 1));
  }
  
  /**
   * perform get operation to verify total entry count recieved
   *
   */
  public static void doGet(final Integer n) throws Exception {
    final Region reg = cache.getRegion(Region.SEPARATOR + REGION_NAME);
    WaitCriterion wc = new WaitCriterion() {
      String excuse;
      public boolean done() {
        int curSize = reg.keySet().size();
        if (n.intValue() == curSize) {
          return true;
        }
        excuse = "Current size " + curSize + " not expected size " + n.intValue();
        return false;
      }
      public String description() {
        return excuse;
      }
    };
    DistributedTestCase.waitForCriterion(wc, 120 * 1000, 1000, true);
  }
  
  /* 
   * Write version byte from client to server 
   * during handshake.
   */  
  public static void setHandshakeVersionForTesting(Short val) throws Exception
  {
    HandShake.setVersionForTesting(val.shortValue());
    ServerHandShakeProcessor.setSeverVersionForTesting(val.shortValue()); 
  }
  
  /* 
   * Add test command to CommandInitializer.ALL_COMMANDS.
   */ 
  /*public static void setTestCommands() throws Exception
  {
    getLogWriter().info("setTestCommands invoked");  
        Map testCommands = new HashMap();
        testCommands.putAll((Map) CommandInitializer.ALL_COMMANDS.get(Version.GFE_57));
        //testCommands.put(new Integer(MessageType.PUT), new TestPut());          
        CommandInitializer.testSetCommands(testCommands);
    getLogWriter().info("end of setTestCommands");
  }
*/  
  public static void addExceptions() throws Exception
  {
    if (cache != null && !cache.isClosed()) {
      cache.getLogger().info("<ExpectedException action=add>"
          + "UnsupportedVersionException</ExpectedException>");
      cache.getLogger().info("<ExpectedException action=add>"
          + "Unknown message type</ExpectedException>");
      cache.getLogger().info("<ExpectedException action=add>"
          + "Unexpected message type</ExpectedException>");
      // expected exception on WAN due to incompatible REGISTER_DATASERIALIZERS
      /*cache.getLogger().info("<ExpectedException action=add>" + "ServerRefusedConnectionException"
               + "</ExpectedException>");
      cache.getLogger().info("<ExpectedException action=add>" + "SocketException"
               + "</ExpectedException>");
      cache.getLogger().info("<ExpectedException action=add>" + "Could not initialize a primary queue"
               + "</ExpectedException>");*/
        }  
  } 

  public static void removeExceptions()
  {
    if (cache != null && !cache.isClosed()) {
      cache.getLogger().info("<ExpectedException action=remove>"
          + "UnsupportedVersionException</ExpectedException>");
      cache.getLogger().info("<ExpectedException action=remove>"
          + "Unknown message type</ExpectedException>");
      cache.getLogger().info("<ExpectedException action=remove>"
          + "Unexpected message type</ExpectedException>");
      /*cache.getLogger().info("<ExpectedException action=remove>" + "ServerRefusedConnectionException"
               + "</ExpectedException>");
      cache.getLogger().info("<ExpectedException action=remove>" + "SocketException"
           + "</ExpectedException>");
      cache.getLogger().info("<ExpectedException action=remove>" + "Could not initialize a primary queue"
           + "</ExpectedException>");*/
        }  
  }
}
