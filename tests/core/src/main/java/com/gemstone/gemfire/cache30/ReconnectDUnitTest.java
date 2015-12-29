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

import hydra.Log;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.ForcedDisconnectException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.LossAction;
import com.gemstone.gemfire.cache.MembershipAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionDestroyedException;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.ResumptionAction;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem.ReconnectListener;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.distributed.internal.membership.jgroup.MembershipManagerHelper;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.JChannel;
import com.gemstone.org.jgroups.protocols.pbcast.GMS;
import com.gemstone.org.jgroups.stack.Protocol;

import dunit.AsyncInvocation;
import dunit.DistributedTestCase;
import dunit.Host;
import dunit.SerializableCallable;
import dunit.SerializableRunnable;
import dunit.VM;

public class ReconnectDUnitTest extends CacheTestCase
{
  int locatorPort;
  Locator locator;
  static DistributedSystem savedSystem;
  static int locatorVMNumber = 3;
  
  Properties dsProperties;
  
  public ReconnectDUnitTest(String name) {
    super(name);
  }
  
  @Override
  public void setUp() throws Exception {
    super.setUp();
    this.locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    final int locPort = this.locatorPort;
    Host.getHost(0).getVM(locatorVMNumber)
      .invoke(new SerializableRunnable("start locator") {
      public void run() {
        try {
          locatorPort = locPort;
          Properties props = getDistributedSystemProperties();
          props.put("log-file", "autoReconnectLocatorVM"+VM.getCurrentVMNum()+"_"+getPID()+".log");
          locator = Locator.startLocatorAndDS(locatorPort, null, props);
          addExpectedException("com.gemstone.gemfire.ForcedDisconnectException||Possible loss of quorum");
//          MembershipManagerHelper.getMembershipManager(InternalDistributedSystem.getConnectedInstance()).setDebugJGroups(true);
        } catch (IOException e) {
          fail("unable to start locator", e);
        }
      }
    });
  }

  @Override
  public Properties getDistributedSystemProperties() {
    if (dsProperties == null) {
      dsProperties = super.getDistributedSystemProperties();
      dsProperties.put(DistributionConfig.MAX_WAIT_TIME_FOR_RECONNECT_NAME, "20000");
      dsProperties.put(DistributionConfig.ENABLE_NETWORK_PARTITION_DETECTION_NAME, "true");
      dsProperties.put(DistributionConfig.DISABLE_AUTO_RECONNECT_NAME, "false");
      dsProperties.put(DistributionConfig.LOCATORS_NAME, "localHost["+this.locatorPort+"]");
      dsProperties.put(DistributionConfig.MCAST_PORT_NAME, "0");
      dsProperties.put(DistributionConfig.MEMBER_TIMEOUT_NAME, "1000");
    }
    return dsProperties;
  }
  
  public void tearDown2() throws Exception
  {
    try {
      super.tearDown2();
      Host.getHost(0).getVM(3).invoke(new SerializableRunnable("stop locator") {
        public void run() {
          if (locator != null) {
            locator.stop();
          }
        }
      });
    } finally {
      invokeInEveryVM(new SerializableRunnable() {
        public void run() {
          ReconnectDUnitTest.savedSystem = null;
        }
      });
      disconnectAllFromDS();
    }
  }

  /**
   * Creates some region attributes for the regions being created.
   * */
  private RegionAttributes createAtts()
  {
    AttributesFactory factory = new AttributesFactory();

    {
      // TestCacheListener listener = new TestCacheListener(){}; // this needs to be serializable
      //callbacks.add(listener);
      //factory.setDataPolicy(DataPolicy.REPLICATE);
      factory.setDataPolicy(DataPolicy.REPLICATE);
      factory.setScope(Scope.DISTRIBUTED_ACK);
      // factory.setCacheListener(listener);
    }

    return factory.create();
  }

  /**
   * (comment from Bruce: this test doesn't seem to really do anything)
   * </p>
   * Test reconnect with the max-time-out of 200 and max-number-of-tries
   * 1. The test first creates an xml file and then use it to create
   * cache and regions. The test then fires reconnect in one of the
   * vms. The reconnect uses xml file to create and intialize cache.
   * @throws Exception 
   * */
  
  public void testReconnect() throws TimeoutException, CacheException,
      IOException
  {
    final int locPort = this.locatorPort;
    
    beginCacheXml();
    createRegion("myRegion", createAtts());
    finishCacheXml("MyDisconnect");
    //Cache cache = getCache();
    closeCache();
    getSystem().disconnect();
    getLogWriter().fine("Cache Closed ");

    final String xmlFileLoc = (new File(".")).getAbsolutePath();

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);

    VM vm1 = host.getVM(1);
    //VM vm2 = host.getVM(2);

    SerializableRunnable create1 = new CacheSerializableRunnable(
        "Create Cache and Regions from cache.xml") {
      public void run2() throws CacheException
      {
        //      DebuggerSupport.waitForJavaDebugger(getLogWriter(), " about to create region");
        locatorPort = locPort;
        Properties props = getDistributedSystemProperties();
        props.put("cache-xml-file", xmlFileLoc+"/MyDisconnect-cache.xml");
        props.put("max-wait-time-reconnect", "200");
        props.put("max-num-reconnect-tries", "1");
        getLogWriter().info("test is creating distributed system");
        getSystem(props);
        getLogWriter().info("test is creating cache");
        Cache cache = getCache();
        Region myRegion = cache.getRegion("root/myRegion");
        myRegion.put("MyKey1", "MyValue1");
        // myRegion.put("Mykey2", "MyValue2");

      }
    };

    SerializableRunnable create2 = new CacheSerializableRunnable(
        "Create Cache and Regions from cache.xml") {
      public void run2() throws CacheException
      {
        //            DebuggerSupport.waitForJavaDebugger(getLogWriter(), " about to create region");
        locatorPort = locPort;
        Properties props = getDistributedSystemProperties();
        props.put("cache-xml-file", xmlFileLoc+"/MyDisconnect-cache.xml");
        props.put("max-wait-time-reconnect", "200");
        props.put("max-num-reconnect-tries", "1");
        getSystem(props);
        Cache cache = getCache();
        Region myRegion = cache.getRegion("root/myRegion");
        //myRegion.put("MyKey1", "MyValue1");
        myRegion.put("Mykey2", "MyValue2");
        assertNotNull(myRegion.get("MyKey1"));
        //getLogWriter().fine("MyKey1 value is : "+myRegion.get("MyKey1"));

      }
    };

    vm0.invoke(create1);
    vm1.invoke(create2);

    SerializableRunnable reconnect = new CacheSerializableRunnable(
        "Create Region") {
      public void run2() throws CacheException
      {
        //        DebuggerSupport.waitForJavaDebugger(getLogWriter(), " about to create region");
       // closeCache();
       // getSystem().disconnect();
        locatorPort = locPort;
        Properties props = getDistributedSystemProperties();
        props.put("cache-xml-file", xmlFileLoc+"/MyDisconnect-cache.xml");
        props.put("max-wait-time-reconnect", "200");
        props.put("max-num-reconnect-tries", "1");
        getSystem(props);
        Cache cache = getCache();
        //getLogWriter().fine("Cache type : "+cache.getClass().getName());
        Region reg = cache.getRegion("root/myRegion");
        //getLogWriter().fine("The reg type : "+reg);
        assertNotNull(reg.get("MyKey1"));
        getLogWriter().fine("MyKey1 Value after disconnect : "
            + reg.get("MyKey1"));
        
        //closeCache();
        //disconnectFromDS();

      }
    };

    vm1.invoke(reconnect);

  }
  
  
  // quorum check fails, then succeeds
  public void testReconnectWithQuorum() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    final int locPort = locatorPort;
    final int secondLocPort = AvailablePortHelper.getRandomAvailableTCPPort();

    final String xmlFileLoc = (new File(".")).getAbsolutePath();
    
    // disable disconnects in the locator so we have some stability
    host.getVM(locatorVMNumber).invoke(new SerializableRunnable("disable force-disconnect") {
      public void run() {
        GMS gms = (GMS)MembershipManagerHelper.getJChannel(InternalDistributedSystem.getConnectedInstance())
          .getProtocolStack().findProtocol("GMS");
        gms.disableDisconnectOnQuorumLossForTesting();
      }}
    );

    SerializableCallable create = new SerializableCallable(
    "Create Cache and Regions from cache.xml") {
      public Object call() throws CacheException
      {
        //      DebuggerSupport.waitForJavaDebugger(getLogWriter(), " about to create region");
        locatorPort = locPort;
        Properties props = getDistributedSystemProperties();
        props.put("cache-xml-file", xmlFileLoc+"/MyDisconnect-cache.xml");
        props.put("max-wait-time-reconnect", "1000");
        props.put("max-num-reconnect-tries", "2");
        props.put("log-file", "autoReconnectVM"+VM.getCurrentVMNum()+"_"+getPID()+".log");
        Cache cache = new CacheFactory(props).create();
        addExpectedException("com.gemstone.gemfire.ForcedDisconnectException||Possible loss of quorum");
        Region myRegion = cache.getRegion("root/myRegion");
        ReconnectDUnitTest.savedSystem = cache.getDistributedSystem();
        myRegion.put("MyKey1", "MyValue1");
//        MembershipManagerHelper.getMembershipManager(cache.getDistributedSystem()).setDebugJGroups(true); 
        // myRegion.put("Mykey2", "MyValue2");
        return savedSystem.getDistributedMember();
      }
    };
    
    vm0.invoke(create);
    vm1.invoke(create);
    vm2.invoke(create);
    
    // view is [locator(3), vm0(15), vm1(10), vm2(10)]
    
    /* now we want to cause vm0 and vm1 to force-disconnect.  This may cause the other
     * non-locator member to also disconnect, depending on the timing
     */
    System.out.println("disconnecting vm0");
    forceDisconnect(vm0);
    pause(10000);
    System.out.println("disconnecting vm1");
    forceDisconnect(vm1);

    /* now we wait for them to auto-reconnect*/
    waitForReconnect(vm0);
    waitForReconnect(vm1);
  }

  private void deleteStateFile(int port) {
    File stateFile = new File("locator" + port + "state.dat");
    if (stateFile.exists()) {
      stateFile.delete();
    }
  }

  public void testReconnectOnForcedDisconnect() throws Exception  {

//    getSystem().disconnect();
//    getLogWriter().fine("Cache Closed ");

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    
    final int locPort = locatorPort;
    final int secondLocPort = AvailablePortHelper.getRandomAvailableTCPPort();

    final String xmlFileLoc = (new File(".")).getAbsolutePath();

    SerializableCallable create1 = new SerializableCallable(
    "Create Cache and Regions from cache.xml") {
      public Object call() throws CacheException
      {
        //      DebuggerSupport.waitForJavaDebugger(getLogWriter(), " about to create region");
        locatorPort = locPort;
        Properties props = getDistributedSystemProperties();
        props.put("cache-xml-file", xmlFileLoc+"/MyDisconnect-cache.xml");
        props.put("max-wait-time-reconnect", "1000");
        props.put("max-num-reconnect-tries", "2");
        props.put("log-file", "autoReconnectVM"+VM.getCurrentVMNum()+"_"+getPID()+".log");
        Cache cache = new CacheFactory(props).create();
        Region myRegion = cache.getRegion("root/myRegion");
        ReconnectDUnitTest.savedSystem = cache.getDistributedSystem();
        myRegion.put("MyKey1", "MyValue1");
        // myRegion.put("Mykey2", "MyValue2");
        return savedSystem.getDistributedMember();
      }
    };

    SerializableCallable create2 = new SerializableCallable(
    "Create Cache and Regions from cache.xml") {
      public Object call() throws CacheException
      {
        //            DebuggerSupport.waitForJavaDebugger(getLogWriter(), " about to create region");
        locatorPort = locPort;
        Properties props = getDistributedSystemProperties();
        props.put("cache-xml-file", xmlFileLoc+"/MyDisconnect-cache.xml");
        props.put("max-wait-time-reconnect", "1000");
        props.put("max-num-reconnect-tries", "2");
        props.put("start-locator", "localhost["+secondLocPort+"]");
        props.put("locators", props.get("locators")+",localhost["+secondLocPort+"]");
        props.put("log-file", "autoReconnectVM"+VM.getCurrentVMNum()+"_"+getPID()+".log");
        getSystem(props);
        Cache cache = getCache();
        ReconnectDUnitTest.savedSystem = cache.getDistributedSystem();
        Region myRegion = cache.getRegion("root/myRegion");
        //myRegion.put("MyKey1", "MyValue1");
        myRegion.put("Mykey2", "MyValue2");
        assertNotNull(myRegion.get("MyKey1"));
        //getLogWriter().fine("MyKey1 value is : "+myRegion.get("MyKey1"));
        return cache.getDistributedSystem().getDistributedMember();
      }
    };

    vm0.invoke(create1);
    DistributedMember dm = (DistributedMember)vm1.invoke(create2);
    forceDisconnect(vm1);
    DistributedMember newdm = (DistributedMember)vm1.invoke(new SerializableCallable("wait for reconnect(1)") {
      public Object call() {
        final DistributedSystem ds = ReconnectDUnitTest.savedSystem;
        ReconnectDUnitTest.savedSystem = null;
        waitForCriterion(new WaitCriterion() {
          public boolean done() {
            return ds.isReconnecting();
          }
          public String description() {
            return "waiting for ds to begin reconnecting";
          }
        }, 30000, 1000, true);
        getLogWriter().info("entering reconnect wait for " + ds);
        getLogWriter().info("ds.isReconnecting() = " + ds.isReconnecting());
        boolean failure = true;
        try {
          ds.waitUntilReconnected(60, TimeUnit.SECONDS);
          ReconnectDUnitTest.savedSystem = ds.getReconnectedSystem();
          InternalLocator locator = (InternalLocator)Locator.getLocator();
          assertTrue("Expected system to be restarted", ds.getReconnectedSystem() != null);
          assertTrue("Expected system to be running", ds.getReconnectedSystem().isConnected());
          assertTrue("Expected there to be a locator", locator != null);
          assertTrue("Expected locator to be restarted", !locator.isStopped());
          failure = false;
          return ds.getReconnectedSystem().getDistributedMember();
        } catch (InterruptedException e) {
          getLogWriter().warning("interrupted while waiting for reconnect");
          return null;
        } finally {
          if (failure) {
            ds.disconnect();
          }
        }
      }
    });
    assertNotSame(dm, newdm);
    // force another reconnect and show that stopReconnecting works
    forceDisconnect(vm1);
    boolean stopped = (Boolean)vm1.invoke(new SerializableCallable("wait for reconnect and stop") {
      public Object call() {
        final DistributedSystem ds = ReconnectDUnitTest.savedSystem;
        ReconnectDUnitTest.savedSystem = null;
        waitForCriterion(new WaitCriterion() {
          public boolean done() {
            return ds.isReconnecting() || ds.getReconnectedSystem() != null;
          }
          public String description() {
            return "waiting for reconnect to commence in " + ds;
          }
          
        }, 10000, 1000, true);
        ds.stopReconnecting();
        assertFalse(ds.isReconnecting());
        DistributedSystem newDs = InternalDistributedSystem.getAnyInstance();
        if (newDs != null) {
          getLogWriter().warning("expected distributed system to be disconnected: " + newDs);
          return false;
        }
        return true;
      }
    });
    assertTrue("expected DistributedSystem to disconnect", stopped);

    // recreate the system in vm1 without a locator and crash it 
    dm = (DistributedMember)vm1.invoke(create1);
    forceDisconnect(vm1);
    newdm = waitForReconnect(vm1);
    assertNotSame("expected a reconnect to occur in member", dm, newdm);
    deleteStateFile(locPort);
    deleteStateFile(secondLocPort);
  }
  
  private DistributedMember getDMID(VM vm) {
    return (DistributedMember)vm.invoke(new SerializableCallable("get ID") {
      public Object call() {
        ReconnectDUnitTest.savedSystem = InternalDistributedSystem.getAnyInstance();
        return ReconnectDUnitTest.savedSystem.getDistributedMember();
      }
    });
  }
  
  private DistributedMember waitForReconnect(VM vm) {
    return (DistributedMember)vm.invoke(new SerializableCallable("wait for Reconnect and return ID") {
      public Object call() {
    	System.out.println("waitForReconnect invoked");
        final DistributedSystem ds = ReconnectDUnitTest.savedSystem;
        ReconnectDUnitTest.savedSystem = null;
        waitForCriterion(new WaitCriterion() {
          public boolean done() {
            return ds.isReconnecting();
          }
          public String description() {
            return "waiting for ds to begin reconnecting";
          }
        }, 60000, 1000, true);
        long waitTime = 180;
        getLogWriter().info("VM"+VM.getCurrentVMNum() + " waiting up to "+waitTime+" seconds for reconnect to complete");
        try {
          ds.waitUntilReconnected(120, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
          fail("interrupted while waiting for reconnect");
        }
        assertTrue("expected system to be reconnected", ds.getReconnectedSystem() != null);
        return ds.getReconnectedSystem().getDistributedMember();
      }
    });
  }
  
  
  public void testReconnectALocator() throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    DistributedMember dm, newdm;
    
    final int locPort = locatorPort;
    final int secondLocPort = AvailablePortHelper.getRandomAvailableTCPPort();
    final String xmlFileLoc = (new File(".")).getAbsolutePath();
    
    File locatorViewLog = new File("locator"+locatorPort+"views.log");
    assertTrue("Expected to find " + locatorViewLog.getPath() + " file", locatorViewLog.exists());
    long logSize = locatorViewLog.length();

    vm0.invoke(new SerializableRunnable("Create a second locator") {
      public void run() throws CacheException
      {
        locatorPort = locPort;
        Properties props = getDistributedSystemProperties();
        props.put("max-wait-time-reconnect", "1000");
        props.put("max-num-reconnect-tries", "2");
        props.put("locators", props.get("locators")+",localhost["+locPort+"]");
        try {
          Locator.startLocatorAndDS(secondLocPort, null, props);
        } catch (IOException e) {
          fail("exception starting locator", e);
        }
      }
    });

    File locator2ViewLog = new File("locator"+secondLocPort+"views.log");
    assertTrue("Expected to find " + locator2ViewLog.getPath() + " file", locator2ViewLog.exists());
    long log2Size = locator2ViewLog.length();

    // create a cache in vm1 so there is more weight in the system
    SerializableCallable create1 = new SerializableCallable(
    "Create Cache and Regions from cache.xml") {
      public Object call() throws CacheException
      {
        //      DebuggerSupport.waitForJavaDebugger(getLogWriter(), " about to create region");
        locatorPort = locPort;
        Properties props = getDistributedSystemProperties();
        props.put("cache-xml-file", xmlFileLoc+"/MyDisconnect-cache.xml");
        props.put("max-wait-time-reconnect", "1000");
        props.put("max-num-reconnect-tries", "2");
        ReconnectDUnitTest.savedSystem = getSystem(props);
        Cache cache = getCache();
        Region myRegion = cache.getRegion("root/myRegion");
        myRegion.put("MyKey1", "MyValue1");
        // myRegion.put("Mykey2", "MyValue2");
        return savedSystem.getDistributedMember();
      }
    };
    vm1.invoke(create1);

    
    try {
      
      dm = getDMID(vm0);
      forceDisconnect(vm0);
      newdm = waitForReconnect(vm0);

      boolean running = (Boolean)vm0.invoke(new SerializableCallable("check for running locator") {
        public Object call() {
          if (Locator.getLocator() == null) {
            getLogWriter().error("expected to find a running locator but getLocator() returns null");
            return false;
          }
          if (((InternalLocator)Locator.getLocator()).isStopped()) {
            getLogWriter().error("found a stopped locator");
            return false;
          }
          return true;
        }
      });
      if (!running) {
        fail("expected the restarted member to be hosting a running locator");
      }
      
      assertNotSame("expected a reconnect to occur in the locator", dm, newdm);

      // the log should have been opened and appended with a new view
      assertTrue("expected " + locator2ViewLog.getPath() + " to grow in size",
          locator2ViewLog.length() > log2Size);
      // the other locator should have logged a new view
      assertTrue("expected " + locatorViewLog.getPath() + " to grow in size",
          locatorViewLog.length() > logSize);

    } finally {
      vm0.invoke(new SerializableRunnable("stop locator") {
        public void run() {
          Locator loc = Locator.getLocator();
          if (loc != null) {
            loc.stop();
          }
        }
      });
      deleteStateFile(locPort);
      deleteStateFile(secondLocPort);
    }
  }
  
  /**
   * Test the reconnect behavior when the required roles are missing.
   * Reconnect is triggered as a Reliability policy. The test is to
   * see if the reconnect is triggered for the configured number of times
   */
  
  public void testReconnectWithRoleLoss() throws TimeoutException,
      RegionExistsException  {

    final String rr1 = "RoleA";
    final String rr2 = "RoleB";
    final String[] requiredRoles = { rr1, rr2 };
    final int locPort = locatorPort;
    final String xmlFileLoc = (new File(".")).getAbsolutePath();


    beginCacheXml();

    locatorPort = locPort;
    Properties config = getDistributedSystemProperties();
    config.put(DistributionConfig.ROLES_NAME, "");
    config.put(DistributionConfig.LOG_LEVEL_NAME, getDUnitLogLevel());
    config.put("log-file", "roleLossController.log");
    //creating the DS
    getSystem(config);

    MembershipAttributes ra = new MembershipAttributes(requiredRoles,
        LossAction.RECONNECT, ResumptionAction.NONE);

    AttributesFactory fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(Scope.DISTRIBUTED_ACK);

    RegionAttributes attr = fac.create();
    createRootRegion("MyRegion", attr);

    //writing the cachexml file.

    File file = new File("RoleReconnect-cache.xml");
    try {
      PrintWriter pw = new PrintWriter(new FileWriter(file), true);
      CacheXmlGenerator.generate(getCache(), pw);
      pw.close();
    }
    catch (IOException ex) {
      fail("IOException during cache.xml generation to " + file, ex);
    }
    closeCache();
    getSystem().disconnect();

    getLogWriter().info("disconnected from the system...");
    Host host = Host.getHost(0);

    VM vm0 = host.getVM(0);

    // Recreating from the cachexml.

    SerializableRunnable roleLoss = new CacheSerializableRunnable(
        "ROLERECONNECTTESTS") {
      public void run2() throws CacheException, RuntimeException
      {
        getLogWriter().fine("####### STARTING THE REAL TEST ##########");
        locatorPort = locPort;
        Properties props = getDistributedSystemProperties();
        props.put("cache-xml-file", xmlFileLoc+"/RoleReconnect-cache.xml");
        props.put("max-wait-time-reconnect", "200");
        final int timeReconnect = 3;
        props.put("max-num-reconnect-tries", "3");
        props.put(DistributionConfig.LOG_LEVEL_NAME, getDUnitLogLevel());
        props.put("log-file", "roleLossVM0.log");

        getSystem(props);

        addReconnectListener();
        
        system.getLogWriter().info("<ExpectedException action=add>" 
            + "CacheClosedException" + "</ExpectedException");
        try{
          getCache();
          throw new RuntimeException("The test should throw a CancelException ");
        }
        catch (CancelException ignor){ // can be caused by role loss during intialization.
          Log.getLogWriter().info("Got Expected CancelException ");
        }
        finally {
          system.getLogWriter().info("<ExpectedException action=remove>" 
              + "CacheClosedException" + "</ExpectedException");
        }
        getLogWriter().fine("roleLoss Sleeping SO call dumprun.sh");
        WaitCriterion ev = new WaitCriterion() {
          public boolean done() {
            return reconnectTries >= timeReconnect;
          }
          public String description() {
            return "Waiting for reconnect count " + timeReconnect + " currently " + reconnectTries;
          }
        };
        DistributedTestCase.waitForCriterion(ev, 60 * 1000, 200, true);
        getLogWriter().fine("roleLoss done Sleeping");
        assertEquals(timeReconnect,
            reconnectTries);
      }

    };

    vm0.invoke(roleLoss);


  }
  
     
  
  public static volatile int reconnectTries;
  
  public static volatile boolean initialized = false;
  
  public static volatile boolean initialRolePlayerStarted = false;
   
  //public static boolean rPut;
  public static Integer reconnectTries(){
    return new Integer(reconnectTries);
  }
  
  public static Boolean isInitialized(){
	  return new Boolean(initialized);
  }
  
  public static Boolean isInitialRolePlayerStarted(){
	  return new Boolean (initialRolePlayerStarted);
  }
  
  
  // See #50944 before enabling the test.  This ticket has been closed with wontFix
  // for the 2014 8.0 release.
  public void DISABLED_testReconnectWithRequiredRoleRegained()throws Throwable {

    final String rr1 = "RoleA";
    //final String rr2 = "RoleB";
    final String[] requiredRoles = { rr1 };
    //final boolean receivedPut[] = new boolean[1];

    final Integer[] numReconnect = new Integer[1];
    numReconnect[0] = new Integer(-1);
    final String myKey = "MyKey";
    final String myValue = "MyValue"; 
    final String regionName = "MyRegion";
    final int locPort = locatorPort;

    beginCacheXml();

    locatorPort = locPort;
    Properties config = getDistributedSystemProperties();
    config.put(DistributionConfig.ROLES_NAME, "");
    config.put(DistributionConfig.LOG_LEVEL_NAME, getDUnitLogLevel());
    //creating the DS
    getSystem(config);

    MembershipAttributes ra = new MembershipAttributes(requiredRoles,
        LossAction.RECONNECT, ResumptionAction.NONE);

    AttributesFactory fac = new AttributesFactory();
    fac.setMembershipAttributes(ra);
    fac.setScope(Scope.DISTRIBUTED_ACK);
    fac.setDataPolicy(DataPolicy.REPLICATE);

    RegionAttributes attr = fac.create();
    createRootRegion(regionName, attr);

    //writing the cachexml file.

    File file = new File("RoleRegained.xml");
    try {
      PrintWriter pw = new PrintWriter(new FileWriter(file), true);
      CacheXmlGenerator.generate(getCache(), pw);
      pw.close();
    }
    catch (IOException ex) {
      fail("IOException during cache.xml generation to " + file, ex);
    }
    closeCache();
    //disconnectFromDS();
    getSystem().disconnect(); //added

    // ################################################################### //
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    SerializableRunnable roleLoss = new CacheSerializableRunnable("roleloss runnable") {
      public void run2()
      {
        Thread t = null;
        try {
          //  closeCache();
          //  getSystem().disconnect();
          getLogWriter().info("####### STARTING THE REAL TEST ##########");
          WaitCriterion ev = new WaitCriterion() {
            public boolean done() {
              return ((Boolean)vm1.invoke(ReconnectDUnitTest.class, "isInitialRolePlayerStarted")).booleanValue();
            }
            public String description() {
              return null;
            }
          };
          DistributedTestCase.waitForCriterion(ev, 10 * 1000, 200, true);

          getLogWriter().info("Starting the test and creating the cache and regions etc ...");
          locatorPort = locPort;
          Properties props = getDistributedSystemProperties();
          props.put("cache-xml-file", "RoleRegained.xml");
          props.put("max-wait-time-reconnect", "3000");
          props.put("max-num-reconnect-tries", "8");
          props.put(DistributionConfig.LOG_LEVEL_NAME, getDUnitLogLevel());

          getSystem(props);
          system.getLogWriter().info("<ExpectedException action=add>" 
              + "CacheClosedException" + "</ExpectedException");

          disconnectFromDS();
          
          addReconnectListener();

          t = new Thread (){
            public void run(){
              WaitCriterion ev2 = new WaitCriterion() {
                public boolean done() {
                  if (reconnectTries == 0) {
                    return false;
                  }
                  return true;
                }
                public String description() {
                  return null;
                }
              };
              DistributedTestCase.waitForCriterion(ev2, 30 * 1000, 200, true);
            }
          };
          t.start();
          try {
            getCache();
            //	cache = CacheFactory.create(ds);
          }
          catch (CancelException ignor){
            // throws CCE when the roles are missing while initializing
            getLogWriter().info("Get CacheCloseException while creating the cache");	

          }

          initialized  = true;
          ev = new WaitCriterion() {
            public boolean done() {
              return reconnectTries != 0;
            }
            public String description() {
              return null;
            }
          };
          DistributedTestCase.waitForCriterion(ev, 30 * 1000, 200, true);

          getLogWriter().info("ReconnectTries=" + reconnectTries);
          //        long startTime = System.currentTimeMillis();

          ev = new WaitCriterion() {
            String excuse;
            public boolean done() {
              if (InternalDistributedSystem.getReconnectCount() != 0) {
                excuse = "reconnectCount is " + reconnectTries
                    + " waiting for it to be zero";
                return false;
              }
              Object key = null;
              Object value= null;
              Region.Entry keyValue = null;
              try {
                Cache cache = CacheFactory.getAnyInstance();
                if (cache == null) {
                  excuse = "no cache";
                  return false;
                }
                Region myRegion = cache.getRegion(regionName);
                if (myRegion == null) {
                  excuse = "no region";
                  return false;
                }

                Set keyValuePair = myRegion.entrySet();
                Iterator it = keyValuePair.iterator();
                while (it.hasNext()) {
                  keyValue = (Region.Entry)it.next();
                  key = keyValue.getKey();
                  value = keyValue.getValue();
                }
                if (key == null) {
                  excuse = "key is null";
                  return false;
                }
                if (!myKey.equals(key)) {
                  excuse = "key is wrong";
                  return false;
                }
                if (value == null) {
                  excuse = "value is null";
                  return false;
                }
                if (!myValue.equals(value)) {
                  excuse = "value is wrong";
                  return false;
                }
                getLogWriter().info("All assertions passed");
                getLogWriter().info("MyKey : "+key+" and myvalue : "+value);
                return true;
              }
              catch (CancelException ecc){ 
                // ignor the exception because the cache can be closed/null some times 
                // while in reconnect.
              }
              catch(RegionDestroyedException rex){

              }
              finally {
                getLogWriter().info("waiting for reconnect.  Current status is '"+excuse+"'");
              }
              return false;
            }
            public String description() {
              return excuse;
            }
          };

          DistributedTestCase.waitForCriterion(ev,  60 * 1000, 200, true); // was 5 * 60 * 1000

          Cache cache = CacheFactory.getAnyInstance();
          if (cache != null) {
            cache.getDistributedSystem().disconnect();
          }
        } 
        catch (VirtualMachineError e) {
          SystemFailure.initiateFailure(e);
          throw e;
        }
        catch (Error th) {
          getLogWriter().severe("DEBUG", th);
          throw th;
        } finally {
          if (t != null) {
            DistributedTestCase.join(t, 2 * 60 * 1000, getLogWriter());
          }
          // greplogs won't care if you remove an exception that was never added,
          // and this ensures that it gets removed.
          system.getLogWriter().info("<ExpectedException action=remove>" 
              + "CacheClosedException" + "</ExpectedException");
        }

      }

    }; // roleloss runnable

    // ################################################################## //    
    SerializableRunnable roleAPlayer = new CacheSerializableRunnable(
        "ROLEAPLAYER") {
      public void run2() throws CacheException
      {
        //closeCache();
        // getSystem().disconnect();
        locatorPort = locPort;
        Properties props = getDistributedSystemProperties();
        props.put(DistributionConfig.LOG_LEVEL_NAME, getDUnitLogLevel());
        props.put(DistributionConfig.ROLES_NAME, rr1);

        getSystem(props);
        getCache();
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(Scope.DISTRIBUTED_ACK);
        fac.setDataPolicy(DataPolicy.REPLICATE);

        RegionAttributes attr = fac.create();
        Region region = createRootRegion(regionName, attr);
        getLogWriter().info("STARTED THE REQUIREDROLES CACHE");
        try{
          Thread.sleep(120);
        }
        catch (Exception ee) {
          fail("interrupted");
        }

        region.put(myKey,myValue);
        try {
          Thread.sleep(5000); // why are we sleeping for 5 seconds here?
          // if it is to give time to avkVm0 to notice us we should have
          // him signal us that he has seen us and then we can exit.
        }
        catch(InterruptedException ee){
          fail("interrupted");
        }
        getLogWriter().info("RolePlayer is done...");


      }


    };

    SerializableRunnable roleAPlayerForCacheInitialization = new CacheSerializableRunnable(
        "ROLEAPLAYERInitializer") {
      public void run2() throws CacheException
      {
        //  closeCache();
        // getSystem().disconnect();
        locatorPort = locPort;
        Properties props = getDistributedSystemProperties();
        props.put(DistributionConfig.LOG_LEVEL_NAME, getDUnitLogLevel());
        props.put(DistributionConfig.ROLES_NAME, rr1);

        getSystem(props);
        getCache();
        AttributesFactory fac = new AttributesFactory();
        fac.setScope(Scope.DISTRIBUTED_ACK);
        fac.setDataPolicy(DataPolicy.REPLICATE);

        RegionAttributes attr = fac.create();
        createRootRegion(regionName, attr);
        getLogWriter().info("STARTED THE REQUIREDROLES CACHE");
        initialRolePlayerStarted = true;

        while(!((Boolean)vm0.invoke(ReconnectDUnitTest.class, "isInitialized")).booleanValue()){
          try{
            Thread.sleep(15);
          }catch(InterruptedException ignor){
            fail("interrupted");
          }
        }
        getLogWriter().info("RoleAPlayerInitializer is done...");
        closeCache();

      }


    };



    //#####################################################################//
    getLogWriter().info("starting roleAplayer, which will initialize, wait for "
        + "vm0 to initialize, and then close its cache to cause role loss");
    AsyncInvocation avkVm1 = vm1.invokeAsync(roleAPlayerForCacheInitialization);
    
    getLogWriter().info("starting role loss vm.  When the role is lost it wills start"
        + " trying to reconnect");
    final AsyncInvocation avkVm0 = vm0.invokeAsync(roleLoss);
    
    getLogWriter().info("waiting for role loss vm to start reconnect attempts");
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        if (!avkVm0.isAlive()) {
          return true;
        }
        Object res = vm0.invoke(ReconnectDUnitTest.class, "reconnectTries");
        if (((Integer)res).intValue() != 0) {
          return true;
        }
        return false;
      }
      public String description() {
        return "waiting for event";
      }
    };
    DistributedTestCase.waitForCriterion(ev, 120 * 1000, 200, true);

    VM vm2 = host.getVM(2);
    if (avkVm0.isAlive()) {
      getLogWriter().info("starting roleAPlayer in a different vm."
          + "  After this reconnect should succeed in vm0");
      vm2.invoke(roleAPlayer);

      //      long startTime = System.currentTimeMillis();
      /*
      while (numReconnect[0].intValue() > 0){
        if((System.currentTimeMillis()-startTime )> 120000)
          fail("The test failed because the required role not satisfied" +
               "and the number of reconnected tried is not set to zero for " +
               "more than 2 mins");
        try{
          Thread.sleep(15);
        }catch(Exception ee){
          getLogWriter().severe("Exception : "+ee);
        }
      }*/
      getLogWriter().info("waiting for vm0 to finish reconnecting");
      DistributedTestCase.join(avkVm0, 120 * 1000, getLogWriter());
    }

    if (avkVm0.getException() != null){
      fail("Exception in Vm0", avkVm0.getException());
    }

    DistributedTestCase.join(avkVm1, 30 * 1000, getLogWriter());
    if (avkVm1.getException() != null){
      fail("Exception in Vm1", avkVm1.getException());
    }

  }
  
  void addReconnectListener() {
    reconnectTries = 0; // reset the count for this listener
    getLogWriter().info("adding reconnect listener");
    ReconnectListener reconlis = new ReconnectListener() {
      public void reconnecting(InternalDistributedSystem oldSys) {
        getLogWriter().info("reconnect listener invoked");
        reconnectTries++;
      }
      public void onReconnect(InternalDistributedSystem system1, InternalDistributedSystem system2) {}
    };
    InternalDistributedSystem.addReconnectListener(reconlis);
  }
  
  private void waitTimeout() throws InterruptedException
  {
    Thread.sleep(500);

  }
  public boolean forceDisconnect(VM vm) {
    return (Boolean)vm.invoke(new SerializableCallable("crash distributed system") {
      public Object call() throws Exception {
        final DistributedSystem msys = InternalDistributedSystem.getAnyInstance();
        final Locator oldLocator = Locator.getLocator();
//        MembershipManagerHelper.inhibitForcedDisconnectLogging(true);
        MembershipManagerHelper.playDead(msys);
        JChannel c = MembershipManagerHelper.getJChannel(msys);
        Protocol udp = c.getProtocolStack().findProtocol("UDP");
//        udp.stop();
        udp.passUp(new Event(Event.EXIT, new ForcedDisconnectException("killing member's ds")));
//        try {
//          MembershipManagerHelper.getJChannel(msys).waitForClose();
//        }
//        catch (InterruptedException ie) {
//          Thread.currentThread().interrupt();
//          // attempt rest of work with interrupt bit set
//        }
//        MembershipManagerHelper.inhibitForcedDisconnectLogging(false);
        if (oldLocator != null) {
          WaitCriterion wc = new WaitCriterion() {
            public boolean done() {
              return ((InternalLocator)oldLocator).isStopped();
            }
            public String description() {
              return "waiting for locator to stop: " + oldLocator;
            }
          };
          waitForCriterion(wc, 10000, 50, true);
        }
        return true;
      }
    });
  }
  
  private static int getPID() {
    String name = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();            
    int idx = name.indexOf('@'); 
    try {             
      return Integer.parseInt(name.substring(0,idx));
    } catch(NumberFormatException nfe) {
      //something changed in the RuntimeMXBean name
    }                         
    return 0;
  }

}
