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
package com.gemstone.gemfire.internal.cache.rollingupgrade;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import junit.framework.AssertionFailedError;

import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.FileUtil;
import com.gemstone.gemfire.internal.shared.Version;
import com.gemstone.gemfire.internal.cache.LocalRegion;

import dunit.AsyncInvocation;
import dunit.DistributedTestCase;
import dunit.Host;
import dunit.RMIException;
import dunit.VM;
import dunit.eclipse.DUnitLauncher;

/**
 * RollingUpgrade dunit tests are distributed among subclasses of
 * RollingUpgradeDUnitTest to avoid spurious "hangs" being declared
 * by Hydra.  The base class tests rolling upgrade from the last
 * major revision and subclasses should test upgrading from intervening
 * minor revisions.
 *
 * This test will not run properly in eclipse at this point due to having to bounce vms
 * Currently, bouncing vms is necessary because we are starting gemfire with 
 * different class loaders and the class loaders are lingering (possibly due to
 * daemon threads - the vm itself is still running)
 * 
 * Note: to run in eclipse, I had to copy over the jg-magic-map.txt file into 
 * my GEMFIRE_OUTPUT location, in the same directory as #MagicNumberReader
 * otherwise the two systems were unable to talk to one another due to one 
 * using a magic number and the other not.
 * Also turnOffBounce will need to be set to true so that bouncing a vm doesn't lead
 * to a NPE.
 * 
 * @author jhuynh
 *
 */
public class RollingUpgradeDUnitTest extends DistributedTestCase {

  //just a test flag that can be set when trying to run a test in eclipse and avoiding IllegalState or NPE due to bouncing
  private boolean turnOffBounce = false;

  private File[] testingDirs = new File[3]; 
  
  //This will be the classloader for each specific VM if the 
  //VM requires a classloader to load an older gemfire jar version/tests
  //Use this classloader to obtain a #RollingUpgradeUtils class and 
  //execute the helper methods from that class
  private static ClassLoader classLoader;
  
  private static String diskDir = "RollingUpgradeDUnitTest";

  // initialized in a static initializer below
  protected static LocationAndOrdinal[] gemfireLocations;
  
  //Each vm will have a cache object
  private static Object cache;
  

  @Override
  public void setUp() throws Exception {
    super.setUp();
    
    System.out.println("RollingUpgradeDUnitTest.setUp invoked");
    
    // Windows machines should mount \\samba-bvt.gemstone.com\gcm on drive J:
    String jarRootDir = "/gcm/where";
    String osDir = "/Linux";
    String os = System.getProperty("os.name");
    if (os != null) {
        if (os.indexOf("Windows") != -1) {
            jarRootDir = "J:/where";
            osDir = "/Windows_NT";
        }
        else if (os.indexOf("SunOS") != -1) {
          osDir = "Solaris";
        }
    }
    
    //Add locations of previous releases of gemfire
    //For testing, other build locations for gemfire can be added
    gemfireLocations = new LocationAndOrdinal[]{
      new LocationAndOrdinal(jarRootDir+"/gemfireXD/releases/GemFireXD1.0.0-all/product", Version.GFXD_10.ordinal()),
      };    
  }
  
   protected void basicSetUp() throws Exception {
     super.setUp();
   }
  
  
  public RollingUpgradeDUnitTest(String name) {
    super(name);
  }
  
  public void bounceAll(VM...vms) {
    if (!turnOffBounce) {
      for (VM vm:vms) {
        vm.bounce();
      }
    }
  }

  /**
   * Demonstrate that a process using an old version of the product
   * can't join a system that has begun upgrading.
   */
  public void testOldMemberCantJoin() throws Exception {
    VM oldServer = Host.getHost(0).getVM(1);
    int length = gemfireLocations.length;
    Properties props = getSystemProperties(); // uses the DUnit locator
    for (int i = 0; i < length; i++) {
      if (gemfireLocations[i].ordinal < Version.CURRENT_ORDINAL) {
        oldServer.invoke(configureClassLoader(gemfireLocations[i].gemfireLocation));
        try {
          oldServer.invoke(invokeCreateCache(props));
        } catch (RMIException e) {
          Throwable cause = e.getCause();
          if (cause != null && (cause instanceof AssertionFailedError)) {
            cause = cause.getCause();
            if (cause != null && !cause.getMessage().startsWith("Rejecting the attempt of a member using an older version")) {
              throw e;
            } else {
              getLogWriter().info("found rejection message in throwable");
            }
          }
        } finally {
          oldServer.invoke(configureClassLoaderForCurrent());
        }
      }
    }
  }

  public void testRollSingleLocatorWithMultipleServersReplicatedRegion() throws Exception {
    System.out.println("gemfireLocations[0].version="+gemfireLocations[0].ordinal);
    int length = gemfireLocations.length;
    for (int i = 0; i < length; i++) {
      doTestRollSingleLocatorWithMultipleServers(false, gemfireLocations[i]);
    }
  }

  //1 locator, 3 servers
  public void doTestRollSingleLocatorWithMultipleServers(boolean partitioned, LocationAndOrdinal oldLocationAndOrdinal) throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM server2 = host.getVM(1);
    final VM server3 = host.getVM(2);
    final VM server4 = host.getVM(3);
    
    final String objectType = "strings";
    final String regionName = "aRegion";
    
    String shortcutName = RegionShortcut.REPLICATE.name();
    if (partitioned) {
      shortcutName = RegionShortcut.PARTITION_REDUNDANT.name();
    }
    
    String oldVersionLocation = oldLocationAndOrdinal.gemfireLocation;
    short oldOrdinal = oldLocationAndOrdinal.ordinal;
    int[] locatorPorts = AvailablePortHelper.getRandomAvailableTCPPorts(1);
    
    //configure all class loaders for each vm
    server1.invoke(configureClassLoader(oldVersionLocation));
    server2.invoke(configureClassLoader(oldVersionLocation));
    server3.invoke(configureClassLoader(oldVersionLocation));
    server4.invoke(configureClassLoader(oldVersionLocation));
    
    String hostName =  getServerHostName(host);
    String locatorString = getLocatorString(locatorPorts);
    try {
      server1.invoke(invokeStartLocator(hostName, locatorPorts[0], testName, locatorString));
      invokeRunnableInVMs(invokeCreateCache(getSystemProperties(locatorPorts)), server2, server3, server4);
      invokeRunnableInVMs(invokeAssertVersion(oldOrdinal), server2, server3, server4);
      invokeRunnableInVMs(invokeCreateRegion(regionName, shortcutName), server2, server3, server4);

      putAndVerify(objectType, server2, regionName, 0, 10, server3, server4);
      rollLocatorToCurrent(server1, hostName, locatorPorts[0], testName, locatorString);
      
      rollServerToCurrentAndCreateRegion(server2, shortcutName, regionName, locatorPorts);
      putAndVerify(objectType, server2, regionName, 5, 15, server3, server4);
      putAndVerify(objectType, server3, regionName, 10, 20, server2, server4);
        
      rollServerToCurrentAndCreateRegion(server3, shortcutName, regionName, locatorPorts);
      putAndVerify(objectType, server2, regionName, 15, 25, server3, server4);
      putAndVerify(objectType, server3, regionName, 20, 30, server2, server4);
      
      rollServerToCurrentAndCreateRegion(server4, shortcutName, regionName, locatorPorts);
      putAndVerify(objectType, server4, regionName, 25, 35, server3, server2);
      putAndVerify(objectType, server3, regionName, 30, 40, server2, server4);
      
    } finally {
      invokeRunnableInVMs(true, invokeStopLocator(), server1);
      invokeRunnableInVMs(true, invokeCloseCache(), server2, server3, server4);
      invokeRunnableInVMs(true, resetClassLoader(), server1, server2, server3, server4);
      bounceAll(server1, server2, server3, server4);
    }
  }
  
  //Replicated regions
  public void testRollTwoLocatorsWithTwoServers() throws Exception {
    int length = gemfireLocations.length;
    for (int i = 0; i < length; i++) {
      doTestRollTwoLocatorsWithTwoServers(false, gemfireLocations[i]);
    }
  }
  
  //2 locator, 2 servers
  public void doTestRollTwoLocatorsWithTwoServers(boolean partitioned, LocationAndOrdinal oldLocationAndOrdinal) throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM server2 = host.getVM(1);
    final VM server3 = host.getVM(2);
    final VM server4 = host.getVM(3);
    
    final String objectType = "strings";
    final String regionName = "aRegion";
    
    String shortcutName = RegionShortcut.REPLICATE.name();
    if (partitioned) {
      shortcutName = RegionShortcut.PARTITION_REDUNDANT.name();
    }
    
    String oldVersionLocation = oldLocationAndOrdinal.gemfireLocation;
    short oldOrdinal = oldLocationAndOrdinal.ordinal;
    int[] locatorPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    
    //configure all class loaders for each vm
    server1.invoke(configureClassLoader(oldVersionLocation));
    server2.invoke(configureClassLoader(oldVersionLocation));
    server3.invoke(configureClassLoader(oldVersionLocation));
    server4.invoke(configureClassLoader(oldVersionLocation));
    
    String hostName =  getServerHostName(host);
    String locatorString = getLocatorString(locatorPorts);
    try {
      server1.invoke(invokeStartLocator(hostName, locatorPorts[0], testName, locatorString));
      server2.invoke(invokeStartLocator(hostName, locatorPorts[1], testName, locatorString));
      invokeRunnableInVMs(invokeCreateCache(getSystemProperties(locatorPorts)), server3, server4);
      invokeRunnableInVMs(invokeAssertVersion(oldOrdinal), server3, server4);
      invokeRunnableInVMs(invokeCreateRegion(
          regionName, shortcutName), server3, server4);

      putAndVerify(objectType, server3, regionName, 0, 10, server3, server4);
      rollLocatorToCurrent(server1, hostName, locatorPorts[0], testName, locatorString);
      
      rollLocatorToCurrent(server2, hostName, locatorPorts[1], testName, locatorString);

      rollServerToCurrentAndCreateRegion(server3, shortcutName, regionName, locatorPorts);
      putAndVerify(objectType, server4, regionName, 15, 25, server3, server4);
      putAndVerify(objectType, server3, regionName, 20, 30, server3, server4);
      
      rollServerToCurrentAndCreateRegion(server4, shortcutName, regionName, locatorPorts);
      putAndVerify(objectType, server4, regionName, 25, 35, server3, server4);
      putAndVerify(objectType, server3, regionName, 30, 40, server3, server4);
      
    } finally {
      invokeRunnableInVMs(true, invokeStopLocator(), server1, server2);
      invokeRunnableInVMs(true, invokeCloseCache(), server3, server4);
      invokeRunnableInVMs(true, resetClassLoader(), server1, server2, server3, server4);
      bounceAll(server1, server2, server3, server4);
    }
  }
  
  public void testClients() throws Exception {
    int length = gemfireLocations.length;
    for (int i = 0; i < length; i++) {
      doTestClients(false, gemfireLocations[i]);
    }
  }
  
  public void doTestClients(boolean partitioned, LocationAndOrdinal oldLocationAndOrdinal) throws Exception {
    final Host host = Host.getHost(0);
    final VM locator = host.getVM(0);
    final VM server2 = host.getVM(1);
    final VM server3 = host.getVM(2);
    final VM client = host.getVM(3);
    
    final String objectType = "strings";
    final String regionName = "aRegion";
    
    String shortcutName = RegionShortcut.REPLICATE.name();
    if (partitioned) {
      shortcutName = RegionShortcut.PARTITION_REDUNDANT.name();
    }
    
    String oldVersionLocation = oldLocationAndOrdinal.gemfireLocation;
    short oldOrdinal = oldLocationAndOrdinal.ordinal;
    int[] ports = AvailablePortHelper.getRandomAvailableTCPPorts(3);
    int[] locatorPorts = new int[] {ports[0]};
    int[] csPorts = new int[] {ports[1], ports[2]};
    
    //configure all class loaders for each vm
    locator.invoke(configureClassLoader(oldVersionLocation));
    server2.invoke(configureClassLoader(oldVersionLocation));
    server3.invoke(configureClassLoader(oldVersionLocation));
    client.invoke(configureClassLoader(oldVersionLocation));
    
    String hostName =  getServerHostName(host);
    String[] hostNames = new String[] { hostName };
    String locatorString = getLocatorString(locatorPorts);
    try {
      locator.invoke(invokeStartLocator(hostName, locatorPorts[0], testName, locatorString));
      invokeRunnableInVMs(invokeCreateCache(getSystemProperties(locatorPorts)),server2, server3);
      invokeRunnableInVMs(invokeStartCacheServer( csPorts[0]),server2);
      invokeRunnableInVMs(invokeStartCacheServer( csPorts[1]),server3);

      invokeRunnableInVMs(invokeCreateClientCache(getClientSystemProperties(), hostNames, locatorPorts), client);
      invokeRunnableInVMs(invokeAssertVersion(oldOrdinal), server2, server3, client);
      invokeRunnableInVMs(invokeCreateRegion(
          regionName, shortcutName), server2, server3);
      invokeRunnableInVMs(invokeCreateClientRegion(
          regionName, ClientRegionShortcut.PROXY.name()), client);

      putAndVerify(objectType, client, regionName, 0, 10, server3, client);
      putAndVerify(objectType, server3, regionName, 100, 110, server3, client);
      rollLocatorToCurrent(locator, hostName, locatorPorts[0], testName, locatorString);
      
      rollServerToCurrentAndCreateRegion(server3, shortcutName, regionName, locatorPorts);
      invokeRunnableInVMs(invokeStartCacheServer( csPorts[1]),server3);
      putAndVerify(objectType, client, regionName, 15, 25, server3, client);
      putAndVerify(objectType, server3, regionName, 20, 30, server3, client);
      
      rollServerToCurrentAndCreateRegion(server2, shortcutName, regionName, locatorPorts);
      invokeRunnableInVMs(invokeStartCacheServer( csPorts[0]),server2);
      putAndVerify(objectType, client, regionName, 25, 35, server2, client);
      putAndVerify(objectType, server2, regionName, 30, 40, server3, client);
      
      rollClientToCurrentAndCreateRegion(client, ClientRegionShortcut.PROXY.name(), regionName, hostNames, locatorPorts);
      putAndVerify(objectType, client, regionName, 35, 45, server2, server3);
      putAndVerify(objectType, server2, regionName, 40, 50, server3, client);
      
    } finally {
      invokeRunnableInVMs(true, invokeStopLocator(), locator);
      invokeRunnableInVMs(true, invokeCloseCache(), server2, server3, client);
      invokeRunnableInVMs(true, resetClassLoader(), locator, server2, server3, client);
      bounceAll(locator, server2, server3, client);
    }
  }
  
  
  //starts 3 locators and 1 server
  //rolls all 3 locators and then the server
  public void testRollLocatorsWithOldServer() throws Exception {
    int length = gemfireLocations.length;
    for (int i = 0; i < length; i++) {
      doTestRollLocatorsWithOldServer(gemfireLocations[i]);
    }
  }
  
  public void doTestRollLocatorsWithOldServer(LocationAndOrdinal oldLocationAndOrdinal) throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM server2 = host.getVM(1);
    final VM server3 = host.getVM(2);
    final VM server4 = host.getVM(3);
   
    String oldVersionLocation = oldLocationAndOrdinal.gemfireLocation;
    short oldOrdinal = oldLocationAndOrdinal.ordinal;
    int[] locatorPorts = AvailablePortHelper.getRandomAvailableTCPPorts(3);
          
    //configure all class loaders for each vm
    server1.invoke(configureClassLoader(oldVersionLocation));
    server2.invoke(configureClassLoader(oldVersionLocation));
    server3.invoke(configureClassLoader(oldVersionLocation));
    server4.invoke(configureClassLoader(oldVersionLocation));
    
    String hostName =  getServerHostName(host);
    String locatorString = getLocatorString(locatorPorts);
    try {
      server1.invoke(invokeStartLocator(hostName, locatorPorts[0], testName, locatorString));
      server2.invoke(invokeStartLocator(hostName, locatorPorts[1], testName, locatorString));
      server3.invoke(invokeStartLocator(hostName, locatorPorts[2], testName, locatorString));
      invokeRunnableInVMs(invokeCreateCache( getSystemProperties(locatorPorts)), server4);
      invokeRunnableInVMs(invokeAssertVersion( oldOrdinal), server4);

      rollLocatorToCurrent(server1, hostName, locatorPorts[0], testName, locatorString);  
      rollLocatorToCurrent(server2, hostName, locatorPorts[1], testName, locatorString);  
      rollLocatorToCurrent(server3, hostName, locatorPorts[2], testName, locatorString);  
      
    } finally {
      invokeRunnableInVMs(true, invokeStopLocator(), server1, server2, server3);
      invokeRunnableInVMs(true, invokeCloseCache(), server4);
      invokeRunnableInVMs(true, resetClassLoader(), server1, server2, server3, server4);
      bounceAll(server1, server2, server3, server4);
    }
  }
  
  
  //A test that will start 4 locators and rolls each one
  public void testRollLocators() throws Exception{
    int length = gemfireLocations.length;
    for (int i = 0; i < length; i++) {
      doTestRollLocators(gemfireLocations[i]);
    }
  }
  
  public void doTestRollLocators( LocationAndOrdinal oldLocationAndOrdinal ) throws Exception {
      final Host host = Host.getHost(0);
      final VM server1 = host.getVM(0);
      final VM server2 = host.getVM(1);
      final VM server3 = host.getVM(2);
      final VM server4 = host.getVM(3);
      
      String oldVersionLocation = oldLocationAndOrdinal.gemfireLocation;
      int[] locatorPorts = AvailablePortHelper.getRandomAvailableTCPPorts(4);
            
      //configure all class loaders for each vm
      server1.invoke(configureClassLoader(oldVersionLocation));
      server2.invoke(configureClassLoader(oldVersionLocation));
      server3.invoke(configureClassLoader(oldVersionLocation));
      server4.invoke(configureClassLoader(oldVersionLocation));
      
      String hostName =  getServerHostName(host);
      String locatorString = getLocatorString(locatorPorts);
      try {
        server1.invoke(invokeStartLocator( hostName, locatorPorts[0], testName, locatorString));
        server2.invoke(invokeStartLocator( hostName, locatorPorts[1], testName, locatorString));
        server3.invoke(invokeStartLocator( hostName, locatorPorts[2], testName, locatorString));
        server4.invoke(invokeStartLocator( hostName, locatorPorts[3], testName, locatorString));

        rollLocatorToCurrent(server1, hostName, locatorPorts[0], testName, locatorString);  
        rollLocatorToCurrent(server2, hostName, locatorPorts[1], testName, locatorString);  
        rollLocatorToCurrent(server3, hostName, locatorPorts[2], testName, locatorString);  
        rollLocatorToCurrent(server4, hostName, locatorPorts[3], testName, locatorString);  
        
      } finally {
        invokeRunnableInVMs(true, invokeStopLocator(), server1, server2, server3, server4);
        invokeRunnableInVMs(true, resetClassLoader(), server1, server2, server3, server4);
        bounceAll(server1, server2, server3, server4);
      }
    
  }
  
  //Starts 2 servers with old classloader
  //puts in one server while the other bounces
  //verifies values are present in bounced server
  //puts in the newly started/bounced server and bounces the other server
  //verifies values are present in newly bounced server
  public void testConcurrentPutsReplicated() throws Exception {
    int length = gemfireLocations.length;
    for (int i = 0; i < length; i++) {
      doTestConcurrent(false, gemfireLocations[i]);
    }
  }
 
  public void doTestConcurrent(boolean partitioned, LocationAndOrdinal oldLocationAndOrdinal) throws Exception {
    final Host host = Host.getHost(0);
    final VM locator = host.getVM(1);
    final VM server1 = host.getVM(2);
    final VM server2 = host.getVM(3);
    
    final String objectType = "strings";
    final String regionName = "aRegion";
    
    String shortcutName = RegionShortcut.REPLICATE.name();
    if (partitioned) {
      shortcutName = RegionShortcut.PARTITION_REDUNDANT.name();
    }
    
    String oldVersionLocation = oldLocationAndOrdinal.gemfireLocation;
    short oldOrdinal = oldLocationAndOrdinal.ordinal;
    int[] locatorPorts = AvailablePortHelper.getRandomAvailableTCPPorts(1);
    String hostName =  getServerHostName(host);
    String locatorString = getLocatorString(locatorPorts);
    
    //configure all class loaders for each vm
    locator.invoke(configureClassLoader(oldVersionLocation));
    server1.invoke(configureClassLoader(oldVersionLocation));
    server2.invoke(configureClassLoader(oldVersionLocation));
    
    try {
      locator.invoke(invokeStartLocator( hostName, locatorPorts[0], testName, locatorString, getLocatorProperties(locatorString)));
      invokeRunnableInVMs(invokeCreateCache( getSystemProperties(locatorPorts)), server1, server2);
      invokeRunnableInVMs(invokeAssertVersion(oldOrdinal), server1, server2);
      // create region
      invokeRunnableInVMs(invokeCreateRegion(
          regionName, shortcutName), server1,  server2);
      
      //async puts through server 2
      AsyncInvocation asyncPutsThroughOld = server2.invokeAsync(new CacheSerializableRunnable("async puts") {
        public void run2() {
          try {
            for (int i = 0; i < 500; i++) {
              put(RollingUpgradeDUnitTest.cache, regionName, "" + i, "VALUE(" + i + ")");
            }
          }
          catch (Exception e) {
            fail("error putting");
          }
        }
      });
      rollLocatorToCurrent(locator, hostName, locatorPorts[0], testName, locatorString);
      
      rollServerToCurrentAndCreateRegion(server1, shortcutName, regionName, locatorPorts);
      DistributedTestCase.join(asyncPutsThroughOld, 30000, getLogWriter());
      
      //verifyValues in server1
      verifyValues(objectType, regionName, 0, 500, server1);
       
      //aync puts through server 1
      AsyncInvocation asyncPutsThroughNew = server1.invokeAsync(new CacheSerializableRunnable("async puts") {
        public void run2() {
          try {
            for (int i = 250; i < 750; i++) {
              put(RollingUpgradeDUnitTest.cache, regionName, "" + i, "VALUE(" + i + ")");
            }
          }
          catch (Exception e) {
            fail("error putting");
          }
        }
      });
      rollServerToCurrentAndCreateRegion(server2, shortcutName, regionName, locatorPorts);
      DistributedTestCase.join(asyncPutsThroughNew, 30000, getLogWriter());
      
      //verifyValues in server2
      verifyValues(objectType, regionName, 250, 750, server2);    
      
    } finally {
      invokeRunnableInVMs(true, invokeStopLocator(), locator);
      invokeRunnableInVMs(true, invokeCloseCache(), server1, server2);
      invokeRunnableInVMs(true, resetClassLoader(), server1, server2);
      bounceAll(server1, server2, locator);
    }
  }

  public void testRollServersOnReplicatedRegion() throws Exception {
    int length = gemfireLocations.length;
    for (int i = 0; i < length; i++) {
      doTestRollAll("replicate", "strings", gemfireLocations[i]);
      doTestRollAll("replicate", "serializable", gemfireLocations[i]);
      doTestRollAll("replicate", "dataserializable", gemfireLocations[i]);
    }
  }

  public void testRollServersOnPartitionedRegion() throws Exception {
    int length = gemfireLocations.length;
    for (int i = 0; i < length; i++) {
      doTestRollAll("partitionedRedundant", "strings", gemfireLocations[i]);
      doTestRollAll("partitionedRedundant", "serializable", gemfireLocations[i]);
      doTestRollAll("partitionedRedundant", "dataserializable", gemfireLocations[i]);
    }
  }

  public void testRollServersOnPersistentRegion() throws Exception {
    int length = gemfireLocations.length;
    for (int i = 0; i < length; i++) {
      doTestRollAll("persistentReplicate", "strings", gemfireLocations[i]);
      //doTestRollAll("persistentReplicate", "serializable", gemfireLocations[i]);
      //doTestRollAll("persistentReplicate", "dataserializable", gemfireLocations[i]);
    }
  }
  //We start an "old" locator and old servers
  //We roll the locator
  //Now we roll all the servers from old to new
  public void doTestRollAll(String regionType, String objectType, LocationAndOrdinal oldLocationAndOrdinal) throws Exception {
    final Host host = Host.getHost(0);
    final VM server1 = host.getVM(0);
    final VM server2 = host.getVM(1);
    final VM server3 = host.getVM(2);
    final VM locator = host.getVM(3);

    
    String regionName = "aRegion";
    String shortcutName = RegionShortcut.REPLICATE.name();
    if(regionType.equals("replicate")) {
      shortcutName = RegionShortcut.REPLICATE.name();
    } else if ((regionType.equals("partitionedRedundant"))) {
      shortcutName = RegionShortcut.PARTITION_REDUNDANT.name();
    } else if ((regionType.equals("persistentReplicate"))) {
      shortcutName = RegionShortcut.PARTITION_PERSISTENT.name();
      for(int i=0 ; i < testingDirs.length; i++){
        testingDirs[i] = new File(diskDir, "diskStoreVM_" + String.valueOf(host.getVM(i).getPid())).getAbsoluteFile();
        if(!testingDirs[i].exists()) {
           System.out.println(" Creating diskdir for server: " +  i );
           testingDirs[i].mkdirs();
        }
      }
    }
    
    String oldVersionLocation = oldLocationAndOrdinal.gemfireLocation;
    short oldOrdinal = oldLocationAndOrdinal.ordinal;
    int[] locatorPorts = AvailablePortHelper.getRandomAvailableTCPPorts(1);
    String hostName =  getServerHostName(host);
    String locatorString = getLocatorString(locatorPorts);
    final Properties locatorProps = new Properties();
    //configure all class loaders for each vm
    locator.invoke(configureClassLoader(oldVersionLocation));
    server1.invoke(configureClassLoader(oldVersionLocation));
    server2.invoke(configureClassLoader(oldVersionLocation));
    server3.invoke(configureClassLoader(oldVersionLocation));
    
    try {
      locator.invoke(invokeStartLocator( hostName, locatorPorts[0], testName, locatorString, locatorProps));
      invokeRunnableInVMs(invokeCreateCache( getSystemProperties(locatorPorts)), server1, server2, server3);
      invokeRunnableInVMs(invokeAssertVersion(oldOrdinal), server1, server2, server3);
      // create region
      if ((regionType.equals("persistentReplicate"))) {
        for(int i = 0 ; i < testingDirs.length ; i++){
          CacheSerializableRunnable runnable = invokeCreatePersistentReplicateRegion( regionName, testingDirs[i]);
          invokeRunnableInVMs(runnable, host.getVM(i));
        }
      } else {
        invokeRunnableInVMs(invokeCreateRegion(regionName, shortcutName), server1,  server2, server3);
      }
      
      putAndVerify(objectType, server1, regionName, 0, 10, server2, server3);
      rollLocatorToCurrent(locator, hostName, locatorPorts[0], testName, locatorString);
      
      rollServerToCurrentAndCreateRegion(server1, regionType, testingDirs[0], shortcutName, regionName, locatorPorts);
      verifyValues(objectType, regionName, 0, 10, server1);
      putAndVerify(objectType, server1, regionName,5, 15, server2, server3);
      putAndVerify(objectType, server2, regionName,10, 20, server1, server3);
      
      rollServerToCurrentAndCreateRegion(server2, regionType, testingDirs[1], shortcutName, regionName, locatorPorts);
      verifyValues(objectType, regionName, 0, 10, server2);
      putAndVerify(objectType, server2, regionName,15, 25, server1, server3);
      putAndVerify(objectType, server3, regionName,20, 30, server2, server3);
      
      rollServerToCurrentAndCreateRegion(server3, regionType, testingDirs[2], shortcutName, regionName, locatorPorts);
      verifyValues(objectType, regionName, 0, 10, server3);
      putAndVerify(objectType, server3, regionName,15, 25, server1, server2);
      putAndVerify(objectType, server1, regionName,20, 30, server1, server2, server3);
         
      
    } finally {
      invokeRunnableInVMs(true, invokeStopLocator(), locator);
      invokeRunnableInVMs(true, invokeCloseCache(), server1, server2, server3);
      invokeRunnableInVMs(true, resetClassLoader(), server1, server2, server3);
      bounceAll(server1, server2, server3, locator);
      if ((regionType.equals("persistentReplicate"))) {
        deleteDiskStores();
      }
    }
  }
  
  public void testPutAndGetMixedServersReplicateRegion() throws Exception {
    int length = gemfireLocations.length;
    for (int i = 0; i < length; i++) {
      doTestPutAndGetMixedServers("strings", false, gemfireLocations[i]);
      doTestPutAndGetMixedServers("serializable", false, gemfireLocations[i]);
      doTestPutAndGetMixedServers("dataserializable", false, gemfireLocations[i]);
    }
  }
  
  public void testPutAndGetMixedServerPartitionedRegion() throws Exception {
    int length = gemfireLocations.length;
    for (int i = 0; i < length; i++) {
      doTestPutAndGetMixedServers("strings", true, gemfireLocations[i]);
      doTestPutAndGetMixedServers("serializable", true, gemfireLocations[i]);
      doTestPutAndGetMixedServers("dataserializable", true, gemfireLocations[i]);
    }
  }
  
  /**
   * This test starts up multiple servers from the current code base
   * and multiple servers from the old version and executes
   * puts and gets on a new server and old server and verifies that the
   * results are present.  Note that the puts have overlapping region keys
   * just to test new puts and replaces
   */
  public void doTestPutAndGetMixedServers(String objectType, boolean partitioned, LocationAndOrdinal oldLocationAndOrdinal) throws Exception {
    final Host host = Host.getHost(0);
    final VM currentServer1 = host.getVM(0);
    final VM oldServer = host.getVM(1);
    final VM currentServer2 = host.getVM(2);
    final VM oldServer2 = host.getVM(3);
    
    String regionName = "aRegion";
    String oldVersionLocation = oldLocationAndOrdinal.gemfireLocation;
    short oldOrdinal = oldLocationAndOrdinal.ordinal;
    
    String shortcutName = RegionShortcut.REPLICATE.name();
    if (partitioned) {
      shortcutName = RegionShortcut.PARTITION.name();
    }
    
    //configure all class loaders for each vm
    currentServer1.invoke(configureClassLoaderForCurrent());
    currentServer2.invoke(configureClassLoaderForCurrent());
    oldServer.invoke(configureClassLoader(oldVersionLocation));
    oldServer2.invoke(configureClassLoader(oldVersionLocation));
    
    try {
      Properties props = getSystemProperties();
      int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
      props.put(DistributionConfig.LOCATORS_NAME, host.getHostName()+"["+port+"]");
      
      invokeRunnableInVMs(invokeStartLocatorAndServer(host.getHostName(), 
          port, testName, props), oldServer);
      invokeRunnableInVMs(invokeCreateCache(props), currentServer1, currentServer2, oldServer2);

      currentServer1.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));
      currentServer2.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));
      oldServer.invoke(invokeAssertVersion(oldOrdinal));
      oldServer2.invoke(invokeAssertVersion(oldOrdinal));
      
      // create region
      invokeRunnableInVMs(invokeCreateRegion(
           regionName, shortcutName), currentServer1, currentServer2, oldServer, oldServer2);

      putAndVerify(objectType, currentServer1, regionName, 0, 10, currentServer2, oldServer, oldServer2);
      putAndVerify(objectType, oldServer, regionName, 5, 15, currentServer1, currentServer2, oldServer2);
      
    } 
    finally {
      invokeRunnableInVMs(true, invokeCloseCache(), currentServer1, currentServer2, oldServer, oldServer2);
      invokeRunnableInVMs(true, resetClassLoader(), currentServer1, currentServer2, oldServer, oldServer2);
      bounceAll(currentServer1, oldServer, currentServer2, oldServer2);
    }
  }
  
  public void testQueryMixedServersOnReplicatedRegions() throws Exception {
    int length = gemfireLocations.length;
    for (int i = 0; i < length; i++) {
      doTestQueryMixedServers(false, gemfireLocations[i]);
    }
  }
  
  public void testQueryMixedServersOnPartitionedRegions() throws Exception {
    int length = gemfireLocations.length;
    for (int i = 0; i < length; i++) {
      doTestQueryMixedServers(true, gemfireLocations[i]);
    }
  }
  
  public void doTestQueryMixedServers(boolean partitioned, LocationAndOrdinal oldLocationAndOrdinal) throws Exception {
    final Host host = Host.getHost(0);
    final VM currentServer1 = host.getVM(0);
    final VM oldServer = host.getVM(1);
    final VM currentServer2 = host.getVM(2);
    final VM oldServerAndLocator = host.getVM(3);
    
    String regionName = "cqs";
    String oldVersionLocation = oldLocationAndOrdinal.gemfireLocation;
    short oldOrdinal = oldLocationAndOrdinal.ordinal;
    
    String shortcutName = RegionShortcut.REPLICATE.name();
    if (partitioned) {
      shortcutName = RegionShortcut.PARTITION.name();
    }
    
    //configure all class loaders for each vm
    currentServer1.invoke(configureClassLoaderForCurrent());
    currentServer2.invoke(configureClassLoaderForCurrent());
    oldServer.invoke(configureClassLoader(oldVersionLocation));
    oldServerAndLocator.invoke(configureClassLoader(oldVersionLocation));
    
    String serverHostName = getServerHostName(Host.getHost(0));
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    try {
      Properties props = getSystemProperties();
      props.remove(DistributionConfig.LOCATORS_NAME);
      props.put("license-data-management",
          "Y150V-00XD3-08J8Z-04YUV-Z4PFV,4H65K-FUH9Q-H8J90-01396-CJQKG,156DH-AN0D7-M8R10-0J2F6-3TZHG");
      invokeRunnableInVMs(invokeStartLocatorAndServer(serverHostName, port, shortcutName, props), oldServerAndLocator);

      props.put(DistributionConfig.LOCATORS_NAME, serverHostName+"["+port+"]");
      invokeRunnableInVMs(invokeCreateCache(props), currentServer1, currentServer2, oldServer);

      currentServer1.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));
      currentServer2.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));
      oldServer.invoke(invokeAssertVersion(oldOrdinal));
      oldServerAndLocator.invoke(invokeAssertVersion(oldOrdinal));
      
      // create region
      invokeRunnableInVMs(invokeCreateRegion(
          regionName, shortcutName), currentServer1, currentServer2, oldServer, oldServerAndLocator);

      putDataSerializableAndVerify(currentServer1, regionName, 0, 100, currentServer2, oldServer, oldServerAndLocator);
      query("Select * from /" + regionName + " p where p.timeout > 0L", 99, currentServer1, currentServer2, oldServer, oldServerAndLocator);
      
    } finally {
      invokeRunnableInVMs(invokeCloseCache(), currentServer1, currentServer2, oldServer, oldServerAndLocator);
      invokeRunnableInVMs(true, resetClassLoader(), currentServer1, currentServer2, oldServer, oldServerAndLocator);
      bounceAll(currentServer1, oldServer,currentServer2, oldServerAndLocator);
    }
  }

  //This is test was used to test for changes on objects that used
  //java serialization.  Left in just to have in case we want to do more manual testing
//  public void testSerialization() throws Exception {
//    final Host host = Host.getHost(0);
//    final VM currentServer = host.getVM(0);
//    final VM oldServer = host.getVM(1);
//    
//    String oldVersionLocation = gemfireLocations[0].gemfireLocation;
//    short oldOrdinal = gemfireLocations[0].ordinal;
//    
//    //configure all class loaders for each vm
//    currentServer.invoke(configureClassLoaderForCurrent());
//    oldServer.invoke(configureClassLoader(oldVersionLocation));
//    
//    doTestSerialization(currentServer, oldServer);
//    
//    currentServer.invoke(configureClassLoaderForCurrent());
//    oldServer.invoke(configureClassLoader(oldVersionLocation));
//    doTestSerialization(oldServer, currentServer);
//  }
//  
//  public void doTestSerialization(VM vm1, VM vm2) throws Exception {
//
//    try {      
//      final byte[] bytes = (byte[])vm1.invoke(new SerializableCallable("serialize") {
//        public Object call() {
//          try {
//            ByteOutputStream byteArray = new ByteOutputStream();
//            ClassLoader ogLoader = Thread.currentThread().getContextClassLoader();
//            Thread.currentThread().setContextClassLoader(RollingUpgradeDUnitTest.classLoader);
//            ClassLoader loader = RollingUpgradeDUnitTest.classLoader;
//            
//            Class clazzSerializer = loader.loadClass("com.gemstone.gemfire.internal.InternalDataSerializer");
//            Method m = clazzSerializer.getMethod("writeObject", Object.class, DataOutput.class);
//            VersionedDataOutputStream stream = new VersionedDataOutputStream(byteArray, Version.GFE_7099);
//            
//            Class exceptionClass = loader.loadClass("com.gemstone.gemfire.internal.cache.ForceReattemptException");
//            m.invoke(null, exceptionClass.getConstructor(String.class).newInstance("TEST ME"), stream);
//            m.invoke(null, exceptionClass.getConstructor(String.class).newInstance("TEST ME2"), stream);
//            Thread.currentThread().setContextClassLoader(ogLoader);
//            return byteArray.getBytes();
//          }
//          catch (Exception e) {
//            e.printStackTrace();
//
//            fail("argh");
//          }
//          return null;
//        }
//      });
//      
//      vm2.invoke(new CacheSerializableRunnable("deserialize") {
//        public void run2() {
//          try {
//            ClassLoader ogLoader = Thread.currentThread().getContextClassLoader();
//            Thread.currentThread().setContextClassLoader(RollingUpgradeDUnitTest.classLoader);
//            ClassLoader loader = RollingUpgradeDUnitTest.classLoader;
//            Class clazz = loader.loadClass("com.gemstone.gemfire.internal.InternalDataSerializer");
//            Method m = clazz.getMethod("readObject", DataInput.class);
//            VersionedDataInputStream stream = new VersionedDataInputStream(new ByteArrayInputStream(bytes), Version.GFE_71);
//            
//            Object e = m.invoke(null, stream);
//            Object e2 = m.invoke(null, stream);
//            
//            Class exceptionClass = loader.loadClass("com.gemstone.gemfire.internal.cache.ForceReattemptException");
//            exceptionClass.cast(e);
//            exceptionClass.cast(e2);
//            Thread.currentThread().setContextClassLoader(ogLoader);
//          }
//          catch (Exception e) {
//            e.printStackTrace();
//            fail("argh");
//          }
//        }
//      });
//   
//    } finally {
//      invokeRunnableInVMs(true, resetClassLoader(), vm1, vm2);
//      bounceAll(vm1, vm2);
//    }
//  }

  //******** TEST HELPER METHODS ********/  
  public Properties getLocatorProperties(String locatorsString) {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, locatorsString);
    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, getDUnitLogLevel());
    return props;
  }
  
  private void putAndVerify(String objectType, VM putter, String regionName, int start, int end, VM check1, VM check2, VM check3) throws Exception {
    if (objectType.equals("strings")) {
      putStringsAndVerify(putter, regionName, start, end, check1, check2, check3);
    }
    else if (objectType.equals("serializable")) {
      putSerializableAndVerify(putter, regionName, start, end, check1, check2, check3);
    }
    else if (objectType.equals("dataserializable")) {
      putDataSerializableAndVerify(putter, regionName, start, end, check1, check2, check3);
    }
    else {
      throw new Error("Not a valid test object type");
    }
  }
  
  //******** TEST HELPER METHODS ********/  
  private void putAndVerify(String objectType, VM putter, String regionName, int start, int end, VM check1, VM check2) throws Exception {
    if (objectType.equals("strings")) {
      putStringsAndVerify(putter, regionName, start, end, check1, check2);
    }
    else if (objectType.equals("serializable")) {
      putSerializableAndVerify(putter, regionName, start, end, check1, check2);
    }
    else if (objectType.equals("dataserializable")) {
      putDataSerializableAndVerify(putter, regionName, start, end, check1, check2);
    }
    else {
      throw new Error("Not a valid test object type");
    }
  }

  private void putStringsAndVerify(VM putter, final String regionName, final int start, final int end, VM... vms) {
    for (int i = start; i < end; i++) {
      putter.invoke(invokePut(regionName, "" + i, "VALUE(" + i + ")"));
    }

    // verify present in others
    for (VM vm: vms) {
      vm.invoke(invokeAssertEntriesEqual(regionName, start, end));
    }
  }
  
  private void putSerializableAndVerify(VM putter, String regionName, int start, int end, VM... vms) {
    for (int i = start; i < end; i++) {
      putter.invoke(invokePut(regionName, "" + i, new Properties() ));
    }

    // verify present in others
    for (VM vm: vms) {
      vm.invoke(invokeAssertEntriesExist(regionName, start, end));
    }
  }
  
  private void putDataSerializableAndVerify(VM putter, String regionName, int start, int end, VM... vms) throws Exception {
    for (int i = start; i < end; i++) {
      Class aClass = Thread.currentThread().getContextClassLoader().loadClass("com.gemstone.gemfire.cache.ExpirationAttributes");
      Constructor constructor = aClass.getConstructor(int.class);
      Object testDataSerializable = constructor.newInstance(i);
      putter.invoke(invokePut(regionName, "" + i, testDataSerializable));
    }

    // verify present in others
    for (VM vm: vms) {
      vm.invoke(invokeAssertEntriesExist(regionName, start, end));
    }
  }
  
  private void verifyValues(String objectType, String regionName, int start, int end, VM... vms) {
    if (objectType.equals("strings")) {
      for (VM vm: vms) {
        vm.invoke(invokeAssertEntriesEqual(regionName, start, end));
      }
    }
    else if (objectType.equals("serializable")) {
      for (VM vm: vms) {
        vm.invoke(invokeAssertEntriesExist(regionName, start, end));
      }
    }
    else if (objectType.equals("dataserializable")) {
      for (VM vm: vms) {
        vm.invoke(invokeAssertEntriesExist(regionName, start, end));
      }
    }
   
  }
  
  private void query(String queryString, int numExpectedResults, VM...vms) {
    for (VM vm: vms) {
      vm.invoke(invokeAssertQueryResults(queryString, numExpectedResults));
    }
  }
  
  private void invokeRunnableInVMs(CacheSerializableRunnable runnable, VM...vms) throws Exception {
    for (VM vm:vms) {
      vm.invoke(runnable);
    }
  }
  
  //Used to close cache and make sure we attempt on all vms even if some do not have a cache
  private void invokeRunnableInVMs(boolean catchErrors, CacheSerializableRunnable runnable, VM...vms) throws Exception {
    for (VM vm:vms) {
      try {
        vm.invoke(runnable);
      }
      catch (Exception e) {
        if (!catchErrors) {
          throw e;
        }
      }
    }
  }
  
  private void rollServerToCurrent(VM rollServer, int[] locatorPorts) throws Exception {
    //Roll the server
    rollServer.invoke(invokeCloseCache());
    if (!turnOffBounce) {
      rollServer.bounce();
    }
    rollServer.invoke(setClassLoaderToCurrentVersionClassLoader());
    rollServer.invoke(invokeCreateCache( locatorPorts == null? getSystemProperties(): getSystemProperties(locatorPorts)));
    rollServer.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));
  }
  
  private void rollClientToCurrent(VM rollClient, String[] hostNames, int[] locatorPorts) throws Exception {
    //Roll the client
    rollClient.invoke(invokeCloseCache());
    if (!turnOffBounce) {
      rollClient.bounce();
    }
    rollClient.invoke(setClassLoaderToCurrentVersionClassLoader());
    rollClient.invoke(invokeCreateClientCache( getClientSystemProperties(), hostNames, locatorPorts));
    rollClient.invoke(invokeAssertVersion(Version.CURRENT_ORDINAL));
  }
  
  /* 
   * @param rollServer
   * @param createRegionMethod
   * @param regionName
   * @param locatorPorts if null, uses dunit locator
   * @throws Exception
   */
  private void rollServerToCurrentAndCreateRegion(VM rollServer, String shortcutName, String regionName, int[] locatorPorts) throws Exception {
    rollServerToCurrent(rollServer, locatorPorts);
    //recreate region on "rolled" server
    invokeRunnableInVMs(invokeCreateRegion(regionName, shortcutName), rollServer);
    rollServer.invoke(invokeRebalance());
  }
  
  private void rollServerToCurrentAndCreateRegion(VM rollServer, String regionType, File diskdir, String shortcutName, String regionName, int[] locatorPorts) throws Exception {
    rollServerToCurrent(rollServer, locatorPorts);
    //recreate region on "rolled" server
    if ((regionType.equals("persistentReplicate"))) {
      CacheSerializableRunnable runnable = invokeCreatePersistentReplicateRegion(regionName, diskdir);
      invokeRunnableInVMs(runnable, rollServer);
    } else {
      invokeRunnableInVMs(invokeCreateRegion(regionName, shortcutName), rollServer);
    }
    rollServer.invoke(invokeRebalance());
  }
  /* 
   * @param rollClient
   * @param createRegionMethod
   * @param regionName
   * @param locatorPorts if null, uses dunit locator
   * @throws Exception
   */
  private void rollClientToCurrentAndCreateRegion(VM rollClient, String shortcutName, String regionName, String[] hostNames, int[] locatorPorts) throws Exception {
    rollClientToCurrent(rollClient, hostNames, locatorPorts);
    //recreate region on "rolled" client
    invokeRunnableInVMs(invokeCreateClientRegion(
        regionName, shortcutName), rollClient);
  }
  
  private void rollLocatorToCurrent(VM rollLocator, final String serverHostName, final int port, final String testName, final String locatorString ) throws Exception {
    //Roll the locator
    rollLocator.invoke(invokeStopLocator());
    if (!turnOffBounce) {
      rollLocator.bounce();
    }
    rollLocator.invoke(setClassLoaderToCurrentVersionClassLoader());
    rollLocator.invoke(invokeStartLocator(serverHostName, port, testName, locatorString));
  }
  
  private CacheSerializableRunnable resetClassLoader() {
    return new CacheSerializableRunnable("reset class loader") {
      public void run2() {
        RollingUpgradeDUnitTest.classLoader = null;
        System.gc();
        System.gc();
      }
    };
  }
  
  
  public Properties getSystemProperties() {
    return super.getAllDistributedSystemProperties(new Properties());
  }
  
  public Properties getSystemProperties(int[] locatorPorts) {
    Properties p = new Properties();
    String locatorString = getLocatorString(locatorPorts);
    p.setProperty("locators", locatorString);
    p.setProperty("mcast-port", "0");
    p.setProperty("log-level", DUnitLauncher.LOG_LEVEL);
    return p;
  }
  
  public Properties getClientSystemProperties() {
    Properties p = new Properties();
    p.setProperty("mcast-port", "0");
    p.setProperty("log-level", DUnitLauncher.LOG_LEVEL);
    return p;
  }
  
  public static String getLocatorString(int locatorPort) {
    return "localhost[" + locatorPort + "]";
  }
  
  public static String getLocatorString(int[] locatorPorts) {
    String locatorString = "";
    int numLocators = locatorPorts.length;
    for (int i = 0; i < numLocators; i++) {
      locatorString += getLocatorString(locatorPorts[i]);
      if (i + 1 < numLocators) {
        locatorString += ",";
      }
    }
    System.out.println("Locator string is:" + locatorString);
    return locatorString;
  }
  
  //Assumes currentVersionClassLoader was previously set with either configureClassLoader method calls
  private CacheSerializableRunnable setClassLoaderToCurrentVersionClassLoader() {
    return new CacheSerializableRunnable("setClassLoaderToCurrentVersionClassLoader") {
      public void run2() {
        RollingUpgradeDUnitTest.classLoader = null;
        RollingUpgradeDUnitTest.classLoader = Thread.currentThread().getContextClassLoader();
        //Trying to unload the class loader, two gcs seems to do the trick for now...
        System.gc();
        System.gc();
      }
    };
  }
    
  private CacheSerializableRunnable configureClassLoaderForCurrent() {
    return new CacheSerializableRunnable("configure class loader for current") {
      public void run2() {
        RollingUpgradeDUnitTest.classLoader = Thread.currentThread().getContextClassLoader();
      }
    };
  }
 
    //This runnable will configure a childfirst class loader that will use a specific gemfire jar
  private CacheSerializableRunnable configureClassLoader(final String gemfireLocation) {
    return new CacheSerializableRunnable("configure class loader") {
      public void run2() {
        try {
          List<URL> urlList = addFile(new File(gemfireLocation));
          URL[] urls = new URL[urlList.size()];
          int i = 0;
          for (URL url:urlList) {
            urls[i++] = url;
          }
          ChildFirstClassLoader loader = new ChildFirstClassLoader(urls, null);
          RollingUpgradeDUnitTest.classLoader = loader;
        }
        catch (MalformedURLException e) {
          fail("Failed in initializing class loader:" + e);
        }   
      }
    };
  }
  
  private List<URL> addFile(File file) throws MalformedURLException {
    ArrayList<URL> urls = new ArrayList<URL>();
    if (file.isDirectory()) {
      //Do not want to start cache with sample code xml
      if (file.getName().contains("SampleCode")) {
        return urls;
      }
      else {
        File[] files = file.listFiles();
        for (File afile: files) {
          urls.addAll(addFile(afile));
        }
      }
    }
    else {
      URL url = file.toURI().toURL();
      urls.add(url);
    }
    return urls;
  }
  
  
  
  
  private CacheSerializableRunnable invokeStartLocator(final String serverHostName, final int port, final String testName, final String locatorsString, final Properties props) {
    return new CacheSerializableRunnable("execute: startLocator") {
      public void run2() {
        ClassLoader ogLoader = Thread.currentThread().getContextClassLoader();
        try {
          Thread.currentThread().setContextClassLoader(RollingUpgradeDUnitTest.classLoader);
          startLocator(serverHostName, port, testName, locatorsString, props);
        }
        catch (Exception e) {
          e.printStackTrace();
          fail("Error starting locators");
        }
        finally {
          Thread.currentThread().setContextClassLoader(ogLoader);
        }
      }
    };
  }
  
  private CacheSerializableRunnable invokeStartLocator(final String serverHostName, final int port, final String testName, final String locatorsString) {
    return new CacheSerializableRunnable("execute: startLocator") {
      public void run2() {
        ClassLoader ogLoader = Thread.currentThread().getContextClassLoader();
        try {
          Thread.currentThread().setContextClassLoader(RollingUpgradeDUnitTest.classLoader);
          startLocator(serverHostName, port, testName, locatorsString);
        }
        catch (Exception e) {
          e.printStackTrace();
          fail("Error starting locators", e);
        }
        finally {
          Thread.currentThread().setContextClassLoader(ogLoader);
        }
      }
    };
  }
  
  /**
   * Starts a locator with given configuration.
   */
  public static void startLocator(final String serverHostName, final int port,
      final String testName, final String locatorsString, final Properties props) throws Exception {
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, locatorsString);
    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, getDUnitLogLevel());

    InetAddress bindAddr = null;
    try {
      bindAddr = InetAddress.getByName(serverHostName);// getServerHostName(vm.getHost()));
    } catch (UnknownHostException uhe) {
      throw new Error("While resolving bind address ", uhe);
    }

    File logFile = new File(testName + "-locator" + port + ".log");
    Class locatorClass = Thread.currentThread().getContextClassLoader()
        .loadClass("com.gemstone.gemfire.distributed.Locator");
    Method startLocatorAndDSMethod = locatorClass.getMethod("startLocatorAndDS", int.class, File.class,
        InetAddress.class, Properties.class, boolean.class, boolean.class,
        String.class);
    startLocatorAndDSMethod.setAccessible(true);
    startLocatorAndDSMethod.invoke(null, port, logFile, bindAddr, props, true, true,
        null);
  }

  private CacheSerializableRunnable invokeStartLocatorAndServer(final String serverHostName, final int port, final String testName, final Properties systemProperties) {
    return new CacheSerializableRunnable("execute: startLocator") {
      public void run2() {
        ClassLoader ogLoader = Thread.currentThread().getContextClassLoader();
        try {
          Thread.currentThread().setContextClassLoader(RollingUpgradeDUnitTest.classLoader);
          systemProperties.put(DistributionConfig.START_LOCATOR_NAME, ""+serverHostName+"["+port+"]");
          RollingUpgradeDUnitTest.cache = createCache(systemProperties);
        }
        catch (Exception e) {
          fail("Error starting locators", e);
        }
        finally {
          Thread.currentThread().setContextClassLoader(ogLoader);
        }
      }
    };
  }
  
  private CacheSerializableRunnable invokeCreateCache(final Properties systemProperties) {
    return new CacheSerializableRunnable("execute: createCache") {
      public void run2() {
        ClassLoader ogLoader = Thread.currentThread().getContextClassLoader();
        try {
          Thread.currentThread().setContextClassLoader(RollingUpgradeDUnitTest.classLoader);
          RollingUpgradeDUnitTest.cache = createCache(systemProperties);
        }
        catch (Exception e) {
          e.printStackTrace();
          fail("Error creating cache", e);
        }
        finally {
          Thread.currentThread().setContextClassLoader(ogLoader);
        }
      }
    };
  }
  
  private CacheSerializableRunnable invokeCreateClientCache(final Properties systemProperties, final String[] hosts, final int[] ports) {
    return new CacheSerializableRunnable("execute: createCache") {
      public void run2() {
        ClassLoader ogLoader = Thread.currentThread().getContextClassLoader();
        try {
          Thread.currentThread().setContextClassLoader(RollingUpgradeDUnitTest.classLoader);
          RollingUpgradeDUnitTest.cache = createClientCache(systemProperties, hosts, ports);
        }
        catch (Exception e) {
          e.printStackTrace();
          fail("Error creating client cache", e);
        }
        finally {
          Thread.currentThread().setContextClassLoader(ogLoader);
        }
      }
    };
  }
  
  private CacheSerializableRunnable invokeStartCacheServer(final int port) {
    return new CacheSerializableRunnable("execute: startCacheServer") {
      public void run2() {
        ClassLoader ogLoader = Thread.currentThread().getContextClassLoader();
        try {
          Thread.currentThread().setContextClassLoader(RollingUpgradeDUnitTest.classLoader);
          startCacheServer(RollingUpgradeDUnitTest.cache, port);
        }
        catch (Exception e) {
          e.printStackTrace();
          fail("Error creating cache", e);
        }
        finally {
          Thread.currentThread().setContextClassLoader(ogLoader);
        }
      }
    };
  }
  
  private CacheSerializableRunnable invokeAssertVersion(final short version) {
    return new CacheSerializableRunnable("execute: assertVersion") {
      public void run2() {
        ClassLoader ogLoader = Thread.currentThread().getContextClassLoader();
        try {
          Thread.currentThread().setContextClassLoader(RollingUpgradeDUnitTest.classLoader);
          assertVersion(RollingUpgradeDUnitTest.cache, version);
        }
        catch (Exception e) {
          e.printStackTrace();
          fail("Error asserting version", e);
        }
        finally {
          Thread.currentThread().setContextClassLoader(ogLoader);
        }
      }
    };
  }
  
  private CacheSerializableRunnable invokeCreateRegion(final String regionName, final String shortcutName) {
    return new CacheSerializableRunnable("execute: createRegion") {
      public void run2() {
        ClassLoader ogLoader = Thread.currentThread().getContextClassLoader();
        try {
          Thread.currentThread().setContextClassLoader(RollingUpgradeDUnitTest.classLoader);
          createRegion(RollingUpgradeDUnitTest.cache, regionName, shortcutName);
        }
        catch (Exception e) {
          e.printStackTrace();
          fail("Error createRegion", e);
        }
        finally {
          Thread.currentThread().setContextClassLoader(ogLoader);
        }
      }
    };
  }
  
  private CacheSerializableRunnable invokeCreatePersistentReplicateRegion(final String regionName, final File diskstore) {
    return new CacheSerializableRunnable("execute: createPersistentReplicateRegion") {
      public void run2() {
        ClassLoader ogLoader = Thread.currentThread().getContextClassLoader();
        try {
          Thread.currentThread().setContextClassLoader(RollingUpgradeDUnitTest.classLoader);
          createPersistentReplicateRegion(RollingUpgradeDUnitTest.cache, regionName, diskstore);
          Thread.currentThread().setContextClassLoader(ogLoader);
        }
        catch (Exception e) {
          fail("Error createPersistentReplicateRegion", e);
        }
        finally {
          Thread.currentThread().setContextClassLoader(ogLoader);
        }
      }
    };
  }
  
  private CacheSerializableRunnable invokeCreateClientRegion(final String regionName, final String shortcutName) {
    return new CacheSerializableRunnable("execute: createClientRegion") {
      public void run2() {
        ClassLoader ogLoader = Thread.currentThread().getContextClassLoader();
        try {
          Thread.currentThread().setContextClassLoader(RollingUpgradeDUnitTest.classLoader);
          createClientRegion(RollingUpgradeDUnitTest.cache, regionName, shortcutName);
        }
        catch (Exception e) {
          e.printStackTrace();
          fail("Error creating client region", e);
        }
        finally {
          Thread.currentThread().setContextClassLoader(ogLoader);
        }
      }
    };
  }
  
  private CacheSerializableRunnable invokePut(final String regionName, final Object key, final Object value) {
    return new CacheSerializableRunnable("execute: put") {
      public void run2() {
        ClassLoader ogLoader = Thread.currentThread().getContextClassLoader();
        try {
          Thread.currentThread().setContextClassLoader(RollingUpgradeDUnitTest.classLoader);
          put(RollingUpgradeDUnitTest.cache, regionName, key, value);
        }
        catch (Exception e) {
          e.printStackTrace();
          fail("Error put", e);
        }
        finally {
          Thread.currentThread().setContextClassLoader(ogLoader);
        }
      }
    };
  }
  
  private CacheSerializableRunnable invokeAssertEntriesEqual(final String regionName, final int start, final int end) {
    return new CacheSerializableRunnable("execute: assertEntriesEqual") {
      public void run2() {
        ClassLoader ogLoader = Thread.currentThread().getContextClassLoader();
        try {
          Thread.currentThread().setContextClassLoader(RollingUpgradeDUnitTest.classLoader);
          for (int i=start; i<end; i++) {
            assertEntryEquals(RollingUpgradeDUnitTest.cache, regionName, ""+i, "VALUE("+i+")");
          }
        }
        catch (Exception e) {
          e.printStackTrace();
          fail("Error asserting equals", e);
        }
        finally {
          Thread.currentThread().setContextClassLoader(ogLoader);
        }
      }
    };
  }
  
  private CacheSerializableRunnable invokeAssertEntriesExist(final String regionName, final int start, final int end) {
    return new CacheSerializableRunnable("execute: assertEntryExists") {
      public void run2() {
        ClassLoader ogLoader = Thread.currentThread().getContextClassLoader();
        try {
          Thread.currentThread().setContextClassLoader(RollingUpgradeDUnitTest.classLoader);
          for (int i=start; i<end; i++) {
            assertEntryExists(RollingUpgradeDUnitTest.cache, regionName, ""+i);
          }
        }
        catch (Exception e) {
          e.printStackTrace();
          fail("Error asserting exists", e);
        }
        finally {
          Thread.currentThread().setContextClassLoader(ogLoader);
        }
      }
    };
  }
  
  private CacheSerializableRunnable invokeStopLocator() {
    return new CacheSerializableRunnable("execute: stopLocator") {
      public void run2() {
        ClassLoader ogLoader = Thread.currentThread().getContextClassLoader();
        try {
          Thread.currentThread().setContextClassLoader(RollingUpgradeDUnitTest.classLoader);
          stopLocator();
        }
        catch (Exception e) {
          e.printStackTrace();
          fail("Error stopping locator", e);
        }
        finally {
          Thread.currentThread().setContextClassLoader(ogLoader);
        }
      }
    };
  }
  
  private CacheSerializableRunnable invokeCloseCache() {
    return new CacheSerializableRunnable("execute: closeCache") {
      public void run2() {
        ClassLoader ogLoader = Thread.currentThread().getContextClassLoader();
        try {
          Thread.currentThread().setContextClassLoader(RollingUpgradeDUnitTest.classLoader);
          closeCache(RollingUpgradeDUnitTest.cache);
        }
        catch (Exception e) {
          e.printStackTrace();
          fail("Error closing cache", e);
        }
        finally {
          Thread.currentThread().setContextClassLoader(ogLoader);
        }
      }
    };
  }
  
  private CacheSerializableRunnable invokeRebalance() {
    return new CacheSerializableRunnable("execute: rebalance") {
      public void run2() {
        ClassLoader ogLoader = Thread.currentThread().getContextClassLoader();
        try {
           Thread.currentThread().setContextClassLoader(RollingUpgradeDUnitTest.classLoader);
           rebalance(RollingUpgradeDUnitTest.cache);
        }
        catch (Exception e) {
          e.printStackTrace();
          fail("Error rebalancing", e);
        }
        finally {
          Thread.currentThread().setContextClassLoader(ogLoader);
        }
      }
    };
  }
  
  private CacheSerializableRunnable invokeAssertQueryResults(final String queryString, final int numExpected) {
    return new CacheSerializableRunnable("execute: assertQueryResults") {
      public void run2() {
        ClassLoader ogLoader = Thread.currentThread().getContextClassLoader();
        try {
          Thread.currentThread().setContextClassLoader(RollingUpgradeDUnitTest.classLoader);
           assertQueryResults(RollingUpgradeDUnitTest.cache, queryString, numExpected);
        }
        catch (Exception e) {
          e.printStackTrace();
          fail("Error asserting query results", e);
        }
        finally {
          Thread.currentThread().setContextClassLoader(ogLoader);
        }
      }
    };
  }
  
  protected static class LocationAndOrdinal {
    protected String gemfireLocation;
    protected short ordinal;
    protected LocationAndOrdinal(String gemfireLocation, short ordinal) {
      this.gemfireLocation = gemfireLocation;
      this.ordinal = ordinal;
    }
  }
  

  public void deleteDiskStores() throws Exception {
    try {
      FileUtil.delete(new File(diskDir).getAbsoluteFile());
    } catch (IOException e) {
      e.printStackTrace();
      throw new Error("Error deleting files", e);
    }
  }

  public static Object createCache(Properties systemProperties)
      throws Exception {
    systemProperties.put(DistributionConfig.LOG_FILE_NAME, "rollingUpgradeCacheVM"+VM.getCurrentVMNum()+".log");
    Class aClass = Thread.currentThread().getContextClassLoader()
        .loadClass("com.gemstone.gemfire.cache.CacheFactory");
    Constructor constructor = aClass.getConstructor(Properties.class);
    Object cacheFactory = constructor.newInstance(systemProperties);

    Method createMethod = aClass.getMethod("create");
    Object cache = createMethod.invoke(cacheFactory);

    return cache;
  }

  public static void startCacheServer(Object cache, int port) throws Exception {
    Method addCacheServerMethod = cache.getClass().getMethod("addCacheServer");
    Object cacheServer = addCacheServerMethod.invoke(cache);

    Method setPortMethod = cacheServer.getClass().getMethod("setPort",
        int.class);
    setPortMethod.invoke(cacheServer, port);

    Method startMethod = cacheServer.getClass().getMethod("start");
    startMethod.invoke(cacheServer);
  }

  public static Object createClientCache(Properties systemProperties,
      String[] hosts, int[] ports) throws Exception {
    Class aClass = Thread.currentThread().getContextClassLoader()
        .loadClass("com.gemstone.gemfire.cache.client.ClientCacheFactory");
    Constructor constructor = aClass.getConstructor(Properties.class);

    Object ccf = constructor.newInstance(systemProperties);
    Method addPoolLocatorMethod = aClass.getMethod("addPoolLocator",
        String.class, int.class);

    int hostsLength = hosts.length;
    for (int i = 0; i < hostsLength; i++) {
      addPoolLocatorMethod.invoke(ccf, hosts[i], ports[i]);
    }

    Method createMethod = aClass.getMethod("create");
    Object cache = createMethod.invoke(ccf);

    return cache;
  }

  public static boolean assertRegionExists(Object cache, String regionName)
      throws Exception {
    Object region = cache.getClass().getMethod("getRegion", String.class)
        .invoke(cache, regionName);
    if (region == null) {
      throw new Error("Region: " + regionName + " does not exist");
    }
    return true;
  }

  public static Object getRegion(Object cache, String regionName)
      throws Exception {
    ClassLoader ogLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(
        RollingUpgradeDUnitTest.classLoader);
    try {
      return cache.getClass().getMethod("getRegion", String.class)
          .invoke(cache, regionName);
    } finally {
      Thread.currentThread().setContextClassLoader(ogLoader);
    }
  }

  public static boolean assertEntryEquals(Object cache, String regionName,
      Object key, Object value) throws Exception {
    assertRegionExists(cache, regionName);
    Object region = getRegion(cache, regionName);
    Object regionValue = region.getClass().getMethod("get", Object.class)
        .invoke(region, key);
    if (regionValue == null) {
      getLogWriter().info("region value does not exist for key: " + key);
      region.getClass().getMethod("dumpBackingMap").invoke(region);
      throw new Error("Region value does not exist for key: " + key);
    }
    if (!regionValue.equals(value)) {
      getLogWriter().info("Entry for key: " + key + " does not equal value: "
          + value);
      throw new Error("Entry for key: " + key + " does not equal value: "
          + value + "(got=" + regionValue + ')');
    }
    return true;
  }

  public static boolean assertEntryExists(Object cache, String regionName,
      Object key) throws Exception {
    assertRegionExists(cache, regionName);
    Object region = getRegion(cache, regionName);
    Object regionValue = region.getClass().getMethod("get", Object.class)
        .invoke(region, key);
    if (regionValue == null) {
      System.out.println("Entry for key:" + key + " does not exist");
      throw new Error("Entry for key:" + key + " does not exist");
    }
    return true;
  }

  public static Object put(Object cache, String regionName, Object key,
      Object value) throws Exception {
    Object region = getRegion(cache, regionName);
    return region.getClass().getMethod("put", Object.class, Object.class)
        .invoke(region, key, value);
  }

  public static void createRegion(Object cache, String regionName,
      String shortcutName) throws Exception {
    Class aClass = Thread.currentThread().getContextClassLoader()
        .loadClass("com.gemstone.gemfire.cache.RegionShortcut");
    Object[] enumConstants = aClass.getEnumConstants();
    Object shortcut = null;
    int length = enumConstants.length;
    for (int i = 0; i < length; i++) {
      Object constant = enumConstants[i];
      if (((Enum) constant).name().equals(shortcutName)) {
        shortcut = constant;
        break;
      }
    }

    Object regionFactory = cache.getClass()
        .getMethod("createRegionFactory", aClass).invoke(cache, shortcut);
    regionFactory.getClass().getMethod("create", String.class)
        .invoke(regionFactory, regionName);
  }

  public static void createPartitionedRegion(Object cache, String regionName)
      throws Exception {
    createRegion(cache, regionName, RegionShortcut.PARTITION.name());
  }

  public static void createPartitionedRedundantRegion(Object cache,
      String regionName) throws Exception {
    createRegion(cache, regionName, RegionShortcut.PARTITION_REDUNDANT.name());
  }

  public static void createReplicatedRegion(Object cache, String regionName)
      throws Exception {
    createRegion(cache, regionName, RegionShortcut.REPLICATE.name());
  }

  // Assumes a client cache is passed
  public static void createClientRegion(Object cache, String regionName,
      String shortcutName) throws Exception {
    Class aClass = Thread.currentThread().getContextClassLoader()
        .loadClass("com.gemstone.gemfire.cache.client.ClientRegionShortcut");
    Object[] enumConstants = aClass.getEnumConstants();
    Object shortcut = null;
    int length = enumConstants.length;
    for (int i = 0; i < length; i++) {
      Object constant = enumConstants[i];
      if (((Enum) constant).name().equals(shortcutName)) {
        shortcut = constant;
        break;
      }
    }
    Object clientRegionFactory = cache.getClass()
        .getMethod("createClientRegionFactory", shortcut.getClass())
        .invoke(cache, shortcut);
    clientRegionFactory.getClass().getMethod("create", String.class)
        .invoke(clientRegionFactory, regionName);
  }

  public static void createRegion(String regionName, Object regionFactory)
      throws Exception {
    regionFactory.getClass().getMethod("create", String.class)
        .invoke(regionFactory, regionName);
  }

  public static void createPersistentReplicateRegion(Object cache,
      String regionName, File diskStore) throws Exception {
    Object store = cache.getClass().getMethod("findDiskStore", String.class)
        .invoke(cache, "store");
    Class dataPolicyObject = Thread.currentThread().getContextClassLoader()
        .loadClass("com.gemstone.gemfire.cache.DataPolicy");
    Object dataPolicy = dataPolicyObject.getField("PERSISTENT_REPLICATE").get(
        null);
    if (store == null) {
      Object dsf = cache.getClass().getMethod("createDiskStoreFactory")
          .invoke(cache);
      dsf.getClass().getMethod("setMaxOplogSize", long.class).invoke(dsf, 1L);
      dsf.getClass()
          .getMethod("setDiskDirs", File[].class)
          .invoke(dsf,
              new Object[] { new File[] { diskStore.getAbsoluteFile() } });
      dsf.getClass().getMethod("create", String.class).invoke(dsf, "store");
    }
    Object rf = cache.getClass().getMethod("createRegionFactory").invoke(cache);
    rf.getClass().getMethod("setDiskStoreName", String.class)
        .invoke(rf, "store");
    rf.getClass().getMethod("setDataPolicy", dataPolicy.getClass())
        .invoke(rf, dataPolicy);
    rf.getClass().getMethod("create", String.class).invoke(rf, regionName);
  }

  public static void assertVersion(Object cache, short ordinal)
      throws Exception {
    Class idmClass = Thread
        .currentThread()
        .getContextClassLoader()
        .loadClass(
            "com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember");
    Object ds = cache.getClass().getMethod("getDistributedSystem")
        .invoke(cache);
    Object member = ds.getClass().getMethod("getDistributedMember").invoke(ds);
    short thisVersion = (Short) member.getClass()
        .getMethod("getVersionOrdinal").invoke(member);
    if (ordinal != thisVersion) {
      throw new Error("Version ordinal:" + thisVersion
          + " was not the expected ordinal of:" + ordinal);
    }
  }

  public static void assertQueryResults(Object cache, String queryString,
      int numExpectedResults) {
    ClassLoader ogLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(
        RollingUpgradeDUnitTest.classLoader);
    try {
      Object qs = cache.getClass().getMethod("getQueryService").invoke(cache);
      Object query = qs.getClass().getMethod("newQuery", String.class)
          .invoke(qs, queryString);
      Object results = query.getClass().getMethod("execute").invoke(query);

      int numResults = (Integer) results.getClass().getMethod("size")
          .invoke(results);

      if (numResults != numExpectedResults) {
        System.out.println("Num Results was:" + numResults);
        throw new Error("Num results:" + numResults + " != num expected:"
            + numExpectedResults);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new Error("Query Exception " + e, e);
    } finally {
      Thread.currentThread().setContextClassLoader(ogLoader);
    }
  }

  public static void stopCacheServers(Object cache) throws Exception {
    ClassLoader ogLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(
        RollingUpgradeDUnitTest.classLoader);
    try {
      List cacheServers = (List) cache.getClass().getMethod("getCacheServers")
          .invoke(cache);
      Method stopMethod = null;
      for (Object cs : cacheServers) {
        if (stopMethod == null) {
          stopMethod = cs.getClass().getMethod("stop");
        }
        stopMethod.invoke(cs);
      }
    } finally {
      Thread.currentThread().setContextClassLoader(ogLoader);
    }
  }

  public static void closeCache(Object cache) throws Exception {
    ClassLoader ogLoader = Thread.currentThread().getContextClassLoader();
    Thread.currentThread().setContextClassLoader(
        RollingUpgradeDUnitTest.classLoader);
    try {
      boolean cacheClosed;
      if (cache != null && !(cacheClosed = (Boolean)cache.getClass()
          .getMethod("isClosed").invoke(cache))) {
        stopCacheServers(cache);
        cache.getClass().getMethod("close").invoke(cache);
        long startTime = System.currentTimeMillis();
        while (!cacheClosed && System.currentTimeMillis() - startTime < 30000) {
          try {
            Thread.sleep(1000);
            cacheClosed = (Boolean) cache.getClass().getMethod("isClosed")
                .invoke(cache);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      }
      cache = null;
      System.gc();
      System.gc();
    } finally {
      Thread.currentThread().setContextClassLoader(ogLoader);
    }
  }

  public static void rebalance(Object cache) throws Exception {
    try {
    Object manager = cache.getClass().getMethod("getResourceManager")
        .invoke(cache);

    Object rebalanceFactory = manager.getClass()
        .getMethod("createRebalanceFactory").invoke(manager);
    rebalanceFactory.getClass().getMethod("start");
    Object op = null;
    Method m = rebalanceFactory.getClass().getMethod("start");
    m.setAccessible(true);
    op = m.invoke(rebalanceFactory);

    // Wait until the rebalance is completex
    Object results = op.getClass().getMethod("getResults").invoke(op);
    Method getTotalTimeMethod = results.getClass().getMethod("getTotalTime");
    getTotalTimeMethod.setAccessible(true);
      
    Method getTotalBucketTransferBytesMethod = results.getClass().getMethod("getTotalBucketTransferBytes");
    getTotalBucketTransferBytesMethod.setAccessible(true);
    System.out.println("Took " + getTotalTimeMethod.invoke(results)
          + " milliseconds\n");
    System.out.println("Transfered "
          + getTotalBucketTransferBytesMethod.invoke(results) + "bytes\n");
    }
    catch (Exception e) {
      e.printStackTrace();
      Thread.currentThread().interrupt();
      throw e;
    }
  }

  /**
   * Starts a locator with given configuration.
   */
  public static void startLocator(final String serverHostName, final int port,
      final String testName, final String locatorsString) throws Exception {
    Properties props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, locatorsString);
    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, getDUnitLogLevel());

    InetAddress bindAddr = null;
    try {
      bindAddr = InetAddress.getByName(serverHostName);// getServerHostName(vm.getHost()));
    } catch (UnknownHostException uhe) {
      throw new Error("While resolving bind address ", uhe);
    }

    File logFile = new File(testName + "-locator" + port + ".log");
    Class locatorClass = Thread.currentThread().getContextClassLoader()
        .loadClass("com.gemstone.gemfire.distributed.Locator");
    locatorClass.getMethod("startLocatorAndDS", int.class, File.class,
        InetAddress.class, Properties.class, boolean.class, boolean.class,
        String.class).invoke(null, port, logFile, bindAddr, props, true, true,
        null);
  }

  public static void stopLocator() throws Exception {
    Class internalLocatorClass = Thread.currentThread().getContextClassLoader()
        .loadClass("com.gemstone.gemfire.distributed.internal.InternalLocator");
    Object locator = internalLocatorClass.getMethod("getLocator").invoke(null);
    locator.getClass().getMethod("stop").invoke(locator);
  }
 
}

