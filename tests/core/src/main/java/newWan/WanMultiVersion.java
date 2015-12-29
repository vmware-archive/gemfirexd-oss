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
package newWan;

import hydra.BridgeHelper;
import hydra.CacheHelper;
import hydra.ConfigPrms;
import hydra.DistributedSystemHelper;
import hydra.GatewayHubHelper;
import hydra.GatewayReceiverHelper;
import hydra.GatewaySenderHelper;
import hydra.Log;
import hydra.RegionHelper;
import hydra.RegionPrms;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import hydra.blackboard.SharedMap;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Vector;

import objects.PSTObject;
import splitBrain.RegionEntryComparator;
import util.NameFactory;
import util.QueryObject;
import util.RandomValues;
import util.SilenceListener;
import util.TestException;
import util.ValueHolder;
import wan.CacheClientPrms;
import wan.CacheServerPrms;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.ClientHelper;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.RegionQueue;
import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderImpl;
import com.gemstone.gemfire.internal.cache.wan.serial.SerialGatewaySenderImpl;

import durableClients.DurableClientsBB;

public class WanMultiVersion {

  static Set<String> regionNames = new HashSet<String>();
  static LogWriter logger = Log.getLogWriter();
  static final int ITERATIONS = WANTestPrms.getIterations();
  static RandomValues rd = new RandomValues();
  /**
   * Creates a (disconnected) locator.
   */
  public synchronized static void createLocatorTask() {
    DistributedSystemHelper.createLocator();
  }
 
  /**
   * Connects a locator to its (admin-only) distributed system.
   */
  public synchronized static void startLocatorAndAdminDSTask() {
    DistributedSystemHelper.startLocatorAndAdminDS();
  }
  
  /**
   * Creates GatewaySender ids based on the
   * {@link WANTestPrms#gatewaySenderConfig}.
   */
  public synchronized static void createGatewaySenderIdsTask() {
    String senderConfig = ConfigPrms.getGatewaySenderConfig();
    GatewaySenderHelper.createGatewaySenderIds(senderConfig);
  }
  
  public synchronized static void initPeerTask(){
    String cacheConfig = ConfigPrms.getCacheConfig();    
    Cache c = CacheHelper.createCache(cacheConfig);
    
    if(WANTestPrms.isNewWanConfig()){
      String senderConfig = ConfigPrms.getGatewaySenderConfig();
      GatewaySenderHelper.createGatewaySenders(senderConfig);
    }
    
    Vector regionDescriptions = TestConfig.tab().vecAt(RegionPrms.names, null);
    for (int j = 0; j < regionDescriptions.size(); j++) {
      String regionDescriptName = (String)(regionDescriptions.get(j));
      String regionName = RegionHelper.getRegionDescription(regionDescriptName).getRegionName();
      if (!regionNames.contains(regionName)){
        RegionHelper.createRegion(regionName, regionDescriptName);
        Log.getLogWriter().info("Created region " + regionName + " with region descript name " + regionDescriptName); 
        regionNames.add(regionName);
      }     
    }
  }
 
  public synchronized static void initServerTask(){
    String cacheConfig = ConfigPrms.getCacheConfig();    
    Cache c = CacheHelper.createCache(cacheConfig);
    
    if(WANTestPrms.isNewWanConfig()){
      String senderConfig = ConfigPrms.getGatewaySenderConfig();
      GatewaySenderHelper.createGatewaySenders(senderConfig);
    }
    
    Vector regionDescriptions = TestConfig.tab().vecAt(RegionPrms.names, null);
    for (int j = 0; j < regionDescriptions.size(); j++) {
      String regionDescriptName = (String)(regionDescriptions.get(j));
      String regionName = RegionHelper.getRegionDescription(regionDescriptName).getRegionName();
      if (!regionNames.contains(regionName) && !regionDescriptName.contains("client")){
        RegionHelper.createRegion(regionName, regionDescriptName);
        Log.getLogWriter().info("Created region " + regionName + " with region descript name " + regionDescriptName); 
        regionNames.add(regionName);
      }     
    }
    
    String bridgeConfig = ConfigPrms.getBridgeConfig();
    BridgeHelper.startBridgeServer(bridgeConfig);
  }
  
  public static synchronized void initEdgeClientTask() {
    String cacheConfig = WANTestPrms.getClientCacheConfig();
    Cache cache = CacheHelper.createCache(cacheConfig);
    
    String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
        .getAnyInstance()).getConfig().getDurableClientId();

    if (!VmDurableId.equals("")) {
      Log.getLogWriter().info(" VM Durable Client Id is " + VmDurableId);
      if (!DurableClientsBB.getBB().getSharedMap().containsKey(VmDurableId)) {
        HashMap map = new HashMap();
        DurableClientsBB.getBB().getSharedMap().put(VmDurableId, map);
      }
      cache.readyForEvents();
    }
    
    Vector regionDescriptions = TestConfig.tab().vecAt(RegionPrms.names, null);
    for (int j = 0; j < regionDescriptions.size(); j++) {
      String regionDescriptName = (String)(regionDescriptions.get(j));
      if(regionDescriptName.startsWith("client")){
        String regionName = RegionHelper.getRegionDescription(
            regionDescriptName).getRegionName();
        Region region = RegionHelper.createRegion(regionName, regionDescriptName);
        Log.getLogWriter().info("Created edge client region " + regionName
                  + " with region descript name " + regionDescriptName);
        if(!regionNames.contains(regionName)){
          regionNames.add(regionName);
        }
        PoolImpl mybw = ClientHelper.getPool(region);

        ServerLocation primaryEndpoint = (ServerLocation )mybw.getPrimary();
        Log.getLogWriter()
            .info("The primary server endpoint is " + primaryEndpoint);
        registerInterest(region);
      } 
    }
  }
    
  /**
   * Initializes GatewaySender based on the
   * {@link WANTestPrms#gatewaySenderConfig}.
   */
  public synchronized static void initGatewaySenderTask() {
    String senderConfig = ConfigPrms.getGatewaySenderConfig();
    GatewaySenderHelper.startGatewaySenders(senderConfig);
  }
  
  
  /**
   * Initializes GatewayReceiver based on the
   * {@link WANTestPrms#GatewayReceiverConfig}.
   */
  public synchronized static void initGatewayReceiverTask() {
    String receiverConfig = ConfigPrms.getGatewayReceiverConfig();
    GatewayReceiverHelper.createAndStartGatewayReceivers(receiverConfig);
  }
  
  /**
   * Creates a gateway hub using the {@link CacheServerPrms}.
   */
  public synchronized static void createGatewayHubTask() {
    String gatewayHubConfig = ConfigPrms.getGatewayHubConfig();        
    GatewayHubHelper.createGatewayHub(gatewayHubConfig);
  }

  /**
   * Starts a gateway hub in a VM that previously created one, after creating
   * gateways.
   */
  public synchronized static void startGatewayHubTask() {
    String gatewayConfig = ConfigPrms.getGatewayConfig();
    GatewayHubHelper.addGateways(gatewayConfig);
    GatewayHubHelper.startGatewayHub();
  }

  /**
   * Registers interest in all keys using the client interest policy.
   */
  public static void registerInterest(Region region) {
    InterestResultPolicy interestPolicy = CacheClientPrms.getInterestPolicy();
    LocalRegion localRegion = (LocalRegion)region;
   String VmDurableId = ((InternalDistributedSystem)InternalDistributedSystem
        .getAnyInstance()).getConfig().getDurableClientId();

    if (!VmDurableId.equals("")) {
      Log.getLogWriter().info("Doing durable register interest");
      localRegion.registerInterest("ALL_KEYS", interestPolicy, true);
    }
    else {
      localRegion.registerInterest("ALL_KEYS", interestPolicy);
    }
    Log.getLogWriter().info("Initialized region " + region
       + "\nRegistered interest in ALL_KEYS with InterestResultPolicy = "
       + interestPolicy);
  }
  
  /**
   * Puts integers into the cache with a globally increasing shared counter as
   * the key. Does 1000 updates on each key.
   */
  public static void putSequentialKeysTask() throws Exception {
    for (String name : WanMultiVersion.regionNames){
       Region region = RegionHelper.getRegion(name);
       
       Long keyCounter = WANBlackboard.getInstance().increamentAndReadKeyCounter(region.getName(), 1);
       Object key = NameFactory.getObjectNameForCounter(keyCounter.longValue());        
       
       logger.info("Working on key :" + key + " for region " + region.getFullPath());
       for (long i = 1; i <= ITERATIONS; i++) {         
         ValueHolder val = new ValueHolder(NameFactory.getObjectNameForCounter(i), rd);      
         region.put(key, val);
       }
    }      
  }
   
  /**
   * Task to wait for the listener silence before doing validation tasks.
   */
  public static void waitForListenerSilence() {
    Log.getLogWriter().info("Silence Listeenr waiting for 30 seconds ...");
    SilenceListener.waitForSilence(30, 5000);
  }
  
  /**
   * Waits {@link newWan.WANTestPrms#secToWaitForQueue} for queues to drain.
   * Suitable for use as a helper method or hydra task.
   */
  public static void waitForQueuesToDrainTask(){
    SharedMap bb = WANBlackboard.getInstance().getSharedMap();
    // Lets check to make sure the queues are empty
    long startTime = System.currentTimeMillis();
    long maxWait = WANTestPrms.getSecToWaitForQueue();
    long entriesLeft = 0;
    while ((System.currentTimeMillis() - startTime) < (maxWait * 1000)) {
      boolean pass = true;
      entriesLeft = 0;
      
      Iterator<GatewaySender> itr = GatewaySenderHelper.getGatewaySenders().iterator();
      while (itr.hasNext()){
        GatewaySender gs = itr.next();       
        if (gs instanceof SerialGatewaySenderImpl){
          int queuesize = 0;
          Set<RegionQueue> rqs = ((SerialGatewaySenderImpl)gs).getQueues();
          for (RegionQueue rq: rqs){
            queuesize += rq.size();
          }
          entriesLeft += queuesize;
          if (queuesize > 0){
            logger.warning("Still waiting for queue to drain. SerialGatewaySender " + gs
              + " has " + queuesize + " entries in it.");
            pass = false;
          } 
        }else if(gs instanceof ParallelGatewaySenderImpl){
          RegionQueue rq = ((ParallelGatewaySenderImpl)gs).getQueues().toArray(new RegionQueue[1])[0];
          int queuesize = rq.size();
          entriesLeft += queuesize;
          if (queuesize > 0){
            logger.warning("Still waiting for queue to drain. ParallelGatewaySender " + gs
              + " has " + queuesize + " entries in it.");
            pass = false;
          }          
        }else{
          throw new TestException(
              "Unknown class of gateway sender: " + GatewaySenderHelper.gatewaySenderToString(gs));
        }          
      }
      
      if (pass) {
        entriesLeft = 0;
        break;
      } else {
        try {
          Thread.sleep(1000);
        }
        catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    if (entriesLeft > 0) {
      throw new TestException(
          "Timed out waiting for queue to drain. Waited for " + maxWait + " sec, Entries left in queues are " + entriesLeft);
    }
    else {
      logger.info("SENDER QUEUES ARE DRAINED");
    }
  }
  
  /** Validates the contents of the local cache. */
  public static void validateSequentialKeysTask() {
    Log.getLogWriter().info("validateSequentialKeysInRegion: Validation started");
    
    // Wait for silence listener
    SilenceListener.waitForSilence(30, 5000);

    for (String name : regionNames){
      Region region = RegionHelper.getRegion(name);
      validateRegion(region);
    }    
    Log.getLogWriter().info("validateSequentialKeysInRegion: Validation complete");
  }
  
  public static void validateRegion(Region aRegion) {
    Log.getLogWriter().info("Validating region: " + aRegion.getFullPath());
    
    if(aRegion == null){
      throw new TestException("Region is null ");      
    }
    
    StringBuffer aStr = new StringBuffer();
    if(RemoteTestModule.getMyClientName().contains("edge")){
   // All clients/servers should have all keys
      Set serverKeys = aRegion.keySetOnServer();
      int expectedRegionSize = serverKeys.size();

      Set localKeys = aRegion.keySet();
//      int regionSize = localKeys.size();
      
      Set myEntries = aRegion.entrySet();
      Object[] entries = myEntries.toArray();
      int localKeySize = entries.length;
      Comparator myComparator = new RegionEntryComparator();
      java.util.Arrays.sort(entries, myComparator);

      // If region is empty (on client & server), there's no need to go further
      if ((entries.length == 0) && (expectedRegionSize == 0)) {
         return;
      }

      Log.getLogWriter().info("Expecting " + expectedRegionSize + " entries from server in Region " + aRegion.getFullPath() + ", found " + localKeySize + " entries locally\n");
      if (localKeySize != expectedRegionSize) {
         aStr.append("Expected " + expectedRegionSize + " keys from  server in Region " + aRegion.getFullPath() + " but found " + localKeySize + " entries locally\n");
      }

      Log.getLogWriter().info("Checking for missing or extra keys in client region");
      // Extra keys (not in the server region)?
      List unexpectedKeys = new ArrayList(localKeys);
      unexpectedKeys.removeAll(serverKeys);
      if (unexpectedKeys.size() > 0) {
         aStr.append("Extra keys (not found on server): " + unexpectedKeys + "\n");
      } 

      // Are we missing keys (found in server region)?
      List missingKeys= new ArrayList(serverKeys);
      missingKeys.removeAll(localKeys);
      if (missingKeys.size() > 0) { 
         aStr.append("Missing keys (found on server, but not locally) = " + missingKeys + "\n");
      }
    }           
    
    // check the value of each entry in the local cache
    WANBlackboard bb = WANBlackboard.getInstance();
    int requiredSizeFromBB = (bb.getSharedMap().get(aRegion.getName()) != null) ? ((Long)bb.getSharedMap().get(aRegion.getName())).intValue() : 0;
    int keySize = aRegion.size();
    
    String str = "SUPPOSED TO HAVE:" + requiredSizeFromBB + " FROM BB, DOES HAVE:" + keySize;
    Log.getLogWriter().info(str);
    if (requiredSizeFromBB != keySize) {
      aStr.append(str);
    } 
    
    // Check whether each entry has value as ITERATIONS (default to 1000)
    Log.getLogWriter().info("Entering the validation loop ");
    for (long i = 1; i < requiredSizeFromBB; i++) {
      Object key = NameFactory.getObjectNameForCounter(i);
      Object val = aRegion.get(key);
      
      logger.info("Region " + aRegion.getName() + " has entry: " + key + " => " + val);
      if (val == null) {
        String s = "No value in cache at " + key;
        throw new TestException(s);
      }
      else if (val instanceof Integer) {
        int ival = ((Integer)val).intValue();
        if (ival != ITERATIONS) {
          String s = "Wrong value in cache at " + key + ", expected "
              + ITERATIONS + " but got " + ival;
          throw new TestException(s);
        }
      }
      else if (val instanceof byte[]) {
        if (((byte[])val).length != ITERATIONS) {
          String s = "Wrong value in cache at " + key
              + ", expected byte[] of length " + ITERATIONS
              + " but got length " + ((byte[])val).length;
          throw new TestException(s);
        }
      }
      else if (val instanceof PSTObject) {
        if (((PSTObject)val).getIndex() != ITERATIONS) {
          String s = "Wrong value in cache at " + key
              + ", expected PSTObject with index " + ITERATIONS
              + " but got index " + ((PSTObject)val).getIndex();
          throw new TestException(s);
        }
      }
      else if (val instanceof ValueHolder){
        if (!((ValueHolder)val).getMyValue().equals(new Long(ITERATIONS))) {
          String s = "Wrong value in cache at " + key
              + ", expected ValueHolder with myValue " + ITERATIONS
              + " but got " + ((ValueHolder)val).getMyValue();
          throw new TestException(s);
        }
      }
      else if (val instanceof QueryObject){
        if(((QueryObject)val).aPrimitiveLong != ITERATIONS){
          String s = "Wrong value in cache at " + key + ", expected QueryObject with aPrimitiveLong=" + ITERATIONS
          + " but got " + ((QueryObject)val).aPrimitiveLong;
          throw new TestException(s);
        }
      }
      else {
        String s = "Wrong value object type in cache at " + key + ", got " + val + " of type " + val.getClass().getName();
        throw new TestException(s);
      }
    }
    
    if (aStr.length() > 0) {
      throw new TestException(aStr.toString());
    }
    
    Log.getLogWriter().info("Validated region: " + aRegion.getFullPath());
  }
  
  /** Prints the contents of the local cache. */
  public static void printRegionDataTask() throws Exception {   
    logger.info("Inside printRegionDataTask.");
    for (String regionName : regionNames){      
      Region region = RegionHelper.getRegion(regionName);
      if(region == null){
        throw new TestException("Region in printRegionDataTask is null");
      }
      
      StringBuffer buffer = new StringBuffer();
      buffer.append("printRegionDataTask: region data for " + region.getFullPath() + " => \n");
      Set keyset = new TreeSet(region.keySet());
      Iterator keyItr = keyset.iterator();
      while (keyItr.hasNext()) {
        Object key = keyItr.next();
        buffer.append(key).append(":").append(region.get(key)).append("\n");        
      }
      logger.info(buffer.toString());
    }
    logger.info("Done printRegionDataTask.");
  }  
  
  /**
   * Written for explicit wan version tests which has three sites as 
   * A (7.0) connected to B (7.0) and C (6.5).
   * Returns remote distributed system names for sites running on 7.0. 
   * This is used to create topology of GatewaySenders
   */
  public static SortedSet<String> getDistributedSystemsForV70() {
    String localds = DistributedSystemHelper.getDistributedSystemName();    
    SortedSet<String> hubds = new TreeSet();
    //for p2p test
    if (localds.equals("ds_1")) { 
      hubds.add("ds_2");      
    } else if(localds.equals("ds_2")) { 
      hubds.add("ds_1");
    }
    
    //for hct test
    if (localds.equals("bridgeds_1")) { 
      hubds.add("bridgeds_2");      
    } else if(localds.equals("bridgeds_2")) { 
      hubds.add("bridgeds_1");
    }
    
    return hubds;
  }
}
