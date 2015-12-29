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
package newWan.security;

import hydra.BridgeHelper;
import hydra.ConfigPrms;
import hydra.GatewaySenderHelper;
import hydra.Log;
import hydra.RegionHelper;
import hydra.TestConfig;
import hydra.BridgeHelper.Endpoint;
import hydratest.security.SecurityTestPrms;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import newWan.WANBlackboard;
import newWan.WANTest;
import newWan.WANTestPrms;
import security.SecurityClientsPrms;
import util.TestException;
import wan.CacheClientPrms;
import wan.CacheServerPrms;

import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.cache.ClientHelper;
import com.gemstone.gemfire.cache.EntryDestroyedException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.cache.RegionQueue;
import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderImpl;
import com.gemstone.gemfire.internal.cache.wan.serial.SerialGatewaySenderImpl;
import com.gemstone.gemfire.security.AuthenticationFailedException;

/**
 * New wan security tests
 * @author rdiyewar
 * @version 7.0
 * 
 */

public class WanSecurity extends WANTest{

  public static String VALID_PRIFIX = "Valid";
  public static String INVALID_PRIFIX = "Invalid";

  public static boolean isInvalid = false;
  
  private static WanSecurity instance ;
  
  static ArrayList keyList = new ArrayList(); //to store keys per vm
  
  public synchronized static void initializeDatastore(){ 
    if (instance == null) {
      instance = new WanSecurity();
      WANTest.initializeDatastore();
    }
  }
  
  /**
   * Initializes a peer cache based on the {@link CacheClientPrms}. The task is
   * same as initPeerCacheTask but it doesnot create GateWayHub
   */
  public static synchronized void HydraTask_initSecurityPeerCache() {
    if(instance == null){
      initializeDatastore();
    }
    instance.initSecurityPeerCache();
  }
  
  public void initSecurityPeerCache(){
    boolean expectedFail = SecurityClientsPrms.isExpectedException();
    try {      
      initPeerCache();
      initDatastoreRegion();
      if (expectedFail) {
        throw new TestException("Expected this to throw AuthFailException");
      }
    }
    catch (AuthenticationFailedException e) {
      if (expectedFail) {
        Log.getLogWriter().info(
            "Got expected AuthenticationFailedException: " + e.getMessage());
      }
      else {
        throw new TestException(
            "AuthenticationFailedException while openCacheTask :"
                + e.getMessage());
      }
    }
    catch (Exception e) {
      throw new TestException("Exception while openCacheTask :"
          + e.getMessage());
    }

  }
  
  /**
   * Initializes a server cache based on the {@link CacheServerPrms}. Looks for
   * Security credentials if required
   */
  public static void initSecurityServerCacheTask() {
    if(instance == null){
      initializeDatastore();
    }
    instance.initSecurityServerCache();
  }
  
  public void initSecurityServerCache() {
    boolean expectedFail = SecurityClientsPrms.isExpectedException();
    try {
      initPeerCache();
      initDatastoreRegion();
      String bridgeConfig = ConfigPrms.getBridgeConfig();
      BridgeHelper.startBridgeServer(bridgeConfig);      
      if (expectedFail) {
        throw new TestException("Expected this to throw AuthFailException");
      }
    }
    catch (AuthenticationFailedException e) {
      if (expectedFail) {
        Log.getLogWriter().info(
            "Got expected AuthenticationFailedException: " + e.getMessage());
      }
      else {
        throw new TestException(
            "AuthenticationFailedException while openCacheTask :"
                + e.getMessage());
      }
    }
    catch (Exception e) {
      throw new TestException("Exception while openCacheTask :"
          + e.getMessage());
    }
  }
  
  public synchronized static void HydraTask_initWANComponents(){
    if(instance == null){
      initializeDatastore();
    }
    instance.initWanComponents();
  }
  
  public void initWanComponents(){    
    boolean expectedFail = SecurityClientsPrms.isExpectedException();
    isInvalid = SecurityTestPrms.useBogusPassword();            
    try {      
      initGatewaySender();
      startGatewaySender();
      if (expectedFail) {
        throw new TestException("Expected this to throw AuthenticationFailedException");
      }
    }
    catch (Exception e) {
      if ((e.getCause() instanceof AuthenticationFailedException)
          || (e.getCause().getCause() instanceof AuthenticationFailedException)) {
        if (expectedFail) {
          Log.getLogWriter().info(
              "Got expected AuthenticationFailedException: " + e.getMessage());
        }
        else {
          throw new TestException(
              "AuthenticationFailedException while openCacheTask :"
                  + e.getMessage());
        }
      }
      else {
        throw new TestException("Exception while openCacheTask :"
            + e.getMessage());
      }
    }
    
    initGatewayReceiver(); // Receivers are started without any security auth check.
  }
  
  /**
   * Same as the putSequentialKeysTask() but with the keys prefixed with "valid"
   */
  public static void putSequentialKeysTaskForValid() throws Exception {
    Set regionVisited = new HashSet();
    for (String regionName : regionNames) {
      if (!regionVisited.contains(regionName)) {
        regionVisited.add(regionName);
        Region region = RegionHelper.getRegion(regionName);
        Log.getLogWriter().info(
            "putSequentialKeysTaskForValid : working on region " + regionName);

        String key = VALID_PRIFIX
            + +WANBlackboard.getInstance().getSharedCounters()
                .incrementAndRead(WANBlackboard.currentEntry_valid);
        Log.getLogWriter().info("The vm will be operating on the key : " + key);

        for (int i = 1; i <= ITERATIONS; i++) {
          if (TestConfig.tab().getRandGen().nextBoolean()) {
            region.put(key, new Integer(i));
          }
          else {
            Object v = region.replace(key, new Integer(i));
            if (v == null) { // force update
              region.put(key, new Integer(i));
            }
          }

        }
      }
    }
  }
  
  /**
   * Same as the putSequentialKeysTask() but with the keys prefixed with
   * "invalid"
   */
  public static void putSequentialKeysTaskForInValid() throws Exception {
    Set regionVisited = new HashSet();
    for (String regionName : regionNames) {
      if (!regionVisited.contains(regionName)) {
        regionVisited.add(regionName);
        Region region = RegionHelper.getRegion(regionName);
        Log.getLogWriter()
            .info(
                "putSequentialKeysTaskForInValid : working on region "
                    + regionName);

        String key = INVALID_PRIFIX
            + +WANBlackboard.getInstance().getSharedCounters()
                .incrementAndRead(WANBlackboard.currentEntry_invalid);
        Log.getLogWriter().info("The vm will be operating on the key : " + key);

        for (int i = 1; i <= ITERATIONS; i++) {
          if (TestConfig.tab().getRandGen().nextBoolean()) {
            region.put(key, new Integer(i));
          }
          else {
            Object v = region.replace(key, new Integer(i));
            if (v == null) { // force update
              region.put(key, new Integer(i));
            }
          }
        }
      }
    }
  }
  
  /**
   * Clients doing a putSequentialTask after checking whether the primary is a
   * valid or invalid server
   * 
   */

  public static void clientPutSequentialKeysTask() throws Exception {
    Region region = RegionHelper.getRegion(regionNames.get(0));
    PoolImpl mybw = ClientHelper.getPool(region);
    ServerLocation primary = mybw.getPrimary();
    if(primary == null) {
      throw new InternalGemFireException("Primary is null" + primary);
    }
    
    Endpoint primaryEndpoint = getEndpoint(primary);

    if(primaryEndpoint == null) {
      throw new InternalGemFireException("Unable to find endpoint for primary " + primary);
    }
    Log.getLogWriter().info("Primary name is " + primaryEndpoint.getName());
    if (primaryEndpoint.getName().startsWith("validbridge_")) {
      putSequentialKeysTaskForValid();
    }
    else if (primaryEndpoint.getName().startsWith("invalidbridge_")) {
      putSequentialKeysTaskForInValid();
    }
  }
  
  public static void waitForQueuesToDrainTask() {
    instance.waitForQueuesToDrain();
  }
  
  /**
   * Waits {@link newWan.WANTestPrms#secToWaitForQueue} for queues to drain.
   * Suitable for use as a helper method or hydra task.
   */
  public void waitForQueuesToDrain() {
    // Lets check to make sure the queues are empty
    long startTime = System.currentTimeMillis();
    long maxWait = WANTestPrms.getSecToWaitForQueue();
    long entriesLeft = 0;
    Map queueEntryMap = new HashMap();
    while ((System.currentTimeMillis() - startTime) < (maxWait * 1000)) {
      boolean pass = true;
      entriesLeft = 0;

      for (GatewaySender gs : gatewaySenders) {
        if (gs.getId().matches("(.*)_to_invalid_(.*)")) {
          if (gs instanceof SerialGatewaySenderImpl) {
            if (((SerialGatewaySenderImpl)gs).getQueues() != null) {
              int queuesize = 0;
              Set<RegionQueue> rqs = ((SerialGatewaySenderImpl)gs).getQueues();
              for (RegionQueue rq: rqs){
                queuesize += rq.size();
              }
              if (!queueEntryMap.containsKey(gs.getId())) {
                queueEntryMap.put(gs.getId(), new Integer(queuesize));
              }
              else {
                int queueSizeInBB = (Integer)queueEntryMap.get(gs.getId());
                if (queuesize < queueSizeInBB) {
                  logger
                      .warning("Invalid queue not expected to drain, expected queue size in "
                          + gs.getId()
                          + " to be "
                          + queueSizeInBB
                          + " but it has " + queuesize + " entries.");
                }
              }
            }
            else if (gs instanceof ParallelGatewaySenderImpl) {
              if (((ParallelGatewaySenderImpl)gs).getQueues() != null) {
                RegionQueue rq = ((ParallelGatewaySenderImpl)gs).getQueues().toArray(new RegionQueue[1])[0];
                int queuesize = rq.size();
                if (!queueEntryMap.containsKey(gs.getId())) {
                  queueEntryMap.put(gs.getId(), new Integer(queuesize));
                }
                else {
                  int queueSizeInBB = (Integer)queueEntryMap.get(gs.getId());
                  if (queuesize < queueSizeInBB) {
                    logger
                        .warning("Invalid queue not expected to drain, expected queue size in "
                            + gs.getId()
                            + " to be "
                            + queueSizeInBB
                            + " but it has " + queuesize + " entries.");
                  }
                }
              }
            }
            else {
              throw new TestException("Unknown class of gateway sender: "
                  + GatewaySenderHelper.gatewaySenderToString(gs));
            }
          }
        }
        else {
          if (gs instanceof SerialGatewaySenderImpl) {
            if (((SerialGatewaySenderImpl)gs).getQueues() != null) {
              int queuesize = 0;
              Set<RegionQueue> rqs = ((SerialGatewaySenderImpl)gs).getQueues();
              for (RegionQueue rq: rqs){
                queuesize += rq.size();
              }
              entriesLeft += queuesize;
              queueEntryMap.put(gs.getId(), new Integer(queuesize));
              if (queuesize > 0) {
                logger
                    .warning("Still waiting for queue to drain. SerialGatewaySender "
                        + gs + " has " + queuesize + " entries in it.");
                pass = false;
              }
            }
          }
          else if (gs instanceof ParallelGatewaySenderImpl) {
            if (((ParallelGatewaySenderImpl)gs).getQueues() != null) {
              RegionQueue rq = ((ParallelGatewaySenderImpl)gs).getQueues().toArray(new RegionQueue[1])[0];
              int queuesize = rq.size();
              entriesLeft += queuesize;
              queueEntryMap.put(gs.getId(), new Integer(queuesize));
              if (queuesize > 0) {
                logger
                    .warning("Still waiting for queue to drain. ParallelGatewaySender "
                        + gs + " has " + queuesize + " entries in it.");
                pass = false;
              }
            }
          }
          else {
            throw new TestException("Unknown class of gateway sender: "
                + GatewaySenderHelper.gatewaySenderToString(gs));
          }
        }
      }
      if (pass) {
        entriesLeft = 0;
        break;
      }
      else {
        try {
          Thread.sleep(1000);
        }
        catch (Exception e) {
          e.printStackTrace();
        }
      }
      printQueueContents();
    }
    if (entriesLeft > 0) {
      throw new TestException(
          "Timed out waiting for queue to drain. Waited for " + maxWait
              + " sec, total entries left in all queues are " + entriesLeft
              + ". queueEntryMap=" + queueEntryMap);
    }
    else {
      logger.info("SENDER QUEUES ARE DRAINED");
    }
  }
  
  /** validation for the valid wan clients with the right prefix * */
  public static void validateValidSequentialKeysTask() throws Exception {
    Log.getLogWriter().info("Validating regions for valid sequential keys");
    for (String regionName : regionNames) {
      Region region = RegionHelper.getRegion(regionName);
      Set keys = region.keySet();

      checkKeys(regionName, VALID_PRIFIX);
      
      long requiredSize = WANBlackboard.getInstance().getSharedCounters().read(WANBlackboard.currentEntry_valid);
      Log.getLogWriter().info("In Region " + region.getFullPath() + " SUPPOSED TO HAVE:" + requiredSize + " DOES HAVE:" + keys.size());
      if (requiredSize != keys.size()){
        throw new TestException("In Region " + region.getFullPath() + " SUPPOSED TO HAVE:" + requiredSize + " DOES HAVE:" + keys.size());
      }
      
      //validation
      Iterator kI = keys.iterator();
      for(int i=1; i < requiredSize; i++) {
        Object key = VALID_PRIFIX + i;
        Object val = region.get(key);
        
        if (val == null) {
          String s = "No value in region " + region + " at " + key;
          throw new TestException(s);
        }else if (val instanceof Integer) {
          int ival = ((Integer)val).intValue();
          if (ival != ITERATIONS) {
            String s = "Wrong value in region " + region.getFullPath() + " at " + key + ", expected "
                     + ITERATIONS + " but got " + ival;
            throw new TestException(s);
          }
        } 
      }
    }
  }

  /** validation for the valid wan clients with the right prefix * */
  public static void validateInvalidSequentialKeysTask() throws Exception {
    Log.getLogWriter().info("Validating regions for invalid sequential keys");
    for (String regionName : regionNames) {
      Region region = RegionHelper.getRegion(regionName);
      Set keys = region.keySet();

      checkKeys(regionName, INVALID_PRIFIX);
     
      // can not check for invalid entries as wan sites may not be connected
      long requiredSize = WANBlackboard.getInstance().getSharedCounters().read(WANBlackboard.currentEntry_invalid);
      Log.getLogWriter().info("In Region " + region.getFullPath() + " Entries count is:" + keys.size());
    }
  }
  
  /**
   * validation of the edge clients for the sequential keys after checking for
   * valid and invalid primary servers
   */
  public static void clientValidateSequentialKeysTask() throws Exception {
    Region region = RegionHelper.getRegion(regionNames.get(0));
    PoolImpl mybw = ClientHelper.getPool(region);
    ServerLocation primary = mybw.getPrimary();
    if(primary == null) {
      throw new InternalGemFireException("Primary is null" + primary);
    }
    
    Endpoint primaryEndpoint = getEndpoint(primary);

    if(primaryEndpoint == null) {
      throw new InternalGemFireException("Unable to find endpoint for primary " + primary);
    }
    Log.getLogWriter().info("Primary name is " + primaryEndpoint.getName());
    if (primaryEndpoint.getName().startsWith("validbridge_")) {
      validateValidSequentialKeysTask();
    }
    else if (primaryEndpoint.getName().startsWith("invalidbridge_")) {
      validateInvalidSequentialKeysTask();
    }
  }
  
  /**
   * Checking for the security wan sites whether they have received keys not
   * valid to them
   */

  public static void checkKeys(String regionName, String validKeyPrefix) {
    Region region = RegionHelper.getRegion(regionName);
    Set keys = region.keySet();

    Iterator iterator = keys.iterator();
    while (iterator.hasNext()) {
      String key = (String)(iterator.next());
      if (!key.startsWith(validKeyPrefix)) {
        throw new TestException("Invalid key found in the cache " + key);
      }
      else {
        Log.getLogWriter().info("Found valid key " + key);
      }
    }
  }
  
  private static Endpoint getEndpoint(ServerLocation location) {
    List endpoints = BridgeHelper.getEndpoints();
    
    InetSocketAddress ia = null;
    for(Iterator itr = endpoints.iterator(); itr.hasNext(); ) {
      Endpoint next = (Endpoint) itr.next();
      if(next.getPort() == location.getPort()) {
        if(next.getHost().equals(location.getHostName())) {
          return next;
        } else {
          try {
            ia = new InetSocketAddress(location.getHostName(),location.getPort());
            if(ia.getAddress().getHostAddress().equals(next.getAddress())) {
              return next;
            }
          } catch(Exception e) {
            // continue
            e.printStackTrace();
          }
        }
      } 
    }
    return null;
  }
  
  
  /**
   * Same as the putSequentialKeysTask() but with the keys prefixed with
   * "reader" Adds the key to the ArrayList
   */
  public static void putSequentialKeysTaskForReader() throws Exception {
    Region region = RegionHelper.getRegion(regionNames.get(0));
    String key = "reader_"
        + WANBlackboard.getInstance().getSharedCounters().incrementAndRead(
            WANBlackboard.currentEntry_reader);
    Log.getLogWriter().info("The vm will be operating on the key : " + key);
    keyList.add(key);

    int sleepMs = CacheClientPrms.getSleepSec() * 1000;
    for (int i = 1; i <= ITERATIONS; i++) {
      if (TestConfig.tab().getRandGen().nextBoolean()) {
        region.put(key, new Integer(i));
      }
      else {
        Object v = region.replace(key, new Integer(i));
        if (v == null) { // force update
          region.put(key, new Integer(i));
        }
      }
    }
  }
  
  /**
   * Same as the putSequentialKeysTask() but with the keys prefixed with
   * "writer" Adds the key to the ArrayList
   */
  public static void putSequentialKeysTaskForWriter() throws Exception {   
    Region region = RegionHelper.getRegion(regionNames.get(0));
    String key = "writer_"
        + WANBlackboard.getInstance().getSharedCounters().incrementAndRead(
            WANBlackboard.currentEntry_writer);
    Log.getLogWriter().info("The vm will be operating on the key : " + key);
    keyList.add(key);

    int sleepMs = CacheClientPrms.getSleepSec() * 1000;
    for (int i = 1; i <= ITERATIONS; i++) {
      if (TestConfig.tab().getRandGen().nextBoolean()) {
        region.put(key, new Integer(i));
      }
      else {
        Object v = region.replace(key, new Integer(i));
        if (v == null) { // force update
          region.put(key, new Integer(i));
        }
      }
    }    
  }
  
  /**
   * Prints the keyList (ArrayList
   */
  public static void printKeyListTask() {
    Iterator Itr = keyList.iterator();
    while (Itr.hasNext()) {
      Log.getLogWriter().info((String)Itr.next());
    }
  }
  
  /**
   * Validating Writer Wan Sites
   */
  public static void validateWriterWanSiteEntriesTask() {
    Log.getLogWriter().info("Sleeping for some time ..");
    hydra.MasterController.sleepForMs(100000);
    Region region = RegionHelper.getRegion(regionNames.get(0));
    if (region.isEmpty()) {
      throw new TestException(" Region has no entries to validate ");
    }
    checkKeys(region.getName(), "writer_");
    
    long requiredSize = WANBlackboard.getInstance().getSharedCounters().read(
        WANBlackboard.currentEntry_writer);
    checkKeyRegionEntries(region.getName(), "writer_", requiredSize);
  }
  
  /**
   * Validating Reader Wan Sites
   */
  public static void validateReaderWanSiteEntriesTask() {
    Log.getLogWriter().info("Sleeping for some time ..");
    hydra.MasterController.sleepForMs(100000);
    Region region = RegionHelper.getRegion(regionNames.get(0));
    if (region.isEmpty()) {
      throw new TestException(" Region has no entries to validate ");
    }
    long requiredSize = WANBlackboard.getInstance().getSharedCounters().read(
        WANBlackboard.currentEntry_writer);
    checkKeyRegionEntries(region.getName(), "writer_", requiredSize);

    Iterator iterator = region.entrySet(false).iterator();
    Region.Entry entry = null;
    Object key = null;
    Object value = null;
    while (iterator.hasNext()) {
      entry = (Region.Entry)iterator.next();
      key = entry.getKey();
      value = entry.getValue();

      if (((String)key).startsWith("reader_")) {
        if (!keyList.contains(key)) {
          throw new TestException(
              "Found reader key that is not present in the keyList");
        }
        else if (((Integer)value).intValue() != ITERATIONS) {
          String s = "Wrong value in region " + region.getFullPath() + " at " + key + ", expected "
              + ITERATIONS + " but got " + ((Integer)value).intValue();
          throw new TestException(s);
        }
      }
    }
  }

  /**
   * Check whether a vm has expected entries for the key prefix
   * 
   */
  protected static void checkKeyRegionEntries(String regionName, String keyPrefix,
      long expectedsize) {

    Region region = RegionHelper.getRegion(regionName);
    Log.getLogWriter().info(
        "Key prefix is " + keyPrefix + " Expected size is " + expectedsize);

    for (int i = 1; i <= expectedsize; i++) {
      String key = keyPrefix + i;
      Object val = region.get(key);

      if (val == null) {
        String s = "No value in cache at " + key;
        throw new TestException(s);
      }
      else if (((Integer)val).intValue() != ITERATIONS) {
        Log.getLogWriter().info(
            "Key is :" + key + " Value found in region is "
                + ((Integer)val).intValue());
        String s = "Wrong value in cache at " + key + ", expected "
            + ITERATIONS + " but got " + ((Integer)val).intValue();
        throw new TestException(s);
      }
      else {
        Log.getLogWriter().info(
            "Key is :" + key + " Value found in region is "
                + ((Integer)val).intValue());
      }
    }
  }  
  
  /**
   * Reader wan site destroys all the keys in its region
   */
  public static void readerDestroyAllKeysTask() {

    Region region = RegionHelper.getRegion(regionNames.get(0));

    Iterator iterator = region.entrySet(false).iterator();
    Region.Entry entry = null;
    Object key = null;

    while (iterator.hasNext()) {
      entry = (Region.Entry)iterator.next();
      key = entry.getKey();

      try {
        region.destroy(key);
      }
      catch (EntryNotFoundException e) {
        Log.getLogWriter().info("Entry Not found for key " + key + ". Expected in this test.");
      }
    }

    if (region.isEmpty()) {
      Log.getLogWriter().info(
          "Completed the destroy operation for all the keys in the region");
    }
    else {
      throw new TestException(
          "Region is supposed to be empty but that is not the case");
    }
  }
  
  /**
   * Writer vms destroying only those keys that it created
   */
  public static void writerDestroyCreatedKeysTask() {
    Region region = RegionHelper.getRegion(regionNames.get(0));

    Iterator iterator = keyList.iterator();
    String key = null;

    while (iterator.hasNext()) {
      try {
        key = (String)iterator.next();
        Log.getLogWriter().info(
            "Destroying key :" + key + " which is present in the keyList");
        region.destroy(key);
      }
      catch (EntryNotFoundException e) {
        Log.getLogWriter().info("Entry Not found.");
      }
      catch (EntryDestroyedException e) {
        Log.getLogWriter().info("Entry Already destroyed.");
      }
    }
  }
  
  /**
   * Check whether the writer sites are having empty region
   */
  public static void checkWriterRegionContentsEmpty() {
    Region region = RegionHelper.getRegion(regionNames.get(0));
    if (region.isEmpty()) {
      Log.getLogWriter().info("Region is empty as expected");
    }
    else {
      throw new TestException(
          "Region content supposed to be empty but it is having size of "
              + region.size());
    }
  }
}
