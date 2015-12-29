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
package hct;

import com.gemstone.gemfire.cache.*;

import cq.CQUtilPrms;
import parReg.query.NewPortfolio;

import hydra.BridgeHelper;
import hydra.CacheHelper;
import hydra.ClientVmInfo;
import hydra.ClientVmMgr;
import hydra.ClientVmNotFoundException;
import hydra.Log;
import hydra.ProcessMgr;
import hydra.RegionHelper;
import hydra.TestConfig;
import hydra.blackboard.SharedCounters;
import hydra.MasterController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.Iterator;
import java.util.Set;

import util.EventCountersBB;
import util.ExpCounterValue;
import util.NameFactory;
import util.SilenceListener;
import util.SummaryLogListener;
import util.TestException;
import util.TestHelper;
import util.OperationCountersBB;
import util.PRObserver;

/**
 * Added putAll operations in the BridgeNotify
 * @author eshu
 *
 */
public class PutAllBridgeNotify extends BridgeNotify {
  static protected PutAllBridgeNotify putAllBridgeNotify;
  protected static Random rnd = new Random();  
  protected static long maxNames = TestConfig.tab().longAt(hct.BridgeNotifyPrms.numKeys, 50000);
  
  public synchronized static void HydraTask_initialize() {
    if (bridgeClient == null) {
       bridgeClient = new PutAllBridgeNotify();
       bridgeClient.initialize();

       // Clear out any unwanted AfterRegionCreate counts
       OperationCountersBB.getBB().zeroAllCounters();
    }
  }
  
  public void initialize() {
    super.initialize();
    
    maxNames = TestConfig.tab().longAt(hct.BridgeNotifyPrms.numKeys, 50000);
    Log.getLogWriter().info("Max number of keys in the test is " + maxNames);
    
    //remove the eventListen on the publishers
    Region aRegion = RegionHelper.getRegion(REGION_NAME);
    CacheListener[] assignedListeners = aRegion.getAttributes().getCacheListeners();
    CacheListener myListener = BridgeNotifyPrms.getClientListener();
    if (myListener != null) {
      for (int i=0; i<assignedListeners.length; i++) {
        if (assignedListeners[i] instanceof hct.EventListener)
        aRegion.getAttributesMutator().removeCacheListener(assignedListeners[i]);
      }
    }    
  }
  
  public static void HydraTask_verifyPutAllCounters() {
    SilenceListener.waitForSilence(30, 1000);
    bridgeClient.checkEventCounters();
 }
  
  protected int getSizeForPutAll () {
    // todo@lhughes - ensure putAll entries are colocated, then remove
    // this restrict of 1 entry put putAll
    if (useTransactions) {
       return 1;
    }

    int minQty = TestConfig.tab().intAt(CQUtilPrms.minPutAllSize, 100);
    int maxQty = TestConfig.tab().intAt(CQUtilPrms.maxPutAllSize, 300);
    return rnd.nextInt(maxQty - minQty) + minQty;    
  }
  
  public static void HydraTask_doEntryOperations(){
    Region rootRegion = CacheHelper.getCache().getRegion(BridgeNotify.REGION_NAME);
    bridgeClient.doEntryOperations(rootRegion);
  }
  
  protected Object getUpdateObject(String name) {
    return getObjectToAdd(name);
  }
  
  protected Object getObjectToAdd(String name) {
    NewPortfolio anObject = new NewPortfolio (name, (int) NameFactory.getCounterForName(name));
    return anObject;
  }
  
  /**
   * update BB for listener invocations
   */
  protected void addObject(Region aRegion, String name, boolean logAddition) {
    Object anObj = getObjectToAdd(name);
    String callback = createCallbackPrefix + ProcessMgr.getProcessId();
    if (logAddition)
        Log.getLogWriter().info("addObject: calling put for name " + name + ", object " + TestHelper.toString(anObj) + " callback is " + callback + ", region is " + aRegion.getFullPath());
    try {
        aRegion.put(name, anObj, callback);
    } catch (Exception e) {
        throw new TestException(TestHelper.getStackTrace(e));
    }

    // serialBridgeNotifyMixedInterests needs help knowing how many events to expect
    int numExpectedEvents = 1;               // for ALL_KEYS client
    if (originalKeyList.contains(name)) {    // 1 for odd/even keylist client
       numExpectedEvents++;
    }
    List singleKeyList = (List)BridgeNotifyBB.getBB().getSharedMap().get( BridgeNotifyBB.SINGLE_KEYS_REGISTERED );
    if (singleKeyList.contains(name)) {      // 1 if we selected the singleKey clients key
       numExpectedEvents++;
    }
    BridgeNotifyBB.getBB().getSharedCounters().add(BridgeNotifyBB.EXPECTED_CREATE, numExpectedEvents);
  }

  /**
   * ConcurrentMap putIfAbsent API
   */
  protected void putIfAbsent(Region aRegion, boolean logAddition) {
    String name = NameFactory.getNextPositiveObjectName();
    Object anObj = getObjectToAdd(name);
    if (logAddition)
        Log.getLogWriter().info("putIfAbsent: calling putIfAbsent for name " + name + ", object " + TestHelper.toString(anObj));
    try {
        aRegion.putIfAbsent(name, anObj);
    } catch (Exception e) {
        throw new TestException(TestHelper.getStackTrace(e));
    }

    // serialBridgeNotifyMixedInterests needs help knowing how many events to expect
    int numExpectedEvents = 1;               // for ALL_KEYS client
    if (originalKeyList.contains(name)) {    // 1 for odd/even keylist client
       numExpectedEvents++;
    }
    List singleKeyList = (List)BridgeNotifyBB.getBB().getSharedMap().get( BridgeNotifyBB.SINGLE_KEYS_REGISTERED );
    if (singleKeyList.contains(name)) {      // 1 if we selected the singleKey clients key
       numExpectedEvents++;
    }
    BridgeNotifyBB.getBB().getSharedCounters().add(BridgeNotifyBB.EXPECTED_CREATE, numExpectedEvents);
  }

  /**
   * add object via putAll with randomly chosen size.
   */
  protected void addObjectViaPutAll(Region aRegion, boolean logAddition) {
    int sizeForPutAll = getSizeForPutAll();

    addObjectViaPutAll(aRegion, sizeForPutAll, logAddition);
  }
  protected void addObjectViaPutAll(Region aRegion, int sizeForPutAll, boolean logAddition) {  
    HashMap map = new HashMap();
    String name = null;
    
    for (int i = 0; i < sizeForPutAll; i++) {
      name = NameFactory.getNextPositiveObjectName();
      Object anObj = getObjectToAdd(name);
      map.put(name, anObj);
      if (logAddition)
        Log.getLogWriter().info("addObjectViaPutAll: put in putAll map for name " + name + ", " +
                        "object " + TestHelper.toString(anObj) + ", region is " + aRegion.getFullPath());   
    }
    try {
        aRegion.putAll(map);
        Log.getLogWriter().info("PutAllCreate: put into region " + aRegion.getFullPath() + " with mapSize " + map.size());
    } catch (Exception e) {
        throw new TestException(TestHelper.getStackTrace(e));
    }
    BridgeNotifyBB.add("BridgeNotifyBB.NUM_PUTALL_CREATE", BridgeNotifyBB.NUM_PUTALL_CREATE, sizeForPutAll);

    // MixedInterests needs help knowing how many events to expect
    int numExpectedEvents = map.size();               // for ALL_KEYS client
    List singleKeyList = (List)BridgeNotifyBB.getBB().getSharedMap().get( BridgeNotifyBB.SINGLE_KEYS_REGISTERED );
    
    Iterator itr = map.keySet().iterator();
    while (itr.hasNext()) {
      String key = (String) itr.next();
      if (originalKeyList.contains(key)) {    // 1 for odd/even keylist client
         numExpectedEvents++;
      }
      if (singleKeyList.contains(key)) {      // 1 if we selected the singleKey clients key
        numExpectedEvents++;
     }
    }
    BridgeNotifyBB.getBB().getSharedCounters().add(BridgeNotifyBB.EXPECTED_CREATE, numExpectedEvents);    
  }
  
  //override method to update the correct counter for verifying events
  protected void updateObject(Region aRegion, Object name) {
    Region.Entry anObj = null;
   
    anObj = aRegion.getEntry(name); //check the local cache only, so this will guarantee servers have the entry
    if (anObj == null) return;
    
    Object newObj = getUpdateObject((String)name);
    try {
        String callback = updateCallbackPrefix + ProcessMgr.getProcessId();
        Log.getLogWriter().info("updateObject: replacing name " + name + " with " +
                TestHelper.toString(newObj) + "; old value is " + TestHelper.toString(anObj.getValue()) +
                ", callback is " + callback);
        aRegion.put(name, newObj, callback);
        Log.getLogWriter().info("Done with call to put (update)");
    } catch (Exception e) {
        throw new TestException(TestHelper.getStackTrace(e));
    }

    // serialBridgeNotifyMixedInterests needs help knowing how many events to expect
    int numExpectedEvents = 1;               // for ALL_KEYS client
    if (originalKeyList.contains(name)) {    // 1 for odd/even keylist client
       numExpectedEvents++;
    }
    List singleKeyList = (List)BridgeNotifyBB.getBB().getSharedMap().get( BridgeNotifyBB.SINGLE_KEYS_REGISTERED );
    if (singleKeyList.contains(name)) {      // 1 if we selected the singleKey clients key
       numExpectedEvents++;
    }
    BridgeNotifyBB.getBB().getSharedCounters().add(BridgeNotifyBB.EXPECTED_UPDATE, numExpectedEvents);
  }

  // ConcurrentMap replace api
  protected void replace(Region aRegion, Object name) {
    boolean replaced = false;
    Object anObj = null;
    try {
        anObj = aRegion.get(name);
    } catch (CacheLoaderException e) {
        throw new TestException(TestHelper.getStackTrace(e));
    } catch (TimeoutException e) {
        throw new TestException(TestHelper.getStackTrace(e));
    }

    Object newObj = getUpdateObject((String)name);
    try {
       Log.getLogWriter().info("replace: replacing name " + name + " with " + TestHelper.toString(newObj) + ".");
       Object returnVal = aRegion.replace(name, newObj);
       if (returnVal != null) {
         replaced = true;
       }
       Log.getLogWriter().info("Done with call to replace");
    } catch (Exception e) {
        throw new TestException(TestHelper.getStackTrace(e));
    }

    // serialBridgeNotifyMixedInterests needs help knowing how many events to expect
    int numExpectedEvents = 1;               // for ALL_KEYS client
    if (originalKeyList.contains(name)) {    // 1 for odd/even keylist client
       numExpectedEvents++;
    }
    List singleKeyList = (List)BridgeNotifyBB.getBB().getSharedMap().get( BridgeNotifyBB.SINGLE_KEYS_REGISTERED );
    if (singleKeyList.contains(name)) {      // 1 if we selected the singleKey clients key
       numExpectedEvents++;
    }
    BridgeNotifyBB.getBB().getSharedCounters().add(BridgeNotifyBB.EXPECTED_UPDATE, numExpectedEvents);
  }
  
  //override method with randomly chosen mapSize
  protected void updateObjectViaPutAll(Region aRegion) {
    int sizeForPutAll = getSizeForPutAll();
    HashMap map = new HashMap();
    String name = null;
    
    Set aSet = aRegion.keySet();
    Iterator iter = aSet.iterator();
    if (!iter.hasNext()) {
        Log.getLogWriter().info("updateObject: No names in region");
        return;
    }

    int mapSize = 0;
    Object newObj = null;
    while (iter.hasNext() && mapSize < sizeForPutAll) {
      Object potentialKey = iter.next();
      if (aRegion.getEntry(potentialKey)!=null) {  //check if the key is in local cache or not
        name = (String) potentialKey;
        newObj = getUpdateObject(name);
        map.put(name, newObj);
        Log.getLogWriter().info("updateObjectViaPutAll: put in putAll map for name " + name + 
            ", object " + TestHelper.toString(newObj));
        mapSize ++;
      } //need to make sure that no concurrent destroy or invalidate to verify listener invocations
    }

    try {       
        aRegion.putAll(map);
        Log.getLogWriter().info("Done with call to putall (update) mapSize " + mapSize + " to region " + aRegion.getFullPath());
    } catch (Exception e) {
        throw new TestException(TestHelper.getStackTrace(e));
    }
    
    BridgeNotifyBB.add("BridgeNotifyBB.NUM_PUTALL_UPDATE", BridgeNotifyBB.NUM_PUTALL_UPDATE, mapSize);
  
    // MixedInterests needs help knowing how many events to expect
    int numExpectedEvents = mapSize;               // for ALL_KEYS client and operation client
    List singleKeyList = (List)BridgeNotifyBB.getBB().getSharedMap().get( BridgeNotifyBB.SINGLE_KEYS_REGISTERED );
    
    Iterator itr = map.keySet().iterator();
    while (itr.hasNext()) {
      String key = (String) itr.next();
      if (originalKeyList.contains(key)) {    // 1 for odd/even keylist client
         numExpectedEvents++;
      }
      if (singleKeyList.contains(key)) {      // 1 if we selected the singleKey clients key
        numExpectedEvents++;
     }
    }
    BridgeNotifyBB.getBB().getSharedCounters().add(BridgeNotifyBB.EXPECTED_UPDATE, numExpectedEvents);
    
  }
  
  protected void checkEventCounters() {
    //wait for events to be delivered
    MasterController.sleepForMs(180000);
    
    Log.getLogWriter().info("checkOperationCounters will validate that putAll operations are exact");
    try {
      checkPutAllCounters(true);
    } catch (TestException e1) {
      Log.getLogWriter().info("checkPutAllCounters failed, checkEventCounters will validate that counters are exact");
      try {
        checkEventCounters(true);
      } catch (TestException e2) {
        Log.getLogWriter().info("checkEventCounters failed.");
        throw new TestException("missing events -- " + TestHelper.getStackTrace(e1) + TestHelper.getStackTrace(e2));        
      }
      throw new TestException("missing events -- " + TestHelper.getStackTrace(e1));
    }

    Log.getLogWriter().info("checkEventCounters will validate that counters are exact");
    checkEventCounters(true);
  }
  
  /*
   * check create and update events on the client vm -- PUTALL_CREATE becomes create 
   * and PUTALL_UPDATE becomes update event 
   */
  protected void checkEventCounters(boolean eventCountExact) {
    SharedCounters counters = BridgeNotifyBB.getBB().getSharedCounters();
    long expectedCreate = counters.read(BridgeNotifyBB.EXPECTED_CREATE);
    long expectedUpdate = counters.read(BridgeNotifyBB.EXPECTED_UPDATE);
    ArrayList al = new ArrayList();
    al.add(new ExpCounterValue("numAfterCreateEvents_isNotExp", expectedCreate, eventCountExact)); 
    al.add(new ExpCounterValue("numAfterUpdateEvents_isNotExp", expectedUpdate, eventCountExact)); 
    
    // check qualified events on clients
    EventCountersBB.getBB().checkEventCounters(al);

  }
  
  //only verify the putAll operations in this test (only servers and operational client have putAll operation
  protected void checkPutAllCounters(boolean eventCountExact) {
    int numBridges = TestConfig.tab().intAt(hct.BridgeNotifyPrms.numBridges);
    boolean isPartition = TestConfig.tab().booleanAt(hct.BridgeNotifyPrms.isPartition, false);
    long expPutAllCreate;
    long expPutAllUpdate;
    SharedCounters counters = BridgeNotifyBB.getBB().getSharedCounters();
    boolean isHA = TestConfig.tab().booleanAt(hct.BridgeNotifyPrms.isHATest, false);
   
    if (!isPartition) {
      if (isHA) {
        Log.getLogWriter().info("For HA test, could not check putAll events on bridges");
        return;
      }
      //for replicated bridges and operational client
      expPutAllCreate = counters.read(BridgeNotifyBB.NUM_PUTALL_CREATE) * (numBridges+1);
      expPutAllUpdate = counters.read(BridgeNotifyBB.NUM_PUTALL_UPDATE)*(numBridges+1);
    } else {
      //for parReg bridge and operational client
      expPutAllCreate = counters.read(BridgeNotifyBB.NUM_PUTALL_CREATE) * 2; 
      expPutAllUpdate = counters.read(BridgeNotifyBB.NUM_PUTALL_UPDATE)* 2;
    }

    ArrayList al = new ArrayList();
    al.add(new ExpCounterValue("numAfterCreateEvents_isPutAll", expPutAllCreate, eventCountExact)); 
    al.add(new ExpCounterValue("numAfterUpdateEvents_isPutAll", expPutAllUpdate, eventCountExact)); 
    
    // check qualified putAll operations
    OperationCountersBB.getBB().checkEventCounters(al);
  }
  
  protected void registerInterest(Region aRegion, CacheListener myListener,
      ArrayList oddKeys, ArrayList evenKeys) {

    String clientInterest = TestConfig.tasktab().stringAt( BridgeNotifyPrms.clientInterest, TestConfig.tab().stringAt( BridgeNotifyPrms.clientInterest, null) );
    try {
       if (clientInterest.equalsIgnoreCase("allKeys")) {
          aRegion.registerInterest( "ALL_KEYS", InterestResultPolicy.KEYS_VALUES );
          Log.getLogWriter().info("Registered interest in ALL_KEYS");
       } else if (clientInterest.equalsIgnoreCase("singleKey")) {
          Object myKey = NameFactory.getObjectNameForCounter(TestConfig.tab().getRandGen().nextInt(1, 20000));
          aRegion.registerInterest( myKey, InterestResultPolicy.KEYS_VALUES );
          Log.getLogWriter().info("Registered interest in a singleKey " + myKey);
          synchronized(  BridgeNotifyBB.class ) {
             List registeredKeys = (List)BridgeNotifyBB.getBB().getSharedMap().get( BridgeNotifyBB.SINGLE_KEYS_REGISTERED );
             registeredKeys.add( myKey );
             BridgeNotifyBB.getBB().getSharedMap().put( BridgeNotifyBB.SINGLE_KEYS_REGISTERED, registeredKeys );
          }
       } else if (clientInterest.equalsIgnoreCase("evenKeys")) {
          aRegion.registerInterest( evenKeys, InterestResultPolicy.KEYS_VALUES );
          Log.getLogWriter().info("Registered interest in KeyList " + evenKeys);
       } else if (clientInterest.equalsIgnoreCase("oddKeys")) {
          aRegion.registerInterest( oddKeys, InterestResultPolicy.KEYS_VALUES );
          Log.getLogWriter().info("Registered interest in KeyList " + oddKeys);
       } else if (clientInterest.equalsIgnoreCase("noInterest")) {
          aRegion.registerInterest( "None", InterestResultPolicy.NONE );
          Log.getLogWriter().info("Not registering interest in any keys");
       } else {
         throw new TestException("Invalid clientInterest " + clientInterest);
       }
    } catch (CacheWriterException e) {
       throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  /**
   *  A Hydra TASK that will kill and restart random bridgeServers.
   */
  public static void HydraTask_recycleServer()
  throws ClientVmNotFoundException {
    PRObserver.initialize();
    List endpoints = BridgeHelper.getEndpoints();
    int i = rnd.nextInt(endpoints.size());
    // Kill the next server
    BridgeHelper.Endpoint endpoint = (BridgeHelper.Endpoint)endpoints.get(i);
    ClientVmInfo target = new ClientVmInfo(endpoint);
    ClientVmMgr.stop("HydraTask_recycleServer: " + endpoint,
                      ClientVmMgr.NICE_EXIT, ClientVmMgr.ON_DEMAND, target);
    ClientVmMgr.start("starting " + target, target);

    Object value = BridgeNotifyBB.getBB().getSharedMap().get("expectRecovery");
    if (value instanceof Boolean) {
       if (((Boolean)value).booleanValue()) {
          PRObserver.waitForRebalRecov(target, 1, 1, null, null, false);
       }
    }
  }
}
