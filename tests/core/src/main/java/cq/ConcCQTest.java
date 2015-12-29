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
 * This is to test the CQ operations happening concurrently with region
 * operations.
 * 
 */

package cq;

import objects.ObjectHelper;
import objects.Portfolio;
import mapregion.MapBB;
import mapregion.MapPrms;
import mapregion.MapRegionTest;
import util.PRObserver;
import util.TestException;
import util.TestHelper;
import hct.HctPrms;
import hydra.*;
import hydra.blackboard.*;
import cq.EntryEventListener;

import java.util.Collections;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifier;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;

import dunit.DistributedTestCase;
import dunit.DistributedTestCase.WaitCriterion;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConcCQTest extends MapRegionTest
{

  private static String bridgeRegionName;

  private static String edgeRegionName;

  protected static QueryService cqService;

  private static final int MAX_PUT = 50;

  private static long killInterval = TestConfig.tab().longAt(
      HctPrms.killInterval);

  private static CQUtilBB cqBB = CQUtilBB.getBB();

  private static GsRandom rand = new GsRandom();

  protected static ConcCQTest testInstance;
  
  protected static boolean waitForEventsAlreadyPerformed = false;
  
  protected static boolean cqsOn = true;
  
  private static Class valueConstraint = null;

  // initializes server cache
  public static void initServerRegion()
  {
    PRObserver.installObserverHook();
    // create cache
    CacheHelper.createCache(ConfigPrms.getCacheConfig());
    // create region
    Region reg = RegionHelper.createRegion(ConfigPrms.getBridgeConfig());
    bridgeRegionName = reg.getName();
    Log.getLogWriter().info("created cache and region in bridge");
    // start the bridge server
    BridgeHelper.startBridgeServer(ConfigPrms.getBridgeConfig());
    Log.getLogWriter().info("started bridge server");
  }

  // initializes server cache with multiple regions
  public static void initServerWithMultRegions()
  {
    PRObserver.installObserverHook();
    cqsOn = TestConfig.tab().booleanAt(CQUtilPrms.CQsOn, true); //default CQsOn is true
    
    // create cache
    CacheHelper.createCache(ConfigPrms.getCacheConfig());
    RegionAttributes attributes = RegionHelper.getRegionAttributes(ConfigPrms
        .getRegionConfig());
    CacheListener[] assignedListeners = attributes.getCacheListeners();
    
    AttributesFactory factory= RegionHelper.getAttributesFactory(ConfigPrms
        .getRegionConfig());
    
    //set valueConstraint
    String valueConst = TestConfig.tab().stringAt(CQUtilPrms.valueConstraint, null);
    if (valueConst != null) {
      try {
        valueConstraint = Class.forName(valueConst);
     } catch (ClassNotFoundException e) {
        throw new TestException("Could not find specified class: " + valueConst + "\n" + TestHelper.getStackTrace(e));
     }
     factory.setValueConstraint(valueConstraint);
    }    
    
    //create regions
    String[] regionNames = MapPrms.getRegionNames();
    for (int i = 0; i < regionNames.length; i++) {
     RegionHelper.createRegion(regionNames[i], factory);            
    }
    
    Region reg = RegionHelper.getRegion(regionNames[0]);
    Scope scope = reg.getAttributes().getScope();
    CQUtilBB.getBB().getSharedMap().put(CQUtilBB.Scope, scope);
    
    for (int i = 0; i < regionNames.length; i++) {
      Region aRegion = RegionHelper.getRegion(regionNames[i]);
      EntryEventListener eeListener = new EntryEventListener();

      if (!cqsOn && aRegion !=null) {
        for (int j=0; j<assignedListeners.length; j++) {
          aRegion.getAttributesMutator().removeCacheListener(assignedListeners[j]);
        }
        
        aRegion.getAttributesMutator().addCacheListener(eeListener);
      } 
      
    } // to check entry event, so remove the BridgeEventListner and add EntryEventListener
        
    Log.getLogWriter().info("created cache and region in bridge");
    // start the bridge server
    BridgeHelper.startBridgeServer(ConfigPrms.getBridgeConfig());
    Log.getLogWriter().info("started bridge server");
  }  

  /**
   * Wait for all HA Queues to be drained. The method waits for all the HA
   * Queues corresponding to all client proxies. It looks at Queue sizes after
   * every 5 seconds interval. Because of Bug 47390, if the queue size did not
   * change after 5 seconds, it assumes that all the events have been dispatched
   * for that queue. It also has an overall timeout of 2 minutes.
   */
  public static void waitForServerHAQueuesToDrain() {
    Map<ClientProxyMembershipID, Integer> proxyToQueueSizeMap = new HashMap<ClientProxyMembershipID, Integer>();
    Log.getLogWriter().info(
        "Making use of Queue Size statistics on server"
            + " side to wait for CQ events getting drained");
    final CacheClientNotifier ccnInstance = CacheClientNotifier.getInstance();
    List<CacheClientProxy> clientProxyList = new ArrayList<CacheClientProxy>();
    clientProxyList.addAll(ccnInstance.getClientProxies());
    Log.getLogWriter().info(
        "Server has " + clientProxyList.size() + " client proxies");
    boolean timeout = false;
    long start = NanoTimer.getTime();
    while (clientProxyList.size() != 0) {
      Set<CacheClientProxy> proxySet = new HashSet<CacheClientProxy>();
      proxySet.addAll(clientProxyList);
      for (CacheClientProxy clientProxy : proxySet) {
        int size = clientProxy.getQueueSizeStat();
        if (size == 0) {
          clientProxyList.remove(clientProxy);
          Log.getLogWriter().info(
              "Queue size reached zero for ClientProxy:: "
                  + clientProxy.getProxyID());
        }
        else if (proxyToQueueSizeMap.get(clientProxy.getProxyID()) != null) {
          // Because of Bug 47390, the size may not change even if all the
          // events
          // have been dispatched and we have received ACK for all of them.
          // This is because the event removal from HA Queue is done only after
          // there is dispatch called on some other events.
          // So for last few events, they never get removed because dispatch
          // never
          // gets called.
          int oldSize = proxyToQueueSizeMap.get(clientProxy.getProxyID());
          if (oldSize == size) {
            // The queue size did not change for this clientProxy within last 5
            // seconds.
            clientProxyList.remove(clientProxy);
            Log.getLogWriter().warning(
                "Size of the HA Queue for client proxy did not reach zero,"
                    + " instead it remained constant at: " + oldSize
                    + " for a 5 second period... this could be because"
                    + " of Bug 47390. Continuing with the test...");
          } else {
            Log.getLogWriter().info("For client proxy, " + clientProxy.getProxyID() + " HA queue size reduced from  " + oldSize +" to " + size);
            // Store size for next iteration
            proxyToQueueSizeMap.put(clientProxy.getProxyID(), size);
          }
        }
        else {
          // First timer.
          proxyToQueueSizeMap.put(clientProxy.getProxyID(), size);
        }
      } // for loop ends

      // See if 2 minutes have completed after we started checking queue sizes.
      // If yes, break and declare timeout, the test fails.
      long two_minutes = 2 * 60 * (NanoTimer.NANOS_PER_MILLISECOND * 1000);
      if (NanoTimer.getTime() - start > two_minutes) {
        timeout = true;
        break;
      }

      // Sleep for 5 seconds before next enquiry into queueSizes
      try {
        Log.getLogWriter().info(
            "Sleeping for 5 seconds to allow "
                + "queues to drain for all client proxies");
        Thread.sleep(5 * 1000);
      }
      catch (InterruptedException e) {
        Log.getLogWriter().warning("Caught interrupted exception" + e);
      }
    }// outer while loop ends

    if (timeout) {
      throw new TestException(
          "Wait for all HA Queue sizes to zero or a constant value timedout. "
              + "It took more than 2 minutes but queue size was still changing");
    }
  }

  // perform querying operation on region
  public static void performQuery()
  {
    try {
      QueryService qs = CacheHelper.getCache().getQueryService();
      int id = (int)MapBB.getBB().getSharedCounters().read(MapBB.NUM_PUT);
      String queryStr = "select distinct * from "
          + RegionHelper.getRegion(edgeRegionName).getFullPath()
          + " where ID > " + Integer.toString(-1);
      Query query = qs.newQuery(queryStr);
      Object results = query.execute();
      Log.getLogWriter().info("executed query " + queryStr);

      if (results instanceof SelectResults) {
        int size = ((SelectResults)results).size();
        if (size != id) {
          Log.getLogWriter().info(
              "Result set should have been " + id + " but is " + size);
        }
      }
      else {
        Log.getLogWriter().info("Result set not instanceof SelectResults");
      }
    }
    catch (Exception e) {
      throw new TestException("Caught exception during query execution"
          + TestHelper.getStackTrace(e));
    }
  }

  // monitor server region
  public static void monitorServerRegion()
  {
    Region reg = RegionHelper.getRegion(bridgeRegionName);
    Log.getLogWriter().info(
        "The size of the region on server is: " + reg.size());
  }

  // monitor server regions (more than one region)
  public static void monitorServerRegions()
  {
    String[] regionNames = MapPrms.getRegionNames();
    Region reg = null;
    for (int i = 0; i < regionNames.length; i++) {
      reg = RegionHelper.getRegion(regionNames[i]);
      if (reg != null) {
        Log.getLogWriter().info("The size of the region- " + regionNames[i] + 
            " on server is: " + reg.size());
      }
    }
  }

  // close cache and disconnect from ds
  public static void Close_Task()
  {
    if (!waitForEventsAlreadyPerformed) {
      waitForEvents();
    }
    Log.getLogWriter().info("Printing Counters");
    MapBB.getBB().print();
    Log.getLogWriter().info("closing cache");
    CacheHelper.closeCache();
  }

  /**
   * This task to kill the server on periodically
   * 
   */
  public static synchronized void killServer()  
  {
    PRObserver.initialize();
    try {
      long now = System.currentTimeMillis();
      Long lastKill = (Long)cqBB.getSharedMap().get("lastKillTime");
      long diff = now - lastKill.longValue();
      if (diff < killInterval) {
        Log.getLogWriter().info("No kill executed");
        return;
      }
      else {
        cqBB.getSharedMap().put("lastKillTime", new Long(now));
      }
  
      BridgeHelper.Endpoint endpointToKill = getNextServerToKill();
      Log.getLogWriter().info("killing:- " + endpointToKill.toString());
  
      int index = rand.nextInt(0, 1);
      boolean killServer = (index == 0) ? false : true;
  
      ClientVmInfo target = new ClientVmInfo(endpointToKill);
      if (killServer) {
        ClientVmMgr.stop("Killing cache server", ClientVmMgr.MEAN_KILL,
            ClientVmMgr.ON_DEMAND, target);
      }
      else {
        ClientVmMgr.stop("Stopping cache server", ClientVmMgr.NICE_EXIT,
            ClientVmMgr.ON_DEMAND, target);
      }
      ClientVmMgr.start("Test is restarting server", target); 
      Object value = ConcCQBB.getBB().getSharedMap().get("expectRecovery");
      if (value instanceof Boolean) {
         if (((Boolean)value).booleanValue()) {
            int numPRs = ((Integer)(ConcCQBB.getBB().getSharedMap().get("numPRs"))).intValue();
            PRObserver.waitForRebalRecov(target, 1, numPRs, null, null, false);
         }
      }
    } catch (ClientVmNotFoundException e) {
      Log.getLogWriter().info("Caught expected ClientVmNotFoundException, continuing test");
    }
  }

  /**
   * A STARTTASK that initializes the blackboard to keep track of the last time
   * something was killed.
   * 
   * @return Whether or not this task initialized the blackboard
   */
  public static boolean initBlackboard()
  {
    cqBB.getSharedMap().put("lastKillTime", new Long(0));
    Long val = (Long)cqBB.getSharedMap().get("lastKillTime");
    if (val != null && val.longValue() == 0)
      return true;
    return false;
  }

  static List endpoints;

  static volatile int endpoint_cntr = 0;

  private static BridgeHelper.Endpoint getNextServerToKill()
  {
    BridgeHelper.Endpoint endpoint;
    if (endpoints == null) {
      endpoints = BridgeHelper.getEndpoints();
    }
    endpoint = (BridgeHelper.Endpoint)endpoints.get(endpoint_cntr);
    endpoint_cntr++;
    if (endpoint_cntr >= endpoints.size()) {
      endpoint_cntr = 0;
    }
    Log.getLogWriter().info(
        "returning endpoint " + endpoint.toString() + " to kill");
    return endpoint;
  }

  /**
   * TASK that can be invoked by a bridgeClient thread to execute random CQ
   * operations.
   * 
   */
  public static void doCQOperations()
  {
    testInstance.performCQOperations();
  }

  protected static final int EXCEUTE_CQ = 1;

  protected static final int EXCEUTE_CQ_WITH_INITIAL_RESULTS = 2;

  protected static final int STOP_CQ = 3;

  protected static final int CLOSE_CQ = 4;
  
  protected static final int DO_NOTHING = 99;

  protected static final int[] cqOperations = { EXCEUTE_CQ,
      EXCEUTE_CQ_WITH_INITIAL_RESULTS, STOP_CQ, CLOSE_CQ };
  
  protected static List cqList = Collections.synchronizedList(new ArrayList());
  
  protected CqQuery getCQForOp () 
  {
    long startTime = System.currentTimeMillis();
    CqQuery cq = null;
    cq = getNextRandonCQ();
    while (cqList.contains(cq)
        && (System.currentTimeMillis() -startTime < 10000)) {
      cq = getNextRandonCQ();
    }
    synchronized (cqList) {
      if (!cqList.contains(cq)) {
        cqList.add(cq);
        return cq;
      }
      else {
        return null;
      }
    }    
  }
  
  protected CqQuery getNextRandonCQ () 
  {
    CqQuery cq = null;
    int randInt;
    CqQuery cqArray[] = cqService.getCqs();
    randInt = TestConfig.tab().getRandGen().nextInt(0, cqArray.length - 1);
    cq = cqArray[randInt];
    return cq;
  }
  

  protected void performCQOperations()
  {
    CqQuery cq = null;
    int randInt = 0, whichOp = 0;
    cq = getCQForOp();
    if (cq != null) {
      randInt = TestConfig.tab().getRandGen().nextInt(0,
          cqOperations.length - 1);
      whichOp = cqOperations[randInt];
    }
    else {
      whichOp = DO_NOTHING;
    }
    switch (whichOp) {
    case (EXCEUTE_CQ):
      Log.getLogWriter().info("executing:- " + cq.getName());
      executeCQ(cq);
      break;
    case (EXCEUTE_CQ_WITH_INITIAL_RESULTS):
      /*
       * Prafulla:- this is temorary change made to isolate the problems causing
       * because of executeWithInitialResults
       */
      if (MapPrms.getDoExecuteWithInitialResultsCQ()) {
        Log.getLogWriter().info(
            "executing with initial results:- " + cq.getName());
        executeWithInitialResultsCQ(cq);
      }
      else {
        Log.getLogWriter().info(
            "NOT doing executeWithInitial results:- " + cq.getName());
      }
      break;
    case (STOP_CQ):
      Log.getLogWriter().info("stoping cq:- " + cq.getName());
      stopCQ(cq);
      break;
    case (CLOSE_CQ):
      Log.getLogWriter().info("closing cq:- " + cq.getName());
      closeCQ(cq);
      break;
    case (DO_NOTHING):
      Log.getLogWriter().info(
          "The thread could not find any CQ for operation within 10 seconds. cq:-"
              + cq);
      break;
    default:
      throw new TestException("Unrecognized cqOperation (" + randInt + ")");
    }

    synchronized (cqList) {
      cqList.remove(cq);
    }

  }

  protected void closeCQ(CqQuery cq)
  {
    try {
      if (cq.isRunning() || cq.isStopped()) {
        long startTime = System.currentTimeMillis();
        cq.close();
        long endTime = System.currentTimeMillis();
        Log.getLogWriter().info("closed cq:- " + cq.getName());
        checkTime(startTime, endTime, 20000, "close");
      }

      if (cq.isClosed()) {
        try {
          cq.close();
          Log.getLogWriter().info(
              "CQ:- " + cq.getName()
                  + " is closed hence registering it before execution");
          reRegisterCQ(cq);
        }
        catch (CqClosedException cle) {
          throw new TestException(
          "Should not have thrown CQClosedException. close() on CLOSED query is not successful");
        }
      }
    }
    catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  protected void executeCQ(CqQuery cq)
  {
    try {
      if (cq.isStopped()) {
        long startTime = System.currentTimeMillis();
        cq.execute();
        long endTime = System.currentTimeMillis();
        Log.getLogWriter().info("executed query:- " + cq.getName());
        checkTime(startTime, endTime, 40000, "execute");
      }
      else if (cq.isRunning()) {
        try {
          cq.execute();
          throw new TestException(
              "Should have thrown IllegalStateException. Execute on RUNNING query is successful");
        }
        catch (IllegalStateException expected) {
          // expected
        }
      }
      
      if (cq.isClosed()) {
        try {
          cq.execute();
          throw new TestException(
              "Should have thrown CQClosedException. execute() on CLOSED query is successful");
        }
        catch (CqClosedException cql) {
          Log.getLogWriter().info(
              "CQ:- " + cq.getName()
                  + " is closed hence registering it before execution");
          reRegisterCQ(cq);
        }
      }
    }
    catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  protected void executeWithInitialResultsCQ(CqQuery cq)
  {
    try {
      if (cq.isStopped()) {
        long startTime = System.currentTimeMillis();
        //SelectResults results = cq.executeWithInitialResults();
        CqResults rs = cq.executeWithInitialResults();
        SelectResults results = CQUtil.getSelectResults(rs);
        long endTime = System.currentTimeMillis();
        Log.getLogWriter().info(
            "executed query:- " + cq.getName() + " with initial results");
        /*
         * TODO: Prafulla and Anil
         * Need to add the validations for SelectResults
         */
        checkTime(startTime, endTime, 60000, "executeWithInitialResults");
      }
      else if (cq.isRunning()) {
        try {
          cq.execute();
          throw new TestException(
              "Should have thrown IllegalStateException. executeWithInitialResults on RUNNING query is successful");
        }
        catch (IllegalStateException expected) {
          // expected
        }
      }
      if (cq.isClosed()) {
        try {
          cq.executeWithInitialResults();
          throw new TestException(
              "Should have thrown CQClosedException. executeWithInitialResults() on CLOSED query is succussful");
        }
        catch (CqClosedException cle) {
          Log
              .getLogWriter()
              .info(
                  "CQ:- "
                      + cq.getName()
                      + " is closed hence registering it before executeWithInitialResults");
          reRegisterCQ(cq);
        }
      }
    }
    catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  protected void stopCQ(CqQuery cq)
  {
    try {
      if (cq.isRunning()) {
        long startTime = System.currentTimeMillis();
        cq.stop();
        long endTime = System.currentTimeMillis();
        Log.getLogWriter().info("stopped CQ:- " + cq.getName());
        checkTime(startTime, endTime, 20000, "stop");
      }
      else if (cq.isStopped()) {
        try {
          cq.stop();
          throw new TestException(
              "should have thrown IllegalStateException. executed stop() successfully on STOPPED CQ");
        }
        catch (IllegalStateException expected) {
          // expected
        }
      }
      
      if (cq.isClosed()) {
        try {
          cq.stop();
          throw new TestException(
              "should have thrown CQClosedException. executed stop() successfully on CLOSED CQ");
        }
        catch (CqClosedException cle) {
          // expected
          reRegisterCQ(cq);
        }
      }
    }
    catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  protected void registerCQs()
  {
    try {
      String queryArr[] = MapPrms.getQueryStrs();
      CqAttributesFactory factory = new CqAttributesFactory();
      factory.addCqListener(CQUtilPrms.getCQListener());
      CqAttributes cqAttrs = factory.create();
      CqQuery cqs[] = new CqQuery[queryArr.length];
      for (int i = 0; i < queryArr.length; i++) {
        cqs[i] = cqService.newCq(queryArr[i], cqAttrs);
      }
      StringBuffer aStr = new StringBuffer("Registered CQs (size " + cqs.length
          + ") = \n");
      for (int i = 0; i < cqs.length; i++) {
        Object o = cqs[i];
        aStr.append(o.toString() + "\n");
      }
      Log.getLogWriter().info(aStr.toString());
      Log.getLogWriter().info("Done with registering CQs");
    }
    catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  protected void registerAndExecuteCQs()
  {
    try {
      String queryArr[] = MapPrms.getQueryStrs();
      CqAttributesFactory factory = new CqAttributesFactory();
      factory.addCqListener(CQUtilPrms.getCQListener());
      CqAttributes cqAttrs = factory.create();
      CqQuery cqs[] = new CqQuery[queryArr.length];
      for (int i = 0; i < queryArr.length; i++) {
        cqs[i] = cqService.newCq(queryArr[i], cqAttrs);
        cqs[i].execute();
      }
      StringBuffer aStr = new StringBuffer("Registered and executed CQs (size "
          + cqs.length + ") = \n");
      for (int i = 0; i < cqs.length; i++) {
        Object o = cqs[i];
        aStr.append(o.toString() + "\n");
      }
      Log.getLogWriter().info(aStr.toString());
      Log.getLogWriter().info(
          "Done with registering and started executions of CQs");
    }
    catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }

  protected void reRegisterCQ(CqQuery cq)
  {
    try {      
      Log.getLogWriter().info("re-registering CQ:- " + cq.getName());
      String query = cq.getQueryString();
      CqAttributes attr = cq.getCqAttributes();
      cqService.newCq(query, attr);
      Log.getLogWriter().info("CQ re-registered:- " + cq.getName());      
    }
    catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
    }
  }
  
  
  public static void verifyCQListener() {
    cqsOn = TestConfig.tab().booleanAt(CQUtilPrms.CQsOn, true); //default CQsOn is true
    if (cqsOn) {
      testInstance.verifyEvents();
    }
    else {
      testInstance.verifyBridgeCacheEvent(); //to seperate issues to see if they are not CQ related
    }
  } 
  
  /**
   * to check if cache Listners are correctly invoked without register any CQs. (for bug 37897) 
   *
   */
  protected void verifyBridgeCacheEvent() {
    waitForEvents();
    
    Blackboard mapBB = MapBB.getBB();
    Blackboard eeBB = EntryEventBB.getBB();
    
    int numPuts = (int)mapBB.getSharedCounters().read(MapBB.NUM_PUT);
    int numDestroys = (int)mapBB.getSharedCounters().read(
        MapBB.NUM_REMOVE);
    long tempInvalidates = mapBB.getSharedCounters().read(
        MapBB.NUM_INVALIDATE);
    int numInvalidates = (int) Math.abs(tempInvalidates);
    int numCloses = (int)mapBB.getSharedCounters().read(MapBB.NUM_CLOSE);
    
    int afterCreates = (int)eeBB.getSharedCounters().read(
        EntryEventBB.NUM_CREATE);
    int afterDestroys = (int)eeBB.getSharedCounters().read(
        EntryEventBB.NUM_DESTROY);
    int afterInvalidates = (int)eeBB.getSharedCounters().read(
        EntryEventBB.NUM_INVALIDATE);
    int afterUpdates = (int)eeBB.getSharedCounters().read(
        EntryEventBB.NUM_UPDATE);
    int afterCloses = (int)eeBB.getSharedCounters().read(EntryEventBB.NUM_CLOSE);

    int numListeners = TestConfig.tab().intAt(CQUtilPrms.numBridges);
    
    verifyFromBBs(numPuts, numDestroys,numInvalidates, numCloses, 
        afterCreates, afterDestroys, afterInvalidates, afterUpdates, afterCloses, numListeners);
  }
  
  // to check if any listnerers are missing any event each entry operation (from MapBB)
  protected void verifyFromBBs(int numPuts, int numDestroys,int numInvalidates, int numCloses, 
      int afterCreates, int afterDestroys, int afterInvalidates, int afterUpdates, 
      int afterCloses, int numListeners){
    
    Scope scope = (Scope)CQUtilBB.getBB().getSharedMap().get(CQUtilBB.Scope);   
    boolean isDack = false;
    if (scope.isDistributedAck()) isDack=true;
    //if scope is d_ack, the total ordering is not guaranteed in the distributed system.
    
    StringBuffer errStr = new StringBuffer();
    
    Log.getLogWriter().info("numPuts:- "+numPuts);
    Log.getLogWriter().info("numDestroys:- "+numDestroys);
    Log.getLogWriter().info("numInvalidates:- "+numInvalidates);
    Log.getLogWriter().info("numCloses: - "+numCloses);
    
    Log.getLogWriter().info("afterCreates:- "+afterCreates);
    Log.getLogWriter().info("afterUpdates:- "+afterUpdates);
    Log.getLogWriter().info("afterInvalidates:- "+afterInvalidates);
    Log.getLogWriter().info("afterDestroys:- "+afterDestroys);
    Log.getLogWriter().info("afterCloses:- "+afterCloses);
    Log.getLogWriter().info("numListeners:- "+numListeners);

    try {      
      checkNumberOfEvents(numPuts, ((afterCreates + afterUpdates) / numListeners), "numPuts", "put",
          "afterCreates");
    }
    catch (TestException ex) { 
      errStr.append(ex.getMessage() + "\n");
    }

    try {
      if (!isDack) {
        checkNumberOfEvents(numDestroys, afterDestroys / numListeners, "numDestroys",
            "destroy", "afterDestroys");
      }
      else {
        checkDackEvents(numDestroys, afterDestroys / numListeners, "numDestroys",
            "destroy", "afterDestroys");          
      }
    }
    catch (TestException ex) {
      errStr.append(ex.getMessage() + "\n");
    }
    
    try {
      checkNumberOfEvents(numCloses, afterCloses / numListeners,
            "numCloses", "close", "afterCloses");
    } catch (TestException ex) {
      errStr.append(ex.getMessage() + "\n");
    } //destroyRegion will invoke close()

    try {
      if (!isDack) {
        checkNumberOfEvents(numInvalidates, afterInvalidates / numListeners,
              "numInvalidates", "invalidate", "afterInvalidates");
      }
      else {
        checkDackEvents(numInvalidates, afterInvalidates / numListeners,
            "numInvalidates", "invalidate", "afterInvalidates");
      }
    }
    catch (TestException ex) {      
      errStr.append(ex.getMessage() + "\n");
    }

    if (errStr.length() > 0) {
      throw new TestException(errStr.toString());
    }
  }
  
  protected void verifyWithRegionOpsFromBB(int numPuts, int numDestroys, int numInvalidates, 
      int numCloses, int numClearRegions, int numInvalidateRegions, int afterCreates, 
      int afterDestroys, int afterInvalidates, int afterUpdates, int afterCloses, 
      int afterClearRegions, int afterInvalidateRegions, int numListeners) {
    StringBuffer errStr = new StringBuffer();
    
    Log.getLogWriter().info("numClearRegions:- "+numClearRegions);
    Log.getLogWriter().info("numInvalidateRegions:- "+numInvalidateRegions);
    Log.getLogWriter().info("afterClearRegions:- "+afterClearRegions);
    Log.getLogWriter().info("afterInvalidateRegions:- "+afterInvalidateRegions);
    try {
      verifyFromBBs(numPuts, numDestroys,numInvalidates, numCloses, 
          afterCreates, afterDestroys, afterInvalidates, afterUpdates, afterCloses, numListeners);
    } catch (TestException ex) {
      errStr.append(ex.getMessage() + "\n");
    } //check for entry events mismatches
    
    //check for clearRegion event mismatch (region ops)
    try {      
      checkNumberOfEvents(numClearRegions, afterClearRegions / numListeners, "numClearRegions", 
          "clearRegions", "afterClearRegions");
    }
    catch (TestException ex) { 
      errStr.append(ex.getMessage() + "\n");
    }
    
    //check for invalidateRegion event mismatch (region ops)
    try {      
      checkNumberOfEvents(numInvalidateRegions, afterInvalidateRegions / numListeners, 
          "numInvalidateRegions", "InvalidateRegion", "afterInvalidateRegions");
    }
    catch (TestException ex) { 
      errStr.append(ex.getMessage() + "\n");
    }
    
    if (errStr.length() > 0) {
      throw new TestException(errStr.toString());
    }    
  }

  /**
   * verify if cq listeners miss any cq events by comparing logged expected cq
   * events from MapBB and cq events received from CQUtilBB
   */
  protected void verifyEvents() {
    waitForEvents();
    Blackboard mapBB = MapBB.getBB();
    
    int numPuts = (int)mapBB.getSharedCounters().read(MapBB.NUM_PUT);
    int numDestroys = (int)mapBB.getSharedCounters().read(
        MapBB.NUM_REMOVE);
    long tempInvalidates = mapBB.getSharedCounters().read(
        MapBB.NUM_INVALIDATE);
    int numInvalidates = (int) Math.abs(tempInvalidates);
    int numCloses = (int)mapBB.getSharedCounters().read(MapBB.NUM_CLOSE);
    int numClearRegions = (int)mapBB.getSharedCounters().read(MapBB.NUM_REGION_DESTROY);
    int numInvalidateRegions = (int)mapBB.getSharedCounters().read(MapBB.NUM_REGION_INVALIDATE);
    int afterCreates = (int)cqBB.getSharedCounters().read(
        CQUtilBB.NUM_CREATE);
    int afterDestroys = (int)cqBB.getSharedCounters().read(
        CQUtilBB.NUM_DESTROY);
    int afterInvalidates = (int)cqBB.getSharedCounters().read(
        CQUtilBB.NUM_INVALIDATE);
    int afterUpdates = (int)cqBB.getSharedCounters().read(
        CQUtilBB.NUM_UPDATE);
    int afterCloses = (int)cqBB.getSharedCounters().read(CQUtilBB.NUM_CLOSE);
    int afterClearRegions = (int)cqBB.getSharedCounters().read(CQUtilBB.NUM_CLEARREGION);
    int afterInvalidateRegions = (int)cqBB.getSharedCounters().read(CQUtilBB.NUM_INVALIDATEREGION);

    int numListeners = MapPrms.getNumEdges();
    
    if (numClearRegions !=0 || numInvalidateRegions !=0 ) {
      verifyWithRegionOpsFromBB(numPuts, numDestroys,numInvalidates, numCloses, numClearRegions,
          numInvalidateRegions, afterCreates, afterDestroys, afterInvalidates, afterUpdates, 
          afterCloses, afterClearRegions, afterInvalidateRegions, numListeners);
    }
    else verifyFromBBs(numPuts, numDestroys,numInvalidates, numCloses, 
        afterCreates, afterDestroys, afterInvalidates, afterUpdates, afterCloses, numListeners);
  }

  public static void checkNumberOfEvents(int expected, int actual,
      String cntrName, String op, String listenerEvent)
  {
    if (expected != actual) {
      throw new TestException(cntrName + "(incremented after every " + op
          + ") (= " + expected + " ) are not equal " + listenerEvent
          + " (CQListener) ( = " + actual + " )");
    }
  }
  
  //check for events when the scope is distributed ack.
  //will allow for a few misses as total ordering of events deliver is not guaranteed
  //for replicated dack(bug 38074, which is not a bug). CQ events verification needs to
  // consider this as well.
  public static void checkDackEvents(int expected, int actual,
      String cntrName, String op, String listenerEvent) {
    
    int allowedMisses = 10; 
    //we allow for a few events to be missed due to mixed ordering of events delivery
    //in replicated dack. Therefore destroy or invalidate events might be off if these
    //events are delivered to the nodes before the create events.
                
    if (expected - actual > allowedMisses) {
      throw new TestException("For replicated distributed ack, misses for  " + op +
          " events is " + (expected - actual) + " and greater than expected misses of " +
          allowedMisses + ". Need to check logs to see if there are "  +
          "other causes. \n" +          
          cntrName + "(incremented after every " + op
          + ") (= " + expected + " ) and " + listenerEvent
          + " (CQListener) ( = " + actual + " )");
    }
    else if (expected < actual) {
      throw new TestException(cntrName + "(incremented after every " + op
          + ") (= " + expected + " ) are not equal " + listenerEvent
          + " (CQListener) ( = " + actual + " )");
    }
  }
  
  public static void checkTime(long startTime, long endTime, long limit,
      String cqOperationName)
  {
    if ((endTime - startTime) > limit) {
      Log.getLogWriter().warning(
          "WARNING:- CQ operation:- " + cqOperationName + " took more than "
              + limit / 1000 + " seconds. Actual time taken is:- "
              + (endTime - startTime) / 1000 + " seconds.");
    }
  }
   
  public static void waitForEvents()
  {
    long limit = MapPrms.getTimeToWaitForEvents();
    
    Long lastEventReceived = (Long)cqBB.getSharedMap().get("lastEventReceived");
    if(lastEventReceived == null ) {
      cqBB.getSharedMap().put("lastEventReceived", new Long(0));      
      Log.getLogWriter().info("Initialized lastEventReceived in waitForEvents");
    }

    while (System.currentTimeMillis()
        - (((Long)cqBB.getSharedMap().get("lastEventReceived")).longValue()) < limit) {
      try {
        Thread.sleep(5000);
      } catch (InterruptedException ign) {
        
      }
    }
    
    waitForEventsAlreadyPerformed = true;

    Log
        .getLogWriter()
        .info(
            "Waited for "
                + limit
                / 1000
                + " seconds for events to arrive. Since no events seen during that time test is proceeding for validations");
  }
  
}
