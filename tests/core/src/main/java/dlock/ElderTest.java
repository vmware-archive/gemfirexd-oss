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

package dlock;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.Assert;

import hydra.*;
import hydra.blackboard.*;

import java.util.*;
import java.io.Serializable;

import util.*;

/**
 *
 *  Test Distributed LockingService Elder Recovery 
 *
 */

public class ElderTest {

    protected static final String ObjectToLock = "objectToLock";
    private static final String MEMBER_START_ORDER = "MemberStartOrder";
    private static final String ELDER_CLIENT_NAME = "ElderClientName";
    private static final String ELDER_ID = "ElderId";

public static void initTask() {
  DLockBlackboard bb = DLockBlackboard.getInstance();
  SharedMap aMap = bb.getSharedMap();
  aMap.put(MEMBER_START_ORDER, new LinkedList());
}

  /**
   *  InitTask to track the order in which members are started
   *  via a list stored in the blackboard.
   *  No concurrent execution expected.
   */
  public static void initElderTask() { 
    ElderTest testInstance = new ElderTest();
    testInstance.initElder();
  }
  private void initElder() {
    LinkedList startOrderList = null;

    // keep track of order in which members execute this INITTASK method
    String clientName = RemoteTestModule.getMyClientName();
    DLockBlackboard bb = DLockBlackboard.getInstance();
    startOrderList = (LinkedList)bb.getSharedMap().get(MEMBER_START_ORDER);
    startOrderList.add(clientName);
    log().info(clientName + " added to startOrderList");
    bb.getSharedMap().put(MEMBER_START_ORDER, startOrderList);
    log ().info("Client: " + clientName + " start order is: " + 
                   startOrderList.size());
    
    // first member to lock/unlock will become Elder
    DistributedLockService dls = DLockUtil.getLockService();

    boolean result = dls.lock(ObjectToLock, -1, -1);
    log().info("Client: " + clientName + " got lock: " + result);
    dls.unlock(ObjectToLock);

    DLockBlackboard.printBlackboard();
  }
 
 /*
   *  Task to disrupt the Disributed System's Elder forcing selection of a new Elder.
   *  The task verifies the oldest member become the new Elder.
   */
  public static void disruptElderTask() { 
    ElderTest testInstance = new ElderTest();
    testInstance.disruptElder();
  }
  private void disruptElder() {
    LinkedList startOrderList = null;

    String clientName = RemoteTestModule.getMyClientName();
    log().info("ClientName is: " + clientName);

    DLockBlackboard bb = DLockBlackboard.getInstance();
    hydra.blackboard.SharedCounters sc = bb.getSharedCounters();

    startOrderList = (LinkedList)bb.getSharedMap().get(MEMBER_START_ORDER);
    if (startOrderList.isEmpty()) {
      log().info("Disrupted all elders");
      throw new StopSchedulingTaskOnClientOrder();
    }
    log().info("calling verifyElder at start of disruptElder");
    verifyElder();

    
    // wait for all threads for Elders to disrupt to arrive
    int numThreads = tab().intAt(DLockPrms.numEldersToDisrupt);
    int waitLimit = -1;
    sc.zero(DLockBlackboard.DoneWithTask);
    sc.increment(DLockBlackboard.ReadyToLock);
    TestHelper.waitForCounter(bb, "DLockBlackboard.ReadyToLock", DLockBlackboard.ReadyToLock,
              numThreads, true, waitLimit);
 
    if (startOrderList.contains(clientName)) {
      //In the test run where we want to validate data after the test, we
      // don't want to disconnect all clients because that would throw off the data 
      // validation (I think).  For this case there are clients that won't be in the
      // startOrderList.

      if (clientName.equals(bb.getSharedMap().get(ELDER_CLIENT_NAME))) {
	//current client is the elder member
        log().info("disrupting elder...which is: " + clientName);

        startOrderList.removeFirst();
        DistributedConnectionMgr.disconnect();
        DistributedConnectionMgr.connect();
        // adding client to end of list only works if we want to disrupt every VM that becomes elder.
        //startOrderList.addLast(clientName);      
        //log().info(clientName + " added to end of startOrderList");
        bb.getSharedMap().put(MEMBER_START_ORDER, startOrderList);
        log ().info("Client: " + clientName + " start order is: " + 
                   startOrderList.size());

        log().info("#### requesting DistributedLockService");
        DistributedLockService dls = DLockUtil.getLockService();
        log().info("#### got DistributedLockService");

        if (dls.isLockGrantor()) {
          log().info("checking for grantor to force Elder selection");
        }

        // lock/unlock to force this member to be selected as Grantor
        log().info("#### requesting lock...");
        boolean result = dls.lock(ObjectToLock, -1, -1);
        log().info("Client: " + clientName + " got lock: " + result);

        log().info("#### requesting unlock...");
        dls.unlock(ObjectToLock);
        log().info("#### unlock done.");

       }
    }

   // wait for all threads to be done with the task
   sc.increment(DLockBlackboard.DoneWithTask);  
   TestHelper.waitForCounter(bb, "DLockBlackboard.DoneWithTask", DLockBlackboard.DoneWithTask,
              numThreads, true, waitLimit);
    sc.zero(DLockBlackboard.ReadyToLock);
log().info("Client: " + clientName + " calling identifyElder at end of disrupt");  
    identifyElder();
    try {
      Thread.sleep(1000);
    } catch (InterruptedException ex) {
      Log.getLogWriter().warning("Interrupted while sleeping", ex);
    }

  }
 
 /**
   *  Task to verify Elder per test is same Elder per GemFire
   */
  public static void verifyElderTask() { 

    ElderTest testInstance  = new ElderTest();
    testInstance.verifyElder();
  }

  private void verifyElder() {
    String testElder;  // member that has lowest start order

    SharedMap aMap = DLockBlackboard.getInstance().getSharedMap();
    DLockBlackboard bb = DLockBlackboard.getInstance();
    LinkedList memberStartOrder = null;
    memberStartOrder = (LinkedList)bb.getSharedMap().get(MEMBER_START_ORDER);

    if (memberStartOrder.isEmpty()) {
      //list shrinks as elders are disrupted.
      log().info("In verify test elders completed");
      return;
    }
    testElder = (String)(memberStartOrder.getFirst());
    log().info("Test says elder should be:  " + testElder);
    
    String elderClientName = (String)bb.getSharedMap().get(ELDER_CLIENT_NAME);
    log().info("Elder ClientName is: " + elderClientName);

    if (!testElder.equals(elderClientName)) {

      String s = "Test says elder is: " + testElder + " but elder is: " + elderClientName;
      throw new DLockTestException(s);
    }
    // Verify this member is right about who the elder is
    DistributedSystem system = getDistributedSystem();
    DM dm = ((InternalDistributedSystem)system).getDistributionManager();
    String clientName = RemoteTestModule.getMyClientName();
    Serializable whoIThinkElderIs = dm.getElderId();
    Assert.assertTrue(whoIThinkElderIs != null, "null elder");
    Serializable elderId = (Serializable)bb.getSharedMap().get(ELDER_ID);
    if (!whoIThinkElderIs.equals(elderId)) {
      String s = "Client " + clientName  + " says elder ID is: " + whoIThinkElderIs 
                  + " but elder ID is: " + elderId;
      throw new DLockTestException(s);
    }

  }


  /**
   *  Task to identify the Distributed Lock Service elder 
   *  The only way to identify the elder is to have each member
   *  check to see if it is the elder.
   */
  public static void identifyElderTask() { 

    ElderTest testInstance  = new ElderTest();
    testInstance.identifyElder();
  }

  private boolean identifyElder() {
    SharedMap map = DLockBlackboard.getInstance().getSharedMap();
   
    DistributedSystem system = getDistributedSystem();
    DM dm = ((InternalDistributedSystem)system).getDistributionManager();
    Serializable whoIThinkElderIs = dm.getElderId();
    log().info("ElderID is: " + whoIThinkElderIs);
    Assert.assertTrue(whoIThinkElderIs != null, "null elder");
    boolean elderIsMe = whoIThinkElderIs.equals(dm.getId());
    if (elderIsMe) {
         log().info("Elder identified:  " + RemoteTestModule.getMyClientName());
         log().info("ElderID is: " + whoIThinkElderIs);
         map.put(ELDER_ID, whoIThinkElderIs);
         map.put(ELDER_CLIENT_NAME, RemoteTestModule.getMyClientName());
	 return true;
    } else {
       return false;
    }
  } 


/**
   *  Task to disconnect if current member is the elder 
   */
  public static void disconnectIfElderTask() { 

    ElderTest testInstance  = new ElderTest();
    testInstance.disconnectIfElder();
  }

  private void disconnectIfElder() {
   
    if (identifyElder()) {
      log().info("Disconnecting... Elder is me: " + RemoteTestModule.getMyClientName());
      DistributedConnectionMgr.disconnect();
      DistributedConnectionMgr.connect();
    }
  } 

  /**
   *  Use  DistributedLockService API to become lock grantor.
   *  Current setting is to request becomeLockGrantor 10% of the time
   *  (trying to balance this with other tasks).
   */


 public static void becomeLockGrantorTask() { 

    ElderTest testInstance  = new ElderTest();
    testInstance.becomeLockGrantor();
 }

 private void becomeLockGrantor() {
   
   String lockServiceName = "aLockService";

   DistributedLockService dls = getDistributedLockService(lockServiceName);
   if (!dls.isLockGrantor()) {
     int transferGrantorPercent = 10;
     if ((TestConfig.tab().getRandGen().nextInt(1, 100)) <= transferGrantorPercent) {
       log().info("requesting becomeLockGrantor");
       try {
         dls.becomeLockGrantor();
       }
       catch (CancelException e) {
         log().info("Caught exception " + e);
       }
     }
   }
 }

   /**
   *  Use  DistributedLockService API to detect whether current member is
   *  the lock grantor.  Increments a count in the blackboard if member is 
   *  grantor.
   */

 public static void countGrantorsTask() { 

    ElderTest testInstance  = new ElderTest();
    testInstance.countGrantors();
 }

  private void countGrantors() {
    log().info("counting lock grantors");
    String lockServiceName = "aLockService";

    DistributedLockService dls = getDistributedLockService(lockServiceName);
    if (dls.isLockGrantor()) {
      log().info("is Lock Grantor ... incrementing count");
      DLockBlackboard bb = DLockBlackboard.getInstance();
      SharedCounters sc = bb.getSharedCounters();
      sc.increment(DLockBlackboard.NumGrantors);
    }
  }

 private DistributedLockService getDistributedLockService(String lockServiceName) {
   
   DistributedSystem dSystem = getDistributedSystem();
   if (dSystem == null)
      throw new TestException("DistributedSystem is " + dSystem);
   log().info("Getting DistributedLockService with name " + lockServiceName);
   DistributedLockService dls = DistributedLockService.getServiceNamed(lockServiceName);
   if (dls == null) {
     try {
         log().info("Creating DistributedLockService with name " + lockServiceName);
         dls = DistributedLockService.create(lockServiceName, dSystem);
         Log.getLogWriter().info("Created " + dls);
      } catch (IllegalArgumentException ex) {
         Log.getLogWriter().info("Got expected Exception: Caught while creating dls " + ex);
         dls = DistributedLockService.getServiceNamed(lockServiceName);
      }
   }
   return dls;
  } 

 private DistributedSystem getDistributedSystem() {
   if (DistributedConnectionMgr.isConnected()) {
     return DistributedConnectionMgr.getConnection();
   }
   else {
     return DistributedConnectionMgr.connect();
   }
 }

 protected static LogWriter log() {
  return Log.getLogWriter();
 }
 protected static ConfigHashtable tab() {
   return TestConfig.tab();
 }
}
