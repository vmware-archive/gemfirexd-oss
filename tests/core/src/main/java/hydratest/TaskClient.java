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

package hydratest;

import hydra.*;
import hydra.blackboard.AnyCyclicBarrier;
import hydra.blackboard.SharedLock;
import java.util.*;

/**
 *  Example tasks for testing hydra.
 */

public class TaskClient {

  public static void tryItOut() { 
  }
  public static void tryItOut1() { 
  }
  public static void tryItOut2() { 
  }
  public static void tryItOut3() { 
  }
  public static void tryItOut4() { 
  }
  public static void tryItOut4WithHeapDump() { 
    String clientName = RemoteTestModule.getMyClientName();
    HostDescription hd = TestConfig.getInstance()
      .getClientDescription(clientName).getVmDescription().getHostDescription();
    ProcessMgr.dumpHeap(hd.getHostName(), RemoteTestModule.getMyPid(),
      hd.getUserDir(), TestConfig.tab().stringAt(Prms.jmapHeapDumpOptions, null));
    throw new StopSchedulingTaskOnClientOrder();
  }
  public static void tryItOut5() { 
  }
  public static void tryItOut6() { 
  }
  public static void tryItOutRobinG() { 
    Log.getLogWriter().info(
        "Got " + TestConfig.tab().intAt( hydratest.HydraTestPrms.prm01 ) );
  }
  public static void tryItOutStopSchedulingOrder() { 
    throw new StopSchedulingOrder( "arbitrary decision by client" );
  }
  public static void tryItOutStopSchedulingTaskOnClientOrder() { 
    String s = "this client is done with this task";
    throw new StopSchedulingTaskOnClientOrder( s );
  }
  public static void tryItOutAttributes() { 
    String name =
      TestConfig.tasktab().stringAt( hydratest.TaskAttributes.name, "nobody" );
    Log.getLogWriter().info( "Hi, my name is " + name );
  }
  public static void tryItOutAttributesOneOf() { 
    String name =
      TestConfig.tasktab().stringAt( hydratest.TaskAttributes.names, "nobody" );
    Log.getLogWriter().info( "Hi, my name is " + name );
  }
  public static void tryItOutException() { 
       throw new HydraRuntimeException( "yo" );
  }
  public static void tryItOutHydraTimeoutException() { 
       throw new HydraTimeoutException( "yo" );
  }
  public static void tryItOutAnotherException() { 
    // throws ArrayIndexOutOfBoundsException
    Vector myVec = new Vector();
    myVec.add("yo");
    myVec.elementAt(2);
  }
  public static void tryItOutSleep() { 
    MasterController.sleepForMs( 500000 );
  }
  public static void tryItOutCompute() { 
    Vector v = new Vector();
    double d = 542.9;
    for ( int i = 0; i < 50000; i++ ) {
      double a = d/(double)i;
      v.add( 0, new Double( a ) );
    }
  }
  public static void tryItOutLock() {
    SharedLock lock = HydraTestBlackboard.getInstance().getSharedLock();
    lock.lock();
    Log.getLogWriter().info("Locked shared lock");
    MasterController.sleepForMs(250);
    Log.getLogWriter().info("Unlocking shared lock");
    lock.unlock();
  }
  public static void tryItOutLockStress() {
    SharedLock lock = HydraTestBlackboard.getInstance().getSharedLock();
    for (int i = 0; i < 100; i++) {
      lock.lock();
      lock.unlock();
    }
  }
  public static void tryItOutLockBad1() {
    SharedLock lock = HydraTestBlackboard.getInstance().getSharedLock();
    lock.lock();
    lock.unlock();
    lock.unlock();
  }
  public static void tryItOutLockBad2() {
    SharedLock lock = HydraTestBlackboard.getInstance().getSharedLock();
    lock.lock();
    lock.lock();
    lock.unlock();
  }
  public static void tryItOutNever() { 
    throw new HydraRuntimeException("Should not run");
  }
  public static void shutdownHook() {
    Log.getLogWriter().info("Shutdown hook invoked");
  }
  public static void shutdownHookError() {
    Log.getLogWriter().info("Shutdown hook invoked with error");
    throw new HydraRuntimeException("intentional");
  }
  public static void shutdownHookTimeout() {
    Log.getLogWriter().info("Shutdown hook invoked with timeout");
    MasterController.sleepForMs(10000000);
  }
  // validate checkextravmargs.conf
  public static void checkExtraVMArgs() {
    String clientName = RemoteTestModule.getMyClientName();
    String vendor = TestConfig.getInstance().getClientDescription(clientName)
                              .getVmDescription().getHostDescription()
                              .getJavaVendor();
    if (vendor.equals(HostPrms.HITACHI)) {
      checkArg(vendor, "generic", vendor);
      checkArg(vendor, "moregeneric", null);
      checkArg(vendor, "evenmoregeneric", null);
      checkArg(vendor, "specific", vendor);
      checkArg(vendor, "morespecific", vendor);
    }
    else if (vendor.equals(HostPrms.IBM)) {
      checkArg(vendor, "generic", vendor);
      checkArg(vendor, "moregeneric", vendor);
      checkArg(vendor, "evenmoregeneric", vendor);
      checkArg(vendor, "specific", vendor);
      checkArg(vendor, "morespecific", vendor);
    }
    else if (vendor.equals(HostPrms.JROCKIT)) {
      checkArg(vendor, "generic", vendor);
      checkArg(vendor, "moregeneric", null);
      checkArg(vendor, "evenmoregeneric", null);
      checkArg(vendor, "specific", vendor);
      checkArg(vendor, "morespecific", null);
    }
    else if (vendor.equals(HostPrms.SUN)) {
      checkArg(vendor, "generic", vendor);
      checkArg(vendor, "moregeneric", vendor);
      checkArg(vendor, "evenmoregeneric", null);
      checkArg(vendor, "specific", vendor + "override");
      checkArg(vendor, "morespecific", null);
    }
    else {
      String s = "Unknown java vendor: " + vendor;
      throw new HydraConfigException(s);
    }
  }
  private static void checkArg(String vendor, String arg, String expected) {
    String value = System.getProperty(arg);
    if ((expected == null && value != null)
     || (expected != null && !expected.equals(value))) {
      String s = vendor + " got  wrong value for " + arg + ", "
               + " expected " + expected + ", got " + value;
      throw new HydraRuntimeException(s);
    }
  }

  public static void blackboardTest() {
    String bbname = String.valueOf(RemoteTestModule.getMyVmid());
    HydraTestBlackboard bb = HydraTestBlackboard.getInstance(bbname);
    try {
      bb.getSharedLock().lock();
      Integer counter = (Integer)bb.getSharedMap().get("mine");
      if (counter == null) {
        bb.getSharedMap().put("mine", 1);
      } else {
        bb.getSharedMap().put("mine", counter + 1);
      }
    } finally {
      bb.getSharedLock().unlock();
    }
  }
  public static void blackboardTestResult() {
    String bbname = String.valueOf(RemoteTestModule.getMyVmid());
    HydraTestBlackboard bb = HydraTestBlackboard.getInstance(bbname);
    Log.getLogWriter().info("BB " + bbname + "=" + bb.getSharedMap().get("mine"));
  }

  public static void barrierTask() {
    int n = numThreads();
    AnyCyclicBarrier barrier = AnyCyclicBarrier.lookup(n, "barrier");
    Log.getLogWriter().info("Waiting for " + n + " to meet at barrier");
    barrier.await();
    passedBarrier();
  }
  private static void passedBarrier() {
    Log.getLogWriter().info("Passed the barrier");
  }

  /**
   *  Gets the total number of threads eligible to run the current task.
   */
  public static int numThreads() {
    TestTask task = RemoteTestModule.getCurrentThread().getCurrentTask();
    int t = task.getTotalThreads();
    return t;
  }
}
