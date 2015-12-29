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
package memscale;

import hydra.Log;
import hydra.RemoteTestModule;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import util.TestException;

import com.gemstone.gemfire.internal.offheap.SimpleMemoryAllocatorImpl;

/**
 * @author lynng
 * 
 * Listener to verify off-heap memory before the product's off-heap memory allocator is closed. This is 
 * used in HA tests to look for orphans in a member that is being stopped *before* off-heap memory
 * is lost when the process is gone.
 * 
 * This should be used in conjunction with an end task OffHeapMemoryLifecycleListener.checkForErrors().
 * When the listener detects an error in off-heap memory, throwing an exception at that point does not make it
 * back to hydra, rather it ends up in the bg.log and causes a hang. So this listener writes the error text to the 
 * blackboard, which is checked in the OffHeapMemoryLifecycleListener.checkForErrors() task. 
 * 
 */
public class OffHeapMemoryLifecycleListener implements SimpleMemoryAllocatorImpl.LifecycleListener {

  private static boolean installed = false;
  private static Integer myVmId = null;
  private static Integer myPid = null;

  /** Install an instance of this listener
   * 
   */
  public static synchronized void install() {
    if (installed) {
      Log.getLogWriter().info("Not installing " + OffHeapMemoryLifecycleListener.class.getName() + " in this member because it is already installed");
    } else {
      SimpleMemoryAllocatorImpl.addLifecycleListener(new OffHeapMemoryLifecycleListener());
      Log.getLogWriter().info("Installed " + OffHeapMemoryLifecycleListener.class.getName());
      myVmId = RemoteTestModule.getMyVmid();
      myPid = RemoteTestModule.getMyPid();
      installed = true;
    }
  }

  /** Retrieve any errors from the blackboard and throw to fail the test
   * 
   */
  public static void checkForErrors() {
    List errList = (List) OffHeapMemoryLifecycleBB.getBB().getSharedMap().get(OffHeapMemoryLifecycleBB.errorKey);
    if (errList != null) {
      Log.getLogWriter().info("Detected " + errList.size() + " errors as follows:\n");
      for (Object err: errList) {
        Log.getLogWriter().info(err.toString());
      }
      throw new TestException(OffHeapMemoryLifecycleListener.class.getName() + " detected " + errList.size() + " errors (see logs), first error is " + errList.get(0));
    }
  }

  @Override
  public void afterCreate(SimpleMemoryAllocatorImpl allocator) {
    // this callback is unused
  }

  @Override
  public void afterReuse(SimpleMemoryAllocatorImpl allocator) {
    // this callback is unused 
  }

  @Override
  public void beforeClose(SimpleMemoryAllocatorImpl allocator) {
    Log.getLogWriter().info("In " + this.getClass().getName() + ".beforeClose (of SimpleMemoryAllocator)");
    if (OffHeapHelper.isOffHeapMemoryConfigured()) {
      int objectsStat = OffHeapHelper.getOffHeapMemoryStats().getObjects();
      if (objectsStat != 0) { // wait for off-heap to drain after closing cache
        OffHeapHelper.waitForOffHeapSilence(30); // off-heap can only be draining, off-heap memory allocator is about to be closed, no incoming messages
      }
      try {
        OffHeapHelper.verifyOffHeapMemoryConsistencyOnce();
      } catch (TestException e) { // log the error and write it to the blackboard
        synchronized (this.getClass()) {
          String errStr = this.getClass().getName() + " logged this exception in vmId " + myVmId + " (pid " + myPid + ") at " +
              new Date() + ": " + e.getMessage();
          Log.getLogWriter().info(errStr);
          List aList = (List) OffHeapMemoryLifecycleBB.getBB().getSharedMap().get(OffHeapMemoryLifecycleBB.errorKey);
          if (aList == null) {
            aList = new ArrayList();
          }
          aList.add(errStr);
          OffHeapMemoryLifecycleBB.getBB().getSharedMap().put(OffHeapMemoryLifecycleBB.errorKey, aList);
        }
      }
    } else {
      Log.getLogWriter().info("No off-heap memory in this member");
    }
  }

}
