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

import hydra.*;
import util.*;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.distributed.internal.locks.DLockService;

public class DLSCreateDestroy {

protected static final String LOCK_SERVICE_PREFIX = "MyLockService_";

public static void HydraTask_CreateDestroy() {
   DLSCreateDestroy testInstance = new DLSCreateDestroy();
   long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
   long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
   long startTime = System.currentTimeMillis();
   do {
      testInstance.doCreateDestroy();
   } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
}

private void doCreateDestroy() {
   DistributedSystem dSystem = DistributedConnectionMgr.connect(); // should never return null
   if (dSystem == null)
      throw new TestException("DistributedSystem is " + dSystem);
   int destroyPercent = 10;  // destroy the lock service this % of the time
//   int freeResourcesPercent = 10;  // free DLS resources this % of the time
   int numDLS = 10;
   int randInt = TestConfig.tab().getRandGen().nextInt(1, numDLS);
   String lockServiceName = LOCK_SERVICE_PREFIX + randInt;

   // create the lock service
   Log.getLogWriter().info("Getting DistributedLockService with name " + lockServiceName);
   DistributedLockService dls = DistributedLockService.getServiceNamed(lockServiceName);
   long attempts = 0;
   while (dls == null) {
      try {
         Log.getLogWriter().info("Creating DistributedLockService with name " + lockServiceName);
         dls = DistributedLockService.create(lockServiceName, dSystem);
         Log.getLogWriter().info("Created " + dls);
      } catch (IllegalArgumentException ex) {
         Log.getLogWriter().info("Got expected Exception: Caught while creating dls " + ex);
         dls = DistributedLockService.getServiceNamed(lockServiceName);
      }
      attempts++;
      if (attempts > 100)
         throw new TestException("Could not get dls for " + lockServiceName + " in 100 attempts");
   } // while

   // get a lock; every lock request is a different lock than anybody else is locking
   try {
      Object objectToLock = "LockFor-" + lockServiceName + "_" + 
             DLockBlackboard.getInstance().getSharedCounters().
             incrementAndRead(DLockBlackboard.LockNameIndex);
      Log.getLogWriter().info("Locking object " + objectToLock + " in " + dls);
      boolean gotLock = dls.lock(objectToLock, -1, -1);
      Log.getLogWriter().info("Using " + dls + ", got lock " + gotLock);
      if (!gotLock) { 
         throw new TestException("Didn't get lock: " + gotLock); 
      }
   }
   catch (CancelException ex) {
     Log.getLogWriter().info("Got expected Exception: Caught while getting lock " + ex);
   }
   catch (IllegalStateException ex) {
//      String errStr = ex.toString();
      if (((DLockService)dls).isDestroyed())
         Log.getLogWriter().info("Got expected Exception: Caught while getting lock " + ex);
      else
         throw ex;
   }

   // free the resources
   if ((TestConfig.tab().getRandGen().nextInt(1, 100)) <= destroyPercent) {
      try {
         Log.getLogWriter().info("Freeing resource for " + dls);
         dls.freeResources(lockServiceName);
      } catch (IllegalArgumentException ex) { 
         // can get this if dls was destroyed by another thread or VM and it was never created here
         String errStr = ex.toString();
         if (errStr.indexOf("Service named " + lockServiceName + " not created") >= 0)
            Log.getLogWriter().info("Got expected Exception: Caught while destroying " + ex);
         else
            throw ex;
      }
      catch (CancelException ex) {
        Log.getLogWriter().info("Got expected Exception: Caught while destroying " + ex);
      }
      catch (IllegalStateException ex) {
         Log.getLogWriter().info("Got expected Exception: Caught while destroying " + ex);
      }
   } 

   // destroy the lock service
   if ((TestConfig.tab().getRandGen().nextInt(1, 100)) <= destroyPercent) {
      try {
         Log.getLogWriter().info("Destroying " + dls);
         DistributedLockService.destroy(lockServiceName);
         Log.getLogWriter().info("Destroyed " + dls);
      } catch (IllegalArgumentException ex) { 
         // can get this if dls was destroyed by another thread or VM and it was never created here
         String errStr = ex.toString();
         if (errStr.indexOf("Service named " + lockServiceName + " not created") >= 0)
            Log.getLogWriter().info("Got expected Exception: Caught while destroying " + ex);
         else
            throw ex;
      }
      catch (CancelException ex) {
        Log.getLogWriter().info("Got expected Exception: Caught while destroying " + ex);
      }
      catch (IllegalStateException ex) {
         Log.getLogWriter().info("Got expected Exception: Caught while destroying " + ex);
      }
   } 
}

}
