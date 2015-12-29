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

import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.cache.*;
//import com.gemstone.gemfire.internal.cache.LocalRegion;
import java.util.concurrent.locks.Lock;
import util.*;
import hydra.*;

/**
 *  Utilities for managing distributed locks.
 */

public class DLockUtil {

   public static final String LOCK_SERVICE_NAME = "MyLockService";

  /**
   *  Returns a DistributedLockService with name LOCK_SERVICE_NAME.
   *  Creates one if it doesn't already exist.
   *
   */
  public synchronized static DistributedLockService getLockService() {
//     long startTime = System.currentTimeMillis();
     DistributedLockService dls = DistributedLockService.getServiceNamed(LOCK_SERVICE_NAME);
//     long endTime = System.currentTimeMillis();
     if (dls == null) {
        DistributedSystem ds = DistributedConnectionMgr.connect();
        if (ds == null)
           throw new TestException("DistributedSystem is " + ds);
//        startTime = System.currentTimeMillis();
        dls = DistributedLockService.create(LOCK_SERVICE_NAME, ds);
//        endTime = System.currentTimeMillis();
//        Log.getLogWriter().info("Getting DistributedLockService with create(" +
//            LOCK_SERVICE_NAME + ") took " + (endTime - startTime) + " ms");
     } else {
//        Log.getLogWriter().info("Getting DistributedLockService with getServiceNamed(" +
//            LOCK_SERVICE_NAME + ") took " + (endTime - startTime) + " ms");
     }
     return dls;
  }

  /**
   *  Executes {@link #getLock(Region,Object,long)} with <code>timeoutMs</code>
   *  set to {@link Long#MAX_VALUE}.
   *
   */
  public static void getLock( Object name ) {
    getLock( name, Long.MAX_VALUE );
  }
  /**
   *  Given a region and a key, get a lock on the key's entry in the region.
   */
  public static void getEntryLock( Region aRegion, Object key ) {

    Lock aLock = aRegion.getDistributedLock(key);
    long startTime = System.currentTimeMillis(); 
    aLock.lock();
    long endTime = System.currentTimeMillis(); 
    Log.getLogWriter().info("Getting entry lock on " + key + " took " + (endTime - startTime) + " ms");
  }
  /**
   *  Given a region and a key, unlock the entry lock for that key.
   */
  public static void unlockEntryLock( Region aRegion, Object key) {

    Lock aLock = aRegion.getDistributedLock(key);
    long startTime = System.currentTimeMillis(); 
    aLock.unlock();
    long endTime = System.currentTimeMillis(); 
    Log.getLogWriter().info("Unlocking entry lock for " + key + " took " + (endTime - startTime) + " ms");
  }
  /**
   *  Gets a lock on the object with the specified name.
   *  Waits <code>timeoutMs</code> milliseconds for the lock to be
   *  established before timing out.
   *
   *  @throws HydraRuntimeException if this thread already locked the object
   *          or if the attempt is interrupted or times out or if there is
   *          a cache exception.
   */
  public static void getLock( Object name, long timeoutMs ) {

    DistributedLockService dls = getLockService();
    long startTime = System.currentTimeMillis(); 
    boolean lock = dls.lock( name, timeoutMs, -1 );
    long endTime = System.currentTimeMillis(); 
    Log.getLogWriter().info("Getting lock on " + name + " took " + (endTime - startTime) + " ms");
    if ( ! lock ) {
      throw new HydraRuntimeException( "Timed out getting lock on " +
					name + " with timeout set to " + timeoutMs );
    }
  }
  /**
   *  Unlocks the object with the specified name.
   *
   *  @throws HydraRuntimeException if this thread does have the lock on the object
   *          or if there is a cache exception.
   */
  public static void unlock( Object name ) {

    if ( ! hasLock( name ) )
      throw new HydraRuntimeException( "Thread does not have the lock on " +
					name );
    DistributedLockService dls = getLockService();
    long startTime = System.currentTimeMillis(); 
    dls.unlock( name );
    long endTime = System.currentTimeMillis(); 
    Log.getLogWriter().info("Unlocking " + name + " took " + (endTime - startTime) + " ms");
  }
  /**
   *  Answers whether this thread has the lock for the object with the specified name.
   *
   *  @throws HydraRuntimeException if there is a cache exception.
   */
  public static boolean hasLock( Object name ) {
    return getLockService().isHeldByCurrentThread( name );
  }
}
