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
import com.gemstone.gemfire.cache.*;

import hydra.*;
import java.io.*;

import util.*;

/**
*
* Place for quick little lock tests.
*
*/

public class SimpleDLockClient {

  private static Cache  TheCache;
  private static Region TheRegion;
  private final static String ROOT_REGION_NAME = "DLockRegion";

  /**
   * Serializable Object class
   */
  public static class SerializableObject implements Serializable {
  }


  /**
   *  createCacheTask: General task to hook the vm to the distributed cache.
   */
  public static synchronized void createCacheTask() {
    if ( TheCache == null ) {
      TheCache = CacheUtil.createCache();
      RegionAttributes ratts = getRegionAttributes();
      TheRegion = CacheUtil.createRegion( ROOT_REGION_NAME, ratts );
    }
  }

  private static RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    Scope scope = DLockPrms.getScope();
    factory.setScope( scope );
    Log.getLogWriter().info("Setting scope to " + scope);
    RegionAttributes ratts = factory.createRegionAttributes();
    return ratts;
  }

  /**
   *  closeCacheTask: General task to unhook the vm from the distributed cache.
   */
  public static synchronized void closeCacheTask() {
    if ( TheCache != null ) {
      CacheUtil.closeCache();
      TheCache = null;
      TheRegion = null;
    }
  }

  /**
   *  lockObj: TASK to lock objects created by this thread
   */
  public static void lockObj() {
    SimpleDLockClient client = new SimpleDLockClient();
    client.lockObjWork();
  }
  private void lockObjWork() {

    Object obj = new SerializableObject();

    String myClientName = RemoteTestModule.getMyClientName();
    int tid = RemoteTestModule.getCurrentThread().getThreadId();
    String key = new String("Object_" + tid);

    CacheUtil.put( TheRegion, key, obj );

    int numTimesToEnterLock = tab().intAt(DLockPrms.numTimesToEnterLock);
    Log.getLogWriter().info("numTimesToEnterLock is " + numTimesToEnterLock);
    log().info("locking " + key + " " + obj);
    for (int counter = 1; counter <= numTimesToEnterLock; counter++) {
       DLockUtil.getLock( key );
    }
    log().info( "locked " + key + " " + obj);
    MasterController.sleepForMs( 2000 );

    log().info( "unlocking " + key + " " + obj);
    for (int counter = 1; counter <= numTimesToEnterLock; counter++) {
       DLockUtil.unlock( key );
    }
    log().info( "unlocked " + key + " " + obj);
  }

  //// support methods ////

  protected static ConfigHashtable tab() {
    return TestConfig.tab();
  }
  protected static LogWriter log() {
    return Log.getLogWriter();
  }
}
