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

package cacheperf.comparisons.putAll;

import com.gemstone.gemfire.cache.*;

import distcache.gemfire.GemFireCacheTestImpl;
import hydra.*;

import java.util.*;

import objects.*;
import parReg.query.ParRegQueryBB;
import perffmwk.*;
import cacheperf.*;


/**
 *
 *  Client used to measure cache performance.
 *
 */
public class PutAllPerfClient extends cacheperf.CachePerfClient {

  //----------------------------------------------------------------------------
  //  Trim interval names
  //----------------------------------------------------------------------------

  protected static final int PUTALL_ENTRIES     = 200;

  protected static final String PUTALL_NAME = "putAllEntries";
  protected static final int mapSize = PutAllPerfPrms.getMapSize();
  protected static HashMap[] maps = null;
  protected static int whichMap;
  protected static int numOfMaps = 3;
  
  //----------------------------------------------------------------------------
  //  Tasks
  //----------------------------------------------------------------------------
  /**
   *  putAllTask()
   */
  public static void putAllTask() {
    PutAllPerfClient c = new PutAllPerfClient();
    c.initialize( PUTALL_ENTRIES );
    c.executePutAllOperation();
  }
  
  public static synchronized void initPutAllTask() {
    PutAllPerfClient c = new PutAllPerfClient();
    c.initialize();
    c.initHydraThreadLocals();
    c.generateMapsForPutAll();
  } 
 
  protected void generateMapsForPutAll() {
    if (maps != null) return;
    
    int count = 0;
    int size = numOfMaps;
    maps = new HashMap[size];
    do {
      HashMap aMap = new HashMap();
      int aKey = 1;
      do {
        Object key = ObjectHelper.createName(this.keyType, aKey);
        String objectType = CachePerfPrms.getObjectType();
        Object value = ObjectHelper.createObject(objectType, aKey);
        aMap.put(key,value);
        if (log().finerEnabled()) log().finer("put into putAll map for key = " + key +
            ", value = " + value);
        aKey++;
      } while (aMap.size() < mapSize);
      maps[count] = aMap;
      count++;
    } while (count<size);
  }
  
  protected synchronized HashMap getMap() {
    int i = whichMap % numOfMaps;
    whichMap++;
    return maps[i];
  }
  
  private void executePutAllOperation() {
    if (log().fineEnabled()) log().fine("executing putAll operation");
    do {
      HashMap aMap = getMap();
      
      executeTaskTerminator();
      executeWarmupTerminator();
      
      putAll(aMap);
      
      ++this.batchCount;
      ++this.count;
      ++this.keyCount;

    } while (!executeBatchTerminator());
    if (log().fineEnabled()) log().fine("finishing executing putAll opeation");
  }
  private void putAll(HashMap aMap ) {
    long start;
    int size = aMap.size();
    Region theRegion = ((GemFireCacheTestImpl)super.cache).getRegion();

    start = this.putallstats.startPutAll();
    try {            
      theRegion.putAll(aMap);           
    } catch(Exception e) {     
      throw new CachePerfException( "Could not execute putAll operation ", e );
    }
   
    this.putallstats.endPutAll( start, size );
//    ParRegQueryBB.getBB().getSharedCounters().increment(ParRegQueryBB.operationCount);
    if (log().finerEnabled()) log().finer("finished putAll operation for mapSize = " + size);
  }  

  /**
   *  TASK to register the putAll performance statistics object.
   */
  public static void openStatisticsTask() {
    PutAllPerfClient c = new PutAllPerfClient();
    c.initHydraThreadLocals();
    c.openStatistics();
    c.updateHydraThreadLocals();
  }
  
  private void openStatistics() {
    if ( this.putallstats == null ) {
      log().info( "Opening per-thread putAll performance statistics" );
      this.putallstats = PutAllPerfStats.getInstance();
      log().info( "Opened per-thread putAll performance statistics" );
    }
  }

  /**
   *  TASK to unregister the performance statistics object.
   */
  public static void closeStatisticsTask() {
    PutAllPerfClient c = new PutAllPerfClient();
    c.initHydraThreadLocals();
    c.closeStatistics();
    c.updateHydraThreadLocals();
//    Log.getLogWriter().info("Total putAll operation performed is " + 
//        ParRegQueryBB.getBB().getSharedCounters().read(ParRegQueryBB.operationCount));
  }
  protected void closeStatistics() {
    MasterController.sleepForMs( 2000 );
    if ( this.putallstats != null ) {
      log().info( "Closing per-thread putAll performance statistics" );
      this.putallstats.close();
      log().info( "Closed per-thread putAll performance statistics" );
    }
  }

  //----------------------------------------------------------------------------
  //  Hydra thread locals and their instance field counterparts
  //----------------------------------------------------------------------------

  public PutAllPerfStats putallstats;

  private static HydraThreadLocal localputallstats = new HydraThreadLocal();

  protected void initHydraThreadLocals() {
    super.initHydraThreadLocals();

    this.putallstats = getPutAllStats();

  }

  protected void updateHydraThreadLocals() {
    super.updateHydraThreadLocals();

    setPutAllStats( this.putallstats );
  }

  /**
   *  Gets the per-thread QueryStats wrapper instance.
   */
  protected PutAllPerfStats getPutAllStats() {
    PutAllPerfStats currputallstats = (PutAllPerfStats) localputallstats.get();
    return currputallstats;
  }
  /**
   *  Sets the per-thread QueryStats wrapper instance.
   */
  protected void setPutAllStats(PutAllPerfStats putallstats ) {
    localputallstats.set( putallstats );
  }

  //----------------------------------------------------------------------------
  //  Overridden methods
  //----------------------------------------------------------------------------
  
  protected String nameFor( int name ) {
    switch (name) {
      case PUTALL_ENTRIES:      return PUTALL_NAME;
    }
    return super.nameFor(name);
  }
}
