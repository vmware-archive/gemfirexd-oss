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
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import java.util.concurrent.locks.Lock;

import hydra.*;
import hydra.blackboard.*;

import java.util.*;

import util.CacheUtil;
import util.TestHelper;

/**
 *
 *  N clients compete for M objects and lock P objects at a time.
 *
 */


public class BasicDLockClient {

  private static Cache  TheCache;
  private static Region TheRegion;

  public final static String ROOT_REGION_NAME = "DLockRegion";

  /**
   *  createCacheTask: General task to hook the vm to the distributed cache.
   */
  public static synchronized void createCacheTask() {
    if ( TheCache == null ) {
      TheCache = CacheUtil.createCache();
      int cacheLeaseTime = TestConfig.tab().intAt(DLockPrms.cacheLeaseTime, -1);
      if (cacheLeaseTime != -1) {
         Log.getLogWriter().info("Setting cache lock lease time to " + cacheLeaseTime + " seconds");
         TheCache.setLockLease(cacheLeaseTime);
      }
      int cacheLockTimeout = TestConfig.tab().intAt(DLockPrms.cacheLockTimeout, -1);
      if (cacheLockTimeout != -1) {
         Log.getLogWriter().info("Setting cache lock timeout to " + cacheLockTimeout + " seconds");
         TheCache.setLockTimeout(cacheLockTimeout);
      }
      RegionAttributes ratts = getRegionAttributes();
      TheRegion = CacheUtil.createRegion(ROOT_REGION_NAME, ratts);
    }
  }
  private static RegionAttributes getRegionAttributes() {
    AttributesFactory factory = new AttributesFactory();
    Scope scope = DLockPrms.getScope();
    factory.setScope( DLockPrms.getScope() );
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
   *  createDataTask: STARTTASK to create lockable data.
   */
  public static void createDataTask() {

    Vector datatypes = tab().vecAt( DLockPrms.datatypes );
    if ( datatypes == null )
      throw new HydraConfigException( "No datatypes specified" );

    for ( int i = 0; i < datatypes.size(); i++ ) {

      String datatype = (String) datatypes.elementAt(i);
      int datasize = tab().intAtWild( DLockPrms.datasizes, i, 1 );
      if ( datasize <= 0 )
        throw new HydraConfigException( "Datatype " + datatype + " must have positive datasize" );

      log().info( "Creating " + datasize + " lockable objects of type " +
                   datatype + " in region " + TheRegion.getName() );

      for ( int j = 0; j < datasize; j++ ) {
        Object name = generateName( datatype, i, j );
        Lockable wrapper = generateWrapper( datatype );
        wrapper.createDataInCache( TheRegion, name );
      }
    }
    log().info( "CONTENTS:\n" + CacheUtil.getContentString( TheRegion, true ) );
  }
  /**
   *  initTask: INITTASK to initialize information about lockable data for validation.
   */
  public static void initTask() {

    Vector datatypes = tab().vecAt( DLockPrms.datatypes );

    // initialize info array
    int total = 0;
    for ( int i = 0; i < datatypes.size(); i++ ) {
      int datasize = tab().intAtWild( DLockPrms.datasizes, i, 1 );
      total += datasize;
    }
    Info[] info = new Info[total];
    int index = 0;
    for ( int i = 0; i < datatypes.size(); i++ ) {
      String datatype = (String) datatypes.elementAt(i);
      int datasize = tab().intAtWild( DLockPrms.datasizes, i, 1 );
      for ( int j = 0; j < datasize; j++ ) {
        Object name = generateName( datatype, i, j );
        Lockable wrapper = generateWrapper( datatype );
        info[index] = new Info( name, wrapper );
        ++index;
      }
    }
    log().info( infoAsShortStrings( info ) );

    // set hydra thread locals
    localinfo.set( info );
    localindex.set( new Integer(0) );
  }
  private static Map Wrappers = new HashMap();
  private static synchronized Lockable generateWrapper( String datatype ) {
    Lockable wrapper = (Lockable) Wrappers.get( datatype );
    if ( wrapper == null ) {
      try {
        Class cls = Class.forName( datatype );
        wrapper = (Lockable) cls.newInstance();
        Wrappers.put( datatype, wrapper );
      } catch( ClassNotFoundException e ) {
        throw new HydraConfigException( datatype + " not found", e );
      } catch( ClassCastException e ) {
        throw new HydraConfigException( datatype + " does not implement Lockable", e );
      } catch( IllegalAccessException e ) {
        throw new HydraConfigException( "Unable to access code for " + datatype, e );
      } catch( InstantiationException e ) {
        throw new HydraConfigException( "Unable to instantiate " + datatype, e );
      }
    }
    return wrapper;
  }
  /**
   *  noContentionTask: TASK to lock one object at a time with no contention.
   *
   *  @throws HydraConfigException if there are fewer objects than client threads.
   */
  public static void noContentionTask() {
    BasicDLockClient client = new BasicDLockClient();
    client.noContentionWork();
  }
  private void noContentionWork() {
    boolean getLockFirst = tab().booleanAt( DLockPrms.getLockFirst );

    Info info[] = (Info[]) localinfo.get();

    int totalObjects = info.length;
    int totalThreads = TestConfig.getInstance().getTotalThreads();
    if ( totalThreads > totalObjects )
      throw new HydraConfigException( "Fewer objects than client threads" );

    int tid = RemoteTestModule.getCurrentThread().getThreadId();
    int i = tid % totalObjects;
    for ( int it = 0; it < tab().intAt( DLockPrms.iterations ); it++ ) {

      int numTimesToEnterLock = tab().intAt(DLockPrms.numTimesToEnterLock);
      Log.getLogWriter().info("numTimesToEnterLock is " + numTimesToEnterLock);
      if ( getLockFirst ) {
        for (int counter = 1; counter <= numTimesToEnterLock; counter++) {
           info[i].getWrapper().lock( TheRegion, info[i].getName() );
           info[i].incrementLocks();
        }
      }

      info[i].getWrapper().update( TheRegion, info[i].getName() );
      info[i].incrementUpdates();

      if ( getLockFirst ) {
        for (int counter = 1; counter <= numTimesToEnterLock; counter++) {
           info[i].getWrapper().unlock( TheRegion, info[i].getName() );
           info[i].incrementUnlocks();
        }
        checkLocksNotHeldByThisThread(info[i].getName());
      }
      /*
      i += totalThreads;
      if ( i >= totalObjects )
        i = tid % totalObjects;
      */
    }
  }
  /**
   *  singleFileTask: TASK to lock N objects before updates, with all clients starting at the origin
   */
  public static void singleFileTask() {
    BasicDLockClient client = new BasicDLockClient();
    client.singleFileWork();
  }
  private void singleFileWork() {
    boolean getLockFirst = tab().booleanAt( DLockPrms.getLockFirst );

    Info info[] = (Info[]) localinfo.get();
    int maxobjects = info.length;

    // find out where this client left off in the last task
    int i = ( (Integer) localindex.get() ).intValue();

    for ( int it = 0; it < tab().intAt( DLockPrms.iterations ); it++ ) {

      int numToLock = tab().intAt( DLockPrms.numToLock );

      int loop_start = i; // starting point for each iteration

      int numTimesToEnterLock = tab().intAt(DLockPrms.numTimesToEnterLock);
      Log.getLogWriter().info("numTimesToEnterLock is " + numTimesToEnterLock);
      // lock the objects
      if ( getLockFirst ) {
        i = loop_start;
        for ( int n = 0; n < numToLock; n++ ) {
           for (int counter = 1; counter <= numTimesToEnterLock; counter++) {
              info[i].getWrapper().lock( TheRegion, info[i].getName() );
              info[i].incrementLocks();
           }
           ++i;
           if ( i == maxobjects )
              i = 0;
        }
      }
      // update the objects
      i = loop_start;
      for ( int n = 0; n < numToLock; n++ ) {
        info[i].getWrapper().update( TheRegion, info[i].getName() );
        info[i].incrementUpdates();
        ++i;
        if ( i == maxobjects )
          i = 0;
      }
      // unlock the objects
      if ( getLockFirst ) {
        i = loop_start;
        for ( int n = 0; n < numToLock; n++ ) {
           for (int counter = 1; counter <= numTimesToEnterLock; counter++) {
              info[i].getWrapper().unlock( TheRegion, info[i].getName() );
              info[i].incrementUnlocks();
           }
           checkLocksNotHeldByThisThread(info[i].getName());
           ++i;
           if ( i == maxobjects )
             i = 0;
        }
      }
    }
    // note where this client left off
    localindex.set( new Integer( i ) );
  }
  /**
   *  randomLockTask: TASK to lock random objects on random datatypes before updates
   */
  public static void randomLockTask() {
    BasicDLockClient client = new BasicDLockClient();
    client.randomLockWork();
  }
  protected void randomLockWork() {
    Log.getLogWriter().info("Starting randomLockWork");
    boolean getLockFirst = tab().booleanAt( DLockPrms.getLockFirst );

    Info info[] = (Info[]) localinfo.get();
    GsRandom rng = tab().getRandGen();

    for ( int it = 0; it < tab().intAt( DLockPrms.iterations ); it++ ) {
      int numToLock = tab().intAt( DLockPrms.numToLock );
      if ( numToLock > info.length )
        throw new HydraConfigException( BasePrms.nameForKey( DLockPrms.numToLock ) + " is larger than the number of object available" );
      Log.getLogWriter().info("Finding object to lock: " + numToLock);
      for ( int n = 0; n < numToLock; n++ ) {
        while ( true ) {  // find a random unlocked object
          int i = rng.nextInt( 0, info.length - 1 );
          if ( ! info[i].isLocked() ) {
            info[i].setLocked( true );
            break;
          }
        }
      }
      int numTimesToEnterLock = tab().intAt(DLockPrms.numTimesToEnterLock);
      Log.getLogWriter().info("numTimesToEnterLock is " + numTimesToEnterLock);
      if ( getLockFirst ) {
        for ( int i = 0; i < info.length; i++ ) {
          if ( info[i].isLocked() ) {
            for (int counter = 1; counter <= numTimesToEnterLock; counter++) {
               //log().info( "CONTENTS:\n" + CacheUtil.getContentString( TheRegion, true ) );
               info[i].getWrapper().lock( TheRegion, info[i].getName() );
               info[i].incrementLocks();
            }
          }
        }
      }
      for ( int i = 0; i < info.length; i++ ) {
        if ( info[i].isLocked() ) {
          info[i].getWrapper().update( TheRegion, info[i].getName() );
          info[i].incrementUpdates();
        }
      }
      if ( getLockFirst ) {
        for ( int i = 0; i < info.length; i++ ) {
          if ( info[i].isLocked() ) {
            for (int counter = 1; counter <= numTimesToEnterLock; counter++) {
               info[i].getWrapper().unlock( TheRegion, info[i].getName() );
               info[i].incrementUnlocks();
            }
            checkLocksNotHeldByThisThread(info[i].getName());
          }
        }
      }
      for ( int i = 0; i < info.length; i++ ) {
        info[i].setLocked( false );
      }
    }
  }
  /**
   *  lockRegionTask: TASK to lock all objects on all datatypes by locking the region itself
   */
  public static void lockRegionTask() {

    BasicDLockClient client = new BasicDLockClient();
    client.lockRegionWork();
  }
  private void lockRegionWork() {
    boolean getLockFirst = tab().booleanAt( DLockPrms.getLockFirst );

    Info info[] = (Info[]) localinfo.get();

    int numTimesToEnterLock = tab().intAt(DLockPrms.numTimesToEnterLock);
    if (numTimesToEnterLock > 1) {
      throw new IllegalArgumentException(
      "Test parameter is invalid because RegionDistributedLock is non-re-entrant");
    }
    
    Log.getLogWriter().info("numTimesToEnterLock is " + numTimesToEnterLock);
    if ( getLockFirst ) {
        for (int counter = 1; counter <= numTimesToEnterLock; counter++) {
           Log.getLogWriter().info("Getting region lock on "+ TheRegion.getName());
           Lock aLock = TheRegion.getRegionDistributedLock();
           aLock.lock();
           Log.getLogWriter().info("Done getting region lock on "+ TheRegion.getName());
        }
    }
    for ( int i = 0; i < info.length; i++ ) {
      info[i].getWrapper().update( TheRegion, info[i].getName() );
      info[i].incrementUpdates();
    }
    if ( getLockFirst ) {
      for (int counter = 1; counter <= numTimesToEnterLock; counter++) {
         Log.getLogWriter().info("Unlocking region "+ TheRegion.getName());
         Lock aLock = TheRegion.getRegionDistributedLock();
         aLock.unlock();
         Log.getLogWriter().info("Done unlocking region "+ TheRegion.getName());
      }
    }
  }

  /**
   *  lockEmAllAtOnceTask: TASK to lock all objects on all datatypes before updates
   */
  public static void lockEmAllAtOnceTask() {

    BasicDLockClient client = new BasicDLockClient();
    client.lockEmAllAtOnceWork();
  }
  private void lockEmAllAtOnceWork() {
    boolean getLockFirst = tab().booleanAt( DLockPrms.getLockFirst );

    Info info[] = (Info[]) localinfo.get();

    int numTimesToEnterLock = tab().intAt(DLockPrms.numTimesToEnterLock);
    Log.getLogWriter().info("numTimesToEnterLock is " + numTimesToEnterLock);
    if ( getLockFirst ) {
      for ( int i = 0; i < info.length; i++ ) {
        for (int counter = 1; counter <= numTimesToEnterLock; counter++) {
           info[i].getWrapper().lock( TheRegion, info[i].getName() );
           info[i].incrementLocks();
        }
      }
    }
    for ( int i = 0; i < info.length; i++ ) {
      info[i].getWrapper().update( TheRegion, info[i].getName() );
      info[i].incrementUpdates();
    }
    if ( getLockFirst ) {
      for ( int i = 0; i < info.length; i++ ) {
        for (int counter = 1; counter <= numTimesToEnterLock; counter++) {
           info[i].getWrapper().unlock( TheRegion, info[i].getName() );
           info[i].incrementUnlocks();
        }
        checkLocksNotHeldByThisThread(info[i].getName());
      }
    }
  }

  /**
   *  Task: INITTASK to put starting number of threads in shared counter
   */

  public static void initBBCrashLockHolderTask() {

    BasicDLockClient client = new BasicDLockClient();
    client.initBBCrashLockHolderWork();
  }

  private void initBBCrashLockHolderWork() {

    Blackboard bb = DLockBlackboard.getInstance();
    hydra.blackboard.SharedCounters sc = bb.getSharedCounters();
    sc.increment(DLockBlackboard.NumCurrentThreads);   

 }
  
  /**
   *  Task: TASK to verify crashing the lock holder doesn't prevent other threads
   *        from successfully proceeding to get locks (on same objects).
   *        This test follows the pattern of the LeaseTime test.  All threads get
   *        ready to lock, one (which may also be the lock grantor) starts locking and
   *        then crashes.  
   */
  public static void crashLockHolderTask() throws ClientVmNotFoundException {

    BasicDLockClient client = new BasicDLockClient();
    client.crashLockHolderWork();
  }

  private void crashLockHolderWork() throws ClientVmNotFoundException {

    // if we're starting over after a crash, need to recreate cache
    if (TheCache == null) {
	createCacheTask();
    }

    Blackboard bb = DLockBlackboard.getInstance();
    hydra.blackboard.SharedCounters sc = bb.getSharedCounters();

    // still needed?
    int numVms = TestHelper.getNumVMs();
    int totalThreadsInTest = TestHelper.getNumThreads();
    
    ClientDescription clientDesc = TestConfig.getInstance().getClientDescription(RemoteTestModule.getMyClientName());
    int numThreadsInClientVm = clientDesc.getVmThreads();
    Info info[] = (Info[]) localinfo.get();

    // init test parameters
    int waitLimit = 360000;
    boolean getLockFirst = tab().booleanAt( DLockPrms.getLockFirst );
    int numTimesToCrash = tab().intAt(DLockPrms.numTimesToCrash);
    int numTimesToEnterLock = tab().intAt(DLockPrms.numTimesToEnterLock);
    boolean crashGrantor = tab().booleanAt( DLockPrms.crashGrantor );

    
    Log.getLogWriter().info("### total threads: " + totalThreadsInTest);
    Log.getLogWriter().info("### num Vms: " + numVms);
    Log.getLogWriter().info("### num in this client VM: " + numThreadsInClientVm);
    
    Log.getLogWriter().info("numTimesToEnterLock is " + numTimesToEnterLock);
    Log.getLogWriter().info("numTimesToCrash is " + numTimesToCrash);


    // wait for all threads to be ready to get the lock
    int numCurrentThreads = (int) sc.read(DLockBlackboard.NumCurrentThreads);
    sc.increment(DLockBlackboard.ReadyToLock);
    TestHelper.waitForCounter(bb, "DLockBlackboard.ReadyToLock", DLockBlackboard.ReadyToLock,
              numCurrentThreads, true, waitLimit);
    sc.zero(DLockBlackboard.DoneWithTask);  

    Log.getLogWriter().info("###Thread ready to lock: " + Thread.currentThread().getName());
    Log.getLogWriter().info("###CrashGrantor is: " + crashGrantor); 
    boolean isGrantor = ((DistributedRegion)TheRegion).getLockService().isLockGrantor();   
    Log.getLogWriter().info("###isGrantor is: " + isGrantor);  

    // get locks
    if ( getLockFirst ) {
      for ( int i = 0; i < info.length; i++ ) {
        for (int counter = 1; counter <= numTimesToEnterLock; counter++) {
           info[i].getWrapper().lock( TheRegion, info[i].getName() );
           info[i].incrementLocks();
        }
      }
    }
    if (crashGrantor == isGrantor) {
       // if want to crash Grantor, must be Grantor
       // if want to crash nonGrantor, then must not be Grantor
 	 // crash (if we haven't already exceeded number of crashes specified) 
       if (numTimesToCrash > (sc.incrementAndRead(DLockBlackboard.NumCrashes) - 1)){
	    Log.getLogWriter().info("###### testing crash");
          sc.subtract(DLockBlackboard.NumCurrentThreads, numThreadsInClientVm);
          sc.add(DLockBlackboard.DoneWithTask, numThreadsInClientVm);
          sc.add(DLockBlackboard.DoneWithLock, numThreadsInClientVm);
          // task to save any accumulated info to blackboard for later validation with cached object.
	  closeTask();

          // @todo looks like this is intended to allow restart, so redo later
          ClientVmMgr.stopAsync( "testing crash",
            ClientVmMgr.MEAN_KILL,  // kill -TERM
            ClientVmMgr.NEVER       // never allow this VM to restart
          );
          return;

          //DistributedConnectionMgr.disconnect();
          //throw new StopSchedulingTaskOnClientOrder();
       }
    }
    // if no crash, proceed with updates and unlocking
    for ( int i = 0; i < info.length; i++ ) {
      info[i].getWrapper().update( TheRegion, info[i].getName() );
      info[i].incrementUpdates();
    }
    if ( getLockFirst ) {
      for ( int i = 0; i < info.length; i++ ) {
        for (int counter = 1; counter <= numTimesToEnterLock; counter++) {
          info[i].getWrapper().unlock( TheRegion, info[i].getName() );
          info[i].incrementUnlocks();
        }
        checkLocksNotHeldByThisThread(info[i].getName());
      }
    }

    // wait for all threads to be done, before continuing
    // (if crash happened, numCurrentThreads will be different)
    //numCurrentThreads = (int) sc.read(DLockBlackboard.NumCurrentThreads);

    Log.getLogWriter().info("###numThreads after possible crash: " + numCurrentThreads);

    sc.increment(DLockBlackboard.DoneWithLock);
    TestHelper.waitForCounter(bb, "DLockBlackboard.DoneWithLock",
              DLockBlackboard.DoneWithLock, numCurrentThreads, true, Integer.MAX_VALUE);
    Log.getLogWriter().info("###DoneWithLock: " + sc.read(DLockBlackboard.DoneWithLock));
    sc.zero(DLockBlackboard.ReadyToLock);

    // wait for all threads to be done with the task
    sc.increment(DLockBlackboard.DoneWithTask);  
    TestHelper.waitForCounter(bb, "DLockBlackboard.DoneWithLock", DLockBlackboard.DoneWithTask,
              numCurrentThreads, true, waitLimit);
    Log.getLogWriter().info("###DoneWithTask: " + sc.read(DLockBlackboard.DoneWithTask));
    sc.zero(DLockBlackboard.DoneWithLock);   

  }


  /**
   *  stickLocks: TASK to creat stuck locks 
   *  This works to create stuck lock - but per spec change - that's not
   *  the main point anymore.
   *

   */
  public static void stickLocks() {
    BasicDLockClient client = new BasicDLockClient();
    client.stickLocksWork();
  }
  private void stickLocksWork() {

    boolean getLockFirst = tab().booleanAt( DLockPrms.getLockFirst );
    Info info[] = (Info[]) localinfo.get();
    GsRandom rng = tab().getRandGen();

    
    for ( int it = 0; it < tab().intAt( DLockPrms.iterations ); it++ ) {
      int numToLock = tab().intAt( DLockPrms.numToLock );
      if ( numToLock > info.length )
        throw new HydraConfigException( BasePrms.nameForKey( DLockPrms.numToLock ) + " is larger than the number of object available" );
      // identify and flag what to lock
      for ( int n = 0; n < numToLock; n++ ) {
        while ( true ) {  // find a random unlocked object
          int i = rng.nextInt( 0, info.length - 1 );
          if ( ! info[i].isLocked() ) {
            info[i].setLocked( true );
            break;
          }
        }
      }
      // lock the entries
      int numTimesToEnterLock = tab().intAt(DLockPrms.numTimesToEnterLock);
      Log.getLogWriter().info("numTimesToEnterLock is " + numTimesToEnterLock);
      if ( getLockFirst ) {
        for ( int i = 0; i < info.length; i++ ) {
          if ( info[i].isLocked() ) {
            for (int counter = 1; counter <= numTimesToEnterLock; counter++) {
               //log().info( "CONTENTS:\n" + CacheUtil.getContentString( TheRegion, true ) );
               info[i].getWrapper().lock( TheRegion, info[i].getName() );
               info[i].incrementLocks();
            }
          }
        }
      }
      // update the locked entries
      for ( int i = 0; i < info.length; i++ ) {
        if ( info[i].isLocked() ) {
          info[i].getWrapper().update( TheRegion, info[i].getName() );
          info[i].incrementUpdates();
        }
      }

      // unset the "locked" flag before sticking locks - to avoid sticking
      // these same locks next time
      for ( int i = 0; i < info.length; i++ ) {
        info[i].setLocked( false );
      }

      // stick the locks
      log().info("Bouncing connection..");
      log().info("Old connection -> " + DistributedConnectionMgr.getConnection() );
      DistributedConnectionMgr.disconnect();
      DistributedConnectionMgr.connect();
      log().info("New connection -> " + DistributedConnectionMgr.getConnection() );

    }   
 
  }
  /**
   *  closeTask: CLOSETASK to report results of validation structures
   */
  public static void closeTask() {

    Info[] info = (Info[]) localinfo.get();
    log().info( info.toString() );

    SharedMap map = DLockBlackboard.getInstance().getSharedMap();
    String key = "UpdateCounts" + RemoteTestModule.getCurrentThread().getThreadId();
    map.put( key, info );
  }
  /**
   *  validateTask: CLOSETASK to validate results
   */
  public static void validateTask() {

    // accumulate the client counts
    Info[] totalinfo = null;
    SharedMap map = DLockBlackboard.getInstance().getSharedMap();
    for ( Iterator it = map.getMap().keySet().iterator(); it.hasNext(); ) {
      String key = (String) it.next();
      if ( key.startsWith( "UpdateCounts" ) ) {
        Info[] info = (Info[]) map.get( key );
        if ( totalinfo == null )
          totalinfo = new Info[ info.length ];
        for ( int i = 0; i < info.length; i++ ) {
          if ( totalinfo[i] == null )
            totalinfo[i] = info[i];
          else
            totalinfo[i].addInfo( info[i] );
        }
      }
    }
    // validate the individual objects
    for ( int i = 0; i < totalinfo.length; i++ ) {
      totalinfo[i].getWrapper().validate( TheRegion, totalinfo[i] );
    }
    // @todo lises validate the totals for each wrapper

  }

  //// support methods ////

  /** Check that this thread does not hold a lock on this object */
  private void checkLocksNotHeldByThisThread(Object name) {
     if (TestConfig.tab().booleanAt(DLockPrms.useEntryLock)) { 
        // locks are obtained by getting an entry lock on a region; the Lock API
        // does not have a way to ask if this thread has a lock
        return;
     } else {
        boolean hasLock = DLockUtil.hasLock(name);
        if (hasLock)
           throw new DLockTestException("Expected this thread to hold no locks on " + name +
                     ", but hasLock is " + hasLock);
     }
  }
  private static Object generateName( String datatype, int i, int j ) {
    return datatype + "_" + i + "_" + j;
  }
  private static String infoAsShortStrings( Info[] info ) {
    StringBuffer buf = new StringBuffer();
    buf.append( "INFO...." );
    for ( int i = 0; i < info.length; i++ ) {
      buf.append( "\n    " + info[i].toShortString() );
    }
    return buf.toString();
  }
//  private static String infoAsStrings( Info[] info ) {
//    StringBuffer buf = new StringBuffer();
//    buf.append( "INFO...." );
//    for ( int i = 0; i < info.length; i++ ) {
//      buf.append( "\n    " + info[i] );
//    }
//    return buf.toString();
//  }
  protected static LogWriter log() {
    return Log.getLogWriter();
  }
  protected static ConfigHashtable tab() {
    return TestConfig.tab();
  }
  protected static HydraThreadLocal localinfo  = new HydraThreadLocal(); // info about lockable types
  protected static HydraThreadLocal localindex = new HydraThreadLocal(); // last index used
}
