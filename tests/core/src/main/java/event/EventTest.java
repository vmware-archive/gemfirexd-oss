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
package event;

import java.util.*;
import util.*;
import hydra.*;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;

/**
 * A Hydra test that concurrently performs a number of cache-related
 * (both entry and region) operations.  Along the way, it uses the
 * {@link EventBB}, {@link EventCountersBB} and {@link WriterCountersBB 
 * to keep track of what * has happened and to validate that what has 
 * happened is expected (in terms of CacheListener & Writer callbacks).
 * It requires that the regions that it tests be distributed and
 * mirrored.
 *
 * @see EventPrms
 *
 * @author Lynn Gallinat
 * @since 2.0
 */
public class EventTest {
    
    /* The singleton instance of EventTest in this VM */
    static protected EventTest eventTest;
    
    // operations
    static protected final int ADD_OPERATION = 1;
    static protected final int UPDATE_OPERATION = 2;
    static protected final int INVALIDATE_OPERATION = 3;
    static protected final int DESTROY_OPERATION = 4;
    static protected final int READ_OPERATION = 5;
    static protected final int LOCAL_INVALIDATE_OPERATION = 6;
    static protected final int LOCAL_DESTROY_OPERATION = 7;
    static protected final int REGION_CLOSE_OPERATION = 8;
    static protected final int CLEAR_OPERATION = 9;
    static protected final int PUT_IF_ABSENT_OPERATION = 10;
    static protected final int REMOVE_OPERATION = 11;
    static protected final int REPLACE_OPERATION = 12;
    static protected final int PUTALL_OPERATION = 13;
    
    protected boolean useTransactions;
    // cache whether this instance should perform all operations in one invocation
    // as a single transaction.
    protected boolean isSerialExecution;
    // cache whether this is serial execution
    public boolean isCarefulValidation = false;
    // true if this test does careful validation
    public boolean listenerInstalled = false;
    // true if this test has a listenerInstalled, false otherwise
    static public final int MILLIS_TO_WAIT = 60000;
    // the number of millis to wait for an event to occur in serial execution tests
    protected int numVMs;
    // the number of VMs in this test
    protected long minTaskGranularitySec;
    // the task granularity in seconds
    protected long minTaskGranularityMS;
    // the task granularity in milliseconds
    protected RandomValues randomValues = null;
    // for creating random objects
    protected boolean useEvictionController;
    // true if the test is using an eviction controller, false otherwise
    protected int maxObjects;
    // the maximum number of objects to allow in the region
    protected int maxRegions;
    // the maximum number of regions to allow
    protected DistributedLockService distLockService;
    // the distributed lock service for this VM
    protected boolean isMirrored;
    // for entry event tests, indicates if the region this VM is operating on
    // is replicated (Congo & beyond or mirrored (deprecated in Congo))
    protected boolean isListenerTest;
    // indicates if this is really an instatiation of ListenerTest
    protected boolean useCounters;
    
    //Value of EventPrms.lowerThreshold
    protected int lowerThreshold;

    //Value of EventPrms.upperThreshold
    protected int upperThreshold;

    // indicates whether or not we're using ETListener counters
    // based on (this instanceof ListenerTest) ? false : true
    public String regionName; // the name of the root region
    protected Random rand = new Random();
    
    // String prefixes for event callback object
    protected static final String createCallbackPrefix = "Create event originated in pid ";
    protected static final String updateCallbackPrefix = "Update event originated in pid ";
    protected static final String invalidateCallbackPrefix = "Invalidate event originated in pid ";
    protected static final String destroyCallbackPrefix = "Destroy event originated in pid ";
    protected static final String regionInvalidateCallbackPrefix = "Region invalidate event originated in pid ";
    protected static final String regionDestroyCallbackPrefix = "Region destroy event originated in pid ";

    protected static final String memberIdString = " memberId=";
    protected static final String VmIDStr = "VmId_";
    
    // lock names
    protected static String LOCK_SERVICE_NAME = "MyLockService";
    protected static String LOCK_NAME = "MyLock";
    
    // region name(s)
    protected static final String PR_NAME_PREFIX = "ParReg_";
    
    // ========================================================================
    // initialization methods
    
    /**
     * Creates and {@linkplain #initialize initializes} the singleton
     * instance of <code>EventTest</code> in this VM.
     */
    public synchronized static void HydraTask_initialize() {
        if (eventTest == null) {
            eventTest = new EventTest();
            eventTest.initialize();
        }
    }
    
    /**
     * @see #HydraTask_initialize
     */
    protected void initialize() {
        createRootRegions();
        Region aRegion = (Region)CacheUtil.createCache().rootRegions().iterator().next();
        
        useTransactions = EventPrms.useTransactions();
        isSerialExecution = EventBB.isSerialExecution();
        isCarefulValidation = isCarefulValidation || isSerialExecution;
        numVMs = 0;
        Vector gemFireNamesVec = TestConfig.tab().vecAt(GemFirePrms.names);
        Vector numVMsVec = TestConfig.tab().vecAt(ClientPrms.vmQuantities);
        if (gemFireNamesVec.size() == numVMsVec.size()) {
            for (int i = 0; i < numVMsVec.size(); i++) {
                numVMs = numVMs + (new Integer(((String)numVMsVec.elementAt(i)))).intValue();
            }
        } else {
            numVMs = new Integer((String)(numVMsVec.elementAt(0))).intValue() * gemFireNamesVec.size();
        }
        Log.getLogWriter().info("numVMs is " + numVMs);
        minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
        minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
        maxRegions = TestConfig.tab().intAt(EventPrms.maxRegions, -1);
        maxObjects = TestConfig.tab().intAt(EventPrms.maxObjects, -1);
        randomValues = new RandomValues();
        isListenerTest = this instanceof ListenerTest;
        useCounters = !isListenerTest;
        
        String clientName = System.getProperty( "clientName" );
        EventBB.getBB().getSharedMap().put(EventBB.CURRENT_REGION_NAMES, new ArrayList());

        createLockService();

        EventBB.getBB().printSharedCounters();
        EventCountersBB.getBB().printSharedCounters();
        OperationCountersBB.getBB().printSharedCounters();
        WriterCountersBB.getBB().printSharedCounters();
    }
    
    /**
     * If necessary, creates the {@link DistributedLockService} used by
     * this test.
     */
    static synchronized void createLockService() {
        if (eventTest.distLockService == null) {
            Log.getLogWriter().info("Creating lock service " + LOCK_SERVICE_NAME);
            eventTest.distLockService = DistributedLockService.create(LOCK_SERVICE_NAME, DistributedSystemHelper.getDistributedSystem());
            Log.getLogWriter().info("Created lock service " + LOCK_SERVICE_NAME);
        }
    }

    // ========================================================================
    // hydra task methods
    
    /**
     * Performs randomly selected operations (add, invalidate, etc.) on
     * the root region based on the weightings in {@link
     * EventPrms#entryOperations}.  The operations will continue to be
     * performed until the {@linkplain
     * TestHelperPrms#minTaskGranularitySec minimum task granularity}
     * has been reached.
     */
    public static void HydraTask_doEntryOperations() {
        Region rootRegion = CacheUtil.getCache().getRegion(eventTest.regionName);
        eventTest.doEntryOperations(rootRegion);
    }
    
    /**
     * Performs randomly selected operations (create new region,
     * invalidate an entire randomly selected region, etc.) based on the
     * weightings in {@link EventPrms#regionOperations}.The operations
     * will continue to be performed until the {@linkplain
     * TestHelperPrms#minTaskGranularitySec minimum task granularity}
     * has been reached.
     */
    public static void HydraTask_doRegionOperations() {
        eventTest.doRegionOperations();
    }
    
    /**
     * Adds new entries to randomly selected regions until the
     * {@linkplain TestHelperPrms#minTaskGranularitySec minimum task
     * granularity} has been reached.
     */
    public static void HydraTask_addToRegion() {
        eventTest.addToRegion();
    }
    
    /**
     * Randomly stop and restart vms.
     */
    public static void HydraTask_stopStartVMs() {
       int numVMsToStop = TestConfig.tab().intAt(StopStartPrms.numVMsToStop);  

      Object[] tmpArr = StopStartVMs.getOtherVMsWithExclude(numVMsToStop, "locator");
      // get the VMs to stop; vmList and stopModeList are parallel lists
      List vmList = (List)(tmpArr[0]);
      List stopModeList = (List)(tmpArr[1]);
      StopStartVMs.stopStartVMs(vmList, stopModeList);
    }
    
    // ========================================================================
    // methods to add/update/invalidate/destroy an object in a region
    
    /**
     * @see #HydraTask_doEntryOperations
     */
    protected void doEntryOperations(Region aRegion) {
        long startTime = System.currentTimeMillis();
        if (isSerialExecution) {
            logExecutionNumber();
        }
        
        boolean haveALock = false;
        boolean isMirrored = false;

        DataPolicy dataPolicy = aRegion.getAttributes().getDataPolicy();
        if (dataPolicy == null) {  
          MirrorType mt = aRegion.getAttributes().getMirrorType();
          isMirrored = mt.isMirrored();
        } else {
          isMirrored = dataPolicy.withReplication();
        }
        
        if (useTransactions) {
            TxHelper.begin();
        }
        
        lowerThreshold = TestConfig.tab().intAt(EventPrms.lowerThreshold, -1);
        upperThreshold = TestConfig.tab().intAt(EventPrms.upperThreshold, Integer.MAX_VALUE);
        
        Log.getLogWriter().info("lowerThreshold " + lowerThreshold + ", " + "upperThreshold " + upperThreshold );

        do {
            TestHelper.checkForEventError(EventBB.getBB());
            boolean useRandomLocks = TestConfig.tab().booleanAt(EventPrms.useRandomLocks);
            if (useRandomLocks) {
                Log.getLogWriter().info("Trying to get distributed lock " + LOCK_NAME + "...");
                haveALock = distLockService.lock(LOCK_NAME, -1, -1);
                Log.getLogWriter().info("Returned from trying to get distributed lock " + LOCK_NAME +
                        ", lock acquired is " + haveALock);
                if (haveALock)
                    Log.getLogWriter().info("Obtained distributed lock " + LOCK_NAME);
            }
            
            try {
                int whichOp = getOperation(EventPrms.entryOperations, isMirrored);
                
		int size = aRegion.size();
                if (size >= upperThreshold) {
		    whichOp = getOperation(EventPrms.upperThresholdOperations, isMirrored);
		}else if (size <= lowerThreshold) {
		    whichOp = getOperation(EventPrms.lowerThresholdOperations, isMirrored);
		}

		switch (whichOp) {
                    case ADD_OPERATION:
                        addObject(aRegion, true);
                        break;
                    case INVALIDATE_OPERATION:
                        invalidateObject(aRegion, false);
                        break;
                    case DESTROY_OPERATION:
                        destroyObject(aRegion, false);
                        break;
                    case UPDATE_OPERATION:
                        updateObject(aRegion);
                        break;
                    case READ_OPERATION:
                        readObject(aRegion);
                        break;
                    case LOCAL_INVALIDATE_OPERATION:
                        invalidateObject(aRegion, true);
                        break;
                    case LOCAL_DESTROY_OPERATION:
                        destroyObject(aRegion, true);
                        break;
                    case PUT_IF_ABSENT_OPERATION:
                        putIfAbsent(aRegion, true);
                        break;
                    case REMOVE_OPERATION:
                        remove(aRegion);
                        break;
                    case REPLACE_OPERATION:
                        replace(aRegion);
                        break;
                    case PUTALL_OPERATION:
                        updateObjects(aRegion);
                        break;
                    default: {
                        throw new TestException("Unknown operation " + whichOp);
                    }
                }
            } finally {
                if (haveALock) {
                    haveALock = false;
                    distLockService.unlock(LOCK_NAME);
                    Log.getLogWriter().info("Released distributed lock " + LOCK_NAME);
                }
            }
        } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
        
        // In the transactional tests, EventCounters are verified AFTER commit!
        if (useTransactions) {
            // Note we only support rollback for concurrent execution
            // serialExecution is dependent on counters being incremented for each operation
            int n = 0;
            int commitPercentage = EventPrms.getCommitPercentage();
            if (!isSerialExecution) {
                n = TestConfig.tab().getRandGen().nextInt(1, 100);
            }
            
            if (n <= commitPercentage) {
                try {
                    TxHelper.commit();
                } catch (ConflictException e) {
                    // We don't expect any conflicts in serialExecution mode, but these
                    // may occur in concurrent tests
                    if (isSerialExecution) {
                        throw new TestException("Unexpected conflict Exception " + TestHelper.getStackTrace(e));
                    } else {
                        Log.getLogWriter().info("ConflictException " + e + " expected, continuing test");
                    }
                }
            } else {
                TxHelper.rollback();
            }
            
            // We can't verify event counters in concurrent mode as CommitConflicts
            // will prevent events from being distributed (remote VMs never see).
            if (isSerialExecution) {
                checkEventCounters();
            }
        }
    }
    
    protected void addObject(Region aRegion, boolean logAddition) {
        String name = NameFactory.getNextPositiveObjectName();
        Object anObj = getObjectToAdd(name);
        String callback = createCallbackPrefix + ProcessMgr.getProcessId() + memberIdString + DistributedSystemHelper.getDistributedSystem().getDistributedMember();
        if (logAddition)
            Log.getLogWriter().info("addObject: calling put for name " + name + ", object " +
                    TestHelper.toString(anObj) + " callback is " + callback + ", region is " + aRegion.getFullPath());
        try {
            aRegion.put(name, anObj, callback);
        } catch (RegionDestroyedException e) {
            handleRegionDestroyedException(aRegion, e);
        } catch (RegionRoleException e) {
            throw e;
        } catch (PartitionedRegionStorageException e) {
            if (isSerialExecution) {
               throw new TestException(TestHelper.getStackTrace(e));
            } else {
               Log.getLogWriter().info("Caught expected exception " + e.getMessage() + ";continuing test"); 
            }
        } catch (Exception e) {
            throw new TestException(TestHelper.getStackTrace(e));
        }
        long numPut = EventBB.incrementCounter("EventBB.NUM_CREATE", EventBB.NUM_CREATE);
    }

    /** 
     *  ConcurrentMap API testing
     *  First round ... just do a create (as before)
     */
    protected void putIfAbsent(Region aRegion, boolean logAddition) {
        String name = null;
    
        // Expect success most of the time (put a new entry into the cache)
        boolean expectSuccess = true;
        int randInt = TestConfig.tab().getRandGen().nextInt(1, 100);
        if (randInt <= 25) {
           Set aSet = aRegion.keySet();
           if (aSet.size() > 0) {
             Iterator it = aSet.iterator();
             if (it.hasNext()) {
                 name = (String)it.next();
                 expectSuccess = false;
             } 
          }
        }

        if (name == null) {
           name = NameFactory.getNextPositiveObjectName();
        }
        Object anObj = getObjectToAdd(name);

        if (logAddition) {
            Log.getLogWriter().info("putIfAbsent: calling putIfAbsent for name " + name + ", object " + TestHelper.toString(anObj) + ", region is " + aRegion.getFullPath() + ".  ExpectSuccess = " + expectSuccess);
        }

        Object prevVal = null;
        try {
            prevVal = aRegion.putIfAbsent(name, anObj);
        } catch (RegionDestroyedException e) {
            handleRegionDestroyedException(aRegion, e);
        } catch (RegionRoleException e) {
            throw e;
        } catch (PartitionedRegionStorageException e) {
            if (isSerialExecution) {
               throw new TestException(TestHelper.getStackTrace(e));
            } else {
               Log.getLogWriter().info("Caught expected exception " + e.getMessage() + ";continuing test"); 
            }
        } catch (Exception e) {
            throw new TestException(TestHelper.getStackTrace(e));
        }

        // If we called putIfAbsent with a new entry, prevVal should be null
        if (expectSuccess) {  // new entry, prevVal should be null
           if (isCarefulValidation) {
              if (prevVal != null) {
                 throw new TestException(TestHelper.getStackTrace());
              }
           }
        } else { // expect failure, prevVal should not be null
           if (isCarefulValidation) {
              if (prevVal == null) {
                 throw new TestException(TestHelper.getStackTrace());
              }
           }
        }

        if (prevVal==null) {
           long numPut = EventBB.incrementCounter("EventBB.NUM_CREATE", EventBB.NUM_CREATE);
        }
    }
    
    protected void invalidateObject(Region aRegion, boolean isLocalInvalidate) {
        Set aSet = aRegion.keys();
        if (aSet.size() == 0) {
            Log.getLogWriter().info("invalidateObject: No names in region");
            return;
        }
        Iterator it = aSet.iterator();
        Object name = null;
        if (it.hasNext()) {
            name = it.next();
        } else { // has been destroyed cannot continue
            Log.getLogWriter().info("invalidateObject: Unable to get name from region");
            return;
        }
        boolean containsValue = aRegion.containsValueForKey(name);
        boolean alreadyInvalidated = !containsValue;
        Log.getLogWriter().info("containsValue for " + name + ": " + containsValue);
        Log.getLogWriter().info("alreadyInvalidated for " + name + ": " + alreadyInvalidated);
        try {
            String callback = invalidateCallbackPrefix + ProcessMgr.getProcessId() + memberIdString + DistributedSystemHelper.getDistributedSystem().getDistributedMember();
            if (isLocalInvalidate) {
                Log.getLogWriter().info("invalidateObject: local invalidate for " + name + " callback is " + callback);
                aRegion.localInvalidate(name, callback);
                Log.getLogWriter().info("invalidateObject: done with local invalidate for " + name);
                if (!alreadyInvalidated) {
                    long numInvalidate = EventBB.incrementCounter("EventBB.NUM_LOCAL_INVALIDATE", EventBB.NUM_LOCAL_INVALIDATE);
                }
            } else {
                Log.getLogWriter().info("invalidateObject: invalidating name " + name + " callback is " + callback);
                aRegion.invalidate(name, callback);
                Log.getLogWriter().info("invalidateObject: done invalidating name " + name);
                if (!alreadyInvalidated) {
                    long numInvalidate = EventBB.incrementCounter("EventBB.NUM_INVALIDATE", EventBB.NUM_INVALIDATE);
                }
            }
            if (isCarefulValidation)
                verifyObjectInvalidated(aRegion, name);
        } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
            if (isCarefulValidation)
                throw new TestException(TestHelper.getStackTrace(e));
            else {
                Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
                return;
            }
        }
        //   catch (CacheException e) {
        //      throw new TestException(TestHelper.getStackTrace(e));
        //   }
    }
    
    protected void destroyObject(Region aRegion, boolean isLocalDestroy) {
        Set aSet = aRegion.keys();
        Iterator iter = aSet.iterator();
        if (!iter.hasNext()) {
            Log.getLogWriter().info("destroyObject: No names in region");
            return;
        }
        try {
            Object name = iter.next();
            destroyObject(aRegion, name, isLocalDestroy);
        } catch (NoSuchElementException e) {
            throw new TestException("Bug 30171 detected: " + TestHelper.getStackTrace(e));
        }
    }
    
    private void destroyObject(Region aRegion, Object name, boolean isLocalDestroy) {
        try {
            String callback = destroyCallbackPrefix + ProcessMgr.getProcessId() + memberIdString + DistributedSystemHelper.getDistributedSystem().getDistributedMember();
            if (isLocalDestroy) {
                Log.getLogWriter().info("destroyObject: local destroy for " + name + " callback is " + callback);
                aRegion.localDestroy(name, callback);
                Log.getLogWriter().info("destroyObject: done with local destroy for " + name);
                long numDestroy = EventBB.incrementCounter("EventBB.NUM_LOCAL_DESTROY", EventBB.NUM_LOCAL_DESTROY);
            } else {
                Log.getLogWriter().info("destroyObject: destroying name " + name + " callback is " + callback);
                aRegion.destroy(name, callback);
                Log.getLogWriter().info("destroyObject: done destroying name " + name);
                long numDestroy = EventBB.incrementCounter("EventBB.NUM_DESTROY", EventBB.NUM_DESTROY);
            }
        } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
            if (isCarefulValidation)
                throw new TestException(TestHelper.getStackTrace(e));
            else {
                Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
                return;
            }
        }
        //   catch (CacheException e) {
        //      throw new TestException(TestHelper.getStackTrace(e));
        //   }
    }

    /**
     *  ConcurrentMap API testing
    **/  
    protected void remove(Region aRegion) {
        Set aSet = aRegion.keys();
        Iterator iter = aSet.iterator();
        if (!iter.hasNext()) {
            Log.getLogWriter().info("remove: No names in region");
            return;
        }
        try {
            Object name = iter.next();
            Object oldVal = aRegion.get(name);
            if (oldVal == null) {
              if (isCarefulValidation) {
                throw new TestException(TestHelper.getStackTrace());
              } else {
                 Log.getLogWriter().info("remove: oldVal is null, expected with concurrentExecution, continuing test ...");
              }
            }
            remove(aRegion, name, oldVal);
        } catch (NoSuchElementException e) {
            throw new TestException("Bug 30171 detected: " + TestHelper.getStackTrace(e));
        }
    }
    
    private void remove(Region aRegion, Object name, Object oldVal) {

        boolean expectSuccess = true;
        boolean removed;
        try {
 
           // Force the condition to not be met (small percentage of the time)
           Object conditionValue = oldVal;
           BaseValueHolder diffVal = null;
           int randInt = TestConfig.tab().getRandGen().nextInt(1, 100);
           if ((oldVal instanceof BaseValueHolder) && (randInt <= 25)) {
             int modVal = 0; 
             diffVal = (BaseValueHolder)getUpdateObject((String)name);
             if (diffVal.getModVal() != null) {
               modVal = diffVal.getModVal() + 1;
             }
             diffVal.modVal = new Integer(modVal);
             conditionValue = diffVal;
             expectSuccess = false;
             Log.getLogWriter().info("remove: causing remove condition to fail by overriding " + name + " with previous value " + oldVal + " with " + diffVal + ".  ExpectSuccess = " + expectSuccess);
             if (oldVal.equals(diffVal)) {
               throw new TestException("Test issue: oldVal " + oldVal + " is expected to be different from " + diffVal + ", but .equals() was true");
             }
           } 
           Log.getLogWriter().info("remove: removing " + name + " with conditionValue " + conditionValue + ".  ExpectSuccess = " + expectSuccess);

           removed = aRegion.remove(name, conditionValue);
           Log.getLogWriter().info("remove: done removing " + name + " removed = " + removed);
           if (removed) {
              long numDestroy = EventBB.incrementCounter("EventBB.NUM_DESTROY", EventBB.NUM_DESTROY);
           }
        } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
            if (isCarefulValidation)
                throw new TestException(TestHelper.getStackTrace(e));
            else {
                Log.getLogWriter().info("Caught " + e + " (expected with concurrent execution); continuing with test");
                return;
            }
        }

        if (isCarefulValidation) {
           if (expectSuccess) {
              if (!removed) throw new TestException(TestHelper.getStackTrace());
           } else {  // expect failure
              if (removed) throw new TestException(TestHelper.getStackTrace());
           }
        }
    }
    
    /**
     * Updates the "first" entry in a given region
     */
    protected void updateObject(Region aRegion) {
        Set aSet = aRegion.keys();
        Iterator iter = aSet.iterator();
        if (!iter.hasNext()) {
            Log.getLogWriter().info("updateObject: No names in region");
            return;
        }
        Object name = iter.next();
        updateObject(aRegion, name);
    }
    
    /**
     * Updates the entry with the given key (<code>name</code>) in the
     * given region.
     */
    protected void updateObject(Region aRegion, Object name) {
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
            String callback = updateCallbackPrefix + ProcessMgr.getProcessId() + memberIdString + DistributedSystemHelper.getDistributedSystem().getDistributedMember();
            Log.getLogWriter().info("updateObject: replacing name " + name + " with " +
                    TestHelper.toString(newObj) + "; old value is " + TestHelper.toString(anObj) +
                    ", callback is " + callback);
            aRegion.put(name, newObj, callback);
            Log.getLogWriter().info("Done with call to put (update)");
        } catch (RegionRoleException e) {
            throw e;
        } catch (Exception e) {
            throw new TestException(TestHelper.getStackTrace(e));
        }
        
        long numUpdate = EventBB.incrementCounter("EventBB.NUM_UPDATE", EventBB.NUM_UPDATE);
    }
    
    /**
     * Updates the "first" few entries in a given region
     */
    protected void updateObjects(Region aRegion) {
      long numObjects = TestConfig.tab().longAt(EventPrms.numPutAllObjects, 20);
      updateObjects(aRegion, numObjects);
    }
    
    /**
     * Update specified number of objects in the region.
     * Method picks up "first" numOjects in the region. 
     * @param aRegion
     * @param numObjects
     */
    protected void updateObjects(Region aRegion, long numObjects) {
      Set aSet = aRegion.keySet();
      long keysToAdd = -1;
      if (aSet == null || aRegion.isEmpty()) {
        keysToAdd = numObjects;
      } else if (aSet.size() < numObjects) {
        keysToAdd = numObjects - aSet.size();
      }
      for (int i = 0; i < keysToAdd; i++) {
        addObject(aRegion, true);
      }
      Map aMap = new HashMap();
      Iterator iter = aSet.iterator();
      for (int i = 0; i < numObjects; i++) {
        Object key = iter.next();
        aMap.put(key,  getUpdateObject((String)key));
      }
      updateAllObjects(aRegion, aMap);
    }
    
    /**
     * Updates all the objects specified in aMap in the given region.
     */
    protected void updateAllObjects(Region aRegion, Map aMap) {
        try {
            Log.getLogWriter().info("Calling putAll (updateAll) with an Object map of size: " + aMap.size());
            aRegion.putAll(aMap);
            Log.getLogWriter().info("Done with call to putAll (updateAll) for " + aMap.size() + "objects");
        } catch (RegionRoleException e) {
            throw e;
        } catch (Exception e) {
            throw new TestException(TestHelper.getStackTrace(e));
        }
        long numUpdate = EventBB.add("EventBB.NUM_UPDATE", EventBB.NUM_UPDATE, aMap.size());
    }
    
    /**
     * Updates the "first" entry in a given region
     */
    protected void replace(Region aRegion) {
        Set aSet = aRegion.keys();
        Iterator iter = aSet.iterator();
        if (!iter.hasNext()) {
            Log.getLogWriter().info("replace: No names in region");
            return;
        }
        Object name = iter.next();
        replace(aRegion, name);
    }
    
    /**
     * Updates the entry with the given key (<code>name</code>) in the
     * given region.
     */
    protected void replace(Region aRegion, Object name) {
        Object anObj = null;
        Object prevVal = null;
        boolean replaced = false;
        try {
            anObj = aRegion.get(name);
        } catch (CacheLoaderException e) {
            throw new TestException(TestHelper.getStackTrace(e));
        } catch (TimeoutException e) {
            throw new TestException(TestHelper.getStackTrace(e));
        }
        if (anObj == null) {
           if (isCarefulValidation) {
             throw new TestException(TestHelper.getStackTrace());
           } else {
             Log.getLogWriter().info("replace: currentValue is null, expected in concurrent tests, continuing ...");
             return;
           }
        }
        Object newObj = getUpdateObject((String)name);
        boolean expectSuccess = true;
        try {
            // 1/2 of the time use oldVal => newVal method
            int randInt = TestConfig.tab().getRandGen().nextInt(0, 100);
            if (randInt < 50) {

               // Force the condition to not be met (small percentage of the time)
               // get a new oldVal to cause this
               if (randInt <= 10) {
                 anObj = getUpdateObject((String)name);
                 expectSuccess = false;
               }

               Log.getLogWriter().info("replace: replacing name " + name + " with " + TestHelper.toString(newObj) + "; old value is " + TestHelper.toString(anObj) + ".  ExpectSuccess = " + expectSuccess);
                replaced = aRegion.replace(name, anObj, newObj);
            } else {

               // Force the condition to not be met (small percentage of the time)
               // use a new key for this
               if (randInt <= 10) {
                 name = NameFactory.getNextPositiveObjectName();
                 expectSuccess = false;
               }
               Log.getLogWriter().info("replace: replacing name " + name + " with " + TestHelper.toString(newObj) + ".  ExpectSuccess = " + expectSuccess);
                prevVal = aRegion.replace(name, newObj);

               if (prevVal != null) replaced = true;
            }
            Log.getLogWriter().info("Done with call to replace");
        } catch (RegionRoleException e) {
            throw e;
        } catch (Exception e) {
            throw new TestException(TestHelper.getStackTrace(e));
        }
        
        if (replaced) {
           long numUpdate = EventBB.incrementCounter("EventBB.NUM_UPDATE", EventBB.NUM_UPDATE);
        }
    }
    

    /**
     * Fetches (reads) the value of a randomly selected entry in the
     * given region.
     */
    protected void readObject(Region aRegion) {
        Set aSet = aRegion.keys();
        if (aSet.size() == 0) {
            Log.getLogWriter().info("readObject: No names in region");
            return;
        }
        long maxNames = NameFactory.getPositiveNameCounter();
        if (maxNames <= 0) {
            Log.getLogWriter().info("readObject: max positive name counter is " + maxNames);
            return;
        }
        Object name = NameFactory.getObjectNameForCounter(TestConfig.tab().getRandGen().nextInt(1, (int)maxNames));
        Log.getLogWriter().info("readObject: getting name " + name);
        try {
            Object anObj = aRegion.get(name);
            Log.getLogWriter().info("readObject: got value for name " + name + ": " + TestHelper.toString(anObj));
        } catch (CacheLoaderException e) {
            throw new TestException(TestHelper.getStackTrace(e));
        } catch (TimeoutException e) {
            throw new TestException(TestHelper.getStackTrace(e));
        }
    }
    
    // ========================================================================
    // methods to create/invalidate/destroy regions
    
    /**
     * @see #HydraTask_doRegionOperations
     */
    protected void doRegionOperations() {
        long startTime = System.currentTimeMillis();
        if (isSerialExecution)
            logExecutionNumber();
        TestHelper.checkForEventError(EventBB.getBB());
        
        if (useTransactions) {
            TxHelper.begin();
        }
        
        do {
            
            // Check for max number of regions
            // In C tests, when all regions (other than root) have been destroyed,
            // the test is over. C region tests start with a fully populated region
            // forest, including subregions.
            long numRegions = getNumNonRootRegions();
            int whichOp = getOperation(EventPrms.regionOperations, false);
            if (numRegions == 0)  // no regions other than the roots; add another
                whichOp = ADD_OPERATION;
            else if (numRegions >= maxRegions)
                whichOp = DESTROY_OPERATION;
            
            switch (whichOp) {
                case ADD_OPERATION:
                    addRegion();
                    break;
                case DESTROY_OPERATION:
                    destroyRegion(false);
                    break;
                case INVALIDATE_OPERATION:
                    invalidateRegion(false);
                    break;
                case LOCAL_DESTROY_OPERATION:
                    destroyRegion(true);
                    break;
                case LOCAL_INVALIDATE_OPERATION:
                    invalidateRegion(true);
                    break;
                case REGION_CLOSE_OPERATION:
                    closeRegion();
                    break;
                case CLEAR_OPERATION:
                    clearRegion();
                    break;
                default: {
                    throw new TestException("Unknown operation " + whichOp);
                }
            }
        } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
        
        // In the transactional tests, EventCounters are verified AFTER commit!
        if (useTransactions) {
            // Note we only support rollback for concurrent execution
            // serialExecution is dependent on counters being incremented for each operation
            int n = 0;
            int commitPercentage = EventPrms.getCommitPercentage();
            if (!isSerialExecution) {
                n = TestConfig.tab().getRandGen().nextInt(1, 100);
            }
            if (n <= commitPercentage) {
                try {
                    TxHelper.commit();
                } catch (ConflictException e) {
                    // We don't expect any conflicts in serialExecution mode, but these
                    // may occur in concurrent tests
                    if (isSerialExecution) {
                        throw new TestException("Unexpected conflict Exception " + TestHelper.getStackTrace(e));
                    } else {
                        Log.getLogWriter().info("ConflictException " + e + " expected, continuing test");
                    }
                }
            } else {
                TxHelper.rollback();
            }
            
            // We can't verify event counters in concurrent mode as CommitConflicts
            // will prevent events from being distributed (remote VMs never see).
            if (isSerialExecution) {
                checkEventCounters();
            }
        }
        
        EventBB.getBB().printSharedCounters();
        NameBB.getBB().printSharedCounters();
    }
    
    /**
     * Creates a new subregion of a randomly-selected parent region
     * (which could be the root region).  The child region will have the
     * same (or equivalent) region attributes as its parent.
     */
    protected void addRegion() {
        addRegion( getRandomRegion(true) );
    }

    /**
     *  addRegion - adds a subregion beneath the given region
     *
     *  @see addRegion()
     *
     *  @param parentRegion - add a subregion to the region provided
     */
    protected void addRegion(Region parentRegion) {
        // Create a new region
        boolean parentIsPartitioned = false;
        Region newRegion = null;

        String regionName = NameFactory.getNextRegionName();
        try {
            RegionAttributes parentAttr = parentRegion.getAttributes();
            AttributesFactory factory = new AttributesFactory(parentAttr);
            factory.initCacheListeners(parentAttr.getCacheListeners());
            parentIsPartitioned = (parentAttr.getPartitionAttributes() != null);
            PartitionAttributes prAttr = null;
            if (TestConfig.tab().getRandGen().nextInt(1, 100) <= 30) {
              Object[] tmp = getPartitionAttributes();
              if (tmp != null) {
                DataPolicy dp = (DataPolicy)tmp[0];
                prAttr = (PartitionAttributes)tmp[1];
                Log.getLogWriter().info("Setting dataPolicy " + dp + " in factory " + factory + " that has attributes " + factory.create());
                if (!(parentAttr.getDataPolicy().equals(dp))) { // need to change the datapolicy
                  factory.setDataPolicy(DataPolicy.DEFAULT);// workaround to fix bug 46984 in gemfirexd only (not gemfire) since
                  // gemfirexd does not expose these apis to customers; this fools the setDataPolicy() method by first changing
                  // to default, then the next line sets it to what we really want!
                  factory.setDataPolicy(dp);
                }
              }
              factory.setEntryIdleTimeout(new ExpirationAttributes(0));
              factory.setEntryTimeToLive(new ExpirationAttributes(0));
            }
            factory.setPartitionAttributes(prAttr);
            Log.getLogWriter().info("Creating attributes from " + TestHelper.regionAttributesToString(parentAttr) + " and partition attributes " + prAttr);
            RegionAttributes regAttr = factory.createRegionAttributes();
            if (prAttr != null) // this region is partitioned
               regionName = PR_NAME_PREFIX + regionName;
            Log.getLogWriter().info("Creating a new subregion of: " + TestHelper.regionToString(parentRegion, false) + " with name " + regionName);
            newRegion = parentRegion.createSubregion(regionName, regAttr);
            Log.getLogWriter().info("Created new region: " + TestHelper.regionToString(newRegion, true));
            if (parentIsPartitioned) {
               throw new TestException("Successfully created a subregion of " +
                     TestHelper.regionToString(parentRegion, true) + 
                     ", but subregions of partitioned regions are not supported. Subregion created is " +
                     TestHelper.regionToString(newRegion, true));
            }
            String parentFullPath = parentRegion.getFullPath();
            String subRegionFullPath = newRegion.getFullPath();
            if (subRegionFullPath.indexOf(parentFullPath) < 0) {
               throw new TestException("getFullPath() of new subregion " + subRegionFullPath + " does not include its parent name " + parentFullPath);
            }
        } catch (RegionDestroyedException e) { // the parent region was destroyed
            handleRegionDestroyedException(parentRegion, e);
            return; // if the above call returns, all is well
        } catch (CacheException e) {
            throw new TestException(TestHelper.getStackTrace(e));
        } catch (UnsupportedOperationException e) {
            if (parentIsPartitioned) { // this is ok; we cannot create a subregion of a partitioned region
               Log.getLogWriter().info("Caught " + e + "; expected exception, continuing test");
               return;
            } else {
               throw new TestException(TestHelper.getStackTrace(e));
            }
        }
        
        // Create objects in the new region
        int initRegionNumObjects = TestConfig.tab().intAt(EventPrms.initRegionNumObjects);
        for (int i = 1; i <= initRegionNumObjects; i++) {
            addObject(newRegion, false);
        }
        Log.getLogWriter().info("Added " + initRegionNumObjects + " to " + TestHelper.regionToString(newRegion, false));
        if (isCarefulValidation) {
          EventBB.incrementCounter("EventBB.NUM_REGION_CREATE", EventBB.NUM_REGION_CREATE);
        }
    }
    
    /** Invalidate a random region.
     *
     *  @param isLocalInvalidate true if the invalidate should be a local invalidate, false otherwise.
     *
     *  @returns The total number of regions invalidated (counting subregions of the random region invalidated)
     */
    protected int invalidateRegion(boolean isLocalInvalidate) {
        // get a random region to invalidate
        int numRegions = invalidateRegion( isLocalInvalidate, getRandomRegion(false) );
        return numRegions;
    }

    /** Invalidate a random region.
     *
     *  @param aRegion - region to invalidate
     *  @param isLocalInvalidate true if the invalidate should be a local invalidate, false otherwise.
     *
     *  @returns The total number of regions invalidated (counting subregions of the random region invalidated)
     */
    protected int invalidateRegion(boolean isLocalInvalidate, Region aRegion) {
        if (aRegion == null) { // no regions exist
            Log.getLogWriter().info("invalidateRegion, not causing invalidate event because no regions exist other than roots");
            return 0;
        }
        Log.getLogWriter().info("In invalidateRegion, region is " + TestHelper.regionsToString(aRegion, false));
        String regionName = TestHelper.regionToString(aRegion, false);
        
        // invalidate the region
        int numRegions = 0;
        try {
            numRegions = aRegion.subregions(true).size() + 1;
        } catch (RegionDestroyedException e) {
            if (isCarefulValidation)
                throw new TestException("Unexpected " + TestHelper.getStackTrace(e));
            else
                Log.getLogWriter().info("Not invalidating " + aRegion.getFullPath() + ", got " +
                        e + " while getting number of subregions; continuing test");
        }
        long[] beforeCounter = new long[2];
        beforeCounter[0] = EventCountersBB.getBB().getSharedCounters().read(EventCountersBB.numAfterRegionInvalidateEvents_isNotExp);
        beforeCounter[1] = OperationCountersBB.getBB().getSharedCounters().read(OperationCountersBB.numAfterRegionInvalidateEvents_isNotExp);
        try {
            String callback = regionInvalidateCallbackPrefix + ProcessMgr.getProcessId() + memberIdString + DistributedSystemHelper.getDistributedSystem().getDistributedMember();
            if (isLocalInvalidate) {
                Log.getLogWriter().info("local invalidate for " + regionName + " callback is " + callback);
                aRegion.invalidateRegion(callback);
                Log.getLogWriter().info("Done with local invalidate for " + regionName);
                long counter = EventBB.add("EventBB.NUM_LOCAL_REGION_INVALIDATE", EventBB.NUM_LOCAL_REGION_INVALIDATE, numRegions);
            } else {
                Log.getLogWriter().info("Invalidating " + regionName + " callback is " + callback);
                aRegion.invalidateRegion(callback);
                Log.getLogWriter().info("Done invalidating " + regionName);
                long counter = EventBB.add("EventBB.NUM_REGION_INVALIDATE", EventBB.NUM_REGION_INVALIDATE, numRegions);
            }
        } catch (RegionDestroyedException e) {
            handleRegionDestroyedException(aRegion, e);
            return numRegions; // if the previous line returns, all is well
        } catch (Exception e) {
            throw new TestException(TestHelper.getStackTrace(e));
        }
        
        if (isCarefulValidation) {
            // see if the objects are invalidated in the regions just invalidated
            Set aSet = new HashSet(aRegion.subregions(true));
            aSet.add(aRegion);
            Iterator it = aSet.iterator();
            while (it.hasNext()) {
                Region thisRegion = (Region)it.next();
                Iterator it2 = thisRegion.keys().iterator();
                while (it2.hasNext()) {
                    Object key = it2.next();
                    verifyObjectInvalidated(thisRegion, key);
                }
            }
            
            if (useCounters || listenerInstalled) {
               long expectedCounter = beforeCounter[0] + (numRegions * getNumVMsWithListeners());
               TestHelper.waitForCounter(EventCountersBB.getBB(), "numAfterRegionInvalidateEvents_isNotExp", EventCountersBB.numAfterRegionInvalidateEvents_isNotExp, expectedCounter, true, MILLIS_TO_WAIT);

               expectedCounter = beforeCounter[1] + (numRegions * getNumVMsWithListeners());
               TestHelper.waitForCounter(OperationCountersBB.getBB(), "numAfterRegionInvalidateOperationss_isNotExp", OperationCountersBB.numAfterRegionInvalidateEvents_isNotExp, expectedCounter, true, MILLIS_TO_WAIT);

               checkEventCounters();
           }
        }
        return numRegions;
    }
    
    /**
     *  Close a random region.
     */
    protected int closeRegion() {
        // get a random region to close
        Region aRegion = getRandomRegion(false);
        if (aRegion == null) { // no regions exist
            Log.getLogWriter().info("closeRegion, not causing close event bcause no regions exist other than roots");
            return 0;
        }
        return closeRegion(aRegion);
    }
    
    
    protected int closeRegion(Region aRegion) {
        String regionName = TestHelper.regionToString(aRegion, false);
        Log.getLogWriter().info("In closeRegion, region is " + regionName);
        
        // close the region
        long[] beforeCounter = new long[2];
        beforeCounter[0] = EventCountersBB.getBB().getSharedCounters().read(EventCountersBB.numAfterRegionDestroyEvents_isNotExp);
        beforeCounter[1] = OperationCountersBB.getBB().getSharedCounters().read(OperationCountersBB.numAfterRegionDestroyEvents_isNotExp);
        Set regionSet = null;
        int numRegions = 0;
        if (isCarefulValidation) {
            regionSet = aRegion.subregions(true);
            numRegions = regionSet.size() + 1; // +1 to include aRegion
            Log.getLogWriter().info("numRegions being closed is " + numRegions + " " + TestHelper.regionsToString(aRegion, false));
        }
        try {
            Log.getLogWriter().info("Closing region " + regionName);
            aRegion.close();
            Log.getLogWriter().info("Done closing " + regionName);
            if (isCarefulValidation) {
                EventBB.add("EventBB.NUM_CLOSE", EventBB.NUM_CLOSE, numRegions);
            }
        } catch (RegionDestroyedException e) {
            handleRegionDestroyedException(aRegion, e);
            return numRegions; // if the previous line returns, all is well
        }
        
        StringBuffer errStr = new StringBuffer();
        if (isCarefulValidation) {
            // see if the regions/objects are destroyed
            Set newSet = new HashSet(regionSet);
            newSet.add(aRegion);
            Iterator it = regionSet.iterator();
            while (it.hasNext()) {
                Region thisRegion = (Region)it.next();
                boolean isDestroyed = thisRegion.isDestroyed();
                if (!isDestroyed)
                    errStr.append("Unexpected " + thisRegion + ".isDestroyed() returned " + isDestroyed);
                try {
                    Iterator it2 = thisRegion.keys().iterator();
                    errStr.append("Successfully was able to get keys() of destroyed region\n");
                } catch (RegionDestroyedException e) {
                    // expected
                }
            }
            if (useCounters) {
               checkEventCounters();
            }
        }
        if (errStr.length() > 0)
            throw new TestException(errStr.toString());
        return numRegions;
    }
    
    
    /** Destroys a random region.
     *
     *  @param isLocalDestroy true if the destroy should be a local destroy, false otherwise.
     *
     *  @returns The total number of regions destroyed (counting subregions of the random region destroyed)
     */
    protected int destroyRegion(boolean isLocalDestroy) {
        // get a random region to destroy
        Region aRegion = getRandomRegion(false);
        if (aRegion == null) { // no regions exist
            Log.getLogWriter().info("destroyRegion, not causing destroy event bcause no regions exist other than roots");
            return 0;
        }
        return destroyRegion(isLocalDestroy, aRegion);
    }
    
    protected int destroyRegion(boolean isLocalDestroy, Region aRegion) {
        String regionName = TestHelper.regionToString(aRegion, false);
        Log.getLogWriter().info("In destroyRegion, region is " + regionName);
        
        // destroy the region
        long[] beforeCounter = new long[3];
        beforeCounter[0] = EventCountersBB.getBB().getSharedCounters().read(EventCountersBB.numAfterRegionDestroyEvents_isNotExp);
        beforeCounter[1] = OperationCountersBB.getBB().getSharedCounters().read(OperationCountersBB.numAfterRegionDestroyEvents_isNotExp);
        beforeCounter[2] = WriterCountersBB.getBB().getSharedCounters().read(WriterCountersBB.numBeforeRegionDestroyEvents_isNotExp);
        Set regionSet = null;
        int numRegions = 0;
        if (isCarefulValidation) {
            regionSet = aRegion.subregions(true);
            numRegions = regionSet.size() + 1; // +1 to include aRegion
            Log.getLogWriter().info("numRegions being destroyed is " + numRegions + " " + TestHelper.regionsToString(aRegion, false));
        }
        try {
            String callback = regionDestroyCallbackPrefix + ProcessMgr.getProcessId() + memberIdString + DistributedSystemHelper.getDistributedSystem().getDistributedMember();
            if (isLocalDestroy) {
                Log.getLogWriter().info("local destroy for region " + regionName + " callback is " + callback);
                aRegion.localDestroyRegion(callback);
                Log.getLogWriter().info("Done with local destroy for " + regionName);
                if (isCarefulValidation) {
                    EventBB.add("EventBB.NUM_LOCAL_REGION_DESTROY", EventBB.NUM_LOCAL_REGION_DESTROY, numRegions);
                    EventBB.add("EventBB.NUM_CLOSE", EventBB.NUM_CLOSE, numRegions);
                }
            } else {
                Log.getLogWriter().info("destroying region " + regionName + " callback is " + callback);
                aRegion.destroyRegion(callback);
                Log.getLogWriter().info("Done destroying " + regionName);
                if (isCarefulValidation) {
                    EventBB.add("EventBB.NUM_REGION_DESTROY", EventBB.NUM_REGION_DESTROY, numRegions);
                    EventBB.add("EventBB.NUM_CLOSE", EventBB.NUM_CLOSE, numRegions);
                }
            }
        } catch (RegionDestroyedException e) {
            handleRegionDestroyedException(aRegion, e);
            return numRegions; // if the previous line returns, all is well
        } catch (Exception e) {
            throw new TestException(TestHelper.getStackTrace(e));
        }
        
        StringBuffer errStr = new StringBuffer();
        if (isCarefulValidation) {
            // see if the regions/objects are destroyed
            Set newSet = new HashSet(regionSet);
            newSet.add(aRegion);
            Iterator it = regionSet.iterator();
            while (it.hasNext()) {
                Region thisRegion = (Region)it.next();
                boolean isDestroyed = thisRegion.isDestroyed();
                if (!isDestroyed)
                    errStr.append("Unexpected " + thisRegion + ".isDestroyed() returned " + isDestroyed);
                try {
                    Iterator it2 = thisRegion.keys().iterator();
                    errStr.append("Successfully was able to get keys() of destroyed region\n");
                } catch (RegionDestroyedException e) {
                    // expected
                }
            }

            if (useCounters || listenerInstalled) {
               long expectedCounter = beforeCounter[0] + (numRegions * getNumVMsWithListeners());
               TestHelper.waitForCounter(EventCountersBB.getBB(), "numAfterRegionDestroyEvents_isNotExp", EventCountersBB.numAfterRegionDestroyEvents_isNotExp, expectedCounter, true, MILLIS_TO_WAIT);
   
               expectedCounter = beforeCounter[1] + (numRegions * getNumVMsWithListeners());
               TestHelper.waitForCounter(OperationCountersBB.getBB(), "numAfterRegionDestroyEvents_isNotExp", OperationCountersBB.numAfterRegionDestroyEvents_isNotExp, expectedCounter, true, MILLIS_TO_WAIT);

               // CacheWriter (only one CacheWriter invoked, local preferred
               expectedCounter = beforeCounter[2] + numRegions;
               TestHelper.waitForCounter(WriterCountersBB.getBB(), "numBeforeRegionDestroyEvents_isNotExp", WriterCountersBB.numBeforeRegionDestroyEvents_isNotExp, expectedCounter, true, MILLIS_TO_WAIT);
               checkEventCounters();
            }
        }
        if (errStr.length() > 0)
            throw new TestException(errStr.toString());
        return numRegions;
    }
       // Adding method for testing region.clear()
    protected int clearRegion() {
        Region aRegion = getRandomRegion(false);
        //check if Region is null or destroyed
         if (aRegion == null) { // no regions exist
            Log.getLogWriter().info("clearRegion, not causing clear event because no regions exist other than roots");
            return 0;
        }
        int numRegions = clearRegion(aRegion);
        return numRegions;
    }

    /**
     *  Perform clear on targeted region, verify all VMs receive clearRegion event
     *
     *  @param aRegion - targeted region
     *
     *  @returns The total number of regions cleared (counting subregions 
     *  of the random region cleared)
     */
    protected int clearRegion(Region aRegion) {

        String regionName = TestHelper.regionToString(aRegion, false);
        Log.getLogWriter().info("In clearRegion, region is " + regionName);
        
        // clear the region
        long[] beforeCounter = new long[3];
        beforeCounter[0] = EventCountersBB.getBB().getSharedCounters().read(EventCountersBB.numAfterClearEvents_isNotExp);
        beforeCounter[1] = OperationCountersBB.getBB().getSharedCounters().read(OperationCountersBB.numAfterClearEvents_isNotExp);
        beforeCounter[2] = WriterCountersBB.getBB().getSharedCounters().read(WriterCountersBB.numBeforeRegionClearEvents_isNotExp);
//        Set regionSet = null;
        int numRegions = 0;
        if (isCarefulValidation) {
          //regionSet = aRegion.subregions(true);
          //  numRegions = regionSet.size() + 1; // +1 to include aRegion
          //  Log.getLogWriter().info("numRegions being cleared is " + numRegions + " " + TestHelper.regionsToString(aRegion, false));
        }
        try{
            Log.getLogWriter().info("clearing region " + regionName);
           ((Map)aRegion).clear();
            Log.getLogWriter().info("Done clearing " + regionName);
             long counter = EventBB.add("EventBB.NUM_CLEAR", EventBB.NUM_CLEAR, numRegions);
            }
        catch (RegionDestroyedException e) {
            handleRegionDestroyedException(aRegion, e);
            //change to return numregions.
             return numRegions;
        }catch (Exception e) {
            throw new TestException(TestHelper.getStackTrace(e));
        }
        if (isCarefulValidation) {
          //Set entrySet = aRegion.entrySet();
          if(!aRegion.isEmpty()) {
              throw new TestException("The region was not found empty after clear");
          }

           if (useCounters || listenerInstalled) {
              long expectedCounter = beforeCounter[0] + (numRegions * getNumVMsWithListeners());
               TestHelper.waitForCounter(EventCountersBB.getBB(), "numAfterClearEvents_isNotExp", EventCountersBB.numAfterClearEvents_isNotExp, expectedCounter, true, MILLIS_TO_WAIT);

              expectedCounter = beforeCounter[1] + (numRegions * getNumVMsWithListeners());
              TestHelper.waitForCounter(OperationCountersBB.getBB(), "numAfterClearEvents_isNotExp", OperationCountersBB.numAfterClearEvents_isNotExp, expectedCounter, true, MILLIS_TO_WAIT);

              // CacheWriters (one invoked, local writer preferred)
              expectedCounter = beforeCounter[2] + numRegions;
              TestHelper.waitForCounter(WriterCountersBB.getBB(), "numBeforeClearEvents_isNotExp", WriterCountersBB.numBeforeRegionClearEvents_isNotExp, expectedCounter, true, MILLIS_TO_WAIT);
              checkEventCounters();
            }
        }
        return numRegions;
    }
    
    /** Random choose a region and add an object to it for the duration
     *  of minTaskGranularity hydra param
     */
    protected void addToRegion() {
        long startTime = System.currentTimeMillis();
        if (isSerialExecution)
            logExecutionNumber();
        TestHelper.checkForEventError(EventBB.getBB());
        
        Log.getLogWriter().info("Faulting in available regions...");
        faultInNewRegions();
        Log.getLogWriter().info("Done faulting in available regions.");
        do {
            Region aRegion = getRandomRegion(false);
            if (aRegion != null)
                addObject(aRegion, true);
        } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
    }
    
    // ========================================================================
    // methods to fault in regions
    
    /**
     * Find regions created during the test (in any cache) using the
     * {@link EventBB} and create those regions in this cache also. By
     * doing this, a cache which did not originally create a region will
     * be able to invalidate or destroy it.
     *
     *  @return The number of regions created
     */
    protected int faultInNewRegions() {
        int numCreated = 0;
        StringBuffer logStr = new StringBuffer();
        long startTime = System.currentTimeMillis();
        Region rootRegion = CacheUtil.getCache().getRegion(regionName);
        Log.getLogWriter().info("Before faulting in: " + TestHelper.regionsToString(rootRegion, false));
        RegionAttributes attr = rootRegion.getAttributes();
        List aList = (List)(EventBB.getBB().getSharedMap()).get(EventBB.CURRENT_REGION_NAMES);
        for (int i = 0; i < aList.size(); i++) { // iterate through the latest list of regions defined elsewhere
            String currRegionName = (String)aList.get(i);
            Region previousRegion = null;
            StringTokenizer st = new StringTokenizer(currRegionName, "/", false);
            while (st.hasMoreTokens()) {
                String singleRegionName = st.nextToken();
                if (singleRegionName.equals(regionName)) {
                    previousRegion = rootRegion;
                } else {
                    Object[] tmp = faultInRegion(previousRegion, singleRegionName, attr);
                    previousRegion = (Region)tmp[0];
                    logStr.append(tmp[1]);
                    boolean created = ((Boolean)tmp[2]).booleanValue();
                    if (created) numCreated++;
                    if (previousRegion == null)
                        break;
                }
            }
        }
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        EventBB.getBB().getSharedCounters().setIfLarger(EventBB.MAX_FAULT_IN_REGIONS_MILLIS, duration);
        Log.getLogWriter().info("Done faulting in new regions, " + logStr.toString());
        Log.getLogWriter().info("Done faulting in new regions, elapsed time " +
                TestHelper.millisToString(duration) + ", all regions: " +
                TestHelper.regionsToString(rootRegion, false));
        return numCreated;
    }
    
/** Create a region in this cache, which was previously created in another cache.
 *  Allow that the region to create may already exist or may have been destroyed.
 *
 *  @param parentRegion The parent region of the region to create.
 *  @param newRegionName The new of the new subregion of parentRegion.
 *  @param attr The region attributes for the new region.
 *
 *  @returns An array containing
 *           [0] a region with name newRegionName; this region was either newly created
 *               or already existing; may be null if the region was destroyed
 *           [1] a String logging what this method did
 *           [2] a boolean indicating if a new region was created (true), or if the
 *               region already existed (false)
 *
*/
protected Object[] faultInRegion(Region parentRegion, String newRegionName, RegionAttributes attr) {
   boolean verbose = false;
//        StringBuffer logStr = new StringBuffer();
   try {
      if (verbose) {
         String s = "In faultInRegion: Attempting to get or create " + newRegionName + " as a subregion of " +
                    TestHelper.regionToString(parentRegion, false);
         Log.getLogWriter().info(s);
      }
      Region newRegion = CacheUtil.getCache().getRegion(parentRegion.getFullPath() + "/" + newRegionName);
      if (newRegion == null) { // create the region
         boolean newRegionIsPartitioned = newRegionName.startsWith(PR_NAME_PREFIX);
         if (newRegionIsPartitioned) {
            AttributesFactory factory = new AttributesFactory(attr);
            Object[] tmp = getPartitionAttributes();
            DataPolicy dp = (DataPolicy)(tmp[0]);
            PartitionAttributes prAttr = (PartitionAttributes)(tmp[1]);
            factory.setDataPolicy(dp);
            factory.setPartitionAttributes(prAttr);
            factory.setEntryIdleTimeout(new ExpirationAttributes(0));
            factory.setEntryTimeToLive(new ExpirationAttributes(0));
            attr = factory.createRegionAttributes();
         }
         if (verbose) {
            Log.getLogWriter().info("In faultInRegion: Creating " + newRegionName + " with attributes " +
                TestHelper.regionAttributesToString(attr));
         }
         newRegion = parentRegion.createSubregion(newRegionName, attr);
         if (verbose) {
            Log.getLogWriter().info("In faultInRegion: Created " + TestHelper.regionToString(newRegion, false));
         }
      } else { // region already exists; we were able to get the region
         if (verbose) {
            Log.getLogWriter().info("In faultInRegion: Got " + TestHelper.regionToString(newRegion, false));
         }
      }
            
      return new Object[] {newRegion, "Faulted in new region " +
                 TestHelper.regionToString(newRegion, false) + "\n", new Boolean(true)};
                    
   } catch (RegionExistsException e) {
      if (verbose) {
         String s = "In faultInRegion: Region " + newRegionName + " already exists as a subregion of " +
                    TestHelper.regionToString(parentRegion, false) + "; continuing" ;
         Log.getLogWriter().info(s);
      }
            
      try {
         return new Object[] { parentRegion.getSubregion(newRegionName), "Region " +
                    newRegionName + " already exists\n", new Boolean(false) };
      } catch (RegionDestroyedException anExcept) {
         if (!isCarefulValidation) {
            if (verbose) {
               String s = "In faultInRegion: Region " + newRegionName + " has been destroyed ; continuing";
               Log.getLogWriter().info(s);
            }
            return new Object[] {null, "Region " + newRegionName + " has been destroyed\n", new Boolean(false)};
         }
         throw new TestException(TestHelper.getStackTrace(anExcept));
      }
  } catch (RegionDestroyedException e) {
     if (verbose) Log.getLogWriter().info("In faultInRegion: Region " + newRegionName + " has been destroyed; continuing");
     return new Object[] {null, "Region " + newRegionName + " has been destroyed\n", new Boolean(false)};
  } catch (TimeoutException e) {
     throw new TestException(TestHelper.getStackTrace(e));
  }
}
    
    // ========================================================================
    // methods that can be overridden for a more customized test
    
    /**
     * Must overridden in a subclass
     */
    protected void checkEventCounters() {
        throw new TestException("checkEventCounters must be implemented in a subclass");
    }
    
    /**
     * Creates a new object with the given <code>name</code> to add to a
     * region.
     *
     * @see BaseValueHolder
     * @see RandomValues
     */
    protected Object getObjectToAdd(String name) {
        BaseValueHolder anObj = new ValueHolder(name, randomValues);
        return anObj;
    }
    
    /**
     * Returns the "updated" value of the object with the given
     * <code>name</code>.
     *
     * @see BaseValueHolder#getAlternateValueHolder
     */
    protected Object getUpdateObject(String name) {
        Region rootRegion = CacheUtil.getCache().getRegion(regionName);
        BaseValueHolder anObj = null;
        BaseValueHolder newObj = null;
        try {
          anObj = (BaseValueHolder)rootRegion.get(name);
        } catch (CacheLoaderException e) {
          throw new TestException(TestHelper.getStackTrace(e));
        } catch (TimeoutException e) {
          throw new TestException(TestHelper.getStackTrace(e));
        }
        newObj = (anObj == null) ? new ValueHolder(name, randomValues) :
          anObj.getAlternateValueHolder(randomValues);
        return newObj;
    }
    
    /**
     * Returns the <code>CacheListener</code> that is installed on
     * regions created by this test.
     *
     * @see ETListener
     */
    protected CacheListener getCacheListener() {
        listenerInstalled = true;
        return new ETListener(isCarefulValidation);
    }
    
    protected Object[] getPartitionAttributes() {
       return null;
    }
    
    protected int getNumVMsWithListeners() {
        throw new TestException("getNumVMsWithListeners must be implemented in a subclass");
    }
    
    /**
     * Creates the root region in the {@link Cache} used in this test
     * according to the configuration in a {@link RegionDefinition}.
     *
     * @see RegionDefinition#createRegionDefinition()
     * @see RegionDefinition#createRootRegion
     */
    protected void createRootRegions() {
        RegionDefinition regDef = RegionDefinition.createRegionDefinition();
        regionName = regDef.getRegionName();
        List listeners = regDef.getCacheListeners();
        if ((listeners != null) && (listeners.size() == 0)) {
          CacheListener listener = eventTest.getCacheListener();
          regDef.addCacheListener(listener.getClass().getName());
        }
        String key = VmIDStr + RemoteTestModule.getMyVmid();
        String xmlFile = key + ".xml";
        CacheDefinition cacheDef = CacheDefinition.createCacheDefinition(CacheDefPrms.cacheSpecs, "cache1");
        DeclarativeGenerator.createDeclarativeXml(key + ".xml", cacheDef, regDef, true);
        CacheUtil.createRegion(cacheDef, regDef, xmlFile);

        isMirrored = false;

        // Try to determined if replicated/mirrored based on dataPolicy,
        // fall back to mirrorType if no dataPolicy defined
        DataPolicy dataPolicy = regDef.getDataPolicy();
        if (dataPolicy == null) {
          MirrorType mt = regDef.getMirroring();
          isMirrored = (mt == null) ? false : mt.isMirrored();
        } else {
          isMirrored = (dataPolicy.withReplication()) ? true : false;
        }
    }
    
    // ========================================================================
    // end task methods
    public static void HydraTask_printBB() throws Throwable {
        CacheBB.getBB().print();
        EventBB.getBB().print();

        EventCountersBB.getBB().print();
        OperationCountersBB.getBB().print();
        WriterCountersBB.getBB().print();
        TestHelper.checkForEventError(EventBB.getBB());
    }
    
    public static void HydraTask_iterate() throws Throwable {
        RegionDefinition regDef = RegionDefinition.createRegionDefinition();
        regDef.setEntryIdleTimeoutSec(null);
        regDef.setEntryIdleTimeoutAction(null);
        regDef.setEntryTTLSec(null);
        regDef.setEntryTTLAction(null);
        regDef.createRootRegion(CacheUtil.createCache(), regDef.getRegionName(),
                null, null, null);
        CacheBB.getBB().print();
        EventBB.getBB().print();

        EventCountersBB.getBB().print();
        OperationCountersBB.getBB().print();
        WriterCountersBB.getBB().print();
        TestHelper.checkForEventError(EventBB.getBB());
    }
    
    public static void HydraTask_endTask() throws Throwable {
        TestHelper.checkForEventError(EventBB.getBB());
        CacheBB.getBB().print();
        EventBB.getBB().print();
        EventCountersBB.getBB().print();
        OperationCountersBB.getBB().print();
        WriterCountersBB.getBB().print();

        eventTest = new EventTest();
        eventTest.initialize();
        StringBuffer errStr = new StringBuffer();
        try {
            eventTest.checkEventCounters();
        } 
        catch (VirtualMachineError e) {
          SystemFailure.initiateFailure(e);
          throw e;
        }
        catch (Throwable e) {
            Log.getLogWriter().info(e.toString());
            errStr.append(e.toString());
        }
        if (errStr.length() > 0)
            throw new TestException(errStr.toString());
        TestHelper.checkForEventError(EventBB.getBB());
    }
    
    /** Used for end task validation, iterate the keys/values in the given region,
     *  checking that the key/value match according to the test strategy.
     *
     * @param aRegion - the region to iterate.
     * @param allowZeroKeys - If the number of keys in the region is 0, then allow it
     *        if this is true, otherwise log an error to the return string.
     * @param allowZeroNonNullValues - If the number of non-null values in the region
     *        is 0, then allow it if this is true, otherwise log an error to the return
     *        string.
     *
     * @return [0] the number of keys in aRegion
     *         [1] the number of non-null values in aRegion
     *         [2] a String containg a description of any errors detected, or "" if none.
     *
     */
    protected Object[] iterateRegion(Region aRegion, boolean allowZeroKeys, boolean allowZeroNonNullValues) {
        StringBuffer errStr = new StringBuffer();
        Set keys = aRegion.keys();
        Log.getLogWriter().info("For " + TestHelper.regionToString(aRegion, false) + ", found " + keys.size() + " keys");
        int numKeys = keys.size();
        if (numKeys == 0) {
            if (!allowZeroKeys)
                errStr.append("Region " + TestHelper.regionToString(aRegion, false) + " has " + numKeys + " keys\n");
        }
        int valueCount = 0;
        Iterator it = keys.iterator();
        while (it.hasNext()) {
            Object key = it.next();
            Object value;
            try {
                value = aRegion.get(key);
            } catch (CacheLoaderException e) {
                throw new TestException(TestHelper.getStackTrace(e));
            } catch (TimeoutException e) {
                throw new TestException(TestHelper.getStackTrace(e));
            }
            Log.getLogWriter().info("Checking key " + key + ", value " + value);
            if (value != null) {
                valueCount++;
                BaseValueHolder vh = (BaseValueHolder)value;
                String nameValue = "" + NameFactory.getCounterForName(key);
                String valueHolderValue = "" + vh.myValue;
                if (!nameValue.equals(valueHolderValue)) {
                    String aStr = "Expected counter of key/value to match, key: " + key + ", value: " +
                            vh.toString();
                    Log.getLogWriter().info(aStr);
                    errStr.append(aStr + "\n");
                }
            }
        }
        if (valueCount == 0) {
            if (!allowZeroNonNullValues)
                errStr.append("Region " + TestHelper.regionToString(aRegion, false) + " has " + valueCount + " non-null values\n");
        }
        return new Object[] {new Integer(numKeys), new Integer(valueCount), errStr.toString()};
    }
    
    // ========================================================================
    // other methods
    
    /** Return a random operation from the hydra parameter specified by whichPrm.
     *
     *  @param whichPrm - the hydra parameter that is a list of operations
     *  @param disallowLocalEntryOps - if true, then do not return localInvalidate
     *         or localDestroy. This is used for regions that are mirrored, since
     *         localInvalidate and localDestroy are disallowed for mirrored regions.
     */
    protected int getOperation(Long whichPrm, boolean disallowLocalEntryOps) {
        long limit = 60000;
        long startTime = System.currentTimeMillis();
        int op = 0;
        do {
            String operation = TestConfig.tab().stringAt(whichPrm);
            if (operation.equals("add"))
                op =  ADD_OPERATION;
            else if (operation.equals("update"))
                op =  UPDATE_OPERATION;
            else if (operation.equals("invalidate"))
                op =  INVALIDATE_OPERATION;
            else if (operation.equals("destroy"))
                op =  DESTROY_OPERATION;
            else if (operation.equals("read"))
                op =  READ_OPERATION;
            else if (operation.equals("localInvalidate"))
                op =  LOCAL_INVALIDATE_OPERATION;
            else if (operation.equals("localDestroy"))
                op =  LOCAL_DESTROY_OPERATION;
            else if (operation.equals("close"))
                op = REGION_CLOSE_OPERATION;
            else if (operation.equals("clear"))
                op = CLEAR_OPERATION;
            else if (operation.equals("putIfAbsent"))
                op = PUT_IF_ABSENT_OPERATION;
            else if (operation.equals("remove"))
                op = REMOVE_OPERATION;
            else if (operation.equals("replace"))
                op = REPLACE_OPERATION;
            else if (operation.equals("putAll"))
                op = PUTALL_OPERATION;
            else
                throw new TestException("Unknown entry operation: " + operation);
            if (System.currentTimeMillis() - startTime > limit) {
                // could not find a suitable operation in the time limit; there may be none available
                throw new TestException("Could not find an operation in " + limit + " millis; disallowLocalEntryOps is " + true + "; check that the operations list has allowable choices");
            }
        } while (disallowLocalEntryOps && ((op == LOCAL_INVALIDATE_OPERATION) || (op == LOCAL_DESTROY_OPERATION)));
        return op;
    }
    
    static protected void logExecutionNumber() {
        long exeNum = EventBB.getBB().getSharedCounters().incrementAndRead(EventBB.EXECUTION_NUMBER);
        Log.getLogWriter().info("Beginning task with execution number " + exeNum);
    }
    
    protected void writeMyRegionNames() {
        writeMyRegionNames(null);
    }
    
    protected void writeMyRegionNames(Region excludeRegion) {
        String excludeRegionName = "";
        if (excludeRegion != null)
            excludeRegionName = TestHelper.regionToString(excludeRegion, false);
        Region rootRegion = CacheUtil.getCache().getRegion(regionName);
        Log.getLogWriter().info("In writeMyRegionNames: getting regions to write...");
        StringBuffer logStr = new StringBuffer();
        Set regionSet = rootRegion.subregions(true);
        Iterator it = regionSet.iterator();
        ArrayList aList = new ArrayList();
        while (it.hasNext()) {
            Region aRegion = ((Region)it.next());
            String regionName = aRegion.getFullPath();
            if (!regionName.equals(excludeRegionName)) {
                aList.add(regionName);
                logStr.append("   " + TestHelper.regionToString(aRegion, false) + "\n");
            }
        }
        logStr.insert(0, "In writeMyRegionNames: writing " + aList.size() + " regions to blackboard with name " + EventBB.CURRENT_REGION_NAMES + "\n");
        Log.getLogWriter().info(logStr.toString());
        long start = System.currentTimeMillis();
        EventBB.getBB().getSharedMap().put(EventBB.CURRENT_REGION_NAMES, aList);
        long end = System.currentTimeMillis();
        long duration = end - start;
        EventBB.getBB().getSharedCounters().setIfLarger(EventBB.MAX_WRITE_CURR_REGION_NAMES_MILLIS, duration);
        EventBB.getBB().getSharedCounters().setIfLarger(EventBB.MAX_NUM_WRITE_CURR_REGION_NAMES, aList.size());
        Log.getLogWriter().info("In writeMyRegionNames: time to write " +
                aList.size() + " region names is " + TestHelper.millisToString(end - start));
    }
    
    /** Return a currently existing random region. Assumes the root region exists.
     *
     *  @param allowRootRegion true if this call can return the root region, false otherwise.
     *
     *  @return A random region, or null of none available. Null can only be returned
     *          if allowRootRegion is false.
     */
    protected Region getRandomRegion(boolean allowRootRegion) {
        // select a root region to work with
        Set rootRegions = CacheUtil.getCache().rootRegions();
        int randInt = TestConfig.tab().getRandGen().nextInt(0, rootRegions.size() - 1);
        Object[] regionList = rootRegions.toArray();
        Region rootRegion = (Region)regionList[randInt];
        
        Set subregionsSet = rootRegion.subregions(true);
        if (subregionsSet.size() == 0) {
            if (allowRootRegion)
                return rootRegion;
            else
                return null;
        }
        ArrayList aList = null;
        try {
            Object[] array = subregionsSet.toArray();
            aList = new ArrayList(array.length);
            for (int i=0; i<array.length; i++) {
                aList.add(array[i]);
            }
        } catch (NoSuchElementException e) {
            throw new TestException("Bug 30171 detected: " + TestHelper.getStackTrace(e));
        }
        if (allowRootRegion)
            aList.add(rootRegion);
        if (aList.size() == 0) // this can happen because the subregionSet can change size after the toArray
            return null;
        randInt = TestConfig.tab().getRandGen().nextInt(0, aList.size() - 1);
        Region aRegion = (Region)aList.get(randInt);
        if (aRegion == null)
            throw new TestException("Bug 30171 detected: aRegion is null");
        return aRegion;
    }
    
    /** Called when the test gets a RegionDestroyedException. Sometimes the
     *  test expects this exception, sometimes not. Check for error scenarios
     *  and throw an error if the test should not get the RegionDestroyedException.
     *
     *  @param aRegion - the region that supposedly was destroyed and triggered the
     *         RegionDestroyedException
     *  @param anException - the exception that was thrown.
     */
    protected void handleRegionDestroyedException(Region aRegion, RegionDestroyedException anException) {
        if (isCarefulValidation) {
            // no concurrent threads destroying regions, so should not get RegionDestroyedException
            throw new TestException(TestHelper.getStackTrace(anException));
        } else {
            
            // make sure the region destroyed is this region
            if (!anException.getRegionFullPath().equals(aRegion.getFullPath())) {
                TestException te = new TestException("Got a RegionDestroyedException when operating on region " +
                        TestHelper.regionToString(aRegion, false) + ", but the region destroyed is '" +
                        anException.getRegionFullPath() +"'");
                te.initCause(anException);
                throw te;
            }
            
            // Note: the test only creates a region with a given name once. Once that region
            // has been destroyed, the test will never create another region with the same name
            boolean isDestroyed = aRegion.isDestroyed();
            if (isDestroyed) {
                // Make sure it really is destoyed and is not causing the RegionDestroyedException to be
                // thrown because one of its subregions was destroyed.
                Log.getLogWriter().info("Got " + RegionDestroyedException.class.getName() +
                        " on " + TestHelper.regionToString(aRegion, false) + "; exception expected, continuing test");
            } else { // the region was not destroyed, but we got RegionDestroyedException anyway
                throw new TestException("Bug 30645 or 34383 (likely): isDestroyed returned " + isDestroyed + " for region " +
                        TestHelper.regionToString(aRegion, false) + ", but a region destroyed exception was thrown: " +
                        TestHelper.getStackTrace(anException));
            }
        }
    }
    
    /**
     *  Given a region, return the number of names in the region and any
     *  of its subregions.
     *
     *  @param aRegion The region to use to determin the number of names.
     *  @param hasValueOnly True if the number of names returned should only
     *         include those names that return containsValueForKey true, false
     *         if all names should be counted.
     *
     *  @return The number of names in aRegion and its subregions.
     */
    protected int getNumNames(Region aRegion, boolean hasValueOnly) {
        int count = 0;
        StringBuffer aStr = new StringBuffer();
        aStr.append("Names in " + TestHelper.regionToString(aRegion, false) + " and its subregions:\n");
        Set aSet = new HashSet(aRegion.subregions(true));
        aSet.add(aRegion);
        Iterator it = aSet.iterator();
        while (it.hasNext()) {
            Region thisRegion = (Region)it.next();
            Iterator it2 = thisRegion.keys().iterator();
            while (it2.hasNext()) {
                Object key = it2.next();
                boolean containsValue = thisRegion.containsValueForKey(key);
                if (!hasValueOnly || (hasValueOnly && containsValue)) count++;
                aStr.append("   " + key + " in " + TestHelper.regionToString(thisRegion, false) +
                        " (containsValueForKey: " + containsValue + ")\n");
            }
        }
        Log.getLogWriter().info(aStr.toString());
        return count;
    }
    
    /** Return the current number of non-root regions.
     *
     */
    protected long getNumNonRootRegions() {
        Set rootRegions = CacheUtil.getCache().rootRegions();
        long numRegions = 0;
        Iterator iter = rootRegions.iterator();
        while (iter.hasNext()) {
            Region aRegion = (Region)iter.next();
            numRegions += aRegion.subregions(true).size(); // count does not include root
        }
        
        Log.getLogWriter().info("Num non-root regions is " + numRegions + ", num root region is " + rootRegions.size());
        return numRegions;
    }
    
    /** Verify that the given key in the given region is invalid (has a null value).
     *  If not, then throw an error. Checking is done locally, without invoking a
     *  cache loader or doing a net search.
     *
     *  @param aRegion The region containing key
     *  @param key The key that should have a null value
     */
    protected void verifyObjectInvalidated(Region aRegion, Object key) {
        StringBuffer errStr = new StringBuffer();
        boolean containsKey = aRegion.containsKey(key);
        if (!containsKey)
            errStr.append("Unexpected containsKey " + containsKey + " for key " + key + " in region " +
                    TestHelper.regionToString(aRegion, false) + "\n");
        boolean containsValueForKey = aRegion.containsValueForKey(key);
        if (containsValueForKey)
            errStr.append("Unexpected containsValueForKey " + containsValueForKey + " for key " + key +
                    " in region " + TestHelper.regionToString(aRegion, false) + "\n");
        // todo@lhughes - allow once getEntry() supported by PartitionedRegions
        if (aRegion.getAttributes().getPartitionAttributes() == null) {
           Region.Entry entry = aRegion.getEntry(key);
           if (entry == null)
               errStr.append("getEntry for key " + key + " in region " + TestHelper.regionToString(aRegion, false) +
                       " returned null\n");
           Object entryKey = entry.getKey();
           if (!entryKey.equals(key))
               errStr.append("getEntry.getKey() " + entryKey + " does not equal key " + key + " in region " + TestHelper.regionToString(aRegion, false) + "\n");
           Object entryValue = entry.getValue();
           if (entryValue != null)
               errStr.append("Expected getEntry.getValue() " + TestHelper.toString(entryValue) + " to be null.\n");
           if (errStr.length() > 0)
               throw new TestException(errStr.toString());
       }
    }
    
    /** Verify that the given key in the given region is destroyed (has no key/value).
     *  If not, then throw an error. Checking is done locally, without invoking a
     *  cache loader or doing a net search.
     *
     *  @param aRegion The region contains the destroyed key
     *  @param key The destroyed key
     */
    protected void verifyObjectDestroyed(Region aRegion, Object key) {
        StringBuffer errStr = new StringBuffer();
        boolean containsKey = aRegion.containsKey(key);
        if (containsKey)
            errStr.append("Unexpected containsKey " + containsKey + " for key " + key + " in region " +
                    TestHelper.regionToString(aRegion, false) + "\n");
        boolean containsValueForKey = aRegion.containsValueForKey(key);
        if (containsValueForKey)
            errStr.append("Unexpected containsValueForKey " + containsValueForKey + " for key " + key + " in region " + TestHelper.regionToString(aRegion, false) + "\n");
        // todo@lhughes - allow once getEntry() supported by PartitionedRegions
        if (aRegion.getAttributes().getPartitionAttributes() != null) {
           Region.Entry entry = aRegion.getEntry(key);
           if (entry != null)
               errStr.append("getEntry for key " + key + " in region " + TestHelper.regionToString(aRegion, false) +
                       " returned was non-null; getKey is " + entry.getKey() + ", value is " +
                       TestHelper.toString(entry.getValue()) + "\n");
           if (errStr.length() > 0)
               throw new TestException(errStr.toString());
       }
   }
}
