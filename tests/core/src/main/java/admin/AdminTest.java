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
package admin; 

import hydra.*;
import hydra.log.*;
import hydra.blackboard.*;
//import distcache.gemfire.*;
import java.util.*;
import util.*;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.admin.*;
import com.gemstone.gemfire.admin.internal.*;
import com.gemstone.gemfire.admin.RegionNotFoundException;
import com.gemstone.gemfire.admin.OperationCancelledException;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.Assert;

public class AdminTest {

  // the single instance of this test class
  public static AdminTest testInstance = null;

  // static fields for test config
  // should admin dist system share VM with distributed system
  protected static final boolean adminInDsVm = AdminPrms.adminInDsVm();
  protected static final String logLevel = TestConfig.tab().stringAt(LogPrms.file_logLevel);

  // static fields for GemFireManager operations; start numbering ops at 0
  protected static int[] GfManagerOperations = null;
  protected static final int getProductDirectoryOp = 0;
  protected static final int firstGfManagerOperation = 0;
  protected static final int lastGfManagerOperation = 0;
 
  // static fields for SystemMemper operations; start numbering ops at 100
  protected static int[] SystemMemberOperations = null;
  protected static final int accessAllRegions  = 100;
  protected static final int dispCacheAttrs    = 101;
  protected static final int modifyCacheAttrs  = 102;
  protected static final int dispRegionAttrs   = 103;
  protected static final int firstSystemMemberOperation = accessAllRegions;
  protected static final int lastSystemMemberOperation = dispRegionAttrs;
 
  // static fields for GemFireHealth operations; start numbering ops at 200
  protected static int[] GfHealthOperations = null;
  protected static final int setHealthEvalIntervalOp = 200;
  protected static final int firstGfHealthOperation = 200;
  protected static final int lastGfHealthOperation = 200;
 
  // instance variables
  public AdminDistributedSystem ds = null;

// static initializer to initialize simple static fields
static {
  GfManagerOperations = new int[lastGfManagerOperation - firstGfManagerOperation + 1];
  for (int i = firstGfManagerOperation; i <= lastGfManagerOperation; i++) 
      GfManagerOperations[i - firstGfManagerOperation] = i;
  SystemMemberOperations = new int[lastSystemMemberOperation - firstSystemMemberOperation + 1];
  for (int i = firstSystemMemberOperation; i <= lastSystemMemberOperation; i++) 
      SystemMemberOperations[i - firstSystemMemberOperation] = i;
  GfHealthOperations = new int[lastGfHealthOperation - firstGfHealthOperation + 1];
  for (int i = firstGfHealthOperation; i <= lastGfHealthOperation; i++) 
      GfHealthOperations[i - firstGfHealthOperation] = i;
}

/** Given an operation on some admin component, return a string description of 
 *  the operation.
 */
protected String opToString(int op) {
   switch (op) {
      case getProductDirectoryOp:          return "getProductDirectory";
      case accessAllRegions:               return "accessAllRegions";
      case dispCacheAttrs:                 return "dispCacheAttrs";
      case modifyCacheAttrs:               return "modifyCacheAttrs";
      case dispRegionAttrs:                return "dispRegionAttrs";
      case setHealthEvalIntervalOp:        return "setHealthEvalInterval";
      default:
         throw new TestException("Unknown operation " + op);
   }
}

  //============================================================================
  // Client Managed Locator related tasks
  //============================================================================

  /**
   * Creates a (disconnected) locator.
   */
  public static void createLocatorTask() {
    DistributedSystemHelper.createLocator();
  }

  /**
   * Connects a locator to its distributed system.
   */
  public static void startAndConnectLocatorTask() {
    DistributedSystemHelper.startLocatorAndAdminDS();
  }

  /**
   * Stops a locator.
   */
  public static void stopLocatorTask() {
    DistributedSystemHelper.stopLocator();
  }

/**
 * Hydra task to start JMX agent and connect it to an admin distributed system.
 */
public static void startAgentTask() {
  String agentConfig = ConfigPrms.getAgentConfig();
  AgentHelper.createAgent(agentConfig);
  AgentHelper.startAgent();
}

/** 
 * Create a region hierarchy  -- we need dozens of regions
 */
public synchronized static void createRegionHierarchyTask() {
   Cache c = CacheUtil.createCache();
   if (c.rootRegions().size() <= 0) {
      testInstance.createRegionHierarchy();
   }
}

/** Create a hierarchy of regions using admin.AdminPrms.numRootRegions,
 *  admin.AdminPrms.numSubRegions and admin.AdminPrms.regionDepth
 */
protected void createRegionHierarchy() {
   Cache c = CacheUtil.createCache();
   int numRoots = TestConfig.tab().intAt(AdminPrms.numRootRegions);
   int breadth = TestConfig.tab().intAt(AdminPrms.numSubRegions);
   int depth = TestConfig.tab().intAt(AdminPrms.regionDepth);

//   Vector roots = new Vector();
   Region r = null;

   c.setLockTimeout(TestConfig.tab().intAt(AdminPrms.lockTimeout));
   RegionDefinition regDef = RegionDefinition.createRegionDefinition();
   for (int i=0; i<numRoots; i++) {
      String rootName = "root" + (i+1);
      r = regDef.createRootRegion(c, rootName, null, null, null);
      Log.getLogWriter().info("Created root region " + TestHelper.regionToString(r, true));
      createSubRegions(r, breadth, depth, "Region", regDef);
   }


   // Save the total number of regions (per root) in the BB
   // for later use in verification of admin api
   Set croots = c.rootRegions();
   for (Iterator it = croots.iterator(); it.hasNext();) {
     Region aRegion = (Region) it.next();
     Set subRegions = aRegion.subregions(true);
     String mapKey = new String(aRegion.getName() + "_RegionCount");
     Log.getLogWriter().fine(" putting ( " + mapKey + ", " + subRegions.size() + ") into sharedMap" );
     AdminBB.getInstance().getSharedMap().put( mapKey, new Integer(subRegions.size()) );
   } 

   // print out the subregions created
   croots = c.rootRegions();
   if (croots.size() != numRoots)
       Log.getLogWriter().info("ERROR: wrong number of roots: " + c.rootRegions().size());
   for (Iterator it=croots.iterator(); it.hasNext(); ) {
       r = (Region) it.next();
//       System.out.println(r.getName());
       Set subs = r.subregions(true);
       for (Iterator sit=subs.iterator(); sit.hasNext(); ) {
          Log.getLogWriter().fine( "  " + ((Region)sit.next()).getName() );
       }
       Log.getLogWriter().fine("===");
   }
}

/** 
 *  Creates a well defined set of branches from the given root
 */
public static void createSubRegions( Region r, int numChildren, int levelsLeft, String parentName, RegionDefinition regDef) {
   String currentName;
   for (int i=1; i<=numChildren; i++) {
      currentName = parentName + "-" + i;
      Region child = null;
      try {
         child = r.createSubregion(currentName, regDef.getRegionAttributes(null, null, null));
         Log.getLogWriter().fine("Created subregion " + TestHelper.regionToString(child, true));
      } catch (RegionExistsException e) {
         child = r.getSubregion(currentName);
      } catch (TimeoutException e) {
         throw new TestException(TestHelper.getStackTrace(e)); 
      }
      if (levelsLeft > 1)
         createSubRegions(child, numChildren, levelsLeft-1, currentName, regDef);
   }
}

public static void dispRegions() {
   // print out the subregions created
   Set croots = CacheUtil.getCache().rootRegions();

   for (Iterator it=croots.iterator(); it.hasNext(); ) {
       Region r = (Region) it.next();
       Log.getLogWriter().fine(r.getName());
       Set subs = r.subregions(true);
       for (Iterator sit=subs.iterator(); sit.hasNext(); ) {
          Log.getLogWriter().fine( "  " + ((Region)sit.next()).getName() );
       }
       Log.getLogWriter().fine("===");
   }
}

/** Hydra task to initialize admin variables in an instance of
 *  this class. Connects to an admin distributed system.
 */
public synchronized static void initializeTask() {
  if (testInstance == null) {
    testInstance = new AdminTest();
    testInstance.initialize();
  }
}

/** 
 * Hydra task to initialize an <code>AdminTest</code> for hosting a
 * <code>Cache</code>. Connects to a non-admin distributed system.
 */
public synchronized static void initializeForCacheTask() {
  if (testInstance == null) {
    testInstance = new AdminTest();
    CacheUtil.createCache();
  }
}

/**
 * Hydra task to initialize an <code>AdminTest</code> for hosting a
 * <code>Cache</code> and for admin of the distributed system in the 
 * same VM. Connects to an admin distributed system and to a non-admin
 *  distributed system..
 */
public synchronized static void initializeForSameVmTask() {
  if (testInstance == null) {
    testInstance = new AdminTest();
    CacheUtil.createCache();
    testInstance.initialize();
  }
}


/** Create an admin DistributedSystem and connect
 */
protected void initialize() {
  Log.getLogWriter().info("Creating an admin DistributedSystem...");

  DistributedSystemConfig dsConfig; 
  
  if (!adminInDsVm) {
    // --Admin APIs not being used in same VM as cache APIs
    // Close the (non-admin) connection to the distributed system.  See
    // bug 31409.
    Cache cache = CacheUtil.createCache();
    cache.getDistributedSystem().disconnect();

    // Treat this member as if it were a GUI console
    AdminDistributedSystemFactory.setEnableAdministrationOnly(true);

    // get multicast and disable-tcp setting from system properties 
    String gemfireName = System.getProperty( GemFirePrms.GEMFIRE_NAME_PROPERTY );
    GemFireDescription gfd = TestConfig.getInstance().getGemFireDescription( gemfireName );
    Integer mcastPort = gfd.getMcastPort();
    String mcastAddress = gfd.getMcastAddress();
    Boolean disableTcp = gfd.getDisableTcp();

    Properties p = gfd.getDistributedSystemProperties();
    String locatorPortString = p.getProperty(DistributionConfig.LOCATORS_NAME);
    Log.getLogWriter().info("locatorPortString = " + locatorPortString);

    // get config for the distributed system
    dsConfig = AdminDistributedSystemFactory.defineDistributedSystem();
    dsConfig.setLocators( locatorPortString );
    if (TestConfig.tab().booleanAt(Prms.useIPv6)) {
      String addr = HostHelper.getHostAddress();
      dsConfig.setBindAddress(addr);
    }
    dsConfig.setMcastPort(mcastPort.intValue());
    dsConfig.setMcastAddress(mcastAddress);
    dsConfig.setDisableTcp(disableTcp.booleanValue());
  
  }

  else {
    // --Admin APIs will be used in same VM as Cache APIs
    // get config for the distributed system for admin of the distributed system 
    // to which this VM is currently connected
    Cache cache = CacheUtil.getCache();
    DistributedSystem thisVmDs = cache.getDistributedSystem();
    try {
      dsConfig = AdminDistributedSystemFactory.defineDistributedSystem(thisVmDs, null);
    } catch (AdminException ae) {
      String s = "Problem getting config for ds to which VM is currently connected";
      throw new AdminTestException (s, ae);
    }
  }

  // Use our AdminTestDistributedSystemFactory to select between admin & jmx interfaces
  try {
     ds = AdminTestDistributedSystemFactory.getDistributedSystem( dsConfig );
     Log.getLogWriter().fine("getDistributedSystem returned " + ds.toString());
  } catch (AdminTestException ae) {  
    throw new HydraRuntimeException("Could not getDistributedSystem", ae);
  }


  // Add the SystemMembershipListener
  // Not yet supported for JMX by our admin api facade
  if (AdminPrms.getAdminInterface() == AdminPrms.ADMIN) {
    SystemMembershipListener systemMembershipListener = AdminPrms.getSystemMembershipListener();
    if (systemMembershipListener != null) {
      ds.addMembershipListener( systemMembershipListener );
    }
  }

  // Add the SystemMemberCacheListener
  // Not yet supported for JMX by our admin api facade
  if (AdminPrms.getAdminInterface() == AdminPrms.ADMIN) {
    SystemMemberCacheListener systemMemberCacheListener = AdminPrms.getSystemMemberCacheListener();
    if (systemMemberCacheListener != null) {
      ds.addCacheListener( systemMemberCacheListener );
    }
  }

  Log.getLogWriter().info("Connecting to the AdminDistributedSystem " + ds);
  if (ds instanceof AdminDistributedSystemImpl) {
    // Admin API
    ((AdminDistributedSystemImpl) ds).connect( Log.getLogWriter().convertToLogWriterI18n() );

  } else {
    // JMX API
    ds.connect();
  }

  // Wait for the connection to be made
  long timeout = 30 * 1000;
  try {
    if (!ds.waitToBeConnected(timeout)) {
      String s = "Could not connect after " + timeout + "ms";
      throw new TestException(s);
    }

  } catch (InterruptedException ex) {
    String s = "Interrupted while waiting to be connected";
    throw new HydraRuntimeException(s, ex);
  }

  dsConfig = ds.getConfig();

  while (true) {
    if (!ds.isRunning()) {
      try {
        Thread.sleep(500);
      } catch (InterruptedException ex) {
        Log.getLogWriter().warning("Interrupted while sleeping", ex);
      }
    } else
      break;
  }

  Log.getLogWriter().fine("getConfig returns " + dsConfig.toString());
  String runningStatus = new String( (ds.isRunning()) ? " is " :  " is not " );
  Log.getLogWriter().info("DistributedSystem " 
                           + ds.getName() + ":" + ds.getId()
                           + runningStatus + "running");

  Log.getLogWriter().info("Exiting initialize()");
}

/** Hydra task to do random admin operations on an instance of GemFireHealth */
public static void GFHealthOpsTask() {
  long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
  long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
  testInstance.doGFHealthOps(minTaskGranularityMS);
}

/** Get an instance of GemFireHealth and randomly exercise it for msToRun millis.
 *
 *  @param msToRun - the number of milliseconds to do random operations.
 */
protected void doGFHealthOps(long msToRun) {

  /* remove from logging for now:  AdminBB.getInstance().printSharedCounters(); */

  // Get an instance of GemFireHealth
  GemFireHealth health = ds.getGemFireHealth();
  //String hostName = HostHelper.getHostAddress();
  String hostName = HostHelper.getIPAddress().getCanonicalHostName();
  Log.getLogWriter().info("hostName is " + hostName);

  // loop while doing random operations
  Log.getLogWriter().info("Doing operations on GemFireHealth " + health + " for " + msToRun + " ms");
  long startTime = System.currentTimeMillis();
  do {
     int randInt = TestConfig.tab().getRandGen().nextInt(0, GfHealthOperations.length-1);
     int whichOp = GfHealthOperations[randInt];
     Log.getLogWriter().fine("Doing GemFireHealth operation " + opToString(whichOp) + " on " + health);
     GemFireHealthConfig healthConfig = null;
     boolean configIsDefault = false;
     if (TestConfig.tab().getRandGen().nextBoolean()) { // get config for default
        Log.getLogWriter().fine("Getting default GemFireHealthConfig...");
        configIsDefault = true;
        healthConfig = health.getDefaultGemFireHealthConfig();
     } else { // get config for host
        Log.getLogWriter().fine("Getting GemFireHealthConfig for host " + hostName + "...");
        configIsDefault = false;
        healthConfig = health.getGemFireHealthConfig(hostName);
     }
     switch (whichOp) {
        case setHealthEvalIntervalOp: {
           int newInterval = TestConfig.tab().getRandGen().nextInt(1, 10);
           Log.getLogWriter().fine("Setting new healthEvaluationInterval to " + newInterval);
           healthConfig.setHealthEvaluationInterval(newInterval);
           break;
        }
        default: {
           throw new TestException("Unknown operation " + opToString(whichOp));
        }
     }

     // set the new config
     if (configIsDefault) // set it as the default
        health.setDefaultGemFireHealthConfig(healthConfig);
     else // set it for the local host
        health.setGemFireHealthConfig(hostName, healthConfig);

     // get the health
     GemFireHealth.Health currentHealth = health.getHealth();
     Log.getLogWriter().fine("Current health is " + currentHealth + ", diagnosis is " + health.getDiagnosis());
  } while (System.currentTimeMillis() - startTime < msToRun);
  Log.getLogWriter().fine("Done doing operations on GemFireManager for " + msToRun + " ms");
}

/** Hydra task to do random admin operations on a random GemFireManager */
public static void SystemMemberOpsTask() {
  long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
  long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
  try {
    testInstance.doSystemMemberOps(minTaskGranularityMS);

  } catch (RuntimeException ex) {
    Log.getLogWriter().severe("During doSystemMemberOps", ex);
    throw ex;
  }
}

/** Choose a random SystemMember, and do random operations on it for msToRun millis.
 *
 *  @param msToRun - the number of milliseconds to do random operations.
 */
protected void doSystemMemberOps(long msToRun) {
  // Get a list of all SystemMembers
  SystemMember[] systemMembers = null;

  /* remove from logging for now:  AdminBB.getInstance().printSharedCounters(); */
  TestHelper.checkForEventError(AdminBB.getInstance());

  try {
     systemMembers = ds.getSystemMemberApplications();
  } catch (AdminException ae) {
     throw new HydraRuntimeException("Could not get SystemMemberApplications");
  }

  // if there are no SystemMembers, then return
  if (systemMembers.length == 0) {
     Log.getLogWriter().info("Cannot do operations on SystemMembers, there are no SystemMembers");
     return;
  }

  StringBuffer aStr = new StringBuffer();
  aStr.append("Discovered " + systemMembers.length + " SystemMembers\n");
  for (int i=0; i<systemMembers.length; i++ ) {
     aStr.append("   SystemMembers[" + i + "] = " + systemMembers[i].toString() + "\n");
  }
  Log.getLogWriter().fine(aStr.toString());

  // loop while doing random operations
  Log.getLogWriter().info("Doing operations on SystemMembers for " + msToRun + " ms");
  long startTime = System.currentTimeMillis();
  do {
     int randInt = TestConfig.tab().getRandGen().nextInt(0, systemMembers.length-1);
     SystemMember sm = systemMembers[randInt];
     randInt = TestConfig.tab().getRandGen().nextInt(0, SystemMemberOperations.length-1);
     int whichOp = SystemMemberOperations[randInt];
     Log.getLogWriter().fine("Doing SystemMember operation " + opToString(whichOp) + " on " + sm);

     SystemMemberCache cache;
     try {
       cache = sm.getCache();

     } catch (AdminException e) {
       String s = "During accessAllRegions";
       throw new TestException(s, e);

     } catch (OperationCancelledException e) {
       Log.getLogWriter().fine("Ignoring exception " + e );
       continue;
     }

     if (cache == null) {
       // Race with administrating a VM whose connection has been
       // recycled and is still initializing its cache
       Log.getLogWriter().fine("Member " + sm + " has no cache");
       continue;
     }

     switch (whichOp) {
        case accessAllRegions: {
           try {
              accessAllRegions(cache);
           } 
           catch (CancelException e) {
              // Caches can get closed while we're accessing them
              Log.getLogWriter().fine("Ignoring exception " + e );
           } catch (OperationCancelledException e) {
              Log.getLogWriter().fine("Ignoring exception " + e );
           }
           break;
        }
        case modifyCacheAttrs: {
           try {
              modifyCacheAttrs(cache);
           } catch (AdminException e) {
             String s = "During modifyCacheAttrs on " + sm.getName() +
               " " + sm.getId();
             throw new TestException(s, e);
           } 
           catch (CancelException e) {
              // Caches can get closed while we're accessing them
              Log.getLogWriter().fine("Ignoring exception " + e );
           } catch (OperationCancelledException e) {
              Log.getLogWriter().fine("Ignoring exception " + e );
           }
           break;
        }
        case dispCacheAttrs: {
           try {
              dispCacheAttrs(cache);
           } 
           catch (CancelException e) {
              // Caches can get closed while we're accessing them
              Log.getLogWriter().fine("Ignoring exception " + e );
           } catch (OperationCancelledException e) {
              Log.getLogWriter().fine("Ignoring exception " + e );
           }
           break;
        }
        case dispRegionAttrs: {
           try {
              dispRegionAttrs(cache);
           } catch (AdminException e) {
             String s = "During dispRegionAttrs on " + sm.getName() +
               " " + sm.getId();
             throw new TestException(s, e);
           } 
           catch (CancelException e) {
              // Caches can get closed while we're accessing them
              Log.getLogWriter().fine("Ignoring exception " + e );
           } catch (OperationCancelledException e) {
              Log.getLogWriter().fine("Ignoring exception " + e );
           }
           break;
        }
        default: {
           throw new TestException("Unknown operation " + opToString(whichOp));
        }
     }
  } while (System.currentTimeMillis() - startTime < msToRun);
  Log.getLogWriter().fine("Done doing operations on GemFireManager for " + msToRun + " ms");
}

protected void accessAllRegions( SystemMemberCache smCache ) {
  Log.getLogWriter().fine("SystemMemberCache = " + smCache.toString());

//  SystemMemberRegion smRegion = null;
  Set rootRegions = smCache.getRootRegionNames();
  Log.getLogWriter().fine(" root regions = " + rootRegions);

  // get a single list of all regions in the cache
  ArrayList aList = getAllRegionNames( smCache );
  StringBuffer displayList = new StringBuffer();
  for (int i = 0; i < aList.size(); i++) {
    displayList.append( aList.get(i) + " \n");
  }
  Log.getLogWriter().fine("accessAllRegions regionList = \n" + displayList.toString());
}

protected void dispCacheAttrs( SystemMemberCache smCache ) {

  Log.getLogWriter().fine("dispCacheAttrs: SystemMemberCache = " + smCache.toString());

  StringBuffer aStr = new StringBuffer( "dispCacheAttrs values:\n" );
  try {
    // get latest updates
    smCache.refresh();
    aStr.append( "LockLease = " + smCache.getLockLease() + "\n");
    aStr.append( "LockTimeout = " + smCache.getLockTimeout() + "\n");
    aStr.append( "SearchTimeout = " + smCache.getSearchTimeout() + "\n");

  } catch ( RegionNotFoundException ex) {
    Log.getLogWriter().fine("dispCacheAttrs caught " + ex);
  }

  Log.getLogWriter().fine ( aStr.toString() );
  
}

protected void modifyCacheAttrs( SystemMemberCache smCache ) 
  throws AdminException {
  Log.getLogWriter().fine("modifyCacheAttrs: SystemMemberCache = " + smCache.toString());

  // save the current values
  int lockLease = smCache.getLockLease();
  int lockTimeout = smCache.getLockTimeout();
  int searchTimeout = smCache.getSearchTimeout();

  StringBuffer aStr = new StringBuffer( "modifyCacheAttrs original values:\n");
  aStr.append( "lockLease = " + lockLease + "\n");
  aStr.append( "lockTimeout = " + lockTimeout + "\n");
  aStr.append( "searchTimeout = " + searchTimeout + "\n");
  Log.getLogWriter().fine( aStr.toString() );

  // modify
  smCache.setLockLease(lockLease + 10);
  smCache.setLockTimeout(lockTimeout + 10);
  smCache.setSearchTimeout(searchTimeout + 10);

  // get updated values
  smCache.refresh();

  aStr = new StringBuffer( "modifyCacheAttrs updated values: \n");
  aStr.append( "lockLease = " + smCache.getLockLease() + "\n");
  aStr.append( "lockTimeout = " + smCache.getLockTimeout() + "\n");
  aStr.append( "searchTimeout = " + smCache.getSearchTimeout() + "\n");
  Log.getLogWriter().fine( aStr.toString() );

}

/** Display various attributes about a randomly chosen region in the given
 *  SystemMemberCache.
 *
 *  @param smCache The SystemMemberCache to get a random region from.
 */
protected void dispRegionAttrs( SystemMemberCache smCache ) 
  throws AdminException {

  Log.getLogWriter().fine("dispRegionAttrs: SystemMemberCache = " + smCache.toString());

  // make sure we have the latest picture
  smCache.refresh();

  String regionName = getRandomRegion( smCache );
  if (regionName == null) {
    return;
  }

  SystemMemberRegion smRegion = null;
  try {
     smRegion = smCache.getRegion( regionName );
  } catch (AdminException ae) {
    throw new AdminTestException(" could not getRegion " + regionName, ae );
  } catch (RegionNotFoundException ex) {
    Log.getLogWriter().fine("Ignoring RegionNotFoundException for " + regionName);
    return;
  } catch (RegionDestroyedException de) {
    Log.getLogWriter().fine("Ignoring RegionDestroyedException for " + regionName);
    return;
  }

  if (smRegion == null) {
    Log.getLogWriter().fine("dispRegionAttrs : getRegion returned null for " + regionName);
    return;
  }
  
  // display current values
  try {
    smRegion.refresh();
  } catch (RegionNotFoundException ex) {
    Log.getLogWriter().fine("Ignoring RegionNotFoundException for " +
                            regionName + " during refresh");
    return;

  } catch (RegionDestroyedException de) {
    Log.getLogWriter().fine("Ignoring RegionDestroyedException for " +
                            regionName + " during refresh");
    return;
  }

  // protect against concurrent region destroys
  StringBuffer aStr = new StringBuffer( "dispRegionAttrs: \n");
  try {
    int entryTTL = smRegion.getEntryTimeToLiveTimeLimit();
    int entryIdleTime = smRegion.getEntryIdleTimeoutTimeLimit();
    int regionTTL = smRegion.getRegionTimeToLiveTimeLimit();
    int regionIdleTime = smRegion.getRegionIdleTimeoutTimeLimit();
    int entryCount = smRegion.getEntryCount();
    long lastModifiedTime = smRegion.getLastModifiedTime() ;
    long lastAccessedTime = smRegion.getLastAccessedTime();
    long hitCount = smRegion.getHitCount();
    long missCount = smRegion.getMissCount();
    Set subregionNames = smRegion.getSubregionNames();

    aStr.append( "Region = " + regionName + "\n");
    aStr.append( "entryTTL = " + entryTTL + "\n");
    aStr.append( "entryIdleTime = " + entryIdleTime + "\n");
    aStr.append( "regionTTL = " + regionTTL + "\n");
    aStr.append( "regionIdleTime = " + regionIdleTime + "\n");
    aStr.append( "entryCount = " + entryCount + "\n");
    aStr.append( "lastModifiedTime = " + lastModifiedTime + "\n");
    aStr.append( "lastAccessedTime = " + lastAccessedTime + "\n");
    aStr.append( "hitCount = " + hitCount + "\n");
    aStr.append( "missCount = " + missCount + "\n");
    aStr.append( "subregionNames = " + subregionNames + "\n");
  } catch (RegionNotFoundException ex) {
    Log.getLogWriter().fine("Ignoring RegionNotFoundException for " + regionName + " getAttributes()");
    return;
  } catch (RegionDestroyedException de) {
    Log.getLogWriter().fine("Ignoring RegionDestroyedException for " + regionName + "getAttributes()");
    return;
  }

  Log.getLogWriter().fine( aStr.toString() );
}

  /**
   * Returns the name of a randomly-selected region in the given
   * cache.  If the cache does not contain any regions, then
   * <code>null</code> is returned.
   */
protected String getRandomRegion( SystemMemberCache smCache ) 
  throws AdminException {

  ArrayList regionList = getAllRegionNames( smCache );
  for (int i = 0; i < 20 && regionList.isEmpty(); i++) {
    // Race condition between getting cache info and root regions
    // being created after connection is recycled
    try {
      Thread.sleep(500);

    } catch (InterruptedException ex) {
      throw new TestException("Interrupted while sleeping", ex);
    }
    smCache.refresh();
    regionList = getAllRegionNames( smCache );
  }

  if (regionList.isEmpty()) {
    // Do we really want to blow up here?  Could it be a race
    // condition in the test?  Or is it really a bug in the product?

    String s = "Member Cache " + smCache + " has no regions";
    Log.getLogWriter().fine(s);
    return null;
  }

  int randInt = TestConfig.tab().getRandGen().nextInt(0, regionList.size()-1);
  String regionName = (String) regionList.get( randInt );
  Log.getLogWriter().fine("getRandomRegion returning " + regionName);
  return regionName;
  
}

protected ArrayList getAllRegionNames( SystemMemberCache smCache ) {
  ArrayList regionList = new ArrayList();
  SystemMemberRegion smRegion = null;

  smCache.refresh();

  Set rootRegions = smCache.getRootRegionNames();
  regionList.addAll( rootRegions );

  for (Iterator it = rootRegions.iterator(); it.hasNext(); ) {
    String regionName = null;
    try {
      regionName = (String) it.next();
      smRegion = smCache.getRegion( regionName );
    } catch (AdminException ae) {
      throw new AdminTestException(" could not getRegion " + regionName, ae );
    } catch (RegionNotFoundException ex) {
      Log.getLogWriter().fine("Ignoring RegionNotFoundException for "  + regionName);
      continue;
    } catch (RegionDestroyedException de) {
      Log.getLogWriter().fine("Ignoring RegionDestroyedException for " + regionName);
      continue;
    }
    regionList.addAll( getRegionNames( smCache, smRegion ) );
  }
  return regionList;
}

protected ArrayList getRegionNames( SystemMemberCache smCache, SystemMemberRegion smRegion ) {
  ArrayList aList = new ArrayList();
  String subregionName = null;

//   // end the recursion!
//   if (smRegion == null) {
//     Log.getLogWriter().fine("getRegionNames: smRegion is null!");
//     return aList;
//   }

  Set subRegions = null;
  String parentRegionName =  null;
  try {
    smRegion.refresh();
    parentRegionName = smRegion.getFullPath();
    subRegions = smRegion.getSubregionNames();
  } catch (Exception e) {
    Log.getLogWriter().fine("getRegionNames caught " + e);
    return aList;
  }

  for (Iterator it = subRegions.iterator(); it.hasNext();) {
    subregionName = new String(parentRegionName + "/" + (String)it.next());
    aList.add( subregionName );
    SystemMemberRegion aRegion = null;
    try {
      aRegion = smCache.getRegion( subregionName );
    } catch (AdminException ae) {
      throw new AdminTestException(" Could not get subregion given subregionName " + subregionName, ae);
    } catch (RegionNotFoundException ex) {
      Log.getLogWriter().fine("Ignoring RegionNotFoundException for " + subregionName);
      continue;
    } catch (RegionDestroyedException dx) {
      Log.getLogWriter().fine("Ignoring RegionDestroyedException for " + subregionName);
      continue;
    }

    if (aRegion == null) {
      if (!AdminPrms.areRegionsDestroyed()) {
        String s = "Could not find subregion \"" + subregionName + "\"";
        throw new TestException(s);
      }

    } else {
      aList.addAll( getRegionNames(smCache, aRegion) );
    }
  }
  return aList;
}

/** 
 *  space holder for future endTask
 */
public static void validateTest() {
  Log.getLogWriter().info("Entered validateTest()");

  // We can only validate in Admin API currently
  if (AdminPrms.getAdminInterface() == AdminPrms.ADMIN) {
    AdminBB.getInstance().printSharedCounters();

    SharedCounters sc = AdminBB.getInstance().getSharedCounters();

    // Make sure we did recycle the connections and that 
    // members reported joining & departing
    long recycleRequests = sc.read( AdminBB.recycleRequests );
    long joinedNotifications = sc.read( AdminBB.joinedNotifications );
    long departedNotifications = sc.read( AdminBB.departedNotifications );
  
    if ( recycleRequests == 0)  {
      throw new AdminTestException("ValidationFailed: No recycling of Connections completed");
    }

    if ( (departedNotifications == 0) || (joinedNotifications == 0) ) {
      throw new AdminTestException("ValidationFailed:  Expected non-zero values , but found joined:" + joinedNotifications + " departed: " + departedNotifications);
    }
  }
}

/** Hydra task to do random admin operations to any admin component for a time period
 *  specified by minTaskGranularitySec.
 */
public static void anyAdminOpsTask() {
  testInstance.doAnyAdminOps();
}

/** Randomly do any admin operation on any admin component for minTaskGranularitySec.
 */
protected void doAnyAdminOps() {
  long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
  long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
  Log.getLogWriter().info("Doing admin operations for " + minTaskGranularityMS + " ms");
  long startTime = System.currentTimeMillis();
  do {
     doGFHealthOps(0);
     doSystemMemberOps(0);
  } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
  Log.getLogWriter().fine("Done doing admin operations for " + minTaskGranularityMS + " ms");
}

/** Hydra task to wait for all system members to appear. The expected
 *  number of system members is the number of VMs in the test.
 */
public synchronized static void waitForSystemMembersTask() {
   testInstance.waitForSystemMembers();
}

/** Wait for the number of system members to be equal to the number of VMs
 */
protected void waitForSystemMembers() {
   int expectedNumSystemMembers = TestHelper.getNumVMs();
   if (!adminInDsVm) {
       // admin dist system is in different VM than distributed system - so deduct 1 for
       // the admin VM
     expectedNumSystemMembers--;
   }
   List agents = AgentHelper.getEndpoints();
   expectedNumSystemMembers -= agents.size(); // deduct JMX agent VMs
   Log.getLogWriter().info("Waiting for " + expectedNumSystemMembers + " system members");
   int numSystemMembers = 0;
   long lastLogTime = 0;
   long logInterval = 5000;
   while (true) {
      try {
         Assert.assertTrue(ds != null);
         numSystemMembers = ds.getSystemMemberApplications().length;
         if (numSystemMembers == expectedNumSystemMembers) { // got them all
            break;
         }
	 else if (numSystemMembers > expectedNumSystemMembers) { 
	     SystemMember[] systemMembers = ds.getSystemMemberApplications();
	     StringBuffer aStr = new StringBuffer();
	     aStr.append("Discovered " + systemMembers.length + " SystemMembers\n");
	     for (int i=0; i<systemMembers.length; i++ ) {
		 aStr.append("   SystemMembers[" + i + "] = " + systemMembers[i].getId() + "\n");
	     }
	     Log.getLogWriter().fine(aStr.toString());


	   throw new AdminTestException("Number of system members exceeds number expected.  Expected: " +              expectedNumSystemMembers + " but found: " + numSystemMembers);  
	 }
	 else if (System.currentTimeMillis() - lastLogTime >= logInterval) {
            Log.getLogWriter().fine("Waiting for " + expectedNumSystemMembers + " system members, " +
                "current number of system members: " + numSystemMembers);
            lastLogTime = System.currentTimeMillis();
         }
         try { 
            Thread.sleep(1000); 
         } catch (InterruptedException e) { 
            throw new TestException(TestHelper.getStackTrace(e));
         }
      } catch (AdminException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
   }
   Log.getLogWriter().fine("Done waiting for system members, found " + numSystemMembers);
}

/** Hydra task to monitor the health of the distributed system.
 *  This will set the evaluation interval to 1 second (to stress things as much
 *  as possible) and will run for minTaskGranularitySec. If another thread in
 *  this VM is running a task that would cause the interval to be changed, then
 *  the 1 second setting will not remain throughout this task.
 */
public static void healthMonitorTask() {
  testInstance.doHealthMonitor();
}

/** Monitors the health of the distributed system.
 *  This will set the evaluation interval to 1 second (to stress things as much
 *  as possible) and will run for minTaskGranularitySec. If another thread in
 *  this VM is running a task that would cause the interval to be changed, then
 *  the 1 second setting will not remain throughout this task.
 */
protected void doHealthMonitor() {
  // Set the evaluation interval to 1
  GemFireHealth health = ds.getGemFireHealth();
  GemFireHealthConfig healthConfig = health.getDefaultGemFireHealthConfig();
  int newInterval = 1;
  Log.getLogWriter().fine("Setting new healthEvaluationInterval to " + newInterval);
  healthConfig.setHealthEvaluationInterval(newInterval);
  health.setDefaultGemFireHealthConfig(healthConfig);

  long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec);
  long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
  Log.getLogWriter().info("Monitoring health for " + minTaskGranularityMS + " ms");
  long startTime = System.currentTimeMillis();
  do { // monitor the health
     GemFireHealth.Health currentHealth = health.getHealth();
     String diagnosis = health.getDiagnosis();
     String aStr = "Current health is " + currentHealth + ", diagnosis is " + diagnosis;
     Log.getLogWriter().fine(aStr);
     if ((currentHealth.readResolve().equals(GemFireHealth.OKAY_HEALTH)) ||
         (currentHealth.readResolve().equals(GemFireHealth.POOR_HEALTH)))
        throw new TestException(aStr);
     try {
        Thread.sleep(500);
     } catch (InterruptedException e) {
        throw new TestException(TestHelper.getStackTrace(e));
     }
  } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
  Log.getLogWriter().fine("Done monitoring health for " + minTaskGranularityMS + " ms");
}

}
