
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

package hct;

import hydra.BridgeHelper;
import hydra.CacheHelper;
import hydra.ClientVmInfo;
import hydra.ClientVmMgr;
import hydra.ClientVmNotFoundException;
import hydra.ConfigHashtable;
import hydra.ConfigPrms;
import hydra.DistributedSystemHelper;
import hydra.GsRandom;
import hydra.HydraRuntimeException;
import hydra.Log;
import hydra.MasterController;
import hydra.RegionHelper;
import hydra.StopSchedulingOrder;
import hydra.TestConfig;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import objects.ConfigurableObject;
import objects.SizedString;
import perffmwk.PerfStatMgr;
import perffmwk.PerfStatValue;
import util.NameFactory;
import util.PRObserver;
import util.TestException;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.AdminDistributedSystemFactory;
import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.DistributedSystemConfig;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.ClientHelper;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.internal.Connection;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.util.BridgeMembership;
import com.gemstone.gemfire.cache.util.BridgeMembershipListener;
import com.gemstone.gemfire.distributed.internal.ServerLocation;
import com.gemstone.gemfire.internal.cache.LocalRegion;

import cq.CQUtil;

/**
 * Contains Hydra tasks and supporting methods for testing the GemFire
 * hierarchical cache (cache server).  It has methods for
 initializing and configuring the cache server and edge clients,
 performing gets from the hierarchical cache,
 *
 * @author Belinda Buenafe
 * @since 2.0.2
 */
public class HierCache {

  static ConfigHashtable conftab = TestConfig.tab();
  static LogWriter logger = Log.getLogWriter();

  static GsRandom rand = new GsRandom();

  /** The name of the region to use in this test */
  static String regionName = conftab.stringAt(HctPrms.regionName);
  static long killInterval = conftab.longAt(HctPrms.killInterval);
  static int numClients = -1; // lazily initialized

  static boolean toggleKill = conftab.booleanAt(HctPrms.toggleKill);
  static boolean allowDisconnect = conftab.booleanAt(HctPrms.allowDisconnect, false);
  static boolean killServer = true;
  static BBoard bb = BBoard.getInstance();

  /** The bridge loader used by test clients (edge VMs) */
  
  static int jp_count;

  /** The following two variables are used by multiple invocations of the
   *  killSomething TASK method. The first time the method is called, both
   *  are set. Each subsequent time the method is called, their values are
   *  used and updated.
   */
  static List endpoints;
  static BridgeHelper.Endpoint endpointToggle;

  static int nameCount = 0;

  /**
   * Initializes the test region in the cache server VM according to
   * the configurations in {@link ConfigPrms}.
   *
   * @see hydra.BridgeHelper#startBridgeServer
   */
public static void initServerRegion() {
   PRObserver.installObserverHook();

   // create the cache
   CacheHelper.createCache(ConfigPrms.getCacheConfig());

   // Set-up UniversalMembershipListener (if configured)
   // Note that you wouldn't want this along with the BridgeMembershipListeners
   UniversalMembershipListener uListener = HctPrms.getUniversalMembershipListener();
   if (uListener != null) {
     try {
       DistributedSystemConfig dsConfig = AdminDistributedSystemFactory.defineDistributedSystem( DistributedSystemHelper.getDistributedSystem(), null );
       AdminDistributedSystem adminDS = AdminDistributedSystemFactory.getDistributedSystem( dsConfig ); 
       Log.getLogWriter().info("initServerRegion: registering UniversalMembershipListener " + uListener + " with AdminDS " + adminDS + " with config " + dsConfig);
       uListener.registerMembershipListener( adminDS );
     } catch (AdminException ae) {
       throw new TestException("Could not register UniversalMembershipListener " + ae);
     }
   }

   // Set-up the server to listen for client membership events
   BridgeMembershipListener membershipListener = HctPrms.getMembershipListener( HctPrms.serverMembershipListener );
   if (membershipListener != null) {
     BridgeMembership.registerBridgeMembershipListener( membershipListener );
     Log.getLogWriter().info("Registered BridgeMembershipListener " + membershipListener);
   }

   // create the region
   Region aRegion = RegionHelper.createRegion(regionName, ConfigPrms.getRegionConfig());
   if (aRegion.getAttributes().getDataPolicy().withPartitioning()) {
      if (aRegion.getAttributes().getPartitionAttributes().getRedundantCopies() > 0) {
         Log.getLogWriter().info("Recovery is expected in this test if data stores are stopped");
         BBoard.getInstance().getSharedMap().put("expectRecovery", new Boolean(true));
      }
   }

   // start the bridge server
   BridgeHelper.startBridgeServer(ConfigPrms.getBridgeConfig());
}

  /**
   * A Hydra INIT task that initialize the test region in an edge
   * client according to the configurations in {@link ConfigPrms}.
   */
public static void initEdgeRegion() {

    // create the cache
    boolean isPureJava = Boolean.getBoolean("gemfire.pureJavaMode");
    Log.getLogWriter().info("Initializing cache, pureJava is " + isPureJava + "...");
    CacheHelper.createCache(ConfigPrms.getCacheConfig());

   // BridgeMembership & UniversalMembership Listeners must be registered
   // prior to the client's bridgeClient instantiation

   // Set-up UniversalMembershipListener (if configured)
   // Note that you wouldn't want this along with the BridgeMembershipListeners
   UniversalMembershipListener uListener = HctPrms.getUniversalMembershipListener();
   if (uListener != null) {
     try {
       DistributedSystemConfig dsConfig = AdminDistributedSystemFactory.defineDistributedSystem( DistributedSystemHelper.getDistributedSystem(), null );
       AdminDistributedSystem adminDS = AdminDistributedSystemFactory.getDistributedSystem( dsConfig );
       Log.getLogWriter().info("initEdgeRegion: registering UniversalMembershipListener " + uListener + " with AdminDS " + adminDS + " with config " + dsConfig);
       uListener.registerMembershipListener( adminDS );
     } catch (AdminException ae) {
       throw new TestException("Could not register UniversalMembershipListener " + ae);
     }
   }

   // Set-up the client to listen for server membership events
   BridgeMembershipListener membershipListener = HctPrms.getMembershipListener( HctPrms.edgeMembershipListener );
   if (membershipListener != null) {
     BridgeMembership.registerBridgeMembershipListener( membershipListener );
     Log.getLogWriter().info("Registered BridgeMembershipListener " + membershipListener);
   }

   Region region = RegionHelper.createRegion(
     regionName, ConfigPrms.getRegionConfig());

   if (conftab.booleanAt(HctPrms.receiveValuesAsInvalidates)){
     // Check if subscription enabled.
     PoolImpl pool = ClientHelper.getPool(region);
     if (pool.getSubscriptionEnabled()) {
       Log.getLogWriter().info("Registering Interest for invalidates.");
       region.registerInterestRegex(".*", false, false);
     }
   }

   // initialize CQService, if needed
   CQUtil.initialize();
   CQUtil.initializeCQService();
   CQUtil.registerCQ(region);
   
}

  /**
   * A Hydra INIT task that sets the threadlocal connection of this thread. This
   * is used to create all connections up from to fix bug 39481 for membership
   * tests.
   */
public static void acquireThreadLocalConnection() {
  LocalRegion aRegion = (LocalRegion) RegionHelper.getRegion(regionName);
  PoolImpl pool = ClientHelper.getPool(aRegion);
  Connection conn = pool.acquireConnection();
  pool.setThreadLocalConnection(conn);
}
  /**
   * A Hydra INIT task that zeros out the MembershipListener counters
   * after the servers & clients come up (the first time).
   *
   * @see BBoard.expectedServerJoinedEvents
   */
  public static void clearMembershipCounters() {
    BBoard.getInstance().zeroAllCounters();
  }

  public static synchronized int getNumClients() {
    if (numClients == -1) {
      int totalVMs = TestConfig.getInstance().getTotalVMs();
      int bridgeVMs = BridgeHelper.getEndpoints().size();
      numClients = totalVMs - bridgeVMs;

      Log.getLogWriter().info("numBridgeServers = " + bridgeVMs);
      Log.getLogWriter().info("numEdgeClients = " + numClients);
    }
    return numClients;
  }

  /**
   * Begin a JProbe snapshot
   */
public static boolean startSnapshot() {
   return true;
}

  /**
   * End a JProbe snapshot
   */
public static boolean endSnapshot() {
   return true;
}

  /**
   * Request an entry from the region several times.  Make the key a
   * brand new name to cause a cache miss on the first try.
   */
private static void doGet() {
   // For JProbe debugging
   //jp_count++;
   //if (jp_count % 10 == 0)
   //    startSnapshot();

   int numHits = conftab.intAt(HctPrms.hitsPerMiss);
   boolean debug = conftab.booleanAt(HctPrms.debug);
   long maxKeys = conftab.intAt(HctPrms.maxKeys, -1);
   Region aRegion = RegionHelper.getRegion(regionName);

   Object name = NameFactory.getNextPositiveObjectName();

   // For workload based tests, limit the range of keys
   if (maxKeys > 0) {
     long nextKey = NameFactory.getCounterForName(name);
     if (nextKey >= maxKeys) {
        throw new StopSchedulingOrder("maxKeys (" + maxKeys + ") reached");
     }
   }

   if (debug)
      logger.info("doGet: requesting name " + name + " " + (numHits+1) + " times");
   Object first = null;
   String data = null;

   for (int n=0; n < numHits+1; n++) {
      try {
        long start = System.currentTimeMillis();
        Object got = null;
        try {
         got = aRegion.get(name);
        } catch (CacheLoaderException e) {
           // CacheLoaderExceptions can be thrown if CacheClosedException encountered during remote get
           String message = e.getMessage();
           Log.getLogWriter().info("CacheLoaderException " + e + " expected, continuing test");

           if (message.indexOf("com.gemstone.gemfire.cache.CacheClosedException") > 0) {
              // this can happen on slow shutdown of server
              // todo@lhughes - try sleep & continue,should get backup server
              // we must break out of loop since first & subsequent gets
              // must yield the same value
              MasterController.sleepForMs( 500 );
              break;
           }
        } 
        catch (CancelException e) {
           Log.getLogWriter().info("CancelException " + e + " expected, continuing test");
           // this can happen on slow shutdown of server
           // todo@lhughes - try sleep & continue,should get backup server
           MasterController.sleepForMs( 500 );
           break;
        } 

         if (got == null) {
            throw new HydraRuntimeException ("Could not get object with name: " + name);
         }
         if (n == 0) {
            long end = System.currentTimeMillis();
            if (debug)
               logger.info("Elapsed time on cache miss: " + (end - start));
            first = got;
            if (! dataCheck(name, got)) {
               if (got.getClass().getName().startsWith("[B"))
                  data = new String( (byte[]) got);
               throw new HydraRuntimeException ("ERROR: unexpected data on cache miss for key: "
                  + name + "--\n" + first);
            }
         } else {
            // byte arrays should be tested for equality differently than other objects
            if (got.getClass().getName().startsWith("[B")) {
               if (!Arrays.equals( (byte[])got, (byte[])first ) ) {
                  data = new String( (byte[]) got );
                  throw new HydraRuntimeException ("ERROR: unexpected data on cache hit for key: " + name + "--\n" + data);
               }
            } else {
               if (! got.equals(first) ) {
                  throw new HydraRuntimeException ("ERROR: unexpected data on cache hit for key: " + name + "--\n" + got);
               }
            }
         }
         MasterController.sleepForMs(conftab.intAt(HctPrms.getIntervalMs));
      //} catch (CacheLoaderException cle) {
         //logger.info("backend load error: " + cle.getMessage());
         //Throwable cause = cle.getCause();
         //if (cause != null)
            //throw new TestException(getStackTrace(cause));
         //else
            //throw new TestException(getStackTrace(cle));
      } catch (Exception e) {
         throw new TestException(getStackTrace(e));
      }
   }

   // JProbe Debugging
   //if (jp_count % 10 == 0)
   //    endSnapshot();
}

  /**
   * Returns whether or not the data received over the cache bridge is
   * what we expect.
   *
   * @see ConfigurableObject
   */
private static boolean dataCheck(Object name, Object got) {

      String objtype = got.getClass().getName();
      String data;
      String key = (String) name;
      int index = Integer.parseInt (key.substring(key.indexOf('_') + 1));

      if (objtype.startsWith("[B")) {
         data = new String ((byte[]) got );
         SizedString.validate(index, data);
         return true;

      } else if (objtype.equals("java.lang.String")) {
         data = (String) got;
         if (conftab.booleanAt(HctPrms.debug))
            logger.info ("DEBUG: Got value " + got + " for key " + key);

         if (data.startsWith("**"))
            return data.startsWith("***" + name + "***");

         SizedString.validate(index, data);

      } else {   // Configurable objects
         ((ConfigurableObject) got).validate(index);
      }

   return true;

}

  /**
   * Performs a {@linkplain HctPrms#getBatchSize bunch} of gets on the
   * hierarchical cache.
   */
public static void doGetBatch() {
   long pureJavaErrorCase = BBoard.getInstance().getSharedCounters().read(BBoard.PureJavaErrorCase);
   if (pureJavaErrorCase != 0) {
      throw new StopSchedulingOrder("Detected pure java error case; expected error"); 
   } 
   int num = conftab.intAt(HctPrms.getBatchSize);
   for (int n=0; n < num; n++) {
      doGet();
   }
   
   Region aRegion = RegionHelper.getRegion(regionName);
   ClientHelper.release(aRegion);
}

  /**
   * A STARTTASK that initializes the blackboard to keep track of the
   * last time something was killed.
   *
   * @return Whether or not this task initialized the blackboard
   */
public static boolean initBlackboard() {
   bb.getSharedMap().put("lastKillTime", new Long(0));
   Long val = (Long) bb.getSharedMap().get("lastKillTime");
   if (val != null && val.longValue() == 0 )
      return true;
   return false;
}

  /**
   * A Hydra TASK that chooses a test component (either randomly or
   * {@linkplain HctPrms#toggleKill alternately) such as a cache
   * server or the "database" that it accesses and kills it.  Checks
   * to make sure that the bridge loader (edge client) discovers that
   * the cache server has been killed.
   */
public synchronized static void killSomething()
throws ClientVmNotFoundException {
   PRObserver.initialize();
   String thing = conftab.stringAt(HctPrms.whatToKill);
   Region region = RegionHelper.getRegion(regionName);
   boolean doChecks = true;
   Set active;

   long now = System.currentTimeMillis();
   Long lastKill = (Long) bb.getSharedMap().get("lastKillTime");
   long diff = now - lastKill.longValue();
   
   if (diff < killInterval) {
      logger.info ("No kill executed");
      return;
   } else {
       bb.getSharedMap().put("lastKillTime", new Long(now));
   }

   if ( (! ClientHelper.getPool(region).getThreadLocalConnections()) ) {
      logger.info("Setting doChecks to FALSE for non-Sticky load balancing or kill of CacheServer's GemFire system");
      doChecks = false;
   }

   // Choose a server to kill
   BridgeHelper.Endpoint endpoint;
   if (toggleKill) {
     // If we just killed server 0, then kill server 1; and vice versa
     if (endpointToggle.equals(getEndpoint(0))) {
        endpointToggle = getEndpoint(1);
     } else if (endpointToggle.equals(getEndpoint(1))) {
        endpointToggle = getEndpoint(0);
     }
     endpoint = endpointToggle;

   } else {
     // Kill a random server
     int index = rand.nextInt(getEndpoints().size() - 1);
     endpoint = getEndpoint(index);
   }
   logger.info("Server chosen for kill: " + endpoint);

   killComponent(thing, endpoint);

   // sleep
   int sleepSec = TestConfig.tab().intAt( HctPrms.restartWaitSec );
   logger.info( "Sleeping for " + sleepSec + " seconds" );
   MasterController.sleepForMs( sleepSec * 1000 );

   // more gets to trigger failover
   try {
      doGet();
   } catch (StopSchedulingOrder sso){
      throw sso;
   } catch (Exception x) {
      throw new HydraRuntimeException("Error in doGet after killing cache server", x);
   }

   // Check to make sure the bridge loader (client) detected the
   // server failure
   ServerLocation server = new ServerLocation(endpoint.getHost(), endpoint.getPort());
   if (doChecks) {
      active = ClientHelper.getActiveServers(region);

      if (active.contains(server)) {
        logger.info("ERROR: Killed server " + server + " found in Active Server List: " + active);
      }
   }

   // restart and wait a little longer than retryInterval
   restartComponent(thing, endpoint);
   int sleepMs = ClientHelper.getRetryInterval(region) + 1000;
   logger.info( "Sleeping for " + sleepMs + " ms" );
   MasterController.sleepForMs( sleepMs );

   try {
      doGet();
   } catch (StopSchedulingOrder sso) {
      throw sso;
   } catch (Exception x) {
      throw new HydraRuntimeException("Error in doGet after reviving cache server", x);
   }

   // more checks on restart
   if (doChecks) {
      active = ClientHelper.getActiveServers(region);
      if (!active.contains(server)) {
          //throw new HydraRuntimeException("ERROR: Restarted server "
          logger.info("ERROR: Restarted server "
              + server + " not in Active Server List: " + active);
      }
   }
   return;
}

  /**
   * Kills a test component: a cache server, or the
   * {@linkplain DatabaseLoader database} that the cache server accesses.
   */
private static void killComponent(String comp, BridgeHelper.Endpoint endpoint)
throws ClientVmNotFoundException {

   if (comp.equals("cacheserver")) {
      if (allowDisconnect) {
        int index = rand.nextInt(0, 1);
        killServer = (index == 0) ? false : true;
      } 

      ClientVmInfo target = new ClientVmInfo(endpoint);
      if (killServer) {
         ClientVmMgr.stop("Killing cache server",
                     ClientVmMgr.MEAN_KILL, ClientVmMgr.ON_DEMAND, target);
         // Each client VM should receive a server memberCrashed event
         long count = BBoard.getInstance().getSharedCounters().add( BBoard.expectedServerCrashedEvents, getNumClients() );
         Log.getLogWriter().info("After incrementing, BBoard.expectedServerCrashedEvents = " + count);
      } else {
         // todo@lhughes - current server events are always crashed
         // product can't detected memberLeft
         ClientVmMgr.stop("Stopping cache server",
                     ClientVmMgr.NICE_EXIT, ClientVmMgr.ON_DEMAND, target);
         // Each client VM should receive a server memberDeparted event
         long count = BBoard.getInstance().getSharedCounters().add( BBoard.expectedServerCrashedEvents, getNumClients() );
         Log.getLogWriter().info("After incrementing, BBoard.expectedServerCrashedEvents = " + count);
      }
      return;
   }
   if (comp.equals("serverDB")) {
      Region aRegion = RegionHelper.getRegion(regionName);
      try {
         Object got = aRegion.get("disableDatabase");
         if (got == null)
            logger.info ("Could not get object for name: disableDatabase (expected)");
      } catch (Exception e) {
         logger.info("Got CacheException when disabling database");
      }
      return;
   }
   logger.info("ERROR in killComponent - unknown argument: " + comp);
   throw new HydraRuntimeException("error in killComponent");
}

public static void stopServers() throws ClientVmNotFoundException {
   MasterController.sleepForMs( 30000 );

   // kill all but 1 bridge server, do netstats after each kill
   for (int i = 0; i < getEndpoints().size() - 1; i++) {
     ClientVmInfo target = new ClientVmInfo(getEndpoint(i));
     ClientVmMgr.stop("Stopping cache server",
                       ClientVmMgr.NICE_EXIT, ClientVmMgr.ON_DEMAND, target);
     MasterController.sleepForMs( 30000 );
   }
}

public static void restartComponent(String comp, BridgeHelper.Endpoint endpoint)
throws ClientVmNotFoundException {

   if (comp.equals("cacheserver")) {
      ClientVmInfo target = new ClientVmInfo(endpoint);
      ClientVmMgr.start("Restarting cache server", target);
      Object value = BBoard.getInstance().getSharedMap().get("expectRecovery");
      if (value instanceof Boolean) {
         if (((Boolean)value).booleanValue()) {
            PRObserver.waitForRebalRecov(target, 1, 1, null, null, false);
         }
      }

      // each client VM should receive a server memberJoined event
      long count = BBoard.getInstance().getSharedCounters().add( BBoard.expectedServerJoinedEvents, getNumClients() );
      Log.getLogWriter().info("After restarting server and incrementing, BBoard.expectedServerJoinedEvents = " + count);
      // clients join (get a new socket) when the servers come back up, so
      // we need to expect clientJoined events as well (one per client in the server that got restarted)
      count = BBoard.getInstance().getSharedCounters().add( BBoard.expectedClientJoinedEvents, getNumClients() );
      Log.getLogWriter().info("After incrementing, BBoard.expectedClientJoinedEvents = " + count);
      return;
   }
   if (comp.equals("serverDB")) {
      Region aRegion = RegionHelper.getRegion(regionName);
      try {
         Object got = aRegion.get("enableDatabase");
         if (got == null)
            logger.info ("Could not get object for name: enableDatabase");
      } catch (Exception e) {
         logger.info("Got CacheException when enabling database");
      }
      return;
   }
   logger.info("ERROR in restartComponent - unknown argument: " + comp);
   throw new HydraRuntimeException("error in restartComponent");
}

   /** 
    *  Hydra TASK to cause client to recycle connection to the distributed
    *  system (should appear as memberLeft & memberJoined events to the  
    *  BridgeMembershipListener
    *
    *  @see BBoard.expectedClientDeparted
    *  @see BBoard.expectedClientJoined
    */
   public static void recycleClientConnection() {
     // If we're connected, disconnect
     // Note that the disconnect is threadsafe per VM,
     // but not for multiple threads within the VM 
     if (DistributedSystemHelper.getDistributedSystem() != null) {

       // close the cache and disconnect from the distributed system
       CacheHelper.closeCache();
       DistributedSystemHelper.disconnect();
     } else {
       return;
     } 
     // Each server should get this clientDeparted event
     long count = BBoard.getInstance().getSharedCounters().add( BBoard.expectedClientDepartedEvents, getEndpoints().size() );
     Log.getLogWriter().info("After incrementing, BBoard.expectedClientDepartedEvents = " + count);
                                                                                
     MasterController.sleepForMs( 5000 );

     // de-register the old Bridge(Server)MembershipListener for this client
     BridgeMembershipListener[] allListeners = BridgeMembership.getBridgeMembershipListeners();
     if (allListeners.length != 1) {
       throw new TestException("Expected 1 BridgeMembershipListener, but found " + allListeners.length + "\n BridgeMembershipListeners =  " + allListeners);
     } 
     BridgeMembershipListener membershipListener = allListeners[0];
     BridgeMembership.unregisterBridgeMembershipListener( membershipListener );

     // connect to distributedSystem, create cache & re-establish region with listener
     initEdgeRegion();
     // each server should get the client memberJoined event
     count = BBoard.getInstance().getSharedCounters().add( BBoard.expectedClientJoinedEvents, getEndpoints().size() );
     Log.getLogWriter().info("After incrementing, BBoard.expectedClientJoinedEvents = " + count);

     // each time the client connects, it should also generate a (server) memberJoined event (isClient = false) for each server it connect to
     count = BBoard.getInstance().getSharedCounters().add( BBoard.expectedServerJoinedEvents, getEndpoints().size() );
     Log.getLogWriter().info("After recycling client cache and incrementing, BBoard.expectedServerJoinedEvents = " + count);
   }

   /** 
    *  Hydra CLOSETASK to cause client to kill itself immediately with
    *  a mean kill.
    *
    *  These should appear as memberCrashed events BridgeClientListener
    *  Hydra doesn't permit us to kill all clientVms, so we must leave
    *  one clientVM behind.  
    *
    *  @see BBoard.expectedClientCrashedEvents
    */
   public synchronized static void killClientVm()
   throws ClientVmNotFoundException {

     /* For Debugging ***************************
     try {
       Region aRegion = RegionHelper.getRegion(regionName);
       PoolImpl p = (PoolImpl)((LocalRegion)aRegion).getServerProxy().getPool();
       Log.getLogWriter().severe("ME CONNECTED TO THIS MANY:"+p.getConnectedServerCount());
       Thread.sleep(10*1000);
       Log.getLogWriter().severe("SLEEPYME CONNECTED TO THIS MANY:"+p.getConnectedServerCount());
     } catch(InterruptedException tie) {
       tie.printStackTrace();
     }
     */
     
     // Each server should receive this client memberCrashed event
     long count = BBoard.getInstance().getSharedCounters().add( BBoard.expectedClientCrashedEvents, getEndpoints().size() );
     Log.getLogWriter().info("After incrementing, BBoard.expectedClientCrashedEvents = " + count);

     // Set our expected counts (assuming the client start is immediate)
     // each server should get the client memberJoined event
     count = BBoard.getInstance().getSharedCounters().add( BBoard.expectedClientJoinedEvents, getEndpoints().size() );
     Log.getLogWriter().info("After incrementing, BBoard.expectedClientJoinedEvents = " + count);

     // each time the client connects, it should also generate a (server) memberJoined event (isClient = false) for each server it connects to
     count = BBoard.getInstance().getSharedCounters().add( BBoard.expectedServerJoinedEvents, getEndpoints().size() );
     Log.getLogWriter().info("After killing client VM and incrementing, BBoard.expectedServerJoinedEvents = " + count);

     try {
       ClientVmMgr.stop("killing myself", ClientVmMgr.MEAN_KILL, ClientVmMgr.IMMEDIATE);
     } catch (ClientVmNotFoundException ex) {
       throw new HydraRuntimeException("I should always be able to kill myself", ex);
     }

   }

  /**
   * A Hydra CLOSETASK to validate membershipEvents (expected vs. actual)
   *
   * @see BBoard.expectedServerCrashedEvents
   * @see BBoard.actualServerCrashedEvents
   * @see BBoard.expectedServerDepartedEvents
   * @see BBoard.actualServerDepartedEvents
   * @see BBoard.expectedServerJoinedEvents
   * @see BBoard.actualServerJoinedEvents
   *
   * @see BBoard.expectedClientCrashedEvents
   * @see BBoard.actualClientCrashedEvents
   * @see BBoard.expectedClientDepartedEvents
   * @see BBoard.actualClientDepartedEvents
   * @see BBoard.expectedClientJoinedEvents
   * @see BBoard.actualClientJoinedEvents
   */
  public static void validateMembershipEvents() {

   // allow some time for distribution of events
   MasterController.sleepForMs( 60000 );

   BBoard.getInstance().print();

   // Server membership events
   // MemberCrashedEvents
   long expected = BBoard.getInstance().getSharedCounters().read( BBoard.expectedServerCrashedEvents );
   long actual = BBoard.getInstance().getSharedCounters().read( BBoard.actualServerCrashedEvents );
   if (expected != actual) {
     throw new TestException("Expected " + expected + " serverCrashedEvents, but actual = " + actual);
   }

   // MemberDepartedEvents
   expected = BBoard.getInstance().getSharedCounters().read( BBoard.expectedServerDepartedEvents );
   actual = BBoard.getInstance().getSharedCounters().read( BBoard.actualServerDepartedEvents );
   if (expected != actual) {
     throw new TestException("Expected " + expected + " serverDepartedEvents, but actual = " + actual);
   }

   // MemberJoinedEvents
   expected = BBoard.getInstance().getSharedCounters().read( BBoard.expectedServerJoinedEvents );
   actual = BBoard.getInstance().getSharedCounters().read( BBoard.actualServerJoinedEvents );
   if (expected != actual) {
     throw new TestException("Expected " + expected + " serverJoinedEvents, but actual = " + actual);
   }

   // ClientMembershipEvents 
   // MemberCrashedEvents
   expected = BBoard.getInstance().getSharedCounters().read( BBoard.expectedClientCrashedEvents );
   actual = BBoard.getInstance().getSharedCounters().read( BBoard.actualClientCrashedEvents );
   if (expected != actual) {
     throw new TestException("Expected " + expected + " clientCrashedEvents, but actual = " + actual);
   }

   // MemberDepartedEvents
   expected = BBoard.getInstance().getSharedCounters().read( BBoard.expectedClientDepartedEvents );
   actual = BBoard.getInstance().getSharedCounters().read( BBoard.actualClientDepartedEvents );
   if (expected != actual) {
     throw new TestException("Expected " + expected + " clientDepartedEvents, but actual = " + actual);
   }

   // MemberJoinedEvents
   expected = BBoard.getInstance().getSharedCounters().read( BBoard.expectedClientJoinedEvents );
   actual = BBoard.getInstance().getSharedCounters().read( BBoard.actualClientJoinedEvents );
   if (expected != actual) {
     throw new TestException("Expected " + expected + " clientJoinedEvents, but actual = " + actual);
   }
  }

  /**
   * A Hydra ENDTASK that prints out mean values for
   * loadTime/loadsCompleted and getTime/gets on edge and server
   */
  public static void reportAverages() {
   long pureJavaErrorCase = BBoard.getInstance().getSharedCounters().read(BBoard.PureJavaErrorCase);
   if (pureJavaErrorCase != 0) {
      throw new StopSchedulingOrder("Detected pure java error case; expected error"); 
   } 
    String loadTimeSpec = "edge* ClientStats * getTime "
                        + "filter=none ops=max combine=combineAcrossArchives trimspec=untrimmed";
    String loadsSpec = "edge* ClientStats * gets "
                        + "filter=none ops=max combine=combineAcrossArchives trimspec=untrimmed";
    String getTimeSpec = "edge* CachePerfStats cachePerfStats getTime "
                        + "filter=none ops=max combine=combineAcrossArchives trimspec=untrimmed";
    String getsSpec = "edge* CachePerfStats cachePerfStats gets "
                        + "filter=none ops=max combine=combineAcrossArchives trimspec=untrimmed";

    String bridgeGetTimeSpec = "bridgegemfire1 CachePerfStats cachePerfStats getTime "
                        + "filter=none ops=max combine=combineAcrossArchives trimspec=untrimmed";
    String bridgeGetsSpec = "bridgegemfire1 CachePerfStats cachePerfStats gets "
                        + "filter=none ops=max combine=combineAcrossArchives trimspec=untrimmed";

    double edgeAvgLoad = getMaxStat(loadTimeSpec)/getMaxStat(loadsSpec)/1000000.0;
    double edgeAvgGet = getMaxStat(getTimeSpec)/getMaxStat(getsSpec)/1000000.0;
    double serverAvgGet = getMaxStat(bridgeGetTimeSpec)/getMaxStat(bridgeGetsSpec)/1000000.0;
    logger.info(
       "\n  Server avg get time:  " + serverAvgGet + " ms" +
       "\n  Edge avg load time:  " + edgeAvgLoad + " ms" +
       "\n  Edge avg get time:  " + edgeAvgGet+ " ms" +
       "\n\n  Load latency: " + (edgeAvgLoad - serverAvgGet) + " ms" +
       "\n  Get latency: " + (edgeAvgGet - serverAvgGet) + " ms"
    );
  }

  /**
   * Return the max value for the stat specified in the argument
   */
  private static long getMaxStat(String statSpecString) {
    List statVals = PerfStatMgr.getInstance().readStatistics(statSpecString);
    if (statVals == null) {
       throw new HydraRuntimeException("No statistic found for: " + statSpecString);
    } else {
       PerfStatValue psv = (PerfStatValue) statVals.get(0);
       long max = (long) psv.getMax();
       return max;
    }
  }

//   public static Object getNextUniqueName() {
//     return "Object_" + RemoteTestModule.MyPid + ++nameCount;
//   }

  public static String getStackTrace(Throwable aThrowable) {
     StringWriter sw = new StringWriter();
     aThrowable.printStackTrace(new PrintWriter(sw, true));
     return sw.toString();
  }

  /**
   * Returns the i'th bridge server endpoint.
   */
  private static BridgeHelper.Endpoint getEndpoint(int i) {
    return (BridgeHelper.Endpoint)getEndpoints().get(i);
  }

  /**
   * Returns endpoints for all bridge servers that have ever started,
   * regardless of their current active status.  Initializes the toggle
   * used in stop/start methods as a side effect.
   */
  private static synchronized List getEndpoints() {
    if (endpoints == null) {
       endpoints = BridgeHelper.getEndpoints();
       endpointToggle = (BridgeHelper.Endpoint)endpoints.get(0);
    }
    return endpoints;
  }
}
