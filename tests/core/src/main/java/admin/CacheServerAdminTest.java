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

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.admin.*;
import com.gemstone.gemfire.admin.internal.*;

import hydra.*;
import hydra.blackboard.*;
import util.*;

/**
 *
 *  Start/Stop/Configure CacheServers via Admin API
 *
 */
public class CacheServerAdminTest {

  // the single instance of this test class
  protected static CacheServerAdminTest cacheServerAdminTest = null;

  //instance variables
  protected AdminDistributedSystem admin = null;

  /*
   * Initialize for admin of the distributed system in  
   * application VM
   */
  public synchronized static void initializeForAdminTask() {
    if (cacheServerAdminTest == null) {
      cacheServerAdminTest = new CacheServerAdminTest();
      CacheUtil.createCache();
      cacheServerAdminTest.initializeForAdmin();
    }
  }


 /* task to add, configure and start a dedicated cache server
    in a remote VM.
 */
 public static void startCacheServerTask() {
   cacheServerAdminTest.startCacheServer();
  }
 
 protected void startCacheServer() {

   SharedCounters sc = AdminBB.getInstance().getSharedCounters();
   int maxCacheServers = TestConfig.tab().intAt(AdminPrms.maxCacheServers);
   if (sc.read(AdminBB.runningCacheServers) < maxCacheServers){
     // another thread could get in before increment runningCacheServers, but
     // the maxCacheServers limit is not a hard limit.
     sc.increment(AdminBB.runningCacheServers);
     sc.increment(AdminBB.startCacheServerRequests);
     
     // start cache server (haven't exceeded max CacheServers as specified in test config)
     String hostName = TestConfig.tab().stringAt(AdminPrms.cacheServerHost);
     String remoteCommand = TestConfig.tab().stringAt(AdminPrms.cacheServerRemoteCommand);
     
     CacheVm cacheServer;
     String cacheServerId;
     try {
       cacheServer = admin.addCacheVm();
       cacheServerId = cacheServer.getId();
       log().info("cache server added, ID is: " + cacheServerId);

     } catch (AdminException ae) {
       throw new TestException("Unable to add Cache Server");
     }
     CacheVmConfig cacheServerConfig = cacheServer.getVmConfig();
     log().info("got cache server config");
     if (hostName.equals("localhost")) {
       log().info("host is localhost");
       hostName = HostHelper.getCanonicalHostName();
     }
     log().info("host has been set");
     if (!remoteCommand.equals("none")) {
	 log().info("setting remote command to: " + remoteCommand);
	 cacheServerConfig.setRemoteCommand(remoteCommand);
	 log().info("set remote command to: " + remoteCommand);
     }

     cacheServerConfig.setHost(hostName);
      log().info("set host in config for cache server");

     // Need unique working dir for each cache server
     String workingDir = cacheServerConfig.getWorkingDirectory();
     workingDir += "/" + cacheServerId + "_" + ProcessMgr.getProcessId();
     FileUtil.mkdir(workingDir);

     // If we're running on UNIX, append "/export" to the working
     // directory and product directory so that everyone plays nice
     // with the new file system.
     if (!HostHelper.isWindows()) {
       if (!workingDir.startsWith("/export")) {
         //cacheServerConfig.setWorkingDirectory("/export" + workingDir);
         workingDir = "/export" + workingDir;
       }

       String productDir = cacheServerConfig.getProductDirectory();
       if (!productDir.startsWith("/export")) {
         cacheServerConfig.setProductDirectory("/export" + productDir);
       }
     }
     cacheServerConfig.setWorkingDirectory(workingDir);
     log().info("set working dir to: " + workingDir);
     log().info("ready to start cache server: " + cacheServerId);
     
     long timeout = 60000;
     try {
       cacheServer.start();
       if (!cacheServer.waitToStart(timeout)) {
         throw new TestException("CacheServer in " + workingDir +
                                 "not started after " 
                                  + timeout + " ms");
       }
       log().info("CacheServer " + cacheServer.getId() + " is running is: " + cacheServer.isRunning());
     } catch (Exception e) {
       throw  new TestException("Unable to start Cache Server", e);
     } 
   }  // end if 
   else {
     log().info("already started max cache servers (as specified in test config)");
   }
 }


 /* Stop a random cache server */
 public static void stopRandomCacheServerTask() {
   cacheServerAdminTest.stopRandomCacheServer();
 }

 private void stopRandomCacheServer() {

   GsRandom rng = new GsRandom();
   CacheVm[] cacheServers = getCacheServers();
   int numCacheServers = cacheServers.length;

   if (numCacheServers > 0) {
       int i = rng.nextInt(0, numCacheServers - 1);
       try {
	   log().info("cache server to stop is: " + i);
         if (cacheServers[i].isRunning()) {
           log().info("Stopping cache server: " + cacheServers[i].getId());
           cacheServers[i].stop();
           SharedCounters sc = AdminBB.getInstance().getSharedCounters();
           sc.increment(AdminBB.stopCacheServerRequests);
           sc.decrement(AdminBB.runningCacheServers);
         }
	 else {
	     log().info("No cache server stopped, selected cache server was not running");
	 }
       } catch (AdminException ae) {
         throw  new TestException("Unable to stop Cache Server: " + cacheServers[i].getId(), ae);
       } catch (IllegalStateException ise) {
         throw  new TestException("Problem with CacheServer remote command for: " 
                                      + cacheServers[i].getId(), ise);
       }
   }
     
 }
        

 
 public static void stopAllCacheServersTask() {
   //END task 
   initializeForAdminTask();
   cacheServerAdminTest.stopAllCacheServers();
 }

 protected void stopAllCacheServers() {

  CacheVm[] cacheServers = getCacheServers();

  for (int i=0; i<cacheServers.length; i++) {
    CacheVm cacheServer = cacheServers[i];
    if (cacheServer.isRunning()) {
      try {
        log().info("Stopping cache server: " + cacheServer.getId());
	cacheServer.stop();
      } catch (AdminException ae) {
	throw  new TestException("Unable to stop Cache Server: " + cacheServer.getId(), ae);
      } catch (Exception e) {
        // for now just log - there is a bug that causes there to be an extra cache server
        // Attempts to stop the "phantom" cache server fail because it has no remote command.
	log().info("Exception - trying to stop cache server: " + cacheServer.getId());
	/* After bug is fixed...
           } catch (IllegalStateException ise) {
	   throw  new TestException("Problem with CacheServer remote command for: " 
                                    + cacheServer.getId(), ise);
	*/
      }
    }
    else {
	// it's possible test has already stopped all cache servers
	log().info("No running cache servers found in stopAllCacheServers.");
    }
  }
 }


/* Find any CacheServers still running and check
   isRunning status */

public static void validateCacheServersTask() {
  
  initializeForAdminTask();
  cacheServerAdminTest.validateCacheServers();
}

protected void validateCacheServers() {

  SharedCounters sc = AdminBB.getInstance().getSharedCounters();
  long numStops = sc.read(AdminBB.stopCacheServerRequests);
  long numStarts = sc.read(AdminBB.startCacheServerRequests);
  int currRunningCacheServers = 0;

  CacheVm[] cacheServers = getCacheServers();
  for (int i=0; i < cacheServers.length; i++) {
    if (cacheServers[i].isRunning()) {
	log().info("In validate cache server: " + cacheServers[i].getId() + 
		   " isRunning status is: " + cacheServers[i].isRunning());

      currRunningCacheServers++;
    }
  }
      

  log().info("requested start " + numStarts + " cache servers");
  log().info("requested stop " + numStops + " cache servers");
  log().info("admin found " + currRunningCacheServers + " running cache servers");

}
  

public static void accessCacheServersTask() {
  cacheServerAdminTest.accessCacheServers();
}

protected void accessCacheServers() {

  CacheVm[] cacheServers = getCacheServers();
  for (int i=0; i<cacheServers.length; i++) {
    CacheVm cacheServer = cacheServers[i];
    log().info("Cache Server: " + cacheServer.toString());
    log().info("CacheServer is running: " + cacheServer.isRunning());
    log().info("type: " + cacheServer.getType());
    log().info("CacheServer classpath is: " + cacheServer.getVmConfig().getClassPath());
    log().info("product dir is: " + cacheServer.getVmConfig().getProductDirectory());
    log().info("working dir is: " + cacheServer.getVmConfig().getWorkingDirectory());
    log().info("host is: " + cacheServer.getVmConfig().getHost());
    log().info("remoteCommand is: " + cacheServer.getVmConfig().getRemoteCommand());
    log().info("CacheServer is running: " + cacheServer.isRunning());

    try {
      SystemMemberCache cache = cacheServer.getCache();
      log().info("getCache() succeeded for cache server: " + cacheServer.getId());
    } catch (AdminException ae) {
      throw new TestException("Unable to get cache for cache server: " 
                                + cacheServer.getId());
    }
  }
 }


  /* returns the system members for the distributed system */

  protected SystemMember[] getApplicationSystemMembers() {
    // get the system members
    SystemMember[] systemMembers = null;
    try {
      systemMembers = admin.getSystemMemberApplications();
    } catch (AdminException ae) {
     throw new HydraRuntimeException("Could not get SystemMemberApplications");
    }
    if (systemMembers.length == 0) {
      throw new HydraRuntimeException("Found no system members");
    } 
    else {
	log().info("DEBUG Found: " + systemMembers.length + " system members");
	return systemMembers;
    }
  }

  /* returns the  dedicated cache servers for the distributed system */

  protected CacheVm[] getCacheServers() {

    CacheVm[] cacheServers = null;
    try {
	cacheServers = admin.getCacheVms();
    } catch (AdminException ae) {
     throw new HydraRuntimeException("Could not get Cache Servers");
    }
    if (cacheServers.length == 0) {
	log().info("Admin found 0 configured cache servers");
    } 
    else {
	log().info("Admin found: " + cacheServers.length + " cacheServers");
    }
    return cacheServers;
  }

/* Creates an admin DistributedSystem and connects */
protected void initializeForAdmin() {
  log().info("Creating an admin DistributedSystem...");
  DistributedSystemConfig dsConfig; 

  Cache cache = CacheUtil.getCache();
  DistributedSystem thisVmDs = cache.getDistributedSystem();
  try {
    dsConfig = AdminDistributedSystemFactory.defineDistributedSystem(thisVmDs, null);
  } catch (AdminException ae) {
    String s = "Problem getting config for ds to which VM is currently connected";
    throw new AdminTestException (s, ae);
  }

  // Use AdminTestDistributedSystemFactory to select between admin & jmx interfaces
  try {
    admin = AdminTestDistributedSystemFactory.getDistributedSystem( dsConfig );
  } catch (AdminTestException ae) {  
    throw new HydraRuntimeException("Could not getDistributedSystem", ae);
  }
  if (admin instanceof AdminDistributedSystemImpl) {
    // Admin API
    ((AdminDistributedSystemImpl) admin).connect( Log.getLogWriter().convertToLogWriterI18n() );
  } else {
    // JMX API
    admin.connect();
  }

  // Wait for the connection to be made
  long timeout = 30 * 1000;
  try {
    if (!admin.waitToBeConnected(timeout)) {
      String s = "Could not connect after " + timeout + "ms";
      throw new TestException(s);
    }

  } catch (InterruptedException ex) {
    String s = "Interrupted while waiting to be connected";
    throw new HydraRuntimeException(s, ex);
  }
  log().info("initialize completed");
}


  protected static ConfigHashtable tab() {
    return TestConfig.tab();
  }

//==============================================================================
  protected static LogWriter log() {
    return Log.getLogWriter();
  }

}

