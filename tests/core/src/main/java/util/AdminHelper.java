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
package util;

import java.util.*;

import splitBrain.SplitBrainBB;
import splitBrain.SplitBrainPrms;
import hydra.*;
import hydra.blackboard.*;

import com.gemstone.gemfire.admin.*;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;

/**
 *  Helper class to initialize a hydra client VM as an Admin VM.
 *  These VMs will be admin only; they do not host a GemFire Cache.
 *
 *  Ensure that at least one other member of the DistributedSystem is
 *  initialized prior to initializeAdminDS() as this method waits for
 *  the other members to be active before returning.  (So, using this 
 *  method as an INITTASK prior to initializing Cache Client VMs will
 *  cause HydraTask_initializeAdminDS() to hang).
 *
 *  @author Lynn Hughes-Godfrey
 *  @since 6.0
 */

public class AdminHelper {

private static AdminDistributedSystem adminDS = null;

/**
 * accessor method for the one AdminDistributedSystem for this VM
 */
public static AdminDistributedSystem getAdminDistributedSystem() {
   return adminDS;
}

/**
 * Create and connect to an admin distributed system
 */
public synchronized static void HydraTask_initializeAdminDS() {
   if (adminDS == null) {
      adminDS = initializeAdminDS();
   }
}

/** 
 * ENDTASK to wait for distributed system to reconnect
 */
public synchronized static void HydraEndTask_waitForReconnect() {
  String losingPartition = TestConfig.tab().stringAt(SplitBrainPrms.losingPartition);
  if ((losingPartition.equalsIgnoreCase("host1")) || (losingPartition.equalsIgnoreCase("host2"))) {
    String losingSideHost = TestConfig.getInstance().getHostDescription(losingPartition).getHostName();
    if (RemoteTestModule.getMyHost().equalsIgnoreCase(losingSideHost)) {
      // wait to be reconnected
      try {
        while (DistributedSystemHelper.getDistributedSystem() == null) {
          Thread.sleep(1000);
        }
      } catch (InterruptedException e) {
        return;
      }
    }
  }
}

/** Create and connect to an admin distributed system.
 */
public static AdminDistributedSystem initializeAdminDS() {

   DistributedSystemConfig dsConfig;

   Log.getLogWriter().info("Creating an admin DistributedSystem...");

   boolean adminInDsVm = AdminHelperPrms.adminInDsVm();
   if (!adminInDsVm) {
     // Treat this member as if it were a GUI console
     AdminDistributedSystemFactory.setEnableAdministrationOnly(true);

     // Create the DSconfig (from GemFire properties)
     // get multicast and disable-tcp setting from system properties
     String gemfireName = System.getProperty( GemFirePrms.GEMFIRE_NAME_PROPERTY );
     GemFireDescription gfd = TestConfig.getInstance().getGemFireDescription( gemfireName );
     Integer mcastPort = gfd.getMcastPort();
     String mcastAddress = gfd.getMcastAddress();
     Boolean disableTcp = gfd.getDisableTcp();
     Boolean disableAutoReconnect = gfd.getDisableAutoReconnect(); 
     
     Properties p = gfd.getDistributedSystemProperties();
     String locatorPortString = p.getProperty(DistributionConfig.LOCATORS_NAME);
     Log.getLogWriter().info("locatorPortString = " + locatorPortString);
     
     // get config for the distributed system
     dsConfig = AdminDistributedSystemFactory.defineDistributedSystem();
     dsConfig.setLocators( locatorPortString );
     if (TestConfig.tab().booleanAt(Prms.useIPv6)) {
       String ipv6 = HostHelper.getHostAddress(HostHelper.getIPv6Address());
       if (ipv6 == null) {
         String host = gfd.getHostDescription().getHostName();
         String s = "IPv6 address is not available for host " + host;
         throw new HydraRuntimeException(s);
       }
       dsConfig.setBindAddress(ipv6);
     }
     dsConfig.setMcastPort(mcastPort.intValue());
     dsConfig.setMcastAddress(mcastAddress);
     dsConfig.setDisableTcp(disableTcp.booleanValue());
     if (disableAutoReconnect != null) {
       dsConfig.setDisableAutoReconnect(disableAutoReconnect.booleanValue());
     }
     dsConfig.setLogFile(gfd.getSysDirName()+"/system.log");
     dsConfig.setLogLevel(gfd.getLogLevel());
   } else {
    // --Admin APIs will be used in same VM as Cache APIs
    // get config for the distributed system for admin of the distributed system 
    // to which this VM is currently connected
    Cache cache = CacheHelper.getCache();
    DistributedSystem thisVmDs = cache.getDistributedSystem();
    try {
      dsConfig = AdminDistributedSystemFactory.defineDistributedSystem(thisVmDs, null);
    } catch (AdminException ae) {
      String s = "Problem getting config for ds to which VM is currently connected";
      throw new AdminTestException (s, ae);
    }
   }
   
   // create the AdminDistributedSystem
   // Use our AdminTestDistributedSystemFactory to select between admin & jmx interfaces
   AdminDistributedSystem adminDS = AdminTestDistributedSystemFactory.getDistributedSystem( dsConfig );
   if (adminDS == null) {
     throw new HydraRuntimeException("Could not getDistributedSystem");
   }
   Log.getLogWriter().fine("getDistributedSystem returned " + adminDS.toString());
    
   // Add the SystemMembershipListener (if configured)
   // Not yet supported for JMX by our admin api facade
   if (AdminHelperPrms.getAdminInterface() == AdminHelperPrms.ADMIN) {
     SystemMembershipListener systemMembershipListener = AdminHelperPrms.getSystemMembershipListener();
     if (systemMembershipListener != null) {
       adminDS.addMembershipListener( systemMembershipListener );
     }
   } 
     
   // Add the SystemMemberCacheListener (if configured)
   // Not yet supported for JMX by our admin api facade
   if (AdminHelperPrms.getAdminInterface() == AdminHelperPrms.ADMIN) {
     SystemMemberCacheListener systemMemberCacheListener = AdminHelperPrms.getSystemMemberCacheListener();
     if (systemMemberCacheListener != null) {
       adminDS.addCacheListener( systemMemberCacheListener );
     }
   } 

   // Add the AlertListener (if configured)
   // Not yet supported for JMX by our admin api facade
   if (AdminHelperPrms.getAdminInterface() == AdminHelperPrms.ADMIN) {
      AlertListener aListener = AdminHelperPrms.getAlertListener();
      if (aListener != null) {
         adminDS.addAlertListener( aListener );
      }
   }

   // Connect to the AdminDistributedSystem
   adminDS.connect();

   // Give a few seconds to connect (so we don't miss events)
   try {
      adminDS.waitToBeConnected(3000);
   } catch (InterruptedException ie) {
      Log.getLogWriter().warning("Interrupted while waiting to connect", ie);
   }

   // Ensure that other membes of the DS are up and running before
   // we return (isRunning refers to the DS, not the Admin VM.
   dsConfig = adminDS.getConfig();
   while (true) {
     if (!adminDS.isRunning()) {
       try {
         Thread.sleep(500);
       } catch (InterruptedException ex) {
         Log.getLogWriter().warning("Interrupted while sleeping", ex);
       }
     } else
       break;
   }

   Log.getLogWriter().fine("getConfig returns " + dsConfig.toString());
   String runningStatus = new String( (adminDS.isRunning()) ? " is " :  " is not " );
   Log.getLogWriter().info("DistributedSystem "
                            + adminDS.getName() + ":" + adminDS.getId()
                            + runningStatus + "running");
 
   Log.getLogWriter().info("Exiting initializeAdminDS()");
   return adminDS;
}

/**
 * Hydra task to start JMX agent and connect it to an admin distributed system.
 * This must be invoked as an INITTASK prior to initializing an AdminDS 
 * (with adminInterface = JMX).
 */
public static void startAgentTask() {
  String agentConfig = ConfigPrms.getAgentConfig();
  AgentHelper.createAgent(agentConfig);
  AgentHelper.startAgent();
}

/** Hydra task to wait for all system members to appear. The expected
 *  number of system members is the number of VMs in the test.
 */
public synchronized static void waitForSystemMembersTask() {
   waitForSystemMembers();
}

/** Wait for the number of system members to be equal to the number of VMs
 */
protected static void waitForSystemMembers() {
   int expectedNumSystemMembers = TestHelper.getNumVMs();

   if (adminDS == null) {
      throw new TestException("AdminDistributedSystem is null.  Please verify that AdminHelper.HydraTask_initializeAdminDS was invoked prior to this call.");
   }

// todo@lhughes -- right now, we know its an AdminVM ... bring this support
// to AdminHelper when $JTESTS/admin converted to use AdminHelper
/*
   if (!adminInDsVm) {
*/
       // admin dist system is in different VM than distributed system - so deduct 1 for
       // the admin VM
     expectedNumSystemMembers--;
/*
   }
*/
   List agents = AgentHelper.getEndpoints();
   expectedNumSystemMembers -= agents.size(); // deduct JMX agent VMs
   Log.getLogWriter().info("Waiting for " + expectedNumSystemMembers + " system members");
   int numSystemMembers = 0;
   long lastLogTime = 0;
   long logInterval = 5000;
   while (true) {
      try {
         numSystemMembers = adminDS.getSystemMemberApplications().length;
         if (numSystemMembers == expectedNumSystemMembers) { // got them all
            break;
         }
	 else if (numSystemMembers > expectedNumSystemMembers) { 
	     SystemMember[] systemMembers = adminDS.getSystemMemberApplications();
	     StringBuffer aStr = new StringBuffer();
	     aStr.append("Discovered " + systemMembers.length + " SystemMembers\n");
	     for (int i=0; i<systemMembers.length; i++ ) {
		 aStr.append("   SystemMembers[" + i + "] = " + systemMembers[i].getId() + "\n");
	     }
	     Log.getLogWriter().info(aStr.toString());

	   throw new AdminTestException("Number of system members exceeds number expected.  Expected: " +              expectedNumSystemMembers + " but found: " + numSystemMembers);  
	 }
	 else if (System.currentTimeMillis() - lastLogTime >= logInterval) {
            Log.getLogWriter().info("Waiting for " + expectedNumSystemMembers + " system members, " +
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

}
