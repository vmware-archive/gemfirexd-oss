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
package splitBrain;

import util.*;
import hydra.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.DistributedMember;
import java.util.*;

/** Test class for serial and concurrent member selection tests 
 *  (tests that select new lead members or new coordinators).
 */
public abstract class SelectionTest {
    
/* The singleton test instance in this VM */
static protected SelectionTest testInstance = null;

protected static final String VmIDStr = "VmId_";
protected Region aRegion;
protected long changeLimitMS; // the max number of millis to allow for a change
    
// ========================================================================
// abstract method definitions
protected abstract Object[] waitForChange(DistributedMember member, int msToSleep);
protected abstract int getNumMembersOfInterest();
protected abstract int getNumToStop();
protected abstract void serialVerifyAfterStop(DistributedMember member, int totalNumStopped);
protected abstract DistributedMember getCurrentMember();

// ========================================================================
// initialization methods
    
/** Log the current lead member and coordinator.
 */
public synchronized static void HydraTask_logMembers() {
   Log.getLogWriter().info("Current lead member is " + SBUtil.getLeadMember());
   Log.getLogWriter().info("Current coordinator is " + SBUtil.getCoordinator());
}
    
/**
 *  Create a region with the given region description name.
 *
 *  @param regDescriptName The name of a region description.
 */
protected void initializeRegion(String regDescriptName) {
   CacheHelper.createCache("cache1");
   String key = VmIDStr + RemoteTestModule.getMyVmid();
   String xmlFile = key + ".xml";
   try {
      CacheHelper.generateCacheXmlFile("cache1", regDescriptName, xmlFile);
   } catch (HydraRuntimeException e) {
      String errStr = e.toString();
      if (errStr.indexOf("Cache XML file was already created") >= 0) {
         // ok; we use this to reinitialize returning VMs, so the xml file is already there
      } else {
         throw e;
      }
   }   
   aRegion = RegionHelper.createRegion(regDescriptName);
}
    
/** Initialize the test instance.
 */
protected void initializeInstance() {
   // It must not take longer than 3 times the member-timeout value, but it
   // might take less time.
   String aStr = DistributedSystemHelper.getDistributedSystem().getProperties().
                 getProperty("member-timeout");
   int memberTimeout = (Integer.valueOf(aStr)).intValue();
   int limit = (memberTimeout * 3) + 20000;
   changeLimitMS = limit;
   Log.getLogWriter().info("changeLimitMS is " + changeLimitMS);
}

// ========================================================================
// methods to do work of hydra tasks
    
/** Serial selection test.
 *  Find the appropriate member, stop it, wait for a new member to be chosen.
 *  Repeat this until all members have been stopped.
 *  Restart all members and wait for a member to be chosen.
 */
protected void serialSelectionTest() {
   long exeNum = SplitBrainBB.getBB().getSharedCounters().incrementAndRead(SplitBrainBB.ExecutionNumber);
   Log.getLogWriter().info("Beginning task with execution number " + exeNum);
   int numToStop = getNumToStop(); 
   Log.getLogWriter().info("numToStop is " + numToStop);
   DistributedMember member = getCurrentMember();
   for (int i = 1; i <= numToStop; i++) {
      Log.getLogWriter().info("Member is " + member);
      if (member == null) {
         throw new TestException("Unexpected: member is " + member);
      }
      String host = member.getHost();
      int pid = member.getProcessId();
      ClientVmInfo targetVm = SBUtil.getClientVmInfo(host, pid);
      try {
         String stopMode = TestConfig.tab().stringAt(StopStartPrms.stopModes);
         Log.getLogWriter().info("Test is stopping " + member + " with " + stopMode);
         ClientVmMgr.stop("Stopping member " + member + " with " + stopMode,
                           ClientVmMgr.toStopMode(stopMode),
                           ClientVmMgr.ON_DEMAND,
                           targetVm);
         member = waitForChangeWithTimeout(member);
         serialVerifyAfterStop(member, i);
      } catch (ClientVmNotFoundException e) {
         throw new TestException(TestHelper.getStackTrace(e));
      }
   }
   Log.getLogWriter().info("Done stopping members");

   // restart all the stopped members
   Log.getLogWriter().info("Restarting all stopped members.");
   List threadList = new ArrayList();
   final StringBuffer errStr = new StringBuffer();
   for (int i = 1; i <= numToStop; i++) {
      Thread startThread = new HydraSubthread(new Runnable() {
         public void run() {
            try {
               ClientVmMgr.start("Restarting a random member");
            } catch (Exception e) {
               synchronized (errStr) {
                  errStr.append(TestHelper.getStackTrace(e));
               }
            }
         }
      });
      startThread.start();
      threadList.add(startThread);
   };
   for (int i = 0; i < threadList.size(); i++) {
      Thread aThread = (Thread)(threadList.get(i));
      try {
         aThread.join();
      } catch (InterruptedException e) {
         synchronized (errStr) {
            errStr.append(TestHelper.getStackTrace(e));
         }
      }
   }
   if (errStr.length() > 0) {
      throw new TestException(errStr.toString());
   }
   Log.getLogWriter().info("All members have restarted");
   member = getCurrentMember();
}

/** Concurrent selection test.
 *  Concurrently stop and restart any number of client VMs, then verify
 *  the current member.
 * 
 *  @param matchStr The members to stop must contain this String in their name.
 */
protected void concSelectionTest(String matchStr) {
   long exeNum = SplitBrainBB.getBB().getSharedCounters().incrementAndRead(SplitBrainBB.ExecutionNumber);
   Log.getLogWriter().info("Beginning task with execution number " + exeNum);
   DistributedMember member = getCurrentMember();
   Log.getLogWriter().info("The current member for this test is " + member);
   int numToStop = getNumToStop();
   int randInt =  TestConfig.tab().getRandGen().nextInt(1, numToStop);
   Object[] objArr = StopStartVMs.getOtherVMs(randInt, matchStr);
   List clientVmInfoList = (List)(objArr[0]);
   List stopModeList = (List)(objArr[1]);
   StopStartVMs.stopStartVMs(clientVmInfoList, stopModeList);
   Log.getLogWriter().info("Done restarting members");
   DistributedMember newMember = getCurrentMember();
}

// ========================================================================
// other methods
    
/** Wait for a member change. If it takes more than changeLimitMS 
 *  for the change to occur, throw a TestException.
 *
 *  @param member A distributed member or null. This method waits until
 *         the current lead member or coordinator is different than this one.
 *  @return The new member.
 */
protected DistributedMember waitForChangeWithTimeout(DistributedMember member) {
   Object[] tmpArr = waitForChange(member, 0);
   long duration = ((Long)(tmpArr[0])).longValue();
   DistributedMember newMember = (DistributedMember)(tmpArr[1]);

   // make sure the change did not take too long
   if (duration > changeLimitMS) {
      int memberTimeout = TestConfig.tab().intAt(GemFirePrms.memberTimeout);
      throw new TestException("It took " + duration + " ms to detect a member change " +
                "(member-timeout is " + memberTimeout + ")");
   }
   return newMember;
}

/** Verify that the given member exists.
 *
 */  
protected void verifyMemberExists(DistributedMember member) {
   if (member != null) {
      int pid = member.getProcessId();
      String host = member.getHost();
      boolean exists = ProcessMgr.processExists(host, pid);
      if (!exists) {
         throw new TestException("Member " + member + " is a process that does not exist");
      }  
   }
}

}
