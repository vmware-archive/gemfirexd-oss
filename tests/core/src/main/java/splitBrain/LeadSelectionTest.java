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

/** Test class for serial and concurrent lead selection tests
 */
public class LeadSelectionTest extends SelectionTest {
    
// ========================================================================
// initialization methods
    
/** Creates and initializes the singleton instance of LeadSelectionTest 
 *  in this VM.
 */
public synchronized static void HydraTask_initializeLocator() {
   if (testInstance == null) {
      testInstance = new LeadSelectionTest();
      testInstance.initializeInstance();
   }
}
    
/** Creates and initializes the singleton instance of LeadSelectionTest 
 *  in this VM and initializes a region.
 */
public synchronized static void HydraTask_initializeClient() {
   if (testInstance == null) {
      testInstance = new LeadSelectionTest();
      testInstance.initializeRegion("clientRegion");
      testInstance.initializeInstance();
   }
}
    
// ========================================================================
// hydra task methods
    
/** Hydra task for serial lead selection test.
 */
public static void HydraTask_serialLeadSelectionTest() {
   testInstance.serialSelectionTest();
}
 
/** Hydra task for concurrent lead selection test.
 */
public static void HydraTask_concLeadSelectionTest() {
   testInstance.concSelectionTest("client");
}
 
// ========================================================================
// definitions of abstract methods

/** Given a member, wait for the lead member to become different than
 *  the given member.
 *
 *  @param member A distributed member or null. This method waits until
 *         the current lead member is different than this one.
 *  @param msToSleep The number of millis to sleep between checks for
 *         the lead member.
 *
 *  @return [0] (Long) The number of millseconds it took to detect a lead change. 
 *              Note that the value of msToSleep can affect this value.
 *          [1] (DistributedMember) The new DistributedMember
 */
protected Object[] waitForChange(DistributedMember member,
                                 int msToSleep) {
   return SBUtil.waitForLeadChange(member, msToSleep);
}

/** Return the number of client members in this test as we want to stop them all.
 */
protected int getNumToStop() {
   return getNumClients();
}

/** In a serial test, do verification after stopping a lead member.
 *
 *  @param currentLead The current lead member.
 *  @param totalNumStopped The total number of members stopped (and not
 *         yet restarted).
 */
protected void serialVerifyAfterStop(DistributedMember currentLead, int totalNumStopped) {
   int numClients = getNumClients();
   if (totalNumStopped == numClients) { // the last one
      if (currentLead != null) {
         throw new TestException("Expected new member to be null, but it is " + currentLead);
      }
   } else { // not the last one
      if (currentLead == null) {
         throw new TestException("Expected a new lead member, but it is " + currentLead);
      }
   }
}

/** Return the current lead member
 */
protected DistributedMember getCurrentMember() {
   return SBUtil.getLeadMember();
}

/** Return the total number of lead member candidates
 */
protected int getNumMembersOfInterest() {
   return getNumClients();
}

// ========================================================================
// other methods

/** Return the number of client members in this test as we want to stop them all.
 */
protected int getNumClients() {
   int numClients = 0;
   Vector aVec = TestConfig.tab().vecAt(ClientPrms.names);
   for (int i = 0; i < aVec.size(); i++) {
      String clientName = (String)(aVec.get(i));
      if (clientName.startsWith("client")) {
         numClients++;
      }
   }
   return numClients;
}

}
