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
import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.DistributedMember;
import java.util.*;

/** Test class for serial and concurrent coordinator selection tests
 */
public class CoordSelectionTest extends SelectionTest {
    
// ========================================================================
// initialization methods
    
/** Creates and initializes the singleton instance of CoordSelectionTest 
 *  in this VM.
 */
public synchronized static void HydraTask_initializeLocator() {
   if (testInstance == null) {
      testInstance = new CoordSelectionTest();
      testInstance.initializeInstance();
   }
}
    
/** Creates and initializes the singleton instance of CoordSelectionTest 
 *  in this VM and initializes a region.
 */
public synchronized static void HydraTask_initializeClient() {
   if (testInstance == null) {
      testInstance = new CoordSelectionTest();
      testInstance.initializeRegion("clientRegion");
      testInstance.initializeInstance();
   }
}
    
// ========================================================================
// hydra task methods
    
/** Hydra task for serial coordinator selection test.
 */
public static void HydraTask_serialCoordSelectionTest() {
   testInstance.serialSelectionTest();
}
 
/** Hydra task for concurrent coordinator selection test.
 */
public static void HydraTask_concCoordSelectionTest() {
   testInstance.concSelectionTest("locator");
}
 
/** Keep a client busy with puts. We should not become disconnected from
 *  the DS (the puts should always succeed). 
 */
public static void HydraTask_busyClient() {
   long minTaskGranularitySec = TestConfig.tab().longAt(TestHelperPrms.minTaskGranularitySec, 15);
   long minTaskGranularityMS = minTaskGranularitySec * TestHelper.SEC_MILLI_FACTOR;
   long startTime = System.currentTimeMillis();
   do {
      testInstance.aRegion.put("key", "value");
      MasterController.sleepForMs(200);
   } while (System.currentTimeMillis() - startTime < minTaskGranularityMS);
}
 
// ========================================================================
// definitions of abstract methods

/** Given a member, wait for the coordinator to become different than
 *  the given member.
 *
 *  @param member A distributed member or null. This method waits until
 *         the current coordinator is different than this one.
 *  @param msToSleep The number of millis to sleep between checks for
 *         the coordinator.
 *
 *  @return [0] (Long) The number of millseconds it took to detect a 
 *              coordinator change. 
 *              Note that the value of msToSleep can affect this value.
 *          [1] (DistributedMember) The new DistributedMember
 */
protected Object[] waitForChange(DistributedMember member,
                                 int msToSleep) {
   return SBUtil.waitForCoordChange(member, msToSleep);
}

/** Return the appropriate number of locators to stop.
 *  Always make sure there is one locator present.
 */
protected int getNumToStop() {
   int numLocators = getNumLocators();
   return numLocators-1;
}

/** In a serial test, do verification after stopping a coordinator.
 *
 *  @param currentCoord The current coordinator.
 *  @param totalNumStopped The total number of members stopped (and not
 *         yet restarted).
 */
protected void serialVerifyAfterStop(DistributedMember currentCoord, int totalNumStopped) {
   if (currentCoord == null) {
      throw new TestException("Expected a new coordinator, but it is " + currentCoord);
   }
}

/** Return the current coordinator.
 */
protected DistributedMember getCurrentMember() {
   return SBUtil.getCoordinator();
}

/** Return the total number of coordinators
 */
protected int getNumMembersOfInterest() {
   return getNumLocators();
}

// ========================================================================
// other methods

/** Return the number of locators.
 */
protected int getNumLocators() {
   int num = 0;
   Vector aVec = TestConfig.tab().vecAt(ClientPrms.names);
   for (int i = 0; i < aVec.size(); i++) {
      String clientName = (String)(aVec.get(i));
      if (clientName.startsWith("locator")) {
         num++;
      }
   }
   return num;
}

}
