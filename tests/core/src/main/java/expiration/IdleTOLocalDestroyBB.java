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
package expiration;

//import java.util.*;
import util.*;
import hydra.*;
import hydra.blackboard.*;
//import com.gemstone.gemfire.cache.*;

/** Blackboard defining counters for events. This blackboard has counters
 *  with names used by the increment methods in {@link util#AbstractListener}.
 *  Any subclass of AbstractListener that wants to use AbstractListener's
 *  counter increment methods can use this blackboard for that purpose.
 *  Note that the names of the blackboard counters defined here must be
 *  the same names as defined in AbstractListener.
 */
public class IdleTOLocalDestroyBB extends Blackboard {
   
// Blackboard variables
static String BB_NAME = "IdleTOLocalDestroyBB_Blackboard";
static String BB_TYPE = "RMI";

// singleton instance of blackboard
private static IdleTOLocalDestroyBB bbInstance = null;

// Event counters
// afterCreate events
public static int numAfterCreateEvents_isDist;
public static int numAfterCreateEvents_isNotDist;
public static int numAfterCreateEvents_isExp;
public static int numAfterCreateEvents_isNotExp;
public static int numAfterCreateEvents_isRemote;
public static int numAfterCreateEvents_isNotRemote;
public static int numAfterCreateEvents_isLoad;
public static int numAfterCreateEvents_isNotLoad;
public static int numAfterCreateEvents_isLocalLoad;
public static int numAfterCreateEvents_isNotLocalLoad;
public static int numAfterCreateEvents_isNetLoad;
public static int numAfterCreateEvents_isNotNetLoad;
public static int numAfterCreateEvents_isNetSearch;
public static int numAfterCreateEvents_isNotNetSearch;

// afterDestroy events
public static int numAfterDestroyEvents_isDist;
public static int numAfterDestroyEvents_isNotDist;
public static int numAfterDestroyEvents_isExp;
public static int numAfterDestroyEvents_isNotExp;
public static int numAfterDestroyEvents_isRemote;
public static int numAfterDestroyEvents_isNotRemote;
public static int numAfterDestroyEvents_isLoad;
public static int numAfterDestroyEvents_isNotLoad;
public static int numAfterDestroyEvents_isLocalLoad;
public static int numAfterDestroyEvents_isNotLocalLoad;
public static int numAfterDestroyEvents_isNetLoad;
public static int numAfterDestroyEvents_isNotNetLoad;
public static int numAfterDestroyEvents_isNetSearch;
public static int numAfterDestroyEvents_isNotNetSearch;

// afterInvalidate events
public static int numAfterInvalidateEvents_isDist;
public static int numAfterInvalidateEvents_isNotDist;
public static int numAfterInvalidateEvents_isExp;
public static int numAfterInvalidateEvents_isNotExp;
public static int numAfterInvalidateEvents_isRemote;
public static int numAfterInvalidateEvents_isNotRemote;
public static int numAfterInvalidateEvents_isLoad;
public static int numAfterInvalidateEvents_isNotLoad;
public static int numAfterInvalidateEvents_isLocalLoad;
public static int numAfterInvalidateEvents_isNotLocalLoad;
public static int numAfterInvalidateEvents_isNetLoad;
public static int numAfterInvalidateEvents_isNotNetLoad;
public static int numAfterInvalidateEvents_isNetSearch;
public static int numAfterInvalidateEvents_isNotNetSearch;

// afterUpdate events
public static int numAfterUpdateEvents_isDist;
public static int numAfterUpdateEvents_isNotDist;
public static int numAfterUpdateEvents_isExp;
public static int numAfterUpdateEvents_isNotExp;
public static int numAfterUpdateEvents_isRemote;
public static int numAfterUpdateEvents_isNotRemote;
public static int numAfterUpdateEvents_isLoad;
public static int numAfterUpdateEvents_isNotLoad;
public static int numAfterUpdateEvents_isLocalLoad;
public static int numAfterUpdateEvents_isNotLocalLoad;
public static int numAfterUpdateEvents_isNetLoad;
public static int numAfterUpdateEvents_isNotNetLoad;
public static int numAfterUpdateEvents_isNetSearch;
public static int numAfterUpdateEvents_isNotNetSearch;

// afterRegionDestroy events
public static int numAfterRegionDestroyEvents_isDist;
public static int numAfterRegionDestroyEvents_isNotDist;
public static int numAfterRegionDestroyEvents_isExp;
public static int numAfterRegionDestroyEvents_isNotExp;
public static int numAfterRegionDestroyEvents_isRemote;
public static int numAfterRegionDestroyEvents_isNotRemote;

// afterRegionInvalidate events
public static int numAfterRegionInvalidateEvents_isDist;
public static int numAfterRegionInvalidateEvents_isNotDist;
public static int numAfterRegionInvalidateEvents_isExp;
public static int numAfterRegionInvalidateEvents_isNotExp;
public static int numAfterRegionInvalidateEvents_isRemote;
public static int numAfterRegionInvalidateEvents_isNotRemote;

// close events
public static int numClose;

/**
 *  Get the IdleTOLocalDestroyBB
 */
public static IdleTOLocalDestroyBB getBB() {
   if (bbInstance == null) {
      synchronized ( IdleTOLocalDestroyBB.class ) {
         if (bbInstance == null) 
            bbInstance = new IdleTOLocalDestroyBB(BB_NAME, BB_TYPE);
      }
   }
   return bbInstance;
}
   
/**
 *  Zero-arg constructor for remote method invocations.
 */
public IdleTOLocalDestroyBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public IdleTOLocalDestroyBB(String name, String type) {
   super(name, type, IdleTOLocalDestroyBB.class);
}

/**
 *  Check the value of all event counters.
 *
 *  @param expectedValues An array of the expected counter values, in the
 *         same order as they are defined in this class. If any value is
 *         < 0, then don't check it.
 *
 *  @throws TestException if any counter does not have the expected value.
 */
public void checkEventCounters(long[] expectedValues) {
   Log.getLogWriter().info("Checking event counters in " + this.getClass().getName());
   Blackboard BB = getBB();
   String[] counterNames = BB.getCounterNames();

   // make sure expectedValues is same length as number of counters in this BB
   if (counterNames.length != expectedValues.length) { 
      StringBuffer aStr = new StringBuffer();
      for (int i = 0; i < counterNames.length; i++) 
          aStr.append("   counterNames[" + i + "] is " + counterNames[i] + "\n");
      for (int i = 0; i < expectedValues.length; i++) 
          aStr.append("   expectedValues[" + i + "] is " + expectedValues[i] + "\n");
      Log.getLogWriter().info(aStr.toString());
      throw new TestException("Expected length of expectedValues " + expectedValues.length +
            " to be = length of counterNames " + counterNames.length);
   }

   SharedCounters counters = BB.getSharedCounters();
   for (int i = 0; i < expectedValues.length; i++) {
      if (expectedValues[i] >= 0) 
         try {
            TestHelper.waitForCounter(BB, counterNames[i], 
                       BB.getSharedCounter(counterNames[i]), expectedValues[i], 
                       true, event.EventTest.MILLIS_TO_WAIT);
         } catch (TestException e) {
            BB.printSharedCounters();
            throw e;
         }
   }
   Log.getLogWriter().info(BB.getClass().getName() + ", all counters are OK");
}

/** Zero all counters in this blackboard.
 */
public void zeroAllCounters() {
   SharedCounters sc = getSharedCounters();      
   sc.zero(numAfterCreateEvents_isDist);
   sc.zero(numAfterCreateEvents_isNotDist);
   sc.zero(numAfterCreateEvents_isExp);
   sc.zero(numAfterCreateEvents_isNotExp);
   sc.zero(numAfterCreateEvents_isRemote);
   sc.zero(numAfterCreateEvents_isNotRemote);
   sc.zero(numAfterCreateEvents_isLoad);
   sc.zero(numAfterCreateEvents_isNotLoad);
   sc.zero(numAfterCreateEvents_isLocalLoad);
   sc.zero(numAfterCreateEvents_isNotLocalLoad);
   sc.zero(numAfterCreateEvents_isNetLoad);
   sc.zero(numAfterCreateEvents_isNotNetLoad);
   sc.zero(numAfterCreateEvents_isNetSearch);
   sc.zero(numAfterCreateEvents_isNotNetSearch);
   sc.zero(numAfterDestroyEvents_isDist);
   sc.zero(numAfterDestroyEvents_isNotDist);
   sc.zero(numAfterDestroyEvents_isExp);
   sc.zero(numAfterDestroyEvents_isNotExp);
   sc.zero(numAfterDestroyEvents_isRemote);
   sc.zero(numAfterDestroyEvents_isNotRemote);
   sc.zero(numAfterDestroyEvents_isLoad);
   sc.zero(numAfterDestroyEvents_isNotLoad);
   sc.zero(numAfterDestroyEvents_isLocalLoad);
   sc.zero(numAfterDestroyEvents_isNotLocalLoad);
   sc.zero(numAfterDestroyEvents_isNetLoad);
   sc.zero(numAfterDestroyEvents_isNotNetLoad);
   sc.zero(numAfterDestroyEvents_isNetSearch);
   sc.zero(numAfterDestroyEvents_isNotNetSearch);
   sc.zero(numAfterInvalidateEvents_isDist);
   sc.zero(numAfterInvalidateEvents_isNotDist);
   sc.zero(numAfterInvalidateEvents_isExp);
   sc.zero(numAfterInvalidateEvents_isNotExp);
   sc.zero(numAfterInvalidateEvents_isRemote);
   sc.zero(numAfterInvalidateEvents_isNotRemote);
   sc.zero(numAfterInvalidateEvents_isLoad);
   sc.zero(numAfterInvalidateEvents_isNotLoad);
   sc.zero(numAfterInvalidateEvents_isLocalLoad);
   sc.zero(numAfterInvalidateEvents_isNotLocalLoad);
   sc.zero(numAfterInvalidateEvents_isNetLoad);
   sc.zero(numAfterInvalidateEvents_isNotNetLoad);
   sc.zero(numAfterInvalidateEvents_isNetSearch);
   sc.zero(numAfterInvalidateEvents_isNotNetSearch);
   sc.zero(numAfterUpdateEvents_isDist);
   sc.zero(numAfterUpdateEvents_isNotDist);
   sc.zero(numAfterUpdateEvents_isExp);
   sc.zero(numAfterUpdateEvents_isNotExp);
   sc.zero(numAfterUpdateEvents_isRemote);
   sc.zero(numAfterUpdateEvents_isNotRemote);
   sc.zero(numAfterUpdateEvents_isLoad);
   sc.zero(numAfterUpdateEvents_isNotLoad);
   sc.zero(numAfterUpdateEvents_isLocalLoad);
   sc.zero(numAfterUpdateEvents_isNotLocalLoad);
   sc.zero(numAfterUpdateEvents_isNetLoad);
   sc.zero(numAfterUpdateEvents_isNotNetLoad);
   sc.zero(numAfterUpdateEvents_isNetSearch);
   sc.zero(numAfterUpdateEvents_isNotNetSearch);
   sc.zero(numAfterRegionDestroyEvents_isDist);
   sc.zero(numAfterRegionDestroyEvents_isNotDist);
   sc.zero(numAfterRegionDestroyEvents_isExp);
   sc.zero(numAfterRegionDestroyEvents_isNotExp);
   sc.zero(numAfterRegionDestroyEvents_isRemote);
   sc.zero(numAfterRegionDestroyEvents_isNotRemote);
   sc.zero(numAfterRegionInvalidateEvents_isDist);
   sc.zero(numAfterRegionInvalidateEvents_isNotDist);
   sc.zero(numAfterRegionInvalidateEvents_isExp);
   sc.zero(numAfterRegionInvalidateEvents_isNotExp);
   sc.zero(numAfterRegionInvalidateEvents_isRemote);
   sc.zero(numAfterRegionInvalidateEvents_isNotRemote);
   sc.zero(numClose);
}

}
