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
//import util.*;
import hydra.*;
import hydra.blackboard.*;
//import com.gemstone.gemfire.cache.*;

/** Blackboard defining counters for events. This blackboard has counters
 *  with names used by the increment methods in {@link util#AbstractWriter}.
 *  Any subclass of AbstractWriter that wants to use AbstractWriter's
 *  counter increment methods can use this blackboard for that purpose.
 *  Note that the names of the blackboard counters defined here must be
 *  the same names as defined in AbstractWriter.
 */
public class TxWriterCountersBB extends Blackboard {
   
// Blackboard variables
static String BB_NAME = "TxWriterCounters_Blackboard";
static String BB_TYPE = "RMI";

private static TxWriterCountersBB bbInstance = null;

// TxEvent counters
// afterCreate events
public static int numCreateTxEvents_isDist;
public static int numCreateTxEvents_isNotDist;
public static int numCreateTxEvents_isExp;
public static int numCreateTxEvents_isNotExp;
public static int numCreateTxEvents_isRemote;
public static int numCreateTxEvents_isNotRemote;
public static int numCreateTxEvents_isLoad;
public static int numCreateTxEvents_isNotLoad;
public static int numCreateTxEvents_isLocalLoad;
public static int numCreateTxEvents_isNotLocalLoad;
public static int numCreateTxEvents_isNetLoad;
public static int numCreateTxEvents_isNotNetLoad;
public static int numCreateTxEvents_isNetSearch;
public static int numCreateTxEvents_isNotNetSearch;

// afterDestroy events
public static int numDestroyTxEvents_isDist;
public static int numDestroyTxEvents_isNotDist;
public static int numDestroyTxEvents_isExp;
public static int numDestroyTxEvents_isNotExp;
public static int numDestroyTxEvents_isRemote;
public static int numDestroyTxEvents_isNotRemote;
public static int numDestroyTxEvents_isLoad;
public static int numDestroyTxEvents_isNotLoad;
public static int numDestroyTxEvents_isLocalLoad;
public static int numDestroyTxEvents_isNotLocalLoad;
public static int numDestroyTxEvents_isNetLoad;
public static int numDestroyTxEvents_isNotNetLoad;
public static int numDestroyTxEvents_isNetSearch;
public static int numDestroyTxEvents_isNotNetSearch;

// afterInvalidate events
public static int numInvalidateTxEvents_isDist;
public static int numInvalidateTxEvents_isNotDist;
public static int numInvalidateTxEvents_isExp;
public static int numInvalidateTxEvents_isNotExp;
public static int numInvalidateTxEvents_isRemote;
public static int numInvalidateTxEvents_isNotRemote;
public static int numInvalidateTxEvents_isLoad;
public static int numInvalidateTxEvents_isNotLoad;
public static int numInvalidateTxEvents_isLocalLoad;
public static int numInvalidateTxEvents_isNotLocalLoad;
public static int numInvalidateTxEvents_isNetLoad;
public static int numInvalidateTxEvents_isNotNetLoad;
public static int numInvalidateTxEvents_isNetSearch;
public static int numInvalidateTxEvents_isNotNetSearch;

// afterUpdate events
public static int numUpdateTxEvents_isDist;
public static int numUpdateTxEvents_isNotDist;
public static int numUpdateTxEvents_isExp;
public static int numUpdateTxEvents_isNotExp;
public static int numUpdateTxEvents_isRemote;
public static int numUpdateTxEvents_isNotRemote;
public static int numUpdateTxEvents_isLoad;
public static int numUpdateTxEvents_isNotLoad;
public static int numUpdateTxEvents_isLocalLoad;
public static int numUpdateTxEvents_isNotLocalLoad;
public static int numUpdateTxEvents_isNetLoad;
public static int numUpdateTxEvents_isNotNetLoad;
public static int numUpdateTxEvents_isNetSearch;
public static int numUpdateTxEvents_isNotNetSearch;

/**
 *  Get the EventCountersBB
 */
public static TxWriterCountersBB getBB() {
   if (bbInstance == null) {
      synchronized ( TxWriterCountersBB.class ) {
         if (bbInstance == null)
            bbInstance = new TxWriterCountersBB(BB_NAME, BB_TYPE);
      }
   }
   return bbInstance;
}
   
/**
 *  Zero-arg constructor for remote method invocations.
 */
public TxWriterCountersBB() {
}
   
/**
 *  Creates a sample blackboard using the specified name and transport type.
 */
public TxWriterCountersBB(String name, String type) {
   super(name, type, TxWriterCountersBB.class);
}

/** Increment the appropriate entry event counters.
 *
 *  @param eventName - An event name as used in this blackboard's counters, such as "TxCreate"
 *  @param isDistributed - Entry event boolean.
 *  @param isExpiration - Entry event boolean.
 *  @param isRemote - Entry event boolean.
 *  @param isLoad - Entry event boolean.
 *  @param isLocalLoad - Entry event boolean.
 *  @param isNetLoad - Entry event boolean.
 *  @param isNetSearch - Entry event boolean.
 *
 */
public static void incrementEntryTxEventCntrs(String eventName,
                                              boolean isDistributed,
                                              boolean isExpiration,
                                              boolean isRemote,
                                              boolean isLoad,
                                              boolean isLocalLoad,
                                              boolean isNetLoad,
                                              boolean isNetSearch) {
   Blackboard bb = TxWriterCountersBB.getBB();
   SharedCounters sc = bb.getSharedCounters();      
   String counterName;
 
   counterName = "num" + eventName + "TxEvents_" + (isDistributed ? "isDist" : "isNotDist");
   sc.increment(bb.getSharedCounter(counterName));
   counterName = "num" + eventName + "TxEvents_" + (isExpiration ? "isExp" : "isNotExp");
   sc.increment(bb.getSharedCounter(counterName));
   counterName = "num" + eventName + "TxEvents_" + (isRemote ? "isRemote" : "isNotRemote");
   sc.increment(bb.getSharedCounter(counterName));
   counterName = "num" + eventName + "TxEvents_" + (isLoad ? "isLoad" : "isNotLoad");
   sc.increment(bb.getSharedCounter(counterName));
   counterName = "num" + eventName + "TxEvents_" + (isLocalLoad ? "isLocalLoad" : "isNotLocalLoad");
   sc.increment(bb.getSharedCounter(counterName));
   counterName = "num" + eventName + "TxEvents_" + (isNetLoad ? "isNetLoad" : "isNotNetLoad");
   sc.increment(bb.getSharedCounter(counterName));
   counterName = "num" + eventName + "TxEvents_" + (isNetSearch ? "isNetSearch" : "isNotNetSearch");
   sc.increment(bb.getSharedCounter(counterName));
}

/**
 *  Check the value of all tx event counters.
 *
 *  @param expectedValues An ArrayList of instances of ExpCounterValue.
 *
 *  @throws TestException if any counter does not have the expected value.
 */
public void checkEventCounters(ArrayList expectedValues) {
   Log.getLogWriter().info("Checking " + expectedValues.size() + " tx event counters in " + this.getClass().getName());
   Blackboard BB = getBB();
   SharedCounters counters = BB.getSharedCounters();
   String[] counterNames = BB.getCounterNames();

   for (int i = 0; i < expectedValues.size(); i++) {
      ExpCounterValue expValue = (ExpCounterValue)expectedValues.get(i);
      if ((expValue.getCounterName1() != null) && (expValue.getCounterName2() != null)) { // want sum of counters
         try {
            TestHelper.waitForCounterSum(BB, expValue.getCounterName1(), expValue.getCounterName2(),
                       expValue.getExpectedValue(), true, 60000);
         } catch (TestException e) {
            BB.printSharedCounters();
            throw e;
         }
      } else { // only one counterName has a value
         boolean exact = expValue.getExact();
         try {
            TestHelper.waitForCounter(BB, expValue.getCounterName1(), 
                       BB.getSharedCounter(expValue.getCounterName1()), expValue.getExpectedValue(), 
                       exact, 60000);
         } catch (TestException e) {
            BB.printSharedCounters();
            throw e;
         }
      }
   }
   Log.getLogWriter().info(BB.getClass().getName() + ", all counters are OK");
}

/**
 *  Check the value of all tx event counters.
 *
 *  @param expectedValues An array of the expected counter values, in the
 *         same order as they are defined in this class. If any value is
 *         < 0, then don't check it.
 *
 *  @throws TestException if any counter does not have the expected value.
 */
public void checkTxWriterCounters(long[] expectedValues) {
   Log.getLogWriter().info("Checking tx event counters in " + this.getClass().getName());
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
                       true, 60000);
         } catch (TestException e) {
            BB.printSharedCounters();
            throw e;
         }
   }
   Log.getLogWriter().info(BB.getClass().getName() + ", all counters are OK");
}

/**
 *  Check the value of update/create combined.
 */
public static void checkCreateUpdate(long expectedIsDist,
                                     long expectedIsNotDist,
                                     long expectedIsExp,
                                     long expectedIsNotExp,
                                     long expectedIsRemote,
                                     long expectedIsNotRemote) {
   Blackboard BB = EventCountersBB.getBB();
   SharedCounters counters = EventCountersBB.getBB().getSharedCounters();
   long isDist = counters.read(numCreateTxEvents_isDist) + counters.read(numUpdateTxEvents_isDist);
   long isNotDist = counters.read(numCreateTxEvents_isNotDist) + counters.read(numUpdateTxEvents_isNotDist);
   long isExp = counters.read(numCreateTxEvents_isExp) + counters.read(numUpdateTxEvents_isExp);
   long isNotExp = counters.read(numCreateTxEvents_isNotExp) + counters.read(numUpdateTxEvents_isNotExp);
   long isRemote = counters.read(numCreateTxEvents_isRemote) + counters.read(numUpdateTxEvents_isRemote);
   long isNotRemote = counters.read(numCreateTxEvents_isNotRemote) + counters.read(numUpdateTxEvents_isNotRemote);
   if (isDist != expectedIsDist) {
      BB.print();
      throw new TestException("Expected create/update sum of isDist to be " + expectedIsDist + ", but it is " + isDist);
   }
   if (isNotDist != expectedIsNotDist) {
      BB.print();
      throw new TestException("Expected create/update sum of isNotDist to be " + expectedIsNotDist + ", but it is " + isNotDist);
   }
   if (isExp != expectedIsExp) {
      BB.print();
      throw new TestException("Expected create/update sum of isExp to be " + expectedIsExp + ", but it is " + isExp);
   }
   if (isNotExp != expectedIsNotExp) {
      BB.print();
      throw new TestException("Expected create/update sum of isNotExp to be " + expectedIsNotExp + ", but it is " + isNotExp);
   }
   if (isRemote != expectedIsRemote) {
      BB.print();
      throw new TestException("Expected create/update sum of isRemote to be " + expectedIsRemote + ", but it is " + isRemote);
   }
   if (isNotRemote != expectedIsNotRemote) {
      BB.print();
      throw new TestException("Expected create/update sum of isNotRemote to be " + expectedIsNotRemote + ", but it is " + isNotRemote);
   }
   Log.getLogWriter().info("EventCountersBB, create/update counters are OK");
}

/** Zero all counters in this blackboard.
 */
public void zeroAllCounters() {
   SharedCounters sc = getSharedCounters();      
   sc.zero(numCreateTxEvents_isDist);
   sc.zero(numCreateTxEvents_isNotDist);
   sc.zero(numCreateTxEvents_isExp);
   sc.zero(numCreateTxEvents_isNotExp);
   sc.zero(numCreateTxEvents_isRemote);
   sc.zero(numCreateTxEvents_isNotRemote);
   sc.zero(numCreateTxEvents_isLoad);
   sc.zero(numCreateTxEvents_isNotLoad);
   sc.zero(numCreateTxEvents_isLocalLoad);
   sc.zero(numCreateTxEvents_isNotLocalLoad);
   sc.zero(numCreateTxEvents_isNetLoad);
   sc.zero(numCreateTxEvents_isNotNetLoad);
   sc.zero(numCreateTxEvents_isNetSearch);
   sc.zero(numCreateTxEvents_isNotNetSearch);
   sc.zero(numDestroyTxEvents_isDist);
   sc.zero(numDestroyTxEvents_isNotDist);
   sc.zero(numDestroyTxEvents_isExp);
   sc.zero(numDestroyTxEvents_isNotExp);
   sc.zero(numDestroyTxEvents_isRemote);
   sc.zero(numDestroyTxEvents_isNotRemote);
   sc.zero(numDestroyTxEvents_isLoad);
   sc.zero(numDestroyTxEvents_isNotLoad);
   sc.zero(numDestroyTxEvents_isLocalLoad);
   sc.zero(numDestroyTxEvents_isNotLocalLoad);
   sc.zero(numDestroyTxEvents_isNetLoad);
   sc.zero(numDestroyTxEvents_isNotNetLoad);
   sc.zero(numDestroyTxEvents_isNetSearch);
   sc.zero(numDestroyTxEvents_isNotNetSearch);
   sc.zero(numInvalidateTxEvents_isDist);
   sc.zero(numInvalidateTxEvents_isNotDist);
   sc.zero(numInvalidateTxEvents_isExp);
   sc.zero(numInvalidateTxEvents_isNotExp);
   sc.zero(numInvalidateTxEvents_isRemote);
   sc.zero(numInvalidateTxEvents_isNotRemote);
   sc.zero(numInvalidateTxEvents_isLoad);
   sc.zero(numInvalidateTxEvents_isNotLoad);
   sc.zero(numInvalidateTxEvents_isLocalLoad);
   sc.zero(numInvalidateTxEvents_isNotLocalLoad);
   sc.zero(numInvalidateTxEvents_isNetLoad);
   sc.zero(numInvalidateTxEvents_isNotNetLoad);
   sc.zero(numInvalidateTxEvents_isNetSearch);
   sc.zero(numInvalidateTxEvents_isNotNetSearch);
   sc.zero(numUpdateTxEvents_isDist);
   sc.zero(numUpdateTxEvents_isNotDist);
   sc.zero(numUpdateTxEvents_isExp);
   sc.zero(numUpdateTxEvents_isNotExp);
   sc.zero(numUpdateTxEvents_isRemote);
   sc.zero(numUpdateTxEvents_isNotRemote);
   sc.zero(numUpdateTxEvents_isLoad);
   sc.zero(numUpdateTxEvents_isNotLoad);
   sc.zero(numUpdateTxEvents_isLocalLoad);
   sc.zero(numUpdateTxEvents_isNotLocalLoad);
   sc.zero(numUpdateTxEvents_isNetLoad);
   sc.zero(numUpdateTxEvents_isNotNetLoad);
   sc.zero(numUpdateTxEvents_isNetSearch);
   sc.zero(numUpdateTxEvents_isNotNetSearch);
}

}
