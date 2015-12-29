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
 *  with names used by the increment methods in {@link util#AbstractListener}.
 *  Any subclass of AbstractListener that wants to use AbstractListener's
 *  counter increment methods can use this blackboard for that purpose.
 *  Note that the names of the blackboard counters defined here must be
 *  the same names as defined in AbstractListener.
 */
public class EventCountersBB extends Blackboard {
    
    // Blackboard variables
    static String BB_NAME = "EventCounters_Blackboard";
    static String BB_TYPE = "RMI";

    // singleton instance of blackboard
    private static EventCountersBB bbInstance;
    
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
    
    // afterRegionCreate events
    public static int numAfterRegionCreateEvents_isDist;
    public static int numAfterRegionCreateEvents_isNotDist;
    public static int numAfterRegionCreateEvents_isExp;
    public static int numAfterRegionCreateEvents_isNotExp;
    public static int numAfterRegionCreateEvents_isRemote;
    public static int numAfterRegionCreateEvents_isNotRemote;
    
    // close events
    public static int numClose;
    
    //afterClear events
    public static int numAfterClearEvents_isDist;
    public static int numAfterClearEvents_isNotDist;
    public static int numAfterClearEvents_isExp;
    public static int numAfterClearEvents_isNotExp;
    public static int numAfterClearEvents_isRemote;
    public static int numAfterClearEvents_isNotRemote;
    
    /**
     *  Get the EventCountersBB
     */
    public static EventCountersBB getBB() {
        if (bbInstance == null) {
           synchronized ( EventCountersBB.class ) {
              if (bbInstance == null)
                 bbInstance = new EventCountersBB(BB_NAME, BB_TYPE);
           }
        }
        return bbInstance;
    }
    
    /**
     *  Zero-arg constructor for remote method invocations.
     */
    public EventCountersBB() {
    }
    
    /**
     *  Creates a sample blackboard using the specified name and transport type.
     */
    public EventCountersBB(String name, String type) {
        super(name, type, EventCountersBB.class);
    }
    
    /** Increment the appropriate entry event counters.
     *
     *  @param eventName - An event name as used in this blackboard's counters, such as "AfterCreate"
     *  @param isDistributed - Entry event boolean.
     *  @param isExpiration - Entry event boolean.
     *  @param isRemote - Entry event boolean.
     *  @param isLoad - Entry event boolean.
     *  @param isLocalLoad - Entry event boolean.
     *  @param isNetLoad - Entry event boolean.
     *  @param isNetSearch - Entry event boolean.
     *
     */
    public static void incrementEntryEventCntrs(String eventName,
            boolean isDistributed,
            boolean isExpiration,
            boolean isRemote,
            boolean isLoad,
            boolean isLocalLoad,
            boolean isNetLoad,
            boolean isNetSearch) {
        Blackboard bb = EventCountersBB.getBB();
        SharedCounters sc = bb.getSharedCounters();
        String counterName;
        
        counterName = "num" + eventName + "Events_" + (isDistributed ? "isDist" : "isNotDist");
        sc.increment(bb.getSharedCounter(counterName));
        counterName = "num" + eventName + "Events_" + (isExpiration ? "isExp" : "isNotExp");
        sc.increment(bb.getSharedCounter(counterName));
        counterName = "num" + eventName + "Events_" + (isRemote ? "isRemote" : "isNotRemote");
        sc.increment(bb.getSharedCounter(counterName));
        counterName = "num" + eventName + "Events_" + (isLoad ? "isLoad" : "isNotLoad");
        sc.increment(bb.getSharedCounter(counterName));
        counterName = "num" + eventName + "Events_" + (isLocalLoad ? "isLocalLoad" : "isNotLocalLoad");
        sc.increment(bb.getSharedCounter(counterName));
        counterName = "num" + eventName + "Events_" + (isNetLoad ? "isNetLoad" : "isNotNetLoad");
        sc.increment(bb.getSharedCounter(counterName));
        counterName = "num" + eventName + "Events_" + (isNetSearch ? "isNetSearch" : "isNotNetSearch");
        sc.increment(bb.getSharedCounter(counterName));
    }
    
    /** Increment the appropriate region event counters.
     *
     *  @param eventName - An event name as used in this blackboard's counters, such as "RegionDestroy"
     *  @param isDistributed - Entry event boolean.
     *  @param isExpiration - Entry event boolean.
     *  @param isRemote - Entry event boolean.
     *
     */
    public static void incrementRegionEventCntrs(String eventName,
            boolean isDistributed,
            boolean isExpiration,
            boolean isRemote) {
        Blackboard bb = EventCountersBB.getBB();
        SharedCounters sc = bb.getSharedCounters();
        String counterName;
        
        counterName = "num" + eventName + "Events_" + (isDistributed ? "isDist" : "isNotDist");
        sc.increment(bb.getSharedCounter(counterName));
        counterName = "num" + eventName + "Events_" + (isExpiration ? "isExp" : "isNotExp");
        sc.increment(bb.getSharedCounter(counterName));
        counterName = "num" + eventName + "Events_" + (isRemote ? "isRemote" : "isNotRemote");
        sc.increment(bb.getSharedCounter(counterName));
    }
    
    /**
     *  Check the value of all event counters.
     *
     *  @param expectedValues An ArrayList of instances of ExpCounterValue.
     *
     *  @throws TestException if any counter does not have the expected value.
     */
    public void checkEventCounters(ArrayList expectedValues) {
        Log.getLogWriter().info("Checking " + expectedValues.size() + " event counters in " + this.getClass().getName());
        Blackboard BB = getBB();
        SharedCounters counters = BB.getSharedCounters();
        String[] counterNames = BB.getCounterNames();
        
        for (int i = 0; i < expectedValues.size(); i++) {
            ExpCounterValue expValue = (ExpCounterValue)expectedValues.get(i);
            if ((expValue.getCounterName1() != null) && (expValue.getCounterName2() != null)) { // want sum of counters
                try {
                    TestHelper.waitForCounterSum(BB, expValue.getCounterName1(), expValue.getCounterName2(),
                            expValue.getExpectedValue(), true, event.EventTest.MILLIS_TO_WAIT);
                } catch (TestException e) {
                    BB.printSharedCounters();
                    throw e;
                }
            } else { // only one counterName has a value
                boolean exact = expValue.getExact();
                try {
                    TestHelper.waitForCounter(BB, expValue.getCounterName1(),
                            BB.getSharedCounter(expValue.getCounterName1()), expValue.getExpectedValue(),
                            exact, event.EventTest.MILLIS_TO_WAIT);
                } catch (TestException e) {
                    BB.printSharedCounters();
                    throw e;
                }
            }
        }
        Log.getLogWriter().info(BB.getClass().getName() + ", all counters are OK");
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
        long isDist = counters.read(numAfterCreateEvents_isDist) + counters.read(numAfterUpdateEvents_isDist);
        long isNotDist = counters.read(numAfterCreateEvents_isNotDist) + counters.read(numAfterUpdateEvents_isNotDist);
        long isExp = counters.read(numAfterCreateEvents_isExp) + counters.read(numAfterUpdateEvents_isExp);
        long isNotExp = counters.read(numAfterCreateEvents_isNotExp) + counters.read(numAfterUpdateEvents_isNotExp);
        long isRemote = counters.read(numAfterCreateEvents_isRemote) + counters.read(numAfterUpdateEvents_isRemote);
        long isNotRemote = counters.read(numAfterCreateEvents_isNotRemote) + counters.read(numAfterUpdateEvents_isNotRemote);
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
        sc.zero(numAfterRegionCreateEvents_isDist);
        sc.zero(numAfterRegionCreateEvents_isNotDist);
        sc.zero(numAfterRegionCreateEvents_isExp);
        sc.zero(numAfterRegionCreateEvents_isNotExp);
        sc.zero(numAfterRegionCreateEvents_isRemote);
        sc.zero(numAfterRegionCreateEvents_isNotRemote);
        sc.zero(numClose);
        sc.zero(numAfterClearEvents_isDist);
        sc.zero(numAfterClearEvents_isNotDist);
        sc.zero(numAfterClearEvents_isExp);
        sc.zero(numAfterClearEvents_isNotExp);
        sc.zero(numAfterClearEvents_isRemote);
        sc.zero(numAfterClearEvents_isNotRemote);
    }
    
}
