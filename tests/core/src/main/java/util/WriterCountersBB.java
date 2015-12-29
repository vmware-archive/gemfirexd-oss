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
public class WriterCountersBB extends Blackboard {
    
    // Blackboard variables
    static String BB_NAME = "WriterCounters_Blackboard";
    static String BB_TYPE = "RMI";

    private static WriterCountersBB bbInstance = null;
    
    // Event counters
    // beforeCreate events
    public static int numBeforeCreateEvents_isDist;
    public static int numBeforeCreateEvents_isNotDist;
    public static int numBeforeCreateEvents_isExp;
    public static int numBeforeCreateEvents_isNotExp;
    public static int numBeforeCreateEvents_isRemote;
    public static int numBeforeCreateEvents_isNotRemote;
    public static int numBeforeCreateEvents_isLoad;
    public static int numBeforeCreateEvents_isNotLoad;
    public static int numBeforeCreateEvents_isLocalLoad;
    public static int numBeforeCreateEvents_isNotLocalLoad;
    public static int numBeforeCreateEvents_isNetLoad;
    public static int numBeforeCreateEvents_isNotNetLoad;
    public static int numBeforeCreateEvents_isNetSearch;
    public static int numBeforeCreateEvents_isNotNetSearch;
    
    // beforeDestroy events
    public static int numBeforeDestroyEvents_isDist;
    public static int numBeforeDestroyEvents_isNotDist;
    public static int numBeforeDestroyEvents_isExp;
    public static int numBeforeDestroyEvents_isNotExp;
    public static int numBeforeDestroyEvents_isRemote;
    public static int numBeforeDestroyEvents_isNotRemote;
    public static int numBeforeDestroyEvents_isLoad;
    public static int numBeforeDestroyEvents_isNotLoad;
    public static int numBeforeDestroyEvents_isLocalLoad;
    public static int numBeforeDestroyEvents_isNotLocalLoad;
    public static int numBeforeDestroyEvents_isNetLoad;
    public static int numBeforeDestroyEvents_isNotNetLoad;
    public static int numBeforeDestroyEvents_isNetSearch;
    public static int numBeforeDestroyEvents_isNotNetSearch;
    
    // beforeUpdate events
    public static int numBeforeUpdateEvents_isDist;
    public static int numBeforeUpdateEvents_isNotDist;
    public static int numBeforeUpdateEvents_isExp;
    public static int numBeforeUpdateEvents_isNotExp;
    public static int numBeforeUpdateEvents_isRemote;
    public static int numBeforeUpdateEvents_isNotRemote;
    public static int numBeforeUpdateEvents_isLoad;
    public static int numBeforeUpdateEvents_isNotLoad;
    public static int numBeforeUpdateEvents_isLocalLoad;
    public static int numBeforeUpdateEvents_isNotLocalLoad;
    public static int numBeforeUpdateEvents_isNetLoad;
    public static int numBeforeUpdateEvents_isNotNetLoad;
    public static int numBeforeUpdateEvents_isNetSearch;
    public static int numBeforeUpdateEvents_isNotNetSearch;
    
    // beforeRegionDestroy events
    public static int numBeforeRegionDestroyEvents_isDist;
    public static int numBeforeRegionDestroyEvents_isNotDist;
    public static int numBeforeRegionDestroyEvents_isExp;
    public static int numBeforeRegionDestroyEvents_isNotExp;
    public static int numBeforeRegionDestroyEvents_isRemote;
    public static int numBeforeRegionDestroyEvents_isNotRemote;
    
    //beforeRegionClear events
    public static int numBeforeRegionClearEvents_isDist;
    public static int numBeforeRegionClearEvents_isNotDist;
    public static int numBeforeRegionClearEvents_isExp;
    public static int numBeforeRegionClearEvents_isNotExp;
    public static int numBeforeRegionClearEvents_isRemote;
    public static int numBeforeRegionClearEvents_isNotRemote;
    
    /**
     *  Get the WriterCountersBB
     */
    public static WriterCountersBB getBB() {
        if (bbInstance == null) {
           synchronized ( WriterCountersBB.class ) {
              if (bbInstance == null) 
                 bbInstance = new WriterCountersBB(BB_NAME, BB_TYPE);
           }
        }
        return bbInstance;
    }
    
    /**
     *  Zero-arg constructor for remote method invocations.
     */
    public WriterCountersBB() {
    }
    
    /**
     *  Creates a sample blackboard using the specified name and transport type.
     */
    public WriterCountersBB(String name, String type) {
        super(name, type, WriterCountersBB.class);
    }
    
    /** Increment the appropriate entry event counters.
     *
     *  @param eventName - An event name as used in this blackboard's counters, such as "BeforeCreate"
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
        Blackboard bb = WriterCountersBB.getBB();
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
        Blackboard bb = WriterCountersBB.getBB();
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
        Blackboard BB = WriterCountersBB.getBB();
        SharedCounters counters = WriterCountersBB.getBB().getSharedCounters();
        long isDist = counters.read(numBeforeCreateEvents_isDist) + counters.read(numBeforeUpdateEvents_isDist);
        long isNotDist = counters.read(numBeforeCreateEvents_isNotDist) + counters.read(numBeforeUpdateEvents_isNotDist);
        long isExp = counters.read(numBeforeCreateEvents_isExp) + counters.read(numBeforeUpdateEvents_isExp);
        long isNotExp = counters.read(numBeforeCreateEvents_isNotExp) + counters.read(numBeforeUpdateEvents_isNotExp);
        long isRemote = counters.read(numBeforeCreateEvents_isRemote) + counters.read(numBeforeUpdateEvents_isRemote);
        long isNotRemote = counters.read(numBeforeCreateEvents_isNotRemote) + counters.read(numBeforeUpdateEvents_isNotRemote);
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
        Log.getLogWriter().info("WriterCountersBB, create/update counters are OK");
    }
    
    /** Zero all counters in this blackboard.
     */
    public void zeroAllCounters() {
        SharedCounters sc = getSharedCounters();
        sc.zero(numBeforeCreateEvents_isDist);
        sc.zero(numBeforeCreateEvents_isNotDist);
        sc.zero(numBeforeCreateEvents_isExp);
        sc.zero(numBeforeCreateEvents_isNotExp);
        sc.zero(numBeforeCreateEvents_isRemote);
        sc.zero(numBeforeCreateEvents_isNotRemote);
        sc.zero(numBeforeCreateEvents_isLoad);
        sc.zero(numBeforeCreateEvents_isNotLoad);
        sc.zero(numBeforeCreateEvents_isLocalLoad);
        sc.zero(numBeforeCreateEvents_isNotLocalLoad);
        sc.zero(numBeforeCreateEvents_isNetLoad);
        sc.zero(numBeforeCreateEvents_isNotNetLoad);
        sc.zero(numBeforeCreateEvents_isNetSearch);
        sc.zero(numBeforeCreateEvents_isNotNetSearch);
        sc.zero(numBeforeDestroyEvents_isDist);
        sc.zero(numBeforeDestroyEvents_isNotDist);
        sc.zero(numBeforeDestroyEvents_isExp);
        sc.zero(numBeforeDestroyEvents_isNotExp);
        sc.zero(numBeforeDestroyEvents_isRemote);
        sc.zero(numBeforeDestroyEvents_isNotRemote);
        sc.zero(numBeforeDestroyEvents_isLoad);
        sc.zero(numBeforeDestroyEvents_isNotLoad);
        sc.zero(numBeforeDestroyEvents_isLocalLoad);
        sc.zero(numBeforeDestroyEvents_isNotLocalLoad);
        sc.zero(numBeforeDestroyEvents_isNetLoad);
        sc.zero(numBeforeDestroyEvents_isNotNetLoad);
        sc.zero(numBeforeDestroyEvents_isNetSearch);
        sc.zero(numBeforeDestroyEvents_isNotNetSearch);
        sc.zero(numBeforeUpdateEvents_isDist);
        sc.zero(numBeforeUpdateEvents_isNotDist);
        sc.zero(numBeforeUpdateEvents_isExp);
        sc.zero(numBeforeUpdateEvents_isNotExp);
        sc.zero(numBeforeUpdateEvents_isRemote);
        sc.zero(numBeforeUpdateEvents_isNotRemote);
        sc.zero(numBeforeUpdateEvents_isLoad);
        sc.zero(numBeforeUpdateEvents_isNotLoad);
        sc.zero(numBeforeUpdateEvents_isLocalLoad);
        sc.zero(numBeforeUpdateEvents_isNotLocalLoad);
        sc.zero(numBeforeUpdateEvents_isNetLoad);
        sc.zero(numBeforeUpdateEvents_isNotNetLoad);
        sc.zero(numBeforeUpdateEvents_isNetSearch);
        sc.zero(numBeforeUpdateEvents_isNotNetSearch);
        sc.zero(numBeforeRegionDestroyEvents_isDist);
        sc.zero(numBeforeRegionDestroyEvents_isNotDist);
        sc.zero(numBeforeRegionDestroyEvents_isExp);
        sc.zero(numBeforeRegionDestroyEvents_isNotExp);
        sc.zero(numBeforeRegionDestroyEvents_isRemote);
        sc.zero(numBeforeRegionDestroyEvents_isNotRemote);
        sc.zero(numBeforeRegionClearEvents_isDist);
        sc.zero(numBeforeRegionClearEvents_isNotDist);
        sc.zero(numBeforeRegionClearEvents_isExp);
        sc.zero(numBeforeRegionClearEvents_isNotExp);
        sc.zero(numBeforeRegionClearEvents_isRemote);
        sc.zero(numBeforeRegionClearEvents_isNotRemote);
    }
    
}
