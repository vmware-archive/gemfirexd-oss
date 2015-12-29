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
package rebalance;

import java.util.*;
import hydra.*;
import hydra.blackboard.*;
import util.*;

/** Blackboard defining counters for PartitionRebalanceEvents. 
 *  This blackboard has counters with names used by the increment method 
 *  in {@link rebalance#AbstractResourceListener}.
 *  Any subclass of AbstractResourceListener that wants to use 
 *  AbstractResourceListener's counter increment methods can use this 
 *  blackboard for that purpose.
 *  Note that the names of the blackboard counters defined here must be
 *  the same names as defined in AbstractResourceListener.
 */
public class RebalanceEventCountersBB extends Blackboard {

   // Blackboard variables
   static String BB_NAME = "PartitionRebalanceEventCounters_Blackboard";
   static String BB_TYPE = "RMI";

   // singleton instance of blackboard
   // forces counter indexs to be created (at class initialization) before accessed
   private static RebalanceEventCountersBB bbInstance = new RebalanceEventCountersBB( BB_NAME, BB_TYPE );

   // Event counters
   public static int STARTING;
   public static int STARTED;
   public static int CANCELLED;
   public static int FINISHED;

   public static int BUCKET_CREATES;
   public static int BUCKET_DESTROYS;
   public static int BUCKET_TRANSFERS;
   public static int PRIMARY_TRANSFERS;

   /**
    *  Get the EventCountersBB
    */
   public static RebalanceEventCountersBB getBB() {
       if (bbInstance == null) {
          synchronized ( RebalanceEventCountersBB.class ) {
             if (bbInstance == null)
                bbInstance = new RebalanceEventCountersBB(BB_NAME, BB_TYPE);
          }
       }
       return bbInstance;
   }

   /**
    *  Zero-arg constructor for remote method invocations.
    */
   public RebalanceEventCountersBB() {
   }

   /**
    *  Creates a sample blackboard using the specified name and transport type.
    */
   public RebalanceEventCountersBB(String name, String type) {
       super(name, type, RebalanceEventCountersBB.class);
   }


   /* Method to validate an ArrayList of ExpCounterValues for this BB.
    * Note that these ExpCounterValues take a counter index (vs. a counterName)
    * The index to name resolution is done via getNameForIndex (see below).
    */
   public static void checkEventCounters(ArrayList aList) {
   
      StringBuffer aStr = new StringBuffer();
   
      RebalanceEventCountersBB bb = getBB();
      SharedCounters sc = bb.getSharedCounters();
      bb.printSharedCounters();
   
      for (int i=0; i < aList.size(); i++) {
         ExpCounterValue expCtrVal = (ExpCounterValue)aList.get(i);
         int counterIndex = expCtrVal.getCounterIndex();
         long expectedValue = expCtrVal.getExpectedValue();
         long actualValue = sc.read(counterIndex);
         String counterName = getNameForIndex(counterIndex);
         Log.getLogWriter().info("Checking " + counterName + ": expecting " + expectedValue + ", found " + actualValue);
         if (actualValue != expectedValue) {
            aStr.append("Expected " + expectedValue + " " + counterName + " events, but found " + actualValue + "\n");
         }
      }
      if (aStr.length() > 0) {
         throw new TestException(aStr.toString());
      }
      Log.getLogWriter().info("All Counter Values are correct");
   }
   
   private static String getNameForIndex(int index) {
      RebalanceEventCountersBB bb = getBB();
      
      String[] counterNames = bb.getCounterNames();
      if (index >= counterNames.length) {
         throw new TestException("Counter index " + index + " is outside of bounds for RebalanceEventCountersBB of size " + counterNames.length);
      }
      return counterNames[index];
   }

   /** Zero all counters in this blackboard.
    */
   public static void zeroAllCounters() {
      SharedCounters sc = getBB().getSharedCounters();
      sc.zero(STARTING);
      sc.zero(STARTED);
      sc.zero(CANCELLED);
      sc.zero(FINISHED);
      sc.zero(BUCKET_CREATES);
      sc.zero(BUCKET_DESTROYS);
      sc.zero(BUCKET_TRANSFERS);
      sc.zero(PRIMARY_TRANSFERS);
      Log.getLogWriter().info("Cleared RebalanceEventCounters");
   }
}
