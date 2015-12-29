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

import hydra.ClientVmInfo;
import hydra.Log;
import hydra.MasterController;
import hydra.RemoteTestModule;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;

/** Class to observe when recovery or rebalancing occurs.
 *  The callbacks implemented here are called for both recovery and rebalancing.
 */
public class PRObserver extends InternalResourceManager.ResourceObserverAdapter {
    
// Key for PRObserverBB 
public static final String activityListKey = "activityList_vm_";

// Activity list content strings
public static final String rebalRecovStarted = "Rebalancing/recovery started at ";
public static final String rebalRecovFinished = "Rebalancing/recovery finished at ";
public static final String movingBucket = "Moving bucket at ";
public static final String movingPrimary = "Moving primary at ";
public static final String recoveryConflated = "Recovery conflated at ";

// Activity list content strings
private static boolean recordBucketMoves = false;   // if true, record bucket moves in the blackboard activity list
private static boolean recordPrimaryMoves = false;  // if true, record primary moves in the blackboard activity list

//=========================================================================
// Implementation of interface methods

/** Log that rebalancing/recovery has started.
 *  Mark in the blackboard the earliest start time for recovery.
 *  Record in the blackboard that a recovery has started.
 *  Note that for the earliest start time to be accurate, initialize() must have been called prior to 
 *  expecting rebalancing/recovery.
 *
 *  @param region The region that is undergoing rebalancing/recovery.
 */
public synchronized void rebalancingOrRecoveryStarted(Region region) {
   try {
      long startTime = System.currentTimeMillis();
      _addToActivityList(rebalRecovStarted + new Date() + " startTime=" + startTime + " for " + region.getFullPath());
      // use setIfSmaller; the first vm to start rebalance/recover wins
      PRObserverBB.getBB().getSharedCounters().setIfSmaller(PRObserverBB.rebalRecovStartTime,
                           new Long(startTime));
      PRObserverBB.getBB().getSharedCounters().increment(PRObserverBB.rebalRecovStartCounter);
   } catch (Exception e) {
      Log.getLogWriter().severe(TestHelper.getStackTrace(e));
   }
}

/** Log that rebalancing/recovery has finished.
 *  Mark in the blackboard the latest finish time for recovery.
 *  Record in the blackboard that a recovery has finished.
 *  Note that for the latest finish time to be accurate, initialize() must have been called prior to 
 *  expecting rebalancing/recovery.
 *
 *  @param region The region that finished rebalancing/recovery.
 */
public synchronized void rebalancingOrRecoveryFinished(Region region) {
   try {
      long finishTime = System.currentTimeMillis();
      Date finishDate = new Date();
      addFinishToActivityList(finishDate, finishTime, region); // this logs
      // use setIfLarger; the last vm to finish rebalance/recover wins
      PRObserverBB.getBB().getSharedCounters().setIfLarger(PRObserverBB.rebalRecovFinishTime,
                           finishTime);
   } catch (Exception e) {
      Log.getLogWriter().severe(TestHelper.getStackTrace(e));
   }
}

/** Log that recovery was rejected because it was already in progress.
 *  Increment a counter that this vm rejected recovery.
 *
 */
public synchronized void recoveryConflated(PartitionedRegion region) {
   try {
      _addToActivityList(recoveryConflated + new Date() + " for " + region.getFullPath());
   } catch (Exception e) {
      Log.getLogWriter().severe(TestHelper.getStackTrace(e));
   }
}

/** Callback for moving a bucket
 */
public synchronized void movingBucket(Region region, 
                         int bucketId,
                         DistributedMember source, 
                         DistributedMember target) {
   if (recordBucketMoves) {
      String aStr = "moving bucket with ID " + bucketId + " in " + 
          region.getFullPath() + " from " + source + " to " + target;
      Log.getLogWriter().info("PRObserver: " + aStr);
      _addToActivityList(movingBucket + new Date() + "; " + aStr);
   }
}
  
/** Callback for moving a primary
 */
public synchronized void movingPrimary(Region region, 
                          int bucketId,
                          DistributedMember source, 
                          DistributedMember target) {
   if (recordPrimaryMoves) {
      String aStr = "moving primary for bucket with ID " + bucketId + " in " + 
          region.getFullPath() + " from " + source + " to " + target;
      Log.getLogWriter().info("PRObserver: " + aStr);
      _addToActivityList(movingPrimary + new Date() + "; " + aStr);
   }
}

//=========================================================================
// Initialization methods

/** Install a hook to determine when rebalancing/recovery starts/stops
 */
public synchronized static void installObserverHook() {
   Log.getLogWriter().info("Installing PRObserver");
   InternalResourceManager.setResourceObserver(new PRObserver());
}

/** Install a hook to determine when rebalancing/recovery starts/stops
 */
public synchronized static void installObserverHook(boolean recordBucketMovesArg, boolean recordPrimaryMovesArg) {
   Log.getLogWriter().info("Installing PRObserver");
   InternalResourceManager.setResourceObserver(new PRObserver());
   recordBucketMoves = recordBucketMovesArg;
   recordPrimaryMoves = recordPrimaryMovesArg;
}

/** Initialize counters. This should be called prior to every rebalance/recovery.
 */
public synchronized static void initialize() {
   Log.getLogWriter().info("Initializing PRObserverBB counters");
   PRObserverBB.getBB().getSharedCounters().zero(PRObserverBB.rebalRecovStartTime); 
   PRObserverBB.getBB().getSharedCounters().zero(PRObserverBB.rebalRecovFinishTime); 
   PRObserverBB.getBB().getSharedCounters().zero(PRObserverBB.approxTimerStartTime); 
   PRObserverBB.getBB().getSharedCounters().zero(PRObserverBB.rebalRecovStartCounter); 
   PRObserverBB.getBB().getSharedCounters().add(PRObserverBB.rebalRecovStartTime, Long.MAX_VALUE); 
   PRObserverBB.getBB().getSharedCounters().add(PRObserverBB.rebalRecovFinishTime, Long.MIN_VALUE); 
   PRObserverBB.getBB().getSharedCounters().add(PRObserverBB.approxTimerStartTime, Long.MAX_VALUE); 
   PRObserverBB.getBB().getSharedMap().clear();
}

/** Initialize counters for a particular vmid.
 */
public synchronized static void initialize(int vmId) {
   Log.getLogWriter().info("PRObserver: Removing activity list for vmId " + vmId);
   Object key = activityListKey + vmId;
   PRObserverBB.getBB().getSharedMap().remove(key);
}

//=========================================================================
// Wait methods

/** Wait for rebalancing/recovery to start in a particular vm.
 * 
 *  @param vmID Wait for this vmID to start rebalancing/recovery.
 *  @param millisToWait The number of milliseconds to wait for rebalancing/recovery to start.
 *  @param millisToSleep The number of milliseconds to sleep between polling for rebalancing/recovery.
 *
 *  @return True if rebalancing/recovery started, false if not. 
 */
public static boolean waitForRebalRecovToStart(int vmID, long millisToWait, int millisToSleep) {
   Log.getLogWriter().info("PRObserver: Waiting " + millisToWait + " ms for vm " + vmID + " to start rebalancing/recovery...");
   String key = activityListKey + vmID;
   long waitStartTime = System.currentTimeMillis();
   while (true) {
      List activityList = (List)(PRObserverBB.getBB().getSharedMap().get(key));
      if (activityList != null) {
         for (int i = 0; i < activityList.size(); i++) {
            String activity = (String)(activityList.get(i));
            if (activity.startsWith(rebalRecovStarted)) {
               Log.getLogWriter().info("PRObserver: Rebalancing/recovery started in vmID " + vmID +
                   " after waiting " + (System.currentTimeMillis() - waitStartTime) + " ms: " +
                   activityListToString(activityList));
               return true;
            }
            MasterController.sleepForMs(millisToSleep);
            long waitTime = System.currentTimeMillis() - waitStartTime;
            if (waitTime > millisToWait) {
               Log.getLogWriter().info("PRObserver: Rebalancing/recovery did not start in " + millisToWait + " ms");
               return false;
            }
         }
      }
   }
}

/** Wait for rebalancing/recovery to start in ANY vm.
 * 
 *  @param millisToWait The number of milliseconds to wait for rebalancing/recovery to start.
 *  @param millisToSleep The number of milliseconds to sleep between polling for rebalancing/recovery.
 *
 *  @return True if rebalancing/recovery started, false if not. 
 */
public static boolean waitForAnyRebalRecovToStart(long millisToWait, int millisToSleep) {
   Log.getLogWriter().info("PRObserver: Waiting " + millisToWait + " ms for vms to start rebalancing/recovery...");
   long recovStartTime = PRObserverBB.getBB().getSharedCounters().read(PRObserverBB.rebalRecovStartTime);
   long waitStartTime = System.currentTimeMillis();
   while (recovStartTime == Long.MAX_VALUE) {
      MasterController.sleepForMs(millisToSleep);
      recovStartTime = PRObserverBB.getBB().getSharedCounters().read(PRObserverBB.rebalRecovStartTime);
      long waitTime = System.currentTimeMillis() - waitStartTime;
      if (waitTime > millisToWait) {
         Log.getLogWriter().info("PRObserver: Rebalancing/recovery did not start in " + millisToWait + " ms");
         return false;
      }
   }
   Log.getLogWriter().info("PRObserver: Rebalancing/recovery started after waiting " + 
       (System.currentTimeMillis() - waitStartTime) + " ms");
   return true;
}

/** Wait for rebalanc/recovery to finish.
 * 
 *  @param rebalRecovVMs The vm(s) expecting rebalancing/recovery. 
 *         This can either be a an Integer (a vmID), a ClientVmInfo containing
 *         a vmID, or a List of Integers or ClientVmInfo instances. 
 *  @param numRecovRebalActivities The number of recovery/rebalance activities 
 *         each vm should receive.
 *  @param numPRsToRebalRecov The number of PRs to rebalance/recover in each VM.
 *  @param prNames A List of Strings, where each string is the name
 *         of the PR to experience rebalanc/recovery. This is optional and can be null.
 *  @param noRebalRecovVMs The vm(s) expecting no rebalancing/recovery. 
 *         This can either be a an Integer (a vmID), a ClientVmInfo containing
 *         a vmID, or a List of Integers or ClientVmInfo instances. 
 *         This is optional and can be ull.
 *  @param allowRebalRecovConflation True if rebalancing/recovery conflation is 
 *         allowed to have occurred in the rbalRecovVMs, false otherwise.
 */
public static void waitForRebalRecov(Object rebalRecovVMs,
                                     int numRecovRebalActivities, 
                                     int numPRsToRebalRecov, 
                                     List prNames, 
                                     Object noRebalRecovVMs,
                                     boolean allowRecovConflation) {
   Log.getLogWriter().info("PRObserver: Waiting for " + numRecovRebalActivities + 
       " rebalancing/recovery activit" + ((numRecovRebalActivities == 1) ? "y" : "ies") +
       " in vm(s) " + rebalRecovVMs + 
       "; rebalancing/recovering " + numPRsToRebalRecov + " PR(s), " +
       "recovery conflation allowed: " + allowRecovConflation +
       ", (optional specified PR names: " + prNames + 
       ", optional vms expecting no rebalancing/recovery: " + noRebalRecovVMs + ")");

   if ((prNames != null) && (prNames.size() != numPRsToRebalRecov)) {
      throw new TestException("Test error; prNames is " + prNames + 
                ", but numPRsToRebalRecov is " + numPRsToRebalRecov); 
   }

   waitForEachVMToFinish(rebalRecovVMs, numRecovRebalActivities, numPRsToRebalRecov, prNames, allowRecovConflation);
   verifyNoRebalRecov(noRebalRecovVMs);

   Log.getLogWriter().info("PRObserver: Done waiting for " + numRecovRebalActivities + 
       " rebalancing/recovery activit" + ((numRecovRebalActivities == 1) ? "y" : "ies") +
       " in vm(s) " + rebalRecovVMs + 
       "; rebalancing/recovering " + numPRsToRebalRecov + " PRs, " +
       "recovery conflation allowed: " + allowRecovConflation +
       ", (optional specified PR names: " + prNames + 
       ", optional vms expecting no rebalancing/recovery: " + noRebalRecovVMs + ")");
}

/** Wait for recovery after a stop and restart.
 * 
 *  @param recoveryDelay The recovery delay setting for departed vms.
 *  @param startupRecoveryDelay The startup recovery delay setting.
 *  @param startupRecoveryVMs The vm(s) expecting startup recovery. 
 *         This can either be a an Integer (a vmID), a ClientVmInfo containing
 *         a vmID, or a List of Integers or ClientVmInfo instances. 
 *  @param departedRecoveryVMs The vm(s) expecting departed recovery. 
 *         This can either be a an Integer (a vmID), a ClientVmInfo containing
 *         a vmID, or a List of Integers or ClientVmInfo instances. 
 *  @param numDepartedRecoveries The number of recovery activities the 
 *         departedRecoveryVMs should receive (usually the number of vms 
 *         that departed).
 *  @param numPRsToRecovery The number of PRs to recovery in the vm.
 *  @param prNames A List of Strings, where each string is the name
 *         of the PR to experience recovery. This is optional and can be null.
 *  @param noRecoveryVMs The vm(s) expecting no recovery. 
 *         This can either be a an Integer (a vmID), a ClientVmInfo containing
 *         a vmID, or a List of Integers or ClientVmInfo instances. 
 *         This is optional and can be ull.
 */
public static void waitForRecovery(
                   long recoveryDelay,
                   long startupRecoveryDelay,
                   Object startupRecoveryVMs,
                   Object departedRecoveryVMs,
                   int numDepartedRecoveries, 
                   int numPRsToRecover, 
                   List prNames, 
                   Object noRecoveryVMs) {
   if (recoveryDelay >= 0) {
      if ((departedRecoveryVMs instanceof List) && (((List)departedRecoveryVMs).size() == 0)) {
         Log.getLogWriter().info("No vms to recover due to departing, not waiting");
         return;
      }
      PRObserver.waitForRebalRecov(departedRecoveryVMs, numDepartedRecoveries, 
                 numPRsToRecover, prNames, noRecoveryVMs, true);
   }
   if (startupRecoveryDelay >= 0) {
      if ((startupRecoveryVMs instanceof List) && (((List)startupRecoveryVMs).size() == 0)) {
         Log.getLogWriter().info("No vms to recover on startup, not waiting");
         return;
      }
      PRObserver.waitForRebalRecov(startupRecoveryVMs, 1, 
                 numPRsToRecover, prNames, noRecoveryVMs, false);
   }
}

/** Verify rebalancing/recovery has not run for the given vms.
 *
 *  @param noRebalRecovVMs The vm(s) expecting no rebalancing/recovery. 
 *         This can either be a an Integer (a vmID), a ClientVmInfo containing
 *         a vmID, or a List of Integers or ClientVmInfo instances. 
 */
public static void verifyNoRebalRecov(Object noRebalRecovVMs) {
   if (noRebalRecovVMs == null) {
      return;
   }
   List aList = new ArrayList();
   if (noRebalRecovVMs instanceof List) {
      aList.addAll((List)noRebalRecovVMs);
   } else { 
      aList.add(noRebalRecovVMs);
   }
   for (int i = 0; i < aList.size(); i++) {
      Object anObj = aList.get(i);
      int vmID = -1;
      if (anObj instanceof Integer) {
         vmID = ((Integer)anObj).intValue();
      } else {
         vmID = ((ClientVmInfo)anObj).getVmid();
      }
      String key = activityListKey + vmID;
      List activityList = (List)(PRObserverBB.getBB().getSharedMap().get(key));
      if (activityList == null) {
         Log.getLogWriter().info("Rebalancing/recovery has not run for vmID " + vmID);
      } else {
         throw new TestException("Rebalancing/recovery ran for vmID " + vmID + 
               ", but it was not expected; recovery activities for vmID " + vmID + ": " + 
               activityListToString(activityList));
      }
   }
}

//=========================================================================
// Other methods

/** Wait for a vm to finish rebalancing/recovery.
 * 
 *  @param rebalRecovVM A VM expected to rebalance/recover. This is either
 *         an Integer (vmId) or an instance of ClientVmInfo containing a
 *         vmId.
 *  @param numExpectedActivities The number of times rebalancing/recovery should
 *         either start/finish, or be conflated.
 *  @param numPRsToRebalRecov The number of PRs expected to rebalance/recover in
 *         recovRebalVMs.
 *  @param prNames A List of Strings, which are the names of the PRs expected
 *         for recovery. This is optional and can be null.
 *  @param allowConflation True if recovery conflation is allowed in
 *         the recoveryVM, false otherwise. 
 */
protected static void waitForVMToFinish(
          Object rebalRecovVM, 
          int numExpectedActivities, 
          int numPRsToRebalRecov, 
          List prNames,
          boolean allowConflation) {
   int vmId = -1;
   if (rebalRecovVM instanceof Integer) {
      vmId = ((Integer)rebalRecovVM).intValue();
   } else {
      vmId = ((ClientVmInfo)rebalRecovVM).getVmid();
   }
   String key = activityListKey + vmId;
   while (true) {
      List activityList = (List)(PRObserverBB.getBB().getSharedMap().get(key));
      if (completedActivities(activityList, numExpectedActivities, allowConflation, numPRsToRebalRecov, prNames, vmId)) {
         Log.getLogWriter().info("PRObserver: Completed " + numExpectedActivities + 
             " rebalancing/recovery activities for each of " + numPRsToRebalRecov + " PR(s) in vmId " + vmId + 
             ((prNames == null) ? "" : ", PR(s) " + prNames) + ": " + activityListToString(activityList));
         return;
      } else {
         Log.getLogWriter().info("PRObserver: Waiting for " + numExpectedActivities + 
             " rebalancing/recovery activities for each of " + numPRsToRebalRecov + " PR(s) in vmId " + vmId + 
             ((prNames == null) ? "" : ", PR(s) " + prNames) + ": " + 
             activityListToString(activityList));
         MasterController.sleepForMs(2000);
      }
   }
}

/** Wait for vm(s) to finish rebalancing/recovery.
 * 
 *  @param rebalRecovVMs The vm(s) expecting rebalancing/recovery. 
 *         This can either be a an Integer (a vmID), a ClientVmInfo containing
 *         a vmID, or a List of Integers or ClientVmInfo instances. 
 *  @param numExpectedActivities The number of times rebalancing/recovery should
 *         either start/finish, or be conflated for each PR in numPRsToRebalRecov.
 *  @param numPRsToRebalRecov The number of PRs expected to rebalance/recover in
 *         each vm in rebalRecovVMs.
 *  @param prNames A List of Strings, which are the names of the PRs expected
 *         for rebalancing/recovery. This is optional and can be null.
 *  @param allowConflation True if recovery conflation is allowed in
 *         the rebalRecovVMs, false otherwise. 
 */
protected static void waitForEachVMToFinish(
          Object rebalRecovVMs, 
          int numExpectedActivities, 
          int numPRsToRebalRecov, 
          List prNames,
          boolean allowConflation) {
   if ((rebalRecovVMs instanceof Integer) || (rebalRecovVMs instanceof ClientVmInfo)) {
      waitForVMToFinish(rebalRecovVMs, numExpectedActivities, numPRsToRebalRecov, prNames, allowConflation);
   } else if (rebalRecovVMs instanceof List) {
      List vmList = (List)rebalRecovVMs;
      if (vmList.size() == 0) {
         throw new TestException("Test error: vm list is empty");
      }
      for (int i = 0; i < vmList.size(); i++) {
         waitForVMToFinish(vmList.get(i), numExpectedActivities, numPRsToRebalRecov, prNames, allowConflation);
      }
   } else {
      throw new TestException("Unknown vms: " + rebalRecovVMs);
   }
}

/** Given a list of recovery activites, determine whether a certain number
 *  has occurred for each of numPRs.
 *
 *  @param activityList A List of Strings containing information about
 *         recovery activities (start/finish/conflated).
 *  @param expectedNumActivities The number of expected recovery activities.
 *  @param allowConflatedActivities True if the activities are allowed to
 *         be conflated activities, false otherwise.
 *  @param numPRs The number of distinct PRs that should rebalance/recover.
 *  @param prNames A List of PR names the activities must occur for, or null.
 *  @param vmID The vmID that we are checking activities for.
 *
 *  @return true if the activityList contains expectedNumActivities, false
 *          otherwise.
 */
protected static boolean completedActivities(
          List activityList, 
          int expectedNumActivities,
          boolean allowConflatedActivities,
          int numPRs,
          List prNames,
          int vmID) {
   if (activityList == null) {
      return false;
   }

   // create parallel lists
   List regNames = new ArrayList();       // (Strings) region names
   List finishedCount = new ArrayList();    // (Integer) num finished activities for the region name
   List conflatedCount = new ArrayList(); // (Integer) num conflated activities for the region name
   for (int i = 0; i < activityList.size(); i++) {
      String activity = (String)(activityList.get(i));
      String regNameFromActivity = getRegionNameFromActivity(activity);
      int index = regNames.indexOf(regNameFromActivity);
      if (index < 0) {
         regNames.add(regNameFromActivity);
         finishedCount.add(new Integer(0));
         conflatedCount.add(new Integer(0));
         index = regNames.size() - 1;
      }
      if (activity.startsWith(rebalRecovFinished)) {
         finishedCount.set(index, new Integer((((Integer)(finishedCount.get(index))).intValue()) + 1)); 
      } else if (activity.startsWith(recoveryConflated)) {
         if (!allowConflatedActivities) {
            throw new TestException("Found unexpected conflated activity for " + activityListToString(activityList) +
                  " for vm " + vmID);
         }
         conflatedCount.set(index, new Integer((((Integer)(conflatedCount.get(index))).intValue()) + 1)); 
      }
   } 

   // check for the wrong number of activities for a particular region
   if (regNames.size() > numPRs) { 
      // too many; this is an exception now; if too few we probably need to wait and check again  
      throw new TestException("Unexpected regions experienced rebalancing/recovery activities for vm " + vmID +
                "; expected " + expectedNumActivities + " activities for each of " + numPRs +
                " PR(s) with optional region names: " + prNames + ", activities found: " +
                activityListToString(activityList) + ", regions found: " + regNames);
   }

   // now the number of distinct region names with activities is correct
   // check for optional pr names
   if (prNames != null) {
      for (int i = 0; i < prNames.size(); i++) {
         if (regNames.indexOf(prNames.get(i)) < 0) { 
            // did not find prNames[i] as expected; return false 
            return false;
         }
      }
   } else if (regNames.size() < numPRs) {
     return false;
   }


   // now the regions names are correct; check for the correct number of activities for each one
   for (int i = 0; i < regNames.size(); i++) {
      int finished = ((Integer)(finishedCount.get(i))).intValue();
      int conflated = ((Integer)(conflatedCount.get(i))).intValue();
      int sum = finished + conflated;
      if (sum > expectedNumActivities) {
         // too many; this is an exception now
         throw new TestException("Too many rebalancing/recovery activities experienced in vm " + vmID + " for region " + 
               regNames.get(i) + "; expected " + expectedNumActivities + " activities for each of " + 
               numPRs + " PR(s) with optional region names: " + prNames + ", activities found: " +
               activityListToString(activityList));
      }
      if (sum < expectedNumActivities) { // return false; we didn't get the activities we want yet
         return false;
      }
      if (finished == 0) {
         return false;
      } 
   }
   return true;
}

//================================================================================ 
// Activity list methods

/** Add a conflated recovery activity to the activity List in the blackboard for this vm
 */
protected void addFinishToActivityList(Date finishDate, long finishTime, Region aRegion) {
   int vmId = RemoteTestModule.getMyVmid();
   String key = activityListKey + vmId;
   List activityList = (List)(PRObserverBB.getBB().getSharedMap().get(key));
   if (activityList == null) {
      throw new TestException("Got a finish callback for recovery, but no previous recovery activities found");
   }
   String lastStartActivity = null;
   for (int i = 0; i < activityList.size(); i++) {
      String activity = (String)(activityList.get(i));
      if (activity.startsWith(rebalRecovStarted)) {
         lastStartActivity = activity;
      }
   }
   if (lastStartActivity == null) {
      throw new TestException("Got a finish activity, but did not find a previous start: " + 
            activityListToString(activityList));
   }
   String searchStr = "startTime=";
   int index = lastStartActivity.indexOf(searchStr);
   if (index >= 0) {
      int index2 = lastStartActivity.indexOf(" ", index);
      if (index2 >= 0) {
         String startTimeStr = lastStartActivity.substring(index + searchStr.length(), index2);
         try {
            Long startTime = Long.valueOf(startTimeStr);
            long duration = finishTime - startTime;
            _addToActivityList(rebalRecovFinished + finishDate + " " + finishTime + " for " +
                aRegion.getFullPath() + " duration: " + duration + " ms");
         } catch (NumberFormatException e) {
            String aStr = "Test error; Could not find start time in " + startTimeStr;
            Log.getLogWriter().info(aStr);
            throw new TestException(aStr);
         }
      } else {
         String aStr = "Test error; Could not find start time in " + lastStartActivity;
         Log.getLogWriter().info(aStr);
         throw new TestException(aStr);
      }
   } else {
      String aStr = "Test error; Could not find start time in " + lastStartActivity;
      Log.getLogWriter().info(aStr);
      throw new TestException(aStr);
   }
}

/** Add an activity to the activity List in the blackboard for this vm
 */
protected void _addToActivityList(String activityStr) {
   Log.getLogWriter().info("PRObserver: " + activityStr);
   int vmId = RemoteTestModule.getMyVmid();
   String key = activityListKey + vmId;
   List activityList = (List)(PRObserverBB.getBB().getSharedMap().get(key));
   if (activityList == null) {
      activityList = new ArrayList();
   }
   activityList.add(activityStr);
   PRObserverBB.getBB().getSharedMap().put(key, activityList);
}

/** Return a string representation of an activityList
 *
 * @param activityList A list of rebalancing/recovery activites.
 *
 */
protected static String activityListToString(List activityList) {
   if ((activityList == null) || (activityList.size() == 0)) {
      return "No activities";
   }
   StringBuffer aStr = new StringBuffer();
   for (int i = 0; i < activityList.size(); i++) {
      aStr.append(activityList.get(i) + "\n");
   }
   return aStr.toString();
}

//================================================================================ 
// Other methods

/* Given an activity String, return the region name from it.
 *
 * @param activityStr A rebalancing/recovery activity string.
 * @returns The region name from activityStr.
 */
protected static String getRegionNameFromActivity(String activityStr) {
   String searchStr = "for /";
   int index = activityStr.indexOf(searchStr);
   if (index < 0) {
      searchStr = "in /";
      index = activityStr.indexOf(searchStr);
   }
   if (index >= 0) {
      index = index - 1 + searchStr.length();
      int index2 = activityStr.indexOf(" ", index);
      if (index2 < 0) {
         index2 = activityStr.length();
      }
      return activityStr.substring(index, index2);
   } else {
      throw new TestException("Could not get region name from " + activityStr);
   }
}

}

