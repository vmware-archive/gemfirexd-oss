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
package membership;

import java.util.*;
import java.rmi.*;
import util.*;
import hydra.*;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

/**
 * A Hydra test that kills, starts, disconnects, connects members of the distributed
 * system.  It uses the {@link MembershipBB} to keep track of the members of the
 * distributed system according to the test.
 * @see MemberPrms
 *
 * @author Jean Farris
 * @since 5.0
 */
public class MembershipTest {
    
  /* The singleton instance of MembershipTest in this VM */
  static protected MembershipTest memberTest;

  /*
  =================
  INIT TASKS
  =================*/

  /**
   * Hydra task for test initialization.
   *
   * @author Jean Farris
   * @since 5.0
   */
  public synchronized static void HydraTask_initialize() {
    if (memberTest == null) {
	// do this as start task?
     MembershipBB.getBB().getSharedCounters().setIfLarger(
                                    MembershipBB.startTime,
                                    System.currentTimeMillis()); 
      memberTest = new MembershipTest();
      memberTest.initialize();    
    }    
  } 

  /**
   * Records vmid and pid of client in the blackboard shared map, connects
   * to distributed system.
   *
   * @author Jean Farris
   * @since 5.0
   */
  protected void initialize() {

    int memberPid = RemoteTestModule.getMyPid();
    int memberVmid = RemoteTestModule.getMyVmid();

    // if pid exists for different vmid - stop tests - test dependds on members having unique pids
    if ( MembershipBB.getBB().getSharedMap().containsValue( new Integer(memberPid) ) ) {
      throw new TestException ("Duplicate pids:  " + memberPid + " for vmid: " + memberVmid);
    }
    // put vmid, pid into shared map
    MembershipBB.getBB().getSharedMap().put(new Integer(memberVmid), new Integer(memberPid));
    log().info("### Completed adding member vmid, pidto map: " + memberVmid + ", " + memberPid);
    
    CacheUtil.createCache();
    verifyIsMember(memberPid);    

    log().info("### New member intialized vmid, pid: " + memberVmid + ", " + memberPid);
  }

  /**
   * Hydra task for test initialization upon dynamic restart of VM
   *
   * @author Jean Farris
   * @since 5.0
   */
  public synchronized static void HydraTask_initForRestart() {
    if (memberTest == null) {
      memberTest = new MembershipTest();
      memberTest.initForRestart();    
    }    
  } 

  /**
   * Calls initialize to add member info to blackboard and then
   * updates blackboard to indicated member change no longer in progress.
   *
   * @author Jean Farris
   * @since 5.0
   */
  protected void initForRestart() {

    log().info("### starting init task after restart");
    initialize();
    MembershipBB.zeroCounter(MembershipBB.membershipChangeInProgress);
  }


  /*
  ===========================
  TEST TASKS - member changes
  ===========================*/

   /**
   * A Hydra task that disrupts members of the distributed system  
   * (kills/restarts, disconnects/reconnects)
   *
   * @author Jean Farris
   * @since 5.0
   */
  public static void HydraTask_disruptMember() {
    memberTest.disruptMember();
  } 

   /**
   * Disrupts members of the distributed system (kills, kills/restarts, closes cache, 
   * disconnects/reconnects) and then checks for system membership update.
   *
   * @author Jean Farris
   * @since 5.0
   */
  public void disruptMember() {

    String operation = TestConfig.tab().stringAt(MembershipPrms.memberOperations);
    log().info("### Disrupt op: " + operation);
    if (operation.equals("kill")) {
      killVm();
    }
    else if (operation.equals("disconnect")) {
      disconnect();
    }
    else if (operation.equals("close")) {
      closeCache();
    }
    else {
      throw new TestException("Unknown member operation: " + operation);
    }
  }

   /**
   * A Hydra task that selects and kills another members of the distributed system  
   *
   * @author Jean Farris
   * @since 5.0
   */
  public static void HydraTask_disruptOther() {
    memberTest.disruptOther();
  } 

   /**
   * Kills another member of the distributed system. 
   * Verifies member departed.
   * Starts new member.
   * Verifies new member is part of distributed system.
   *
   * @author Jean Farris
   * @since 5.0
   */
  public void disruptOther() {

    // wait for membership verify to be completed
    TestHelper.waitForCounter (MembershipBB.getBB(),
                               "MembershipBB.membershipChangeInProgress",
			       MembershipBB.membershipChangeInProgress,
			       TestHelper.getNumVMs()-1,
                               true,
                               3000000);

    Integer vmId = getOtherRandomVmId();
    removeMemberFromMap(vmId);
    int disruptedPid = ClientVmMgr.getPid(vmId); // must precede stop
    // mean kill of other member
    try {
      ClientVmMgr.stop ("Mean Kill" + vmId,
                         ClientVmMgr.MEAN_KILL,
                         ClientVmMgr.ON_DEMAND,
                         new ClientVmInfo(vmId, null, null));
    } catch (ClientVmNotFoundException ex) {
      throw new TestException("Kill other vm failed for vm id: " + vmId, ex);
    }


    // verify this vm no longer sees killed member 
    verifyNotMember(disruptedPid);

    // restart - verify this vm sees new members
    try {
      ClientVmMgr.start ("Starting on demand", new ClientVmInfo(vmId, null, null));
    } catch (Exception ex) {
      throw new TestException("Restart other vm failed for vm id: " + vmId, ex);
    }
    // verify this vm sees new member
    int newMemberPid = ClientVmMgr.getPid(vmId); // must succeed start
    verifyIsMember(newMemberPid);

    // membershipChangeInProgress will be set to zero for verify tasks to proceed, this is done
    // when above ClientVmMgr.start is done
    long startTime = MembershipBB.readCounter(MembershipBB.startTime);;
    long elapsedTime = System.currentTimeMillis() - startTime;
    long totalTaskTimeSec = 
                   TestConfig.tab().longAt(MembershipPrms.totalTaskTimeSec);
    // want this task to stop before others - deduct 15 sec.
    if (elapsedTime > ((totalTaskTimeSec - 15) * TestHelper.SEC_MILLI_FACTOR)) {
      log().info("### disruptOther reached end of task time");
      throw new StopSchedulingTaskOnClientOrder();
    } 

  }


  /**
   * A Hydra task that selects and kills another member of the distributed system  
   *
   * @author Jean Farris
   * @since 5.0
   */
  public static void HydraTask_killVm() {
    memberTest.killVm();
  } 

   /**
   * Kills a client VM other than the current one. 
   * Starts new member.
   *
   * @author Jean Farris
   * @since 5.0
   */
  public void killVm() {

    Integer vmId = getOtherRandomVmId();     
    removeMemberFromMap(vmId);

    // mean kill of other member
    try {
      ClientVmMgr.stop ("Mean Kill" + vmId,
                         ClientVmMgr.MEAN_KILL,
                         ClientVmMgr.ON_DEMAND,
                         new ClientVmInfo(vmId, null, null));
    } catch (ClientVmNotFoundException ex) {
      throw new TestException("Kill other vm failed for vm id: " + vmId, ex);
    }

    try {
      ClientVmMgr.start ("Starting on demand", new ClientVmInfo(vmId, null, null));
    } catch (Exception ex) {
      throw new TestException("Restart other vm failed for vm id: " + vmId, ex);
    }
  }

   /**
   * A Hydra task that kills/restarts the current logical VM
   *
   * @author Jean Farris
   * @since 5.0
   */
  public static void HydraTask_stopSelf() {
    memberTest.stopSelf();
  } 

  /**
   * Kills  logical VM  with immediate restart requested.  GemFire should see 
   * restarted logical vm as a client as a member joining.
   *
   * @author Jean Farris
   * @since 5.0
   */
  protected void stopSelf() {

    Integer myVmid = new Integer(RemoteTestModule.getMyVmid());
    Integer myPid = new Integer(RemoteTestModule.getMyPid());

    log().info("### kill self: " + myVmid);
    ClientVmInfo clientVm = new ClientVmInfo(myVmid, null, null);
    
    // track VM status in blackboard
    removeMemberFromMap(myVmid);
    String stopModePrm = TestConfig.tab().stringAt(MembershipPrms.stopMode);
    log().info("stop mode is: " + stopModePrm);
    int stopMode = -1;
    if (stopModePrm.equals("MEAN_KILL"))
      stopMode = ClientVmMgr.MEAN_KILL;
    else if (stopModePrm. equals("NICE_KILL"))
      stopMode = ClientVmMgr.NICE_KILL;
    else if (stopModePrm.equals("MEAN_EXIT"))
      stopMode = ClientVmMgr.MEAN_EXIT;
    else
      throw new TestException("test does not support stopMode: " + stopMode);
    
    try { 
      ClientVmMgr.stopAsync ("Test kill, immediate restart", stopMode,
                              ClientVmMgr.IMMEDIATE);
    } catch (ClientVmNotFoundException ex) {
      throw new TestException("Kill with immediate restart failed for vmid: " + myVmid, ex);
    }
  }
 

  /**
   * A Hydra task that closes the cache and optionally re-connects
   *
   * @author Jean Farris
   * @since 5.0
   */
  public static void HydraTask_closeCache() {
    memberTest.closeCache();
  } 


  /**
   * Closes the cache and optionally re-opens
   *
   * @author Jean Farris
   * @since 5.0
   */
  protected void closeCache() {

    Integer myVmid = new Integer(RemoteTestModule.getMyVmid());
    boolean cacheClosed = false;
    log().info("### starting close cache, vmid: " + myVmid);
    //Cache.close doesn't cause change in membership -- verified with Bruce after close cache
    // still distributed system member,
    CacheUtil.closeCache();
    MasterController.sleepForMs(1000);
    log().info("### Still have connection so reopen cache for vmid: " + myVmid);
    CacheUtil.createCache();

  }  

  /**
   * A Hydra task that disconnects the current VM from the distributed system.
   * 
   *
   * @author Jean Farris
   * @since 5.0
   */
  public static void HydraTask_disconnect() {
    memberTest.disconnect();
  } 

  /**
   * Disconnects the current VM from the distributed system.
   * 
   *
   * @author Jean Farris
   * @since 5.0
   */
  protected void disconnect() {

    Integer myVmid = new Integer(RemoteTestModule.getMyVmid());
    boolean reconnectMember = TestConfig.tab().booleanAt(MembershipPrms.reconnectMember);

    log().info("### in disconnect reconnectMember: " + reconnectMember);
    boolean disconnected = false;
    log().info("### Disconnecting vmid: " + myVmid);
    Integer pid = (Integer)MembershipBB.getBB().getSharedMap().get(myVmid);
    if (! (pid == null)) {   
      DistributedConnectionMgr.disconnect();
      CacheUtil.closeCache();
      log().info("### Disconnected");
      disconnected = true;
      MembershipBB.getBB().getSharedMap().remove(myVmid);
    } else {
      throw new TestException("Requested member to disconnect not in test map of system members");
    }
    if (reconnectMember && disconnected) {
      log().info("### initialize membership test for vmid: " + myVmid);
      initialize();
    }

    if (! reconnectMember) {
      // Don't do anything further with this client
      throw new StopSchedulingTaskOnClientOrder();
    }

  }  

  /*
  ================================
  TEST TASKS - member verification
  ================================*/

  /**
   * Hydra task which verifies members of the distributed system (per GemFire) match
   * members per the test/
   *
   * @author Jean Farris
   * @since 5.0
   */
  public static void HydraTask_verifyMembership() {
    memberTest.verifyMembership();
  } 

  /**
   * Waits for membership in GemFire to match test, current vm
   * must already be connected to distributed system.
   * @author Jean Farris
   * @since 5.0
   */
  private void verifyMembership() {
    //Proceed only if already a member of distributed system.
    if (! DistributedConnectionMgr.isConnected()) {
      return;
    }

    boolean membersMatchTest = false;
    long startTime = System.currentTimeMillis();
    long endTime = System.currentTimeMillis();
    long elapsedTime = 0
;
    int sleepMs = TestConfig.tab().intAt(MembershipPrms.waitInMembershipCheck);
    int waitThresholdMs = TestConfig.tab().intAt(MembershipPrms.membershipWaitThreshold);

    // while GemFire members do not match test's members 
    log().info("### Starting verify membership");
    while (! membersMatchTest) {
      log().info("### checking test members vs. GemFire members");
      if (membersMatchTest()) {
        membersMatchTest = true;
        endTime = System.currentTimeMillis();
        log().fine("### Member pids match Test pids");
        elapsedTime = endTime - startTime;
        log().fine("###Elapsed time waiting ds member update:: " + elapsedTime);
      }
      if (membersMatchTest) {
	break;
      } else {
	  if (elapsedTime > waitThresholdMs) {
	    // Waited too long - report diffs between members per GemFire and members per test
            // Get test pids
            Set testPids = getTestMemberPids();
            // Get member pids from distribution manager
            Set dsMembers = getMembers();
            Set dsMemberPids = extractPids(dsMembers);

            // report error and diffs in member pids
            String str = "Exceeded threshold waiting for membership update. ";
	    str = str + " Threshold: " + waitThresholdMs + "\n";
            // diff between test and ds members
	    Set setDiffs = setDiff(testPids, dsMemberPids); 
            str = str + " Pids in Test but not in ds members: " + setDiffs + "\n";
            setDiffs = setDiff(dsMemberPids, testPids);
            str = str + " Pids in ds members but not in test: " + setDiffs + "\n";
            throw new TestException(str);
          }
          MasterController.sleepForMs(sleepMs);
      } 
    }
    elapsedTime = endTime - startTime;
    log().info("###Elapsed time waiting for member update: " + elapsedTime);
  }

  /**
   * Waits until no membership change in progress and then
   * proceeds to verify membership
   * @author Jean Farris
   * @since 5.0
   */
  public static void HydraTask_verifyMembershipAfterChange() {

    long totalTaskTimeSec = 
                   TestConfig.tab().longAt(MembershipPrms.totalTaskTimeSec);
    // wait until ok to proceed with membership check
    TestHelper.waitForCounter (MembershipBB.getBB(),
                               "MembershipBB.membershipChangeInProgress",
                               MembershipBB.membershipChangeInProgress,
                               0,
                               true,
                               3000000);

    MembershipBB.incrementCounter(MembershipBB.taskReadyToStart);

    TestHelper.waitForCounter (MembershipBB.getBB(),
                               "MembershipBB.taskReadyToStart",
                               MembershipBB.taskReadyToStart,
                               TestHelper.getNumVMs() - 1,
                               true,
                               3000000);

    MembershipBB.zeroCounter(MembershipBB.verifyCompleted);


    memberTest.verifyMembership();

    MembershipBB.incrementCounter(MembershipBB.taskCompletedToHere);
    TestHelper.waitForCounter (MembershipBB.getBB(),
                               "MembershipBB.taskCompletedToHere",
                               MembershipBB.taskCompletedToHere,
                               TestHelper.getNumVMs() - 1,
                               true,
                               3000000);

    MasterController.sleepForMs(10000);

    long startTime = MembershipBB.readCounter(MembershipBB.startTime);
    long elapsedTime = System.currentTimeMillis() - startTime;
    if (MembershipBB.readCounter(MembershipBB.numTotalTaskCompletions) > 1 || 
        (elapsedTime > (totalTaskTimeSec * TestHelper.SEC_MILLI_FACTOR))) {
	log().info("### Verify reached end of task time");
        MembershipBB.incrementCounter(MembershipBB.numTotalTaskCompletions);
	throw new StopSchedulingTaskOnClientOrder();

    }

    MembershipBB.zeroCounter(MembershipBB.taskReadyToStart);

    /*long startTime = MembershipBB.readCounter(MembershipBB.startTime);;
    long elapsedTime = System.currentTimeMillis() - startTime;
    if (elapsedTime > (totalTaskTimeSec * TestHelper.SEC_MILLI_FACTOR)) {
	log().info("### Verify reached end of task time");
	throw new StopSchedulingOrder();
	}*/

    MembershipBB.incrementCounter(MembershipBB.verifyCompleted);
    TestHelper.waitForCounter (MembershipBB.getBB(),
                               "MembershipBB.verifyCompleted",
                               MembershipBB.verifyCompleted,
                               TestHelper.getNumVMs() - 1,
                               true,
                               3000000);
    MembershipBB.zeroCounter(MembershipBB.taskCompletedToHere);
    MembershipBB.incrementCounter(MembershipBB.membershipChangeInProgress);


  }
    
  /*
  =================================
  Test utility methods use by TASKS
  =================================*/

  /**
   * Removes a member from the blackboard map of members 
   *
   * @author Jean Farris
   * @since 5.0
   */
  private void removeMemberFromMap(Integer vmid) {

    Integer pid = (Integer)MembershipBB.getBB().getSharedMap().remove(vmid);
    if (pid == null) {
      throw new TestException("Expected vmid: " + vmid.intValue() +
                            " not found in member map");
    }
    log().info("### removed Member from test map, pid is: " + pid.intValue());
  }

   /**
   * Selects random VM ID (excluding VM ID of current VM)from blackboard map of members
   *
   * @author Jean Farris
   * @since 5.0
   */
  private Integer getOtherRandomVmId() {


    // get hydra vm Ids for clients (other than this one)
    Vector vmIds = ClientVmMgr.getOtherClientVmids();
     
    // select random member to kill
    int randInt = TestConfig.tab().getRandGen().nextInt(0, vmIds.size() -1);
    // vm id may be in ClientVmMgr but may have been removed from test map of vm ids.
    while (! (MembershipBB.getBB().getSharedMap().containsKey( new Integer(randInt)))) {
	log().info("### try another random member, rand int was: " + randInt);
      randInt = TestConfig.tab().getRandGen().nextInt(0, vmIds.size() -1);
    }

    return (Integer)vmIds.elementAt(randInt);
  }

  /**
   * Verifies members of the distributed system (per GemFire) match
   * members per the test/
   *
   * @author Jean Farris
   * @since 5.0
   */
  private boolean membersMatchTest() {

    Set memberSet = getMembers();
    Set gemFireMemberPids = extractPids(memberSet);
    log().info("###System members pids for Distribution Manager Ids are: " + gemFireMemberPids.toString());

    Set testMemberPids =  getTestMemberPids();
    log().info("###System members pids per test are: " + testMemberPids.toString());

    if (gemFireMemberPids.equals(testMemberPids)) {
      log().info("### Member pids match test pids");
      return true;
    }
    else {
      log().info("### Member pids do not match test pids");
      return false;
    } 
  }


  /**  
   * Verifies input pid is member - waits for configurable amount
   * of time.
   * @author Jean Farris
   * @since 5.0
   */
  private void verifyIsMember(int pid) {
    log().info("###Verifying is member: " + pid);

    boolean isMember = false;
    long elapsedTime = 0;
    long endTime = 0;
    int waitThresholdMs = TestConfig.tab().intAt(MembershipPrms.membershipWaitThreshold);

    log().info("### Waiting for membership updates");
    long startTime = System.currentTimeMillis();
    while ( ! isMember(pid) ) {
      endTime = System.currentTimeMillis();
      elapsedTime = endTime - startTime;
      log().fine("###Elapsed time waiting for new member: " + elapsedTime);
      if (elapsedTime > waitThresholdMs) {
        String str = "Exceeded threshold waiting for membership update. ";
        str = str + " Threshold: " + waitThresholdMs + "\n";
        throw new TestException(str);
      }
    }
    endTime = System.currentTimeMillis();
    elapsedTime = endTime - startTime;
    log().info("###Elapsed time waiting for new member: " + elapsedTime);
  }


  /**
   * Verifies input pid is not member - waits for configurable amount
   * of time.
   * @author Jean Farris
   * @since 5.0
   */
  private void verifyNotMember(int pid) {
    log().info("###Verifying is not member: " + pid);

    boolean isMember = true;

    long startTime = System.currentTimeMillis();
    long endTime = 0;
    long elapsedTime = 0;
    int waitThresholdMs = TestConfig.tab().intAt(MembershipPrms.membershipWaitThreshold);

    log().info("### Waiting for membership update");
    while ( isMember(pid) ) {
      endTime = System.currentTimeMillis();
      elapsedTime = endTime - startTime;
      log().fine("###Elapsed time waiting for member to depart: " + elapsedTime);
      if (elapsedTime > waitThresholdMs) {
        String str = "Exceeded threshold waiting for member to depart. ";
        str = str + " Threshold: " + waitThresholdMs + "\n";
        throw new TestException(str);
      }
    }
    endTime = System.currentTimeMillis();
    elapsedTime = endTime - startTime;
    log().info("###Elapsed time waiting for member to depart: " + elapsedTime);
  }



  /**
   * Returns true if input pid is pid for a current member
   * @author Jean Farris
   * @since 5.0
   */
  private boolean isMember(int pid) {

    Integer memberPid = new Integer(pid);
    Set dsMembers = getMembers();
    Set dsMemberPids = extractPids(dsMembers);
    return dsMemberPids.contains (memberPid);
  }

  /**
   * Returns pids for test members (from blackboard map)
   *
   * @author Jean Farris
   * @since 5.0
   */
  private Set getTestMemberPids() {
    Map memberMap = MembershipBB.getBB().getSharedMap().getMap();
    Set keys = memberMap.keySet();
    TreeSet pids = new TreeSet();
    for ( Iterator it = keys.iterator(); it.hasNext(); ) {
      Integer memberPid = (Integer)memberMap.get(it.next());
      if (! pids.add(memberPid)) {
         throw new TestException(
         "Test has more than one member with same pid, unable to proceed with test, problem pid is: "
         + memberPid);
      }
    }
    return pids;
  }


  /**
   * Returns set of members from GemFire
   *
   * @author Jean Farris
   * @since 5.0
   */
  private Set getMembers() {

      //DistributedSystem ds = DistributedConnectionMgr.connect();
    if (CacheUtil.getCache() == null) {
      CacheUtil.createCache();
    }  
    return CacheUtil.getMembers();

  }

  /**
   * Prints member pids, vm kind for GemFire members, View members
   *
   * @author Jean Farris
   * @since 5.0
   */
  public void printMemberInfo() {

    Set testMemberPids = getTestMemberPids();
    log().info("### Test Member Pids ###");
    for ( Iterator it = testMemberPids.iterator(); it.hasNext(); ) {
      log().info("Test Member Pid:  " + (String)it.next());
    }

    Set members = getMembers();
    log().info("### DM Members ###");
    for ( Iterator it = members.iterator(); it.hasNext(); ) {
      InternalDistributedMember member = (InternalDistributedMember)it.next();
      log().info("Member.toString: " + member.toString());
      log().info("Member vmKind: " + member.getVmKind());
      log().info("Member vmPid:  " + member.getVmPid());
    }

  }


  /**
   * Extracts a set of pids from a set of DistributedMembers
   *
   * @author Jean Farris
   * @since 5.0
   */
  private Set extractPids(Set members) {

    String pid = "";
    TreeSet pids= new TreeSet();
    for ( Iterator it = members.iterator(); it.hasNext(); ) {
      InternalDistributedMember member = (InternalDistributedMember)it.next();

      log().info("### per GF member pid is: " + member.getVmPid());
      Integer vmPid = new Integer(member.getVmPid());
      //log().info("###Member: " + vmPid.toString());
      if (! pids.add(vmPid)) {
	throw new TestException(
                    "GemFire has more than one member with same pid, unable to proceed with test");
      }
    }
    return pids;  
  }

  /**
   * Returns set of elements in set1 but not in set2
   *
   * @author Jean Farris
   * @since 5.0
   */
  private TreeSet setDiff (Set set1, Set set2) {

    log().fine("set 1: " + set1.toString());
    log().fine("set 2: " + set2.toString());

    TreeSet diffs = new TreeSet();
    for ( Iterator it = set1.iterator(); it.hasNext(); ) {
      Object element = it.next();
      if (! (set2.contains(element)) ) {
	  diffs.add(element);
      }
    }
    log().fine("In set1 not in set2: " + diffs.toString());
    return diffs;
  }


  private static LogWriter log() {
    return Log.getLogWriter();
  }


}


