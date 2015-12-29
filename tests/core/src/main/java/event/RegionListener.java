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
package event;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;
import util.*;
import hydra.*;

/**
 * RegionMembershipListener to handle region membership events : 
 * afterRemoteRegionCreate, afterRemoteRegionDeparture, afterRemoteRegionCrash
 * Listeners are named by the region they monitor based on Region names 
 * received from util.NameFactory()
 *
 * @author lhughes
 * @since 5.0
 *
 * @see RegionBB counters (operation execution counts)
 * @see RegionEventCountersBB (counts event receipt by listeners)
 * @see util.NameFactory
 */
public class RegionListener extends util.AbstractListener implements RegionMembershipListener, Declarable {

  private String name = "EventRegion";
  private boolean isCarefulValidation;

  /**
   * noArg contstructor
   */
  public RegionListener() {
     this.isCarefulValidation = TestConfig.tab().booleanAt(Prms.serialExecution);
  }

  /**
   *  Create a new listener and specify the assigned name for this listener
   */
  public RegionListener(String name) {
     this.name = name;
     this.isCarefulValidation = TestConfig.tab().booleanAt(Prms.serialExecution);
  }

  /**
   *  Set method for name when instantiated with noArg constructor
   */
  public void setName(String name) {
     this.name = name;
  }

  /**
   *  Get method for name
   */
  public String getName() {
     return this.name;
  }

  /**
   * Implementation of initialMembers (invoked at Listener init time with
   * list of members who already have this region defined).
   *
   * Writes this list to 
   *
   * @params members - DistributedMember[] of the members who already have this
   *                   region defined.
   */
  public void initialMembers(Region r, DistributedMember[] members) {
     if (r.getParentRegion() == null) { // this is initializing the root region
        return;
     }
     StringBuffer aStr = new StringBuffer("RegionListener: InitialMembers for region " + this.getName() + " =\n");
     for (int i = 0; i < members.length; i++) {
        aStr.append("   member[" + i + "] = " + members[i] + "\n");
     }
     Log.getLogWriter().info(aStr.toString());

     // For serialRR tests, check this against the EventBB.numInRound
     // For RRLeader, there should be 0 members, for numInRound 1, there should be 1, etc.
     if (EventBB.isSerialRR()==true) {
        long rrNum = EventBB.getBB().getSharedCounters().read(EventBB.numInRound);
        if (rrNum != members.length) {
           throwException("RegionListener.initialMembers list length may not have enough members.  Expected " + rrNum + " members, but found " + members.length + " : " + aStr.toString());
        }
     }
  }

  /**
   * Implementation of afterRemoteRegionCreate()
   *
   * @param event RegionEvent 
   */
  public void afterRemoteRegionCreate(RegionEvent event) {
    logCall("afterRemoteRegionCreate", event);
    EventBB.incrementCounter("EventBB.actualRegionCreateEvents", EventBB.actualRegionCreateEvents);
  }
  
  /**
   * Implementation of afterRemoteRegionDeparture
   *
   * @param event RegionEvent 
   */
  public void afterRemoteRegionDeparture(RegionEvent event) {
    logCall("afterRemoteRegionDeparture", event);
    EventBB.incrementCounter("EventBB.actualRegionDepartedEvents", EventBB.actualRegionDepartedEvents);
  }

  /**
   * Implementation of afterRemoteRegionDeparture
   *
   * @param event RegionEvent 
   */
  public void afterRemoteRegionCrash(RegionEvent event) {
    logCall("afterRemoteRegionCrash", event);
  }

  // CacheListener implmentation
  public void afterCreate(EntryEvent event) {
     logCall("afterCreate", event);
  }

  public void afterDestroy(EntryEvent event) {
     logCall("afterDestroy", event);
  }

  public void afterInvalidate(EntryEvent event) {
     logCall("afterInvalidate", event);
  }

  public void afterRegionDestroy(RegionEvent event) {
     logCall("afterRegionDestroy", event);
  }

  public void afterRegionInvalidate(RegionEvent event) {
     logCall("afterRegionInvalidate", event);
  }

  public void afterUpdate(EntryEvent event) {
     logCall("afterUpdate", event);
  }

  public void close() {
     logCall("close", null);
  }

  public void afterRegionClear(RegionEvent event) {
    logCall("afterRegionClear", event);
  }

  public void afterRegionCreate(RegionEvent event) {
    logCall("afterRegionCreate", event);
  }
  
  public void afterRegionLive(RegionEvent event) {
    logCall("afterRegionLive", event);
  }

  /**
   * Utility method to write an Exception string to the Event Blackboard and
   * to also throw an exception containing the same string.
   *
   * @param errStr String to log, post to EventBB and throw
   * @throws TestException containing the passed in String
   *
   * @see util.TestHelper.checkForEventError
   */
  protected void throwException(String errStr) {
        hydra.blackboard.SharedMap aMap = EventBB.getBB().getSharedMap();
        aMap.put(TestHelper.EVENT_ERROR_KEY, errStr + " " + TestHelper.getStackTrace());
        Log.getLogWriter().info(errStr);
        throw new TestException(errStr);
  }

public void init(java.util.Properties prop) {
   logCall("init(Properties)", null);
}

}


