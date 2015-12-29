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

package com.gemstone.gemfire.distributed;

import com.gemstone.gemfire.internal.Assert;
//import hydra.*;
import hydra.blackboard.*;

/**
*
* Manages the blackboard for DistributedLockServiceTest.
* Holds a singleton instance per VM.
*
* Usage example:
*
*   DistributedLockBlackboard.getInstance().getSharedCounters()
*        .increment( DistributedLockBlackboard.Count );
*
*   long count = DistributedLockBlackboard.getInstance().getSharedCounters()
*                              .read( DistributedLockBlackboard.Count );
*
*   DistributedLockBlackboard.getInstance().printBlackboard();
*/

public class DistributedLockBlackboard extends Blackboard {
  public static int Count;
  public static int IsLocked;

  public static DistributedLockBlackboard blackboard;

  /**
   *  Zero-arg constructor for remote method invocations.
   */
  public DistributedLockBlackboard() {
  }

  public DistributedLockBlackboard( String name, String type ) {
    super( name, type, DistributedLockBlackboard.class );
  }
  /**
   *  Creates a singleton event listeners blackboard.
   */
  public static DistributedLockBlackboard getInstance() {
    if ( blackboard == null )
      initialize();
    return blackboard;
  }
  private static synchronized void initialize() {
    if ( blackboard == null )
      blackboard = new DistributedLockBlackboard( "DistributedLockBlackboard", "rmi" );
  }
  
  public void initCount() {
    getSharedCounters().zero(Count);
  }

  public void incCount() {  
    getSharedCounters().increment(Count);
  }
  
  
  public long getCount() {
    return getSharedCounters().read(Count);
  }
  
  public void setIsLocked(boolean isLocked) {
    if (isLocked) {
      getSharedCounters().setIfLarger(IsLocked, 1);
    } else {
      getSharedCounters().setIfSmaller(IsLocked, 0);
    }
  }
  
  public boolean getIsLocked() {
    long isLocked = getSharedCounters().read(IsLocked);
    Assert.assertTrue(isLocked == 0 || isLocked == 1, 
      "DistributedLockBlackboard internal error - IsLocked is " + isLocked);
    return isLocked == 1;
  }
  
  /**
   *  Prints contents of this blackboard.
   */
  public static void printBlackboard() {
    getInstance().print();
  }
}
