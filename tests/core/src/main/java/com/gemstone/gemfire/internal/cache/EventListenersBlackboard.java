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

package com.gemstone.gemfire.internal.cache;

//import hydra.*;
import hydra.blackboard.*;

/**
*
* Manages the blackboard for tests in this package.
* Holds a singleton instance per VM.
*
* Usage example:
*
*   EventListenersBlackboard.getInstance().getSharedCounters()
*        .increment( EventListenersBlackboard.NumInvocations );
*
*   long numInvocations = EventListenersBlackboard.getInstance().getSharedCounters()
*                              .read( EventListenersBlackboard.NumInvocations );
*
*   EventListenersBlackboard.getInstance().printBlackboard();
*/

public class EventListenersBlackboard extends Blackboard {
  public static int NumInvocations1;
  public static int NumInvocations2;

  public static EventListenersBlackboard blackboard;

  /**
   *  Zero-arg constructor for remote method invocations.
   */
  public EventListenersBlackboard() {
  }

  public EventListenersBlackboard( String name, String type ) {
    super( name, type, EventListenersBlackboard.class );
  }
  /**
   *  Creates a singleton event listeners blackboard.
   */
  public static EventListenersBlackboard getInstance() {
    if ( blackboard == null )
      initialize();
    return blackboard;
  }
  private static synchronized void initialize() {
    if ( blackboard == null )
      blackboard = new EventListenersBlackboard( "EventListenersBlackboard", "rmi" );
  }
  
  public void initNumInvocations() {
    getSharedCounters().zero(NumInvocations1);
    getSharedCounters().zero(NumInvocations2);
  }

  public void incNumInvocations1() {  
    getSharedCounters().increment(NumInvocations1);
  }
  
  public void incNumInvocations2() {  
    getSharedCounters().increment(NumInvocations2);
  }
  
  
  public long getNumInvocations1() {
    return getSharedCounters().read(NumInvocations1);
  }
  
  public long getNumInvocations2() {
    return getSharedCounters().read(NumInvocations2);
  }

  /**
   *  Prints contents of event listeners blackboard.
   */
  public static void printBlackboard() {
    getInstance().print();
  }
}
