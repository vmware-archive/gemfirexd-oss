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
package parReg.eviction;

// import java.util.*;
import util.*;
import hydra.*;
import hydra.blackboard.*;

// import com.gemstone.gemfire.cache.*;

/**
 * Blackboard defining counters for events. This blackboard has counters with
 * names used by the increment methods in {@link util#AbstractListener}. Any
 * subclass of AbstractListener that wants to use AbstractListener's counter
 * increment methods can use this blackboard for that purpose. Note that the
 * names of the blackboard counters defined here must be the same names as
 * defined in AbstractListener.
 */
public class EvictionBB extends Blackboard {

  // Blackboard variables
  static String BB_NAME = "Eviction_Blackboard";

  static String BB_TYPE = "RMI";

  // singleton instance of blackboard
  private static EvictionBB bbInstance = null;

  // Exception counters
  public static int NUM_EXCEPTION;

  public static int NUM_COMPLETED_EXCEPTION_LOGGING;
  
//Excecution counters

  public static int ExecutionNumber;

  public static int RoundPosition;

  public static int RoundNumber;

  /**
   * Get the ParRegExpirationBB
   */
  public static EvictionBB getBB() {
    if (bbInstance == null) {
      synchronized (EvictionBB.class) {
        if (bbInstance == null)
          bbInstance = new EvictionBB(BB_NAME, BB_TYPE);
      }
    }
    return bbInstance;
  }

  /**
   * Zero-arg constructor for remote method invocations.
   */
  public EvictionBB() {
  }

  /**
   * Creates a sample blackboard using the specified name and transport type.
   */
  public EvictionBB(String name, String type) {
    super(name, type, EvictionBB.class);
  }

  /**
   * Zero all counters in this blackboard.
   */
  public void zeroAllCounters() {
    SharedCounters sc = getSharedCounters();
    sc.zero(NUM_EXCEPTION);
    sc.zero(NUM_COMPLETED_EXCEPTION_LOGGING);
  }

}
