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
package hct.ha;
import hydra.blackboard.Blackboard;


/**
 * A Hydra blackboard that keeps track of what the various task threads in an
 * {@link Feeder} do.
 * @author Suyog Bhokare
 * @author Dinesh Patel
 */
public class HAClientQueueBB extends Blackboard
{

  // Blackboard creation variables
  static String BRIDGE_BB_NAME = "HAClientQueue_Blackboard";

  static String BRIDGE_BB_TYPE = "RMI";

  // Counters for number of times test did certain operations
  public static int NUM_CREATE;

  public static int NUM_UPDATE;

  public static int NUM_INVALIDATE;
  
  public static int NUM_DESTROY;
  
  public static int NUM_EXCEPTION;
  
  public static int NUM_CLIENTS_KILL;
  
  //1 if OverFlow happens in HA
  public static int HA_OVERFLOW_STATUS;

  //Counter to check for the total no. of conflation happened across all the VMs
  public static int NUM_GLOBAL_CONFLATE;

  // Counters used for synchronization
  public static int feedSignal;
  public static int stableSignal;

  private static HAClientQueueBB blackboard;
  
  /**
   * initialize HAClientQueueBB
   */
  public static void initialize()
  {
    getBB().printSharedCounters();
  }

  /**
   * Get the HAClientQueueBB
   */
  public static HAClientQueueBB getBB()
  {
    if (blackboard == null)
      synchronized (HAClientQueueBB.class) {
        if (blackboard == null)
          blackboard = new HAClientQueueBB(BRIDGE_BB_NAME, BRIDGE_BB_TYPE);
      }
    return blackboard;
  }

  /**
   * Zero-arg constructor for remote method invocations.
   */
  public HAClientQueueBB() {
  }

  /**
   * Creates a sample blackboard using the specified name and transport type.
   */
  public HAClientQueueBB(String name, String type) {
    super(name, type, HAClientQueueBB.class);
  }

}
