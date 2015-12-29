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
package swarm.bb;

import swarm.TestType;
import hydra.blackboard.Blackboard;

/**
 * A Hydra blackboard that keeps track of which tests need to be dispatched
 * 
 * @author kbanks 
 * @since 5.5
 * 
 */
public class SwarmBB extends Blackboard {

  // Blackboard creation variables
  static String BB_NAME = "Swarm_Blackboard";

  static String BB_TYPE = "RMI";
  public static String RUN_ID = "RUN_ID";
  
  private static SwarmBB blackboard;
  
  public static int totalTestCount;
  public static int currentTestCount;
  
  public static final String TEST_TYPE = "TEST_TYPE";
  
  
  public static long getTotalTestCount() {
    return (int)getBB().getSharedCounters().read(totalTestCount);
  }
  
  public static void setTotalTestCount(long i) {
    getBB().getSharedCounters().setIfLarger(totalTestCount,i);
  }
  
  
  public static long getCurrentTestCount() {
    return (int)getBB().getSharedCounters().read(currentTestCount);
  }
  
  public static void incCurrentTestCount() {
    getBB().getSharedCounters().increment(currentTestCount);
  }
  
  public static void setTestType(TestType t) {
    getBB().getSharedMap().put(TEST_TYPE,t);  
  }
  
  public static TestType getTestType() {
    TestType t = (TestType)getBB().getSharedMap().get(TEST_TYPE);
    if(t==null) {
      return TestType.HYDRA;
    } else {
      return t;
    }
  }

  
  /**
   * initialize SwarmBB
   */
  public static void initialize() {
    getBB().printSharedCounters();
  }

  /**
   * Get the SwarmBB
   */
  public static SwarmBB getBB() {
    if (blackboard == null)
      synchronized (SwarmBB.class) {
        if (blackboard == null)
          blackboard = new SwarmBB(BB_NAME, BB_TYPE);
      }
    return blackboard;
  }

  /**
   * Zero-arg constructor for remote method invocations.
   */
  public SwarmBB() {
  }

  /**
   * Creates a sample blackboard using the specified name and transport type.
   */
  public SwarmBB(String name, String type) {
    super(name, type, SwarmBB.class);
  }

}
