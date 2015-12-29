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
/**
 * 
 */
package cq;

import hydra.blackboard.Blackboard;
import parReg.query.Position;

/**
 * @author eshu
 *
 */
public class ConcCQBB extends Blackboard {
//Blackboard creation variables
  static String CONCCQ_BB_NAME = "concCQ_Blackboard";
  static String CONCCQ_BB_TYPE = "RMI";

  public static ConcCQBB bbInstance = null;
  
  public static int OpsForQualifiedCount;
  
  public static synchronized ConcCQBB getBB() {
    if (bbInstance == null) {
        bbInstance = new ConcCQBB(CONCCQ_BB_NAME, CONCCQ_BB_TYPE);
    }      
    return bbInstance;  
  }
  
  public static void HydraTask_initialize() {
    //do nothing
  }
  
  public void initialize(String key) {
    hydra.blackboard.SharedMap aMap = getBB().getSharedMap();
    
    aMap.put(key, new Position());
  }
  
  public ConcCQBB() {
    
  }
  
  public ConcCQBB(String name, String type) {
    super(name, type, ConcCQBB.class);
 }
  
  /**
   * To write the poistion onto the shared map.
   * @param key
   * @param position
   */
  public static void setPosition(String key, Position position) {
     hydra.blackboard.SharedMap aMap = getBB().getSharedMap();
     aMap.put(key, position);
  }

  /**
   * To get the position from the shared map.
   * @param key
   * @return
   */
  public static Position getPosition(String key) {
     return ((Position)(getBB().getSharedMap().get(key)));
  }
}






