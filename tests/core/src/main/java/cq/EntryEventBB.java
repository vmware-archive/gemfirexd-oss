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

import hydra.*;
import hydra.blackboard.Blackboard;

/**
 * A Hydra blackboard used for entry event
 * 
 * @author eshu
 *
 */
public class EntryEventBB extends Blackboard {
  static String BB_NAME = "EntryEventBB";
  static String BB_TYPE = "RMI";

  private static EntryEventBB blackboard = null; 
  
//Counters for number of times entry Listener processed various events
  public static int NUM_CREATE;
  public static int NUM_UPDATE;
  public static int NUM_DESTROY;
  public static int NUM_INVALIDATE;
  public static int NUM_CLOSE;
  
  /**
   *  Gets a singleton instance of the BB
   */
  public static synchronized EntryEventBB getBB() {
    if (blackboard == null) {
      blackboard = new EntryEventBB(BB_NAME, BB_TYPE );
    }      
    return blackboard;  
  }
  
  public EntryEventBB() {
    
  }
  
  /**
   *  Creates a sample blackboard using the specified name and type.
   */
  public EntryEventBB(String name, String type) {
    super(name, type, EntryEventBB.class);
 }
}
