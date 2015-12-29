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
package sql.hdfs;

import hydra.blackboard.*;
import hydra.Log;
import hydra.MasterController;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class AlterHdfsStoreBB extends Blackboard {
  static String SQL_BB_NAME = "AlterHdfsStoreBB_Blackboard";
  static String SQL_BB_TYPE = "RMI";
  
  public static AlterHdfsStoreBB bbInstance = null;

  // Counters
  public static int readyToBegin;        // validator threads wait here before executing alterHdfsStore op
  public static int completedOps;        // validator threads wait at sync point for all to complete concAlterHdfsStore task
  public static int finishedValidation;  // validator threads wiat at sync poing for all to complete validation before returning results
  public static int ExecutionNumber;     // maintains a count of the numbers of rounds of execution
  public static int TimeToStop;       // used to tell all clients it is time to throw StopSchedulingException

  // SharedMaps
  static String ALTERED_ATTRIBUTES = "ALTERED HDFS ATTRIBUTES";

  public static AlterHdfsStoreBB getBB() {
    if (bbInstance == null) {
      synchronized (AlterHdfsStoreBB.class){
        if (bbInstance == null){
          bbInstance = new AlterHdfsStoreBB(SQL_BB_NAME, SQL_BB_TYPE);  
        }
      }        
    }      
    return bbInstance;  
  }
  
  public AlterHdfsStoreBB() {}
  
  public AlterHdfsStoreBB(String name, String type) {
    super(name, type, AlterHdfsStoreBB.class);
  }

  /**
   *  Methods to support modified HDFSStore attributes across all members
   */
  public synchronized static void addAlteredAttribute(String columnName, Object value) {
    Blackboard bb = getBB();
    SharedMap bbMap = bb.getSharedMap();
    // coordinate access
    bb.getSharedLock().lock();
    Map attrMap = (Map)bbMap.get(ALTERED_ATTRIBUTES);
    if (attrMap == null) {
      attrMap = new HashMap();
    }
    attrMap.put(columnName, value);
    bbMap.put(ALTERED_ATTRIBUTES, attrMap);
    bb.getSharedLock().unlock();
    Log.getLogWriter().info("After adding " + columnName + ": " + value + " AlteredAttributesMap = " + bbMap.toString());
  }

  public static Map getAlteredAttributes() {
    Map attrMap = (Map)getBB().getSharedMap().get(ALTERED_ATTRIBUTES);
    return attrMap;
  }

  public static void clearAlteredAttributes() {
    Blackboard bb = getBB();
    SharedMap bbMap = bb.getSharedMap();
    if (bbMap.get(ALTERED_ATTRIBUTES) != null) {
      bbMap.put(ALTERED_ATTRIBUTES, new HashMap());
    }
    Log.getLogWriter().info("AlterHdfsStoreBB: clearing AlteredAttributes Map");
  }
}
