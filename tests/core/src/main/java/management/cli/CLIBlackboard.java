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
package management.cli;

import hydra.blackboard.Blackboard;
import hydra.blackboard.SharedLock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import management.operations.ops.cli.TestCommand;
import management.operations.ops.cli.TestCommandInstance;

/**
 * 
 * CLIBlackboard stores following information
 * 
 * Commands fired and its results
 * 
 * Expected State Expected Commands and its results
 * 
 * 
 * @author tushark
 * 
 */
@SuppressWarnings("rawtypes")
public class CLIBlackboard extends Blackboard {

  static String BB_NAME = "CLI_Blackboard";
  static String BB_TYPE = "RMI";

  private static CLIBlackboard bbInstance = null;
  public static int VM_TASKS_COMPLETE_COUNT;
  
  //Gfsh command number
  public static int gfshExecutionNumber;
  public static int gfshExecutionNumberCompleted;
  
  //Gemfire Exec number : This is used if command requires any precondition which it can perform
  //from non-gfsh members
  public static int gemfireExecutionNumber;
  public static int gemfireExecutionNumberCompleted;
  
  public static int functionExecutionNumber;
  public static int functionDeletionNumber;
  
  public static int FILE_COUNTER;
  public static int DIRECTORY_COUNTER;
  public static int REGION_COUNTER;
  
  
  public static CLIBlackboard getBB() {
    if (bbInstance == null) {
      synchronized (CLIBlackboard.class) {
        if (bbInstance == null)
          bbInstance = new CLIBlackboard(BB_NAME, BB_TYPE);
      }
    }
    return bbInstance;
  }

  public CLIBlackboard() {
  }

  public CLIBlackboard(String name, String type) {
    super(name, type, CLIBlackboard.class);
  }

  public static final String COMMAND_MAP = "commandMap";
  public static final String COMMAND_EXECUTOR_MAP = "commandExecutorsMap";
  public static final String COMMAND_TASK_MAP = "commandTaskMap";
  public static final String FUNCTION_EXEC_LIST = "FUNCTION_EXEC_LIST";
  public static final String FUNCTION_EXEC_RESULTCOLLECTOR = "FUNCTION_EXEC_RESULTCOLLECTOR";
  public static final String FUNCTION_DELETE_LIST = "FUNCTION_DELETE_LIST";
  public static final String GW_SENDER_ID_LIST = "GW_SENDER_ID_LIST";
  public static final String GW_SENDER_RUNNING_LIST = "GW_SENDER_RUNNING";
  public static final String GW_SENDER_PAUSED_LIST = "GW_SENDER_PAUSED";
  public static final String GW_SENDER_STOPPED_LIST = "GW_SENDER_STOPPED";
  public static final String GW_RECEIVER_STOPPED_LIST = "GW_RECEIVER_STOPPED";
  

  public void saveMap(String key, Map map) {
    getSharedMap().put(key, map);
  }

  public Map getMap(String key) {
    return (Map) getSharedMap().get(key);
  }
  
  @SuppressWarnings("unchecked")
  public void addCommandsForNextExecution(TestCommand command, TestCommandInstance instance){
    SharedLock lock = getSharedLock();
    try {
      lock.lock();      
      Map map = getBB().getMap(COMMAND_TASK_MAP);
      map.put("command", command);
      map.put("instance", instance);
      getBB().saveMap(COMMAND_TASK_MAP,map);
    } finally {
      lock.unlock();
    }
  }
  
  public Object[] getCommandsForNextExecution(){
    SharedLock lock = getSharedLock();
    try {
      lock.lock();      
      Map map = getBB().getMap(COMMAND_TASK_MAP);
      TestCommand command = (TestCommand) map.get("command");
      TestCommandInstance instance = (TestCommandInstance) map.get("instance");
      return new Object[] {command, instance};      
    } finally {
      lock.unlock();
    }
  }
  @SuppressWarnings("unchecked")
  public synchronized void addToList(String list, Object object) {
    SharedLock lock = getSharedLock();
    try {
      lock.lock();
      List bbList = (List) getBB().getSharedMap().get(list);
      bbList.add(object);
      getBB().getSharedMap().put(list,bbList);
    } finally {
      lock.unlock();
    }
  }
  
  @SuppressWarnings("unchecked")
  public synchronized void clearList(String list) {
    SharedLock lock = getSharedLock();
    try {
      lock.lock();
      List bbList = (List) getBB().getSharedMap().get(list);
      bbList.clear();
      getBB().getSharedMap().put(list,bbList);
    } finally {
      lock.unlock();
    }
  }
  
  @SuppressWarnings("unchecked")
  public synchronized void saveList(String list, List bbList) {
    SharedLock lock = getSharedLock();
    try {
      lock.lock();      
      getBB().getSharedMap().put(list,bbList);
    } finally {
      lock.unlock();
    }
  }
  
  @SuppressWarnings("unchecked")
  public synchronized void removeFromList(String list, Object object) {
    SharedLock lock = getSharedLock();
    try {
      lock.lock();
      List bbList = (List) getBB().getSharedMap().get(list);
      bbList.remove(object);
      getBB().getSharedMap().put(list,bbList);
    } finally {
      lock.unlock();
    }
  }
  
  @SuppressWarnings("unchecked")
  public synchronized List getList(String list) {
    SharedLock lock = getSharedLock();
    try {
      lock.lock();
      List bbList = (List) getBB().getSharedMap().get(list);
      return bbList;
    } finally {
      lock.unlock();
    }
  }
  
  public synchronized void initBlackBoard() {
    SharedLock lock = getSharedLock();
    try {
      lock.lock();
      if (!getBB().getSharedMap().containsKey(COMMAND_TASK_MAP))
        getBB().getSharedMap().put(COMMAND_TASK_MAP, new HashMap());
      
      if (!getBB().getSharedMap().containsKey(FUNCTION_EXEC_LIST))
        getBB().getSharedMap().put(FUNCTION_EXEC_LIST, new ArrayList());
      
      if (!getBB().getSharedMap().containsKey(FUNCTION_EXEC_RESULTCOLLECTOR))
        getBB().getSharedMap().put(FUNCTION_EXEC_RESULTCOLLECTOR, new ArrayList());
      
      if (!getBB().getSharedMap().containsKey(FUNCTION_DELETE_LIST))
        getBB().getSharedMap().put(FUNCTION_DELETE_LIST, new ArrayList());
      
      if (!getBB().getSharedMap().containsKey(GW_SENDER_ID_LIST))
        getBB().getSharedMap().put(GW_SENDER_ID_LIST, new ArrayList());
      
    } finally {
      lock.unlock();
    }
  }

  /*
   * public String getCLIExecutor(String name){ Map<String,String> map
   * =getMap(COMMAND_EXECUTOR_MAP); if(map.containsKey(name)) return
   * map.get(name); else throw new TestException("No Executor found named " +
   * name); }
   */

}
