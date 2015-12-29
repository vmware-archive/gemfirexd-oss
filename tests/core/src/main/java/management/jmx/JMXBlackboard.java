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
package management.jmx;

import hydra.Log;
import hydra.blackboard.Blackboard;
import hydra.blackboard.SharedLock;
import hydra.blackboard.SharedMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import management.util.HydraUtil;

import com.gemstone.gemfire.LogWriter;

/**
 * -
 * 
 * JMXBlackboards stores following information
 * 
 * Config Information
 * JMXEndPoints : Urls of all RMI Connectors started
 * 
 * Runtime Events
 * JMXNotifications
 * JMX Operation Results
 * 
 * Expected State
 * JMXNotifications
 * JMX Attribute Values
 * Operation Results
 * 
 * List of Validation Objects to add mandatory validations
 * 
 * @author tushark
 * 
 */

public class JMXBlackboard extends Blackboard implements JMXEventRecorder {

  public static final String MBEAN_MAP = "MBEAN_MAP";
  static String CONNECTOR_URL_LIST = "CONNECTOR_URL_MAP";
  static String JMX_EVENTS = "JMX_EVENTS";
  static String JMX_EXPECTED_STATE = "JMX_EXPECTED_STATE";
  public static String REGION_DELETE_LIST = "REGION_DELETE_LIST";
  
  static String BB_NAME = "JMX_Blackboard";
  static String BB_TYPE = "RMI";

  private static JMXBlackboard bbInstance = null;

  public static JMXBlackboard getBB() {
    if (bbInstance == null) {
      synchronized (JMXBlackboard.class) {
        if (bbInstance == null)
          bbInstance = new JMXBlackboard(BB_NAME, BB_TYPE);
      }
    }
    return bbInstance;
  }
  
  public static int VM_TASKS_COMPLETE_COUNT;

  public JMXBlackboard() {
  }

  public JMXBlackboard(String name, String type) {
    super(name, type, JMXBlackboard.class);
  }

  public void addRMIPort(String clientName, String url) {
    synchronized (this) {
      Map<String, String> map = (Map<String, String>) getSharedMap().get(CONNECTOR_URL_LIST);
      map.put(clientName, url);
      getSharedMap().put(CONNECTOR_URL_LIST, map);
    }
  }

  public String getRandomClient() {
    LogWriter logger = Log.getLogWriter();
    Map<String, String> map = (Map<String, String>) getSharedMap().get(CONNECTOR_URL_LIST);
    if (map.size() > 0) {
      Random r = new Random();
      int randomKey = r.nextInt(map.size());
      Set<String> keys = map.keySet();
      Object keysarray[] = keys.toArray();
      String clientName = (String) keysarray[randomKey];
      logger.info("URL for client " + clientName + " is " + map.get(clientName));
      String url = map.get(clientName);
      return url;
    } else
      return null;

  }

  public synchronized void addEvent(JMXEvent event) {
    SharedLock lock = getSharedLock();
    try {
      lock.lock();
      ArrayList<JMXEvent> list = (ArrayList<JMXEvent>) getSharedMap().get(JMX_EVENTS);
      list.add(event);
      getSharedMap().put(JMX_EVENTS, list);
    } finally {
      lock.unlock();
    }
  }

  public synchronized void addExpectation(Expectation ex) {
    SharedLock lock = getSharedLock();
    try {
      lock.lock();
      ArrayList<Expectation> list = (ArrayList<Expectation>) getSharedMap().get(JMX_EXPECTED_STATE);
      list.add(ex);
      getSharedMap().put(JMX_EXPECTED_STATE, list);
    } finally {
      lock.unlock();
    }
  }

  public static void JMXBlackboard_Init(){
    getBB().initBlackBoard();
  }

  public  synchronized void initBlackBoard() {
    SharedLock lock = getSharedLock();
    try {
      lock.lock();
      if (!getBB().getSharedMap().containsKey(CONNECTOR_URL_LIST))
        getBB().getSharedMap().put(CONNECTOR_URL_LIST, new HashMap());
  
      if (!getBB().getSharedMap().containsKey(JMX_EVENTS))
        getBB().getSharedMap().put(JMX_EVENTS, new ArrayList<JMXEvent>());
  
      if (!getBB().getSharedMap().containsKey(JMX_EXPECTED_STATE))
        getBB().getSharedMap().put(JMX_EXPECTED_STATE, new ArrayList<Expectation>());
    } finally {
      lock.unlock();
    }
  }

  public List<JMXEvent> getRecordedEvents() {
    return (ArrayList<JMXEvent>) getSharedMap().get(JMX_EVENTS);
  }

  public List<Expectation> getExpectedState() {
    return (ArrayList<Expectation>) getSharedMap().get(JMX_EXPECTED_STATE);
  }
  
  public void saveMap(String key, Map map) {
    SharedLock lock = getSharedLock();
    try {
      lock.lock();
      SharedMap smap = getSharedMap();//.put(key, map);
      int size = smap.size();
      int newSize = map.size();
      if(size!=newSize){//based on size only
        smap.put(key, map);
        HydraUtil.logInfo("Updating map for key " + key + " old Map : " + smap + " new Map : " + map);
      }else{
        HydraUtil.logInfo("Not Updating map for key " + key + " since size is matching. old Map : " + smap + " new Map : " + map);
      }
    }finally{
      lock.unlock();
    }
  }
  
  @SuppressWarnings("unchecked")
  public synchronized void addToList(String list, Object object) {
    SharedLock lock = getSharedLock();
    try {
      lock.lock();
      List bbList = (List) getBB().getSharedMap().get(list);
      if(bbList==null)
        bbList = new ArrayList();
      bbList.add(object);
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
      if(bbList==null)
        return new ArrayList();
      return bbList;
    } finally {
      lock.unlock();
    }
  }
  

  public Map getMap(String key) {
    return (Map) getSharedMap().get(key);
  }

}
