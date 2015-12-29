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
package management.test.federation;

import static management.util.HydraUtil.logErrorAndRaiseException;
import hydra.JMXManagerHelper;
import hydra.JMXManagerHelper.Endpoint;
import hydra.RemoteTestModule;
import hydra.blackboard.Blackboard;
import hydra.blackboard.SharedLock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import management.util.HydraUtil;
import management.util.ManagementUtil;

public class FederationBlackboard extends Blackboard {

  static final String BB_NAME = "MGMT_FED_Blackboard";
  static final String BB_TYPE = "RMI";
  private static FederationBlackboard bbInstance = null;
  public static String urlTemplate = "service:jmx:rmi:///jndi/rmi://?1:?2/jmxrmi";

  public static FederationBlackboard getBB() {
    if (bbInstance == null) {
      synchronized (FederationBlackboard.class) {
        if (bbInstance == null)
          bbInstance = new FederationBlackboard(BB_NAME, BB_TYPE);
      }
    }
    return bbInstance;
  }

  public FederationBlackboard() {
  }

  public FederationBlackboard(String name, String type) {
    super(name, type, FederationBlackboard.class);
  }

  public static int MBEAN_OBJECTNAME_COUNT;

  public int getNextObjectNameCounter() {
    return (int) getBB().getSharedCounters().incrementAndRead(MBEAN_OBJECTNAME_COUNT);
  }

  public int getObjectNameCounter() {
    return (int) getSharedCounters().read(MBEAN_OBJECTNAME_COUNT);
  }

  public void addManagingNode(String vmId, String url) {
    SharedLock lock = this.getSharedLock();
    try {
      lock.lock();
      Map map = (Map) getSharedMap().get("MANAGING_NODE_URLS");
      if (map == null)
        map = new HashMap();
      map.put(vmId, url);
      getSharedMap().put("MANAGING_NODE_URLS", map);
    } finally {
      lock.unlock();
    }
  }
  
  public Map getMemberONs(){
    SharedLock lock = this.getSharedLock();
    try {
      lock.lock();
      Map map = (Map) getSharedMap().get("MEMBERS");
      return map;
    } finally {
      lock.unlock();
    }
  }
  
  public Map getMemberNames(){
    SharedLock lock = this.getSharedLock();
    try {
      lock.lock();
      Map map = (Map) getSharedMap().get("MEMBERNAMES");
      return map;
    } finally {
      lock.unlock();
    }
  }
  
  public Map getManagerONs(){
    SharedLock lock = this.getSharedLock();
    try {
      lock.lock();
      Map map = (Map) getSharedMap().get("MANAGERS");
      return map;
    } finally {
      lock.unlock();
    }
  }
  
  public void addMemberON(String vmId, String on) {
    SharedLock lock = this.getSharedLock();
    try {
      lock.lock();
      Map map = (Map) getSharedMap().get("MEMBERS");
      if (map == null)
        map = new HashMap();
      HydraUtil.logInfo("Adding " + vmId + " into map " + HydraUtil.ObjectToString(map));
      map.put(vmId, on);
      getSharedMap().put("MEMBERS", map);
    } finally {
      lock.unlock();
    }
  }
  
  public void addMemberName(String vmId, String name) {
    SharedLock lock = this.getSharedLock();
    try {
      lock.lock();
      Map map = (Map) getSharedMap().get("MEMBERNAMES");
      if (map == null)
        map = new HashMap();
      HydraUtil.logInfo("Adding " + vmId + " into map " + HydraUtil.ObjectToString(map));
      map.put(vmId, name);
      getSharedMap().put("MEMBERNAMES", map);
    } finally {
      lock.unlock();
    }
  }
  
  public void addManagerON(String vmId, String on) {
    SharedLock lock = this.getSharedLock();
    try {
      lock.lock();
      Map map = (Map) getSharedMap().get("MANAGERS");
      if (map == null)
        map = new HashMap();
      map.put(vmId, on);
      HydraUtil.logInfo("Adding " + vmId + " into map " + HydraUtil.ObjectToString(map));
      getSharedMap().put("MANAGERS", map);
    } finally {
      lock.unlock();
    }
  }

  public String getManagingNode() {
    /*SharedLock lock = this.getSharedLock();
    try {
      lock.lock();
      Map map = (Map) getSharedMap().get("MANAGING_NODE_URLS");
      if (map == null)
        logErrorAndRaiseException("No Urls registered. Map is not initialised");
      if (map.size() > 0){
        //return (String) map.values().toArray()[0];
        String url = (String)HydraUtil.getRandomElement(map.values().toArray());
        return url;
      }
      else
        logErrorAndRaiseException("No Urls registered. Map is empty");
    } finally {
      lock.unlock();
    }*/
    Endpoint pt = HydraUtil.getRandomElement(ManagementUtil.filterForThisDS(JMXManagerHelper.getEndpoints()));
    if(pt!=null){
      String url = urlTemplate.replace("?1", pt.getHost());
      url = url.replace("?2", ""+pt.getPort());
      return url;
    }else    
      return null;
  }
  
  public String getOtherManagingNode() {
    /*SharedLock lock = this.getSharedLock();
    try {
      lock.lock();
      Map map = (Map) getSharedMap().get("MANAGING_NODE_URLS");
      if (map == null)
        logErrorAndRaiseException("No Urls registered. Map is not initialised");
      if (map.size() > 0){
        //return (String) map.values().toArray()[0];
        String url = (String)HydraUtil.getRandomElement(map.values().toArray());
        return url;
      }
      else
        logErrorAndRaiseException("No Urls registered. Map is empty");
    } finally {
      lock.unlock();
    }*/
    List<Endpoint> dsFilteredList = ManagementUtil.filterForThisDS(JMXManagerHelper.getEndpoints());
    List<Endpoint> otherManagingNodes = new ArrayList<Endpoint>();
    for(Endpoint e : dsFilteredList){
      int vmId = RemoteTestModule.getMyVmid();
      if(vmId!=e.getVmid())
        otherManagingNodes.add(e);
    }
    Endpoint pt = HydraUtil.getRandomElement(otherManagingNodes);
    if(pt!=null){
      String url = urlTemplate.replace("?1", pt.getHost());
      url = url.replace("?2", ""+pt.getPort());
      return url;
    }else    
      return null;
  }
  
  public String getMyManagingNode() {
    /*SharedLock lock = this.getSharedLock();
    try {
      lock.lock();
      Map map = (Map) getSharedMap().get("MANAGING_NODE_URLS");
      if (map == null)
        logErrorAndRaiseException("No Urls registered. Map is not initialised");
      if (map.size() > 0){
        //return (String) map.values().toArray()[0];
        String url = (String)HydraUtil.getRandomElement(map.values().toArray());
        return url;
      }
      else
        logErrorAndRaiseException("No Urls registered. Map is empty");
    } finally {
      lock.unlock();
    }*/
    Endpoint pt = null;
    for(Endpoint e : JMXManagerHelper.getEndpoints()){
      int vmId = RemoteTestModule.getMyVmid();
      if(vmId==e.getVmid()){
        pt = e;
        break;
      }
    }    
    if(pt!=null){
      String url = urlTemplate.replace("?1", pt.getHost());
      url = url.replace("?2", ""+pt.getPort());
      return url;
    }else    
      return null;
  }

  public String getManagingNode(String vmId) {
    SharedLock lock = this.getSharedLock();
    try {
      lock.lock();
      Map map = (Map) getSharedMap().get("MANAGING_NODE_URLS");
      if (map == null)
        logErrorAndRaiseException("No Urls registered. Map is not initialised");
      if (map.size() > 0)
        return (String) map.get(vmId);
      else
        logErrorAndRaiseException("No Urls registered. Map is empty");
    } finally {
      lock.unlock();
    }
    return null;
  }

  public Collection<String> getManagingNodes() {
    /*SharedLock lock = this.getSharedLock();
    try {
      lock.lock();
      Map map = (Map) getSharedMap().get("MANAGING_NODE_URLS");
      return map.values();
    } finally {
      lock.unlock();
    }*/
    List<Endpoint> dsFilteredList = ManagementUtil.filterForThisDS(JMXManagerHelper.getEndpoints());
    List<String> urls = new ArrayList<String>();
    for(Endpoint pt : dsFilteredList){
      String url = urlTemplate.replace("?1", pt.getHost());
      url = url.replace("?2", ""+pt.getPort());
      if(urlsChecked)
        urls.add(url);
      else{
        if(ManagementUtil.checkUrl(url))
          urls.add(url);
      }
    }
    return urls;    
  }
  
  public static boolean urlsChecked = false;

}
