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
package management.operations.ops.jmx;

import static management.util.HydraUtil.logInfo;
import hydra.CacheHelper;
import hydra.ClientVmInfo;
import hydra.RemoteTestModule;
import hydra.TestConfig;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.ObjectName;

import util.StopStartPrms;
import util.StopStartVMs;

import management.jmx.JMXPrms;
import management.operations.SimpleRegionKeyValueConfig;
import management.operations.events.impl.EntryEvents;
import management.operations.events.impl.RegionEvents;
import management.operations.ops.EntryOperations;
import management.operations.ops.JMXOperations;
import management.util.HydraUtil;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.TypeMismatchException;

/**
 * Test Class for generating workload to test Pulse
 * @author tushark
 *
 */
public class PulseTestMBean extends AbstractTestMBean<PulseTestMBean> {
  
  
  private static final long serialVersionUID = 1L;

  static {
    prefix = "PulseTestMBean : ";
  }
  
  
  
  public PulseTestMBean(List<String> attrs, List<Object[]> ops, String ton, String[] tests) {
    super(attrs, ops, PulseTestMBean.class, ton, tests);

  }
  
  public void bounceManager(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling bounceManager");
    
    List<ClientVmInfo> vmList = StopStartVMs.getAllVMs();
    List<ClientVmInfo> selectedVmList = new ArrayList<ClientVmInfo>();
    List<String> stopModeList = new ArrayList<String>();
    int i=0;
    for(ClientVmInfo info : vmList){
      String name = info.getClientName();
      if(name.startsWith("managing")){
        selectedVmList.add(info);
        stopModeList.add(TestConfig.tab().stringAt(StopStartPrms.stopModes));
        i++;
        break;
      }
    }    
    if(i==1){
      StopStartVMs.stopVMs(selectedVmList, stopModeList);
      HydraUtil.sleepForReplicationJMX();
      HydraUtil.sleepForReplicationJMX();
      try {
        Thread.sleep(JMXPrms.pulseManagerHATime());
      } catch (InterruptedException e) {        
        e.printStackTrace();
      }
    }else{
      logInfo(prefix + " bounceManager did not find any manager");
    }    
    StopStartVMs.startVMs(selectedVmList);
    logInfo("Restarting VMS : " + HydraUtil.ObjectToString(selectedVmList));    
    HydraUtil.sleepForReplicationJMX();    
    logInfo(prefix + " Completed bounceManager test successfully");
  }
  
  public void pulseTest(JMXOperations ops, ObjectName targetMbean){
    logInfo(prefix + " Calling pulseTest");
    doEntryOperations();
    raiseAlerts();
    HydraUtil.logInfo("Sleeping for 1 seconds ");
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {      
      //e.printStackTrace();
    }
    HydraUtil.logInfo("Done Sleeping for 5 seconds ");
    logInfo(prefix + " Completed pulseTest test successfully");
  }
  
  private void raiseAlerts() {
    int num = HydraUtil.getnextRandomInt(5);
    if(num==0)
      num++;
    Cache cache = CacheHelper.getCache();
    LogWriter writer = cache.getLogger();
    for(int i=0;i<num;i++){
      String message = "MSG" + System.nanoTime(); 
      writer.severe(message);
    }
  }

  private static Map<String,EntryOperations> allEops = new HashMap<String,EntryOperations>();
  private EntryOperations getEntryOperations(String region){
    EntryOperations eop = null;
    synchronized (allEops) {
      if(!allEops.containsKey(region)){
        Region r = CacheHelper.getCache().getRegion(region);
        EntryOperations eops = new EntryOperations(r, new EntryEvents());
        eops.setKeyValueConfig(new SimpleRegionKeyValueConfig(RemoteTestModule.getMyClientName(), 100000));
        allEops.put(region,eops);
      }
      return allEops.get(region);
    }    
  }
  
  private void doEntryOperations() {    
    List<String> regionList = RegionEvents.getAllRegions();
    for(String region : regionList){
      HydraUtil.logInfo("Doing 10 EntryOperations on " + region);
      EntryOperations eOps = getEntryOperations(region);
      for(int i=0;i<10;i++){
        eOps.doEntryOperation();
      }
      
      //poorman's query operation. Need to be replaced with QueryOperations class.
      try {
        HydraUtil.logInfo("Doing a queryOperations on " + region);
        String query =null;
        if(!region.contains(Region.SEPARATOR))
          query = "select * from /" + region;  
        else
          query = "select * from " + region;
        Cache cache = CacheHelper.getCache();
        Query queryObj = CacheHelper.getCache().getQueryService().newQuery(query);       
        HydraUtil.logInfo("Firing query |" + query + "|");
        queryObj.execute();
        HydraUtil.logInfo("Completed query |" + query + "|");
      } catch (FunctionDomainException e) {       
        HydraUtil.logError("error doing query op",e);
      } catch (TypeMismatchException e) {
        HydraUtil.logError("error doing query op",e);
      } catch (NameResolutionException e) {
        HydraUtil.logError("error doing query op",e);
      } catch (QueryInvocationTargetException e) {
        HydraUtil.logError("error doing query op",e);
      }
    }
    HydraUtil.logInfo("Finished with EntryOperations on " + regionList);
  }

  @Override  
  public String getType() {
    return gemfireDefinedMBeanTypes[PULSEMBean];
  }

  @Override
  public void doValidation(JMXOperations ops) {    
  }

}
