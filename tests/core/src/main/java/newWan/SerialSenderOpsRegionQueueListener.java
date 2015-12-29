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
package newWan;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import hydra.GatewaySenderHelper;
import hydra.Log;
import hydra.RemoteTestModule;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventCallbackArgument;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventImpl;
import com.gemstone.gemfire.internal.cache.wan.parallel.ParallelGatewaySenderImpl;
import com.gemstone.gemfire.internal.cache.wan.serial.SerialGatewaySenderImpl;

import util.SilenceListener;
import util.SilenceListenerBB;
import util.TestException;
import util.ValueHolder;

public class SerialSenderOpsRegionQueueListener extends SilenceListener {
  
  public final static String KEY_SENDER_RQ_LAST_VAL = "SenderRegionQueueLastVal_";
  
  private long lastKeyInSender = -1;
  private int keysEvictionted = 0;
  private Map pausedMap = new HashMap();
  
  WANBlackboard bb = WANBlackboard.getInstance();
  
  public void afterCreate(EntryEvent event) {  
    SilenceListenerBB.getBB().getSharedCounters().setIfLarger(SilenceListenerBB.lastEventTime, System.currentTimeMillis());
    //super.afterCreate(event);
    // create, update or destroy operation on region in own site.
    // event propogating to other site via this sender.    
    
    // no event should get enqueued  when sender status is stopped
    GatewaySender sender = getSenderForEvent(event);
    if(sender != null && !sender.isRunning()){
      boolean isPrimary = (sender instanceof SerialGatewaySenderImpl) ? ((SerialGatewaySenderImpl)sender).isPrimary() 
          : ((ParallelGatewaySenderImpl)sender).isPrimary(); 
      String s = "Event is enqueued even when sender is stopped in vm " + getMyUniqueName() + ". Not allowed with sender config isPrimary=" + isPrimary 
      + ", isRunning=" + sender.isRunning() + ", isPaused=" + sender.isPaused() 
      + GatewaySenderHelper.gatewaySenderToString(sender) + "\n" + toString("afterCreate", event);            
      // for bug #44153      
      // bb.throwException(s);
    }
  }
  
  public void afterDestroy(EntryEvent event) {   
//    super.afterDestroy(event);
    SilenceListenerBB.getBB().getSharedCounters().setIfLarger(SilenceListenerBB.lastEventTime, System.currentTimeMillis());
    GatewaySender sender = getSenderForEvent(event);    
    if(sender !=null){
      if(sender.isPaused()){
        Integer pausedCount = (Integer)pausedMap.get(sender.getId());    
        pausedCount = (pausedCount == null) ? new Integer(1) : pausedCount + 1;        
        pausedMap.put(sender.getId(), pausedCount);
        if(pausedCount.intValue() > sender.getBatchSize()){
          boolean isPrimary = (sender instanceof SerialGatewaySenderImpl) ? ((SerialGatewaySenderImpl)sender).isPrimary() 
              : ((ParallelGatewaySenderImpl)sender).isPrimary();
          String s = "Event dispatched from sender are more than a batch size in vm " + getMyUniqueName() + " , not allowed with sender config isPrimary=" + isPrimary 
          + ", isRunning=" + sender.isRunning() + ", isPaused=" + sender.isPaused() + ", dispatched events from last sender operation=" + pausedCount
          + GatewaySenderHelper.gatewaySenderToString(sender) + "\n" + toString("afterDestroy", event);
          
         // for bug #44153      
         // bb.throwException(s);
        }        
      }else{                 
        pausedMap.put(sender.getId(), new Integer(0));          
      }
    }
  }
  
  private GatewaySender getSenderForEvent(EntryEvent event){
    String regionName = event.getRegion().getName();    
    GatewaySender sender = null;
    Set<GatewaySender> senders = GatewaySenderHelper.getGatewaySenders();    
    if (senders != null) {
      for (GatewaySender s : senders) {
        if (regionName.contains(s.getId())) {
          sender = s;
          break;
        }
      }
    }else {
      String s = "Possible test issue. No senders are found in " + getMyUniqueName() + " for event " + toString("afterDestroy", event);
      Log.getLogWriter().info(s);
//      bb.throwException(s);      
    }
    
    if(sender == null){
      String s = "Possible test issue. No sender is found in " + getMyUniqueName() + " for event " + toString("afterDestroy", event);
      Log.getLogWriter().info(s);
//      bb.throwException(s);
    }    
    return sender;
  }
  
  /**
   *  Uses RemoteTestModule information to produce a name to uniquely identify
   *  a client vm (vmid, clientName, host, pid) for the calling thread
   */
  public String getMyUniqueName() {
    StringBuffer buf = new StringBuffer( 50 );
    buf.append("vm_" ).append(RemoteTestModule.getMyVmid());
    buf.append( "_" ).append(RemoteTestModule.getMyClientName());
    buf.append( "_" ).append(RemoteTestModule.getMyHost());
    buf.append( "_" ).append(RemoteTestModule.getMyPid());
    return buf.toString();
  }
}
