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

public class SenderRegionQueueListener extends SilenceListener {
  
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
//      bb.throwException(s);
    }
  }
  
  public void afterDestroy(EntryEvent event) {   
//    super.afterDestroy(event);
    SilenceListenerBB.getBB().getSharedCounters().setIfLarger(SilenceListenerBB.lastEventTime, System.currentTimeMillis());
    
    Long currentKey = (Long)event.getKey();    
    if(lastKeyInSender != -1 && (currentKey.longValue() - lastKeyInSender) != 1){
      String s = "Event is not dispatched sequentially from sender queue in vm " + getMyUniqueName() + ". Expected event for key " + (lastKeyInSender + 1) 
      + " , found " + currentKey + ". " + toString("afterDestroy", event);
      lastKeyInSender = currentKey.longValue();
      bb.throwException(s);
    }
    lastKeyInSender = currentKey.longValue();
    
    GatewaySender sender = getSenderForEvent(event);
//    
//    // check for sequential destory (dispatch) of events from a sender
//    GatewaySenderEventImpl senderEvent = (GatewaySenderEventImpl)event.getOldValue();
//    if (senderEvent == null){
//      // this is possible in case the event was evicted
//      Log.getLogWriter().info("Looks like event was evicted from sender " + serialSender.getId() + ". " + toString("afterDestroy", event));
//      keysEvictionted++;
//    }else{
//      String senderEventKey = (String)senderEvent.getKey();
//      ValueHolder lastValue = (ValueHolder)bb.getSharedMap().get(getKeyForSenderRegionQueueLastVal(serialSender.getId(), senderEventKey, senderEvent.getRegionName()));
//      ValueHolder newValue = (ValueHolder)senderEvent.deserialize(senderEvent.getValue());
//      
//      bb.getSharedMap().put(getKeyForSenderRegionQueueLastVal(serialSender.getId(), senderEventKey, senderEvent.getRegionName()), newValue);
//      if(lastValue == null){
//        //first time entry for the key  
//        Log.getLogWriter().info("First time entry for key " + senderEventKey + " " + toString("afterDestroy", event));
//      }else {    
//        long nval = ((Long)newValue.getMyValue()).longValue();
//        long lval = ((Long)lastValue.getMyValue()).longValue();
//        if ((keysEvictionted == 0 && (nval - lval) != 1)){
//          keysEvictionted = 0;
//          String s = "Event is not dispatched sequentially from sender queue in vm " + getMyUniqueName() + " . Expected event for key " + senderEventKey 
//          + " with ValueHolder with myValue=" + (lval + 1 ) + " , found " + nval + ". " + toString("afterDestroy", event);
//          bb.throwException(s);
//        } else if (keysEvictionted != 0){          
//          if ((nval - lval) > 1 + keysEvictionted){
//            String s = "Events are missed from sender queue in " + getMyUniqueName() + " . Expected event for key " + senderEventKey 
//            + " with ValueHolder with myValue=" + (lval + keysEvictionted + 1) + " , found " + nval + ", KeysEvicted=" + keysEvictionted + ". " + toString("afterDestroy", event);            
//            keysEvictionted = 0;
//            bb.throwException(s);            
//          }else if ((nval - lval) < 0){
//            String s = "Events are duplicated at sender queue in " + getMyUniqueName() + " . Expected event for key " + senderEventKey 
//            + " with ValueHolder with myValue=" + (lval + keysEvictionted + 1) + " , found " + nval + ", KeysEvicted=" + keysEvictionted + ". " + toString("afterDestroy", event);            
//            keysEvictionted = 0;
//            bb.throwException(s);            
//          }          
//        }
//      }
//      keysEvictionted = 0;
//    }
    
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
//          bb.throwException(s);
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
//      bb.throwException(s);
      Log.getLogWriter().info(s);
    }
    
    if(sender == null){
      String s = "Possible test issue. No sender is found in " + getMyUniqueName() + " for event " + toString("afterDestroy", event);      
//      bb.throwException(s);
      Log.getLogWriter().info(s);
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

  public static String getKeyForSenderRegionQueueLastVal(String senderid, String key, String regionName) {
    StringBuffer buf = new StringBuffer(KEY_SENDER_RQ_LAST_VAL);
    buf.append("vm_").append(RemoteTestModule.getMyVmid()).append("_").append(
        regionName).append("_").append(senderid).append("_").append(key);
    return buf.toString();
  }
}
