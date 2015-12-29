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
package cq.newWan.serial;

import java.util.List;
import java.util.Set;

import newWan.WANBlackboard;

import hydra.DistributedSystemHelper;
import hydra.GatewaySenderHelper;
import hydra.Log;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventCallbackArgument;

import util.QueryObject;
import util.SilenceListener;
import util.SilenceListenerBB;
import util.TestException;
import util.ValueHolder;

public class WANCQCacheListener extends SilenceListener {
  public void afterUpdate(EntryEvent event) {
    SilenceListenerBB.getBB().getSharedCounters().setIfLarger(SilenceListenerBB.lastEventTime, System.currentTimeMillis());
    
    Long oldValue = ((QueryObject)event.getOldValue()).aLong;
    Long newValue = ((QueryObject)event.getNewValue()).aLong;
    
    if((newValue.longValue() - oldValue.longValue()) < 1){
      String err = new String("Expected aLong in newValue should be greated than aLong in oldValue, but found smaller with the difference of " 
          + (newValue.longValue() - oldValue.longValue()) + " \n" + toString("afterUpdate", event));
      WANBlackboard.throwException(err);      
    } else if ((newValue.longValue() - oldValue.longValue()) > 1) {
      //check for isBatchConflationEnabled in bridge servers. No check in adge.
      GatewaySenderEventCallbackArgument callback = (GatewaySenderEventCallbackArgument)((EntryEventImpl)event).getRawCallbackArgument();      
      
      if(callback != null){       
        Integer dsId = DistributedSystemHelper.getDistributedSystemId();        
        if(callback.getOriginatingDSId() != -1 && dsId.intValue() == callback.getOriginatingDSId()){
          String err = new String("Event Loopback issue detected. Event's originatingDSId is same as localDSID which is " + dsId.intValue() + ".\n" + toString("afterUpdate", event));
          WANBlackboard.throwException(err);         
        }
                
        Set<GatewaySender> senders = GatewaySenderHelper.getGatewaySenders();
        if(senders == null || senders.size() == 0){
          String err = new String("Possible test issue, No gateway sender found." + " \n" + toString("afterUpdate", event));
          WANBlackboard.throwException(err);         
        }else{
        //Assuming that all gateway sender has same conflation property set.
          GatewaySender sender = senders.iterator().next();
          if (sender.isBatchConflationEnabled()){
            Log.getLogWriter().info("Difference between aLong in newValue and aLong in oldValue is more than 1." +
                " Sender isBatchedConflationEnabled is true and this is expected in this situation. " + event);
          }else{
            String err = new String("Difference between aLong in newValue and aLong in oldValue should not be more than 1, but found " + (newValue.longValue() - oldValue.longValue()) 
              + ". This is not expected as Gateway sender isBatchedConflationEnabled=false. " + toString("afterUpdate", event));
            WANBlackboard.throwException(err);     
          }
        }
      }     
    }
 }
}
