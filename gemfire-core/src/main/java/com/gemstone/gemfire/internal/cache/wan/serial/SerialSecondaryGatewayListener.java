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
package com.gemstone.gemfire.internal.cache.wan.serial;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.internal.cache.RegionQueue;
import com.gemstone.gemfire.internal.cache.wan.AbstractGatewaySender;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventImpl;
/**
 * @author Suranjan Kumar
 * @author Yogesh Mahajan
 * @since 7.0
 *
 */
public class SerialSecondaryGatewayListener extends CacheListenerAdapter
{

 private final SerialGatewaySenderEventProcessor processor;
 
 private final AbstractGatewaySender sender;

 protected SerialSecondaryGatewayListener(SerialGatewaySenderEventProcessor eventProcessor) {
   this.processor = eventProcessor;
   this.sender = eventProcessor.getSender();
 }

 @Override
 public void afterCreate(EntryEvent event)
 {
   if (this.sender.isPrimary()) {
     // The secondary has failed over to become the primary. There is a small
     // window where the secondary has become the primary, but the listener
     // is
     // still set. Ignore any updates to the map at this point. It is unknown
     // what the state of the map is. This may result in duplicate events
     // being sent.
     //GatewayImpl.this._logger.severe(GatewayImpl.this + ": XXXXXXXXX IS
     // PRIMARY BUT PROCESSING AFTER_DESTROY EVENT XXXXXXXXX: " + event);
     return;
   }
   // There is a small window where queue has not been created fully yet. 
   // The underlying region of the queue is created, and it receives afterDestroy callback
   if (this.sender.getQueues() != null && !this.sender.getQueues().isEmpty()) {
//     int size = 0;
//     for(RegionQueue q: this.sender.getQueues()) {
//       size += q.size();
//     }
     this.sender.getStatistics().incQueueSize();
   }
   // fix bug 35730

   // Send event to the event dispatcher
   GatewaySenderEventImpl senderEvent = (GatewaySenderEventImpl)event.getNewValue();
   this.processor.handlePrimaryEvent(senderEvent);
 }

 @Override
 public void afterDestroy(EntryEvent event) {
   if (this.sender.isPrimary()) {
     return;
   }
    // fix bug 37603
    // There is a small window where queue has not been created fully yet. The region is created, and it receives afterDestroy callback.
   
   if (this.sender.getQueues() != null && !this.sender.getQueues().isEmpty()) {
//     int size = 0;
//     for(RegionQueue q: this.sender.getQueues()) {
//       size += q.size();
//     }
     this.sender.getStatistics().decQueueSize();
   }

   // Send event to the event dispatcher
   Object oldValue = event.getOldValue();
   if (oldValue instanceof GatewaySenderEventImpl) {
     GatewaySenderEventImpl senderEvent = (GatewaySenderEventImpl)oldValue;
     if(this.sender.getLogger().fineEnabled()) {
        this.sender.getLogger().fine(
            "Received after Destroy for Secondary event " + senderEvent
                + " the key was " + event.getKey());
     }
     this.processor.handlePrimaryDestroy(senderEvent);
   }
 }
}
