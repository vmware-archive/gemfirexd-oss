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
package newWan.serial.filters;

import hydra.Log;
import hydra.ProcessMgr;
import hydra.RemoteTestModule;

import java.io.Serializable;

import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.wan.GatewayEventFilter;
import com.gemstone.gemfire.cache.wan.GatewayQueueEvent;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventCallbackArgument;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventImpl;

public class MyEventFilterBeforeTransmit implements GatewayEventFilter {

  private String id = new String("MyEventFilterBeforeEnqueue");

  public void afterAcknowledgement(GatewayQueueEvent event) {
    //logCall("afterAcknowledgement", event);
  }

  public boolean beforeEnqueue(GatewayQueueEvent event) {
    //logCall("beforeEnqueue", event);
    return true;
  }

  public boolean beforeTransmit(GatewayQueueEvent event) {
    //logCall("beforeTransmit", event);

    // filter event
    if (((String)event.getKey()).contains(WanFilterTest.FILTER_KEY_PRIFIX)) {
      Log.getLogWriter().info(
          "Filtering event beforeTransmit for key " + event.getKey()
             // + " in region " + event.getRegion().getFullPath());
              + " in region " + ((GatewaySenderEventImpl)event).getRegionPath());    
      return false;
    }

    return true;
  }

  public void close() {

  }

  /**
   * Log that an event occurred.
   * 
   * @param methodName
   *          The name of the CacheEvent method that was invoked.
   * @param event
   *          The event object that was passed to the event.
   */
  public void logCall(String methodName, AsyncEvent event) {
    StringBuffer aStr = new StringBuffer();
    String clientName = RemoteTestModule.getMyClientName();
    aStr.append("Invoked " + this.getClass().getName() + " for key "
        + event.getKey() + ": " + methodName + " in " + clientName + " event="
        + event + "\n");

    aStr.append("   whereIWasRegistered: " + ProcessMgr.getProcessId() + "\n");
    aStr.append("   key: " + event.getKey() + "\n");

    GatewaySenderEventImpl e = (GatewaySenderEventImpl)event;
    aStr.append("   event.getEventId(): " + e.getEventId() + "\n");
    aStr.append("   event.getValue(): " + e.getValueAsString(true) + "\n");

    Region region = event.getRegion();
    aStr.append("   region: " + event.getRegion().getFullPath() + "\n");

    if (region.getAttributes() instanceof PartitionAttributes) {
      aStr.append("   event.getBucketId(): " + e.getBucketId() + "\n");
      aStr.append("   event.getShadowKey(): " + e.getShadowKey() + "\n");
    }

    aStr.append("   callbackArgument: " + e.getSenderCallbackArgument() + "\n");
    if (e.getSenderCallbackArgument() instanceof GatewaySenderEventCallbackArgument) {
      GatewaySenderEventCallbackArgument callback = (GatewaySenderEventCallbackArgument)e
          .getSenderCallbackArgument();
      aStr.append("   callback.getOriginatingDSId(): "
          + callback.getOriginatingDSId() + "\n");
      aStr.append("   callback.getRecipientDSIds(): "
          + callback.getRecipientDSIds() + "\n");
    }

    Operation op = event.getOperation();
    aStr.append("   operation: " + op.toString() + "\n");
    aStr.append("   Operation.isDistributed(): " + op.isDistributed() + "\n");

    Log.getLogWriter().info(aStr.toString());
  }

  @Override
  public String toString() {
    return id;
  }
}
