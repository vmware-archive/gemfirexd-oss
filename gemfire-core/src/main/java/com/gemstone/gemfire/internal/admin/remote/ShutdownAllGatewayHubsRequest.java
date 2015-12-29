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
package com.gemstone.gemfire.internal.admin.remote;

import java.util.Collection;

import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.util.ArrayUtils;

/**
 * An instruction to all members with cache that their PR should gracefully
 * close and disconnect DS
 * @author xzhou
 *
 */
public class ShutdownAllGatewayHubsRequest extends AdminRequest {
  public ShutdownAllGatewayHubsRequest() {
  }
  
  @Override
  public boolean sendViaJGroups() {
    return true;
  }
  
  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    if(cache != null && !cache.isClosed()) {
      cache.stopGatewayHubs(true);
      //TODO: Pushkar: later this needs to be moved into a separate request
      //e.g. ShutdownAllGatewaySendersAndReceivers which will be called from ShutdownAllRequest
      cache.stopGatewaySenders(true);
      cache.stopGatewayReceivers(true);
    }
  
    return new ShutdownAllGatewayHubsResponse(this.getSender());
  }

  public int getDSFID() {
    return SHUTDOWN_ALL_GATEWAYHUBS_REQUEST;
  }
  
  @Override  
  public String toString() {
    return "ShutdownAllGatewayHubsRequest sent to " + ArrayUtils.toString((Object[])this.getRecipients()) +
      " from " + this.getSender();
  }

  public static class ShutDownAllGatewayHubsReplyProcessor extends AdminMultipleReplyProcessor {
    public ShutDownAllGatewayHubsReplyProcessor(DM dm, Collection initMembers) {
      super(dm, initMembers);
    }

    @Override
    protected boolean stopBecauseOfExceptions() {
      return false;
    }

  }
}
