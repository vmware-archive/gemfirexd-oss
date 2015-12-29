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

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author xzhou
 *
 */
public class ShutdownAllResponse extends AdminResponse {
  private transient boolean isToShutDown = true;
  public ShutdownAllResponse() {
  }
  
  @Override
  public boolean getInlineProcess() {
    return true;
  }
  
  @Override
  public boolean sendViaJGroups() {
    return true;
  }
  
  @Override
  public boolean orderedDelivery(boolean threadOwnsResources) {
    return true;
  }

  public ShutdownAllResponse(InternalDistributedMember sender, boolean isToShutDown) {
    this.setRecipient(sender);
    this.isToShutDown = isToShutDown;
  }

  public int getDSFID() {
    return SHUTDOWN_ALL_RESPONSE;
  }
  
  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeBoolean(isToShutDown);
  }

  @Override  
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.isToShutDown = in.readBoolean();
  }

  @Override
  public String toString() {
    return "ShutdownAllResponse from " + this.getSender()
    + " msgId=" + this.getMsgId() + " isToShutDown=" + this.isToShutDown; 
  }

  public boolean isToShutDown() {
    return isToShutDown;
  }
}
