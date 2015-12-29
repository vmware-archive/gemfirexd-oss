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

//import com.gemstone.gemfire.*;
import com.gemstone.gemfire.admin.GemFireHealthConfig;
//import com.gemstone.gemfire.internal.*;
//import com.gemstone.gemfire.internal.admin.*;
import com.gemstone.gemfire.distributed.internal.*;
import java.io.*;
//import java.util.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * The response to adding a health listener.
 * @since 3.5
 */
public final class AddHealthListenerResponse extends AdminResponse {
  // instance variables
  int listenerId;
  
  /**
   * Returns a <code>AddHealthListenerResponse</code> that will be returned to the
   * specified recipient.
   */
  public static AddHealthListenerResponse create(DistributionManager dm, InternalDistributedMember recipient, GemFireHealthConfig cfg) {
    AddHealthListenerResponse m = new AddHealthListenerResponse();
    m.setRecipient(recipient);
    dm.createHealthMonitor(recipient, cfg);
    m.listenerId = dm.getHealthMonitor(recipient).getId();
    return m;
  }

  // instance methods
  public int getHealthListenerId() {
    return this.listenerId;
  }
  
  public int getDSFID() {
    return ADD_HEALTH_LISTENER_RESPONSE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.listenerId);
  }

  @Override
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.listenerId = in.readInt();
  }

  @Override
  public String toString() {
    return "AddHealthListenerResponse from " + this.getRecipient() + " listenerId=" + this.listenerId;
  }
}
