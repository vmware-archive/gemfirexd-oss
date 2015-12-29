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

import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
//import com.gemstone.gemfire.*;
//import com.gemstone.gemfire.internal.*;
//import com.gemstone.gemfire.internal.admin.*;
import java.io.*;
//import java.util.*;

/**
 * A message that is sent to a particular distribution manager to
 * remove a health listener.
 * @since 3.5
 */
public final class RemoveHealthListenerRequest extends AdminRequest {
  // instance variables
  private int id;

  /**
   * Returns a <code>RemoveHealthListenerRequest</code> to be sent to the specified recipient.
   */
  public static RemoveHealthListenerRequest create(int id) {
    RemoveHealthListenerRequest m = new RemoveHealthListenerRequest();
    m.id = id;
    return m;
  }

  public RemoveHealthListenerRequest() {
    friendlyName = LocalizedStrings.RemoveHealthListenerRequest_REMOVE_HEALTH_LISTENER.toLocalizedString();
  }

  /**
   * Must return a proper response to this request.
   */
  @Override  
  protected AdminResponse createResponse(DistributionManager dm) {
    return RemoveHealthListenerResponse.create(dm, this.getSender(), this.id);
  }

  public int getDSFID() {
    return REMOVE_HEALTH_LISTENER_REQUEST;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.id);
  }

  @Override
  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.id = in.readInt();
  }

  @Override
  public String toString() {
    return "RemoveHealthListenerRequest from " + this.getRecipient() + " id=" + this.id;
  }
}
