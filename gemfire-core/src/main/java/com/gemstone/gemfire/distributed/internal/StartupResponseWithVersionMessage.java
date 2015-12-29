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
package com.gemstone.gemfire.distributed.internal;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.GemFireVersion;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;

/**
 * @author xzhou
 * @since 6.6.2
 */
public class StartupResponseWithVersionMessage extends StartupResponseMessage {
  private String version; // added for bug 43945

  // additional fields using StartupMessageData below here...
  private Collection<String> hostedLocators;
  
  public StartupResponseWithVersionMessage() {
    
  }
  
  StartupResponseWithVersionMessage(DistributionManager dm,
      int processorId,
      InternalDistributedMember recipient,
      String rejectionMessage,
      boolean responderIsAdmin) {
    super(dm, processorId, recipient, rejectionMessage, responderIsAdmin);
    version = GemFireVersion.getGemFireVersion();
    this.hostedLocators = InternalLocator.getLocatorStrings();
  }
  
  @Override
  protected void process(DistributionManager dm) {
    super.process(dm);
    if (this.hostedLocators != null) {
      dm.addHostedLocators(getSender(), this.hostedLocators);
    }
    dm.getLoggerI18n().fine("Received StartupResponseWithVersionMessage from a member with version:"
        +this.version);
  }

  @Override
  public int getDSFID() {
    return STARTUP_RESPONSE_WITHVERSION_MESSAGE;
  }

  @Override
  public String toString() {
    return super.toString() + " version="+this.version;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeString(this.version, out);
    
    StartupMessageData data = new StartupMessageData();
    data.writeHostedLocators(this.hostedLocators);
    data.toData(out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.version = DataSerializer.readString(in);
    
    StartupMessageData data = new StartupMessageData(in, this.version);
    this.hostedLocators = data.readHostedLocators();
  }
}
