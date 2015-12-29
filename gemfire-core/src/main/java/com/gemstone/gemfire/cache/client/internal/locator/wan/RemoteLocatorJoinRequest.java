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
package com.gemstone.gemfire.cache.client.internal.locator.wan;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.client.internal.locator.ServerLocationRequest;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.admin.remote.DistributionLocatorId;
import com.gemstone.gemfire.internal.shared.Version;

/**
 * Requests remote locators of a remote WAN site
 * 
 * @author Suranjan Kumar
 * @author Yogesh Mahajan
 * @author Kishor Bachhav
 * 
 * @since 6.6
 * 
 */
public class RemoteLocatorJoinRequest implements DataSerializableFixedID {

  private DistributionLocatorId locator = null;
 
  private int distributedSystemId = -1;

  public RemoteLocatorJoinRequest() {
    super();
  }

  public RemoteLocatorJoinRequest(int distributedSystemId, DistributionLocatorId locator,
      String serverGroup) {
    this.distributedSystemId = distributedSystemId;
    this.locator = locator;
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.locator = DataSerializer.readObject(in);
    this.distributedSystemId = in.readInt();
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(locator, out);
    out.writeInt(this.distributedSystemId);
  }

  public DistributionLocatorId getLocator() {
    return this.locator;
  }
  
  public int getDistributedSystemId() {
    return distributedSystemId;
  }
  
  public int getDSFID() {
    return DataSerializableFixedID.REMOTE_LOCATOR_JOIN_REQUEST;
  }

  @Override
  public String toString() {
    return "RemoteLocatorJoinRequest{locator=" + locator + "}";
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }

}
