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
package com.gemstone.gemfire.cache.client.internal.locator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.shared.Version;

/**
 * @author dsmith
 *
 */
public abstract class ServerLocationRequest implements DataSerializableFixedID {

 private String serverGroup;
  
  public ServerLocationRequest(String serverGroup) {
    super();
    this.serverGroup = serverGroup;
  }
  
  /** Used by DataSerializer */
  public ServerLocationRequest() {
    
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    serverGroup = DataSerializer.readString(in);
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(serverGroup, out);
  }

  public String getServerGroup() {
    return serverGroup;
  }
  
  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}