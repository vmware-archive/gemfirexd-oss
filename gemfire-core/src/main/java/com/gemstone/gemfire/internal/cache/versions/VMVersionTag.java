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
package com.gemstone.gemfire.internal.cache.versions;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.shared.Version;

/**
 * The version tag class for version tags for non-persistent regions. The
 * VersionSource held in these tags is an InternalDistributedMember.
 * 
 * @author dsmith
 * 
 */
public class VMVersionTag extends VersionTag<InternalDistributedMember> {
  
  public VMVersionTag() {
    super();
  }

  @Override
  public void writeMember(InternalDistributedMember member, DataOutput out) throws IOException {
    member.writeEssentialData(out);
    
  }

  @Override
  public InternalDistributedMember readMember(DataInput in) throws IOException, ClassNotFoundException {
    return InternalDistributedMember.readEssentialData(in);
  }

  @Override
  public int getDSFID() {
    return VERSION_TAG;
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }


}
