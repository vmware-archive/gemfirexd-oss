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
import java.io.*;

/**
 * A message that is sent to a particular distribution manager to get its
 * current {@link com.gemstone.gemfire.admin.GemFireMemberStatus}
 * 
 * @author Harsh Khanna
 */
public class RefreshMemberSnapshotRequest extends AdminRequest {
  /**
   * Returns a <code>RefreshMemberSnapshotRequest</code> to be sent to the
   * specified recipient.
   */
  public static RefreshMemberSnapshotRequest create() {
    RefreshMemberSnapshotRequest m = new RefreshMemberSnapshotRequest();
    return m;
  }

  public RefreshMemberSnapshotRequest() {
    friendlyName = "Refresh the Member's Status";
  }

  /**
   * Must return a proper response to this request.
   */
  @Override
  protected AdminResponse createResponse(DistributionManager dm) {
    return RefreshMemberSnapshotResponse.create(dm, this.getSender());
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
  }

  /**
   * Returns the DataSerializer fixed id for the class that implements this method.
   */
  public int getDSFID() {    
    return REFRESH_MEMBER_SNAP_REQUEST;
  }


  @Override
  public String toString() {
    return "RefreshMemberSnapshotRequest sent to " + this.getRecipient()
        + " from " + this.getSender();
  }
}
