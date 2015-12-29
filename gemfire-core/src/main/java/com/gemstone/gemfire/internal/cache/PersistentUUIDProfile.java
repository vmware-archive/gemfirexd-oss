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

package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisor.Profile;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.cache.LocalRegion.UUIDAdvisor;

/**
 * Profile used for exchanging this VM's and max VM ID information with other
 * members in the distributed system for UUID generation using
 * {@link UUIDAdvisor} of this region.
 * 
 * @author swale
 * @since 7.0
 */
public final class PersistentUUIDProfile extends VMIdAdvisor.VMIdProfile
    implements DataSerializableFixedID {

  /** full path of the region */
  private String regionPath;

  /** for deserialization */
  public PersistentUUIDProfile() {
  }

  /** construct a new instance for given member and with given version */
  public PersistentUUIDProfile(final InternalDistributedMember memberId,
      final int version, final String regionPath, final long id,
      final long maxId) {
    super(memberId, version, id, maxId);
    this.regionPath = regionPath;
  }

  @Override
  public void processIncoming(final DistributionManager dm,
      final String adviseePath, final boolean removeProfile,
      final boolean exchangeProfiles, final List<Profile> replyProfiles,
      final LogWriterI18n logger) {
    final GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
    final LocalRegion region;
    final UUIDAdvisor advisor;
    if (cache != null && !cache.isClosed()
        && (region = cache.getRegionByPathForProcessing(regionPath)) != null
        && (advisor = region.uuidAdvisor) != null) {
      // exchange our profile even if not initialized so that the receiver
      // records this VM and continues to send updates to its own profile
      handleDistributionAdvisee(advisor.getAdvisee(), removeProfile,
          exchangeProfiles, replyProfiles);
    }
  }

  @Override
  public boolean getInlineProcess() {
    // don't process inline since now it will put in a region which can
    // deadlock the shared P2P socket
    return false;
  }

  @Override
  public int getDSFID() {
    return PERSISTENT_UUID_PROFILE_MESSAGE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeString(this.regionPath, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.regionPath = DataSerializer.readString(in);
  }

  @Override
  public StringBuilder getToStringHeader() {
    return new StringBuilder("UUIDProfile");
  }

  @Override
  public void fillInToString(StringBuilder sb) {
    super.fillInToString(sb);
    sb.append("; regionPath=").append(this.regionPath);
  }
}
