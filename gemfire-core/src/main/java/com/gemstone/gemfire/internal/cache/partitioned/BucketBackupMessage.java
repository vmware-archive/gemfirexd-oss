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
package com.gemstone.gemfire.internal.cache.partitioned;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import java.util.Set;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDataStore;

/**
 * A message sent requesting that an evaluation of buckets be made to determine
 * if one or more needs to be backed-up in order to satisfy the redundantCopies
 * setting
 * 
 * @author mthomas
 * @since 5.0
 */
public final class BucketBackupMessage extends PartitionMessage
  {

  private int bucketId;
  /**
   * Empty contstructor provided for {@link com.gemstone.gemfire.DataSerializer}
   */
  public BucketBackupMessage() {
    super();
  }

  private BucketBackupMessage(Set recipients, int regionId, int bucketId) {
    super(recipients, regionId, null /* no processor */, null /* no TX */);
    this.bucketId = bucketId;
  }

  /**
   * Sends a BucketBackupMessage requesting that another VM backup an existing
   * bucket
   * 
   * @param recipients
   *          the member that the contains keys/value message is sent to
   * @param r
   *          the PartitionedRegion that contains the bucket
   */
  public static void send(Set recipients, PartitionedRegion r, int bucketId)
  {
    Assert.assertTrue(recipients != null,
        "BucketBackupMessage NULL sender list");
    BucketBackupMessage m = new BucketBackupMessage(recipients, r.getPRId(), bucketId);
    r.getDistributionManager().putOutgoing(m);
  }
  
  /**
   * This message may be sent to nodes before the PartitionedRegion is
   * completely initialized due to the RegionAdvisor(s) knowing about the
   * existance of a partitioned region at a very early part of the
   * initialization
   */
  @Override
  protected final boolean failIfRegionMissing() {
    return false;
  }

  @Override
  protected boolean operateOnPartitionedRegion(DistributionManager dm, 
      PartitionedRegion pr, long startTime) throws CacheException {

    // This call has come to an uninitialized region.
    // This can occur as bucket grab Op is done outside the 
    // d-lock.
    if(pr == null || !pr.isInitialized()) {
    	return false;
    }
    
    LogWriterI18n l = pr.getCache().getLoggerI18n();
    if (DistributionManager.VERBOSE) {
      l.fine("BucketBackupMessage operateOnRegion: " + pr.getFullPath());
    }
    PartitionedRegionDataStore ds = pr.getDataStore();
    if (ds != null) {
      pr.getRedundancyProvider().finishIncompleteBucketCreation(bucketId);
    }
    else {
      l.warning(LocalizedStrings.BucketBackupMessage_BUCKETBACKUPMESSAGE_DATA_STORE_NOT_CONFIGURED_FOR_THIS_MEMBER);
    }
    pr.getPrStats().endPartitionMessagesProcessing(startTime); 
    return false;
  }
  
  @Override
  public int getMessageProcessorType()
  {
    return DistributionManager.WAITING_POOL_EXECUTOR;
  }
  public int getDSFID() {
    return PR_BUCKET_BACKUP_MESSAGE;
  }

  @Override
  public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
    super.fromData(in);
    bucketId = in.readInt();
  }

  @Override
  public void toData(DataOutput out)
      throws IOException {
    super.toData(out);
    out.writeInt(bucketId);
  }
}
