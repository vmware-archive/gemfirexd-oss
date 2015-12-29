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

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import java.util.Set;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDataStore;

/**
 * A message sent to a data store telling that data store to globally
 * destroy the region on behalf of a PR accessor.
 * 
 * @author mthomas
 * @since 5.0
 */
public final class DestroyRegionOnDataStoreMessage extends PartitionMessage
  {

  private Object callbackArg;

  /**
   * Empty contstructor provided for {@link com.gemstone.gemfire.DataSerializer}
   */
  public DestroyRegionOnDataStoreMessage() {
    super();
  }

  private DestroyRegionOnDataStoreMessage(InternalDistributedMember recipient, int regionId, ReplyProcessor21 rp, Object callbackArg) {
    super(recipient, regionId, rp, null /* no TX */);
    this.callbackArg = callbackArg;
  }

  /**
   * Sends a DestroyRegionOnDataStoreMessage requesting that another VM destroy an existing
   * region
   * 
   */
  public static void send(InternalDistributedMember recipient, PartitionedRegion r, Object callbackArg)
  {
    DM dm = r.getDistributionManager();
    ReplyProcessor21 rp = new ReplyProcessor21(dm, recipient);
    int procId = rp.getProcessorId();
    DestroyRegionOnDataStoreMessage m = new DestroyRegionOnDataStoreMessage(recipient, r.getPRId(), rp, callbackArg);
    r.getDistributionManager().putOutgoing(m);
    rp.waitForRepliesUninterruptibly();
  }
  
  @Override
  protected boolean operateOnPartitionedRegion(DistributionManager dm, 
      PartitionedRegion pr, long startTime) throws CacheException {

    // This call has come to an uninitialized region.
    if(pr == null || !pr.isInitialized()) {
    	return true;
    }
    
    LogWriterI18n l = pr.getCache().getLoggerI18n();
    if (DistributionManager.VERBOSE) {
      l.fine("DestroyRegionOnDataStore operateOnRegion: " + pr.getFullPath());
    }
    pr.destroyRegion(callbackArg);
    return true;
  }
  
  @Override
  public int getMessageProcessorType()
  {
    return DistributionManager.WAITING_POOL_EXECUTOR;
  }
  public int getDSFID() {
    return PR_DESTROY_ON_DATA_STORE_MESSAGE;
  }

  @Override
  public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
    super.fromData(in);
    callbackArg= DataSerializer.readObject(in);
  }

  @Override
  public void toData(DataOutput out)
      throws IOException {
    super.toData(out);
    DataSerializer.writeObject(callbackArg, out);
  }
}
