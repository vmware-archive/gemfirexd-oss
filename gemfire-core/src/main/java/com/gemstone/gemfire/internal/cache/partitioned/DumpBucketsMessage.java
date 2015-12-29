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
import java.util.Set;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.PartitionedRegionDataStore;
import com.gemstone.gemfire.internal.cache.TXStateInterface;

/**
 * A message used for debugging purposes.  For example if a test
 * fails it can call {@link PartitionedRegion#dumpAllBuckets(boolean,
 * LogWriterI18n)} which sends this message to all VMs that have that 
 * PartitionedRegion defined.
 * 
 * @see com.gemstone.gemfire.internal.cache.PartitionedRegion#dumpAllBuckets(boolean, LogWriterI18n)
 * @author Mitch Thomas
 */
public final class DumpBucketsMessage extends PartitionMessage
{
  boolean validateOnly;
  boolean bucketsOnly;
  
  public DumpBucketsMessage() {}

  private DumpBucketsMessage(Set recipients, int regionId,
      ReplyProcessor21 processor, boolean validate, boolean buckets,
      final TXStateInterface tx) {
    super(recipients, regionId, processor, tx);
    this.validateOnly = validate;
    this.bucketsOnly = buckets;
  }

  public static PartitionResponse send(Set recipients, PartitionedRegion r, 
      final boolean validateOnly, final boolean onlyBuckets) {
    final TXStateInterface tx = r.getTXState();
    PartitionResponse p = new PartitionResponse(r.getSystem(), recipients, tx);
    DumpBucketsMessage m = new DumpBucketsMessage(recipients, r.getPRId(), p,
        validateOnly, onlyBuckets, tx);

    /*Set failures =*/ r.getDistributionManager().putOutgoing(m);
//    if (failures != null && failures.size() > 0) {
//      throw new PartitionedRegionCommunicationException("Failed sending ", m);
//    }
    return p;
  }

  @Override
  protected boolean operateOnPartitionedRegion(DistributionManager dm, 
      PartitionedRegion pr, long startTime) throws CacheException {
    
    final LogWriterI18n l = pr.getCache().getLoggerI18n();
    if (DistributionManager.VERBOSE) {
      l.fine("DumpBucketsMessage operateOnRegion: "
          + pr.getFullPath());
    }

    PartitionedRegionDataStore ds = pr.getDataStore();
    if (ds != null) {
      if (this.bucketsOnly) {
        ds.dumpBuckets();
      } else {
        ds.dumpEntries(this.validateOnly, null);
      }
      if (DistributionManager.VERBOSE) {
        l.fine(getClass().getName() + " dumped buckets");
      }
    }
    return true;
  }

  public int getDSFID() {
    return PR_DUMP_BUCKETS_MESSAGE;
  }

  @Override
  public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.validateOnly = in.readBoolean();
    this.bucketsOnly = in.readBoolean();
  }

  @Override
  public void toData(DataOutput out)
      throws IOException {
    super.toData(out);
    out.writeBoolean(this.validateOnly);
    out.writeBoolean(this.bucketsOnly);
  }
}
