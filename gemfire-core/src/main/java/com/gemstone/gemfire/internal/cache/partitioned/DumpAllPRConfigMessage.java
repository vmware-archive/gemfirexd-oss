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

import com.gemstone.gemfire.i18n.LogWriterI18n;
import java.util.Set;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.TXStateInterface;

/**
 * A message used for debugging purposes.  For example if a test
 * fails it can call {@link com.gemstone.gemfire.internal.cache.PartitionedRegion#sendDumpAllPartitionedRegions()} 
 * which sends this message to all VMs that have that PartitionedRegion defined.
 * 
 * @see com.gemstone.gemfire.internal.cache.PartitionedRegion#sendDumpAllPartitionedRegions()
 * @author Tushar Apshankar
 */
public final class DumpAllPRConfigMessage extends PartitionMessage
  {
  
  public DumpAllPRConfigMessage() {}

  private DumpAllPRConfigMessage(Set recipients, int regionId,
      ReplyProcessor21 processor, final TXStateInterface tx) {
    super(recipients, regionId, processor, tx);
  }

  public static PartitionResponse send(Set recipients, PartitionedRegion r) {
    final TXStateInterface tx = r.getTXState();
    PartitionResponse p = new PartitionResponse(r.getSystem(), recipients, tx);
    DumpAllPRConfigMessage m = new DumpAllPRConfigMessage(recipients,
        r.getPRId(), p, tx);

    /*Set failures = */r.getDistributionManager().putOutgoing(m);
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
      l.fine("DumpAllPRConfigMessage operateOnRegion: "
          + pr.getFullPath());
    }
    pr.dumpSelfEntryFromAllPartitionedRegions();

    if (DistributionManager.VERBOSE) {
      l.fine(getClass().getName() + " dumped allPartitionedRegions");
    }
    return true;
  }

  public int getDSFID() {
    return PR_DUMP_ALL_PR_CONFIG_MESSAGE;
  }
}
