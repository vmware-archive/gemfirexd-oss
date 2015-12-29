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
import java.util.Collection;
import java.util.Set;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.shared.Version;

/**
 * This message will be sent when 1. Explicit rollback to savepoint is desired
 * by the user 2. When drda client fails-over to a new server then the new
 * server sends this to all the members so that they come back to a good state
 * till the savepoint mentioned
 * 
 * @author kneeraj
 * 
 */
public class TXRollBackToSavepointMsg extends TXMessage {

  private long saveptToRollbackTo;
  
  private TXRollBackToSavepointMsg(final TXStateInterface tx,
      final ReplyProcessor21 processor, int savept) {
    this.saveptToRollbackTo = savept;
  }
  
  public static void send(final InternalDistributedSystem system, final DM dm,
      final TXStateProxy tx, int savePt) {
    Set<DistributedMember> recipients = GemFireCacheImpl.getInternalProductCallbacks().getDataStores();
    TXRollBackToSavepointMsg msg = new TXRollBackToSavepointMsg(tx, null, savePt);
  }

  /**
   * @see com.gemstone.gemfire.internal.DataSerializableFixedID#getDSFID()
   */
  @Override
  public int getDSFID() {
    return TX_ROLLBACK_TO_SAVEPOINT_MESSAGE;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected boolean operateOnTX(TXStateProxy txProxy, DistributionManager dm) {
    // TODO Auto-generated method stub
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected short computeCompressedShort(short flags) {
    return flags;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void appendFields(StringBuilder sb) {
    // TODO Auto-generated method stub

  }

  @Override
  public void toData(DataOutput out)
          throws IOException {
    super.toData(out);

    Version version = InternalDataSerializer.getVersionForDataStream(out);
    if (Version.GFXD_20.compareTo(version) >= 0) {
      InternalDataSerializer.writeUnsignedVL(this.saveptToRollbackTo, out);
    }
    else {
      // KN: TODO throw exception
    }
  }

  @Override
  public void fromData(DataInput in)
          throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.saveptToRollbackTo = InternalDataSerializer.readUnsignedVL(in);
  }
  public final static class RollbackToSavepointResponse extends ReplyProcessor21 {

    public RollbackToSavepointResponse(DM dm, Collection initMembers) {
      super(dm, initMembers);
      // TODO Auto-generated constructor stub
    }
    
  }
}
