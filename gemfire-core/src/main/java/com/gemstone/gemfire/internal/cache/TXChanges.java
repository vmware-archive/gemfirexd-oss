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

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gnu.trove.THashSet;
import com.gemstone.gnu.trove.TObjectProcedure;

/**
 * This class encapsulates the changes in the TX recorded on a remote node that
 * need to be transmitted all the way back to the coordinator.
 * 
 * @author swale
 * @since 7.0
 */
public final class TXChanges {

  // flags used during serialization
  private static final byte IS_DIRTY = 0x1;
  private static final byte NEW_REGIONS = 0x2;
  private static final byte INCONSISTENT_THR = 0x4;
  private static final byte HAS_READ_OPS = 0x8;
  private static final byte HAS_PENDING_OPS = 0x10;

  /** the {@link TXId} for the transaction */
  private final TXId txId;

  /**
   * true if the transaction on remote node has been marked dirty i.e. having
   * some writes
   */
  private final boolean isDirty;

  /**
   * true if the transaction on remote node has been marked as having read
   * operations (for REPEATABLE_READ)
   */
  private final boolean hasReadOps;

  /** the set of new regions touched in the transaction */
  private final THashSet newRegions;

  /** if the transaction has failed, then stores the reason of the failure */
  private Throwable inconsistentThr;

  /** pending batch ops meant for the source itself */
  private TXBatchMessage pendingOps;

  private TXChanges(TXId txId, boolean txIsDirty, boolean txHasReadOps,
      THashSet newRegions, Throwable txInconsistentThr,
      TXBatchMessage pendingOps) {
    this.txId = txId;
    this.isDirty = txIsDirty;
    this.hasReadOps = txHasReadOps;
    this.newRegions = newRegions;
    this.inconsistentThr = txInconsistentThr;
    this.pendingOps = pendingOps;
  }

  public static TXChanges fromMessage(final AbstractOperationMessage msg,
      final TXStateProxy txProxy) {
    if (!txProxy.isCoordinator() && msg.canStartRemoteTransaction()) {
      final boolean txIsDirty = txProxy.isDirty();
      final boolean txHasReadOps = txProxy.hasReadOps();
      if (TXStateProxy.LOG_FINE) {
        txProxy.getTxMgr().getLogger().info(LocalizedStrings.DEBUG,
            "TXChanges: isDirty=" + txIsDirty + " numTXRegions="
            + txProxy.regions.size() + " checkpoint=" + msg.txRegionCheckpoint
            + " for " + txProxy.getTransactionId().shortToString());
      }
      THashSet newRegions = null;
      Throwable txInconsistentThr = null;
      boolean hasDirtyOrReadChanges = (txIsDirty || txHasReadOps);
      final Checkpoint cp = msg.getTXRegionCheckpoint();
      if (cp != null) {
        cp.attemptLock(-1);
        txInconsistentThr = txProxy.getTXInconsistent();
        // elementAt with -1 indicates getting txFlags from AbstractOperationMsg
        hasDirtyOrReadChanges = (cp.elementAt(-1) != null);

        final int numChanged;
        if ((numChanged = cp.numChanged()) > 0) {
          try {
            newRegions = new THashSet(numChanged);
            for (int index = 0; index < numChanged; index++) {
              newRegions.add(new RegionInfoShip(cp.elementAt(index)));
            }
          } finally {
            cp.releaseLock();
          }
        }
        else {
          cp.releaseLock();
        }
      }
      if (hasDirtyOrReadChanges || newRegions != null
          || txInconsistentThr != null) {
        return new TXChanges(txProxy.getTransactionId(), txIsDirty,
            txHasReadOps, newRegions, txInconsistentThr, null);
      }
    }
    return null;
  }

  public final TXId getTXId() {
    return this.txId;
  }

  public final boolean isDirty() {
    return this.isDirty;
  }

  public final THashSet getNewRegions() {
    return this.newRegions;
  }

  public final Throwable getTXException() {
    return this.inconsistentThr;
  }

  public final void setTXException(final Throwable t) {
    this.inconsistentThr = t;
  }

  public final void applyLocally(final DistributedMember sender) {
    final GemFireCacheImpl cache = GemFireCacheImpl.getExisting();
    final TXStateProxy txProxy;
    if (!cache.getMyId().equals(sender)
        && (txProxy = cache.getCacheTransactionManager().getHostedTXState(
            this.txId)) != null) {
      if (this.isDirty) {
        txProxy.markDirty();
      }
      if (this.hasReadOps) {
        txProxy.markHasReadOps();
      }
      final Throwable thr = this.inconsistentThr;
      if (thr != null) {
        txProxy.markTXInconsistent(thr);
      }
      if (this.newRegions != null) {
        this.newRegions.forEach(new TObjectProcedure() {
          public final boolean execute(final Object reg) {
            final RegionInfoShip regionInfo = (RegionInfoShip)reg;
            final Object region = regionInfo.lookupRegionObject(cache);
            if (region != null) {
              txProxy.addAffectedRegion(region, false);
              return true;
            }
            else {
              txProxy.markTXInconsistent(new UnsupportedOperationException(
                  regionInfo.toString() + " referenced on node " + sender
                      + " does not exist on this node"));
              return false;
            }
          }
        });
      }
      // process the batch message last
      // TODO: TX: need to ignore exceptions in processing here
      final TXBatchMessage pendingOps;
      if (thr == null && (pendingOps = this.pendingOps) != null) {
        pendingOps.apply(txProxy);
      }
    }
  }

  /**
   * @see DataSerializable#toData(DataOutput)
   */
  public final void toData(DataOutput out) throws IOException {
    int status = 0;
    if (this.isDirty) {
      status |= IS_DIRTY;
    }
    if (this.hasReadOps) {
      status |= HAS_READ_OPS;
    }
    if (this.newRegions != null) {
      status |= NEW_REGIONS;
    }
    final Throwable thr = this.inconsistentThr;
    final TXBatchMessage pendingOps = this.pendingOps;
    if (thr != null) {
      status |= INCONSISTENT_THR;
    }
    else if (pendingOps != null) {
      status |= HAS_PENDING_OPS;
    }
    out.writeByte(status);
    this.txId.toData(out);
    if (this.newRegions != null) {
      InternalDataSerializer.writeArrayLength(this.newRegions.size(), out);
      for (Object regionInfo : this.newRegions) {
        InternalDataSerializer.invokeToData((RegionInfoShip)regionInfo, out);
      }
    }
    if (thr != null) {
      DataSerializer.writeObject(thr, out);
    }
    else if (pendingOps != null) {
      InternalDataSerializer.invokeToData(pendingOps, out);
    }
  }

  /**
   * @see DataSerializable#fromData(DataInput)
   */
  public static TXChanges fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    final byte status = in.readByte();
    final TXId txId = TXId.createFromData(in);
    THashSet newRegions = null;
    Throwable inconsistentThr = null;
    TXBatchMessage pendingOps = null;
    if ((status & NEW_REGIONS) != 0) {
      int numChanged = InternalDataSerializer.readArrayLength(in);
      newRegions = new THashSet(numChanged);
      for (int i = 1; i <= numChanged; i++) {
        RegionInfoShip regionInfo = new RegionInfoShip();
        InternalDataSerializer.invokeFromData(regionInfo, in);
        newRegions.add(regionInfo);
      }
    }
    if ((status & INCONSISTENT_THR) != 0) {
      inconsistentThr = DataSerializer.readObject(in);
    }
    else if ((status & HAS_PENDING_OPS) != 0) {
      pendingOps = new TXBatchMessage();
      InternalDataSerializer.invokeFromData(pendingOps, in);
    }
    return new TXChanges(txId, (status & IS_DIRTY) != 0,
        (status & HAS_READ_OPS) != 0, newRegions, inconsistentThr, pendingOps);
  }

  public final void toString(final StringBuilder sb) {
    sb.append(this.txId.shortToString());
    if (this.isDirty) {
      sb.append("{DIRTY}");
    }
    if (this.hasReadOps) {
      sb.append("{HAS_READ_OPS}");
    }
    if (this.newRegions != null) {
      sb.append(" new TX regions=");
      this.newRegions.forEach(new TObjectProcedure() {

        private boolean notFirst;

        public final boolean execute(final Object reg) {
          if (this.notFirst) {
            sb.append(',');
          }
          else {
            this.notFirst = true;
          }
          sb.append(reg);
          return true;
        }
      });
    }
    final Throwable thr = this.inconsistentThr;
    final TXBatchMessage pendingOps;
    if (thr != null) {
      sb.append(" TX exception=").append(thr.toString());
    }
    else if ((pendingOps = this.pendingOps) != null) {
      sb.append(" pendingOps=").append(pendingOps.toString());
    }
  }

  @Override
  public final String toString() {
    final StringBuilder sb = new StringBuilder("TXChanges(");
    toString(sb);
    return sb.append(')').toString();
  }
}
