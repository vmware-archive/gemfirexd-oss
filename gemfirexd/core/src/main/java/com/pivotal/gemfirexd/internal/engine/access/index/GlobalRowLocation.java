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

package com.pivotal.gemfirexd.internal.engine.access.index;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.GemFireException;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.offheap.OffHeapHelper;
import com.gemstone.gemfire.internal.offheap.annotations.Released;
import com.gemstone.gemfire.internal.offheap.annotations.Retained;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdPartitionResolver;
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdCallbackArgument;
import com.pivotal.gemfirexd.internal.engine.store.AbstractCompactExecRow;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.offheap.OffHeapByteSource;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.cache.ClassSize;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.RowLocation;
import com.pivotal.gemfirexd.internal.shared.common.ResolverUtils;

public final class GlobalRowLocation extends AbstractRowLocation implements
    Serializable {

  private static final long serialVersionUID = -8962822593640959063L;

  private int entryHash;

  private int routingObject;

  public GlobalRowLocation() {
  }

  public GlobalRowLocation(int hash, int routingObject) {
    this.entryHash = hash;
    this.routingObject = routingObject;
  }

  @Override
  public GlobalRowLocation getClone() {
    return new GlobalRowLocation(this.entryHash, this.routingObject);
  }

  public DataValueDescriptor getNewNull() {
    return new GlobalRowLocation();
  }

  @Override
  public int hashCode() {
    return this.entryHash ^ this.routingObject;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof GlobalRowLocation)) {
      return false;
    }
    final GlobalRowLocation other = (GlobalRowLocation)obj;
    return (this.entryHash == other.entryHash
        && this.routingObject == other.routingObject);
  }

  public static GlobalRowLocation getRowLocation(RowLocation rl,
      ExecRow insertingRow, GemFireContainer baseContainer,
      EntryEventImpl event, boolean isPrimaryKeyConstraint)
      throws StandardException {
    assert rl != null;
    // base table region
    assert baseContainer.isPartitioned();
    final PartitionedRegion region = (PartitionedRegion)baseContainer
        .getRegion();
    final GfxdPartitionResolver partitionResolver = (GfxdPartitionResolver)region
        .getPartitionResolver();

    final GlobalRowLocation retval;
    final Object key = rl.getKeyCopy(); // the key should exist anyway
    final int entryHash = getEntryHash(event, isPrimaryKeyConstraint, key);
    if (partitionResolver != null) {
      Object resolveKey = null;
      final Object callbackArg;
      if (event != null && (callbackArg = event.getCallbackArgument()) != null) {
        if (callbackArg instanceof GfxdCallbackArgument) {
          final GfxdCallbackArgument sca = (GfxdCallbackArgument)callbackArg;
          if (sca.isRoutingObjectSet()) {
            resolveKey = sca.getRoutingObject();
          }
        }
        else {
          resolveKey = callbackArg;
        }
      }
      if (resolveKey == null) {
        @Retained @Released Object value = null;
        try {
        //if (Token.isRemoved(regionEntry._getValue())) {
        if (rl.isDestroyedOrRemoved()) {
          assert insertingRow != null: "The row to be inserted is null?";
          if (insertingRow instanceof AbstractCompactExecRow) {
            value = ((AbstractCompactExecRow)insertingRow).getRawRowValue(false);
          }
          else {
            value = insertingRow.getRowArray();
          }
        }
        else {
          value = rl.getValue(baseContainer);
        }
        /*
         * // !!ezoerner:20090414 this was trunk version
         * EntryOperation entryOp = new EntryOperationImpl(region, null, key,
         *      row, null);
         * Serializable resolveKey = partitionResolver.getRoutingObject(entryOp);
         */
        try {
          resolveKey = partitionResolver.getRoutingObject(key, value, region);
        } catch (GemFireException gfeex) {
          throw Misc.processGemFireException(gfeex, gfeex,
              "lookup of global index for key " + key, true);
        }
        }finally {
          OffHeapHelper.release(value);
        }
      }
      retval = new GlobalRowLocation(entryHash,
          ((Integer)resolveKey).intValue());
    }
    else {
      // entry hash will always be key hashCode in this case
      retval = new GlobalRowLocation(entryHash, entryHash);
    }
    return retval;
  }

  public static int getEntryHash(EntryEventImpl event,
      boolean isPrimaryKeyConstraint, Object regionKey) {
    int entryHash;
    final EventID eventId;
    if (event != null && isPrimaryKeyConstraint
        && (eventId = event.getEventId()) != null) {
      entryHash = 0;
      entryHash = ResolverUtils.addBytesToHash(eventId.getMembershipID(),
          entryHash);
      entryHash = ResolverUtils.addLongToHash(eventId.getThreadID(), entryHash);
      entryHash = ResolverUtils.addLongToHash(eventId.getSequenceID(),
          entryHash);
    }
    else {
      entryHash = regionKey.hashCode();
    }
    return entryHash;
  }

  @Override
  public Object getKey() {
    return null;
  }

  @Override
  public Serializable getRoutingObject() {
    return Integer.valueOf(this.routingObject);
  }

  @Override
  public GlobalRowLocation cloneObject() {
    return new GlobalRowLocation(this.entryHash, this.routingObject);
  }

  @Override
  public String toString() {
    return "GlobalRowLocation: rowHash=" + this.entryHash + " routingObject="
        + this.routingObject;
  }

  // Externalizable methods required to transport RowLocation in exceptions

  /*
   * (non-Javadoc)
   * 
   * @see java.io.Externalizable#readExternal(java.io.ObjectInput)
   */
  @Override
  public void readExternal(ObjectInput in) throws IOException,
      ClassNotFoundException {
    this.entryHash = in.readInt();
    this.routingObject = in.readInt();
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.io.Externalizable#writeExternal(java.io.ObjectOutput)
   */
  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeInt(this.entryHash);
    out.writeInt(this.routingObject);
  }

  // DataSerializableFixedID methods

  @Override
  public int getDSFID() {
    return DataSerializableFixedID.GFXD_GLOBAL_ROWLOC;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writePrimitiveInt(this.entryHash, out);
    out.writeInt(this.routingObject);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.entryHash = DataSerializer.readPrimitiveInt(in);
    this.routingObject = in.readInt();
  }

  @Override
  public void setValue(DataValueDescriptor theValue) {
    assert theValue instanceof GlobalRowLocation;
    final GlobalRowLocation rowLoc = (GlobalRowLocation)theValue;
    this.entryHash = rowLoc.entryHash;
    this.routingObject = rowLoc.routingObject;
  }

  @Override
  public int estimateMemoryUsage() {
    return (2 * Integer.SIZE / Byte.SIZE) + ClassSize.refSize;
  }

  @Override
  public int getLengthInBytes(DataTypeDescriptor dtd) {
    return estimateMemoryUsage();
  }
 
}
