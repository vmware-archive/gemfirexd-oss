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

package com.pivotal.gemfirexd.internal.engine.distributed;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.KeyInfo;
import com.gemstone.gemfire.internal.cache.lru.Sizeable;
import com.gemstone.gemfire.internal.cache.wan.CloneableCallbackArgument;
import com.pivotal.gemfirexd.internal.engine.GfxdDataSerializable;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;

/**
 * @author Asif, swale
 */
public class GfxdCallbackArgument extends GfxdDataSerializable implements
    Sizeable {

  final static byte RT_SET = 0x01;

  private final static byte CLEAR_RT_SET = ~RT_SET;

  final static byte CONN_SET = 0x02;

  private final static byte CLEAR_CONN_SET = ~CONN_SET;

  final static short IS_CACHE_LOADED = 0x04;

  final static short IS_TRANSACTIONAL = 0x08;

  final static short IS_PKBASED = 0x10;

  final static short SKIP_LISTENER = 0x20;

  final static short IS_TSS_INSTANCE = 0x40;

  final static short BULK_FK_CHECKS_ENABLED = 0x80;

  final static short SKIP_CONSTRAINT_CHECKS = 0x100;

  private final static byte CLEAR_TSS_INSTANCE = ~IS_TSS_INSTANCE;

  private static final GfxdCallbackArgument fixedInstance =
      new GfxdCallbackArgument();

  private static final NoPkBased fixedInstanceNoPkBased = new NoPkBased();

  private static final CacheLoaded fixedInstanceCL = new CacheLoaded();

  private static final CacheLoadedSkipListeners fixedInstanceCLSL =
      new CacheLoadedSkipListeners();

  private static final Transactional fixedInstanceTX = new Transactional();

  private static final TransactionalNoPkBased fixedInstanceTXNoPkBased =
      new TransactionalNoPkBased();

  private static final ThreadLocal<WithInfoFieldsType> tssInstance =
      new ThreadLocal<WithInfoFieldsType>() {
    @Override
    protected WithInfoFieldsType initialValue() {
      final WithInfoFieldsType inst = new WithInfoFieldsType();
      inst.setFields(KeyInfo.UNKNOWN_BUCKET, EmbedConnection.UNINITIALIZED,
          IS_TSS_INSTANCE);
      return inst;
    }
  };

  private GfxdCallbackArgument() {
  }

  public Integer getRoutingObject() {
    return null;
  }

  public boolean isRoutingObjectSet() {
    return false;
  }

  public void setRoutingObject(Integer routingObject) {
    throw new IllegalStateException(
        "GfxdCallbackArgument::setRoutingObject: not expected to get invoked");
  }

  public void setConnectionProperties(long connID) {
    throw new IllegalStateException(
        "GfxdCallbackArgument::setConnectionID: not expected to get invoked");
  }

  public void setSkipListeners() {
    throw new IllegalStateException(
        "GfxdCallbackArgument::setSkipListeners: not expected to get invoked");
  }

  public void setBulkFkChecksEnabled() {
    throw new IllegalStateException("GfxdCallbackArgument::"
        + "setBulkFkChecksEnabled: not expected to get invoked");
  }

  public void setSkipConstraintChecks() {
    throw new IllegalStateException("GfxdCallbackArgument::"
        + "setSkipConstraintChecks: not expected to get invoked");
  }

  public long getConnectionID() {
    return EmbedConnection.UNINITIALIZED;
  }

  public boolean isConnectionIDSet() {
    return false;
  }

  public boolean isFixedInstance() {
    return true;
  }

  public boolean isThreadLocalInstance() {
    return false;
  }

  @Override
  public byte getGfxdID() {
    return GFXD_CALLBACK_ARGUMENT;
  }

  @Override
  public boolean equals(final Object o) {
    return this == o;
  }

  @Override
  public String toString() {
    return "GfxdCallbackArgument: isPkBased=true";
  }

  public void clear() {
  }

  public boolean isCacheLoaded() {
    return false;
  }

  public boolean isTransactional() {
    return false;
  }

  public boolean isPkBased() {
    return true;
  }

  public boolean isSkipListeners() {
    return false;
  }

  public boolean isBulkFkChecksEnabled() {
    return false;
  }

  public boolean isSkipConstraintChecks() {
    return false;
  }

  public void setCacheLoaded() {
    throw new IllegalStateException(
        "GfxdCallbackArgument::setCacheLoaded: not expected to get invoked");
  }

  public void setTransactional() {
    throw new IllegalStateException(
        "GfxdCallbackArgument::setTransactional: not expected to get invoked");
  }

  public void setPkBased() {
    // this one is always PK based
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getSizeInBytes() {
    // just 2 bytes in serialized form, DSFID and GfxdID
    return 2;
  }

  public GfxdCallbackArgument cloneObject() {
    return this;
  }

  public static GfxdCallbackArgument getFixedInstance() {
    return fixedInstance;
  }

  public static GfxdCallbackArgument getFixedInstanceNoPkBased() {
    return fixedInstanceNoPkBased;
  }

  public static GfxdCallbackArgument getThreadLocalInstance() {
    final WithInfoFieldsType callbackArg = tssInstance.get();
    callbackArg.setFields(KeyInfo.UNKNOWN_BUCKET,
        EmbedConnection.UNINITIALIZED, IS_TSS_INSTANCE);
    return callbackArg;
  }

  public static GfxdCallbackArgument getFixedInstanceCacheLoaded() {
    return fixedInstanceCL;
  }

  public static GfxdCallbackArgument getFixedInstanceCacheLoadedSkipListeners() {
    return fixedInstanceCLSL;
  }

  public static GfxdCallbackArgument getFixedInstanceTransactional() {
    return fixedInstanceTX;
  }

  public static GfxdCallbackArgument getFixedInstanceTransactionalNoPkBased() {
    return fixedInstanceTXNoPkBased;
  }

  public static GfxdCallbackArgument getWithInfoFieldsType() {
    return new WithInfoFieldsType();
  }

  public static final class NoPkBased extends GfxdCallbackArgument {

    private NoPkBased() {
    }

    @Override
    public boolean isPkBased() {
      return false;
    }

    @Override
    public void setPkBased() {
      throw new IllegalStateException(
          "GfxdCallbackArgument.NoPkBased::setPkBased: unexpected invocation");
    }

    @Override
    public byte getGfxdID() {
      return GFXD_CALLBACK_ARGUMENT_NO_PK_BASED;
    }

    @Override
    public String toString() {
      return "GfxdCallbackArgument: isPkBased=false";
    }
  }

  public static final class CacheLoaded extends GfxdCallbackArgument {

    private CacheLoaded() {
    }

    @Override
    public boolean isCacheLoaded() {
      return true;
    }

    @Override
    public boolean isSkipListeners() {
      return false;
    }

    @Override
    public byte getGfxdID() {
      return GFXD_CALLBACK_ARGUMENT_CACHE_LOADED;
    }

    @Override
    public String toString() {
      return "CacheLoaded: isSkipListeners=false isPkBased=true";
    }
  }

  public static final class CacheLoadedSkipListeners extends
      GfxdCallbackArgument {

    private CacheLoadedSkipListeners() {
    }

    @Override
    public boolean isCacheLoaded() {
      return true;
    }

    @Override
    public boolean isSkipListeners() {
      return true;
    }

    @Override
    public byte getGfxdID() {
      return GFXD_CALLBACK_ARGUMENT_CL_SKIP_LISTENERS;
    }

    @Override
    public String toString() {
      return "CacheLoadedSkipListeners: isSkipListeners=true isPkBased=true";
    }
  }

  public static final class Transactional extends GfxdCallbackArgument {

    private Transactional() {
    }

    @Override
    public boolean isTransactional() {
      return true;
    }

    @Override
    public boolean isPkBased() {
      return true;
    }

    @Override
    public byte getGfxdID() {
      return GFXD_CALLBACK_ARGUMENT_TRANSACTIONAL;
    }

    @Override
    public String toString() {
      return "GFXD Transactional: isTransactional=true isPkBased=true";
    }
  }

  public static final class TransactionalNoPkBased extends GfxdCallbackArgument {

    private TransactionalNoPkBased() {
    }

    @Override
    public boolean isTransactional() {
      return true;
    }

    @Override
    public boolean isPkBased() {
      return false;
    }

    @Override
    public byte getGfxdID() {
      return GFXD_CALLBACK_ARGUMENT_TRANSACTIONAL_NO_PK_BASED;
    }

    @Override
    public String toString() {
      return "GFXD Transactional: isTransactional=true isPkBased=false";
    }
  }

  public static final class WithInfoFieldsType extends GfxdCallbackArgument
      implements CloneableCallbackArgument {

    private int routingObject = KeyInfo.UNKNOWN_BUCKET;
    private short flags;
    private long connId = EmbedConnection.UNINITIALIZED;

    private final void setFields(final int routingObject, final long connId,
        final short flags) {
      this.routingObject = routingObject;
      this.connId = connId;
      this.flags = flags;
    }

    @Override
    public final Integer getRoutingObject() {
      return isRoutingObjectSet() ? Integer.valueOf(this.routingObject) : null;
    }

    @Override
    public final boolean isRoutingObjectSet() {
      return (this.flags & RT_SET) != 0;
    }

    @Override
    public final void setRoutingObject(final Integer routingObject) {
      if (routingObject != null) {
        this.routingObject = routingObject.intValue();
        this.flags |= RT_SET;
      }
      else {
        this.routingObject = KeyInfo.UNKNOWN_BUCKET;
        this.flags &= CLEAR_RT_SET;
      }
    }

    @Override
    public void setSkipListeners() {
      this.flags |= SKIP_LISTENER;
    }

    @Override
    public void setBulkFkChecksEnabled() {
      this.flags |= BULK_FK_CHECKS_ENABLED;
    }

    @Override
    public void setSkipConstraintChecks() {
      this.flags |= SKIP_CONSTRAINT_CHECKS;
    }

    @Override
    public final void setConnectionProperties(final long connId) {
      this.connId = connId;
    }

    @Override
    public final long getConnectionID() {
      return this.connId;
    }

    @Override
    public final boolean isConnectionIDSet() {
      return this.connId != EmbedConnection.UNINITIALIZED;
    }

    @Override
    public boolean isFixedInstance() {
      return false;
    }

    @Override
    public boolean isThreadLocalInstance() {
      return (this.flags & IS_TSS_INSTANCE) != 0;
    }

    @Override
    public boolean isCacheLoaded() {
      return (this.flags & IS_CACHE_LOADED) != 0;
    }

    @Override
    public final boolean isTransactional() {
      return (this.flags & IS_TRANSACTIONAL) != 0;
    }

    @Override
    public final boolean isPkBased() {
      return (this.flags & IS_PKBASED) != 0;
    }
    
    @Override
    public boolean isSkipListeners() {
      return (this.flags & SKIP_LISTENER) != 0;
    }

    @Override
    public boolean isBulkFkChecksEnabled() {
      return (this.flags & BULK_FK_CHECKS_ENABLED) != 0;
    }

    @Override
    public boolean isSkipConstraintChecks() {
      return (this.flags & SKIP_CONSTRAINT_CHECKS) != 0;
    }

    @Override
    public void setCacheLoaded() {
      this.flags |= IS_CACHE_LOADED;
    }

    @Override
    public void setTransactional() {
      this.flags |= IS_TRANSACTIONAL;
    }

    @Override
    public void setPkBased() {
      this.flags |= IS_PKBASED;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getSizeInBytes() {
      // DSFID + GfxdID + flags + avg routing Object + avg connection ID
      return 1 + 1 + 2 + 2 + 4;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public WithInfoFieldsType cloneObject() {
      final WithInfoFieldsType clone = new WithInfoFieldsType();
      clone.setFields(this.routingObject, this.connId,
          (short)(this.flags & CLEAR_TSS_INSTANCE));
      return clone;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o instanceof WithInfoFieldsType) {
        final WithInfoFieldsType other = (WithInfoFieldsType)o;
        return this.routingObject == other.routingObject
            && this.connId == other.connId
            && (this.flags & CLEAR_TSS_INSTANCE) ==
                 (other.flags & CLEAR_TSS_INSTANCE);
      }
      return false;
    }

    @Override
    public int hashCode() {
      final int h = (int)(this.connId ^ (this.connId >>> 32));
      return (h ^ this.routingObject ^ (this.flags & CLEAR_TSS_INSTANCE));
    }

    @Override
    public byte getGfxdID() {
      return GFXD_CALLBACK_ARGUMENT_W_INFO;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      final long connId = this.connId;

      int flag = (this.flags & CLEAR_TSS_INSTANCE);
      if (connId != EmbedConnection.UNINITIALIZED) {
        flag |= CONN_SET;
      }
//      out.writeByte(flag);
      out.writeShort(flag);
      // write routing object if present & then connection ID, if present
      if (isRoutingObjectSet()) {
        InternalDataSerializer.writeSignedVL(this.routingObject, out);
      }
      if (connId != EmbedConnection.UNINITIALIZED) {
        GemFireXDUtils.writeCompressedHighLow(out, connId);
      }
    }

    @Override
    public void fromData(DataInput in) throws IOException,
            ClassNotFoundException {
      super.fromData(in);
//      byte flag = in.readByte();
      short flag = in.readShort();
      // System.out.println("Asif: Read the data. flag="+flag);
      // check if routing object set
      if ((flag & RT_SET) != 0) {
        this.routingObject = (int)InternalDataSerializer.readSignedVL(in);
      }
      // check if connectionID set
      if ((flag & CONN_SET) != 0) {
        this.connId = GemFireXDUtils.readCompressedHighLow(in);
        flag &= CLEAR_CONN_SET;
      }
      this.flags = flag;
    }

    @Override
    public String toString() {
      final StringBuilder sbuff = new StringBuilder();
      sbuff.append("GfxdCallbackArgument.");
      if (isThreadLocalInstance()) {
        sbuff.append("TSSInstance@");
      }
      else {
        sbuff.append("WithInfoFieldsType@");
      }
      sbuff.append(Integer.toHexString(System.identityHashCode(this)));
      sbuff.append("(isRoutingObjectSet=").append(this.isRoutingObjectSet());
      sbuff.append(";routingObject=").append(this.getRoutingObject());
      sbuff.append(";isConnectionIDSet=").append(this.isConnectionIDSet());
      sbuff.append(";connectionID=").append(this.getConnectionID());
      sbuff.append(";isCacheLoaded=").append(this.isCacheLoaded());
      sbuff.append(";isTransactional=").append(this.isTransactional());
      sbuff.append(";isPkBased=").append(this.isPkBased());
      sbuff.append(";isSkipListeners=").append(this.isSkipListeners());
      sbuff.append(";isBulkFkChecksEnabled=").append(isBulkFkChecksEnabled());
      sbuff.append(";skipConstraintChecks=").append(isSkipConstraintChecks());
      return sbuff.append(')').toString();
    }

    /**
     * @see CloneableCallbackArgument#getClone()
     */
    @Override
    public CloneableCallbackArgument getClone() {
      if (isThreadLocalInstance()) {
        return this.cloneObject();
      }
      return this;
    }
  }
}
