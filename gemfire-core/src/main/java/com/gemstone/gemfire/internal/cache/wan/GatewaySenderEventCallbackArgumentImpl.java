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
package com.gemstone.gemfire.internal.cache.wan;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.cache.WrappedCallbackArgument;
import com.gemstone.gemfire.internal.shared.Version;
import com.gemstone.gnu.trove.THashSet;

/**
 * Class <code>GatewayEventCallbackArgumentImpl</code> is a wrapper on a callback
 * arg plus the id of the <code>GatewaySender</code> making the request. It is
 * created during a batch update request so that the hub id is passed to the
 * <code>GatewayReceiver</code> so that events are not re-distributed back to
 * the originating <code>GatewayReceiver</code>, but are distributed to other
 * <code>GatewayReceiver</code>s. The original callback arg is wrapped by this
 * one and replaced in the event sent to CacheListener, CacheWriter and
 * CacheLoader.
 * <p>
 * This class used to be in package <code>com.gemstone.gemfire.util</code>.
 * 
 * @author Suranjan Kumar
 * @since 7.0
 */
public final class GatewaySenderEventCallbackArgumentImpl extends
    WrappedCallbackArgument implements GatewaySenderEventCallbackArgument,
    DataSerializableFixedID {

  /**
   * The id of the originating <code>GatewayReceiver</code> making the request
   */
  private int originatingDSId = GatewaySender.DEFAULT_DISTRIBUTED_SYSTEM_ID;

  /**
   * The set of <code>GatewayReceiver</code> s to which the event has been sent.
   * This set keeps track of the <code>GatewayReceiver</code> s to which the
   * event has been sent so that downstream <code>GatewayReceiver</code> s don't
   * resend the event to the same <code>GatewayReceiver</code>s.
   */
  private final THashSet receipientDSIds;

  /**
   * No arg constructor for DataSerializable.
   */
  public GatewaySenderEventCallbackArgumentImpl() {
    this.receipientDSIds = new THashSet();
  }

  public GatewaySenderEventCallbackArgumentImpl(Object originalCallbackArg) {
    super(originalCallbackArg);
    this.receipientDSIds = new THashSet();

    assert !(originalCallbackArg instanceof GatewaySenderEventCallbackArgument):
        "unexpected double wrapping for " + originalCallbackArg;
  }

  /**
   * Constructor.
   * @param geca
   *          The original callback argument set by the caller or null if there
   *          was not callback arg
   */
  public GatewaySenderEventCallbackArgumentImpl(
      GatewaySenderEventCallbackArgumentImpl geca) {
    super(geca.getOriginalCallbackArg());
    // _originalEventId = geca._originalEventId;
    this.originatingDSId = geca.getOriginatingDSId();
    this.receipientDSIds = new THashSet(geca.getRecipientDSIds());
  }

  /**
   * Constructor.
   * 
   * @param originalCallbackArg
   *          The original callback argument set by the caller or null if there
   *          was not callback arg
   * @param originatingDSId
   *          The id of the originating <code>GatewayReceiver</code> making the
   *          request
   * @param originalReceivers
   *          The collection of <code>Gateway</code> s to which the event has
   *          been originally sent
   * @param serializeCBArg
   *          boolean indicating whether to serialize callback argument
   * 
   */
  public GatewaySenderEventCallbackArgumentImpl(Object originalCallbackArg,
      int originatingDSId, Collection<Integer> originalReceivers,
      boolean serializeCBArg) {
    super(originalCallbackArg, serializeCBArg);
    this.originatingDSId = originatingDSId;
    if (originalReceivers != null) {
      this.receipientDSIds = new THashSet(originalReceivers);
    }
    else {
      this.receipientDSIds = new THashSet();
    }
  }

  /**
   * Returns the id of the originating <code>GatewayReceiver</code> making the
   * request.
   * 
   * @return the id of the originating <code>GatewayReceiver</code> making the
   *         request
   */
  public int getOriginatingDSId() {
    return this.originatingDSId;
  }

  /**
   * Sets the originating <code>SenderId</code> id
   * 
   * @param originatingDSId
   *          The originating <code>SenderId</code> id
   */
  public void setOriginatingDSId(int originatingDSId) {
    this.originatingDSId = originatingDSId;
  }

  /**
   * Returns the list of <code>Gateway</code> s to which the event has been
   * sent.
   * 
   * @return the list of <code>Gateway</code> s to which the event has been sent
   */
  @SuppressWarnings("unchecked")
  public Set<Integer> getRecipientDSIds() {
    return this.receipientDSIds;
  }

  /**
   * Initialize the original set of recipient <code>Gateway</code>s.
   * 
   * @param originalGatewaysReceivers
   *          The original recipient <code>Gateway</code>s.
   */
  public void initializeReceipientDSIds(
      Collection<Integer> originalGatewaysReceivers) {
    this.receipientDSIds.clear();
    if (originalGatewaysReceivers != null) {
      this.receipientDSIds.addAll(originalGatewaysReceivers);
    }
  }

  /**
   * @see GatewaySenderEventCallbackArgument#getClone()
   */
  public GatewaySenderEventCallbackArgumentImpl getClone() {
    return new GatewaySenderEventCallbackArgumentImpl(this);
  }

  public void setPossibleDuplicate(boolean posDup) {
  }

  public int getDSFID() {
    return GATEWAY_SENDER_EVENT_CALLBACK_ARGUMENT;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    out.writeInt(this.originatingDSId);
    if (this.receipientDSIds != null) {
      out.writeInt(this.receipientDSIds.size());
      for (Object gateway : this.receipientDSIds) {
        out.writeInt(((Integer)gateway).intValue());
      }
    }
    else {
      out.writeInt(0);
    }
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.originatingDSId = DataSerializer.readInteger(in);
    this.receipientDSIds.clear();
    int numberOfRecipientGateways = in.readInt();
    for (int i = 0; i < numberOfRecipientGateways; i++) {
      this.receipientDSIds.add(Integer.valueOf(in.readInt()));
    }
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("GatewaySenderEventCallbackArgument [")
        .append("originalCallbackArg=").append(getOriginalCallbackArg())
        .append(";originatingSenderId=").append(this.originatingDSId)
        .append(";recipientGatewayReceivers=").append(this.receipientDSIds)
        .append("]");
    return buffer.toString();
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
