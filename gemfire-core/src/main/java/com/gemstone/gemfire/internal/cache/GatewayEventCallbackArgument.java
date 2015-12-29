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
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.shared.Version;
import com.gemstone.gnu.trove.THashSet;

/**
 * Class <code>GatewayEventCallbackArgument</code> is a wrapper on a callback
 * arg plus the id of the <code>GatewayHub</code> making the request. It is
 * created during a batch update request so that the hub id is passed to the
 * <code>GatewayHub</code> so that events are not re-distributed back to the
 * originating <code>GatewayHub</code>, but are distributed to other
 * <code>GatewayHub</code>s. The original callback arg is wrapped by this one
 * and replaced in the event sent to CacheListener, CacheWriter and CacheLoader.
 * <p>This class used to be in package <code>com.gemstone.gemfire.util</code>.
 * 
 * @author Barry Oglesby
 * 
 * @since 4.2.1
 */
public class GatewayEventCallbackArgument
  extends WrappedCallbackArgument
  implements DataSerializableFixedID
{
  /**
   * The id of the originating <code>GatewayHub</code> making the request
   */
  private String _originatingGatewayHubId;

  /**
   * The set of <code>Gateway</code> s to which the event has been sent. This
   * set keeps track of the <code>Gateway</code> s to which the event has been
   * sent so that downstream <code>Gateway</code> s don't resend the event to
   * the same <code>Gateway</code>s.
   */
  private THashSet _recipientGateways;

  /**
   * No arg constructor for DataSerializable.
   */
  public GatewayEventCallbackArgument() {
  }

  /**
   * Constructor.
   * 
   * @param originalCallbackArg
   *          The original callback argument set by the caller or null if there
   *          was not callback arg
   */
  public GatewayEventCallbackArgument(Object originalCallbackArg) {
    super(originalCallbackArg);
  }

  /**
   * Constructor.
   * 
   * @param originalCallbackArg
   *          The original callback argument set by the caller or null if there
   *          was not callback arg
   * @param originatingGatewayHubId
   *          The id of the originating <code>GatewayHub</code> making the
   *          request
   * @param originalRecipientGateways
   *          The list of <code>Gateway</code> s to which the event has been
   *          originally sent
   * @param serializeCBArg boolean indicating whether to serialize callback argument 
   *          
   */
  public GatewayEventCallbackArgument(Object originalCallbackArg,
      String originatingGatewayHubId, List originalRecipientGateways, boolean serializeCBArg) {
    super(originalCallbackArg,serializeCBArg);
    this._originatingGatewayHubId = originatingGatewayHubId;
    initializeRecipientGateways(originalRecipientGateways);
  }

  /**
   * Creates a copy of the given GatewayEventCallbackArgument that shares
   * invariant state with the original.
   * 
   * @param geca
   *          the object to be copied
   */
  public GatewayEventCallbackArgument(GatewayEventCallbackArgument geca) {
    super(geca.getOriginalCallbackArg());
    //  _originalEventId = geca._originalEventId;
    _originatingGatewayHubId = geca._originatingGatewayHubId;
    if (geca._recipientGateways != null) {
      _recipientGateways = (THashSet) geca._recipientGateways.clone();
    }
  }

  public void addRecipientGateway(String gatewayId) {
    this._recipientGateways.add(gatewayId);
  }
  
  /**
   * Returns the id of the originating <code>GatewayHub</code> making the
   * request.
   * 
   * @return the id of the originating <code>GatewayHub</code> making the
   *         request
   */
  public String getOriginatingGatewayHubId()
  {
    return this._originatingGatewayHubId;
  }

  /**
   * Sets the originating <code>GatewayHub</code> id
   * 
   * @param originatingGatewayHubId
   *          The originating <code>GatewayHub</code> id
   */
  public void setOriginatingGatewayHubId(String originatingGatewayHubId)
  {
    this._originatingGatewayHubId = originatingGatewayHubId;
  }

  /**
   * Returns the list of <code>Gateway</code> s to which the event has been
   * sent.
   * 
   * @return the list of <code>Gateway</code> s to which the event has been
   *         sent
   */
  public Set getRecipientGateways()
  {
    return this._recipientGateways;
  }


  /**
   * Initialize the original set of recipient <code>Gateway</code>s.
   * 
   * @param originalRecipientGateways
   *          The original recipient <code>Gateway</code>s.
   */
  public void initializeRecipientGateways(List originalRecipientGateways)
  {
    this._recipientGateways = new THashSet(2);
    for (Iterator i = originalRecipientGateways.iterator(); i.hasNext();) {
      this._recipientGateways.add(i.next());
    }
  }

  public int getDSFID() {
    return GATEWAY_EVENT_CALLBACK_ARGUMENT;
  }

  @Override
  public void toData(DataOutput out) throws IOException
  {
    super.toData(out);
    //DataSerializer.writeString(this._originalEventId, out);
    DataSerializer.writeString(this._originatingGatewayHubId, out);
    if (this._recipientGateways != null) {
      out.writeInt(this._recipientGateways.size());
      for (Iterator i = this._recipientGateways.iterator(); i.hasNext();) {
        String gateway = (String)i.next();
        DataSerializer.writeString(gateway, out);
      }
    }
    else {
      out.writeInt(0);
    }
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException
  {
    super.fromData(in);
    //this._originalEventId = DataSerializer.readString(in);
    this._originatingGatewayHubId = DataSerializer.readString(in);
    this._recipientGateways = new THashSet(2);
    int numberOfRecipientGateways = in.readInt();
    for (int i = 0; i < numberOfRecipientGateways; i++) {
      this._recipientGateways.add(DataSerializer.readString(in));
    }
  }

  @Override
  public String toString()
  {
    StringBuffer buffer = new StringBuffer();
    buffer
      .append("GatewayEventCallbackArgument [")
      .append("originalCallbackArg=")
      .append(getOriginalCallbackArg())
      //.append(";originalEventId=")
      //.append(this._originalEventId)
      .append(";originatingGatewayHubId=")
      .append(this._originatingGatewayHubId)
      .append(";recipientGateways=")
      .append(this._recipientGateways)
      .append("]");
    return buffer.toString();
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
