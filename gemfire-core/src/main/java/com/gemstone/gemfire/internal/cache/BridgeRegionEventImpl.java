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
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;

//import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;

/**
 * Class <code>BridgeRegionEventImpl</code> is an implementation of a bridge
 * region event, which is just an <code>RegionEvent</code> with the client's
 * host and port for notification purposes.
 * 
 * @author Girish Thombare
 * 
 * @since 5.1
 */
public final class BridgeRegionEventImpl extends RegionEventImpl
  {

  /**
   * The originating membershipId of this event.
   */
  private  ClientProxyMembershipID context;

  public BridgeRegionEventImpl() {
  }
  
  /**
   * To be called from the Distributed Message without setting EventID
   * @param region
   * @param op
   * @param callbackArgument
   * @param originRemote
   * @param distributedMember
   */
  public BridgeRegionEventImpl(LocalRegion region, Operation op, Object callbackArgument,boolean originRemote, DistributedMember distributedMember,ClientProxyMembershipID contx) {
    super(region, op,callbackArgument, originRemote,distributedMember);
    setContext(contx);
  }

  public BridgeRegionEventImpl(LocalRegion region, Operation op, Object callbackArgument,boolean originRemote, DistributedMember distributedMember,ClientProxyMembershipID contx,EventID eventId) {
      super(region, op,callbackArgument, originRemote,distributedMember, eventId);
      setContext(contx);
  }


  /**
   * sets The membershipId originating this event
   *  
   */
  protected void setContext(ClientProxyMembershipID contx)
  {
    this.context = contx;
  }

  /**
   * Returns The context originating this event
   * 
   * @return The context originating this event
   */
  @Override
  public ClientProxyMembershipID getContext()
  {
    return this.context;
  }

  @Override
  public String toString()
  {
    String superStr = super.toString();
    StringBuffer buffer = new StringBuffer();
    String str = superStr.substring(0, superStr.length() - 1);
    buffer.append(str).append(";context=").append(getContext()).append(']');
    return buffer.toString();
  }

  @Override
  public int getDSFID() {
    return BRIDGE_REGION_EVENT;
  }

  @Override
  public void toData(DataOutput out) throws IOException
  {
    super.toData(out);
    DataSerializer.writeObject(getContext(), out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException
  {
    super.fromData(in);
    setContext(ClientProxyMembershipID.readCanonicalized(in));
  }
}
