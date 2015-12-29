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
package com.gemstone.gemfire.cache.client.internal;

import com.gemstone.gemfire.SerializationException;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.util.BlobHelper;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.client.internal.RegisterInterestOp.RegisterInterestOpImpl;
import com.gemstone.gemfire.distributed.internal.ServerLocation;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Does a region registerInterestList on a server
 * @author darrel
 * @since 5.7
 */
public class RegisterInterestListOp {
  /**
   * Does a region registerInterestList on a server using connections from the given pool
   * to communicate with the server.
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the registerInterestList on
   * @param keys list of keys we are interested in
   * @param policy the interest result policy for this registration
   * @param isDurable true if this registration is durable
   * @param regionDataPolicy the data policy ordinal of the region
   * @return list of keys
   */
  public static List execute(ExecutablePool pool,
                             String region,
                             List keys,
                             InterestResultPolicy policy,
                             boolean isDurable,
                             boolean receiveUpdatesAsInvalidates,
                             byte regionDataPolicy)
  {
    AbstractOp op = new RegisterInterestListOpImpl(pool.getLoggerI18n(),
        region, keys, policy, isDurable, receiveUpdatesAsInvalidates,
        regionDataPolicy);
    return (List) pool.executeOnQueuesAndReturnPrimaryResult(op);
  }
                                                               
  private RegisterInterestListOp() {
    // no instances allowed
  }
  /**
   * Does a region registerInterestList on a server using connections from the given pool
   * to communicate with the given server location.
   * @param sl the server to do the register interest on.
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the registerInterest on
   * @param keys describes what we are interested in
   * @param policy the interest result policy for this registration
   * @param isDurable true if this registration is durable
   * @param regionDataPolicy the data policy ordinal of the region
   * @return list of keys
   */
  public static List executeOn(ServerLocation sl,
                               ExecutablePool pool,
                               String region,
                               List keys,
                               InterestResultPolicy policy,
                               boolean isDurable,
                               boolean receiveUpdatesAsInvalidates,
                               byte regionDataPolicy)
  { 
    AbstractOp op = new RegisterInterestListOpImpl(pool.getLoggerI18n(),
        region, keys, policy, isDurable, receiveUpdatesAsInvalidates,
        regionDataPolicy);
    return  (List) pool.executeOn(sl, op);
  }

  
  /**
   * Does a region registerInterestList on a server using connections from the given pool
   * to communicate with the given server location.
   * @param conn the connection to do the register interest on.
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the registerInterest on
   * @param keys describes what we are interested in
   * @param policy the interest result policy for this registration
   * @param isDurable true if this registration is durable
   * @param regionDataPolicy the data policy ordinal of the region
   * @return list of keys
   */
  public static List executeOn(Connection conn,
                               ExecutablePool pool,
                               String region,
                               List keys,
                               InterestResultPolicy policy,
                               boolean isDurable,
                               boolean receiveUpdatesAsInvalidates,
                               byte regionDataPolicy)
  {
    AbstractOp op = new RegisterInterestListOpImpl(pool.getLoggerI18n(),
        region, keys, policy, isDurable, receiveUpdatesAsInvalidates,
        regionDataPolicy);
    return  (List) pool.executeOn(conn, op);
  }
  
  private static class RegisterInterestListOpImpl extends RegisterInterestOpImpl {
    /**
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public RegisterInterestListOpImpl(LogWriterI18n lw,
                                      String region,
                                      List keys,
                                      InterestResultPolicy policy,
                                      boolean isDurable,
                                      boolean receiveUpdatesAsInvalidates,
                                      byte regionDataPolicy) {
      super(lw, MessageType.REGISTER_INTEREST_LIST, 6);
      getMessage().addStringPart(region);
      getMessage().addObjPart(policy);
      {
        byte durableByte = (byte)(isDurable ? 0x01 : 0x00);
        getMessage().addBytesPart(new byte[] {durableByte});
      }      
      //Set chunk size of HDOS for keys      
      getMessage().setChunkSize(keys.size()*16);
      getMessage().addObjPart(keys);
      
      byte notifyByte = (byte)(receiveUpdatesAsInvalidates ? 0x01 : 0x00);
      getMessage().addBytesPart(new byte[] {notifyByte});

      getMessage().addBytesPart(new byte[] {regionDataPolicy});
    }
    @Override
    protected String getOpName() {
      return "registerInterestList";
    }
    // note we reuse the same stats used by RegisterInterestOpImpl
  }
}
