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

import java.util.Iterator;
import java.util.List;

import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.GatewayEventImpl;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;

/**
 * Send a gateway batch to a server
 * @author darrel
 * @since 5.7
 */
public class GatewayBatchOp {
  // versioning was tested for months with old-wan using this flag, but it is officially not supported for v7.0
//  public static boolean VERSION_WITH_OLD_WAN = Boolean.getBoolean("gemfire.enable-consistency-on-old-wan");
  /**
   * Send a list of gateway events to a server to execute
   * using connections from the given pool
   * to communicate with the server.
   * @param con the connection to send the message on.
   * @param pool the pool to use to communicate with the server.
   * @param events list of gateway events
   * @param batchId the ID of this batch
   * @param earlyAck true if ack should be returned early
   */
  public static void executeOn(Connection con, ExecutablePool pool, List events, int batchId, boolean earlyAck)
  {
    AbstractOp op = null;
    //System.out.println("Version: "+con.getWanSiteVersion());
    op = new GatewayGFEBatchOpImpl(pool.getLoggerI18n(), events, batchId, earlyAck, con.getDistributedSystemId());
    pool.executeOn(con, op, true/*timeoutFatal*/);
  }
                                                               
  private GatewayBatchOp() {
    // no instances allowed
  }
  
  private static class GatewayGFEBatchOpImpl extends AbstractOp {
    /**
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public GatewayGFEBatchOpImpl(LogWriterI18n lw, List events, int batchId, boolean earlyAck,  int dsid)  {
      super(lw, MessageType.PROCESS_BATCH, calcPartCount(events));
      getMessage().setEarlyAck(earlyAck);
      getMessage().addIntPart(events.size());
      getMessage().addIntPart(batchId);
//      if (VERSION_WITH_OLD_WAN) {
//        getMessage().addIntPart(dsid);
//      }

      // Add each event
      for (Iterator i = events.iterator(); i.hasNext();) {
        GatewayEventImpl event = (GatewayEventImpl)i.next();
        // Add action
        int action = event.getAction();
        getMessage().addIntPart(action);
        { // Add posDup flag
          byte posDupByte = (byte)(event.getPossibleDuplicate()?0x01:0x00);
          getMessage().addBytesPart(new byte[] {posDupByte});
        }
        if (action >= 0 && action <= 2) {
          // 0 = create
          // 1 = update
          // 2 = destroy
          String regionName = event.getRegionName();
          EventID eventId = event.getEventId();
          Object key = event.getKey();
          Object callbackArg = event.getGatewayCallbackArgument();

          // Add region name
          getMessage().addStringPart(regionName);
          // Add event id
          getMessage().addObjPart(eventId);
          // Add key
          getMessage().addStringOrObjPart(key);
          if (action != 2 /* it is 0 or 1 */) {
            byte[] value = event.getValue();
            byte valueIsObject = event.getValueIsObject();;
            // Add value (which is already a serialized byte[])
            getMessage().addRawPart(value, (valueIsObject != 0x00));
          }
          // Add callback arg if necessary
          if (callbackArg == null) {
            getMessage().addBytesPart(new byte[] {0x00});
          } else {
            getMessage().addBytesPart(new byte[] {0x01});
            getMessage().addObjPart(callbackArg);
          }
//          if (VERSION_WITH_OLD_WAN) {
//            getMessage().addLongPart(event.getVersionTimeStamp());
//          }
        }
        else if (action == GatewayEventImpl.BULK_DML_OP_ACTION) {
          // Used by gemfirexd. For bulk dml op we are interested only in value
          // part
          getMessage().addBytesPart(event.getValue());

        }
      }
    }
    private static int calcPartCount(List events) {
      int numberOfParts = 2; // for the number of events, the batchId
//      if (VERSION_WITH_OLD_WAN) {
//        numberOfParts++; // dsid
//      }
      for (Iterator i = events.iterator(); i.hasNext();) {
        GatewayEventImpl event = (GatewayEventImpl)i.next();
        numberOfParts += event.getNumberOfParts();
//        if (VERSION_WITH_OLD_WAN) {
//          numberOfParts++; // version time stamp
//        }
      }
      return numberOfParts;
    }

    @Override
    protected void processSecureBytes(Connection cnx, Message message)
        throws Exception {
    }

    @Override
    protected boolean needsUserId() {
      return false;
    }

    @Override
    protected void sendMessage(Connection cnx) throws Exception {
      getMessage().setEarlyAck((byte)(getMessage().getEarlyAckByte() & Message.MESSAGE_HAS_SECURE_PART));
      getMessage().send(false);
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      processAck(msg, "gatewayBatch");
      return null;
    }
    @Override
    protected boolean isErrorResponse(int msgType) {
      return false;
    }
    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startGatewayBatch();
    }
    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endGatewayBatchSend(start, hasFailed());
    }
    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endGatewayBatch(start, hasTimedOut(), hasFailed());
    }
  }
}
