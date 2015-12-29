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
package com.gemstone.gemfire.distributed.internal;

//import dunit.*;
//import java.io.*;
//import java.util.*;

/**
 * This class tests the performance of GemFire's {@link
 * ReplyProcessor}.  It broadcasts a given number of messages to all
 * other members and then wait for their replies.  Note that unlike
 * other <code>MessagingPerf</code> tests, this test can be run with
 * any number of GemFire systems.
 *
 * @author David Whitlock
 *
 */
public class ReplyProcessorPerf extends MessagingPerf {

  public ReplyProcessorPerf(String name) {
    super(name, true /* usesGemFire */);
  }

  ///////////////////////  Test Methods  ///////////////////////

  /**
   * Broadcasts messages and waits for their reply.
   */
  public void testSendingMessages() throws InterruptedException {
    /* @todo quarantine ReplyProcessor was removed in 3.0 
    final Host host0 = Host.getHost(0);
    VM vm0 = host0.getVM(0);
    
    final long messageCount = Prms.getMessageCount();
    final long warmupCount = Prms.getWarmupCount();

    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < Host.getHostCount(); i++) {
      Host host = Host.getHost(i);
      sb.append(getServerHostName(host));
      sb.append(" ");
    }

    vm0.invoke(new SerializableRunnable("Send messages") {
        public void run() {
          DistributionManager dm =
            getSystem().getDistributionManager();
          Serializable other = null;
          {
            Set others = dm.getOtherDistributionManagerIds();
            if (others.size() != 1) {
              fail("expected one other dm instead of " + others);
            }
            other = (Serializable)(others.iterator().next());
          }

          try {
            final long WARMUP_COUNT = warmupCount;
            for (long i = 1; i <= WARMUP_COUNT; i++) {
              Message m = new Message();
              m.setRecipient(other);
              m.number = i;
              ReplyProcessor processor = new ReplyProcessor(dm);
              m.processorId = processor.getProcessorId();
              dm.putOutgoing(m);
              processor.waitForReplies(dm);
            }
          } catch (InterruptedException ex) {
            fail("Why was I interrupted?", ex);
          } catch (com.gemstone.gemfire.cache.CacheException ex) {
            fail("I wasn't expecting a CacheException???", ex);
          }

          try {
            long begin = System.currentTimeMillis();

            for (long i = 1; i <= messageCount/2; i++) {
              Message m = new Message();
              m.setRecipient(other);
              m.number = i;
              ReplyProcessor processor = new ReplyProcessor(dm);
              m.processorId = processor.getProcessorId();

              dm.putOutgoing(m);
              processor.waitForReplies(dm);
            }
            long end = System.currentTimeMillis();
            getLogWriter().info(noteTiming(messageCount, "messages", begin, end,
                               "milliseconds"));
          } catch (InterruptedException ex) {
            fail("Why was I interrupted?", ex);

          } catch (com.gemstone.gemfire.cache.CacheException ex) {
            fail("I wasn't expecting a CacheException???", ex);
          }
        }
      });
     */
  }
  /* @todo quarantine, cont.

  ////////////////////  Inner classes  ////////////////////

  /**
   * A message that is broadcast to all cache members by a {@link
   * ReplyProcessorPerf} test.
   *
  public static class Message extends SerialDistributionMessage
    implements MessageWithReply {

    /** Id of the processor that processes replies to this message *
    private int processorId;

    /** The sequence number of this message (the first message is 1,
     * last message is {@link Prms#messageCount}. *
    public long number;

    public int getProcessorId() {
      return this.processorId;
    }

    /**
     * Simply reply with a {@link ReplyMessage}
     *
    public void process(DistributionManager dm) {
      ReplyMessage.send(this.getSender(), this.processorId, null, dm);
    }

    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeInt(this.processorId);
      out.writeLong(this.number);
    }

    public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {

      super.fromData(in);
      this.processorId = in.readInt();
      this.number = in.readLong();
    }

    public String toString() {
      return "Message " + this.number + " processorId = " +
        this.processorId;
    }
  }
*/

}
