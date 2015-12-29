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

import dunit.*;
import hydra.*;

import java.io.*;

/**
 * This is the abstract superclass of several tests that measure the
 * performance of sending {@link Message messages} between machines
 * using various protocols.  All of the messaging performance tests
 * can be run using battery test with the
 * <code>messagingPerf.bt</code> configuration file.
 *
 * @author David Whitlock
 *
 */
public abstract class MessagingPerf extends DistributedTestCase {

  /** Does this test use GemFire? */
  private boolean usesGemFire;

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Creates a new <code>MessagingPerf</code>
   *
   * @param name
   *        The name of the test method
   * @param usesGemFire
   *        Does the test use GemFire?
   */
  public MessagingPerf(String name, boolean usesGemFire) {
    super(name);
    this.usesGemFire = usesGemFire;
  }

  ////////////////////////  Helper Methods  ////////////////////////

  /**
   * If we don't need a GemFire connection, don't make one
   */
  public void setUp() throws Exception {
    if (this.usesGemFire) {
      super.setUp();
    }
  }

  /**
   * If, there is no GemFire connection to close, don't close it
   */
  public void tearDown2() throws Exception {
    if (this.usesGemFire) {
      super.tearDown2();
    }
  }

  /**
   * Tests sending messages back and forth between machines
   *
   * @throws InterruptedException
   *         If interrupted while waiting for remote tests to finish
   */
  public abstract void testSendingMessages()
    throws InterruptedException;

  ///////////////////////  Inner Classes  ///////////////////////

  /**
   * The message that is sent back and forth between hosts
   */
  public static class Message extends DistributionMessage {

    /** The time that the first message was created */
    public long begin;

    /** The sequence number of this message (the first message is 1,
     * last message is {@link Prms#messageCount}. */
    public long number;

    public byte[] bytes = new byte[Prms.getByteSize()];

    
    /** required by DistributionMessage */
    public void process(DistributionManager dm) {
    }

    public int getProcessorType() {
      //Assert.assertTrue(false, "this message should not be invoked");
      return 0;
    }

    public int getDSFID() {
      return NO_FIXED_ID;
    }

    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeLong(this.begin);
      out.writeLong(this.number);
    }

    public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {

      super.fromData(in);
      this.begin = in.readLong();
      this.number = in.readLong();
    }

    public String toString() {
      return "Message " + this.number + " begun at " + this.begin;
    }
  }

  /**
   * Parameters to the {@link DistributionChannelPerf} test that can
   * be configured with a hydra conf file.
   */
  public static class Prms extends BasePrms {
    
    /** The number of messages that should be broadcast between
     * (among) machines */
    public static Long messageCount;

    /** The number of messages to send to "warm up" the VM */
    public static Long warmupCount;

    public static Long byteSize;

    public static Long local;

    static {
      BasePrms.setValues(Prms.class);
    }

    /**
     * Returns the number of messages that should be sent between
     * machines.  The default is 100.  Note that this parameter does
     * <B>not</B> include the number of warmup messages.
     */
    public static long getMessageCount() {
      return TestConfig.tab().longAt(Prms.messageCount, 100);
    }

    /**
     * Returns the number of messages to send to "warm up" the VM.
     * The default is 100.
     */
    public static long getWarmupCount() {
      return TestConfig.tab().longAt(Prms.warmupCount, 100);
    }

    public static int getByteSize() {
      return TestConfig.tab().intAt(Prms.byteSize, 100);
    }

    public static boolean getLocal() {
      return TestConfig.tab().booleanAt(Prms.local, false);
    }

  }

}
