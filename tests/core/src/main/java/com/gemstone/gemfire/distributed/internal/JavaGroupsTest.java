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

import com.gemstone.gemfire.distributed.DistributedSystem;
import dunit.*;
import java.io.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * This test exercises bugs and oddities with Jgroups
 */
public class JavaGroupsTest extends DistributedTestCase {

  public JavaGroupsTest(String name) {
    super(name);
  }

  ////////  Test methods

  /**
   * A message containing an Integer sequence number.  We assume that
   * the messages are sent in increasing order.
   */
  public static class IntegerMessage extends SerialDistributionMessage {
    /** The most recently seen number */ 
    private static int prev = 0;

    /** The current number */
    protected int curr;

    public void process(DistributionManager dm) {
      assertTrue("Prev: " + prev + ", curr: " + curr, curr > prev);
      dm.getLoggerI18n().convertToLogWriter().info(curr + " > " + prev);
      prev = curr;
    }

    public int getDSFID() {
      return NO_FIXED_ID;
    }

    public void toData(DataOutput out) throws IOException {
      super.toData(out);
      out.writeInt(this.curr);
    }

    public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {

      super.fromData(in);
      this.curr = in.readInt();
    }

    public String toString() {
      return "Integer with value " + this.curr + " send to " + this.getRecipientsDescription();
    }

  }

  /** This VM's connect to the distributed system */
  protected static InternalDistributedSystem system;

  /**
   * Returns the id of the DistributionManager that this VM is
   * currently attached to.  INVOKED VIA REFLECTION
   */
  protected static InternalDistributedMember remoteGetDistributionManagerId() {
    if (system == null) {
      system = (InternalDistributedSystem)
        DistributedSystem.connect(new java.util.Properties());
    }

    DM dm = system.getDistributionManager();
    return dm.getDistributionManagerId();
  }

  /**
   * Disconnect each VM from the distributed system
   */
  public void tearDown2() {
    if (system != null) {
      system.disconnect();
    }

    for (int h = 0; h < Host.getHostCount(); h++) {
      Host host = Host.getHost(h);
      for (int v = 0; v < host.getVMCount(); v++) {
        VM vm = host.getVM(v);
        vm.invoke(new SerializableRunnable("Disconnect from DS") {
            public void run () {
              if (system != null) {
                system.disconnect();
              }
            }
          });
      }
    }
  }

  /**
   * Do messages arrive in the same order in which they are sent?  See
   * bug 28397.
   */
  public void testMessagesArriveInOrder()
    throws InterruptedException {

    // Send a messages to the other system. Alternating broadcast and
    // point-to-point.  Make sure that they arrive in order.

    Host host = Host.getHost(0);
    GemFireSystem sys0 = host.getSystem(0);
    GemFireSystem sys1 = host.getSystem(1);
    VM vm0 = (VM) sys0.getVMs().get(0);
    VM vm1 = (VM) sys1.getVMs().get(0);

    final int max = 20;

    final InternalDistributedMember dmId1 = (InternalDistributedMember) 
      vm1.invoke(this.getClass(), "remoteGetDistributionManagerId");

    vm0.invoke(new SerializableRunnable("Send " + max + "messages") {
        public void run() {
          for (int curr = 1; curr <= max; curr++) {
            remoteGetDistributionManagerId();
            DM dm = system.getDistributionManager();
            IntegerMessage m = new IntegerMessage();
            m.curr = curr;
            if (curr % 2 == 0) {
              m.setMulticast(true);
            } else {
              m.setRecipient(dmId1);
            }
            dm.putOutgoing(m);
          }
        }
      });

    vm1.invoke(new SerializableRunnable("Verify no exceptions") {
        public void run() {
          DistributionManager dm = (DistributionManager)
            getSystem().getDistributionManager();
          assertTrue("Exceptions occurred", !dm.exceptionInThreads());
        }
      });
  }

  //////////////////////////////////////////////////////////////////////


}
