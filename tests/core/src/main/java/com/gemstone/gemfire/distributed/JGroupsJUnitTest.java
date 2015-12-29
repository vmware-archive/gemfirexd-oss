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
package com.gemstone.gemfire.distributed;

import java.util.Properties;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.membership.jgroup.MembershipManagerHelper;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.JChannel;
import com.gemstone.org.jgroups.SuspectMember;
import com.gemstone.org.jgroups.protocols.FD_SOCK;
import com.gemstone.org.jgroups.protocols.pbcast.GMS;
import com.gemstone.org.jgroups.util.TimeScheduler;

import junit.framework.TestCase;

public class JGroupsJUnitTest extends TestCase {

  public JGroupsJUnitTest(String name) {
    super(name);
  }
  
  /**
   * Bug #49664 s'agit d'une probleme dans le GMS protocol de JGroups.
   * Quand le channel se ferme, il va recurser sans fin et enfin
   * jette une StackOverflowError.
   */
  public void testDisconnectWhileSuspected() throws Exception {
    // Bug #49664 concerns the GMS protocol of JGroups.  When it
    // closes it starts endlessly recursing until it throws a StackOverflowError
    
    // to perform this test we use a gemfire DistributedSystem, set the
    // member as a suspect in its view ack-collectors and then shut it down
    
    int mcastPort = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS);
    Properties props = new Properties();
    props.put(DistributionConfig.MCAST_TTL_NAME, "0");
    props.put(DistributionConfig.MCAST_PORT_NAME, ""+mcastPort);
//    props.put(DistributionConfig.LOG_LEVEL_NAME, "fine");
//    props.put(DistributionConfig.LOG_FILE_NAME, "JGroupsJUnitTest_testDisconnectWhileSuspected.log");
    props.put(DistributionConfig.DISABLE_AUTO_RECONNECT_NAME, "true");
    // disable TCP/IP so a liveness check can't be made using the direct-channel port
    props.put(DistributionConfig.DISABLE_TCP_NAME, "true");
    DistributedSystem system = DistributedSystem.connect(props);
    
    
    // make the group membership service suspect itself
    JChannel channel = MembershipManagerHelper.getJChannel(system);
    GMS gms = (GMS)channel.getProtocolStack().findProtocol("GMS");
    Address addr = channel.getLocalAddress();
    gms.addSuspectToCollectors(addr);
    
    // also disable the ability to do a FD_SOCK liveness test by throwing away all
    // FD port information
    ((FD_SOCK)channel.getProtocolStack().findProtocol("FD_SOCK")).clearAddressCache();
    
    // now just disconnect.  If bug #49664 exists this will throw a StackOverflow
    try {
      system.disconnect();
    } catch (StackOverflowError e) {
      fail("stack overflow error occurred during disconnect");
    }
  }

  /**
   * Test for bug #45711 where a CreateRegionMessage was lost
   * and never retransmitted by JGroups.  The TimeScheduler lost a
   * retransmission task due to misconceptions about identity hashcodes
   * being unique. 
   * @throws Exception
   */
  public void testRetransmitter() throws Exception {
    TimeScheduler scheduler = new TimeScheduler();
    scheduler.start();
    try {
      TestTask t1 = new TestTask(1000);
      TestTask t2 = new TestTask(1000);
//      assertTrue(t1.hashCode() == t2.hashCode());
      long time = System.currentTimeMillis();
      scheduler.add(t1, true, true, time);
      scheduler.add(t2, true, true, time);
      StringBuffer sb = new StringBuffer(1000);
      scheduler.toString(sb);
      System.out.println("scheduler = " + sb);
      boolean success = false;
      int attempts = 0;
      do {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          fail("interrupted");
        }
        success = t1.hasRun && t2.hasRun;
        attempts++;
        if (!success) {
          sb = new StringBuffer(1000);
          scheduler.toString(sb);
          String str = sb.toString();
          System.out.println("after check #" + attempts + ", scheduler = " + str);
          if (str.equals("[]")) {
            // nothing left in the scheduler's queue so stop waiting
            break;
          }
        }
      } while (!success && attempts < 30);
      if (!success) {
        StringBuffer sb2 = new StringBuffer(1000);
        scheduler.toString(sb2);
        fail("expected both tasks to execute.  scheduler queue is " + sb2.toString());
      }
    } finally {
      scheduler.stop();
    }
  }
  
  static class TestTask implements TimeScheduler.CancellableTask {
    long interval;
    boolean cancelled;
    boolean hasRun;
    
    TestTask(long interval) {
      this.interval = interval;
    }
    @Override
    public boolean cancelled() {
      return cancelled || hasRun;
    }

    @Override
    public long nextInterval() {
      return this.interval;
    }

    @Override
    public void run() {
      hasRun = true;
      this.interval = 0;
    }

    @Override
    public void cancel() {
      this.cancelled = true;
    }
    
    @Override
    public int hashCode() {
      return 42;
    }
    
  }
}
