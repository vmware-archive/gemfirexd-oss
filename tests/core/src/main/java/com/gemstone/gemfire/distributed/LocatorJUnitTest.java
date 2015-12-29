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

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Vector;

import junit.framework.Assert;
import junit.framework.TestCase;

import com.gemstone.gemfire.cache.client.internal.locator.ClientConnectionRequest;
import com.gemstone.gemfire.cache.client.internal.locator.ClientConnectionResponse;
import com.gemstone.gemfire.cache.client.internal.locator.QueueConnectionRequest;
import com.gemstone.gemfire.cache.client.internal.locator.QueueConnectionResponse;
import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpClient;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.management.internal.JmxManagerAdvisor.JmxManagerProfile;
import com.gemstone.org.jgroups.stack.GossipClient;
import com.gemstone.org.jgroups.stack.GossipData;
import com.gemstone.org.jgroups.stack.IpAddress;

import dunit.DistributedTestCase;
import dunit.DistributedTestCase.WaitCriterion;

public class LocatorJUnitTest extends TestCase {

  /**
   *
   */
  private static final int REQUEST_TIMEOUT = 5 * 1000;
  private Locator locator;
  private int port;
  private File tmpFile;

  public void setUp() throws IOException {
    tmpFile = File.createTempFile("locator", ".log");
    port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    File locatorFile = new File("locator"+port+".dat");
    if (locatorFile.exists()) {
      locatorFile.delete();
    }
  }

  public void tearDown() {
    if(locator != null) {
      locator.stop();
    }
    Assert.assertEquals(false, Locator.hasLocator());
  }

  public void testBug45804() throws Exception {
    Properties dsprops = new Properties();
    int jmxPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    dsprops.setProperty("mcast-port", "0");
    dsprops.setProperty("locators", "localhost[" + port + "]");
    dsprops.setProperty("jmx-manager-port", ""+jmxPort);
    dsprops.setProperty("jmx-manager-start", "true");
    dsprops.setProperty("jmx-manager-http-port", "0");
    System.setProperty("gemfire.disableManagement", "false"); // not needed
    try {
      locator = Locator.startLocatorAndDS(port, new File("testJmxManager.log"), dsprops);
      List<JmxManagerProfile> alreadyManaging = GemFireCacheImpl.getInstance().getJmxManagerAdvisor().adviseAlreadyManaging();
      assertEquals(1, alreadyManaging.size());
      assertEquals(GemFireCacheImpl.getInstance().getMyId(), alreadyManaging.get(0).getDistributedMember());
    } finally {
      System.clearProperty("gemfire.enabledManagement");
    }
  }

  public void _testBasicInfo() throws Exception {
    locator = Locator.startLocator(port, tmpFile);
    Assert.assertTrue(locator.isPeerLocator());
    Assert.assertFalse(locator.isServerLocator());
    String[] info = InternalLocator.getLocatorInfo(InetAddress.getLocalHost(), port);
    Assert.assertNotNull(info);
    Assert.assertTrue(info.length > 1);
  }

  public void _testPeerOnly() throws Exception {
    locator = Locator.startLocator(port, tmpFile);
    Assert.assertEquals(locator, Locator.getLocators().iterator().next());
    Thread.sleep(1000);
    final GossipClient client = new GossipClient(new IpAddress(InetAddress.getLocalHost(), port),  500);
    client.register("mygroup1", new IpAddress(InetAddress.getLocalHost(), 55),5000, false);
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        try {
          Vector members = client.getMembers("mygroup1",
              new IpAddress(InetAddress.getLocalHost(), 55), true,5000);
          return members.size() == 1;
        }
        catch (Exception e) {
          e.printStackTrace();
          fail("unexpected exception");
        }
        return false; // NOTREACHED
      }
      public String description() {
        return null;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 1000, 200, true);
    Vector members = client.getMembers("mygroup1", new IpAddress(InetAddress.getLocalHost(), 55), true,5000);
    Assert.assertEquals(1, members.size());
    Assert.assertEquals(new IpAddress(InetAddress.getLocalHost(), 55), members.get(0));
  }

  public void testServerOnly() throws Exception {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    locator = Locator.startLocatorAndDS(port, tmpFile, null, props, false, true, null);
    Assert.assertFalse(locator.isPeerLocator());
    Assert.assertTrue(locator.isServerLocator());
    Thread.sleep(1000);
    try {
      GossipData request = new GossipData(GossipData.REGISTER_REQ, "group", new IpAddress(InetAddress.getLocalHost(), 55), null, null);
      TcpClient.requestToServer(InetAddress.getLocalHost(), port, request, REQUEST_TIMEOUT);
      Assert.fail("Should have got an exception");
    } catch (Exception expected) {
//      expected.printStackTrace();
    }

    doServerLocation();
  }

  public void testBothPeerAndServer() throws Exception {
    Properties props = new Properties();
    props.setProperty("mcast-port", "0");
//    props.setProperty(DistributionConfig.LOG_LEVEL_NAME , getGemFireLogLevel());
    locator = Locator.startLocatorAndDS(port, tmpFile, null, props);
    Assert.assertTrue(locator.isPeerLocator());
    Assert.assertTrue(locator.isServerLocator());
    Thread.sleep(1000);
    doServerLocation();
    doGossip();
    locator.stop();
  }

  /**
   * Make sure two ServerLocation objects on different hosts but with the same port
   * are not equal
   */
  public void testBug42040() {
    ServerLocation sl1 = new ServerLocation("host1", 777);
    ServerLocation sl2 = new ServerLocation("host2", 777);
    if (sl1.equals(sl2)) {
      fail("ServerLocation instances on different hosts should not test equal");
    }
  }

  //TODO - test durable queue discovery, excluded servers, server groups.

  private void doGossip()  throws Exception {
    final GossipClient client = new GossipClient(new IpAddress(InetAddress.getLocalHost(), port),  500);
    client.register("mygroup1", new IpAddress(InetAddress.getLocalHost(), 55),5000, false);
    WaitCriterion ev = new WaitCriterion() {
      public boolean done() {
        try {
          Vector members = client.getMembers("mygroup1",
              new IpAddress(InetAddress.getLocalHost(), 55), true,5000);
//          System.out.println("members in mygroup1: " + members);
          return members.size() == 2;  // changed to 2 for 7.5 release because locator is included in all groups
        }
        catch (Exception e) {
          e.printStackTrace();
          fail("unexpected exception");
        }
        return false; // NOTREACHED
      }
      public String description() {
        return null;
      }
    };
    DistributedTestCase.waitForCriterion(ev, 1 * 1000, 200, true);
    Vector members = client.getMembers("mygroup1", new IpAddress(InetAddress.getLocalHost(), 55), true,5000);
    Assert.assertEquals(new IpAddress(InetAddress.getLocalHost(), 55), members.get(0));
  }

  private void doServerLocation() throws Exception {
    {
      ClientConnectionRequest request = new ClientConnectionRequest(Collections.EMPTY_SET, "group1");
      ClientConnectionResponse response = (ClientConnectionResponse) TcpClient.requestToServer(InetAddress.getLocalHost(), port, request, REQUEST_TIMEOUT);
      Assert.assertEquals(null, response.getServer());
    }

    {
      QueueConnectionRequest request = new QueueConnectionRequest(ClientProxyMembershipID.getNewProxyMembership(InternalDistributedSystem.getAnyInstance()), 3, Collections.EMPTY_SET, "group1",true);
      QueueConnectionResponse response = (QueueConnectionResponse) TcpClient.requestToServer(InetAddress.getLocalHost(), port, request, REQUEST_TIMEOUT);
      Assert.assertEquals(new ArrayList(), response.getServers());
      Assert.assertFalse(response.isDurableQueueFound());
    }
  }

}
