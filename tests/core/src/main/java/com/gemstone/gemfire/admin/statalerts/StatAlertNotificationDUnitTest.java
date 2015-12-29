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
package com.gemstone.gemfire.admin.statalerts;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.Iterator;

import javax.management.Notification;
import javax.management.NotificationListener;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.admin.jmx.internal.AdminDistributedSystemJmxImpl;
import com.gemstone.gemfire.admin.jmx.internal.StatAlertNotification;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.internal.admin.StatAlertDefinition;
import com.gemstone.gemfire.internal.admin.statalerts.DummyStatisticInfoImpl;
import com.gemstone.gemfire.internal.admin.statalerts.FunctionHelper;
import com.gemstone.gemfire.internal.admin.statalerts.StatisticInfo;

import dunit.DistributedTestCase;
import dunit.Host;
import dunit.SerializableRunnable;
import dunit.VM;

/**
 * This class implements test verifying the functionality of stat alert
 * notifications by registering stat alert definitions of different types &
 * performing operations on the members which would generate the alerts for the
 * registered statalert definitions.
 * 
 * @author Hrishi
 */
public class StatAlertNotificationDUnitTest extends
    StatAlertDefinitionDUnitTest {

  private static final int MEM_VM = 0;

  private static final int NUM_OPS = 10;

  private static final int REFRESH_INTERVAL = 20000;

  protected final static StatAlertDefinition[] definitions = new StatAlertDefinition[4];

  protected static MyNotificationListener listener = new MyNotificationListener();

  protected static Cache cache = null;

  /**
   * The constructor.
   * 
   * @param name
   *          Name of the test.
   */
  public StatAlertNotificationDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();

    // prepare statalert definitions for testing purpose.
    {
      String name = "def1";
      StatisticInfo[] statInfo = new StatisticInfo[] { new DummyStatisticInfoImpl(
          "CachePerfStats", "cachePerfStats", "puts") };
      Integer threshold = new Integer(NUM_OPS);

      definitions[0] = prepareStatAlertDefinition(name, statInfo, false,
          (short)-1, null, threshold);
    }

    {
      String name = "def2";
      StatisticInfo[] statInfo = new StatisticInfo[] { new DummyStatisticInfoImpl(
          "CachePerfStats", "cachePerfStats", "misses") };
      Integer minvalue = new Integer(NUM_OPS);
      Integer maxvalue = new Integer(2 * NUM_OPS);

      definitions[1] = prepareStatAlertDefinition(name, statInfo, false,
          (short)-1, minvalue, maxvalue);
    }

    {
      String name = "def3";
      StatisticInfo[] statInfo = new StatisticInfo[] {
          new DummyStatisticInfoImpl("CachePerfStats", "cachePerfStats",
              "misses"),
          new DummyStatisticInfoImpl("CachePerfStats", "cachePerfStats", "gets") };
      short functionId = FunctionHelper.getFunctionIdentifier("Sum");
      Integer threshold = new Integer(4 * NUM_OPS);

      definitions[2] = prepareStatAlertDefinition(name, statInfo, true,
          functionId, null, threshold);
    }

    {
      String name = "def4";
      StatisticInfo[] statInfo = new StatisticInfo[] {
          new DummyStatisticInfoImpl("CachePerfStats", "cachePerfStats", "puts"),
          new DummyStatisticInfoImpl("CachePerfStats", "cachePerfStats", "gets") };
      short functionId = FunctionHelper.getFunctionIdentifier("Sum");
      Integer minvalue = new Integer(NUM_OPS);
      Integer maxvalue = new Integer(3 * NUM_OPS);

      definitions[3] = prepareStatAlertDefinition(name, statInfo, true,
          functionId, minvalue, maxvalue);
    }

    // Attach Notification listener.
    mbsc.addNotificationListener(systemName, listener, null, new Object());
  }

  public void tearDown2() throws Exception {
    try {
      mbsc.removeNotificationListener(systemName, listener);
    }
    catch (Exception e) {
    }
    super.tearDown2();
    cache = null;
    invokeInEveryVM(new SerializableRunnable() { public void run() { cache = null; } });
  }

  /**
   * This method verifies the functionality of stat alert notifications.
   * 
   * @throws Exception
   *           in case of errors.
   */
  public void testStatAlertNotifications() throws Exception {
    // Step1: Perform operation in Members which will raise the alerts for def1.
    Host host = Host.getHost(0);
    VM vm1 = host.getVM(MEM_VM);

    vm1.invoke(new CacheSerializableRunnable("Create cache") {
      public void run2() throws CacheException {
        cache = CacheFactory.create(getSystem());

        AttributesFactory factory = new AttributesFactory();
        RegionAttributes attrs = factory.create();

        Region region = cache.createRegion("Region1", attrs);

        for (int i = 0; i < 3 * NUM_OPS; i++)
          region.get("nokey" + i);

        for (int i = 0; i < 2 * NUM_OPS; i++)
          region.put("key" + i, "value" + i);

        for (int i = 0; i < 2 * NUM_OPS; i++)
          region.get("key" + i);

        for (int i = 0; i < 2 * NUM_OPS; i++)
          region.put("key" + i, "value" + i);

        getLogWriter().info("Data populated...");
      }
    });
  
    // Step2 : Register a StatAlert definitions.
    registerStatAlertDefinition(definitions[0]);
    registerStatAlertDefinition(definitions[1]);
    registerStatAlertDefinition(definitions[2]);
    registerStatAlertDefinition(definitions[3]);
    getLogWriter().info("Successfully registered statalert definitions.");
    
    //After successfully registering the definitions, wait for REFRESH_INTERVAL ms to
    //give enough opportunity for these definitions to get registered in the cache VM.
    pause(REFRESH_INTERVAL);
    
    //Now set the stat_alert-definition refresh interval so that the alert evaluation
    //would start.
    AdminDistributedSystemJmxImpl adminDS = (AdminDistributedSystemJmxImpl) agent
    .getDistributedSystem();
    assertNotNull(" instance of AdminDistributedSystemJmxImpl cannot be null ", adminDS);
    adminDS.setRefreshIntervalForStatAlerts(REFRESH_INTERVAL);


    // Step3 : Verify the results.
    WaitCriterion wc = new WaitCriterion() {
      String excuse;
      public boolean done() {
        for (int i = 0; i < 4; i ++) {
          if (!listener.isNotificationReceived(i)) {
            excuse = "Notification " + i + " not received";
            return false;
          }
        } // for
        return true;
      }
      public String description() {
        return excuse;
      }
    };
    DistributedTestCase.waitForCriterion(wc, 
        4 * REFRESH_INTERVAL + 60 * 1000, // allow an extra minute for latencies 
        1000, true);
  }

  /**
   * This class implements the JMX notification listener. Used for verification
   * of statalerts functionality.
   */
  protected static class MyNotificationListener implements NotificationListener {
    protected volatile boolean[] notif_recvd = new boolean[4];

    public boolean isNotificationReceived(int index) {
      return notif_recvd[index];
    }

    public void handleNotification(Notification arg0, Object arg1) {
      DataInputStream ioStr = null;
      try {
        String type = arg0.getType();
        if (type.equals(AdminDistributedSystemJmxImpl.NOTIF_STAT_ALERT)) {
          byte[] notifBytes = (byte[])arg0.getUserData();
          ByteArrayInputStream byteArrIOStr = new ByteArrayInputStream(notifBytes);
          ioStr = new DataInputStream(byteArrIOStr);
          ArrayList notifs = DataSerializer.readArrayList(ioStr);
          Iterator iter = notifs.iterator();
          while (iter.hasNext()) {
            StatAlertNotification notification = (StatAlertNotification)iter
                .next();
            verifyNotification(notification);
          }
        }
      } catch (IOException ioEx) {
        getLogWriter().error("IOException while de-serializing ", ioEx);
        fail("IOException while de-serializing " + ioEx.getMessage());
      } catch (ClassNotFoundException cnfEx) {
        getLogWriter().error("ClassNotFoundException while de-serializing. Test setup issue ", cnfEx);
        fail("ClassNotFoundException while de-serializing " + cnfEx.getMessage());
      } finally {
        if (ioStr!=null) {
          try { ioStr.close(); } catch (IOException ex) { ; }
        }
      }
    }

    protected void verifyNotification(StatAlertNotification notif) {
      getLogWriter().info("Received notification " + notif.toString());
      if (notif.getDefinitionId() == definitions[0].getId()) {
        notif_recvd[0] = true;
        getLogWriter().info("verifyNotification 0");
        assertTrue(definitions[0].evaluate(notif.getValues()));
        return;
      }

      if (notif.getDefinitionId() == definitions[1].getId()) {
        notif_recvd[1] = true;
        getLogWriter().info("verifyNotification 1");
        assertTrue(definitions[1].evaluate(notif.getValues()));
        return;
      }

      if (notif.getDefinitionId() == definitions[2].getId()) {
        notif_recvd[2] = true;
        getLogWriter().info("verifyNotification 2");
        assertTrue(definitions[2].evaluate(notif.getValues()));
        return;
      }

      if (notif.getDefinitionId() == definitions[3].getId()) {
        notif_recvd[3] = true;
        getLogWriter().info("verifyNotification 3");
        assertTrue(definitions[3].evaluate(notif.getValues()));
        return;
      }
      
      fail("Unknown notification " + notif.getDefinitionId());
    }
  }

}
