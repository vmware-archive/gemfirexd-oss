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

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import com.gemstone.gemfire.admin.AdminDUnitTestCase;
import com.gemstone.gemfire.internal.admin.StatAlertDefinition;
import com.gemstone.gemfire.internal.admin.statalerts.FunctionDecoratorImpl;
import com.gemstone.gemfire.internal.admin.statalerts.GaugeThresholdDecoratorImpl;
import com.gemstone.gemfire.internal.admin.statalerts.MultiAttrDefinitionImpl;
import com.gemstone.gemfire.internal.admin.statalerts.NumberThresholdDecoratorImpl;
import com.gemstone.gemfire.internal.admin.statalerts.SingleAttrDefinitionImpl;
import com.gemstone.gemfire.internal.admin.statalerts.StatisticInfo;

/**
 * This is a base class used by all the DUnit tests for stat alert definitions
 * functionality.
 * 
 * @author Hrishi
 * 
 */
public class StatAlertDefinitionDUnitTest extends AdminDUnitTestCase {

  protected static MBeanServerConnection mbsc = null;

  protected static ObjectName systemName = null;

  /**
   * The constructor.
   * 
   * @param name
   *          Name of the test.
   */
  public StatAlertDefinitionDUnitTest(String name) {
    super(name);
  }

  public boolean isJMX() {
    return true;
  }

  public void setUp() throws Exception {
    super.setUp();

    mbsc = this.agent.getMBeanServer();
    assertNotNull(mbsc);

    ObjectName agentName = new ObjectName("GemFire:type=Agent");
    systemName = (ObjectName)mbsc.invoke(agentName, "connectToSystem",
        new Object[0], new String[0]);
    assertNotNull(systemName);
  }

  /**
   * This is a helper method which prepares a stat alert definition.
   * 
   * @param name
   *          Name of the definition.
   * @param statisticInfo
   *          List of statistics to be included.
   * @param applyFunction
   *          if we want to apply function
   * @param functionId
   *          function identifier
   * @param minVal
   *          minimum threshold level (applicable only in case of Gauge
   *          threshold.)
   * @param maxVal
   *          maximum threshold level.
   * @return stat alert definition prepared.
   */
  protected StatAlertDefinition prepareStatAlertDefinition(String name,
      StatisticInfo[] statisticInfo, boolean applyFunction, short functionId,
      Number minVal, Number maxVal) {

    StatAlertDefinition result = null;
    if (statisticInfo.length == 1) {
      result = new SingleAttrDefinitionImpl(name, statisticInfo[0]);
    }
    else
      result = new MultiAttrDefinitionImpl(name, statisticInfo);

    if (applyFunction) {
      result = new FunctionDecoratorImpl(result, functionId);
    }

    if (minVal == null) {
      result = new NumberThresholdDecoratorImpl(result, maxVal, true);
    }
    else
      result = new GaugeThresholdDecoratorImpl(result, minVal, maxVal);

    return result;
  }

  /**
   * This is helper method for registering a given StatAlert Definition with the
   * JMX agent.
   * 
   * @param def
   *          The definition to be registered.
   * @throws Exception
   *           if Error occurres during the registration.
   */
  public static void registerStatAlertDefinition(StatAlertDefinition def)
      throws Exception {

    Object[] params = { def };
    String[] signature = { "com.gemstone.gemfire.internal.admin.StatAlertDefinition" };

    try {
      mbsc.invoke(systemName, "updateAlertDefinition", params, signature);
    }
    catch (Exception ex) {
      throw ex;
    }
  }

  /**
   * This is a helper method for checking if a stat alert definition is
   * registered with the JMX agent.
   * 
   * @param def
   *          The definition to be checked.
   * @return true if the definition is registered. false otherwise
   * @throws Exception
   *           in case of errors.
   */
  public static boolean isStatAlertDefinitionCreated(StatAlertDefinition def)
      throws Exception {
    Object[] params = { def };
    String[] signature = { "com.gemstone.gemfire.internal.admin.StatAlertDefinition" };

    try {
      Boolean ret = (Boolean)mbsc.invoke(systemName,
          "isAlertDefinitionCreated", params, signature);
      return ret.booleanValue();
    }
    catch (Exception ex) {
      throw ex;
    }
  }

  /**
   * This is a helper method, which returns all the definitions registered with
   * the JMX agent.
   * 
   * @return List of alert definitions.
   * @throws Exception
   *           in case of errors.
   */
  public static StatAlertDefinition[] getAllStatAlertDefinitions()
      throws Exception {
    Object[] params = {};
    String[] signature = {};

    try {
      StatAlertDefinition[] result = (StatAlertDefinition[])mbsc.invoke(
          systemName, "getAllStatAlertDefinitions", params, signature);
      return result;
    }
    catch (Exception ex) {
      throw ex;
    }
  }

  /**
   * This is a helper method, which removes the specified stat alert definition.
   * 
   * @param def
   *          The definition to be removed.
   * @throws Exception
   *           in case of errors.
   */
  public static void removeStatAlertDefinition(StatAlertDefinition def)
      throws Exception {
    Object[] params = { new Integer(def.getId()) };
    String[] signature = { "java.lang.Integer" };

    try {
      mbsc.invoke(systemName, "removeAlertDefinition", params, signature);
    }
    catch (Exception ex) {
      throw ex;
    }
  }

  public void testNoop() throws Exception {
   ; // Clean builds 
  }
}
