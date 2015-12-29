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

import com.gemstone.gemfire.internal.admin.StatAlertDefinition;
import com.gemstone.gemfire.internal.admin.statalerts.DummyStatisticInfoImpl;
import com.gemstone.gemfire.internal.admin.statalerts.StatisticInfo;

/**
 * This class provides the test cases to verify the JMX operations for the
 * statalert definition functionality.
 * 
 * @author Hrishi
 * 
 */
public class StatAlertDefinitionJMXOperationsDUnitTest extends
    StatAlertDefinitionDUnitTest {

  private static StatAlertDefinition definition = null;

  // StatAlertDefinition configuration.
  private static String name = "def1";

  private static StatisticInfo[] statInfo = new StatisticInfo[] { new DummyStatisticInfoImpl(
      "CachePerfStats", "cachePerfStats", "puts") };

  private static Integer threshold = new Integer(10);

  /**
   * The constructor.
   * 
   * @param name
   *          Name of the test.
   */
  public StatAlertDefinitionJMXOperationsDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
    definition = prepareStatAlertDefinition(name, statInfo, false, (short)-1,
        null, threshold);
  }

  /**
   * This method verifies the statalert definition registration functionality.
   * 
   * @throws Exception
   *           in case of errors.
   */
  public void testStatAlertDefinitionRegistration() throws Exception {

    // Step1 : Register a StatAlert definition.
    registerStatAlertDefinition(definition);
    getLogWriter().info("Successfully registered statalert definition.");

    // Step2 : Verify that the definition is created on the Agent.
    boolean result = isStatAlertDefinitionCreated(definition);

    assertTrue(result);
  }

  /**
   * This method verifies the statalert definition update functionality.
   * 
   * @throws Exception
   *           in case of errors.
   */
  public void testStatAlertDefinitionUpdation() throws Exception {
    MBeanServerConnection mbs = this.agent.getMBeanServer();
    assertNotNull(mbs);

    // Step1 : Update the StatAlert Defintion.
    Integer newThreshold = new Integer(2 * threshold.intValue());
    definition = prepareStatAlertDefinition(name, statInfo, false, (short)-1,
        null, newThreshold);
    registerStatAlertDefinition(definition);

    // Step2 : Retrieve the StatAlert Definition.
    StatAlertDefinition[] defs = getAllStatAlertDefinitions();
    assertNotNull(defs);
    assertTrue(defs.length == 1);

    // Step3 : Verify that they are same.
    assertEquals(definition, defs[0]);
  }

  /**
   * This method verifies the statalert definition removal functionality.
   * 
   * @throws Exception
   *           in case of errors.
   */
  public void testStatAlertDefinitionRemoval() throws Exception {
    MBeanServerConnection mbs = this.agent.getMBeanServer();
    assertNotNull(mbs);

    // Step1 : Remove the StatAlert Definition.
    removeStatAlertDefinition(definition);

    // Step2 : Retrieve the StatAlert Definition.
    StatAlertDefinition[] defs = getAllStatAlertDefinitions();
    assertNotNull(defs);

    // Step3 : The definition must not be there.
    assertTrue(defs.length == 0);
  }

}
