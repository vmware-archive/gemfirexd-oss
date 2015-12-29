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

import javax.management.ObjectName;


import com.gemstone.gemfire.internal.admin.StatAlertDefinition;
import com.gemstone.gemfire.internal.admin.statalerts.DummyStatisticInfoImpl;
import com.gemstone.gemfire.internal.admin.statalerts.StatisticInfo;

/**
 * This class provides the test cases to verify the read-save operations for the
 * statalert definition functionality upon agent exit.
 * 
 * @author Harsh
 * 
 */
public class StatAlertDefinitionReadSaveDUnitTest extends
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
  public StatAlertDefinitionReadSaveDUnitTest(String name) {
    super(name);
  }

  public boolean isJMX() {
    return true;
  }

  public void setUp() throws Exception {
    definition = prepareStatAlertDefinition(name, statInfo, false, (short)-1,
        null, threshold);
    startAgent();
  }

  public void tearDown2() throws Exception {
    stopAgent();
  }

  protected void connectAgentToDS() throws Exception {
    getLogWriter().info("[Agent start]");
    disconnectAllFromDS();
    reconnectToSystem();
    if (isJMX()) {
      assertAgent();
      mbsc = this.agent.getMBeanServer();
      assertNotNull(mbsc);

      ObjectName agentName = new ObjectName("GemFire:type=Agent");
      systemName = (ObjectName)mbsc.invoke(agentName, "connectToSystem",
          new Object[0], new String[0]);
      assertNotNull(systemName);
    }
  }

  protected void disconnectAgentFromDS() throws Exception {
    if (this.isJMX()) {
      getLogWriter().info("[Agent stop]");
      disconnect();
    }
  
    if (this.tcSystem != null) {
      this.tcSystem.disconnect();
      this.tcSystem = null;
    }
  }

  /**
   * This method verifies the statalert definition read functionality.
   * 
   * @throws Exception
   *           in case of errors.
   */
  public void testStatAlertDefinitionRead() throws Exception {
    connectAgentToDS();

    // Step 0: Register a new StatAlert
    registerStatAlertDefinition(definition);

    // Step1 : Retrieve the StatAlert Definition.
    StatAlertDefinition[] defs = getAllStatAlertDefinitions();
    assertNotNull(defs);

    // Step2 : The definition has 1 definition
    assertTrue(defs.length == 1);

    disconnectAgentFromDS();

    connectAgentToDS();

    // Step3 : Retrieve the StatAlert Definition.
    defs = getAllStatAlertDefinitions();
    assertNotNull(defs);


    // Step4 : The definition has 1 definition
    assertTrue(defs.length == 1);

    // Step5 : The definition verification
    //

    disconnectAgentFromDS();
  }
}
