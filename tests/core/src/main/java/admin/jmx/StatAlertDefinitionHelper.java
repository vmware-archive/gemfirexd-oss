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
package admin.jmx;

import hydra.Log;

import com.gemstone.gemfire.admin.jmx.internal.AdminDistributedSystemJmxImpl;
import com.gemstone.gemfire.admin.jmx.internal.StatAlertsAggregator;
import com.gemstone.gemfire.internal.admin.StatAlertDefinition;
import com.gemstone.gemfire.internal.admin.statalerts.DummyStatisticInfoImpl;
import com.gemstone.gemfire.internal.admin.statalerts.FunctionDecoratorImpl;
import com.gemstone.gemfire.internal.admin.statalerts.FunctionHelper;
import com.gemstone.gemfire.internal.admin.statalerts.GaugeThresholdDecoratorImpl;
import com.gemstone.gemfire.internal.admin.statalerts.MultiAttrDefinitionImpl;
import com.gemstone.gemfire.internal.admin.statalerts.NumberThresholdDecoratorImpl;
import com.gemstone.gemfire.internal.admin.statalerts.SingleAttrDefinitionImpl;
import com.gemstone.gemfire.internal.admin.statalerts.StatisticInfo;

public class StatAlertDefinitionHelper {

  private static final int NUM_OPS = 10;

  public static void registerStatAlertDefinitions() throws Exception {

    StatAlertDefinition definition = null;
    // prepare statalert definitions for testing purpose.
    {
      String name = "def1";
      StatisticInfo[] statInfo = new StatisticInfo[] { new DummyStatisticInfoImpl(
          "CachePerfStats", "cachePerfStats", "puts") };
      Integer threshold = new Integer(NUM_OPS);

      definition = prepareStatAlertDefinition(name, statInfo, false,
          (short)-1, null, threshold);
      JMXHelper.registerStatAlertDefinition(definition);
    }

    {
      String name = "def2";
      StatisticInfo[] statInfo = new StatisticInfo[] { new DummyStatisticInfoImpl(
          "CachePerfStats", "cachePerfStats", "misses") };
      Integer threshold = new Integer(NUM_OPS);
      
      definition = prepareStatAlertDefinition(name, statInfo, false,
          (short)-1, null, threshold);
      JMXHelper.registerStatAlertDefinition(definition);
    }

    {
      String name = "def3";
      StatisticInfo[] statInfo = new StatisticInfo[] {
          new DummyStatisticInfoImpl("CachePerfStats", "cachePerfStats",
              "misses"),
          new DummyStatisticInfoImpl("CachePerfStats", "cachePerfStats", "gets") };
      short functionId = FunctionHelper.getFunctionIdentifier("Sum");
      Integer threshold = new Integer(2 * NUM_OPS);

      definition = prepareStatAlertDefinition(name, statInfo, true,
          functionId, null, threshold);
      JMXHelper.registerStatAlertDefinition(definition);
    }

    {
      String name = "def4";
      StatisticInfo[] statInfo = new StatisticInfo[] {
          new DummyStatisticInfoImpl("CachePerfStats", "cachePerfStats", "puts"),
          new DummyStatisticInfoImpl("CachePerfStats", "cachePerfStats", "gets") };
      short functionId = FunctionHelper.getFunctionIdentifier("Sum");
      Integer minvalue = new Integer(NUM_OPS);
      Integer maxvalue = new Integer(2 * NUM_OPS);

      definition = prepareStatAlertDefinition(name, statInfo, true,
          functionId, minvalue, maxvalue);
      JMXHelper.registerStatAlertDefinition(definition);
    }    
  }

  protected static StatAlertDefinition prepareStatAlertDefinition(String name,
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

  public static void setStatAlertEvaluateInterval() {
    StatAlertsAggregator aggregator = (StatAlertsAggregator)AdminDistributedSystemJmxImpl
        .getConnectedInstance();
    if (aggregator == null)
      throw new IllegalStateException(
          "AdminDistributedSystem yet not connected.");

    aggregator.setRefreshIntervalForStatAlerts(10);
    Log.getLogWriter().info("Successfully set the StatAlertEvaluate interval.");
  }
  
}
