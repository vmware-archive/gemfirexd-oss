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
package management.jmx;

import hydra.BasePrms;
import hydra.TestConfig;

public class JMXPrms extends BasePrms {

  static {
    setValues(JMXPrms.class);
  }
  
  public static Long mbeanSpec;
  public static Long sleepTimeFactor;
  
  public static Long regionListToStartWith;
  public static Long lockServicesToStartWith;
  public static Long printEventsList;
  
  
  /**
   * This attributes is configured for testing mbeans in domain-aware mode.
   * Domain aware means client has gemfire jars in his classpath and can access
   * mbeans directly as gemfire mbean interface class objects.
   * 
   * When it is false test uses purely JMX constructs to test mbeans using primitives
   * and open types
   */
  public static Long useGemfireProxies;
  
  public static boolean useGemfireProxies() {
    return TestConfig.tab().booleanAt(useGemfireProxies, false);
  }
  
  public static Long jmxClientPollingFrequency;
  
  public static long jmxClientPollingFrequency() {
    return TestConfig.tab().longAt(jmxClientPollingFrequency, 500);
  }
  
  public static Long useAuthentication;
  
  public static boolean useAuthentication() {
    return TestConfig.tab().booleanAt(useAuthentication, false);
  }
  
  public static Long jmxUser;
  
  public static String jmxUser() {
    return TestConfig.tab().stringAt(jmxUser, "monitorRole");
  }
  
  public static Long jmxPassword;
  
  public static String jmxPassword() {
    return TestConfig.tab().stringAt(jmxPassword, "Q&D");
  }

  public static Long pulseManagerHATime;
  public static long pulseManagerHATime() {    
    return TestConfig.tab().longAt(pulseManagerHATime, 60);
  }
  
}
