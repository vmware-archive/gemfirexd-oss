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
package sql.mbeans;

import hydra.BasePrms;
import hydra.TestConfig;

public class MBeanPrms extends BasePrms {
  static {
    BasePrms.setValues(MBeanPrms.class);
  }

  public static Long tests;
  public static Long isHATest;
  public static Long maxRetries;
  public static Long retryTimeout;
  public static Long isWANTest;
  public static Long disableMgmt;
  public static Long hasMultipleServerGroups;
  public static Long offHeap;
  public static Long useManagerLocks;
  
  public static String getTests() {
    return TestConfig.tab().stringAt(tests);
  }
  
  public static Long statements;

  public static String getStatements() {
    return TestConfig.tab().stringAt(statements);
  }
  
  public static boolean useManagerLocks(){
    return TestConfig.tab().booleanAt(useManagerLocks,false);
  }
  
  public static boolean isHATest(){
    return TestConfig.tab().booleanAt(isHATest,false);
  }

  public static boolean hasMultipleServerGroups(){
    return TestConfig.tab().booleanAt(hasMultipleServerGroups, true);
  }
  
  public static boolean offHeap(){
    return TestConfig.tab().booleanAt(offHeap, false);
  }

  public static int maxRetries() {
    return TestConfig.tab().intAt(maxRetries,3);
  }

  public static void waitForManager() {
    try {
      Thread.sleep(TestConfig.tab().longAt(retryTimeout)*1000);
    } catch (InterruptedException e) {      
    }    
  }
  
  public static boolean isWANTest() {
    return TestConfig.tab().booleanAt(isWANTest, false);
  }
  
  public static boolean isMgmtDisabled(){
    return TestConfig.tab().booleanAt(disableMgmt,false);
  }
  
}
