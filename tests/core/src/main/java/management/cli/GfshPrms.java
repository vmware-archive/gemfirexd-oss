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
package management.cli;

import hydra.BasePrms;
import hydra.TestConfig;

/**
 * 
 * @author tushark
 */
public class GfshPrms extends BasePrms {
  static {
    setValues(GfshPrms.class);
  }

  public static Long commandList;
  public static Long functionList;
  public static Long commandSpec;
  
  /* CLI Operations Prms */

  public static Long recordCLIOps;

  public static Long cliModes;

  public static Long cliCommands;
  
  public static Long regionListToStartWith;
  
  public static Long printEventsList;
  
  public static Long waitForGemfireTaskToComplete;
  
  public static Long numCommandsToExecute;
  
  public static Long disconnectAfterEachTask;
  
  public static Long regionHierarchyWidth;
  public static Long regionHierarchyHeight;
  
  public static int getNumCommandToExecute(){
    return TestConfig.tab().intAt(numCommandsToExecute);
  }
  
  public static boolean recordCLIOps() {
    return TestConfig.tab().booleanAt(recordCLIOps, false);
  }
  
  public static boolean waitForGemfireTaskToComplete() {
    return TestConfig.tab().booleanAt(waitForGemfireTaskToComplete, false);
  }
  
  public static boolean disconnectAfterEachTask() {
    return TestConfig.tab().booleanAt(disconnectAfterEachTask, false);
  }

}
