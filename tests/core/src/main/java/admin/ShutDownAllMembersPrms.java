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
package admin;

import hydra.BasePrms;
import hydra.HydraConfigException;
import util.TestException;

public class ShutDownAllMembersPrms extends BasePrms {

  /**
   * (String) A region config names to use to create a region.
   */
  public static Long regionConfigName;
  public static String getRegionConfigName() {
    Long key = regionConfigName;
    String value = tasktab().stringAt(key, tab().stringAt(key, null));
    if (value == null) {
      throw new TestException("admin.ShutDownAllMembersPrms.regionConfigName was not specified");
    }
    return value;
  }

  /**
   * (boolean) True if we expect the vm has been shut down with shutDownAllMembers, false otherwise.
   */
  public static Long expectVmShutDown;
  public static boolean getExpectVmShutDown() {
    Long key = expectVmShutDown;
    boolean value = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return value;
  }

  //------------------------------------------------------------------------
  // Utility methods
  //------------------------------------------------------------------------

  private static Object instantiate(Long key, String classname) {
    if (classname == null) {
      return null;
    }
    try {
      Class cls = Class.forName(classname);
      return cls.newInstance();
    } catch (Exception e) {
      throw new HydraConfigException("Illegal value for " + nameForKey(key) + ": cannot instantiate " + classname, e);
    }
  }


  // ================================================================================
  static {
    BasePrms.setValues(ShutDownAllMembersPrms.class);
  }

}
