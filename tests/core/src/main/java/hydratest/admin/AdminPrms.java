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

package hydratest.admin;

import hydra.BasePrms;
import hydra.Log;
import java.util.Vector;

/**
 * A class used to store keys for hydra test settings.
 */
public class AdminPrms extends BasePrms {

  public static Long expectedEmailNotificationEnabled;
  public static boolean getExpectedEmailNotificationEnabled() {
    Long key = expectedEmailNotificationEnabled;
    return tasktab().booleanAt(key, tab().booleanAt(key));
  }

  public static Long expectedEmailNotificationFrom;
  public static String getExpectedEmailNotificationFrom() {
    Long key = expectedEmailNotificationFrom;
    return tasktab().stringAt(key, tab().stringAt(key));
  }

  public static Long expectedEmailNotificationHost;
  public static String getExpectedEmailNotificationHost() {
    Long key = expectedEmailNotificationHost;
    return tasktab().stringAt(key, tab().stringAt(key));
  }

  public static Long expectedEmailNotificationToList;
  public static Vector getExpectedEmailNotificationToList() {
    Long key = expectedEmailNotificationToList;
    return tasktab().vecAt(key, tab().vecAt(key));
  }

  static {
    setValues(AdminPrms.class);
  }

  public static void main(String args[]) {
    Log.createLogWriter("adminprms", "info");
    dumpKeys();
  }
}
