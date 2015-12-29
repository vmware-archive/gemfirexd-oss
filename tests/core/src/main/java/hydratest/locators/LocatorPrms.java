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

package hydratest.locators;

import hydra.BasePrms;
import hydra.Log;

/**
 * A class used to store keys for hydra test settings.
 */

public class LocatorPrms extends BasePrms {

  public static Long expectedMembers;
  public static int getExpectedMembers() {
    Long key = expectedMembers;
    int val = tasktab().intAt(key, tab().intAt(key, 0));
    return val;
  }

  static {
    setValues(LocatorPrms.class);
  }

  public static void main(String args[]) {
    Log.createLogWriter("locatorprms", "info");
    dumpKeys();
  }
}
