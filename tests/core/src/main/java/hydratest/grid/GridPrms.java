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

package hydratest.grid;

import hydra.AbstractDescription;
import hydra.BasePrms;
import hydra.Log;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A class used to store keys for hydra test settings.
 */

public class GridPrms extends BasePrms {

  public static Long poolConfigs;
  public static List getPoolConfigs() {
    Long key = poolConfigs;
    List val = (List)tasktab().vecAt(key, tab().vecAt(key, null));
    return val;
  }

  public static Long regionConfigs;
  public static List getRegionConfigs() {
    Long key = regionConfigs;
    List val = (List)tasktab().vecAt(key, tab().vecAt(key, null));
    return val;
  }

  public static Long functions;
  public static List getFunctions() {
    Long key = functions;
    List vals = (List)tasktab().vecAt(key, tab().vecAt(key, null));
    if (vals != null) {
      List fcns = new ArrayList();
      for (Iterator i = vals.iterator(); i.hasNext();) {
        String val = (String)i.next();
        fcns.add(AbstractDescription.getInstance(key, val));
      }
      return fcns;
    }
    return null;
  }

  static {
    setValues(GridPrms.class);
  }

  public static void main(String args[]) {
    Log.createLogWriter("gridprms", "info");
    dumpKeys();
  }
}
