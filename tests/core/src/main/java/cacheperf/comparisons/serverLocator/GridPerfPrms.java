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
package cacheperf.comparisons.serverLocator;

import hydra.BasePrms;
import hydra.HydraVector;

/**
 * @author dsmith
 *
 */
public class GridPerfPrms extends BasePrms {

  public static Long serverGroups;
  public static String[] getServerGroups() {
    HydraVector groups = tasktab().vecAt(serverGroups, tab().vecAt(serverGroups, null));
    return groups == null ? null : (String[]) groups.toArray(new String[0]);
  }
  

  public static Long requestTimeout;
  public static int getRequestTimeout() {
    return tasktab().intAt(requestTimeout, tab().intAt(requestTimeout, 30000));
  }
  
  static {
    setValues( GridPerfPrms.class );
  }
}
