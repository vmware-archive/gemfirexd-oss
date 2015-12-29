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
package cacheperf.comparisons.newWan;

import hydra.BasePrms;

public class NewWanPerfPrms extends BasePrms{

  /**
   * (int)
   * Input throughput i.e. puts per seconds. Default to 100.   
   */
  public static Long inputPutsPerSec;
  public static int getInputPutsPerSec() {
    Long key = inputPutsPerSec;
    return tasktab().intAt(key, tab().intAt(key, 100));
  }
  
  static {
    setValues(NewWanPerfPrms.class);
  }
}
