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
package splitBrain;

import com.gemstone.gemfire.cache.DynamicRegionListener;
import com.gemstone.gemfire.cache.RegionEvent;

class MLRioDynamicRegionListener implements DynamicRegionListener {

  public void afterRegionCreate(RegionEvent<?, ?> event) {
    MLRioBB.getBB().getSharedCounters().increment(MLRioBB.dynamicRegionCreates);
  }

  public void afterRegionDestroy(RegionEvent<?, ?> event) {
  }

  public void beforeRegionCreate(String parentRegionName, String regionName) {
  }

  public void beforeRegionDestroy(RegionEvent<?, ?> event) {
  }
  
}