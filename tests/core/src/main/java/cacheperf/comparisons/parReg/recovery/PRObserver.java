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

package cacheperf.comparisons.parReg.recovery;

import cacheperf.TaskSyncBlackboard;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager;
import com.gemstone.gemfire.internal.cache.control.InternalResourceManager.ResourceObserverAdapter;
import hydra.Log;

public class PRObserver extends ResourceObserverAdapter {

  private long start = -1;

  public void rebalancingOrRecoveryStarted(Region region) {
    start = NanoTimer.getTime();
    Log.getLogWriter().info("Starting recovery at " + start);
  }

  public void rebalancingOrRecoveryFinished(Region region) {
    long end = NanoTimer.getTime();
    long elapsed = end - start;
    TaskSyncBlackboard.getInstance().getSharedMap()
           .put(TaskSyncBlackboard.RECOVERY_KEY, Long.valueOf(elapsed));
    Log.getLogWriter().info("Recovered at " + end + ", in " + elapsed + " ns");
  }

  public static void installObserverHook() {
   InternalResourceManager.setResourceObserver(new PRObserver());
   Log.getLogWriter().info("Installed PRObserver");
 }
}
