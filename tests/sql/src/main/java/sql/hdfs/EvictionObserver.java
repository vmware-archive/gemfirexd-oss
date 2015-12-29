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
package sql.hdfs;

import hydra.Log;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.pivotal.gemfirexd.internal.engine.ddl.GfxdEvictionCriteria.Observer;

public class EvictionObserver implements Observer {

  @Override
  public void keyReturnedForEviction(Object key, Object routingObject,
      long currentMillis, Region<Object, Object> region) {
    Log.getLogWriter().info("EvictionObserver - keyReturnedForEviction for region " );
  }

  @Override
  public void onIterationComplete(Region<Object, Object> region) {
    long evictionCount = HDFSSqlBB.getBB().getSharedCounters().incrementAndRead(HDFSSqlBB.evictionCount);        
    Log.getLogWriter().info("EvictionObserver - onIterationComplete for region " + region.getFullPath() + " evictionCount=" + evictionCount);
  }

  @Override
  public void onDoEvictCompare(EntryEventImpl event) {
    Log.getLogWriter().info("EvictionObserver - onDoEvictCompare for region " );
  }

}
