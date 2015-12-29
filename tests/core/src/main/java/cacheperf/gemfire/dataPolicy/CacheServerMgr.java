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
package cacheperf.gemfire.dataPolicy;

import hydra.CacheServerHelper;

/**
 * Tasks used to start and stop a command-line cache server.
 */
public class CacheServerMgr {

  //----------------------------------------------------------------------------
  //  Tasks
  //----------------------------------------------------------------------------

  /**
   * Task to start a command-line cache server.
   */
  public static void startCacheServerTask() {
    String fn = "$JTESTS/cacheperf/gemfire/dataPolicy/cache.xml";
    String[] args = {"-J-Xmx1024m", "-J-Xms1024m"};
    CacheServerHelper.startCacheServer("cacheserver", fn, args);
  }

  /**
   * Task to stop a command-line cache server.
   */
  public static void stopCacheServerTask() {
    CacheServerHelper.stopCacheServer("cacheserver");
  }
}
