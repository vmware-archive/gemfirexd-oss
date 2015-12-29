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
package cacheServer;

import hydra.*;
import java.util.Random;
import regions.validate.CacheXmlPrms;

/**
 * Tasks used to start and stop a command-line cache server.
 */
public class CacheServerMgr {

  private static final String CACHE_SERVER_NAME = "cacheserver";

  //----------------------------------------------------------------------------
  //  Tasks
  //----------------------------------------------------------------------------

  /**
   * Task to start {@link CacheServerPrms#nServers} command-line cache servers.
   */
  public static void startCacheServers() {
    String xml = TestConfig.tab().stringAt(CacheXmlPrms.cacheXmlFile);
    int n = TestConfig.tab().intAt(CacheServerPrms.nServers);
    for (int i = 0; i < n; i++) {
      String cacheserver = CACHE_SERVER_NAME + i;
      CacheServerHelper.startCacheServer(cacheserver, xml);
    }
  }

  /**
   * Task to bounce a random cache server by killing and restarting it.
   */
  public synchronized static void bounceCacheServer() {
    // select a random cache server by name
    int n = TestConfig.tab().intAt(CacheServerPrms.nServers);
    int i = (new Random()).nextInt(n);
    String cacheserver = CACHE_SERVER_NAME + i;
    Log.getLogWriter().info("Bouncing " + cacheserver);

    // bounce it
    String xml = TestConfig.tab().stringAt(CacheXmlPrms.cacheXmlFile);
    CacheServerHelper.killCacheServer(cacheserver);
    int seconds = 60;
    Log.getLogWriter().info("Sleeping " + seconds + " seconds");
    MasterController.sleepForMs(seconds * 1000);
    CacheServerHelper.startCacheServer(cacheserver, xml);
    Log.getLogWriter().info("Bounced " + cacheserver);
  }

  /**
   * Task to stop {@link CacheServerPrms#nServers} command-line cache servers.
   */
  public static void stopCacheServers() {
    int n = TestConfig.tab().intAt(CacheServerPrms.nServers);
    for (int i = 0; i < n; i++) {
      String cacheserver = CACHE_SERVER_NAME + i;
      CacheServerHelper.stopCacheServer(cacheserver);
    }
  }
}
