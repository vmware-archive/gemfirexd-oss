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
package cacheServer.wan;

import java.util.*;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.cache.execute.*;

import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.internal.cache.GatewayStats;
import com.gemstone.gemfire.SystemFailure;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Declarable;

import hydra.*;
import hydra.blackboard.*;

import util.*;
import wan.*;


/** QueueMonitor Listener
 *  Spawn a long-lived thread to monitor the GatewayStatistics.eventQueueSize stat
 *  and post the number of events queued to the BB.
 *
 * @author Lynn Hughes
 * @since 7.0
 */
public class QueueMonitor extends CacheListenerAdapter implements CacheListener, Declarable {

/**  Stub reference to a remote master controller */
public static MasterProxyIF Master = null;
protected static LogWriter log = null;
protected static String logName;

  static {

    // initialization in CacheServer VM
    // Open a LogWriter
    try {
       Log.getLogWriter();
    } catch (HydraRuntimeException e) {
       // create the task log
       logName = "cacheserver_" + ProcessMgr.getProcessId();
       log = Log.createLogWriter( logName, "info" );
    }

    // We need to set user.dir (for access to TestConfig)
    // RMI lookup of Master (for Blackboard access), relies on TestConfig
    String cacheServerDir = System.getProperty("user.dir");
    String testDir = FileUtil.pathFor(cacheServerDir);
    System.setProperty("test.dir", testDir);

    if ( Master == null ) {
       Master = RmiRegistryHelper.lookupMaster();
    }
  
    // start thread to monitor GatewayQueueSize (GatewayStatistics.eventQueueSize-GatewayStatistics)
    final String key = "EVENT_QUEUE_SIZE:" + logName;
    Log.getLogWriter().info("Started event queue monitor with key: " + key);
    final SharedMap bb = WANBlackboard.getInstance().getSharedMap();
    Thread queueMonitor = new Thread(new Runnable() {

      public void run() {
        StatisticsFactory statFactory = null;
        Statistics[] gStats = null;
        while (true) {
          if (statFactory == null) {
            statFactory = CacheFactory.getAnyInstance().getDistributedSystem();
          }
          if (gStats == null) {
            gStats = statFactory.findStatisticsByType(statFactory.findType(GatewayStats.typeName));
          }
          SystemFailure.checkFailure(); 
          long qSize = 0;
          for(int i=0;i<gStats.length;i++) {
            long gQSize = gStats[i].getInt(GatewayStats.getEventQueueSizeId());
            qSize+=gQSize;
          }
          try {
            bb.put(key, new Long(qSize));
          } catch(Exception e) {
            e.printStackTrace();
          }
          try {
            Thread.sleep(1000);
          } catch(Exception e) {
            e.printStackTrace();
          }
        }
      }

      protected void finalize() throws Throwable {
        Object o = bb.remove(key);
        Log.getLogWriter().severe("REMOVING ["+key+"] from Blackboard, value was:"+o);
        super.finalize();
      }

    });
    queueMonitor.setDaemon(true);
    queueMonitor.start();
  }

  public void init(java.util.Properties prop) {
  }
}
