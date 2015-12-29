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
package event; 

import hydra.*;
import hydra.blackboard.*;
//import java.util.*;
import util.*;
import admin.*;

public class RecycleClient extends EventTest {

  public static void recycleConnection() {

    // If we're connected, disconnect
    // Note that the disconnect is threadsafe per VM,
    // but not for multiple threads within the VM
    if (DistributedConnectionMgr.isConnected()) {
      // close the cache & disconnect from the distributed system
      CacheUtil.closeCache();
      CacheUtil.disconnect();
    }

    // Reconnect & re-open the cache
    // re-initialize the root regions using our singleton test instance
    eventTest.initialize();
   
    SharedCounters sc = AdminBB.getInstance().getSharedCounters();
    sc.increment( AdminBB.recycleRequests );
    AdminBB.getInstance().printSharedCounters();

    Log.getLogWriter().info("SimpleClient: recycleConnection() - getCache returns " + CacheUtil.getCache());
  }
}
