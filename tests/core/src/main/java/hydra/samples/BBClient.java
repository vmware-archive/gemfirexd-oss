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

package hydra.samples;

import com.gemstone.gemfire.LogWriter;

import hydra.*;
import hydra.blackboard.*;

import java.util.*;

/**
*
* A sample client that uses a blackboard.
*
*/

public class BBClient {

  public static void worktask()
  {
    BBClient c = new BBClient();
    c.work();
  }
  private void work()
  {
    SampleBlackboard blackboard = SampleBlackboard.getInstance();

    // increment a counter
    SharedCounters counters = blackboard.getSharedCounters();
    counters.increment( SampleBlackboard.MiscInt );

    // put an entry in the map, using a unique key obtained from a shared counter
    SharedMap map = blackboard.getSharedMap();
    long uid = counters.incrementAndRead( SampleBlackboard.NextUniqueID );
    int tid = RemoteTestModule.getCurrentThread().getThreadId();
    String client = "client" + tid;
    map.put( new Integer( tid ), client );
    HashMap frip = new HashMap();
    frip.put( client, new Integer( 1000 * tid ) );
    map.put( client, frip );
    log().info( "MAP: Put " +  uid + "=" + client );
  }
  public static void printtask() {
    SampleBlackboard blackboard = SampleBlackboard.getInstance();
    blackboard.print();
  }
  private LogWriter log() {
    return Log.getLogWriter();
  }
}
