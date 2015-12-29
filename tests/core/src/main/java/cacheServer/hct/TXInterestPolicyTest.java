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
package cacheServer.hct;

import hydra.*;
import hct.*;

import getInitialImage.InitImageBB;
import getInitialImage.InitImagePrms;
import getInitialImage.InitImageTest;
import util.KeyIntervals;

public class TXInterestPolicyTest extends InterestPolicyTest {

// ======================================================================== 
// initialization tasks/methods 

/** Initialize the known keys for this test
 *  edge clients support transactional invalidates, but not local invalidates or local destroys.
 */
public static void StartTask_initialize() {
   // initialize keyIntervals
   int numKeys = TestConfig.tab().intAt(InitImagePrms.numKeys);
   intervals = new KeyIntervals(
      new int[] {KeyIntervals.NONE, KeyIntervals.INVALIDATE,
                 KeyIntervals.DESTROY, KeyIntervals.UPDATE_EXISTING_KEY,
                 KeyIntervals.GET}, 
                 numKeys);
   InitImageBB.getBB().getSharedMap().put(InitImageBB.KEY_INTERVALS, intervals);
   Log.getLogWriter().info("Created keyIntervals: " + intervals);

   // Set the counters for the next keys to use for each operation
   hydra.blackboard.SharedCounters sc = InitImageBB.getBB().getSharedCounters();
   sc.setIfLarger(InitImageBB.LASTKEY_INVALIDATE, intervals.getFirstKey(KeyIntervals.INVALIDATE)-1);
   sc.setIfLarger(InitImageBB.LASTKEY_DESTROY, intervals.getFirstKey(KeyIntervals.DESTROY)-1);
   sc.setIfLarger(InitImageBB.LASTKEY_UPDATE_EXISTING_KEY, intervals.getFirstKey(KeyIntervals.UPDATE_EXISTING_KEY)-1);
   sc.setIfLarger(InitImageBB.LASTKEY_GET, intervals.getFirstKey(KeyIntervals.GET)-1);
   sc.setIfLarger(InitImageBB.LASTKEY_LOCAL_INVALIDATE, intervals.getFirstKey(KeyIntervals.LOCAL_INVALIDATE)-1);
   sc.setIfLarger(InitImageBB.LASTKEY_LOCAL_DESTROY, intervals.getFirstKey(KeyIntervals.LOCAL_DESTROY)-1);
   
   // for failovertests
    BBoard.getInstance().getSharedMap().put("lastKillTime", new Long(0));

   // show the blackboard
   InitImageBB.getBB().printSharedMap();
   InitImageBB.getBB().printSharedCounters();
}

}
