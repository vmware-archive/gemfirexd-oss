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

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.DistributedSystem;
import hydra.*;
import hydra.blackboard.*;
import util.*;

import java.util.*;

/* Listener used with concParRegHA flavor tests.
 */
public class ParRegListener extends ControllerListener {

/** Reinitialize after a forced disconnect.
 */
public void initAfterDisconnect() {
   DistributedSystemHelper.disconnect(); // let hydra know we disconnected, so it will work on the reconnect
   if (RemoteTestModule.getMyClientName().indexOf("accessor") >= 0) {
      Log.getLogWriter().info("Calling SBParRegTest.initAccessorAfterForcedDisconnect()");
      SBParRegTest.initAccessorAfterForcedDisconnect();
      Log.getLogWriter().info("Done calling SBParRegTest.initAccessorAfterForcedDisconnect()");
   } else {
      Log.getLogWriter().info("Calling SBParRegTest.initDataStoreAfterForcedDisconnect()");
      SBParRegTest.initDataStoreAfterForcedDisconnect();
      Log.getLogWriter().info("Done calling SBParRegTest.initDataStoreAfterForcedDisconnect()");
   }
}

/** After a disconnect in concurrent tests, wait for other threads to get exceptions
 */
public void afterDisconnect() {
   if (!parReg.ParRegTest.testInstance.isSerialExecution) {
      // wait for all threads to get exceptions
      Log.getLogWriter().info("Waiting for all threads in this vm to get exceptions from the forced disconnect");
      int numThreadsInThisVM = (Integer.getInteger("numThreads")).intValue();
      TestHelper.waitForCounter(ControllerBB.getBB(), 
                                "ControllerBB.ExceptionCounter", 
                                ControllerBB.ExceptionCounter, 
                                numThreadsInThisVM,
                                true, 
                                -1,
                                1000);
   }
}

}
