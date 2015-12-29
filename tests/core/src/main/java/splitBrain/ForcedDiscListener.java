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

/* Listener to recognize and validate a forced disconnect via the afterDestroyRegion
 * event, and cause slowness in an afterCreate event.
 */
public class ForcedDiscListener extends ControllerListener {

//================================================================================ 
// Event methods; All event methods are inherited by ControllerListener.
// We just want this listener to 
//    1) cause slowness in afterCreate, so override afterCreate to sleep
//    2) recognize a forced disconnect, so just inherit from ControllerListener

protected void doAfterCreate(EntryEvent event) {
   if (ControllerBB.isPlayDeadEnabled()) {
      SBUtil.playDead();
   }
   if (ControllerBB.isSlowListenerEnabled()) { // slow 
      logCall("afterCreate", event);
      Log.getLogWriter().info("Sleeping while slow listeners are enabled...");
      do {
         MasterController.sleepForMs(5000);
      } while (ControllerBB.isSlowListenerEnabled());
      Log.getLogWriter().info("Done being slow, returning from ForcedDiscListener.afterCreate");
   } else if (ControllerBB.isSicknessEnabled()) { // sick 
      logCall("afterCreate", event);
      SBUtil.beSick();
   }
}

}
