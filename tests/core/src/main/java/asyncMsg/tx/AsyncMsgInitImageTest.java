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
package asyncMsg.tx; 

import getInitialImage.InitImageTest;
import asyncMsg.AdminListener;
import asyncMsg.AsyncMsgTest;
import hydra.*;
import util.*;
import java.util.*;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.*;
//import com.gemstone.gemfire.cache.util.*;

public class AsyncMsgInitImageTest extends getInitialImage.InitImageTest {

/** Hydra task to load a region.
 */
public static void HydraTask_loadRegion() {
   try {
      InitImageTest.HydraTask_loadRegion();
   } catch (StopSchedulingTaskOnClientOrder e) {
      throw e;
   } catch (Exception e) {
      AsyncMsgTest.handlePossibleSlowReceiverException(e);
   }
}

/** Hydra task to load a region via gets from another region.
 */
public static void HydraTask_loadRegionWithGets() {
   try {
      InitImageTest.HydraTask_loadRegionWithGets();
   } catch (Exception e) {
      AsyncMsgTest.handlePossibleSlowReceiverException(e);
   } catch (TestException e) {
      if (e.toString().indexOf("Unexpected value null from getting key") >= 0) {
         AdminListener.waitForSlowReceiverAlert(60);
         throw new StopSchedulingOrder("a slow receiver alert was recognized");
      } else {
         throw e;
      }
   }
}

/** Hydra task to perform operations according to keyIntervals on an previously
 *  initialized region.
 */
public static void HydraTask_doOps() {
   try {
      InitImageTest.HydraTask_doOps();
   } catch (Exception e) {
      AsyncMsgTest.handlePossibleSlowReceiverException(e);
   } catch (TestException e) {
      String errStr = e.toString();
      if ((errStr.indexOf("Unexpected value null from getting key") >= 0) ||
          (errStr.indexOf("returned unexpected null") >= 0)) {
         AdminListener.waitForSlowReceiverAlert(60);
         throw new StopSchedulingOrder("a slow receiver alert was recognized");
      } else {
         throw e;
      }
   }
}

/** Hydra task to do a get region, which does a getInitialImage.
 */
public static void HydraTask_doGetInitImage() {
   try {
      InitImageTest.HydraTask_doGetInitImage();
   } catch (Exception e) {
      AsyncMsgTest.handlePossibleSlowReceiverException(e);
   }
}

/** Hydra task to wait until another thread does a getInitialImage, then this 
 *  get region should block.
 */
public static void HydraTask_blockedGetRegion() {
   try {
      InitImageTest.HydraTask_blockedGetRegion();
   } catch (Exception e) {
      AsyncMsgTest.handlePossibleSlowReceiverException(e);
   }
}

}
