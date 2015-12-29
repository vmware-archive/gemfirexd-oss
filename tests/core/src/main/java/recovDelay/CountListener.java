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
package recovDelay; 

import com.gemstone.gemfire.cache.*;
import hydra.Log;

/** Listener to count events that occur in the current VM while
 *  recovery is running. 
 */
public class CountListener extends util.AbstractListener implements CacheListener, Declarable {

// each vm keeps a count of its entry events
public static int afterCreateCount_isRemote = 0; 
public static int afterCreatePutAllCount_isRemote = 0; 
public static int afterCreateCount_isNotRemote = 0; 
public static int afterCreatePutAllCount_isNotRemote = 0; 
public static int afterDestroyCount_isRemote = 0; 
public static int afterDestroyCount_isNotRemote = 0; 
public static int afterInvalidateCount_isRemote = 0; 
public static int afterInvalidateCount_isNotRemote = 0; 
public static int afterUpdateCount_isRemote = 0; 
public static int afterUpdateCount_isNotRemote = 0; 

//================================================================================ 
// event methods

public void afterCreate(EntryEvent event) { 
   logCall("afterCreate", event);
   if (EventPRObserver.recoveryInProgress) {
      Operation op = event.getOperation();
      if (event.isOriginRemote()) {
         if (op.isPutAll()) {
            afterCreatePutAllCount_isRemote++;
         } else {
            afterCreateCount_isRemote++;
         }
      } else {
         if (op.isPutAll()) {
            afterCreatePutAllCount_isNotRemote++;
         } else {
            afterCreateCount_isNotRemote++;
         }
      }
   }
}

public void afterDestroy(EntryEvent event) { 
   logCall("afterDestroy", event);
   if (EventPRObserver.recoveryInProgress) {
      if (event.isOriginRemote()) {
         afterDestroyCount_isRemote++;
      } else {
         afterDestroyCount_isNotRemote++;
      }
   }
}

public void afterInvalidate(EntryEvent event) { 
   logCall("afterInvalidate", event);
   if (EventPRObserver.recoveryInProgress) {
      if (event.isOriginRemote()) {
         afterInvalidateCount_isRemote++;
      } else {
         afterInvalidateCount_isNotRemote++;
      }
   }
} 

public void afterUpdate(EntryEvent event) { 
   logCall("afterUpdate", event);
   if (EventPRObserver.recoveryInProgress) {
      if (event.isOriginRemote()) {
         afterUpdateCount_isRemote++;
      } else {
         afterUpdateCount_isNotRemote++;
      }
   }
}

public void afterRegionCreate(RegionEvent event) { 
   Log.getLogWriter().info("In afterRegionCreate, " + event);
}
public void afterRegionDestroy(RegionEvent event) { }
public void afterRegionInvalidate(RegionEvent event) { }
public void afterRegionClear(RegionEvent event) { }
public void afterRegionLive(RegionEvent event) { }
public void init(java.util.Properties prop) { }
public void close() { }

/** Log the event counters.
 */
public static void logCounters() {
   StringBuffer aStr = new StringBuffer();
   aStr.append("CountListener.afterCreateCount_isRemote = "          + afterCreateCount_isRemote + "\n");
   aStr.append("CountListener.afterCreatePutAllCount_isRemote = "    + afterCreatePutAllCount_isRemote + "\n");
   aStr.append("CountListener.afterCreateCount_isNotRemote = "       + afterCreateCount_isNotRemote + "\n");
   aStr.append("CountListener.afterCreatePutAllCount_isNotRemote = " + afterCreatePutAllCount_isNotRemote + "\n");
   aStr.append("CountListener.afterDestroyCount_isRemote = "         + afterDestroyCount_isRemote + "\n");
   aStr.append("CountListener.afterDestroyCount_isNotRemote = "      + afterDestroyCount_isNotRemote + "\n");
   aStr.append("CountListener.afterInvalidateCount_isRemote = "      + afterInvalidateCount_isRemote + "\n");
   aStr.append("CountListener.afterInvalidateCount_isNotRemote = "   + afterInvalidateCount_isNotRemote + "\n");
   aStr.append("CountListener.afterUpdateCount_isRemote = "          + afterUpdateCount_isRemote + "\n");
   aStr.append("CountListener.afterUpdateCount_isNotRemote = "       + afterUpdateCount_isNotRemote + "\n");
   Log.getLogWriter().info(aStr.toString());
}

}
