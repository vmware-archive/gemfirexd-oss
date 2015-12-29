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
package util;

import java.io.Serializable;
import com.gemstone.gemfire.cache.CacheEvent;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.RegionEvent;

import hydra.Log;

/** Listener class to log a summary of each event, rather than a lot of logging
 *
 * @author lynn
 *
 */
public class SummaryLogListener extends SilenceListener implements CacheListener, Declarable, Serializable {
  
  /* (non-Javadoc)
   * @see util.AbstractListener#logCall(java.lang.String, com.gemstone.gemfire.cache.CacheEvent)
   */
  public String logCall(String methodName, CacheEvent event) {
    String aStr = null;
    if (event instanceof EntryEvent) {
      aStr = "Invoked " + this.getClass().getName() + " for key " + ((EntryEvent)event).getKey() +
          ": in " + event.getRegion().getFullPath() + " " + methodName  + 
          ", oldValue: " + ((EntryEvent)event).getOldValue() + ", newValue: " + ((EntryEvent)event).getNewValue() + 
          ", event: " + event + "\n";
    } else if (event instanceof RegionEvent) {
      aStr = "Invoked " + this.getClass().getName() + ": " + methodName + 
          " for " + event.getRegion().getFullPath() + "\n";
    } else {
      aStr = "Invoked " + this.getClass().getName() + ": " + methodName + ", event is " + event;
    }
    Log.getLogWriter().info(aStr);
    return aStr;
  }
}
