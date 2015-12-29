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
package hct.ha;

import hydra.Log;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;

/**
 * This class is a <code>CacheListener</code> implementation attached to the
 * cache-clients for test validations. It just signals the validations that the
 * last_key expected at the client has been received and validation can proceed
 * further.
 * 
 */
public class HAClientListener extends CacheListenerAdapter
{

  public void afterCreate(EntryEvent event)
  {
    Validator.createCount++;
    String key = (String)event.getKey();
    if (key.equals(Feeder.LAST_KEY)) {
      HAClientQueue.lastKeyReceived = true;
      Log.getLogWriter().info("'last_key' received at client");
    }
  }
}
