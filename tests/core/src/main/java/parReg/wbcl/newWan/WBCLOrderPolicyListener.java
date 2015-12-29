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
package parReg.wbcl.newWan;

import hydra.CacheHelper;
import hydra.Log;
import hydra.TestConfig;

import java.util.Iterator;
import java.util.List;

import parReg.wbcl.WBCLTestBB;
import util.TestHelper;

import com.gemstone.gemfire.cache.EntryExistsException;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;

public class WBCLOrderPolicyListener extends MyAsyncEventListener{
  
  /**
   * Counts events based on operation type
   */
  public boolean processEvents(List<AsyncEvent<Object, Object>> events) {
    boolean status = false;
    Log.getLogWriter().info("processEvents received List with " + events.size() + " GatewayEvents");
    // Fail 10% of the time ... ensure that we replay these events
    if (TestConfig.tab().getRandGen().nextInt(1,100) < 99) { 
      status = true;    
      for (Iterator i = events.iterator(); i.hasNext();) {
        AsyncEvent event = (AsyncEvent)i.next();
        try {
          logCall("processEvents", event);
          WBCLTestBB.getBB().getSharedCounters().setIfLarger(WBCLTestBB.lastEventTime, System.currentTimeMillis());

          // use the event to update the local wbcl region
          final Region wbclRegion = CacheHelper.getCache().getRegion("wbclRegion");
          final Object key = event.getKey();
          final Object value = event.getDeserializedValue();
          final Operation op = event.getOperation();
          final Object callback = event.getCallbackArgument();

              if (op.isCreate()) {
                try {                  
                  wbclRegion.create(key, value, callback);                  
                } catch (EntryExistsException e) {
                   Log.getLogWriter().info("Caught " + e + ", expected with concurrent operations; continuing with test");
                }
              } else if (op.isUpdate()) {                
                wbclRegion.put(key, value, callback);                
              } else if (op.isInvalidate()) {
                throwException("Unexpected INVALIDATE encounted in WBCLEventListener " + op.toString() + ", " + TestHelper.getStackTrace());
              } else if (op.isDestroy()) {                
                try {
                   wbclRegion.destroy(key, callback);
                } catch (EntryNotFoundException e) {
                   Log.getLogWriter().info("Caught " + e + ", expected with concurrent operations; continuing with test");
                }                
              }
        } catch (Exception e) {
          status = false;
          throwException("WBCL Listener caught unexpected Exception " + e + ", " + TestHelper.getStackTrace(e));
        }
      }
    } 
    if (status) {
      Log.getLogWriter().info("WBCLEventListener processed batch of " + events.size() + " events, returning " + status);
    } else {
      Log.getLogWriter().info("WBCLEventListener DID NOT process batch of " + events.size() + " events, returning " + status);
    }
    return status;
  }
  
}
