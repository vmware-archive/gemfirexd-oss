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

import java.util.*;
import util.*;
import hydra.*;
import com.gemstone.gemfire.cache.*;

public class ShadowListener extends util.AbstractListener implements CacheListener, Declarable {

protected boolean isCarefulValidation;
private boolean useTransactions;
                                                                                                  
/** noArg constructor which sets isCarefulValidation based on the tests
 *  setting of serialExecution
 */
public ShadowListener() {
   TestConfig config = TestConfig.getInstance();
   ConfigHashtable tab = config.getParameters();
   this.isCarefulValidation = tab.booleanAt(Prms.serialExecution);
   this.useTransactions = tab.booleanAt(EventPrms.useTransactions, false);
}

/** Create a new listener and specify whether the test is doing careful validation.
 *
 *  @param isCarefulValidation true if the test is doing careful validate (serial test)
 *         false otherwise.
 */
public ShadowListener(boolean isCarefulValidation) {
   this.isCarefulValidation = isCarefulValidation;
}

/**
 *  Implementation of CacheListener interface 
 */
public void afterCreate(EntryEvent event) {
   logCall("afterCreate", event);

   // We may be backed up processing listener events, check if the cache is closed
   // before moving forward
   if (CacheUtil.getCache().isClosed()) {
      return;
   }

   final Region shadow = CacheUtil.getCache().getRegion(ProxyEventTest.eventTest.shadowRegionName);
   final Object key = event.getKey();
   final Object val = event.getNewValue();

   String threadName = "ShadowListener_CreateThread";
   Thread workThread = new Thread(new Runnable() {
     public void run() {
       Log.getLogWriter().info("Creating key/value pair (" + key + ", " + val + ") in region named " + shadow.getName());
       try {
          shadow.create(key, val);
       } catch (com.gemstone.gemfire.cache.CacheClosedException cce) {
          // ignore
       } catch (com.gemstone.gemfire.cache.RegionDestroyedException e) {
          if (isCarefulValidation) {
             throw new TestException(TestHelper.getStackTrace(e));
          } else {
              Log.getLogWriter().info("ShadowListener: afterCreate() caught exception " + e + " while creating ShadowRegion (expected with concurrent execution); continuing with test");
          }
       } catch (com.gemstone.gemfire.cache.EntryExistsException e) {
          if (isCarefulValidation) {
             throw new TestException(TestHelper.getStackTrace(e));
          } else {
             Log.getLogWriter().info("ShadowListener: afterCreate() caught exception " + e + "while creating entry in ShadowRegion (expected with concurrent execution); continuing with test");
          }
       }

       Log.getLogWriter().info("Done Creating key/value pair (" + key + ", " + val + ") in region named " + shadow.getName());
     }
   }, threadName);
   workThread.start();
}

public void afterDestroy(EntryEvent event) {
   logCall("afterDestroy", event);

   // We may be backed up processing listener events, check if the cache is closed
   // before moving forward
   if (CacheUtil.getCache().isClosed()) {
      return;
   }

   final Region shadow = CacheUtil.getCache().getRegion(ProxyEventTest.eventTest.shadowRegionName);
   final Object key = event.getKey();
   final Operation op = event.getOperation();

   String threadName = "ShadowListener_DestroyThread"; 
   Thread workThread = new Thread(new Runnable() {
     public void run() {
       Log.getLogWriter().info("Destroying key (" + key + ") in region named " + shadow.getName());
       try {
          if (op.equals(Operation.LOCAL_DESTROY)) {
             shadow.localDestroy(key);
          } else {
             shadow.destroy(key);
          }
       } catch (com.gemstone.gemfire.cache.CacheClosedException cce) {
          // ignore
       } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
          if (isCarefulValidation)
              throw new TestException(TestHelper.getStackTrace(e));
          else {
              Log.getLogWriter().info("ShadowListener: afterDestroy() caught exception " + e + " (expected with concurrent execution); continuing with test");
              return;
          }
       }
       Log.getLogWriter().info("Done Destroying key(" + key + ") in region named " + shadow.getName());
     }
   }, threadName);
   workThread.start();
}

public void afterInvalidate(EntryEvent event) {
   logCall("afterInvalidate", event);

   // We may be backed up processing listener events, check if the cache is closed
   // before moving forward
   if (CacheUtil.getCache().isClosed()) {
      return;
   }

   final Region shadow = CacheUtil.getCache().getRegion(ProxyEventTest.eventTest.shadowRegionName);
   final Object key = event.getKey();
   final Operation op = event.getOperation();

   String threadName = "ShadowListener_InvalidateThread";
   Thread workThread = new Thread(new Runnable() {
     public void run() {
       Log.getLogWriter().info("Invalidating key(" + key + ") in region named " + shadow.getName());
       try {
          if (op.equals(Operation.LOCAL_INVALIDATE)) {
             shadow.localInvalidate(key);
          } else {
             shadow.invalidate(key);
          }
       } catch (com.gemstone.gemfire.cache.CacheClosedException cce) {
          // ignore
       } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
          if (isCarefulValidation)
              throw new TestException(TestHelper.getStackTrace(e));
          else {
            Log.getLogWriter().info("ShadowListener: afterInvalidate caught " + e + " (expected with concurrent execution); continuing with test");
              return;
          }
       }
       Log.getLogWriter().info("Done Invalidating key(" + key + ") in region named " + shadow.getName());
     }
   }, threadName);
   workThread.start();
}

public void afterUpdate(EntryEvent event) {
   logCall("afterUpdate", event);

   // We may be backed up processing listener events, check if the cache is closed
   // before moving forward
   if (CacheUtil.getCache().isClosed()) {
      return;
   }

   final Region shadow = CacheUtil.getCache().getRegion(ProxyEventTest.eventTest.shadowRegionName);
   final Object key = event.getKey();
   final Object val = event.getNewValue();

   String threadName = "ShadowListener_UpdateThread";
   Thread workThread = new Thread(new Runnable() {
     public void run() {

       if (val != null) {
         Log.getLogWriter().info("Updating key/value pair (" + key + ", " + val + ") in region named " + shadow.getName());
         try {
           shadow.put(key, val);
           Log.getLogWriter().info("Done Updating key/value pair (" + key + ", " + val + ") in region named " + shadow.getName());
         } catch (com.gemstone.gemfire.cache.CacheClosedException cce) {
             // ignore -- the Listener likely got behind and is still processing events
         }
       } else {
           Log.getLogWriter().info("Invalidating key/value pair (" + key + ", " + val + ") in region named " + shadow.getName());
         try {
           shadow.invalidate(key);
           Log.getLogWriter().info("Done Invalidating key/value pair (" + key + ", " + val + ") in region named " + shadow.getName());
         } catch (com.gemstone.gemfire.cache.CacheClosedException cce) {
             // ignore
         } catch (com.gemstone.gemfire.cache.EntryNotFoundException e) {
             if (isCarefulValidation) {
               throw new TestException(TestHelper.getStackTrace(e));
             } else {
               Log.getLogWriter().info("ShadowListener: afterUpdate (invalidate) caught " + e + " (expected with concurrent execution); continuing with test");
             }
         }
       }
     }
   }, threadName);
   workThread.start();
}

public void afterRegionCreate(RegionEvent event) {
  logCall("afterRegionCreate", event);
}

public void afterRegionDestroy(RegionEvent event) {
   logCall("afterRegionDestroy", event);
}

public void afterRegionInvalidate(RegionEvent event) {
   logCall("afterRegionInvalidate", event);
}

public void afterRegionLive(RegionEvent event) {
  logCall("afterRegionLive", event);
}

public void afterRegionClear(RegionEvent event) {
   logCall("afterRegionClear", event);

   // We may be backed up processing listener events, check if the cache is closed
   // before moving forward
   if (CacheUtil.getCache().isClosed()) {
      return;
   }

   final Region shadow = CacheUtil.getCache().getRegion(ProxyEventTest.eventTest.shadowRegionName);

   String threadName = "ShadowListener_ClearRegionThread";
   Thread workThread = new Thread(new Runnable() {
      public void run() {
         try {
            ((Map)shadow).clear();
         } catch (com.gemstone.gemfire.cache.CacheClosedException cce) {
            // ignore
         } catch (RegionDestroyedException e) {
             if (isCarefulValidation) 
                throw new TestException(TestHelper.getStackTrace(e));
         } 
      }
  }, threadName);
  workThread.start();
}

public void close() {
   logCall("close", null);
}

public void init(java.util.Properties prop) {
   logCall("init(Properties)", null);
}

}
