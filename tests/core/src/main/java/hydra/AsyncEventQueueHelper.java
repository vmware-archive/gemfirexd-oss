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

package hydra;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueueFactory;

import java.util.*;

/**
 * Provides support for async event queues running in hydra-managed client VMs.
 */
public class AsyncEventQueueHelper {

  private static LogWriter log = Log.getLogWriter();

//------------------------------------------------------------------------------
// AsyncEventQueue
//------------------------------------------------------------------------------

  /**
   * Creates and starts an async event queue in the current cache. The queue is
   * configured using the {@link AsyncEventQueueDescription} corresponding to
   * the given configuration from {@link AsyncEventQueuePrms#names}. The id for
   * the queue is the same as the queue configuration name.
   * <p>
   * This method is thread-safe.  The async event queues for a given logical
   * queue configuration will only be created once.
   *
   * @throws HydraRuntimeException if an attempt is made to reconfigure an
   *         existing async event queue.
   */
  public static synchronized AsyncEventQueue createAndStartAsyncEventQueue(
                                             String asyncEventQueueConfig) {
    AsyncEventQueue queue = getAsyncEventQueue(asyncEventQueueConfig);
    if (queue == null) {

      // get the cache
      Cache cache = CacheHelper.getCache();
      if (cache == null) {
        String s = "Cache has not been created yet";
        throw new HydraRuntimeException(s);
      }

      // look up the async event queue configuration
      AsyncEventQueueDescription aeqd =
           getAsyncEventQueueDescription(asyncEventQueueConfig);

      // create the disk store
      DiskStoreHelper.createDiskStore(aeqd.getDiskStoreDescription().getName());

      // create the async event queue
      queue = createAsyncEventQueue(aeqd, cache);
    }
    return queue;
  }

  private static AsyncEventQueue createAsyncEventQueue(
                 AsyncEventQueueDescription aeqd, Cache cache) {
    // configure the factory
    AsyncEventQueueFactory f = cache.createAsyncEventQueueFactory();
    log.info("Configuring async event queue factory");
    aeqd.configure(f);
    log.info("Configured async event queue factory " + f);

    // create the async event queue
    log.info("Creating and starting async event queue " + aeqd.getName());
    AsyncEventQueue queue = f.create(aeqd.getName(),
                                     aeqd.getAsyncEventListenerInstance());
    log.info("Created and started async event queue: "
                                  + asyncEventQueueToString(queue));
    return queue;
  }

  /**
   * Returns the async event queues for the current cache, or null if no async
   * event queues or cache exists.
   */
  public static Set<AsyncEventQueue> getAsyncEventQueues() {
    Cache cache = CacheHelper.getCache();
    if (cache == null) {
      return null;
    } else {
      Set<AsyncEventQueue> queues = cache.getAsyncEventQueues();
      return (queues == null || queues.size() == 0) ? null : queues;
    }
  }

  /**
   * Returns the async event queue for the current cache matching the given
   * async event queue id, or null if no matching id exists.
   */
  public static AsyncEventQueue getAsyncEventQueue(String queueID) {
    Set<AsyncEventQueue> queues = getAsyncEventQueues();
    if (queues != null){
      for (AsyncEventQueue queue : queues){
        if (queue.getId().equals(queueID)){
          return queue;
        }
      }
    }
    return null;
  }
  
  /**
   * Returns the given async event queue as a string.
   */
  public static String asyncEventQueueToString(AsyncEventQueue aeq) {
    return AsyncEventQueueDescription.asyncEventQueueToString(aeq);
  }

//------------------------------------------------------------------------------
// AsyncEventQueueDescription
//------------------------------------------------------------------------------

  /**
   * Returns the {@link AsyncEventQueueDescription} with the given configuration
   * name from {@link AsyncEventQueuePrms#names}.
   */
  public static AsyncEventQueueDescription getAsyncEventQueueDescription(
                                         String asyncEventQueueConfig) {
    if (asyncEventQueueConfig == null) {
      throw new IllegalArgumentException("asyncEventQueueConfig cannot be null");
    }
    log.info("Looking up async event queue config: " + asyncEventQueueConfig);
    AsyncEventQueueDescription aeqd = TestConfig.getInstance()
        .getAsyncEventQueueDescription(asyncEventQueueConfig);
    if (aeqd == null) {
      String s = asyncEventQueueConfig + " not found in "
               + BasePrms.nameForKey(AsyncEventQueuePrms.names);
      throw new HydraRuntimeException(s);
    }
    log.info("Looked up async event queue config:\n" + aeqd);
    return aeqd;
  }
}
