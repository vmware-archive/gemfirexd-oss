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
package query.remote;

import hydra.Log;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A cache listener which spwans a new query executor thread from 
 * QueryExecutorPool in afterDestroy call back 
 * 
 * @author Yogesh Mahajan
 *
 */
public class RemoteQueryListener extends CacheListenerAdapter
{

  private static final int MAX_THREADS = 16;

  private volatile boolean isClosed = false;

  private ThreadPoolExecutor queryExecutorPool = null;

  public RemoteQueryListener() {
    final ThreadGroup queryThreadGroup = new ThreadGroup(
        "Query Executor Thread Group");
    ThreadFactory queryThreadFactory = new ThreadFactory() {
      public Thread newThread(Runnable command)
      {
        return new Thread(queryThreadGroup, command, queryThreadGroup.getName()
            + " Query Executor Thread ");
      }
    };
    queryExecutorPool = new ThreadPoolExecutor(MAX_THREADS, MAX_THREADS, 15,
        TimeUnit.SECONDS, new LinkedBlockingQueue(), queryThreadFactory);

  }

  public void afterDestroy(EntryEvent event)
  {
    if (!isClosed) {
      QueryExecutorThread queryExecutor = new QueryExecutorThread(event);
      this.queryExecutorPool.execute(queryExecutor);
    }
    else {
      // gracefull shutdown 
      this.queryExecutorPool.shutdownNow();
    }
  }

  public void close()
  {
    Log.getLogWriter().info("RemoteQueryListener Closing ...");
    this.isClosed = true;
  }
}
