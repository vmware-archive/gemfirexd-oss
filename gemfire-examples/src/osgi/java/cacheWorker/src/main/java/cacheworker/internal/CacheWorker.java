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

package cacheworker.internal;

import java.io.IOException;
import java.net.URL;
import java.util.Collection;
import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;

import com.gemstone.gemfire.cache.query.QueryException;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;

/**
 * Starting the CacheWorker creates a GemFire Cache and launches a thread
 * which performs caching operations. Stopping it will stop the thread and
 * close the Cache.
 * <p>
 * Every 30 seconds, the internal worker thread will wake up and put 10
 * cache entries in the exampleRegion. Since the exampleRegion is defined
 * in the cache.xml as having an expiration of 10 seconds, these entries
 * will then be destroyed 10 seconds later. This continues for as long as
 * the CacheWorker is running.
 * <p>
 * After performing the cache puts, the internal worker thread will also 
 * perform a query and print the results before sleeping.
 *
 * @author Pivotal Software, Inc.
 * @since 6.6
 */
public class CacheWorker {

  /** The GemFire Cache to use for this worker to use. */
  Cache cache;

  /** Controls internal thread which performs operations on exampleRegion. */
  private Runner runner;

  /**
   * Starts the CacheWorker which creates a GemFire Cache and launches a thread
   * to perform caching operations.
   */
  public void start() throws IOException {
    URL propsUrl = getClass().getClassLoader().getResource("gemfire.properties");
    Properties properties = new Properties();
    properties.load(propsUrl.openStream());

    System.out.println("Loaded properties: " + properties);

    this.cache = new CacheFactory(properties).create();

    URL xmlUrl = getClass().getClassLoader().getResource("cache.xml");
    this.cache.loadCacheXml(xmlUrl.openStream());

    System.out.println("Starting CacheWorker thread...");

    this.runner = new Runner();
    this.runner.startThread();
  }

  /**
   * Stops the CacheWorker thread and closes the Cache.
   */
  public void stop() throws InterruptedException {
    this.runner.stopThread();
    this.cache.close();
    this.cache = null;
  }

  /**
   * Returns formatted query results for printing.
   */
  static String formatQueryResult(Object result) {
    if (result == null) {
     return "null";
    }
    else if (result == QueryService.UNDEFINED) {
      return "UNDEFINED";
    }
    if (result instanceof SelectResults) {
      Collection<?> collection = ((SelectResults<?>)result).asList();
      StringBuffer sb = new StringBuffer();
      for (Object e: collection) {
        sb.append(e + "\n\t");
      }
      return sb.toString();
    }
    else {
      return result.toString();
    }
  }

  /**
   * Starts and stops a thread which performs operations on exampleRegion.
   */
  class Runner implements Runnable {

    /** Internal thread to create and use. */
    private Thread thread;

    /** Internal thread will run for as long as running is true. */
    private volatile boolean running = true;

    /** Performs puts on exampleRegion and intermittently sleeps for half minute. */
    @Override
    public void run() {
      System.out.println(this + " has started.");
      Region<Object, Object> region = cache.getRegion("exampleRegion");
      while (this.running) {
        System.out.println(this + " is performing puts on exampleRegion.");
        for (int i = 0; i < 10; i++) {
          region.put("key"+i, System.currentTimeMillis());
        }

        QueryService queryService = cache.getQueryService();
        Query query = queryService.newQuery("SELECT DISTINCT * FROM /exampleRegion");
        System.out.println("\n" + this + " is executing query:\n\t" + query.getQueryString());
        try {
          Object result = query.execute();
          System.out.println(this + " has query result:\n\t" + formatQueryResult(result));
        }
        catch (QueryException e) {
          e.printStackTrace();
        }

        try {
          System.out.println(this + " is sleeping...");
          Thread.sleep(1000 * 30);
        }
        catch (InterruptedException e) {
          System.out.println(this + " was interrupted.");
        }
        System.out.println(this + " has stopped.");
      }
    }

    /** Starts the internal thread. */
    public void startThread() {
      this.thread = new Thread(this, toString());
      this.thread.start();
    }

    /** Stops the internal thread and waits up to one minute for it to stop. */
    public void stopThread() throws InterruptedException {
      System.out.println(this + " is stopping.");
      this.running = false;
      this.thread.interrupt();
      this.thread.join(1000 * 60);
    }

    /** Returns the name of this thread. */
    @Override
    public String toString() {
      return getClass().getName() + " Thread";
    }
  }
}
