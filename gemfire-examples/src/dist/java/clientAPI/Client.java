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
package clientAPI;

import static com.gemstone.gemfire.cache.client.ClientRegionShortcut.PROXY;
import cacheRunner.LoggingCacheListener;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;

/**
 * This example shows how to use the client APIs to create two different
 * types of clients that will connect to a cache server.
 * <p>See the <code>README.html</code> for instructions on how to run the example.
 * <p>See the source code in <code>Client.java</code>
 * to see how to use the client APIs.
 *
 * <p>The two types of clients are:
 * <ol>
 * <li>A client acting as a publisher. It does not want to store any data
 *     in the client cache but simply send it to the server.
 * <li>A client acting as a subscriber. It just wants events any time a publisher
 *     adds some data.
 * </ol>
 * @since 5.7
 */
public class Client {
  /**
   * Sets up this client's {@link ClientCache}. Since we are using a ClientCache,
   * we can omit a few properties we would usually provide
   * (mcast-port, locators, etc).
   * 
   * <p>
   * Logging and statistics are enabled because it is best to have these
   * recorded.
   * <p>
   * cache-xml-file is disabled since this example uses API to create the client's
   * pools and regions.
   * <p>
   * An alternative would be to use a <code>gemfire.properties</code> file containing:
   * <tt>
   * log-file=name.log
   * statistic-archive-file=name.gfs
   * statistic-sampling-enabled=true
   * cache-xml-file=
   * </tt>
   * and to construct a default {@link ClientCacheFactory}, allowing
   * GemFire to load the properties from the property file.
   *
   * @param name the base name to use for the log file and stats file.
   * @return the created {@link ClientCache}
   */
  public static ClientCacheFactory connectStandalone(String name) {
    return new ClientCacheFactory()
      .set("log-file", name + ".log")
      .set("statistic-archive-file", name + ".gfs")
      .set("statistic-sampling-enabled", "true")
      .set("cache-xml-file", "")
      .addPoolLocator("localhost", LOCATOR_PORT);
  }

  /**
   * The number of puts the publisher should do
   * and the subscriber should receive.
   */
  private static final int NUM_PUTS = 10;

  /**
   * The port the locator is listening on.
   */
  private static final int LOCATOR_PORT = Integer.getInteger("locatorPort", 41111).intValue();

  private static void runPublisher() {
    /*
     * To declare in a cache.xml do this:
      <!DOCTYPE client-cache PUBLIC
        "-//GemStone Systems, Inc.//GemFire Declarative Caching 6.5//EN"
        "http://www.gemstone.com/dtd/cache6_5.dtd">
      <client-cache>
        <pool name="publisher">
          <locator host="localhost" port="41111"/>
        </pool>
      </client-cache>
     */
    ClientCacheFactory ccf = connectStandalone("publisher");
    ClientCache cache = ccf.create();
    
    /*
     * To declare in a cache.xml do this:
        <region name="DATA" refid="PROXY"/>
     */
    ClientRegionFactory<String,String> regionFactory = cache.createClientRegionFactory(PROXY);
    Region<String, String> region = regionFactory.create("DATA");

    // now just do some puts in the publisher and confirm that they
    // show up in the client
    for (int i=1; i <= NUM_PUTS; i++) {
      String key = "key"+i;
      String value = "value"+i;
      System.out.println("putting key " + key);
      region.put(key, value);
    }

    cache.close();
  }
  
  private static void runSubscriber() throws InterruptedException {
    /*
     * To declare in a cache.xml do this:
      <!DOCTYPE client-cache PUBLIC
        "-//GemStone Systems, Inc.//GemFire Declarative Caching 6.5//EN"
        "http://www.gemstone.com/dtd/cache6_5.dtd">
      <client-cache>
        <pool name="subscriber" subscription-enabled="true">
          <locator host="localhost" port="41111"/>
        </pool>
      </client-cache>
     */
    ClientCacheFactory ccf = connectStandalone("subscriber");
    ccf.setPoolSubscriptionEnabled(true);
    ClientCache cache = ccf.create();
    /*
     * To declare in a cache.xml do this:
        <region name="DATA" refid="PROXY">
            <region-attributes>
                <subscription-attributes interest-policy=all/>
                <cache-listener>
                  <class-name>clientAPI.SubscriberListener</class-name>
                </cache-listener>
            </region-attributes>
        </region>
     */
    ClientRegionFactory<String,String> regionFactory = cache.createClientRegionFactory(PROXY);
    Region<String, String> region = regionFactory
      .addCacheListener(new SubscriberListener())
      .create("DATA");
    region.registerInterestRegex(".*", // everything
                                 InterestResultPolicy.NONE,
                                 false/*isDurable*/);
    SubscriberListener myListener = (SubscriberListener)
      region.getAttributes().getCacheListeners()[0];
    System.out.println("waiting for publisher to do " + NUM_PUTS + " puts...");
    myListener.waitForPuts(NUM_PUTS);
    System.out.println("done waiting for publisher.");

    cache.close();
  }

  /**
   * A listener that is used in the subscriber client to wait for the
   * publisher to finish its work.
   */
  public static class SubscriberListener extends LoggingCacheListener {
    private int puts = 0;

    /**
     * Counts the number of puts done by create events.
     */
    @Override
    public void afterCreate(EntryEvent event) {
      super.afterCreate(event);
      synchronized (this) {
        this.puts++;
        notifyAll();
      }
    }

    /**
     * Counts the number of puts done by update events.
     */
    @Override
    public void afterUpdate(EntryEvent event) {
      super.afterUpdate(event);
      synchronized (this) {
        this.puts++;
        notifyAll();
      }
    }

    /**
     * Blocks until <code>numToWaitFor</code> puts have been done.
     */
    public void waitForPuts(int numToWaitFor) throws InterruptedException {
      synchronized (this) {
        while (this.puts < numToWaitFor) {
          wait();
        }
      }
    }
  }

  /**
   * Instances of the class should never be created. It simply has static methods.
   */
  private Client() {
  }
  
  /**
   * See the the <code>README.html</code> for instructions on how to
   * run the class from the command line.
   * @param args must contain one element whose value is "publisher" or "subscriber".
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("Usage: java Client publisher|subscriber");
      System.exit(1);
    }
    String arg = args[0];
    if (arg.equalsIgnoreCase("publisher")) {
      runPublisher();
    } else if (arg.equalsIgnoreCase("subscriber")) {
      runSubscriber();
    } else {
      System.err.println("expected \"publisher\" or \"subscriber\" but found \""
                         + arg + "\".");
      System.exit(1);
    }
  }
}
