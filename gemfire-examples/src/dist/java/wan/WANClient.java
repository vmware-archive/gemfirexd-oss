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
package wan;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.internal.cache.LocalRegion;

import java.util.Random;

/**
 * Class <code>WANClient</code> is a WAN client application. If invoked with the 'us'
 * argument, it writes entries to a region that participates in a WAN. If invoked with
 * the 'eu' argument, it listens for updates to a region that participates in a WAN.
 *
 * @author GemStone Systems, Inc.
 * @since 4.2
 */
public class WANClient {

  /** The GemFire <code>ClientCache</code> */
  protected ClientCache cache;

  /** The GemFire <code>Region</code> */
  protected Region<String, String> region;

  /**
   * Parses the command line and executes the <code>WANClient</code> example.
   * @param args The arguments to parse
   */
  public static void main(String[] args) throws Exception {
    try {
      if(args.length != 1) {
        System.err.println("** Missing site name");
        System.err.println();
        System.err.println("usage: java wan.WANClient"); 
        System.err.println("  site   The name of the WAN site (either 'us' or 'eu')");
        System.err.println();
        System.err.println("Launches a WAN client");
        System.exit(1);
      }
      WANClient client = new WANClient();

      // Initialize the cache
      client.initializeCache();

      // Retrieve the region
      client.initializeRegion("/trades");

      // Execute test
      if (args[0].equals("us")) {
        // Do puts
        client.doPuts();
      } else if (args[0].equals("eu")) {
        // Register interest in all keys and wait for updates
        client.registerInterest();
        client.waitForever();
      } else {
        usage();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Initializes the GemFire <code>ClientCache</code>.
   */
  protected void initializeCache() throws CacheException {
    // Create cache
    this.cache = new ClientCacheFactory().create();
    System.out.println("Created GemFire client cache: " + this.cache.getName());
  }

  /**
   * Initializes the GemFire <code>Region</code>.
   * @param regionName The name of the <code>Region</code> to initialize
   */
  protected void initializeRegion(String regionName) {
    this.region = this.cache.getRegion(regionName);
    System.out.println("Retrieved region: " + this.region);
  }

  /**
   * Executes the <code>WANClient</code> multi-threaded putter.
   */
  protected void doPuts() {
    // Spawn several threads to execute
    for (int i=0; i<10; i++) {
      Thread thread = new Thread(
        new Runnable() {
          public void run() {
            Random random = new Random();
            while (true) {
              SystemFailure.checkFailure(); // GemStoneAddition
              try {
                // Put a random entry into the region
                int j = random.nextInt(5000);
                String key = "key-" + j;
                String value = String.valueOf(j);
                System.out.println(Thread.currentThread().getName() + ": Putting " + key + "->" + value);
                try {
                  WANClient.this.region.put(key, value);
                } 
                catch (CancelException cce) {
                  System.exit(0);
                }
                Thread.sleep(10);
              } 
              catch (Exception e) {
                e.printStackTrace();
              }
            }
          }
        });
      thread.start();
    }
  }

  /**
   * Registers interest in the region entries.
   */
  protected void registerInterest() throws CacheException
  {
    LocalRegion lr = (LocalRegion) this.region;
    lr.registerInterest("ALL_KEYS");
    System.out.println("Registered interest in " + lr.getInterestList() + " for region " + lr.getName());
  }

  /**
   * Waits forever.
   */
  protected void waitForever() throws InterruptedException {
    Object obj = new Object();
    if (Thread.interrupted()) throw new InterruptedException();
    synchronized (obj) {
      obj.wait();
    }
  }

  /**
   * Prints usage information about this program
   */
  protected static void usage() {
    System.err.println("\n** Missing site name\n");
    System.err.println("usage: java wan.WANClient <site>");
    System.err.println("  site   The name of the WAN site (either 'us' or 'eu')");
    System.exit(1);
  }
}
