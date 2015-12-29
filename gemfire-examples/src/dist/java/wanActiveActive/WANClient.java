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
package wanActiveActive;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.NoSubscriptionServersAvailableException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.NoAvailableServersException;
import com.gemstone.gemfire.distributed.DistributedMember;
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
  protected Region<String, Value> region;

  /**
   * Parses the command line and executes the <code>WANClient</code> example.
   * @param args The arguments to parse
   */
  public static void main(String[] args) throws Exception {
    WANClient client = new WANClient();

    // Initialize the cache
    client.initializeCache();

    // Retrieve the region
    try {
      client.initializeRegion("/wanActiveActive");
    } catch (NoSubscriptionServersAvailableException e) {
      System.out.println("My server is not running.  Start it and try again.");
      System.exit(1);
    }

    // Execute test
    client.doPuts();
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

    // register interest in updates from other processes
    LocalRegion lr = (LocalRegion) this.region;
    lr.registerInterest("ALL_KEYS");
    System.out.println("Registered interest in updates for region " + lr.getName());
  }

  /**
   * Executes the <code>WANClient</code> conflicting update thread
   */
  protected void doPuts() {
    final String key = "MyValue";

    final DistributedMember myID = CacheFactory.getAnyInstance().getDistributedSystem().getDistributedMember();
    final String PID = "PID=" + myID.getProcessId();

    Random randomTime = new Random();

    while (true) {
      try {
        Value value = region.get(key);

        if (value == null) {
          // put the first value into the cache
          String str = PID + " count=1";
          value = new Value(1, str);
        }
        else {
          // update the value that's currently in the cache
          if (!value.getModification().contains(PID)) {
            if (value.mergedByConflictResolver()) {
              System.out.println("current value was merged by conflict resolver: " + value);
            }
            else { 
              System.out.println("current value is from other system: " + value.getModification());
            }
          }
          int count = value.getModificationCount();
          count = count + 1;
          String str = PID + " count=" + count;
          value.addHistory(str);
          value.setModification(str);
          value.incrementModificationCount();
          value.clearMergedFlag();
        }
        
        
        System.out.println("PID " + myID.getProcessId() + " putting update #" + value.getModificationCount());
        this.region.put(key, value);

        // now sleep a little, with a random amount thrown in to keep from
        // getting into lock-step with other WANClients
        int extraSleep = randomTime.nextInt(500);
        Thread.sleep(1000+extraSleep);
      }
      catch (NoAvailableServersException e) {
        System.out.println("My server is no longer running - exiting");
        System.exit(1);
        return;
      }
      catch (CancelException e) {
        // cache is closed - probably from control-c
        return;
      }
      catch (Exception e) {
        e.printStackTrace();
        return;
      }
    }
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
}
