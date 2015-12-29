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

package com.gemstone.gemfire.internal.cache;

import hct.ha.HAClientQueueBB;
import hydra.Log;

import java.io.IOException;
import java.util.Iterator;

import util.TestException;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.internal.cache.lru.LRUStatistics;
import com.gemstone.gemfire.internal.cache.lru.NewLRUClockHand;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.internal.cache.tier.sockets.HaHelper;

import delta.DeltaPropagationBB;

/*
 * Helper class to allow test for LocalRegion internal tokens
 */
public class LocalRegionHelper {

  public static boolean isInvalidToken(Object o) {
    return o == Token.INVALID || o == Token.LOCAL_INVALID;
  }
  
  public static boolean isLocalInvalidToken(Object o) {
    return o == Token.LOCAL_INVALID;
  }
  
  public static boolean isDestroyedToken(Object o) {
    return o == Token.DESTROYED;
  }

  /**
   * A helper for creating a subregion, potentially using a package protected
   * method to do so.  
   * @param root the parent region
   * @param name the name of the subregion to create
   * @param attrs the attributes used to create the subregion
   * @param internalArgs if not null, then use the package protected creation mechanism
   * @return the subregion whose parent is the provided root
   * @throws CacheException
   * @see Region#createSubregion(String, RegionAttributes)
   * @see LocalRegion#createSubregion(String, RegionAttributes, InternalRegionArguments)
   */
  public static Region createSubregion(Region root, String name,
      RegionAttributes attrs, final InternalRegionArguments internalArgs) throws CacheException
  {
    if (internalArgs == null) {
      return root.createSubregion(name, attrs);
    } else {
      try {
        LocalRegion lr = (LocalRegion) root;
        return lr.createSubregion(name, attrs, internalArgs);
      } catch (IOException ioe) {
        AssertionError assErr = new AssertionError("unexpected exception");
        assErr.initCause(ioe);
        throw assErr;
      } catch (ClassNotFoundException cnfe) {
        AssertionError assErr = new AssertionError("unexpected exception");
        assErr.initCause(cnfe);
        throw assErr;
      } 
    }
  }
  
  /**
  * Varifies that HAOverflow feature is used in primary server<br>
  * and toogle flag in shared black board
  * @since 5.7
  */
  // placed here because of visibility issue
  public static void isHAOverflowFeaturedUsedInPrimaryPutOnShareBB() {
    Cache cache = GemFireCacheImpl.getInstance();
    LRUStatistics lifoStats = null;
    NewLRUClockHand lifoClockHand = null;
    Iterator itr = cache.getBridgeServers().iterator();
    while (itr.hasNext()) {
      BridgeServerImpl server = (BridgeServerImpl)itr.next();
      Iterator iter_prox = server.getAcceptor().getCacheClientNotifier()
          .getClientProxies().iterator();
      while (iter_prox.hasNext()) {
        CacheClientProxy proxy = (CacheClientProxy)iter_prox.next();
        if (HaHelper.checkPrimary(proxy)) {
          Log.getLogWriter().info(
              " found primary :" + server);
          /*
           * NewLIFOClockHand extends NewLRUClockHand to hold on to the list
           * reference
           */
          lifoClockHand = ((VMLRURegionMap)((LocalRegion)cache
              .getRegion(Region.SEPARATOR
                  + BridgeServerImpl.generateNameForClientMsgsRegion(server
                      .getPort()))).entries)._getLruList();

          /* storing stats reference */
          lifoStats = lifoClockHand.stats();
          int status = (int)HAClientQueueBB.getBB().getSharedCounters().read(
              HAClientQueueBB.HA_OVERFLOW_STATUS);
          // validate Overflow happened
          if (lifoStats.getEvictions() > 0 || status == 1) {
            HAClientQueueBB.getBB().getSharedCounters().setIfLarger(
                HAClientQueueBB.HA_OVERFLOW_STATUS, 1);
            
            Log.getLogWriter().info(
                "eviction count : " + lifoStats.getEvictions()
                    + "HA_OVERFLOW_STATUS : " + status);       
          }
        }
      }
    }
  }
  /**
   * Varifies that overflow used in server<br>
   * @since 6.1
   */
   public static void isOverflowUsedOnBridge() {
    Cache cache = GemFireCacheImpl.getInstance();
    LRUStatistics lifoStats = null;
    NewLRUClockHand lifoClockHand = null;
    Iterator itr = cache.getCacheServers().iterator();
    while (itr.hasNext()) {
      BridgeServerImpl server = (BridgeServerImpl)itr.next();
      lifoClockHand = ((VMLRURegionMap)((LocalRegion)cache
          .getRegion(Region.SEPARATOR
              + BridgeServerImpl.generateNameForClientMsgsRegion(server
                  .getPort()))).entries)._getLruList();

      /* storing stats reference */
      lifoStats = lifoClockHand.stats();
      // validate Overflow happened
      if (lifoStats.getEvictions() > 0) {
        Log.getLogWriter().info(
            "eviction count : " + lifoStats.getEvictions());
      }
      else {
        throw new TestException(
            "Test issue : Test need tuning - no overflow happened");
      }
    }
  }
}
