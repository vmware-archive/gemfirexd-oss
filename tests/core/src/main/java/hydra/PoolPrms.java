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

/**
 * A class used to store keys for pool configuration settings.  The settings
 * are used to create instances of {@link PoolDescription}.
 * <p>
 * The number of description instances is gated by {@link #names}.  For other
 * parameters, if fewer values than names are given, the remaining instances
 * will use the last value in the list.  See $JTESTS/hydra/hydra.txt for more
 * details.
 * <p>
 * Unused parameters default to null, except where noted.  This uses the
 * product default, except where noted.
 * <p>
 * Values and fields of a parameter can be set to {@link #DEFAULT},
 * except where noted.  This uses the product default, except where noted.
 * <p>
 * Values and fields of a parameter can use oneof, range, or robing
 * except where noted, but each description created will use a fixed value
 * chosen at test configuration time.  Use as a task attribute is illegal.
 */
public class PoolPrms extends BasePrms {

  /** Default for {@link #contactNum}. */
  protected static final int ALL_AVAILABLE = -1;

  /**
   * (String(s))
   * Logical names of the pool descriptions and actual names of the pools.
   * Each name must be unique.  Defaults to null.  Not for use with oneof,
   * range, or robing.
   */
  public static Long names;

  /**
   * (Comma-separated String pair(s))
   * Contact algorithm used to autogenerate contacts for each pool.
   * An algorithm consists of a classname followed by a method name.
   * <p>
   * Contact algorithms must be of type <code>public static List</code> and
   * take two arguments: an <code>int</code> representing the {@link
   * #contactNum} and a <code>String</code> optionally naming the {@link
   * #distributedSystem}.
   * <p>
   * The returned list must contain <code>contactNum</code> objects either all
   * of type {@link DistributedSystemHelper.Endpoint} for which {@link
   * DistributedSystemHelper.Endpoint#isServerLocator} is true, or of type
   * {@link BridgeHelper.Endpoint}.  Each endpoint must be in the designated
   * distributed system, if specified.
   * <p>
   * Defaults to {@link PoolHelper#getRandomContacts(int, String)}, which
   * returns server locators.  The equivalent algorithm for bridge servers is
   * {@link EdgeHelper#getRandomEndpoints(int, String)}.  These are suitable
   * for hierarchical cache tests that do not use gateways.
   * <p>
   * Suitable choices for tests using <code>hydraconfig/topology_wan_hct.inc
   * </code> are {@link PoolHelper#getRandomContactsInWanHctSite(int, String)}
   * and {@link EdgeHelper#getRandomEndpointsInWanHctSite(int, String)}.
   * <p>
   * Contact algorithms should generate contacts from hydra clients that host
   * server locators created and started using {@link DistributedSystemHelper}
   * as returned by {@link DistributedSystemHelper#getContacts} and related
   * methods.  Or, they should choose endpoints from hydra clients that host
   * bridge servers created and started using {@link BridgeHelper} as returned
   * by {@link BridgeHelper#getEndpoints} and related methods.
   */
  public static Long contactAlgorithm;

  /**
   * (int(s))
   * Number of contacts for each pool, chosen according to {@link PoolPrms
   * #contactAlgorithm}.  Defaults to {@link #ALL_AVAILABLE}.
   */
  public static Long contactNum;

  /**
   * (String(s))
   * Name of distributed system from which to select contacts, as found
   * in {@link hydra.GemFirePrms-distributedSystem}.  Defaults to null
   * (all available).  Used by contact algorithms in {@link hydra.PoolHelper}.
   */
  public static Long distributedSystem;

  /**
   * (int(s))
   * Free connection timeout for each pool, in milliseconds.
   */
  public static Long freeConnectionTimeout;

  /**
   * (long(s))
   * Idle timeout for each pool, in milliseconds.
   */
  public static Long idleTimeout;

  /**
   * (int(s))
   * Load conditioning interval for each pool, in milliseconds.
   */
  public static Long loadConditioningInterval;

  /**
   * (int(s))
   * Minimum connections for each pool.
   */
  public static Long minConnections;

  /**
   * (int(s))
   * Maximum connections for each pool.
   */
  public static Long maxConnections;

  /**
   * (boolean(s))
   * Multiuser authentication for each pool.
   */  
  public static Long multiuserAuthentication;

  /**
   * (long(s))
   * Ping interval for each pool, in milliseconds.
   */
  public static Long pingInterval;
  
  /**
   * (boolean(s))
   * PR single hop enabled for each pool.
   */
  public static Long prSingleHopEnabled;
  
  /**
   * (int(s))
   * Read timeout for each pool, in milliseconds.
   */
  public static Long readTimeout;

  /**
   * (int(s))
   * Retry attempts for each pool.
   */
  public static Long retryAttempts;

  /**
   * (String(s))
   * Server group for each pool.  Must be one of the groups in
   * {@link BridgePrms#groups}.  Defaults to the default server group.
   */
  public static Long serverGroup;

  /**
   * (int(s))
   * Socket buffer size for each pool, in bytes.
   */
  public static Long socketBufferSize;

  /**
   * (int(s))
   * Statistic interval for each pool, in milliseconds.
   */
  public static Long statisticInterval;

  /**
   * (int(s))
   * Subscription ack interval for each pool, in milliseconds.
   */
  public static Long subscriptionAckInterval;

  /**
   * (boolean(s))
   * Subscription enabled for each pool.
   */
  public static Long subscriptionEnabled;

  /**
   * (int(s))
   * Subscription message tracking timeout for each pool, in milliseconds.
   * Ignored when {@link #subscriptionEnabled} is false.
   */
  public static Long subscriptionMessageTrackingTimeout;

  /**
   * (int(s))
   * Subscription redundancy for each pool.  Ignored when {@link
   * #subscriptionEnabled} is false.
   */
  public static Long subscriptionRedundancy;

  /**
   * (boolean(s))
   * Thread local connections for each pool.
   */
  public static Long threadLocalConnections;
  
//------------------------------------------------------------------------------
// parameter setup
//------------------------------------------------------------------------------

  static {
    setValues(PoolPrms.class);
  }

  public static void main(String args[]) {
    Log.createLogWriter("poolprms", "info");
    dumpKeys();
  }
}
