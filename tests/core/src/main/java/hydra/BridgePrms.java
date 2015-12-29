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
 * A class used to store keys for bridge configuration settings.
 * The settings are used to create instances of {@link BridgeDescription}.
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
 * Values, fields, and subfields can be set to {@link #NONE} where noted, with
 * the documented effect.
 * <p>
 * Values and fields of a parameter can use oneof, range, or robing
 * except where noted, but each description created will use a fixed value
 * chosen at test configuration time.  Use as a task attribute is illegal.
 */
public class BridgePrms extends BasePrms {

  /**
   * (String(s))
   * Logical names of the bridge descriptions.  Each name must be unique.
   * Defaults to null.  Not for use with oneof, range, or robing.
   */
  public static Long names;
  
  /**
   * (int(s))
   * Capacity of client queues for each bridge, in entries or megabytes,
   * depending on the {@link #evictionPolicy}.
   */
  public static Long capacity;

  /**
   * (String(s))
   * Name of logical disk store configuration (and actual disk store name)
   * to use on the client queues for each bridge, as found in {@link
   * DiskStorePrms#names}.  Required when the {@link #evictionPolicy} is set
   * to cause eviction.  Can be specified as {@link #NONE} (default).
   */
  public static Long diskStoreName;

  /**
   * (String(s))
   * Eviction algorithm to use on the client queues for each bridge.
   * Can be specified as ${NONE} (no eviction).  Valid policies are "mem"
   * and "entry".
   * <p>
   * If the algorithm causes eviction, {@link #diskStoreName} must also be set.
   * See also {@link #capacity}.
   */
  public static Long evictionPolicy;
  
  /**
   * (Comma-separated String(s))
   * Server groups for each bridge.
   */
  public static Long groups;

  /**
   * How frequently, in milliseconds, to poll the load probe.
   */
  public static Long loadPollInterval;
  
  /**
   * The fully qualified name of a class to use as the load
   * probe for this bridge server. The class must have a no-args
   * constructor.
   */
  public static Long loadProbe;

  /**
   * (int(s))
   * Max connections for each bridge.
   */
  public static Long maxConnections;

  /**
   * (int(s))
   * Max threads for each bridge (creates pool for NIO selector when non-zero).
   */
  public static Long maxThreads;

  /**
   * (int(s))
   * Maximum message count for each bridge.
   */
  public static Long maximumMessageCount;

  /**
   * (int(s))
   * Maximum time between pings for each bridge.
   */
  public static Long maximumTimeBetweenPings;

  /**
   * (int(s))
   * Message time to live for each bridge, in seconds.
   */
  public static Long messageTimeToLive;

  /**
   * (int(s))
   * Socket buffer size for each bridge.
   */
  public static Long socketBufferSize;

//------------------------------------------------------------------------------
// parameter setup
//------------------------------------------------------------------------------

  static {
    setValues(BridgePrms.class);
  }

  public static void main(String args[]) {
    Log.createLogWriter("bridgeprms", "info");
    dumpKeys();
  }
}
