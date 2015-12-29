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
 * A class used to store keys for gateway and gateway queue configuration
 * settings.  The settings are used to create instances of {@link
 * GatewayDescription}, which are used to create gateway hubs.
 * <p>
 * The number of description instances is gated by {@link #names}.  For other
 * parameters, if fewer values than names are given, the remaining instances
 * will use the last value in the list.  See $JTESTS/hydra/hydra.txt for more
 * details.
 * <p>
 * Unused parameters default to null, except where noted.  This uses the
 * product default, except where noted.
 * <p>
 * Values of a parameter can be set to {@link #DEFAULT}, except where noted.
 * This uses the product default, except where noted.
 * <p>
 * Values and fields can be set to {@link #NONE} where noted, with the
 * documented effect.
 * <p>
 * Values of a parameter can use oneof, range, or robing except where noted, but
 * each description created will use a fixed value chosen at test configuration
 * time.  Use as a task attribute is illegal.
 */
public class GatewayPrms extends BasePrms {

  /** Default for {@link #endpointNumPerDs}. */
  protected static final int ALL_AVAILABLE = -1;

  /**
   * (String(s))
   * Logical names of the gateway descriptions.  Each name must be unique.
   * Defaults to null.  Not for use with oneof, range, or robing.
   */
  public static Long names;

  /**
   * (int(s))
   * Alert threshold for each gateway queue, in milliseconds.
   */
  public static Long alertThreshold;

  /**
   * (boolean(s))
   * Batch conflation for each gateway queue.
   */
  public static Long batchConflation;

  /**
   * (int(s))
   * Batch size for each gateway queue.
   */
  public static Long batchSize;

  /**
   * (int(s))
   * Batch time interval for each gateway queue.
   */
  public static Long batchTimeInterval;

  /**
   * (int(s))
   * Concurrency level for each gateway.  See also {@link #orderPolicy},
   * which can be set in the concurrent case.
   */
  public static Long concurrencyLevel;

  /**
   * (String(s))
   * Name of logical disk store configuration (and actual disk store name)
   * for each gateway queue, as found in {@link DiskStorePrms#names}.
   * Required when {@link #enablePersistence} is true.  Can be specified
   * as {@link #NONE} (default).
   */
  public static Long diskStoreName;

  /**
   * (boolean(s))
   * Enable persistence for each gateway queue.  When true, requires setting
   * a {@link #diskStoreName}.
   */
  public static Long enablePersistence;

  /**
   * (int(s))
   * Number of endpoints per distributed system for each gateway, chosen from
   * available gateway hubs.  Defaults to the total number of available gateway
   * hubs outside of this hub's distributed system.
   */
  public static Long endpointNumPerDs;

  /**
   * (Comma-separated String(s))
   * Class names of listeners for each region.  Can be specified as
   * {@link #NONE}.  See {@link #listenersSingleton} for control over
   * instance creation.
   * <p>
   * Example: To use ClassA and ClassB for the first gateway, no listeners for
   * the second gateway, and ClassC for the third gateway, specify:
   *     <code>ClassA ClassB, none, ClassC</code>
   */
  public static Long listeners;

  /**
   * (boolean(s))
   * Whether the {@link #listeners} for each gateway are singletons across
   * all actual gateways created with this description.  Defaults to false.
   */
  public static Long listenersSingleton;

  /**
   * (int(s))
   * Maximum queue memory for each gateway.
   */
  public static Long maximumQueueMemory;

  /**
   * (String(s))
   * Order policy for each gateway.  See {@link #concurrencyLevel}.
   */
  public static Long orderPolicy;

  /**
   * (int(s))
   * Socket buffer size for each gateway.
   */
  public static Long socketBufferSize;

  /**
   * (int(s))
   * Socket read timeout for each gateway.  This is currently a noop in the
   * product but has not been removed or officially deprecated.
   */
  //public static Long socketReadTimeout;

//------------------------------------------------------------------------------
// parameter setup
//------------------------------------------------------------------------------

  static {
    setValues(GatewayPrms.class);
  }
  public static void main(String args[]) {
    Log.createLogWriter("gatewayprms", "info");
    dumpKeys();
  }
}
