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
 * A class used to store keys for gateway receiver configuration
 * settings.  The settings are used to create instances of {@link
 * GatewayReceiverDescription}.
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
public class GatewayReceiverPrms extends BasePrms {

  /**
   * (String(s))
   * Logical names of the gateway receiver descriptions.  Each name must be
   * unique.  Defaults to null.  Not for use with oneof, range, or robing.
   */
  public static Long names;

  /**
   * (int(s))
   * Ending port to use for each gateway receiver port range.
   */
  public static Long endPort;

  /**
   * (Comma-separated String(s))
   * Class names of gateway transport filters for each gateway receiver.  Can be
   * specified as {@link #NONE}.
   * <p>
   * Example: To use ClassA and ClassB for the first gateway receiver, none for
   * the second gateway receiver, and ClassC for the third gateway receiver,
   * specify:
   *     <code>ClassA ClassB, none, ClassC</code>
   */
  public static Long gatewayTransportFilters;

  /**
   * (int(s))
   * Maximum time between pings for each gateway receiver.
   */
  public static Long maximumTimeBetweenPings;

  /**
   * (int(s))
   * Number of ports to listen on per JVM for each gateway receiver when
   * creating instances with {@link GatewayReceiverHelper
   * #createAndStartGatewayReceiver(String)}.  Defaults to 1.
   * <p>
   * For example,
   */
  public static Long numInstances;

  /**
   * (int(s))
   * Starting port to use for each gateway receiver port range.
   */
  public static Long startPort;

  /**
   * (int(s))
   * Socket buffer size for each gateway receiver.
   */
  public static Long socketBufferSize;

//------------------------------------------------------------------------------
// parameter setup
//------------------------------------------------------------------------------

  static {
    setValues(GatewayReceiverPrms.class);
  }
}
