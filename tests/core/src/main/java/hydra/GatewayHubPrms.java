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
 * A class used to store keys for gateway hub configuration settings.  The
 * settings are used to create instances of {@link GatewayHubDescription},
 * which contain gateways.
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
 * Values of a parameter can use oneof, range, or robing except where noted, but
 * each description created will use a fixed value chosen at test configuration
 * time.  Use as a task attribute is illegal.
 */
public class GatewayHubPrms extends BasePrms {

  protected static final String ASYNC_DIST_PROP_NAME =
        "gemfire.asynchronous-gateway-distribution-enabled";

  /**
   * (String(s))
   * Logical names of the gateway descriptions.  Each name must be unique.
   * Defaults to null.  Not for use with oneof, range, or robing.
   */
  public static Long names;

  /**
   * (boolean(s))
   * Accept gateway connections for each gateway hub.  Defaults to true, which
   * allows hubs in other distributed systems to create gateways to this hub.
   * When set false, the hub is created without a port and will never receive
   * traffic over a gateway.
   */
  public static Long acceptGatewayConnections;

  /**
   * (boolean(s))
   * Asynchronous gateway distribution enabled for each gateway hub.  Sets the
   * system property "gemfire.asynchronous-gateway-distribution-enabled" so you
   * don't have to.  Defaults to the value of the system property from {@link
   * VmPrms#extraVMArgs} if set on the invoking VM, otherwise uses the product
   * default.  Overrides the VM argument if both are set.
   */
  public static Long asynchronousGatewayDistributionEnabled;

  /**
   * (boolean(s))
   * HA enabled for each gateway hub.  Defaults to true, which allows hubs in
   * the same distributed system to provide HA using primaries and secondaries.
   * When set false, hubs are given unique IDs and will not support HA.
   */
  public static Long haEnabled;

  /**
   * (int(s))
   * Maximum time between pings for each gateway hub.
   */
  public static Long maximumTimeBetweenPings;

  /**
   * (int(s))
   * Socket buffer size for each gateway hub.
   */
  public static Long socketBufferSize;

  /**
   * (String(s))
   * Startup policy for each gateway hub.
   */
  public static Long startupPolicy;

//------------------------------------------------------------------------------
// parameter setup
//------------------------------------------------------------------------------

  static {
    setValues(GatewayHubPrms.class);
  }
  public static void main(String args[]) {
    Log.createLogWriter("gatewayhubprms", "info");
    dumpKeys();
  }
}
