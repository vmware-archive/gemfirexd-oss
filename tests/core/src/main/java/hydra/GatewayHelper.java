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

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.internal.NanoTimer;
import java.util.*;

/**
 * Provides support for gateways running in hydra-managed client VMs.
 */
public class GatewayHelper {

//------------------------------------------------------------------------------
// Gateway
//------------------------------------------------------------------------------

  /**
   * Returns the given gateway as a string.
   */
  public static String gatewayToString(Gateway gateway) {
    return GatewayDescription.gatewayToString(gateway);
  }

//------------------------------------------------------------------------------
// GatewayDescription
//------------------------------------------------------------------------------

  /**
   * Returns the {@link GatewayDescription} with the given configuration name
   * from {@link GatewayPrms#names}.
   */
  public static GatewayDescription getGatewayDescription(String gatewayConfig) {
    if (gatewayConfig == null) {
      throw new IllegalArgumentException("gatewayConfig cannot be null");
    }
    log("Looking up gateway config: " + gatewayConfig);
    GatewayDescription gd = TestConfig.getInstance()
                                      .getGatewayDescription(gatewayConfig);
    if (gd == null) {
      String s = gatewayConfig + " not found in "
               + BasePrms.nameForKey(GatewayPrms.names);
      throw new HydraRuntimeException(s);
    }
    log("Looked up gateway config:\n" + gd);
    return gd;
  }

//------------------------------------------------------------------------------
// Endpoints
//------------------------------------------------------------------------------

  /**
   * Returns a randomized list containing the given number of endpoints for
   * the specified distributed system.  If the number of endpoints is
   * {@link GatewayPrms#ALL_AVAILABLE}, then all available matching endpoints
   * are included, in random order.
   *
   * @throws HydraRuntimeException if there are not enough matching endpoints.
   */
  protected static List getRandomEndpoints(String distributedSystemName,
            List endpoints, int numEndpoints) {

    // check arguments
    if (distributedSystemName == null) {
      String s = "distributedSystemName cannot be null";
      throw new IllegalArgumentException(s);
    }
    String ownds = DistributedSystemHelper.getDistributedSystemName();
    if (distributedSystemName.equals(ownds)) {
      String s = "Attempt to create gateway to own distributed system: "
               + ownds;
      throw new IllegalArgumentException(s);
    }

    // get the endpoints in the desired distributed system
    List matchingEndpoints = new ArrayList();
    for (Iterator i = endpoints.iterator(); i.hasNext();) {
      GatewayHubHelper.Endpoint endpoint = (GatewayHubHelper.Endpoint)i.next();
      if (endpoint.getDistributedSystemName().equals(distributedSystemName)) {
        matchingEndpoints.add(endpoint);
      }
    }
    if (numEndpoints > matchingEndpoints.size()) {
      String s = "Cannot find " + numEndpoints + " matching endpoints,"
               + " there are only " + matchingEndpoints.size();
      throw new HydraRuntimeException(s);
    }

    // determine the desired number of endpoints
    int n = (numEndpoints == GatewayPrms.ALL_AVAILABLE)
          ? matchingEndpoints.size() : numEndpoints;

    // select the desired number of endpoints at random
    Random rng = new Random(NanoTimer.getTime());
    List randomEndpoints = new ArrayList();
    for (int i = 0; i < n; i++) {
      int index = rng.nextInt(matchingEndpoints.size());
      randomEndpoints.add(matchingEndpoints.remove(index));
    }
    return randomEndpoints;
  }

//------------------------------------------------------------------------------
// Log
//------------------------------------------------------------------------------

  private static LogWriter log;
  private static synchronized void log(String s) {
    if (log == null) {
      log = Log.getLogWriter();
    }
    if (log.infoEnabled()) {
      log.info(s);
    }
  }
}
