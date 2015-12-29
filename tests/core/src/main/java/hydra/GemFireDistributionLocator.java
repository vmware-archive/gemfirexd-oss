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

import com.gemstone.gemfire.distributed.internal.InternalLocator;

//import java.util.*;

/**
 *  GemFireDistributionLocator 
 * 
 */
public class GemFireDistributionLocator {

    //private static final GemFireDistributionLocator singleton = new GemFireDistributionLocator();
  private static InternalLocator locator = null;
  private static String distributedSystemName = null;
  private static int locatorPort;

  /**
   *  Start the peer locator for the specified distributed system
   *  on the specified port and with the given properties.
   * @param dsName       the name of the distributed system
   * @param port         the locator's port
   * @param dsProperties the Properties used by the locator to create its DistributedSystem
   * @param serverLocator whether the locator supports server info
   */
  public static void start(
    String dsName,
    int port,
    java.util.Properties dsProperties,
    boolean serverLocator)
  {
    distributedSystemName = dsName;
    locatorPort = port;
    try {
      if (dsProperties.getProperty("mcast-port") == null
        || dsProperties.getProperty("mcast-port").length() == 0) {
        dsProperties.setProperty("mcast-port", "0");
      }
      else if (dsProperties.getProperty("mcast-port").equals("10334")) {
        Log.getLogWriter().error("Attempt to create locator with default mcast port", new Exception());
        dsProperties.setProperty("mcast-port", "0");
      }
      locator = GemFireDistributionLocatorVersion.startLocator(
                  locatorPort, 
                  Log.getLogWriter(), 
                  Log.getLogWriter(),
                  HostHelper.getIPAddress(),
                  dsProperties, serverLocator);
      Log.getLogWriter().info( "GemFireDistributionLocator for " + distributedSystemName + " started on port " + locatorPort );
    } catch( Exception e ) {
      throw new HydraRuntimeException( "Could not start GemFireDistributionLocator", e );
    }
  }

  /**
   *  Tell the locator to terminate.
   */
  public static void stop() {
      locator.stop();
      Log.getLogWriter().info( "GemFireDistributionLocator for " + distributedSystemName + " on port " + locatorPort + " stopped" );
  }
    
  /**
   *  There is a single instance of GemFireDistributionLocator.  Create it
   *  using start()
   */
  private GemFireDistributionLocator() {
    super();
  }

}
