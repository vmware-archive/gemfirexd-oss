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
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.wan.GatewayReceiver;
import com.gemstone.gemfire.cache.wan.GatewayReceiverFactory;
import java.io.IOException;
import java.util.*;

/**
 * Provides support for gateway receivers running in hydra-managed client VMs.
 */
public class GatewayReceiverHelper {

  private static LogWriter log = Log.getLogWriter();

  private static String TheGatewayReceiverConfig;

//------------------------------------------------------------------------------
// GatewayReceiver
//------------------------------------------------------------------------------

  /**
   * Creates and starts gateway receivers in the current cache using the given
   * gateway receiver configuration.
   * @see #createGatewayReceivers(String)
   * @see #startGatewayReceivers()
   */
  public static synchronized Set<GatewayReceiver>
         createAndStartGatewayReceivers(String gatewayReceiverConfig) {
    Set<GatewayReceiver> receivers =
        createGatewayReceivers(gatewayReceiverConfig);
    startGatewayReceivers();
    return receivers;
  }

  /**
   * Creates gateway receivers in the current cache. The receivers are
   * configured using the {@link GatewayReceiverDescription} corresponding to
   * the given receiver configuration from {@link GatewayReceiverPrms#names}.
   * The number of receivers created is {@link GatewayReceiverPrms
   * #numInstances}.
   * <p>
   * Each receiver is assigned a different random port chosen by the product.
   * Receivers created after bouncing the JVM can use different ports.
   * <p>
   * This method is thread-safe.  Any number of threads in a given hydra
   * client JVM can invoke it, but only <code>numInstances</code> receivers
   * will be created for the given receiver configuration.
   */
  public static synchronized Set<GatewayReceiver> createGatewayReceivers(
                             String gatewayReceiverConfig) {
    // get the cache
    Cache cache = CacheHelper.getCache();
    if (cache == null) {
      String s = "Cache has not been created yet";
      throw new HydraRuntimeException(s);
    }
    Set<GatewayReceiver> receivers = cache.getGatewayReceivers();
    if (receivers == null || receivers.size() == 0) {
      // look up the gateway receiver configuration
      GatewayReceiverDescription grd =
             getGatewayReceiverDescription(gatewayReceiverConfig);
      int numInstances = grd.getNumInstances();

      // configure the factory
      GatewayReceiverFactory f = cache.createGatewayReceiverFactory();
  
      // create the receivers
      log.info("Creating " + numInstances + " gateway receivers for "
              + grd.getName());
      receivers = new HashSet();
      for (int i = 0; i < numInstances; i++) {
        receivers.add(createGatewayReceiver(grd, f));
      }
      TheGatewayReceiverConfig = gatewayReceiverConfig;

    } else if (TheGatewayReceiverConfig == null) {
      // block attempt to create receivers in multiple ways
      String s = "Gateway receivers were already created without"
               + " GatewayReceiverHelper using  an unknown, and possibly"
               + " different, configuration";
      throw new HydraRuntimeException(s);

    } else if (!TheGatewayReceiverConfig.equals(gatewayReceiverConfig)) {
      // block attempt to recreate receivers with clashing configuration name
      String s = "Gateway receivers already exist using logical gateway"
               + " receiver configuration named " + TheGatewayReceiverConfig
               + ", cannot also use " + gatewayReceiverConfig;
      throw new HydraRuntimeException(s);

    } // else it was already created with this configuration, which is fine

    return receivers;
  }

  private static GatewayReceiver createGatewayReceiver(
                 GatewayReceiverDescription grd, GatewayReceiverFactory f) {
    log.info("Configuring gateway receiver factory");
    grd.configure(f);
    log.info("Configured gateway receiver factory " + f);

    // create the gateway receiver
    log.info("Creating gateway receiver");
    GatewayReceiver receiver = f.create();
    log.info("Created gateway receiver " + gatewayReceiverToString(receiver));

    return receiver;
  }

  /**
   * Starts the previously created gateway receivers, if needed.
   */
  private static void startGatewayReceivers() {
    // get the cache
    Cache cache = CacheHelper.getCache();
    if (cache == null) {
      String s = "Cache has not been created yet";
      throw new HydraRuntimeException(s);
    }
    // start the receivers
    Set<GatewayReceiver> receivers = cache.getGatewayReceivers();
    if (receivers == null || receivers.size() == 0) {
      String s = "Gateway receivers have not been created yet.";
      throw new HydraRuntimeException(s);
    } else {
      for (GatewayReceiver receiver : receivers) {
        startGatewayReceiver(receiver);
      }
    }
  }

  /**
   * Starts the given gateway receiver, if needed.
   */
  private static void startGatewayReceiver(GatewayReceiver receiver) {
    if (!receiver.isRunning()) {
      log.info("Starting gateway receiver: "
              + gatewayReceiverToString(receiver));
      long startTimeMs = System.currentTimeMillis();
      while (true) {
        try {
          receiver.start();
          log.info("Started gateway receiver: "
             + gatewayReceiverToString(receiver));
          break;
        } catch (IOException e) {
          if (!PortHelper.retrySocketBind(e, startTimeMs)) {
            String s = "Problem starting gateway receiver"
                     + gatewayReceiverToString(receiver);
            throw new HydraRuntimeException(s, e);
          }
        }
      }
    }
  }

  /**
   * Returns the gateway receivers for the current cache, or null if no
   * gateway receivers or cache exists.
   */
  public static Set<GatewayReceiver> getGatewayReceivers() {
    Cache cache = CacheHelper.getCache();
    if (cache == null) {
      return null;
    } else {
      Set<GatewayReceiver> receivers = cache.getGatewayReceivers();
      return (receivers == null || receivers.size() == 0) ? null : receivers;
    }
  }

  /**
   * Stops the gateway receivers in the current cache, if needed.
   */
  public static synchronized void stopGatewayReceivers() {
    Set<GatewayReceiver> receivers =
                           CacheHelper.getCache().getGatewayReceivers();
    if (receivers != null) {
      for (GatewayReceiver receiver : receivers) {
        stopGatewayReceiver(receiver);
      }
    }
  }

  /**
   * Stops the given gateway receiver, if needed.
   */
  private static void stopGatewayReceiver(GatewayReceiver receiver) {
    if (receiver.isRunning()) {
      log.info("Stopping gateway receiver: "
              + gatewayReceiverToString(receiver));
      receiver.stop();
      log.info("Stopped gateway receiver: "
              + gatewayReceiverToString(receiver));
    }
  }

  /**
   * Returns the given gatewayReceiver as a string.
   */
  public static String gatewayReceiverToString(GatewayReceiver gr) {
    return GatewayReceiverDescription.gatewayReceiverToString(gr);
  }

//------------------------------------------------------------------------------
// GatewayReceiverDescription
//------------------------------------------------------------------------------

  /**
   * Returns the {@link GatewayReceiverDescription} with the given configuration
   * name from {@link GatewayReceiverPrms#names}.
   */
  public static GatewayReceiverDescription getGatewayReceiverDescription(
      String gatewayReceiverConfig) {
    if (gatewayReceiverConfig == null) {
      throw new IllegalArgumentException("gatewayReceiverConfig cannot be null");
    }
    log.info("Looking up gateway receiver config: " + gatewayReceiverConfig);
    GatewayReceiverDescription grd = TestConfig.getInstance()
        .getGatewayReceiverDescription(gatewayReceiverConfig);
    if (grd == null) {
      String s = gatewayReceiverConfig + " not found in "
               + BasePrms.nameForKey(GatewayReceiverPrms.names);
      throw new HydraRuntimeException(s);
    }
    log.info("Looked up gateway reeiver config:\n" + grd);
    return grd;
  }
}
