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
import com.gemstone.gemfire.cache.wan.GatewaySender;
import com.gemstone.gemfire.cache.wan.GatewaySenderFactory;
import java.util.*;

/**
 * Provides support for gateway senders running in hydra-managed client VMs.
 */
public class GatewaySenderHelper {

  private static Map<String,Set<String>> SerialGatewaySenderIds
                                         = new HashMap();
  private static Map<String,Set<String>> ParallelGatewaySenderIds
                                         = new HashMap();
  private static Integer DistributedSystemId;
  private static String DistributedSystemName;
  private static LogWriter log = Log.getLogWriter();

//------------------------------------------------------------------------------
// GatewaySender
//------------------------------------------------------------------------------

  /**
   * Creates and starts gateway senders in the current cache using the given
   * gateway sender configuration.  If {@link GatewaySenderPrms#manualStart}
   * is false (default), each sender is started as soon as it is created.
   * @see #createGatewaySenders(String)
   * @see #startGatewaySenders(String)
   */
  public static synchronized Set<GatewaySender> createAndStartGatewaySenders(
                                                String gatewaySenderConfig) {
    Set<GatewaySender> senders = createGatewaySenders(gatewaySenderConfig);
    startGatewaySenders(gatewaySenderConfig);
    return senders;
  }

  /**
   * Creates gateway senders in the current cache.  If {@link GatewaySenderPrms
   * #manualStart} is false (default), each sender is started as soon as it
   * is created.  The senders are configured using the {@link
   * GatewaySenderDescription} corresponding to the given configuration from
   * {@link GatewaySenderPrms#names}.
   * <p>
   * One sender is created for each distributed system returned by {@link
   * GatewaySenderPrms#remoteDistributedSystemsAlgorithm}.  The id for each
   * sender is formed from its name as given in {@link GatewaySenderPrms#names}
   * plus the local and remote distributed systems.  For example, a sender
   * named "fred" from local distributed system "ds_3" to remote distributed
   * system "ds_1" would have id set to "fred_ds_3_to_ds_1".
   * <p>
   * This method is thread-safe.  The gateway senders for a given logical
   * gateway sender configuration will only be created once.
   *
   * @throws HydraRuntimeException if an attempt is made to reconfigure an
   *         existing gateway sender.
   */
  public static synchronized Set<GatewaySender> createGatewaySenders(
                                                String gatewaySenderConfig) {
    Set<GatewaySender> senders = getGatewaySenders(gatewaySenderConfig);
    if (senders == null || senders.size() == 0) {

      // get the cache
      Cache cache = CacheHelper.getCache();
      if (cache == null) {
        String s = "Cache has not been created yet";
        throw new HydraRuntimeException(s);
      }

      // look up the gateway sender configuration
      GatewaySenderDescription gsd =
             getGatewaySenderDescription(gatewaySenderConfig);

      // create the disk store
      DiskStoreHelper.createDiskStore(gsd.getDiskStoreDescription().getName());

      // create the gateway senders
      String localds = getDistributedSystemName();
      senders = createGatewaySenders(gsd, localds, cache);
    }
    return senders;
  }

  /**
   * Creates a gateway sender for each remote distributed system.
   * They will automatically start when manual start is false.
   */
  private static Set<GatewaySender> createGatewaySenders(
                 GatewaySenderDescription gsd, String localds, Cache cache) {
    Set<GatewaySender> senders = new HashSet();
    Set<String> remoteDistributedSystems = gsd.getRemoteDistributedSystems();
    if (remoteDistributedSystems.size() > 0) {
      // configure the factory
      GatewaySenderFactory f = cache.createGatewaySenderFactory();
      log.info("Configuring gateway sender factory");
      gsd.configure(f);
      log.info("Configured gateway sender factory " + f);

      // create a gateway sender for each remote distributed system
      String starting = gsd.getManualStart() ? "" : "and starting ";
      String started = gsd.getManualStart() ? "" : "and started ";
      log.info("Creating " + starting + "gateway senders for " + gsd.getName()
              + " with remote distributed systems " + remoteDistributedSystems);
      for (String remoteds : remoteDistributedSystems) {
        String id = getGatewaySenderId(gsd.getName(), localds, remoteds);
        log.info("Creating " + starting + "gateway sender: " + id);
        int remoteid = DistributedSystemHelper.getDistributedSystemId(remoteds);
        GatewaySender sender = f.create(id, remoteid);
        senders.add(sender);
        saveGatewaySenderId(gsd.getName(), gsd.getParallel(), id);
        log.info("Created " + started + "gateway sender: "
                                      + gatewaySenderToString(sender));
      }
    } else {
     log.info("Created no gateway senders since there are no remote distributed systems");
    }
    return senders;
  }

  /**
   * Starts the existing gateway senders in the current cache, if they
   * have not already been started.
   * @throws HydraRuntimeException if no gateway senders have been created.
   */
  public static synchronized void startGatewaySenders() {
    Set<GatewaySender> senders = getGatewaySenders();
    if (senders == null || senders.size() == 0) {
      String s = "Gateway senders have not been created yet";
      throw new HydraRuntimeException(s);
    }
    // start the senders if necessary
    for (GatewaySender sender : senders) {
      startGatewaySender(sender);
    }
  }

  /**
   * Starts the gateway senders for the current cache matching the given
   * gateway sender configuration from {@link GatewaySenderPrms#names}, if
   * they have not already been started.
   * @throws HydraRuntimeException if no gateway senders have been created.
   */
  public static synchronized void startGatewaySenders(
                                  String gatewaySenderConfig) {
    Set<GatewaySender> senders = getGatewaySenders(gatewaySenderConfig);
    if (senders == null || senders.size() == 0) {
      String s = "Gateway senders for " + gatewaySenderConfig
               + " have not been created yet";
      throw new HydraRuntimeException(s);
    }
    // start the senders if necessary
    for (GatewaySender sender : senders) {
      startGatewaySender(sender);
    }
  }

  /**
   * Starts the given gateway sender, if needed.
   */
  private static void startGatewaySender(GatewaySender sender) {
    if (!sender.isRunning()) {
      log.info("Starting gateway sender: " + gatewaySenderToString(sender));
      try {
        sender.start();
      } catch (Exception e) {
        String s = "Problem starting gateway sender"
                 + gatewaySenderToString(sender);
        throw new HydraRuntimeException(s, e);
      }
      log.info("Started gateway sender: " + gatewaySenderToString(sender));
    }
  }

  /**
   * Returns the gateway senders for the current cache, or null if no gateway
   * senders or cache exists.
   */
  public static Set<GatewaySender> getGatewaySenders() {
    Cache cache = CacheHelper.getCache();
    if (cache == null) {
      return null;
    } else {
      Set<GatewaySender> senders = cache.getGatewaySenders();
      return (senders == null || senders.size() == 0) ? null : senders;
    }
  }

  /**
   * Returns the gateway sender for the current cache matching the given
   * gateway sender id, or null if no matching gateway sender id exists.
   */
  public static GatewaySender getGatewaySender(String senderID) {
    Set<GatewaySender> senders = getGatewaySenders();
    if(senders != null){
      for (GatewaySender s : senders){
        if(s.getId().equals(senderID)){
          return s;
        }
      }
    }
    return null;
  }
  
  /**
   * Returns the gateway senders for the current cache matching the given
   * gateway sender configuration from {@link GatewaySenderPrms#names}, or
   * null if no gateway senders or cache exist.
   */
  public static Set<GatewaySender> getGatewaySenders(
                                   String gatewaySenderConfig) {
    Set<GatewaySender> senders = getGatewaySenders();
    if (senders == null || senders.size() == 0) {
      return null;
    } else {
      Set<GatewaySender> matchingSenders = new HashSet();
      GatewaySenderDescription gsd =
             getGatewaySenderDescription(gatewaySenderConfig);
      for (GatewaySender sender : senders) {
        if (gsd.getName().equals(getGatewaySenderConfigName(sender.getId()))) {
          matchingSenders.add(sender);
        }
      }
      return matchingSenders.size() == 0 ? null : matchingSenders;
    }
  }

  /**
   * Stops the gateway senders in the current cache, if any.
   */
  public static synchronized void stopGatewaySenders() {
    Set<GatewaySender> senders = getGatewaySenders();
    if (senders != null) {
      for (GatewaySender sender : senders) {
        stopGatewaySender(sender);
      }
    }
  }

  /**
   * Stops the gateway senders in the current cache matching the given
   * gateway sender configuration from {@link GatewaySenderPrms#names}, if any.
   */
  public static synchronized void stopGatewaySenders(
                                  String gatewaySenderConfig) {
    Set<GatewaySender> senders = getGatewaySenders(gatewaySenderConfig);
    if (senders != null) {
      for (GatewaySender sender : senders) {
        stopGatewaySender(sender);
      }
    }
  }

  /**
   * Stops the given gateway sender, if needed.
   */
  private static void stopGatewaySender(GatewaySender sender) {
    if (sender.isRunning()) {
      log.info("Stopping gateway sender: " + gatewaySenderToString(sender));
      sender.stop();
      log.info("Stopped gateway sender: " + gatewaySenderToString(sender));
    }
  }

  /**
   * Returns the given gatewaySender as a string.
   */
  public static String gatewaySenderToString(GatewaySender gs) {
    return GatewaySenderDescription.gatewaySenderToString(gs);
  }

//------------------------------------------------------------------------------
// GatewaySenderDescription
//------------------------------------------------------------------------------

  /**
   * Returns the {@link GatewaySenderDescription} with the given configuration
   * name from {@link GatewaySenderPrms#names}.
   */
  public static GatewaySenderDescription getGatewaySenderDescription(
                                         String gatewaySenderConfig) {
    if (gatewaySenderConfig == null) {
      throw new IllegalArgumentException("gatewaySenderConfig cannot be null");
    }
    log.info("Looking up gateway sender config: " + gatewaySenderConfig);
    GatewaySenderDescription gsd = TestConfig.getInstance()
        .getGatewaySenderDescription(gatewaySenderConfig);
    if (gsd == null) {
      String s = gatewaySenderConfig + " not found in "
               + BasePrms.nameForKey(GatewaySenderPrms.names);
      throw new HydraRuntimeException(s);
    }
    log.info("Looked up gateway sender config:\n" + gsd);
    return gsd;
  }

//------------------------------------------------------------------------------
// Remote distributed system algorithms
//------------------------------------------------------------------------------

  /**
   * Returns all remote distributed system names from {@link GemFirePrms
   * #distributedSystem}, a possibly empty set.
   */
  public static SortedSet<String> getRemoteDistributedSystems() {
    SortedSet<String> remoteds = getDistributedSystems();
    String localds = getDistributedSystemName();
    remoteds.remove(localds);
    return remoteds;
  }

  /**
   * Returns the next distributed system name alphabetically after this one,
   * wrapping around as needed, from {@link GemFirePrms#distributedSystem},
   * a possibly empty set.
   */
  public static SortedSet<String> getRingDistributedSystems() {
    SortedSet<String> ringds = new TreeSet();
    String localds = getDistributedSystemName();
    SortedSet<String> allds = getDistributedSystems();
    SortedSet<String> tailds = allds.tailSet(localds + "\0");
    if (tailds.size() == 0 && allds.size() > 1) { // wrap around to first one
      ringds.add(allds.first());
    } else if (tailds.size() > 0) { // pick next one over
      ringds.add(tailds.first());
    } // else leave empty
    return ringds;
  }

  /**
   * Returns remote distributed system names from {@link GemFirePrms
   * #distributedSystem} that satisfy a hub and spoke topology, a possibly
   * empty set.  One distributed system, the first alphabetically, is
   * chosen as the hub, for which all remote distributed systems are returned,
   * while for non-hubs, only the hub is returned.
   */
  public static SortedSet<String> getHubAndSpokeDistributedSystems() {
    String localds = getDistributedSystemName();
    SortedSet<String> allds = getDistributedSystems();
    if (localds.equals(allds.first())) { // hub, return everyone else
      allds.remove(localds);
      return allds;
    } else { // spoke, return hub
      SortedSet<String> hubds = new TreeSet();
      hubds.add(allds.first());
      return hubds;
    }
  }

  /**
   * Returns all distributed system names from {@link GemFirePrms
   * #distributedSystem}, a possibly empty set.
   */
  private static SortedSet<String> getDistributedSystems() {
    SortedSet<String> allds = new TreeSet();
    for (GemFireDescription gfd :
                TestConfig.getInstance().getGemFireDescriptions().values()) {
      String ds = gfd.getDistributedSystem();
      if (!ds.equals(GemFirePrms.LONER)) {
        allds.add(ds);
      }
    }
    return allds;
  }

  public static synchronized int getDistributedSystemId() {
    if (DistributedSystemId == null) {
      DistributedSystemId =
                 DistributedSystemHelper.getDistributedSystemId();
    }
    return DistributedSystemId;
  }

  public static synchronized String getDistributedSystemName() {
    if (DistributedSystemName == null) {
      DistributedSystemName =
                 DistributedSystemHelper.getDistributedSystemName();
    }
    return DistributedSystemName;
  }

//------------------------------------------------------------------------------
// GatewaySender IDs
//------------------------------------------------------------------------------

  /**
   * Creates gateway sender ids for each remote distributed system for the
   * specified logical gateway sender configuration.
   */
  public static synchronized void createGatewaySenderIds(String gatewaySenderConfig) {
    if (getGatewaySenderIds(gatewaySenderConfig).size() == 0) {
      GatewaySenderDescription gsd = getGatewaySenderDescription(gatewaySenderConfig);
      Set<String> remoteDistributedSystems = gsd.getRemoteDistributedSystems();
      if (remoteDistributedSystems.size() > 0) {
        String localds = getDistributedSystemName();
        log.info("Creating gateway sender ids for " + gsd.getName()
                + " with remote distributed systems " + remoteDistributedSystems);
        for (String remoteds : remoteDistributedSystems) {
          String id = getGatewaySenderId(gsd.getName(), localds, remoteds);
          saveGatewaySenderId(gsd.getName(), gsd.getParallel(), id);
        }
        log.info("Created gateway sender ids for " + gsd.getName()
                + ": " + getGatewaySenderIds(gatewaySenderConfig));
      } else {
       log.info("Created no gateway sender ids since there are no remote distributed systems");
      }
    }
  }

  /**
   * Returns all gateway sender ids for the specified logical gateway sender
   * configuration that have been created using {@link #createGatewaySenderIds
   * (String)} or {@link #createGatewaySenders(String)}, a possibly empty set.
   */
  public static Set<String> getGatewaySenderIds(String gatewaySenderConfig) {
    Set<String> ids = new HashSet();
    ids.addAll(getSerialGatewaySenderIds(gatewaySenderConfig));
    ids.addAll(getParallelGatewaySenderIds(gatewaySenderConfig));
    return ids;
  }

  /**
   * Returns the serial gateway sender ids for the specified logical gateway
   * sender configuration that have been created using {@link
   * {@link #createGatewaySenderIds(String)} or {@link #createGatewaySenders
   * (String)}, a possibly empty set.
   * #createGatewaySenders(String)}, a possibly empty list.
   */
  public static Set<String> getSerialGatewaySenderIds(
                            String gatewaySenderConfig) {
    Set<String> ids = new HashSet();
    Set<String> serIds = SerialGatewaySenderIds.get(gatewaySenderConfig);
    if (serIds != null) {
      ids.addAll(serIds);
    }
    return ids;
  }

  /**
   * Returns the parallel gateway sender ids for the specified logical gateway
   * sender configuration that have been created using {@link
   * #createGatewaySenderIds(String)} or {@link #createGatewaySenders(String)},
   * a possibly empty set.
   */
  public static Set<String> getParallelGatewaySenderIds(
                            String gatewaySenderConfig) {
    Set<String> ids = new HashSet();
    Set<String> parIds = ParallelGatewaySenderIds.get(gatewaySenderConfig);
    if (parIds != null) {
      ids.addAll(parIds);
    }
    return ids;
  }

  /**
   * Stores the gateway sender id in the global GatewaySenderIds,
   * mapped by logical gateway sender configuration name. Keep parallel
   * and serial gateway sender ids separate so they can be looked up separately.
   */
  protected static synchronized void saveGatewaySenderId(
                   String gatewaySenderConfig, boolean parallel, String id) {
    Map<String,Set<String>> allids = parallel ? ParallelGatewaySenderIds
                                              : SerialGatewaySenderIds;
    Set<String> ids = allids.get(gatewaySenderConfig);
    if (ids == null) {
      ids = new HashSet();
      allids.put(gatewaySenderConfig, ids);
    }
    if (!ids.contains(id)) {
      ids.add(id);
    }
  }

  private static String getGatewaySenderId(String gatewaySenderConfig,
                                           String ds1, String ds2) {
    return gatewaySenderConfig + "_" + ds1 + "_to_" + ds2;
  }

  /**
   * Returns the gateway sender configuration from the given gateway sender id.
   */
  public static String getGatewaySenderConfigName(String id) {
    return id.substring(0,id.indexOf('_'));
  }
}
