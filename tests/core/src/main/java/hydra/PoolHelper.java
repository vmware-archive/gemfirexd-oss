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
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.internal.NanoTimer;
import hydra.blackboard.SharedLock;
import hydra.blackboard.SharedMap;
import java.util.*;

/**
 * Helps clients use {@link PoolDescription}.  Methods are thread-safe.
 * Pools are lazily created by region creation methods in {@link RegionHelper},
 * so the pool creation methods here need only be used when not using <code>
 * RegionHelper</code> to create regions.
 */
public class PoolHelper {

//------------------------------------------------------------------------------
// Pool
//------------------------------------------------------------------------------

  /**
   * Creates the pool using the given pool configuration.  The pool is
   * configured using the {@link PoolDescription} corresponding to the pool
   * configuration from {@link PoolPrms#names}.  The name of the pool is the
   * same as its configuration name.
   *
   * @throws HydraRuntimeException if an attempt is made to reconfigure an
   *         existing pool.
   */
  public static synchronized Pool createPool(String poolConfig) {
    PoolFactory factory = getPoolFactory(poolConfig);
    return createPool(poolConfig, factory);
  }

  /**
   * Creates the pool with the given name.  The pool is configured using the
   * given factory.
   *
   * @throws HydraRuntimeException if an attempt is made to reconfigure an
   *         existing pool.
   */
  public static synchronized Pool createPool(String poolName,
                                             PoolFactory factory) {
    if (poolName == null) {
      throw new IllegalArgumentException("poolName cannot be null");
    }
    if (factory == null) {
      throw new IllegalArgumentException("factory cannot be null");
    }
    Pool pool = PoolManager.find(poolName);
    if (pool == null) {
      log("Creating pool named: " + poolName + " with attributes "
         + poolFactoryToString(poolName, factory));
      try {
        pool = factory.create(poolName);
      } catch (IllegalStateException e) {
        throw new HydraInternalException("Should not happen", e);
      }
      log("Created pool named: " + poolName);
    } else {
      if (!PoolDescription.equals(factory, pool)) {
        // block attempts to create pool with clashing attributes
        String desired = poolFactoryToString(poolName, factory);
        String existing = poolToString(pool);
        String s = "Pool " + poolName
                 + " already exists with different attributes"
                 + "\n  DESIRED = " + desired + "\n  EXISTING = " + existing;
        throw new HydraRuntimeException(s);
      } // else it was already created with this configuration, which is fine
    }
    return pool;
  }

  /*
   * Returns the pool with the given name, or null if no pool with that name
   * exists.
   */
  public static synchronized Pool getPool(String poolName) {
    if (poolName == null) {
      throw new IllegalArgumentException("poolName cannot be null");
    }
    return PoolManager.find(poolName);
  }

  /**
   * Returns the given pool as a string.
   */
  public static String poolToString(Pool pool) {
    if (pool == null) {
      throw new IllegalArgumentException("pool cannot be null");
    }
    return PoolDescription.poolToString(pool);
  }

//------------------------------------------------------------------------------
// PoolFactory
//------------------------------------------------------------------------------

  /**
   * Returns a pool factory for the given pool configuration from {@link
   * PoolPrms#names}, configured using the corresponding {@link
   * PoolDescription}.
   */
  public static PoolFactory getPoolFactory(String poolConfig) {
    // look up the pool configuration
    PoolDescription pd = getPoolDescription(poolConfig);

    // create the pool factory
    PoolFactory factory = PoolManager.createFactory();

    // configure the pool factory
    log("Configuring pool factory for config: " + poolConfig);
    pd.configure(factory);
    log("Configured pool factory: " + factory);

    // return the result
    return factory;
  }

  /**
   * Returns the given named pool factory as a string.
   */
  private static String poolFactoryToString(String poolName,
                                            PoolFactory factory) {
    if (poolName == null) {
      throw new IllegalArgumentException("poolName cannot be null");
    }
    if (factory == null) {
      throw new IllegalArgumentException("factory cannot be null");
    }
    return PoolDescription.poolFactoryToString(poolName, factory);
  }

//------------------------------------------------------------------------------
// PoolDescription
//------------------------------------------------------------------------------

  /**
   * Returns the {@link PoolDescription} with the given configuration name
   * from {@link PoolPrms#names}.
   */
  public static PoolDescription getPoolDescription(String poolConfig) {
    if (poolConfig == null) {
      throw new IllegalArgumentException("poolConfig cannot be null");
    }
    log("Looking up pool config: " + poolConfig);
    PoolDescription pd = TestConfig.getInstance()
                                   .getPoolDescription(poolConfig);
    if (pd == null) {
      String s = poolConfig + " not found in "
               + BasePrms.nameForKey(PoolPrms.names);
      throw new HydraRuntimeException(s);
    }
    log("Looked up pool config:\n" + pd);
    return pd;
  }

//------------------------------------------------------------------------------
// Contacts
//------------------------------------------------------------------------------

  /**
   * Always returns the same list of the given number of contacts, chosen from
   * all server locators in the specified distributed system that have
   * registered endpoints via {@link DistributedSystemHelper}.
   * <p>
   * If the number of contacts is {@link PoolPrms#ALL_AVAILABLE}, then all
   * available contacts in the distributed system are included, in fixed order.
   * If the distributed system is null, contacts are chosen without regard
   * to their distributed system.
   * <p>
   * This is a suitable {@link PoolPrms#contactAlgorithm} for hierarchical
   * cache tests that use <codehydraconfig/topology_hct.inc</code>, and other
   * hierarchical cache tests that do not use gateways.
   *
   * @throws HydraRuntimeException if there are not enough contacts available.
   */
  public static List getSameContacts(int numContacts,
                                     String distributedSystemName) {
    List allContacts = getAllContacts(distributedSystemName);
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("All available contacts: " + allContacts);
    }
    if (numContacts > allContacts.size()) {
      error(numContacts, allContacts, distributedSystemName);
    }

    // select the contacts
    List sameContacts;
    if (numContacts == PoolPrms.ALL_AVAILABLE) {
      sameContacts = allContacts;
    } else {
      sameContacts = allContacts.subList(0, numContacts);
    }
    return new ArrayList(sameContacts);
  }

  /**
   * Returns a round robin list of the given number of contacts, chosen from
   * all server locators in the specified distributed system that have
   * registered endpoints via {@link DistributedSystemHelper}.
   * <p>
   * If the number of contacts is {@link PoolPrms#ALL_AVAILABLE}, then all
   * available contacts in the distributed system are included, in round robin
   * order.  If the distributed system is null, contacts are chosen without
   * regard to their distributed system.
   * <p>
   * The first request for contacts gets the contacts in the order they
   * are returned from the underlying blackboard.  The second request gets the
   * contacts in order starting from the second contact and wrapping around
   * if needed.  The starting contact continues to rotate.
   * <p>
   * This is a suitable {@link PoolPrms#contactAlgorithm} for hierarchical
   * cache tests that use <codehydraconfig/topology_hct.inc</code>, and other
   * hierarchical cache tests that do not use gateways.
   *
   * @throws HydraRuntimeException if there are not enough contacts available.
   */
  public static List getRoundRobinContacts(int numContacts,
                                           String distributedSystemName) {
    List allContacts = getAllContacts(distributedSystemName);
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("All available contacts: " + allContacts);
    }
    return getRoundRobinContacts(numContacts, allContacts,
                                 distributedSystemName);
  }

  /**
   * Returns a round robin list of the given number of remote contacts, chosen
   * from all server locators in the specified distributed system that have
   * registered endpoints via {@link DistributedSystemHelper} and live on a
   * different host. 
   * <p>
   * If the number of contacts is {@link PoolPrms#ALL_AVAILABLE}, then all
   * available remote contacts in the distributed system are included, in round
   * robin order.  If the distributed system is null, contacts are chosen
   * without regard to their distributed system.
   * <p>
   * The first client to ask for contacts gets the remote contacts in the
   * order they are returned from the underlying blackboard.  The second client
   * gets the remote contacts in order starting from the second contact and
   * wrapping around if needed.  The starting contact continues to rotate.
   * <p>
   * This is a suitable {@link PoolPrms#contactAlgorithm} for multihost
   * hierarchical cache tests that use <codehydraconfig/topology_hct.inc</code>,
   * and other hierarchical cache tests that do not use gateways.
   *
   * @throws HydraRuntimeException if there are not enough contacts available.
   */
  public static List getRoundRobinRemoteHostContacts(int numContacts,
                                            String distributedSystemName) {
    // get all available contacts and toss out the local ones
    List remoteContacts = getAllContacts(distributedSystemName);
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("All available contacts: " + remoteContacts);
    }
    String localhost = HostHelper.getLocalHost();
    for (Iterator i = remoteContacts.iterator(); i.hasNext();) {
      Contact contact = (Contact)i.next();
      if (contact.getHost().equals(localhost)) {
        i.remove();
      }
    }
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("Remote host contacts: " + remoteContacts);
    }
    return getRoundRobinContacts(numContacts, remoteContacts,
                                 distributedSystemName);
  }

  /**
   * Returns a round robin list of the given number of local contacts,
   * chosen from all server locators in the specified distributed system
   * that have registered endpoints via {@link DistributedSystemHelper}
   * and live on the local host.
   * <p>
   * If the number of contacts is {@link PoolPrms#ALL_AVAILABLE}, then all
   * available local contacts in the distributed system are included, in round
   * robin order.  If the distributed system is null, contacts are chosen
   * without regard to their distributed system.
   * <p>
   * The first client to ask for contacts gets the local contacts in the
   * order they are returned from the underlying blackboard.  The second client
   * gets the local contacts in order starting from the second contact and
   * wrapping around if needed.  The starting contact continues to rotate.
   * <p>
   * This is a suitable {@link PoolPrms#contactAlgorithm} for multihost
   * hierarchical cache tests that use <codehydraconfig/topology_hct.inc</code>,
   * and other hierarchical cache tests that do not use gateways.
   *
   * @throws HydraRuntimeException if there are not enough contacts available.
   */
  public static List getRoundRobinLocalHostContacts(int numContacts,
                                           String distributedSystemName) {
    // get all available contacts and toss out the local ones
    List localContacts = getAllContacts(distributedSystemName);
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("All available contacts: " + localContacts);
    }
    String localhost = HostHelper.getLocalHost();
    for (Iterator i = localContacts.iterator(); i.hasNext();) {
      Contact contact = (Contact)i.next();
      if (!contact.getHost().equals(localhost)) {
        i.remove();
      }
    }
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("Local host contacts: " + localContacts);
    }
    return getRoundRobinContacts(numContacts, localContacts,
                                 distributedSystemName);
  }

  /**
   * Returns a round robin list of the given number of remote contacts in
   * the specified distributed system, chosen from the given list.
   *
   * @throws HydraRuntimeException if there are not enough contacts available.
   */
  private static List getRoundRobinContacts(int numContacts, List contacts,
                                            String distributedSystemName) {
    if (numContacts == PoolPrms.ALL_AVAILABLE && contacts.size() == 0) {
      return new ArrayList(); // no contacts available at this time
    }
    if (numContacts > contacts.size()) {
      error(numContacts, contacts, distributedSystemName);
    }
    // get the start index and rotate to it
    int startIndex = getRoundRobinStartIndex(contacts.size(),
                                             distributedSystemName);
    Collections.rotate(contacts, startIndex);
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("Round robin contacts: " + contacts);
    }

    // select the contacts
    List roundRobinContacts;
    if (numContacts == PoolPrms.ALL_AVAILABLE) {
      roundRobinContacts = contacts;
    } else {
      roundRobinContacts = contacts.subList(0, numContacts);
    }
    // get rid of java.util.RandomAccessSubList
    return new ArrayList(roundRobinContacts);
  }

  /**
   * Returns the next start index for round robin algorithms.
   */
  private static int getRoundRobinStartIndex(int numContacts,
                                             String distributedSystemName) {
    int startIndex = -1;
    if (distributedSystemName == null) {
      startIndex = (int)EdgeBlackboard.getInstance().getSharedCounters()
                       .incrementAndRead(EdgeBlackboard.startIndex);
    } else {
      SharedMap map = EdgeBlackboard.getInstance().getSharedMap();
      String key = "StartIndex_" + distributedSystemName;
      SharedLock lock = EdgeBlackboard.getInstance().getSharedLock();
      try {
        lock.lock();
        Integer index = (Integer)map.get(key);
        if (index == null) {
          startIndex = 1;
        } else {
          startIndex = index.intValue() + 1;
        }
        map.put(key, Integer.valueOf(startIndex));
      }
      finally {
        lock.unlock();
      }
    }
    return startIndex % numContacts;
  }

  /**
   * Returns a randomized list of the given number of contacts, chosen from all
   * server locators in the specified distributed system that have registered
   * endpoints via {@link DistributedSystemHelper}.
   * <p>
   * If the number of contacts is {@link PoolPrms#ALL_AVAILABLE}, then all
   * available contacts in the distributed system are included, in random
   * order.  If the distributed system is null, contacts are chosen without
   * regard to their distributed system.
   * <p>
   * This is the default {@link PoolPrms#contactAlgorithm}.  This is a suitable
   * {@link PoolPrms#contactAlgorithm} for hierarchical cache tests that use
   * <codehydraconfig/topology_hct.inc</code>, and other hierarchical cache
   * tests that do not use gateways.
   *
   * @throws HydraRuntimeException if there are not enough contacts available.
   */
  public static List getRandomContacts(int numContacts,
                                       String distributedSystemName) {
    // get all available contacts
    List allContacts = getAllContacts(distributedSystemName);
    if (numContacts > allContacts.size()) {
      error(numContacts, allContacts, distributedSystemName);
    }

    // determine the desired number of contacts
    int n = (numContacts == PoolPrms.ALL_AVAILABLE)
          ? allContacts.size() : numContacts;

    // select the desired number of contacts at random
    Random rng = new Random(NanoTimer.getTime());
    List randomContacts = new ArrayList();
    for (int i = 0; i < n; i++) {
      int index = rng.nextInt(allContacts.size());
      randomContacts.add(allContacts.remove(index));
    }
    return randomContacts;
  }

  /**
   * Returns a randomized list of the given number of contacts for server
   * locators in the specified distributed system that are in the same WAN site
   * as the invoking VM, chosen from the server locators that have registered
   * endpoints via {@link DistributedSystemHelper}.
   * <p>
   * If the number of contacts is {@link PoolPrms#ALL_AVAILABLE}, then all
   * matching contacts in the distributed system are included, in random
   * order.  If the distributed system is null, contacts are chosen without
   * regard to their distributed system.
   * <p>
   * This is a suitable {@link PoolPrms#contactAlgorithm} for hierarchical
   * cache tests that use <codehydraconfig/topology_wan_hct.inc </code>.
   *
   * @throws HydraRuntimeException if there are not enough contacts available.
   */
  public static List getRandomContactsInWanHctSite(int numContacts,
                                              String distributedSystemName) {
    // get all available contacts
    List allContacts = getAllContacts(distributedSystemName);

    // get the contacts in this WAN site, assuming topology_wan_hct.inc.
    int myWanSite = toWanSite(RemoteTestModule.getMyClientName());
    List matchingContacts = new ArrayList();
    for (Iterator i = allContacts.iterator(); i.hasNext();) {
      Contact contact = (Contact)i.next();
      if (toWanSite(contact.getName()) == myWanSite) {
        matchingContacts.add(contact);
      }
    }
    if (numContacts > matchingContacts.size()) {
      error(numContacts, matchingContacts, distributedSystemName);
    }

    // determine the desired number of contacts
    int n = (numContacts == PoolPrms.ALL_AVAILABLE)
          ? matchingContacts.size() : numContacts;

    // select the desired number of contacts at random
    Random rng = new Random(NanoTimer.getTime());
    List randomContacts = new ArrayList();
    for (int i = 0; i < n; i++) {
      int index = rng.nextInt(matchingContacts.size());
      randomContacts.add(matchingContacts.remove(index));
    }
    return randomContacts;
  }

  /**
   * Returns all contacts for the specified distributed system name, or all
   * available contacts if null.
   */
  private static List getAllContacts(String distributedSystemName) {
    return distributedSystemName == null
           ? DistributedSystemHelper.getContacts()
           : DistributedSystemHelper.getContacts(distributedSystemName);
  }

  /**
   * Extracts the WAN site number from a value of {@link ClientPrms#names}
   * as generated by <code>hydraconfig/topology_wan_hct.inc</code>.
   */
  public static int toWanSite(String clientName) {
    String site = clientName.substring(clientName.indexOf("_") + 1,
                                       clientName.lastIndexOf("_"));
    try {
      return Integer.parseInt(site);
    } catch (NumberFormatException e) {
      String s = clientName
               + " is not in the form <name>_<wanSiteNumber>_<itemNumber>";
      throw new HydraRuntimeException(s, e);
    }
  }

  private static void error(int numContacts, List contacts,
                            String distributedSystemName) {
    String s = null;
    if (distributedSystemName == null) {
      s = "Cannot find " + numContacts + " contacts"
        + ", there are only " + contacts.size() + ": " + contacts;
    } else {
      s = "Cannot find " + numContacts + " contacts in " + distributedSystemName
        + ", there are only " + contacts.size() + ": " + contacts;
    }
    throw new HydraRuntimeException(s);
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
