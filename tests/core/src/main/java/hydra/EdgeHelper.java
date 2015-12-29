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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.internal.BridgePoolImpl;
import com.gemstone.gemfire.cache.client.internal.Connection;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.client.internal.ServerProxy;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerTestUtil;

import hydra.blackboard.SharedCounters;
import hydra.blackboard.SharedLock;
import hydra.blackboard.SharedMap;

/**
 * Provides support for edge clients running in hydra-managed client VMs.
 */
public class EdgeHelper {

//------------------------------------------------------------------------------
// Endpoints
//------------------------------------------------------------------------------

  /**
   * Always returns the same list of the given number of endpoints, chosen from
   * all bridge servers in the specified distributed system that have
   * registered endpoints via {@link BridgeHelper}.
   * <p>
   * If the number of endpoints is {@link PoolPrms#ALL_AVAILABLE}, then all
   * available endpoints in the distributed system are included, in fixed order.
   * If the distributed system is null, endpoints are chosen without regard
   * to their distributed system.
   * <p>
   * This is a suitable {@link PoolPrms#contactAlgorithm} for hierarchical
   * cache tests that use <codehydraconfig/topology_hct.inc</code>, and other
   * hierarchical cache tests that do not use gateways.
   *
   * @throws HydraRuntimeException if there are not enough endpoints available.
   */
  public static List getSameEndpoints(int numEndpoints,
                                      String distributedSystemName) {
    List allEndpoints = getAllEndpoints(distributedSystemName);
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("All available endpoints: " + allEndpoints);
    }
    if (numEndpoints > allEndpoints.size()) {
      error(numEndpoints, allEndpoints, distributedSystemName);
    }
    // turn off product-level endpoint randomization
    CacheServerTestUtil.disableShufflingOfEndpoints();
    // stays off only through edge initialization

    // select the endpoints
    List sameEndpoints;
    if (numEndpoints == PoolPrms.ALL_AVAILABLE) {
      sameEndpoints = allEndpoints;
    } else {
      sameEndpoints = allEndpoints.subList(0, numEndpoints);
    }
    return new ArrayList(sameEndpoints);
  }

  /**
   * Returns a round robin list of the given number of endpoints in the
   * specified distributed system, chosen from all bridge servers that have
   * registered endpoints via {@link BridgeHelper}.
   * <p>
   * If the number of endpoints is {@link PoolPrms#ALL_AVAILABLE}, then all
   * available endpoints in the distributed system are included, in round robin
   * order.  If the distributed system is null, endpoints are chosen without
   * regard to their distributed system.
   * <p>
   * The first client to ask for endpoints gets the endpoints in the order they
   * are returned from the underlying blackboard.  The second client gets the
   * endpoints in order starting from the second endpoint and wrapping around
   * if needed.  The starting endpoint continues to rotate.
   * <p>
   * This is a suitable {@link PoolPrms#contactAlgorithm} for hierarchical
   * cache tests that use <codehydraconfig/topology_hct.inc</code>, and other
   * hierarchical cache tests that do not use gateways.
   *
   * @throws HydraRuntimeException if there are not enough endpoints available.
   */
  public static List getRoundRobinEndpoints(int numEndpoints,
                                            String distributedSystemName) {
    List allEndpoints = getAllEndpoints(distributedSystemName);
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("All available endpoints: " + allEndpoints);
    }
    return getRoundRobinEndpoints(numEndpoints, allEndpoints,
                                  distributedSystemName);
  }

  /**
   * Returns a round robin list of the given number of remote endpoints in the
   * specified distributed system, chosen from all bridge servers that have
   * registered endpoints via {@link BridgeHelper} and live on a different host.
   * <p>
   * If the number of endpoints is {@link PoolPrms#ALL_AVAILABLE}, then all
   * available remote endpoints in the distributed system are included, in
   * round robin order.  If the distributed system is null, endpoints are
   * chosen without regard to their distributed system.
   * <p>
   * The first client to ask for endpoints gets the remote endpoints in the
   * order they are returned from the underlying blackboard.  The second client
   * gets the remote endpoints in order starting from the second endpoint and
   * wrapping around if needed.  The starting endpoint continues to rotate.
   * <p>
   * This is a suitable {@link PoolPrms#contactAlgorithm} for multihost
   * hierarchical cache tests that use <codehydraconfig/topology_hct.inc</code>,
   * and other hierarchical cache tests that do not use gateways.
   *
   * @throws HydraRuntimeException if there are not enough endpoints available.
   */
  public static List getRoundRobinRemoteHostEndpoints(int numEndpoints,
                                            String distributedSystemName) {
    // get all available endpoints and toss out the local ones
    List remoteEndpoints = getAllEndpoints(distributedSystemName);
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("All available endpoints: " + remoteEndpoints);
    }
    String localhost = HostHelper.getLocalHost();
    for (Iterator i = remoteEndpoints.iterator(); i.hasNext();) {
      BridgeHelper.Endpoint endpoint = (BridgeHelper.Endpoint)i.next();
      if (endpoint.getHost().equals(localhost)) {
        i.remove();
      }
    }
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("Remote host endpoints: " + remoteEndpoints);
    }
    return getRoundRobinEndpoints(numEndpoints, remoteEndpoints,
                                  distributedSystemName);
  }

  /**
   * Returns a round robin list of the given number of local endpoints in the
   * specified distributed system, chosen from all bridge servers that have
   * registered endpoints via {@link BridgeHelper} and live on the local host.
   * <p>
   * If the number of endpoints is {@link PoolPrms#ALL_AVAILABLE}, then all
   * available local endpoints in the distributed system are included, in round
   * robin order.  If the distributed system is null, endpoints are chosen
   * without regard to their distributed system.
   * <p>
   * The first client to ask for endpoints gets the local endpoints in the
   * order they are returned from the underlying blackboard.  The second client
   * gets the local endpoints in order starting from the second endpoint and
   * wrapping around if needed.  The starting endpoint continues to rotate.
   * <p>
   * This is a suitable {@link PoolPrms#contactAlgorithm} for multihost
   * hierarchical cache tests that use <codehydraconfig/topology_hct.inc</code>,
   * and other hierarchical cache tests that do not use gateways.
   *
   * @throws HydraRuntimeException if there are not enough endpoints available.
   */
  public static List getRoundRobinLocalHostEndpoints(int numEndpoints,
                                           String distributedSystemName) {
    // get all available endpoints and toss out the local ones
    List localEndpoints = getAllEndpoints(distributedSystemName);
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("All available endpoints: " + localEndpoints);
    }
    String localhost = HostHelper.getLocalHost();
    for (Iterator i = localEndpoints.iterator(); i.hasNext();) {
      BridgeHelper.Endpoint endpoint = (BridgeHelper.Endpoint)i.next();
      if (!endpoint.getHost().equals(localhost)) {
        i.remove();
      }
    }
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("Local host endpoints: " + localEndpoints);
    }
    return getRoundRobinEndpoints(numEndpoints, localEndpoints,
                                  distributedSystemName);
  }

  /**
   * Returns a round robin list of the given number of remote endpoints in the
   * specified distributed system, chosen from the given list.
   *
   * @throws HydraRuntimeException if there are not enough endpoints available.
   */
  private static List getRoundRobinEndpoints(int numEndpoints, List endpoints,
                                             String distributedSystemName) {
    if (numEndpoints == PoolPrms.ALL_AVAILABLE && endpoints.size() == 0) {
      return new ArrayList(); // no endpoints available at this time
    }
    if (numEndpoints > endpoints.size()) {
      error(numEndpoints, endpoints, distributedSystemName);
    }
    // turn off product-level endpoint randomization
    CacheServerTestUtil.disableShufflingOfEndpoints();
    // stays off only through edge initialization

    // get the start index and rotate to it
    int startIndex = getRoundRobinStartIndex(endpoints.size(),
                                             distributedSystemName);
    Collections.rotate(endpoints, startIndex);
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("Round robin endpoints: " + endpoints);
    }

    // select the endpoints
    List roundRobinEndpoints;
    if (numEndpoints == PoolPrms.ALL_AVAILABLE) {
      roundRobinEndpoints = endpoints;
    } else {
      roundRobinEndpoints = endpoints.subList(0, numEndpoints);
    }
    // get rid of java.util.RandomAccessSubList
    return new ArrayList(roundRobinEndpoints);
  }

  /**
   * Returns the next start index for round robin algorithms.
   */
  private static int getRoundRobinStartIndex(int numEndpoints,
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
    return startIndex % numEndpoints;
  }

  /**
   * Returns a randomized list of the given number of endpoints in the specified
   * distributed system, chosen from all bridge servers that have registered
   * endpoints via {@link BridgeHelper}.
   * <p>
   * If the number of endpoints is {@link PoolPrms#ALL_AVAILABLE}, then all
   * available endpoints in the distributed system are included, in random
   * order.  If the distributed system is null, endpoints are chosen without
   * regard to their distributed system.
   * <p>
   * This is the default {@link PoolPrms#contactAlgorithm}.  This is a
   * suitable {@link PoolPrms#contactAlgorithm} for hierarchical cache tests
   * that use <codehydraconfig/topology_hct.inc</code>, and other hierarchical
   * cache tests that do not use gateways.
   *
   * @throws HydraRuntimeException if there are not enough endpoints available.
   */
  public static List getRandomEndpoints(int numEndpoints,
                                        String distributedSystemName) {
    // get all available endpoints
    List allEndpoints = getAllEndpoints(distributedSystemName);
    if (numEndpoints > allEndpoints.size()) {
      error(numEndpoints, allEndpoints, distributedSystemName);
    }

    // determine the desired number of endpoints
    int n = (numEndpoints == PoolPrms.ALL_AVAILABLE)
          ? allEndpoints.size() : numEndpoints;

    // select the desired number of endpoints at random
    Random rng = new Random(NanoTimer.getTime());
    List randomEndpoints = new ArrayList();
    for (int i = 0; i < n; i++) {
      int index = rng.nextInt(allEndpoints.size());
      randomEndpoints.add(allEndpoints.remove(index));
    }
    return randomEndpoints;
  }

  /**
   * Returns a round robin list of the given number of endpoints in the
   * specified distributed system for bridge servers that are in the same
   * WAN site as the invoking VM, chosen from the servers that have registered
   * endpoints via {@link BridgeHelper}.
   * <p>
   * If the number of endpoints is {@link PoolPrms#ALL_AVAILABLE}, then all
   * matching endpoints in the distributed system are included, in round robin
   * order.  If the distributed system is null, endpoints are chosen without
   * regard to their distributed system.
   * <p>
   * The first client to ask for endpoints gets the endpoints in the order they
   * are returned from the underlying blackboard.  The second client gets the
   * endpoints in order starting from the second endpoint and wrapping around
   * if needed.  The starting endpoint continues to rotate.
   * <p>
   * This is a suitable {@link PoolPrms#contactAlgorithm} for hierarchical
   * cache tests that use <codehydraconfig/topology_wan_hct.inc</code>. 
   * 
   * @throws HydraRuntimeException if there are not enough endpoints available.
   */
  public static List getRoundRobinEndpointsInWanHctSite(int numEndpoints,
                                                String distributedSystemName) {
    // get all available endpoints
    List allEndpoints = getAllEndpoints(distributedSystemName);

    // get the endpoints in this WAN site
    int wanSite = toWanSite(RemoteTestModule.getMyClientName());
    List endpoints = getAllEndpointsInWanHctSite(wanSite, numEndpoints,
                                         allEndpoints, distributedSystemName);
    if (numEndpoints == PoolPrms.ALL_AVAILABLE && endpoints.size() == 0) {
      return new ArrayList(); // no endpoints available at this time
    }
    
    // turn off product-level endpoint randomization
    CacheServerTestUtil.disableShufflingOfEndpoints();
    // stays off only through edge initialization
               
    // get the start index and rotate to it
    SharedCounters sc = EdgeBlackboard.getInstance().getSharedCounters();
    long tmp = -1;
    switch (wanSite) {
      case 1:  tmp = sc.incrementAndRead(EdgeBlackboard.startIndex1); break;
      case 2:  tmp = sc.incrementAndRead(EdgeBlackboard.startIndex2); break;
      case 3:  tmp = sc.incrementAndRead(EdgeBlackboard.startIndex3); break;
      case 4:  tmp = sc.incrementAndRead(EdgeBlackboard.startIndex4); break;
      case 5:  tmp = sc.incrementAndRead(EdgeBlackboard.startIndex5); break;
      case 6:  tmp = sc.incrementAndRead(EdgeBlackboard.startIndex6); break;
      case 7:  tmp = sc.incrementAndRead(EdgeBlackboard.startIndex7); break;
      case 8:  tmp = sc.incrementAndRead(EdgeBlackboard.startIndex8); break;
      case 9:  tmp = sc.incrementAndRead(EdgeBlackboard.startIndex9); break;
      case 10:  tmp = sc.incrementAndRead(EdgeBlackboard.startIndex10); break;
      case 11:  tmp = sc.incrementAndRead(EdgeBlackboard.startIndex11); break;
      case 12:  tmp = sc.incrementAndRead(EdgeBlackboard.startIndex12); break;
      case 13:  tmp = sc.incrementAndRead(EdgeBlackboard.startIndex13); break;
      case 14:  tmp = sc.incrementAndRead(EdgeBlackboard.startIndex14); break;
      case 15:  tmp = sc.incrementAndRead(EdgeBlackboard.startIndex15); break;
      case 16:  tmp = sc.incrementAndRead(EdgeBlackboard.startIndex16); break;
      case 17:  tmp = sc.incrementAndRead(EdgeBlackboard.startIndex17); break;
      case 18:  tmp = sc.incrementAndRead(EdgeBlackboard.startIndex18); break;
      case 19:  tmp = sc.incrementAndRead(EdgeBlackboard.startIndex19); break;
      case 20:  tmp = sc.incrementAndRead(EdgeBlackboard.startIndex20); break;
      default: String s = "Too many WAN sites: " + wanSite;
               throw new UnsupportedOperationException(s);
    } 
    int startIndex = (int)tmp % endpoints.size();
    Collections.rotate(endpoints, startIndex);
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("Round robin endpoints for WAN site " + wanSite
                             + ": " + endpoints);
    }

    // select the endpoints
    List roundRobinEndpoints;
    if (numEndpoints == PoolPrms.ALL_AVAILABLE) {
      roundRobinEndpoints = endpoints;
    } else {
      roundRobinEndpoints = endpoints.subList(0, numEndpoints);
    }
    // get rid of java.util.RandomAccessSubList
    return new ArrayList(roundRobinEndpoints);
  }

  /**
   * Returns a randomized list of the given number of endpoints in the
   * specified distributed system for bridge servers that are in the same
   * WAN site as the invoking VM, chosen from the servers that have registered
   * endpoints via {@link BridgeHelper}.
   * <p>
   * If the number of endpoints is {@link PoolPrms#ALL_AVAILABLE}, then all
   * matching endpoints in the distributed system are included, in random
   * order.  If the distributed system is null, endpoints are chosen without
   * regard to their distributed system.
   * <p>
   * This is a suitable {@link PoolPrms#contactAlgorithm} for hierarchical
   * cache tests that use <codehydraconfig/topology_wan_hct.inc</code>.
   *
   * @throws HydraRuntimeException if there are not enough endpoints available.
   */
  public static List getRandomEndpointsInWanHctSite(int numEndpoints,
                                            String distributedSystemName) {
    // get all available endpoints
    List allEndpoints = getAllEndpoints(distributedSystemName);

    // get the endpoints in this WAN site
    int wanSite = toWanSite(RemoteTestModule.getMyClientName());
    List matchingEndpoints = getAllEndpointsInWanHctSite(wanSite, numEndpoints,
                                   allEndpoints, distributedSystemName);

    // determine the desired number of endpoints
    int n = (numEndpoints == PoolPrms.ALL_AVAILABLE)
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

  /**
   * Returns all endpoints for the specified distributed system name, or all
   * available endpoints if null.
   */
  private static List getAllEndpoints(String distributedSystemName) {
    return distributedSystemName == null
           ? BridgeHelper.getEndpoints()
           : BridgeHelper.getEndpoints(distributedSystemName);
  }

  /**
   * Returns a list of all endpoints for bridge servers that are in the same
   * WAN site as the invoking VM, chosen from the servers that have registered
   * endpoints via {@link BridgeHelper}.
   *
   * @throws HydraRuntimeException if there are not enough endpoints available.
   */
  private static List getAllEndpointsInWanHctSite(int wanSite,
                                       int numEndpoints, List allEndpoints,
                                       String distributedSystemName) {
    // get the endpoints in this WAN site, assuming topology_wan_hct.inc.
    List matchingEndpoints = new ArrayList();
    for (Iterator i = allEndpoints.iterator(); i.hasNext();) {
      BridgeHelper.Endpoint endpoint = (BridgeHelper.Endpoint)i.next();
      if (toWanSite(endpoint.getName()) == wanSite) {
        matchingEndpoints.add(endpoint);
      }
    }
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("All endpoints in WAN site "
                             + wanSite + ": " + matchingEndpoints);
    }
    if (numEndpoints > matchingEndpoints.size()) {
      wanerror(numEndpoints, matchingEndpoints, wanSite, distributedSystemName);
    }
    return matchingEndpoints;
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

  /**
   * Returns an endpoint string for the given list of endpoints, suitable for
   * use in "endpoints" properties.
   */
  public static String endpointsToString(List endpoints) {
    if (endpoints == null || endpoints.size() == 0) {
      String s = "No endpoints found";
      throw new HydraRuntimeException(s);
    } else {
      String str = "";
      for (Iterator i = endpoints.iterator(); i.hasNext();) {
        BridgeHelper.Endpoint endpoint = (BridgeHelper.Endpoint)i.next();
        if (str.length() > 0) {
          str += ",";
        }
        str += endpoint;
      }
      return str;
    }
  }

  private static void error(int numEndpoints, List endpoints,
                            String distributedSystemName) {
    String s = null;
    if (distributedSystemName == null) {
      s = "Cannot find " + numEndpoints + " endpoints"
        + ", there are only " + endpoints.size() + ": " + endpoints;
    } else {
      s = "Cannot find " + numEndpoints + " endpoints in "
        + distributedSystemName
        + ", there are only " + endpoints.size() + ": " + endpoints;
    }
    throw new HydraRuntimeException(s);
  }

  private static void wanerror(int numEndpoints, List endpoints,
                               int wanSite, String distributedSystemName) {
    String s = null;
    if (distributedSystemName == null) {
      s = "Cannot find " + numEndpoints + " endpoints in WAN site " + wanSite
        + ", there are only " + endpoints.size() + ": " + endpoints;
    } else {
      s = "Cannot find " + numEndpoints + " endpoints in "
        + distributedSystemName + " in WAN site " + wanSite
        + ", there are only " + endpoints.size() + ": " + endpoints;
    }
    throw new HydraRuntimeException(s);
  }

//------------------------------------------------------------------------------
// Connection management
//------------------------------------------------------------------------------

  private static HydraThreadLocal localconnections = new HydraThreadLocal();

  /**
   * Puts the current thread local connection for this thread in the given
   * region under management, if present.
   * <p>
   * Use with {@link #restoreThreadLocalConnections} and {@link
   * #saveThreadLocalConnections} to continue connections across hydra task
   * boundaries.  This is required since GemFire stores connections in Java
   * threads, not logical hydra threads.
   */
  public static void addThreadLocalConnection(Region r) {
    LocalRegion lr = (LocalRegion)r;
    ServerProxy proxy = lr.getServerProxy();
    if (proxy != null) {
      Pool pool = (PoolImpl)proxy.getPool();
      if (pool != null && pool.getThreadLocalConnections()) {
        saveThreadLocalConnection(pool);
      }
    }
  }

  /**
   * Removes the current thread local connection for this thread in the given
   * region from management, if present.
   */
  public static void removeThreadLocalConnection(Region r) {
    LocalRegion lr = (LocalRegion)r;
    ServerProxy proxy = lr.getServerProxy();
    if (proxy != null) {
      Pool pool = (PoolImpl)proxy.getPool();
      if (pool != null) {
        getConnectionsMap().remove(pool);
      }
    }
  }

  /**
   * Restores all managed thread local connections to previously saved ones.
   */
  public static void restoreThreadLocalConnections() {
    Map connections = getConnectionsMap();
    for (Iterator i = connections.keySet().iterator(); i.hasNext();) {
      Object obj = i.next();
      Object c = connections.get(obj);
      if (c != null) {
        if (obj instanceof Pool) {
          restoreThreadLocalConnection(obj, c);
        }
      }
    }
  }

  /**
   * Saves all managed thread local connections for later restoration.
   * Ignores connections we think are current in case we failed over.
   * Resets the connections to null if the cache has been closed.
   */
  public static synchronized void saveThreadLocalConnections() {
    Map connections = getConnectionsMap();
    for (Iterator i = connections.keySet().iterator(); i.hasNext();) {
      Pool pool = (Pool)i.next();
      saveThreadLocalConnection(pool);
    }
  }

  /**
   * Resets the connection to the specified one.
   */
  private static void restoreThreadLocalConnection(Object pool, Object c) {
    ((PoolImpl)pool).setThreadLocalConnection((Connection)c);
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("Restored connection for: " + pool + " to: " + c);
    }
  }

  /**
   * Saves the current connection in a hydra thread local.
   */
  private static void saveThreadLocalConnection(Pool pool) {
    Connection c = ((PoolImpl)pool).getThreadLocalConnection();
    getConnectionsMap().put(pool, c);
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("Saved connection for: " + pool + " as: " + c);
    }
  }

  /**
   * Gets the hydra thread local for connections.
   */
  private static Map getConnectionsMap() {
    Map connections = (Map)localconnections.get();
    if (connections == null) {
      connections = new HashMap();
      setConnectionsMap(connections);
    }
    return connections;
  }

  /**
   * Sets the hydra thread local for connections.
   */
  private static void setConnectionsMap(Map connections) {
    localconnections.set(connections);
  }
}
