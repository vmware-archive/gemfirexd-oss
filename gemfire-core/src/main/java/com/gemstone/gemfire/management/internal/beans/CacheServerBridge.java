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
package com.gemstone.gemfire.management.internal.beans;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CqClosedException;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.cache.query.internal.CqService;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.server.ServerLoad;
import com.gemstone.gemfire.cache.server.ServerLoadProbe;
import com.gemstone.gemfire.cache.server.internal.ServerMetricsImpl;
import com.gemstone.gemfire.cache.util.BridgeMembershipListener;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.admin.ClientHealthMonitoringRegion;
import com.gemstone.gemfire.internal.admin.remote.ClientHealthStats;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.tier.InternalBridgeMembership;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientProxyMembershipID;
import com.gemstone.gemfire.internal.process.PidUnavailableException;
import com.gemstone.gemfire.internal.process.ProcessUtils;
import com.gemstone.gemfire.management.ClientHealthStatus;
import com.gemstone.gemfire.management.ServerLoadData;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.gemstone.gemfire.management.internal.beans.stats.StatType;
import com.gemstone.gemfire.management.internal.beans.stats.StatsAverageLatency;
import com.gemstone.gemfire.management.internal.beans.stats.StatsKey;
import com.gemstone.gemfire.management.internal.beans.stats.StatsRate;

/**
 * Represents the GemFire CacheServer . Provides data and notifications about
 * server, subscriptions,durable queues and indices
 * 
 * @author rishim
 * 
 */
public class CacheServerBridge extends ServerBridge{  

  private CacheServer cacheServer;

  private GemFireCacheImpl cache;

  private QueryService qs;

  /**
   * log writer, or null if there is no distributed system available
   */
  private LogWriterI18n logger = InternalDistributedSystem.getLoggerI18n();
   
  private StatsRate clientNotificationRate;
  
  private StatsAverageLatency clientNotificatioAvgLatency;

  protected StatsRate queryRequestRate;
  
  private MemberMBeanBridge memberMBeanBridge;

  private BridgeMembershipListener membershipListener;

  protected static int identifyPid(final LogWriterI18n logger) {
    try {
      return ProcessUtils.identifyPid();
    }
    catch (PidUnavailableException e) {
      if (logger.fineEnabled()) {
        logger.fine(e);
      }
      return 0;
    }
  }

  public CacheServerBridge(CacheServer cacheServer) {
    super(cacheServer);
    this.cacheServer = cacheServer;
    this.cache = GemFireCacheImpl.getInstance();
    this.qs = cache.getQueryService();    
    initializeCacheServerStats();
  }

  // Dummy constructor for testing purpose only TODO why is this public then?
  public CacheServerBridge() {
    super();
    initializeCacheServerStats();
  }

  public void setMemberMBeanBridge(MemberMBeanBridge memberMBeanBridge) {
    this.memberMBeanBridge = memberMBeanBridge;
  }

  public void stopMonitor(){
    super.stopMonitor();
    monitor.stopListener();    
  }

  private void initializeCacheServerStats() {
    
    clientNotificationRate = new StatsRate(
        StatsKey.NUM_CLIENT_NOTIFICATION_REQUEST, StatType.INT_TYPE, monitor);
    
    clientNotificatioAvgLatency = new StatsAverageLatency(
        StatsKey.NUM_CLIENT_NOTIFICATION_REQUEST, StatType.INT_TYPE,
        StatsKey.CLIENT_NOTIFICATION_PROCESS_TIME, monitor);
    
    
    queryRequestRate = new StatsRate(StatsKey.QUERY_REQUESTS, StatType.INT_TYPE, monitor);
  }
  
  /**
   * Returns the configured buffer size of the socket connection for this server
   **/
  public int getSocketBufferSize() {
    return cacheServer.getSocketBufferSize();
  }
  
  public boolean getTcpNoDelay() {
    return cacheServer.getTcpNoDelay();
  }

  /** port of the server **/
  public int getPort() {
    return cacheServer.getPort();
  }

  public int getCapacity() {
    if (cacheServer.getClientSubscriptionConfig() != null) {
      return cacheServer.getClientSubscriptionConfig().getCapacity();
    }
    return 0;
  }

  /** disk store name for overflow **/
  public String getDiskStoreName() {
    if (cacheServer.getClientSubscriptionConfig() != null) {
      return cacheServer.getClientSubscriptionConfig().getDiskStoreName();
    }

    return null;
  }

  /** Returns the maximum allowed client connections **/
  public int getMaxConnections() {
    return cacheServer.getMaxConnections();
  }

  /**
   * Get the frequency in milliseconds to poll the load probe on this cache
   * server.
   **/
  public long getLoadPollInterval() {
    return cacheServer.getLoadPollInterval();
  }

  /** Get the load probe for this cache server **/
  public ServerLoadData fetchLoadProbe() {
    ServerLoadProbe probe = cacheServer.getLoadProbe();
    ServerLoad load = probe.getLoad(new ServerMetricsImpl(cacheServer
        .getMaxConnections()));
    ServerLoadData data = new ServerLoadData(load.getConnectionLoad(), load
        .getSubscriptionConnectionLoad(), load.getLoadPerConnection(), load
        .getLoadPerSubscriptionConnection());
    return data;
  }

  /**
   * Returns the maxium number of threads allowed in this server to service
   * client requests.
   **/
  public int getMaxThreads() {

    return cacheServer.getMaxThreads();
  }

  /** Sets maximum number of messages that can be enqueued in a client-queue. **/
  public int getMaximumMessageCount() {

    return cacheServer.getMaximumMessageCount();
  }

  /** Returns the maximum amount of time between client pings **/
  public int getMaximumTimeBetweenPings() {

    return cacheServer.getMaximumTimeBetweenPings();
  }

  /**
   * Returns the time (in seconds ) after which a message in the client queue
   * will expire.
   **/
  public int getMessageTimeToLive() {
    return cacheServer.getMessageTimeToLive();
  }

  /** is the server running **/
  public boolean isRunning() {
    return cacheServer.isRunning();
  }

  /**
   * Returns the eviction policy that is executed when capacity of the client
   * queue is reached
   **/
  public String getEvictionPolicy() {
    if (cacheServer.getClientSubscriptionConfig() != null) {
      return cacheServer.getClientSubscriptionConfig().getEvictionPolicy();
    }

    return null;
  }

  /**
   * The hostname or IP address to pass to the client as the loca- tion where
   * the server is listening. When the server connects to the locator it tells
   * the locator the host and port where it is listening for client connections.
   * If the host the server uses by default is one that the client can’t
   * translate into an IP address, the client will have no route to the server’s
   * host and won’t be able to find the server. For this situation, you must
   * supply the server’s alternate hostname for the locator to pass to the cli-
   * ent. If null, the server’s bind-address (page 177) setting is used.
   * Default: null.
   **/
  public String getHostnameForClients() {
    return cacheServer.getHostnameForClients();
  }

  /**
   * The hostname or IP address that the server is to listen on for client
   * connections. If null, the server listens on the machine’s default address.
   * Default: null.
   */
  public String getBindAddress() {
    return cacheServer.getBindAddress();
  }


  public String[] getContinuousQueryList() {
    CqService cqService = CqService.getRunningCqService();
    if (cqService != null) {
      CqQuery[] allCqs = cqService.getAllCqs();
      if (allCqs != null && allCqs.length > 0) {
        String[] allCqStr = new String[allCqs.length];
        for (int i = 0; i < allCqs.length; i++) {
          allCqStr[i] = allCqs[i].getName();
        }
        return allCqStr;
      }
    }

    return ManagementConstants.NO_DATA_STRING;
  }

  /**
   * Gets currently executing query count
   */
  public long getRegisteredCQCount() {
    CqService cqService = CqService.getRunningCqService();
    if (cqService != null) {
      CqQuery[] allCqs = cqService.getAllCqs();
      return allCqs != null && allCqs.length > 0 ? allCqs.length : 0;
    }
    return 0;
  }
  
  public String[] getIndexList() {
    Collection<Index> idxs = qs.getIndexes();
    if (!idxs.isEmpty()) {
      Iterator<Index> idx = idxs.iterator();
      String[] indexList = new String[idxs.size()];
      int i = 0;
      while (idx.hasNext()) {
        Index index = idx.next();
        indexList[i] = index.getName();
        i++;

      }
      return indexList;
    }

    return ManagementConstants.NO_DATA_STRING;
  }

  /**
   * 
   * @return a list of client Ids connected to this particular server instance
   */
  public String[] listClientIds() throws Exception {

    String[] allConnectedClientStr = null;
    try {

      Collection<CacheClientProxy> clientProxies = acceptor.getCacheClientNotifier().getClientProxies();
      if (clientProxies.size() > 0) {
        allConnectedClientStr = new String[clientProxies.size()];
        int j = 0;
        for (CacheClientProxy p : clientProxies) {
          StringBuffer buffer = new StringBuffer();
          buffer.append("[").append(p.getProxyID()).append("; port=").append(p.getRemotePort()).append("; primary=").append(p.isPrimary())
              .append("]");

          allConnectedClientStr[j] = buffer.toString();
          j++;
        }
        return allConnectedClientStr;
      } else {
        return new String[0];
      }

    } catch (Exception e) {
      throw new Exception(e.getMessage());
    }
  }
  
  

  /**
   * 
   * @param clientId
   * @return stats for a given client ID
   */
  public ClientHealthStatus showClientStats(String clientId) throws Exception {

    try {
      if (acceptor != null && acceptor.getCacheClientNotifier() != null) {
        Collection<CacheClientProxy> clientProxies = acceptor.getCacheClientNotifier().getClientProxies();
        for (CacheClientProxy p : clientProxies) {
          StringBuffer buffer = new StringBuffer();
          buffer.append("[").append(p.getProxyID()).append("; port=").append(p.getRemotePort()).append("; primary=")
              .append(p.isPrimary()).append("]");

          if (buffer.toString().equals(clientId)) {
            ClientHealthStatus status = getClientHealthStatus(p);
            return status;
          }
        }

      }
    } catch (Exception e) {
      throw new Exception(e.getMessage());
    }
    return null;
  }
  
  
  /**
   * @return stats for a all clients
   */
  public ClientHealthStatus[] showAllClientStats() throws Exception {
    List<ClientHealthStatus> clientHealthStatusList = null;
    try {
      if (acceptor != null && acceptor.getCacheClientNotifier() != null) {
        Collection<CacheClientProxy> clientProxies = acceptor.getCacheClientNotifier().getClientProxies();

        if (clientProxies.size() > 0) {
          clientHealthStatusList = new ArrayList<ClientHealthStatus>();
        } else {
          return new ClientHealthStatus[0];
        }

        for (CacheClientProxy p : clientProxies) {
          ClientHealthStatus status = getClientHealthStatus(p);
          if(status != null){
            clientHealthStatusList.add(status);
          }
          
        }

      }
      ClientHealthStatus[] statusArr = new ClientHealthStatus[clientHealthStatusList.size()];
      return clientHealthStatusList.toArray(statusArr);

    } catch (Exception e) {
      throw new Exception(e.getMessage());
    }

  }
  
  private ClientHealthStatus getClientHealthStatus(CacheClientProxy p) {
    
  
    ClientHealthStatus status = new ClientHealthStatus();
    ClientProxyMembershipID proxyID = p.getProxyID();
    
    if(!p.isConnected() && !proxyID.isDurable()){
      return null;
    }

    Region clientHealthMonitoringRegion = ClientHealthMonitoringRegion.getInstance((GemFireCacheImpl) cache);

    String clientName = proxyID.getDSMembership();

    StringBuffer buffer = new StringBuffer();
    buffer.append("[").append(proxyID).append("; port=").append(p.getRemotePort()).append("; primary=").append(
        p.isPrimary()).append("]");

    status.setClientId(buffer.toString());
    status.setUpTime(p.getUpTime());
    status.setQueueSize(p.getQueueSizeStat());
    status.setName(clientName);
    status.setHostName(p.getSocketHost());

    ClientHealthStats stats = (ClientHealthStats) clientHealthMonitoringRegion.get(clientName);

    if (stats != null) {
      status.setCpus(stats.getCpus());
      status.setNumOfCacheListenerCalls(stats.getNumOfCacheListenerCalls());
      status.setNumOfGets(stats.getNumOfGets());
      status.setNumOfMisses(stats.getNumOfMisses());
      status.setNumOfPuts(stats.getNumOfPuts());
      status.setNumOfThreads(stats.getNumOfThreads());
      status.setProcessCpuTime(stats.getProcessCpuTime());
    }
    return status;
  }


  /**
   * closes a continuous query and releases all the resources associated with
   * it.
   * 
   * @param queryName
   */
  public void closeContinuousQuery(String queryName) throws Exception{
    CqService cqService = CqService.getRunningCqService();
    if (cqService != null) {
      CqQuery[] allCqs = cqService.getAllCqs();
      for (CqQuery query : allCqs) {
        if (query.getName().equals(queryName)) {
          try {
            query.close();
            return;
          } catch (CqClosedException e) {
            throw new Exception(e.getMessage());
          } catch (CqException e) {
            throw new Exception(e.getMessage());
          }

        }
      }
    }
  }

  /**
   * Execute a continuous query
   * 
   * @param queryName
   */
  public void executeContinuousQuery(String queryName) throws Exception{
    CqService cqService = CqService.getRunningCqService();
    if (cqService != null) {
      CqQuery[] allCqs = cqService.getAllCqs();
      for (CqQuery query : allCqs) {
        if (query.getName().equals(queryName)) {
          try {
            query.execute();
            return;
          } catch (CqClosedException e) {
            throw new Exception(e.getMessage());
          } catch (CqException e) {
            throw new Exception(e.getMessage());
          } catch (RegionNotFoundException e) {
            throw new Exception(e.getMessage());
          }

        }
      }
    }
    
  }

  /**
   * Stops a given query witout releasing any of the resources associated with
   * it.
   * 
   * @param queryName
   */
  public void stopContinuousQuery(String queryName) throws Exception{
    CqService cqService = CqService.getRunningCqService();
    if (cqService != null) {
      CqQuery[] allCqs = cqService.getAllCqs();
      for (CqQuery query : allCqs) {
        if (query.getName().equals(queryName)) {
          try {
            query.stop();
            return ;
          } catch (CqClosedException e) {
            throw new Exception(e.getMessage());
          } catch (CqException e) {
            throw new Exception(e.getMessage());
          }

        }
      }
    }
  }

  /**
   * remove a given index
   * 
   * @param indexName
   */
  public void removeIndex(String indexName) throws Exception{
    try{
      Collection<Index> idxs = qs.getIndexes();
      if (!idxs.isEmpty()) {
        Iterator<Index> idx = idxs.iterator();
        while (idx.hasNext()) {
          Index index = idx.next();
          if (index.getName().equals(indexName)) {
            qs.removeIndex(index);
          }
          return ;
        }
      }
    }catch(Exception e){
      throw new Exception(e.getMessage());
    }


  }



  public int getIndexCount() {
    return qs.getIndexes().size();
  }

  
  public int getNumClientNotificationRequests() {
    return getStatistic(StatsKey.NUM_CLIENT_NOTIFICATION_REQUEST).intValue();
  }

  public long getClientNotificationAvgLatency() {
    return clientNotificatioAvgLatency.getAverageLatency();
  }

 
  public float getClientNotificationRate() {
    return clientNotificationRate.getRate();
  }
  
  public float getQueryRequestRate() {
    return queryRequestRate.getRate();
  }

  public long getTotalIndexMaintenanceTime() {    
    return memberMBeanBridge.getTotalIndexMaintenanceTime();
  }
  

  public long getActiveCQCount() {
    CqService cqService = CqService.getRunningCqService();
    if(cqService != null){
      return cqService.getCqStatistics().numCqsActive();
    }
    return 0;
  }

  public int getNumSubscriptions() {
    Map clientProxyMembershipIDMap = InternalBridgeMembership
        .getClientQueueSizes();

    return clientProxyMembershipIDMap.keySet().size();
  }

  public void setBridgeMembershipListener(
      BridgeMembershipListener membershipListener) {
    this.membershipListener = membershipListener;
  }
  
  public BridgeMembershipListener getBridgeMembershipListener() {
    return this.membershipListener;
  }
  

}
