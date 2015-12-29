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
package com.gemstone.gemfire.cache.client;

import java.net.InetSocketAddress;
import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.query.QueryService;


/**
 * A pool for connections from a client to a set of GemFire Cache Servers.
 * <p>A single instance of this interface can be created using {@link ClientCacheFactory#create}.
 * Multiple instances may also be created using
 * {@link PoolFactory#create}.
 * A {@link PoolFactory} instance is created by calling
 * {@link PoolManager#createFactory}. So to create a default <code>Pool</code> do this:
 * <PRE>
     new ClientCacheFactory().create();
 * </PRE>
 * or this:
 * <PRE>
     PoolManager.createFactory().create("myPool");
 * </PRE>
 * Instances may also be created by declaring them in cache.xml with a <code>pool</code> element.
 * <p>Existing Pool instances can be found using {@link PoolManager#find(String)}
 * and {@link PoolManager#getAll}.
 * <p>The pool name must be configured
 * on the client regions that will use this pool by calling
 * {@link RegionFactory#setPoolName}.
 *
 * @author darrel
 * @since 5.7
 *
 */
public interface Pool {

  /**
   * Get the name of the connection pool
   * 
   * @return the name of the pool
   * @see PoolFactory#create
   */
  public String getName();

  /**
   * Returns the connection timeout of this pool.
   * @see PoolFactory#setFreeConnectionTimeout
   */
  public int getFreeConnectionTimeout();
  /**
   * Returns the load conditioning interval of this pool.
   * @see PoolFactory#setLoadConditioningInterval
   */
  public int getLoadConditioningInterval();
  /**
   * Returns the socket buffer size of this pool.
   * @see PoolFactory#setSocketBufferSize
   */
  public int getSocketBufferSize();
  /**
   * Returns the read timeout of this pool.
   * @see PoolFactory#setReadTimeout
   */
  public int getReadTimeout();
  /**
   * Get the minimum connections for this pool.
   * @see PoolFactory#setMinConnections(int)
   */
  public int getMinConnections();
  /**
   * Get the maximum connections for this pool.
   * @see PoolFactory#setMaxConnections(int)
   */
  public int getMaxConnections();
  /**
   * Get the maximum connections for this pool.
   * @see PoolFactory#setIdleTimeout(long)
   */
  public long getIdleTimeout();
  /**
   * Get the ping interval for this pool.
   * @see PoolFactory#setPingInterval(long)
   */
  public long getPingInterval();
  /**
   * Get the statistic interval for this pool.
   * @see PoolFactory#setStatisticInterval(int)
   */
  public int getStatisticInterval();
  /**
   * Get the retry attempts for this pool.
   * @see PoolFactory#setRetryAttempts(int)
   */
  public int getRetryAttempts();
  /**
   * Returns <code>true</code> if thread local connections are enabled on this pool.
   * @see PoolFactory#setThreadLocalConnections
   */
  public boolean getThreadLocalConnections();

  /**
   * Returns the true if a server-to-client subscriptions are enabled on this pool.
   * @see PoolFactory#setSubscriptionEnabled
   */
  public boolean getSubscriptionEnabled();
  /**
   * Returns true if single-hop optimisation is enabled on this pool.
   * @see PoolFactory#setPRSingleHopEnabled
   * @since 6.5
   */
  public boolean getPRSingleHopEnabled();
  /**
   * Returns the subscription redundancy level of this pool.
   * @see PoolFactory#setSubscriptionRedundancy
   */
  public int getSubscriptionRedundancy();
  /**
   * Returns the subscription message tracking timeout of this pool.
   * @see PoolFactory#setSubscriptionMessageTrackingTimeout
   */
  public int getSubscriptionMessageTrackingTimeout();
  /**
   * Returns the subscription ack interval of this pool.
   * @see PoolFactory#setSubscriptionAckInterval(int)
   */
  public int getSubscriptionAckInterval();
  
  /**
   * Returns the server group of this pool.
   * @see PoolFactory#setServerGroup
   */
  public String getServerGroup();
  /**
   * Returns true if multiuser mode is enabled on this pool.
   * @see PoolFactory#setMultiuserAuthentication(boolean)
   * @since 6.5
   */
  public boolean getMultiuserAuthentication();

  
  /**
   * Returns an unmodifiable list of {@link java.net.InetSocketAddress} of the
   * locators this pool is using. Each locator is either one
   * {@link PoolFactory#addLocator added explicitly}
   * when the pool was created or were discovered using the explicit locators.
   * <p> If a pool has no locators then it can not discover servers or locators at runtime.
   */
  public java.util.List<InetSocketAddress> getLocators();
  /**
   * Returns an unmodifiable list of {@link java.net.InetSocketAddress} of the
   * servers this pool is using. These servers where either
   * {@link PoolFactory#addServer added explicitly}
   * when the pool was created or were discovered using this pools {@link #getLocators locators}.
   */
  public java.util.List<InetSocketAddress> getServers();

  /**
   * Destroys this pool closing any connections it produced.
   * @param keepAlive
   *                whether the server should keep the durable client's
   *                subscriptions alive for the timeout period
   * @throws IllegalStateException
   *                 if the pool is still in use
   */
  public void destroy(boolean keepAlive);
  
  /**
   * Destroys this pool closing any connections it produced.
   * @throws IllegalStateException if the pool is still in use
   */
  public void destroy();
  
  /**
   * Indicates whether this Pool has been
   * destroyed.
   * 
   * @return true if the pool has been destroyed
   */
  public boolean isDestroyed();
  
  /**
   * If this pool was configured to to use thread local connections,
   * then this method will release the connection cached for the calling thread.
   * The connection will then be available for use by other threads.
   * 
   * If this pool is not using thread local connections, this method
   * will have no effect.
   */
  public void releaseThreadLocalConnection();
  
  /**
   * Returns the QueryService for this Pool. The query operations performed
   * using this QueryService will be executed on the servers that are associated
   * with this pool.
   * 
   * @return the QueryService
   */
  public QueryService getQueryService();
}
