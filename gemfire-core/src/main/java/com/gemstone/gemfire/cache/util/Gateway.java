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

package com.gemstone.gemfire.cache.util;

import java.io.IOException;
import java.util.List;

import com.gemstone.gemfire.cache.GatewayException;

/**
 * A <code>Gateway</code> represents a local proxy for a remote
 * WAN (or distributed system).
 *
 * @since 4.2
 */
public interface Gateway {

  /**
   * The default buffer size for socket buffers from a sending
   * <code>Gateway</code> to its receiving <code>Gateway</code>.
   */
  public static final int DEFAULT_SOCKET_BUFFER_SIZE = 32768;

  /**
   * The default amount of time in milliseconds that a socket read between a
   * sending <code>Gateway</code> and its receiving <code>Gateway</code> will
   * block.
   */
  public static final int DEFAULT_SOCKET_READ_TIMEOUT = Integer.getInteger("gemfire.cache.gateway.default-socket-read-timeout", 0).intValue();

  /**
   * The default number of parallel threads dispatching events from one member.
   * The member has one logical queue and this many actual physical queues and
   * dispatchers dispatching events from those queues. Events will be enqueued
   * in the physical queues based on the hashCode of the member id and thread
   * id. Per-thread ordering will be maintained. The current value is 1.
   * 
   * @since 6.5.1
   */
  public static final int DEFAULT_CONCURRENCY_LEVEL = 1;

  /**
   * The 'primary' startup policy.
   * This setting means that the VM will attempt to become the primary
   * <code>GatewayHub</code>. If it can become the primary
   * <code>GatewayHub</code>, it will. If not, then it will log a warning
   * and become a secondary <code>GatewayHub</code>.
   */
  public static final String STARTUP_POLICY_PRIMARY = "primary";

  /**
   * The order policy. This enum is applicable only when concurrency-level is > 1.
   * 
   * @since 6.5.1
   */
  public enum OrderPolicy {
    /**
     * Indicates that events will be parallelized based on the event's
     * originating member and thread
     */
    THREAD,
    /**
     * Indicates that events will be parallelized based on the event's key
     */
    KEY,
    /** Indicates that events will be parallelized based on the event's:
     *  - partition (using the PartitionResolver) in the case of a partitioned
     *    region event
     *  - key in the case of a replicated region event
     */
    PARTITION
  }

  /**
   * Returns this <code>Gateway</code>'s <code>GatewayHub</code> identifier.
   * @return this <code>Gateway</code>'s <code>GatewayHub</code> identifier.
   */
  public String getGatewayHubId();

  /**
   * Returns this <code>Gateway</code>'s identifier.
   * @return this <code>Gateway</code>'s identifier.
   */
  public String getId();

  /**
   * Add an <code>Endpoint</code> to this <code>Gateway</code>.
   * @param id The id of the endpoint
   * @param host The host of the endpoint
   * @param port The port that the endpoint is listening on
   *
   * @throws GatewayException if this <code>Gateway</code> already defines
   * an <code>Endpoint</code> with this id
   */
  public void addEndpoint(String id, String host, int port) throws GatewayException;

  /**
   * Returns the list of <code>Endpoint</code>s
   * @return the list of <code>Endpoint</code>s
   */
  public List getEndpoints(); // the element of the returned list is the internal Endpoint interface

  /**
   * Returns whether this <code>Gateway</code> has <code>Endpoint</code>s
   * @return whether this <code>Gateway</code> has <code>Endpoint</code>s
   */
  public boolean hasEndpoints();

  /**
   * Add a <code>GatewayEventListener</code> to this <code>Gateway</code>.
   * @param listener The <code>GatewayEventListener</code> to add
   *
   * @throws GatewayException if this <code>Gateway</code> already defines
   * any <code>Endpoint</code>s
   *
   * @since 5.1
   */
  public void addListener(GatewayEventListener listener) throws GatewayException;

  /**
   * Returns the list of <code>GatewayEventListener</code>s
   * @return the list of <code>GatewayEventListener</code>s
   *
   * @since 5.1
   */
  public List<GatewayEventListener> getListeners();

  /**
   * Returns whether this <code>Gateway</code> has <code>GatewayEventListener</code>s
   * @return whether this <code>Gateway</code> has <code>GatewayEventListener</code>s
   *
   * @since 5.1
   */
  public boolean hasListeners();

  /**
   * Sets whether to enable early acknowledgement for this <code>Gateway</code>'s
   * queue.
   * @param earlyAck Whether or not to enable early acknowledgement for
   * batches sent from this <code>Gateway</code> to its corresponding
   * <code>Gateway</code>.
   * @throws UnsupportedOperationException because of deprecation
   * @deprecated EarlyAck communication is unsafe and no longer supported
   */
  @Deprecated
  public void setEarlyAck(boolean earlyAck);

  /**
   * Gets whether to enable early acknowledgement for this <code>Gateway</code>'s
   * queue.
   * @return whether to enable early acknowledgement for batches sent from this
   * <code>Gateway</code> to its corresponding <code>Gateway</code>.
   * @deprecated EarlyAck communication is unsafe and no longer supported
   */
  @Deprecated
  public boolean getEarlyAck();

  /**
   * Sets the buffer size in bytes of the socket connection between this
   * <code>Gateway</code> and its receiving <code>Gateway</code>. The
   * default is 32768 bytes.
   *
   * @param socketBufferSize The size in bytes of the socket buffer
   *
   * @since 4.2.1
   */
  public void setSocketBufferSize(int socketBufferSize);

  /**
   * Returns the configured buffer size of the socket connection between this
   * <code>Gateway</code> and its receiving <code>Gateway</code>. The default
   * is 32768 bytes.
   * @return the configured buffer size of the socket connection between this
   * <code>Gateway</code> and its receiving <code>Gateway</code>
   *
   * @since 4.2.1
   */
  public int getSocketBufferSize();

  /**
   * Optional operation.
   * Sets the amount of time in milliseconds that a socket read between a
   * sending <code>Gateway</code> and its receiving <code>Gateway</code> will
   * block. The default is 10000 seconds.
   *
   * @param socketReadTimeout The amount of time to block
   *
   * @since 4.2.2
   */
  public void setSocketReadTimeout(int socketReadTimeout);

  /**
   * Optional operation.
   * Returns the amount of time in milliseconds that a socket read between a
   * sending <code>Gateway</code> and its receiving <code>Gateway</code> will
   * block. The default is 10000 seconds.
   * @return the amount of time in milliseconds that a socket read between a
   * sending <code>Gateway</code> and its receiving <code>Gateway</code> will
   * block
   *
   * @since 4.2.2
   */
  public int getSocketReadTimeout();

  /**
   * Sets the <code>GatewayQueueAttributes</code>s for this
   * <code>Gateway</code>. Calling setQueueAttributes on a stopped
   * <code>Gateway</code> will destroy the existing queue and create a new
   * empty queue.
   *
   * @param queueAttributes The <code>GatewayQueueAttributes</code> to use
   */
  public void setQueueAttributes(GatewayQueueAttributes queueAttributes);

  /**
   * Returns the <code>GatewayQueueAttributes</code>s for this <code>Gateway</code>
   * @return the <code>GatewayQueueAttributes</code>s for this <code>Gateway</code>
   */
  public GatewayQueueAttributes getQueueAttributes();

  /**
   * Starts this <code>Gateway</code>.  Once the hub is running, its
   * configuration cannot be changed.
   *
   * @throws IOException
   *         If an error occurs while starting the <code>Gateway</code>
   */
  public void start() throws IOException;

  /**
   * Returns whether or not this <code>Gateway</code> is running
   */
  public boolean isRunning();

  /**
   * Stops this <code>Gateway</code>.  Note that the
   * <code>Gateway</code> can be reconfigured and restarted if
   * desired.
   */
  public void stop();

  /**
   * Returns whether or not this <code>Gateway</code> is connected to its
   * remote <code>Gateway</code>.
   */
  public boolean isConnected();

  /**
   * Returns the number of entries in this <code>Gateway</code>'s queue.
   * @return the number of entries in this <code>Gateway</code>'s queue
   */
  public int getQueueSize();

  /**
   * Pauses this <code>Gateway</code>.
   *
   * @since 6.0
   */
  public void pause();

  /**
   * Resumes this paused <code>Gateway</code>.
   *
   * @since 6.0
   */
  public void resume();

  /**
   * Returns whether or not this <code>Gateway</code> is running.
   *
   * @since 6.0
   */
  public boolean isPaused();

  /**
   * An <code>Endpoint</code> represents a proxy to a remote host
   */
  public interface Endpoint {

    /** Returns the identifier for this <code>Endpoint</code>.
     *
     * @return the identifier for this <code>Endpoint</code>
     */
    public String getId();

    /** Returns the host for this <code>Endpoint</code>.
     *
     * @return the host for this <code>Endpoint</code>
     */
    public String getHost();

    /** Returns the port for this <code>Endpoint</code>.
    *
    * @return the port for this <code>Endpoint</code>
    */
    public int getPort();
  }
  
  /**
   * Returns the concurrency level (number of parallel threads) processing
   * <code>GatewayEvent</code>s
   * 
   * @return the concurrency level (number of parallel threads) processing
   *         <code>GatewayEvent</code>s
   * 
   * @see #DEFAULT_CONCURRENCY_LEVEL
   * @since 6.5.1
   */
  public int getConcurrencyLevel();

  /**
   * Sets the <code>OrderPolicy</code> for this <code>Gateway</code>.
   * 
   * @param orderPolicy
   *          the <code>OrderPolicy</code> for this <code>Gateway</code>
   * 
   * @since 6.5.1
   */
  public void setOrderPolicy(OrderPolicy orderPolicy);

  /**
   * Returns the <code>OrderPolicy</code> for this <code>Gateway</code>.
   * 
   * @return the <code>OrderPolicy</code> for this <code>Gateway</code>
   * 
   * @since 6.5.1
   */
  public OrderPolicy getOrderPolicy();
}
