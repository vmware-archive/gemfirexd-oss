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

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.GatewayException;

import java.io.IOException;
import java.util.List;

/**
 * A <code>GatewayHub</code> manages a collection of <code>Gateways</code>. It
 * has three primary purposes, namely:
 * <ul>
 * <li>To start as primary or secondary
 * <li>To distribute local <code>EntryEvents</code> to its <code>Gateways</code>
 * which in turn queue them for distribution to remote sites or
 * <code>GatewayEventListeners</code> as <code>GatewayEvents</code>
 * <li>To start a listener to accept incoming <code>GatewayEvents</code> from
 * remote sites
 * 
 * </ul>
 * 
 * @see com.gemstone.gemfire.cache.util.Gateway
 * @see com.gemstone.gemfire.cache.util.GatewayEvent
 * @see com.gemstone.gemfire.cache.util.GatewayEventListener
 * 
 * @since 4.2
 */
public interface GatewayHub {

  /**
   * The default buffer size (32768 bytes) for socket buffers from a sending
   * <code>GatewayHub</code> to the receiving one.
   */
  public static final int DEFAULT_SOCKET_BUFFER_SIZE = 32768;

  /**
   * The default maximum amount of time (60000 ms) between client pings. This
   * value is used by the <code>ClientHealthMonitor</code> to determine the
   * health of this <code>GatewayHub</code>'s clients.
   */
  public static final int DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS = 60000;

  /**
   * The 'none' startup policy.
   * This setting (the default) means that the VM will attempt to become the
   * primary <code>GatewayHub</code>. If it can become the primary
   * <code>GatewayHub</code>, it will. If not, then it will become a secondary
   * <code>GatewayHub</code>.
   */
  public static final String STARTUP_POLICY_NONE = "none";

  /**
   * The 'primary' startup policy.
   * This setting means that the VM will attempt to become the primary
   * <code>GatewayHub</code>. If it can become the primary
   * <code>GatewayHub</code>, it will. If not, then it will log a warning
   * and become a secondary <code>GatewayHub</code>.
   */
  public static final String STARTUP_POLICY_PRIMARY = "primary";

  /**
   * The 'secondary' startup policy.
   * This setting means that the VM will wait for
   * {@link #STARTUP_POLICY_SECONDARY_WAIT} milliseconds for another VM to
   * start as the primary <code>GatewayHub</code>. If another VM does start as
   * the primary <code>GatewayHub</code> within
   * {@link #STARTUP_POLICY_SECONDARY_WAIT} milliseconds, then this VM will
   * start as a secondary <code>GatewayHub</code>. If not, then it will log a
   * warning and start as the primary <code>GatewayHub</code>.
   */
  public static final String STARTUP_POLICY_SECONDARY = "secondary";

  /**
   * The default startup policy ({@link #STARTUP_POLICY_NONE}).
   */
  public static final String DEFAULT_STARTUP_POLICY = STARTUP_POLICY_NONE;

  /**
   * The amount of time in milliseconds a <code>GatewayHub</code> defined with
   * secondary startup policy waits for a primary <code>GatewayHub</code> to
   * start before logging a warning and becoming the primary
   * <code>GatewayHub</code> itself. This value is 60000 milliseconds.
   */
  public static final int STARTUP_POLICY_SECONDARY_WAIT = 60000;

  /**
   * The default value (false) for manually starting a <code>GatewayHub</code>.
   *
   * @since 5.7 
   */
  public static final boolean DEFAULT_MANUAL_START = false;

  /**
   * The default value (-1) for this <code>GatewayHub</code>'s port.
   * This is not a valid port for creating connections and should only be
   * used in cases in which the <code>GatewayHub</code> does not accept
   * any connections, such as database write behind applications.
   * @since 5.7
   */ 
  public static final int DEFAULT_PORT = -1;

  /**
   * The default ip address or host name that this <code>GatewayHub</code>'s
   * socket will listen on for connections.
   * The current default is null.
   * @since 6.5.1
   */
  public static final String DEFAULT_BIND_ADDRESS = null;
  
  /**
   * Returns the port on which this <code>GatewayHub</code> listens for
   * remote connections
   * @return the port on which this <code>GatewayHub</code> listens for
   * remote connections
   */
  public int getPort();

  /**
   * Sets the port on which this <code>GatewayHub</code> listens for
   * remote connections
   * @param port The port on which this <code>GatewayHub</code> listens
   * for remote connections
   */
  public void setPort(int port);

  /**
   * Sets the identifier for this <code>GatewayHub</code>
   * @param id The identifier for this <code>GatewayHub</code>
   */
  public void setId(String id);

  /**
   * Returns the identifier of this <code>GatewayHub</code>
   * @return the identifier of this <code>GatewayHub</code>
   */
  public String getId();

  /**
   * Sets the buffer size in bytes of the socket connection for this
   * <code>GatewayHub</code>. The default is 32768 bytes.
   *
   * @param socketBufferSize The size in bytes of the socket buffer
   *
   * @since 4.2.1
   */
  public void setSocketBufferSize(int socketBufferSize);

  /**
   * Returns the configured buffer size of the socket connection for this
   * <code>GatewayHub</code>. The default is 32768 bytes.
   * @return the configured buffer size of the socket connection for this
   * <code>GatewayHub</code>
   *
   * @since 4.2.1
   */
  public int getSocketBufferSize();

  /**
   * Sets the maximum amount of time between client pings. This value is
   * used by the <code>ClientHealthMonitor</code> to determine the health
   * of any foreign <code>Gateway</code>s connected to this
   * <code>GatewayHub</code>. The default is 60000 ms.
   *
   * @param maximumTimeBetweenPings The maximum amount of time between client
   * pings
   *
   * @since 4.2.3
   */
  public void setMaximumTimeBetweenPings(int maximumTimeBetweenPings);

  /**
   * Returns the maximum amount of time between client pings. This value is
   * used by the <code>ClientHealthMonitor</code> to determine the health
   * of any foreign <code>Gateway</code>s connected to this
   * <code>GatewayHub</code>. The default is 60000 ms.
   * @return the maximum amount of time between client pings.
   *
   * @since 4.2.3
   */
  public int getMaximumTimeBetweenPings();

  /**
   * Starts this <code>GatewayHub</code> and notifies all of its
   * <code>Gateways</code> to start. Starting a <code>GatewayHub</code> does
   * several actions, including:
   * <ul>
   * <li>Attempts to start as primary or secondary depending on the start up
   * policy
   * <li>Starts the listener to accept incoming <code>GatewayEvents</code> from
   * remote sites if the listen port is set
   * <li>Starts its <code>Gateways</code> using {@link #startGateways} to queue
   * and send local <code>EntryEvents</code> to remote sites or local
   * <code>GatewayEventListeners</code> as <code>GatewayEvents</code>
   * </ul>
   * 
   * Once the <code>GatewayHub</code> is running, its configuration cannot be
   * changed.
   * 
   * @throws IOException
   *           If an error occurs while starting the <code>GatewayHub</code>
   */
  public void start() throws IOException;

  /**
   * Starts this <code>GatewayHub</code> and notifies all of its
   * <code>Gateways</code> to start if startGateways is true. Starting a
   * <code>GatewayHub</code> does several actions, including:
   * <ul>
   * <li>Attempts to start as primary or secondary depending on the start up
   * policy
   * <li>Starts the listener to accept incoming <code>GatewayEvents</code> from
   * remote sites if the listen port is set
   * <li>If the startGateways argument is true, Starts its <code>Gateways</code>
   * using {@link #startGateways} to queue and send local
   * <code>EntryEvents</code> to remote sites or local
   * <code>GatewayEventListeners</code> as <code>GatewayEvents</code>. If the
   * startGateways argument is false, this <code>GatewayHub</code> will start
   * just as a receiver of remote <code>GatewayEvents</code>. It will neither
   * send nor queue local events for remote delivery. Setting startGateways to
   * false is useful when updates to the local site should not be sent to the
   * remote sites (e.g. during an initial load).
   * </ul>
   * 
   * Once the <code>GatewayHub</code> is running, its configuration cannot be
   * changed.
   * 
   * @param startGateways
   *          Whether to notify the <code>Gateways</code> to start
   */
  public void start(boolean startGateways) throws IOException;

  /**
   * Starts all of this <code>GatewayHub's</code> <code>Gateways</code>. This
   * can be used to start <code>Gateways</code> that have been stopped using
   * {@link #stopGateways} or not started when this <code>GatewayHub</code> is
   * started.
   * 
   * @throws IOException
   */
  public void startGateways() throws IOException;

  /**
   * Stops all of this <code>GatewayHub's</code> <code>Gateways</code>. Stopping
   * the <code>Gateways</code> will cause this <code>GatewayHub</code> to act
   * just as a receiver of remote <code>GatewayEvents</code>. It will neither
   * send nor queue local events for remote delivery. Stopping
   * <code>Gateways</code> is useful when updates to the local site should not
   * be sent to the remote sites (e.g. during an initial load). This action has
   * no effect on this <code>GatewayHub's</code> primary/secondary status.
   */
  public void stopGateways();

  /**
   * Returns whether this <code>GatewayHub</code> is running
   * 
   * @return whether this <code>GatewayHub</code> is running
   * 
   * @deprecated use getCancelCriterion().cancelInProgress() instead
   */
  @Deprecated
  public boolean isRunning();

  /**
   * Stops this <code>GatewayHub</code>. Stopping a <code>GatewayHub</code> does
   * several actions, including:
   * <ul>
   * <li>Stops the listener so that <code>GatewayEvents</code> are no longer
   * received from remote sites
   * <li>Stops each of its <code>Gateways</code> using {@link #stopGateways} so
   * that <code>GatewayEvents</code> are no longer sent to remote sites
   * <li>Releases its primary lock if it is the primary. If another
   * <code>GatewayHub</code> is running in the local site, it will take over as
   * primary if it is not already primary.
   * </ul>
   * 
   * Note that the <code>GatewayHub</code> can be reconfigured and restarted if
   * desired. In this case, the <code>GatewayHub</code> will restart as
   * secondary if another <code>GatewayHub</code> is already running in the
   * local site.
   */
  public void stop();

  /**
   * Returns this <code>GatewayHub</code>'s GemFire cache
   * @return this <code>GatewayHub</code>'s GemFire cache
   */
  public Cache getCache();

  /**
   * Adds a <code>Gateway</code> to this <code>GatewayHub</code>'s
   * known <code>Gateway</code>s.
   *
   * @param id The id of the <code>Gateway</code>
   *
   * @return the added <code>Gateway</code>
   *
   * @throws GatewayException if this <code>GatewayHub</code> already defines
   * a <code>Gateway</code> with this id
   */
  public Gateway addGateway(String id) throws GatewayException;

  /**
   * Adds a <code>Gateway</code> to this <code>GatewayHub</code>'s known
   * <code>Gateway</code>s.
   * 
   * @param id
   *          The id of the <code>Gateway</code>
   * @param concurrencyLevel
   *          The concurrency level (number of parallel threads) processing
   *          <code>GatewayEvent</code>s *
   * @return the added <code>Gateway</code>
   * 
   * @throws GatewayException
   *           if this <code>GatewayHub</code> already defines a
   *           <code>Gateway</code> with this id
   * 
   * @since 6.5.1
   */
  public Gateway addGateway(String id, int concurrencyLevel) throws GatewayException;

  /**
   * Removes a <code>Gateway</code> from this <code>GatewayHub</code>.
   *
   * @param id The id of the <code>Gateway</code>
   *
   * @throws GatewayException if this <code>GatewayHub</code> does not contain
   * a <code>Gateway</code> with this id
   */
  public void removeGateway(String id) throws GatewayException;

  /**
   * Returns this <code>GatewayHub</code>'s list of known
   * <code>Gateway</code>s
   * @return this <code>GatewayHub</code>'s list of known
   * <code>Gateway</code>s
   */
  public List<Gateway> getGateways();

  /**
   * Returns the ids of this <code>GatewayHub</code>'s list of known
   * <code>Gateway</code>s
   * @return the ids of this <code>GatewayHub</code>'s list of known
   * <code>Gateway</code>s
   */
  public List<String> getGatewayIds();

  /**
   * Returns whether this <code>GatewayHub</code> is the primary hub.
   * @return whether this <code>GatewayHub</code> is the primary hub
   */
  public boolean isPrimary();

  /**
   * Returns detailed string representation of this GatewayHub.
   * @return detailed string representation of this GatewayHub
   */
  public String toDetailedString();

  /**
   * Sets the startup policy for this <code>GatewayHub</code>.
   * @see #STARTUP_POLICY_NONE
   * @see #STARTUP_POLICY_PRIMARY
   * @see #STARTUP_POLICY_SECONDARY
   *
   * @param startupPolicy the startup policy for this <code>GatewayHub</code>
   *
   * @throws GatewayException if the input startup policy is not valid.
   *
   * @since 5.1
   */
  public void setStartupPolicy(String startupPolicy) throws GatewayException;

  /**
   * Returns the startup policy for this <code>GatewayHub</code>.
   * @return the startup policy for this <code>GatewayHub</code>
   *
   * @since 5.1
   */
  public String getStartupPolicy();
  
  /**
   * Return a mutex to lock when iterating over the list of gateways
   * @return the mutex to synchronize with
   */
  public Object getAllGatewaysLock();

  /**
   * Sets the manual start boolean property for this <code>GatewayHub</code>.
   * This property is mainly used for controlling when a <code>GatewayHub</code>
   * configured in xml is started. By default, a <code>GatewayHub</code> is
   * started right after the <code>Region</code>s are created. Setting this to
   * true allows the application to start a <code>GatewayHub</code> when desired
   * (e.g. after all remote <code>Region</code>s are created and initialized).
   * 
   * @param manualStart
   *          the manual start boolean property for this <code>GatewayHub</code>
   * 
   * @since 5.7
   */
  public void setManualStart(boolean manualStart);

  /**
   * Returns the manual start boolean property for this
   * <code>GatewayHub</code>.
   *
   * @return the manual start boolean property for this
   * <code>GatewayHub</code>
   *
   * @since 5.7 
   */
  public boolean getManualStart();

  /**
   * Returns the <code>CancelCriterion</code> for this <code>GatewayHub</code>.
   * 
   * @since 6.0
   * @return the <code>CancelCriterion</code> for this <code>GatewayHub</code>
   */
  public CancelCriterion getCancelCriterion();

  /**
   * Notifies all of this <code>GatewayHub</code>'s <code>Gateway</code>s to
   * pause sending events. Events will continue to be queued while the
   * <code>Gateway</code> is paused. Pausing a <code>Gateway</code> only affects
   * outgoing events. It has no affect on incoming ones. It also has no effect
   * on the primary status of this <code>GatewayHub</code>.
   * 
   * @since 6.0
   */
  public void pauseGateways();

  /**
   * Notifies all of this <code>GatewayHub</code>'s <code>Gateway</code>s to
   * resume sending events. Processing the queue will resume when a
   * <code>Gateway</code> is resumed.
   * 
   * @since 6.0
   */
  public void resumeGateways();

  /**
   * Returns a string representing the ip address or host name that this
   * <code>GatewayHub</code> will listen on.
   * @return the ip address or host name that this <code>GatewayHub</code>
   * will listen on
   * @see #DEFAULT_BIND_ADDRESS
   * @since 6.5.1
   */
  public String getBindAddress();

  /**
   * Sets the ip address or host name that this <code>GatewayHub</code> will
   * listen on for connections.
   * <p>Setting a specific bind address will cause the <code>GatewayHub</code>
   * to always use this address and ignore any address specified by "server-bind-address"
   * or "bind-address" in the <code>gemfire.properties</code> file
   * (see {@link com.gemstone.gemfire.distributed.DistributedSystem}
   * for a description of these properties).
   * <p> The value <code>""</code> does not override the <code>gemfire.properties</code>.
   * It will cause the local machine's default address to be listened on if the
   * properties file does not specify and address.
   * If you wish to override the properties and want to have your <code>GatewayHub</code>
   * bind to all local addresses then use this bind address <code>"0.0.0.0"</code>.
   * <p> A <code>null</code> value will be treated the same as the default <code>""</code>.
   * @param address the ip address or host name that this <code>GatewayHub</code>
   * will listen on
   * @see #DEFAULT_BIND_ADDRESS
   * @since 6.5.1
   */
  public void setBindAddress(String address);
}
