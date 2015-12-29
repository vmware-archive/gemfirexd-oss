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

package com.gemstone.gemfire.internal.cache.xmlcache;

import com.gemstone.gemfire.CancelCriterion;
import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.GatewayException;
import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.GatewayHub;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Represents a {@link GatewayHub} that is created declaratively.
 *
 * @author Barry Oglesby
 * @since 4.2
 */
class GatewayHubCreation implements GatewayHub {

  /** The cache that is served by this <code>GatewayHub</code> */
  private Cache _cache;

  /**
   * The port on which this <code>GatewayHub</code> listens for
   * clients to connect.
   */
  private int _port;

  /**
   * The identifier of this <code>GatewayHub</code>.
   */
  private String _id;

  /**
   * The buffer size in bytes of the socket connection between this
   * <code>GatewayHub</code>
   */
  private int _socketBufferSize;

  /**
   * The maximum amount of time between client pings. This value is used by
   * the <code>ClientHealthMonitor</code> to determine the health of this
   * <code>hub</code>'s client gateways.
   */
  private int _maximumTimeBetweenPings;

  /**
   * Synchronizes access to {@link #_gateways}
   */
  private final Object _gatewaysLock = new Object();

  /**
   * A list of this <code>GatewayHub</code>'s known <code>Gateway</code>s.
   */
  private final ArrayList _gateways = new ArrayList();

  /**
   * A list of the ids of this <code>GatewayHub</code>'s known <code>Gateway</code>s.
   */
  private final ArrayList _gatewayIds = new ArrayList();

  /**
   * Whether to manually start this <code>GatewayHub</code>
   */
  private boolean _manualStart;

  /**
   * The startup policy for this <code>GatewayHub</code>. The options are:
   * <ul>
   * <li>none</li>
   * <li>primary</li>
   * <li>secondary</li>
   * </ul>
   */
  private String _startupPolicy;

  /**
   * The ip address or host name that this <code>GatewayHub</code> will listen on.
   * @since 6.5.1
   */
  private String _bindAddress;
  
  //////////////////////  Constructors  //////////////////////

  /**
   * Constructor.
   * Creates a new <code>GatewayHubCreation</code> with the default
   * configuration.
   *
   * @param cache The GemFire cache being served
   */
  GatewayHubCreation(Cache cache, String id, int port) {
    this._cache = cache;
    this._id = id;
    this._port = port;
    this._socketBufferSize = DEFAULT_SOCKET_BUFFER_SIZE;
    this._maximumTimeBetweenPings = DEFAULT_MAXIMUM_TIME_BETWEEN_PINGS;
    this._startupPolicy = DEFAULT_STARTUP_POLICY;
    this._manualStart = DEFAULT_MANUAL_START;
    this._bindAddress = DEFAULT_BIND_ADDRESS;
  }

  /////////////////////  Instance Methods  /////////////////////

  public int getPort() {
    return this._port;
  }

  public void setPort(int port) {
    this._port = port;
  }

  public String getId() {
    return this._id;
  }

  public void setId(String id) {
    this._id = id;
  }

  public void setSocketBufferSize(int socketBufferSize) {
    this._socketBufferSize = socketBufferSize;
  }

  public int getSocketBufferSize() {
    return this._socketBufferSize;
  }

  public void setMaximumTimeBetweenPings(int maximumTimeBetweenPings) {
    this._maximumTimeBetweenPings = maximumTimeBetweenPings;
  }

  public int getMaximumTimeBetweenPings() {
    return this._maximumTimeBetweenPings;
  }

  public void start() throws IOException {
    // This method is invoked during testing, but it is not necessary
    // to do anything.
  }

  public void start(boolean startGateways) throws IOException {
  }

  public void startGateways() throws IOException {
  }

  public void stopGateways() {
    throw new UnsupportedOperationException(LocalizedStrings.GatewayHubCreation_REMOVEGATEWAY_IS_NOT_SUPPORTED.toLocalizedString());
  }

  public boolean isRunning() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public void stop() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public boolean isPrimary() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public Cache getCache() {
    return this._cache;
  }

  public Gateway addGateway(String id) throws GatewayException {
    return addGateway(id, Gateway.DEFAULT_CONCURRENCY_LEVEL);
  }
  
  public Gateway addGateway(String id, int concurrencyLevel)
      throws GatewayException {
    // If a gateway with the id is already defined, throw an exception
    synchronized (_gatewaysLock) {
    if (alreadyDefinesGateway(id)) {
      throw new GatewayException(LocalizedStrings.GatewayHubCreation_GATEWAYHUB_0_ALREADY_DEFINES_A_GATEWAY_WITH_ID_1.toLocalizedString(new Object[] {this._id, id}));
    }
    
    Gateway gateway = new GatewayCreation(this, id, concurrencyLevel);
    this._gateways.add(gateway);
    this._gatewayIds.add(id);
    return gateway;
    } // synchronized
  }

  public void removeGateway(String id) throws GatewayException {
    throw new UnsupportedOperationException(LocalizedStrings.GatewayHubCreation_REMOVEGATEWAY_IS_NOT_SUPPORTED.toLocalizedString());
  }

  public List getGateways() {
    return this._gateways;
  }

  public List getGatewayIds() {
    return this._gatewayIds;
  }

  /**
   * Returns whether a <code>Gateway</code> with id is already defined by
   * this <code>GatewayHub</code>.
   * @param id The id to verify
   * @return whether a <code>Gateway</code> with id is already defined by
   * this <code>GatewayHub</code>
   *
   * @guarded.By {@link #_gatewaysLock}
   */
  protected boolean alreadyDefinesGateway(String id) {
    boolean alreadyDefined = false;
    for (Iterator i = this._gateways.iterator(); i.hasNext();) {
      Gateway gateway = (Gateway) i.next();
      if (gateway.getId().equals(id)) {
        alreadyDefined = true;
        break;
      }
    }
    return alreadyDefined;
  }

  public String toDetailedString() {
    throw new UnsupportedOperationException(LocalizedStrings.GatewayHubCreation_NOT_SUPPORTED.toLocalizedString());
  }

  public void setStartupPolicy(String startupPolicy) {
    if (!startupPolicy.equals(STARTUP_POLICY_NONE)
        && !startupPolicy.equals(STARTUP_POLICY_PRIMARY)
        && !startupPolicy.equals(STARTUP_POLICY_SECONDARY)) {
      throw new GatewayException(LocalizedStrings.GatewayHubCreation_AN_UNKNOWN_GATEWAY_HUB_POLICY_0_WAS_SPECIFIED_IT_MUST_BE_ONE_OF_1_2_3.toLocalizedString(new Object[] {startupPolicy, STARTUP_POLICY_NONE, STARTUP_POLICY_PRIMARY, STARTUP_POLICY_SECONDARY}));
    }
    this._startupPolicy = startupPolicy;
  }

  public String getStartupPolicy() {
    return this._startupPolicy;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.util.GatewayHub#getAllGatewaysLock()
   */
  public Object getAllGatewaysLock() {
    return _gatewaysLock;
  }

  public void setManualStart(boolean manualStart) {
    this._manualStart = manualStart;
  }

  public boolean getManualStart() {
    return this._manualStart;
  }

  /* (non-Javadoc)
   * @see com.gemstone.gemfire.cache.util.GatewayHub#getCancelCriterion()
   */
  public CancelCriterion getCancelCriterion() {
    throw new UnsupportedOperationException();
  }

  public void pauseGateways() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public void resumeGateways() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public String getBindAddress() {
    return this._bindAddress;
  }

  public void setBindAddress(String address) {
    this._bindAddress = address;
  }
}
