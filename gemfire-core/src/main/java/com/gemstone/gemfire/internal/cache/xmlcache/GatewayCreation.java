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

import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.cache.GatewayException;
import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.GatewayEventListener;
import com.gemstone.gemfire.cache.util.GatewayHub;
import com.gemstone.gemfire.cache.util.GatewayQueueAttributes;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;

/**
 * Represents a {@link Gateway} that is created declaratively.
 *
 * @author Barry Oglesby
 *
 * @since 4.2
 */
class GatewayCreation implements Gateway {

  /**
   * This <code>Gateway</code>'s <code>GatewayHub</code>.
   */
  private GatewayHub _hub;

  /**
   * The identifer of this <code>Gateway</code>.
   */
  private String _id;

  /**
   * The list of endpoints (host and port) to which this
   * <code>Gateway</code> is connected.
   */
  private List _endpoints;

  /**
   * The list of <code>GatewayEventListeners</code> to which this
   * <code>Gateway</code> invokes the callback.
   */
  private List _listeners;

  /**
   * The <>GatewayQueueAttributes<> for this <code>Gateway</code>.
   */
  private GatewayQueueAttributes _queueAttributes;

  /**
   * Whether or not to enable early acknowledgement for batches of messages
   * sent between this <code>Gateway</code>and its corresponding
   * <code>Gateway</code>.
   */
  private boolean _earlyAck;

  /**
   * The buffer size of the socket connection between this <code>Gateway</code>
   * and its receiving <code>Gateway</code>
   */
  private int _socketBufferSize;

  /**
   * The amount of time in milliseconds that a socket read between this
   * <code>Gateway</code> and its receiving <code>Gateway</code> will block.
   */
  private int _socketReadTimeout;

  /**
   * The concurrency level (number of parallel threads) processing
   * <code>GatewayEvent</code>s
   * 
   * @since 6.5.1
   */
  private int _concurrencyLevel;

  /**
   * The <code>OrderPolicy</code> used to determine event ordering if
   * concurrency-level is greater than 0.
   * 
   * @since 6.5.1
   */
  private OrderPolicy _orderPolicy;
  
  //////////////////////  Constructors  //////////////////////

  /**
   * Constructor.
   * Creates a new <code>GatewayCreation</code> with the default
   * configuration.
   */
  GatewayCreation(GatewayHub hub, String id, int concurrencyLevel) {
    this._hub = hub;
    this._id = id;
    this._endpoints = new ArrayList();
    this._listeners = new ArrayList();
    this._queueAttributes = new GatewayQueueAttributes();
    this._socketBufferSize = DEFAULT_SOCKET_BUFFER_SIZE;
    this._socketReadTimeout = DEFAULT_SOCKET_READ_TIMEOUT;
    this._concurrencyLevel = concurrencyLevel;
  }

  /////////////////////  Instance Methods  /////////////////////

  public String getGatewayHubId() {
    return this._hub.getId();
  }

  public String getId() {
    return this._id;
  }

  public void addEndpoint(String id, String host, int port) throws GatewayException {
    // If an endpoint with the id, host and port is already defined, throw an exception
    if (alreadyDefinesEndpoint(id, host, port)) {
      throw new GatewayException(LocalizedStrings.GatewayCreation_GATEWAY_0_ALREADY_DEFINES_AN_ENDPOINT_EITHER_WITH_ID_1_OR_HOST_2_AND_PORT_3.toLocalizedString(new Object[] {this._id, id, host, Integer.valueOf(port)}));
    }

    // If another gateway defines this same endpoint, throw an exception
    String[] otherGateway = new String[1];
    if (otherGatewayDefinesEndpoint(host, port, otherGateway)) {
      throw new GatewayException(LocalizedStrings.GatewayCreation_GATEWAY_0_CANNOT_DEFINE_ENDPOINT_HOST_1_AND_PORT_2_BECAUSE_IT_IS_ALREADY_DEFINED_BY_GATEWAY_3.toLocalizedString(new Object[] {this._id, host, Integer.valueOf(port), otherGateway[0]}));
    }

    // If the gateway is attempting to add an endpoint to its own hub, throw an exception
    if (isConnectingToOwnHub(host, port)) {
      throw new GatewayException(LocalizedStrings.GatewayCreation_GATEWAY_0_CANNOT_DEFINE_AN_ENDPOINT_TO_ITS_OWN_HUB_HOST_1_AND_PORT_2.toLocalizedString(new Object[] {this._id, host, Integer.valueOf(port)}));
    }

    // If listeners are already defined, throw an exception
    if (hasListeners()) {
      throw new GatewayException(LocalizedStrings.GatewayCreation_GATEWAY_0_CANNOT_DEFINE_AN_ENDPOINT_BECAUSE_AT_LEAST_ONE_LISTENER_IS_ALREADY_DEFINED_BOTH_LISTENERS_AND_ENDPOINTS_CANNOT_BE_DEFINED_FOR_THE_SAME_GATEWAY.toLocalizedString(this._id));
    }

    this._endpoints.add(new EndpointCreation(id, host, port));
  }

  public List getEndpoints() {
    return this._endpoints;
  }

  public boolean hasEndpoints() {
    return getEndpoints().size() > 0;
  }

  public void addListener(GatewayEventListener listener) throws GatewayException {
    // If endpoints are already defined, throw an exception
    if (hasEndpoints()) {
      throw new GatewayException(LocalizedStrings.GatewayCreation_GATEWAY_0_CANNOT_DEFINE_A_LISTENER_BECAUSE_AT_LEAST_ONE_ENDPOINT_IS_ALREADY_DEFINED_BOTH_LISTENERS_AND_ENDPOINTS_CANNOT_BE_DEFINED_FOR_THE_SAME_GATEWAY.toLocalizedString(this._id));
    }

    this._listeners.add(listener);
  }

  public List getListeners() {
    return this._listeners;
  }

  public boolean hasListeners() {
    return getListeners().size() > 0;
  }

  public void setQueueAttributes(GatewayQueueAttributes queueAttributes) {
    this._queueAttributes = queueAttributes;
  }

  public GatewayQueueAttributes getQueueAttributes() {
    return this._queueAttributes;
  }

  public void setEarlyAck(boolean earlyAck) {
    this._earlyAck = earlyAck;
  }

  public boolean getEarlyAck() {
    return this._earlyAck;
  }

  public void setSocketBufferSize(int socketBufferSize) {
    this._socketBufferSize = socketBufferSize;
  }

  public int getSocketBufferSize() {
    return this._socketBufferSize;
  }

  public void setSocketReadTimeout(int socketReadTimeout) {
    this._socketReadTimeout = socketReadTimeout;
  }

  public int getSocketReadTimeout() {
    return this._socketReadTimeout;
  }
  
  public int getConcurrencyLevel() {
    return this._concurrencyLevel;
  }
  
  public void setOrderPolicy(OrderPolicy orderPolicy) throws GatewayException {
    if (orderPolicy != null) {
      if (getConcurrencyLevel() == Gateway.DEFAULT_CONCURRENCY_LEVEL) {
        throw new GatewayException(
            LocalizedStrings.CacheXmlParser_INVALID_GATEWAY_ORDER_POLICY_CONCURRENCY_0
                .toLocalizedString(getId()));
      } else {
        this._orderPolicy = orderPolicy;
      }
    }
  }

  public OrderPolicy getOrderPolicy() {
    return this._orderPolicy;
  }

  public void start() throws IOException {
    // This method is invoked during testing, but it is not necessary
    // to do anything.
  }

  public boolean isRunning() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public boolean isConnected() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public void stop() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public int getQueueSize() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public void pause() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public void resume() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  public boolean isPaused() {
    throw new UnsupportedOperationException(LocalizedStrings.SHOULDNT_INVOKE.toLocalizedString());
  }

  /**
   * Returns this <code>Gateway</code>'s <code>GatewayHub</code>.
   * @return this <code>Gateway</code>'s <code>GatewayHub</code>
   */
  protected GatewayHub getGatewayHub() {
    return this._hub;
  }

  /**
   * Returns whether another <code>Gateway</code> already defines an
   * <code>Endpoint</code> with the same host and port.
   * @param host The host of the endpoint
   * @param port The port that the endpoint is listening on
   * @param otherGateway The other <code>Gateway</code> defining this
   * <code>Endpoint</code>
   * @return whether another <code>Gateway</code> already defines an
   * <code>Endpoint</code> with the same host and port
   */
  protected boolean otherGatewayDefinesEndpoint(String host, int port, String[] otherGateway) {
    boolean otherGatewayDefined = false;
    // Iterate through all gateways and compare their endpoints to the input
    // host and port.
    for (Iterator i = getGatewayHub().getGateways().iterator(); i.hasNext();) {
      Gateway gateway = (Gateway) i.next();
      // Do not compare the current gateway
      if (!getId().equals(gateway.getId())) {
        for (Iterator j = gateway.getEndpoints().iterator(); j.hasNext();) {
          Endpoint endpoint = (Endpoint) j.next();
          // If the endpoint's host and port are equal to the input host and
          // port, answer true; else continue.
          if (endpoint.getHost().equals(host) && endpoint.getPort() == port) {
            otherGatewayDefined = true;
            otherGateway[0] = gateway.getId();
            break;
          }
        }
      }
    }
    return otherGatewayDefined;
  }

  /**
   * Returns whether an <code>Endpoint</code> with id is already defined by
   * this <code>Gateway</code>.
   * @param id The id to verify
   * @param host The host of the endpoint
   * @param port The port that the endpoint is listening on
   * @return whether an <code>Endpoint</code> with id is already defined by
   * this <code>Gateway</code>
   */
  protected boolean alreadyDefinesEndpoint(String id, String host, int port) {
    boolean alreadyDefined = false;
    for (Iterator i = this._endpoints.iterator(); i.hasNext();) {
      Endpoint endpoint = (Endpoint) i.next();
      // If the ids are equal or the host and port are equal, then the
      // requested endpoint is already defined.
      if (endpoint.getId().equals(id) ||
         (endpoint.getHost().equals(host) && endpoint.getPort() == port)) {
        alreadyDefined = true;
        break;
      }
    }
    return alreadyDefined;
  }

  /**
   * Returns whether this <code>Gateway</code> is attempting to add an
   * <code>Endpoint</code> to its own <code>GatewayHub</code>.
   * @param host The host of the endpoint
   * @param port The port that the endpoint is listening on
   * @return whether this <code>Gateway</code> is attempting to add an
   * <code>Endpoint</code> to its <code>GatewayHub</code>
   */
  protected boolean isConnectingToOwnHub(String host, int port) {
    // These tests work where the host is specified as:
    // IP address string (e.g. host=10.80.10.80)
    // Short host name (e.g. host=bishop)
    // Fully-qualified host name (e.g. bishop.gemstone.com)
    // The localhost (e.g. localhost)
    // The loopback address (e.g. 127.0.0.1)

    // First compare the port with the hub's port. If they are the same,
    // then compare the host with the local host.
    boolean isConnectingToOwnHub = false;
    if (port == this._hub.getPort()) {
      // The ports are equal. Now, compare the hosts. Do a best guess
      // determination whether the input host is the same as the local
      // host.
      try {
        String localHostName = SocketCreator.getLocalHost().getCanonicalHostName();
        String requestedHostName = InetAddress.getByName(host).getCanonicalHostName();
        if (localHostName.equals(requestedHostName) || requestedHostName.startsWith("localhost")) {
          isConnectingToOwnHub = true;
        }
      } catch (UnknownHostException e) {}
    }
    return isConnectingToOwnHub;
  }

  /**
   * Class <code>EndpointCreation</code> represents a <code>Gateway</code>'s
   * endpoint that is created declaratively
   *
   * @author Barry Oglesby
   *
   * @since 4.2
   */
  static protected class EndpointCreation implements Endpoint {

    /**
     * The id of the <code>Endpoint</code>
     */
    protected String _id;

    /**
     * The host of the <code>Endpoint</code>
     */
    protected String _host;

    /**
     * The port of the <code>Endpoint</code>
     */
    protected int _port;

    /**
     * Constructor.
     * @param id The id of the <code>Endpoint</code>
     * @param host The host of the <code>Endpoint</code>
     * @param port The port of the <code>Endpoint</code>
     */
    protected EndpointCreation(String id, String host, int port) {
      this._id = id;
      this._host = host;
      this._port = port;
    }

    public String getId() {
      return this._id;
    }

    public String getHost() {
      return this._host;
    }

    public int getPort() {
      return this._port;
    }
  }
}
