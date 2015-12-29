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
package com.gemstone.gemfire.internal.cache;

import java.util.Iterator;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.GatewayException;
import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.GatewayEventListener;
import com.gemstone.gemfire.cache.util.GatewayHub;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

public abstract class AbstractGateway implements Gateway {

  /** Synchronizes lifecycle state including start, stop, and _isRunning */
  protected final Object controlLock = new Object();

  /**
   * Whether the <code>Gateway</code> is running. This is volatile so that it
   * can be checked outside of the controlLock. Changes of value must be done
   * under controlLock. Also controlLock is used to perform additional
   * functionality atomically with checking this value.
   */
  protected volatile boolean _isRunning = false;

  /**
   * This <code>Gateway</code>'s<code>GatewayHub</code>.
   */
  protected final GatewayHubImpl _hub;

  /**
   * The identifier of this <code>Gateway</code>.
   */
  protected final String _id;

  /**
   * The <code>Cache</code> used by this <code>Gateway</code>.
   */
  protected final Cache _cache;

  /**
   * The <code>LogWriterI8n</code> used by this <code>Gateway</code>.
   */
  protected final LogWriterI18n _logger;

  /**
   * The <code>GatewayStats</code> used by this <code>Gateway</code>.
   */
  protected GatewayStats _statistics;

  /**
   * The <code>OrderPolicy</code> used to determine event ordering if
   * concurrency-level is greater than 0.
   * 
   * @since 6.5.1
   */
  private OrderPolicy _orderPolicy;

  AbstractGateway(GatewayHubImpl hub, String id, String statisticsName, GatewayStats allStatistics) {
    this._cache = hub.getCache();
    this._hub = hub;
    this._id = id;
    this._logger = hub._logger;
    this._statistics = new GatewayStats(this._cache.getDistributedSystem(),
        this._hub.getId(), statisticsName, allStatistics);
  }

  protected GatewayHub getGatewayHub() {
    return this._hub;
  }

  public String getId() {
    return this._id;
  }

  public String getGatewayHubId() {
    return this._hub.getId();
  }

  /**
   * Returns this <code>Gateway</code>'s<code>LogWriterImpl</code>.
   *
   * @return this <code>Gateway</code>'s<code>LogWriterImpl</code>
   */
  protected LogWriterI18n getLogger(){
    return this._logger;
  }

  /**
   * Returns the <code>GatewayStats</code>
   *
   * @return the <code>GatewayStats</code>
   */
  protected GatewayStats getStatistics() {
    return this._statistics;
  }

  protected void setStatistics(GatewayStats statistics) {
   this._statistics = statistics;
  }
  
  public void setEarlyAck(boolean earlyAck) {
    if (earlyAck) {
      throw new UnsupportedOperationException(
          LocalizedStrings.GatewayImpl_GATEWAY_COMMUNICATION_NO_LONGER_SUPPORTS_EARLYACK_BECAUSE_OF_DEPRECATION
              .toLocalizedString());
    }
  }

  public boolean getEarlyAck() {
    return false;
  }

  protected abstract void setPrimary(boolean primary);

  public abstract void emergencyClose();

  protected abstract void becomePrimary();

  protected abstract void distribute(EnumListenerEvent operation, EntryEventImpl event);

  /**
   * Sets the configuration of <b>this </b> <code>Gateway</code> based on the
   * configuration of <b>another </b> <code>Gateway</code>.
   *
   * @param other
   *          The other <code>Gateway</code> from which to configure this one
   */
  public void configureFrom(Gateway other) throws GatewayException
  {
    synchronized (this.controlLock) {
      checkRunning();
      this.setQueueAttributes(other.getQueueAttributes());
      this.setEarlyAck(other.getEarlyAck());
      this.setSocketBufferSize(other.getSocketBufferSize());
      // Set the socket read timeout if it is non-default. This will cause the
      // setSocketReadTimeout method to be invoked only if the
      // socket-buffer-size is set in the xml. This avoids the warning in
      // setSocketReadTimeout.
      if (other.getSocketReadTimeout() != DEFAULT_SOCKET_READ_TIMEOUT) {
        this.setSocketReadTimeout(other.getSocketReadTimeout());
      }
      this.setOrderPolicy(other.getOrderPolicy());

      // Add endpoints
      for (Iterator i=other.getEndpoints().iterator(); i.hasNext();) {
        Gateway.Endpoint otherEndpoint = (Gateway.Endpoint) i.next();
        addEndpoint(otherEndpoint.getId(), otherEndpoint.getHost(), otherEndpoint.getPort());
      }

      // Add listeners
      for (Iterator i=other.getListeners().iterator(); i.hasNext();) {
        GatewayEventListener otherListener = (GatewayEventListener) i.next();
        addListener(otherListener);
      }
    }
  }

  public boolean isRunning() {
    return this._isRunning;
  }

  /**
   * Checks to see whether or not this <code>Gateway</code> is running. If so,
   * an {@link IllegalStateException}is thrown.
   */
  protected void checkRunning() {
    if (this.isRunning()) {
      throw new IllegalStateException(LocalizedStrings.GatewayImpl_A_GATEWAYS_CONFIGURATION_CANNOT_BE_CHANGED_ONCE_IT_IS_RUNNING.toLocalizedString());
    }
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
}
