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

import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.GatewayException;
import com.gemstone.gemfire.cache.util.GatewayEventListener;
import com.gemstone.gemfire.internal.cache.GatewayImpl.GatewayEventProcessor;
import com.gemstone.org.jgroups.util.StringId;

/**
 * Class <code>GatewayEventCallbackDispatcher</code> dispatches batches of
 * <code>GatewayEvent</code>s to <code>GatewayEventListener</code> callback
 * implementers. This dispatcher is used in the write-behind case.
 *
 * @author Barry Oglesby
 * @since 5.1
 */
class GatewayEventCallbackDispatcher implements GatewayEventDispatcher {

  /**
   * The <code>GatewayEventProcessor</code> used by this
   * <code>CacheListener</code> to process events.
   */
  protected final GatewayEventProcessor eventProcessor;

  /**
   * The <code>GatewayEventListener</code>s registered on this
   * <code>GatewayEventCallbackDispatcher</code>.
   */
  private volatile List eventListeners = Collections.EMPTY_LIST;

  /**
   * A lock to protecte access to the registered <code>GatewayEventListener</code>s.
   */
  private final Object eventLock = new Object();

  /**
   * The <code>LogWriterImpl</code> used by this <code>GatewayEventDispatcher</code>
   */
  protected final LogWriterI18n logger;

  protected GatewayEventCallbackDispatcher(GatewayEventProcessor eventProcessor) {
    this.eventProcessor = eventProcessor;
    this.logger = eventProcessor.getLogger();
    initializeEventListeners();
  }

  /**
   * Dispatches a batch of messages to all registered <code>GatewayEventListener</code>s.
   * @param events The <code>List</code> of events to send
   * @param removeFromQueueOnException Unused.
   *
   * @return whether the batch of messages was successfully processed
   */
  public boolean dispatchBatch(List events, boolean removeFromQueueOnException) {
    GatewayStats statistics = this.eventProcessor.getGateway().getStatistics();
    boolean success = false;
    try {
      // Send the batch to the corresponding Gateway
      long start = statistics.startTime();
      success = dispatchBatch(events);
      statistics.endBatch(start, events.size());
    }
    catch (GatewayException e) {
      // Do nothing in this case. The exception has already been logged.
    } 
    catch (CancelException e) {
      this.eventProcessor.setIsStopped(true);
      throw e;
    } catch (Exception e) {
      this.logger.severe(LocalizedStrings.GatewayEventCallbackDispatcher_STOPPING_THE_PROCESSOR_BECAUSE_THE_FOLLOWING_EXCEPTION_OCCURRED_WHILE_PROCESSING_A_BATCH, e);
      this.eventProcessor.setIsStopped(true);
    }
    return success;
  }

  /**
   * Registers a <code>GatewayEventListener</code>.
   * @param listener A GatewayEventListener to be registered
   */
  public void registerGatewayEventListener(GatewayEventListener listener) {
    synchronized (eventLock) {
      List oldListeners = this.eventListeners;
      if (!oldListeners.contains(listener)) {
        List newListeners = new ArrayList(oldListeners);
        newListeners.add(listener);
        this.eventListeners = newListeners;
      }
    }
  }

  /**
   * Removes registration of a previously registered <code>GatewayEventListener</code>.
   * @param listener A GatewayEventListener to be unregistered
   */
  public void unregisterGatewayEventListener(GatewayEventListener listener) {
    synchronized (eventLock) {
      List oldListeners = this.eventListeners;
      if (oldListeners.contains(listener)) {
        List newListeners = new ArrayList(oldListeners);
        if (newListeners.remove(listener)) {
          this.eventListeners = newListeners;
        }
      }
    }
  }

  protected void initializeEventListeners() {
    for (Iterator i = this.eventProcessor.getGateway().getListeners().iterator(); i.hasNext(); ) {
      registerGatewayEventListener((GatewayEventListener) i.next());
    }
  }

  /**
   * Sends a batch of messages to the registered <code>GatewayEventListener</code>s.
   * @param events The <code>List</code> of events to send
   *
   * @throws GatewayException
   */
  protected boolean dispatchBatch(List events) throws GatewayException {
    if (events.isEmpty()) {
      return true;
    }
    int batchId = this.eventProcessor.getBatchId();
    boolean successAll = true;
    try {
      for (Iterator i = this.eventListeners.iterator(); i.hasNext(); ) {
        GatewayEventListener listener = (GatewayEventListener) i.next();
        boolean successOne = listener.processEvents(events);
        if (!successOne) {
          successAll = false;
        }
      }
    }
    catch (Exception e) {
      final StringId alias = LocalizedStrings.GatewayEventCallbackDispatcher__0___EXCEPTION_DURING_PROCESSING_BATCH__1_;
      final Object[] aliasArgs = new Object[] {this, Integer.valueOf(batchId)};
      String exMsg = alias.toLocalizedString(aliasArgs);
      GatewayException ge = new GatewayException(exMsg, e);
      this.logger.warning(alias, aliasArgs, ge);
      throw ge;
    }
    return successAll;
  }
}
