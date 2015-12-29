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

import com.gemstone.gemfire.cache.CacheCallback;

import java.util.List;

/**
 * Implementers of interface <code>GatewayEventListener</code> process
 * batches of <code>GatewayEvent</code>s.
 *
 * @author Barry Oglesby
 * @since 5.1
 * @see GatewayEvent
 */
public interface GatewayEventListener extends CacheCallback {

  /**
   * Process the list of <code>GatewayEvent</code>s. This method will
   * asynchronously be called when events are queued to be processed.
   * The size of the list will be up to batch size events where batch
   * size is defined in the <code>GatewayQueueAttributes</code>.
   *
   * @param events The list of <code>GatewayEvent</code>s to process
   *
   * @return whether the events were successfully processed.
   */
  public boolean processEvents(List<GatewayEvent> events);
}
