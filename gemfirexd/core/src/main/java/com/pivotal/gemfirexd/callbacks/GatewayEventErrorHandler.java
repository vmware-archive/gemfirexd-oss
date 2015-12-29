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
package com.pivotal.gemfirexd.callbacks;

/**
 * A handler for events arriving from other distributed system that failed to be
 * applied on this distributed system for any reason
 * 
 * An implementation of this handler interface can be installed using the
 * SYS.ATTACH_GATEWAY_EVENT_ERROR_HANDLER system procedure.
 * 
 * GemFireXD logs failed events to xml files in the same directory
 * as the member's system logs.  The XML files are failed_gateway_dmls.xml and
 * failed_gateway_dmls_entries.xml.  
 * 
 * If a handler is installed, the events are passed to it in addition to this logging.
 * 
 * @author sjigyasu
 */
public interface GatewayEventErrorHandler {
 
  /**
   * Initialization method
   * @param params Initialization parameters
   */
  public void init(String params);
  
  /**
   * Called for every gateway event that failed to be applied
   * @param event Event
   * @param e Exception thrown while applying the event
   * @throws Exception Exception handling this event by the handler
   */
  public void onError(Event event, Exception e) throws Exception;
}
