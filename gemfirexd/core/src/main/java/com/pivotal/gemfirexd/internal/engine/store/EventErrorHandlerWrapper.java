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
package com.pivotal.gemfirexd.internal.engine.store;

import java.io.File;

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EventErrorHandler;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.pivotal.gemfirexd.callbacks.Event;
import com.pivotal.gemfirexd.callbacks.EventErrorLogger;
import com.pivotal.gemfirexd.callbacks.GatewayEventErrorHandler;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.ddl.EventImpl;

/**
 * A wrapper for default error handler and custom error handler for event errors.
 * Custom handler if set is invoked in addition to default handling.
 * 
 * @author sjigyasu
 *
 */
public class EventErrorHandlerWrapper implements EventErrorHandler{

  public static final String DEFAULT_ERROR_FILE_NAME_FOR_WAN_EVENTS = "failed_gateway_dmls.xml";

  protected EventErrorLogger evLogger;
  
  private GatewayEventErrorHandler customGatewayHandler;
  
  private static EventErrorHandler instance;
  
  public static EventErrorHandler getInstance() {
    if (instance == null) {
      instance = new EventErrorHandlerWrapper();
    }
    return instance;
  }

  private EventErrorHandlerWrapper() {
    
    String basePath = null;
    GemFireStore store = Misc.getMemStore();
    if (store != null) {
      basePath = store.generatePersistentDirName(null);
    }
    String fileName = basePath + File.separator + DEFAULT_ERROR_FILE_NAME_FOR_WAN_EVENTS;
    evLogger = new EventErrorLogger(fileName);
  }
  
  @Override
  public void onError(EntryEvent ev, Exception e) {
    // Avoid possibility of null pointers later in the default xml logging and custom handler processing.
    // Currently onError is called only from GatewayReceiverCommand and GfxdBulkDMLCommand
    // where if the log event was not created, a message saying so is already logged at warning level, 
    // so no need to log that message again here.
    if(ev == null) {
      return;
    }
    LogWriterI18n logger = GemFireCacheImpl.getExisting().getLoggerI18n();

    EventImpl event = getEventImpl(ev);
    
    try {
      evLogger.logError(event, e);
    } catch (Exception ex) {
      if (logger != null && logger.warningEnabled()) {
        logger.warning(LocalizedStrings.EVENT_LOGGER_FAILED_0_1_2,
            new Object[] { ev, e, ex });
      }
    }
    if (customGatewayHandler != null) {
      try {
        customGatewayHandler.onError(event, e);
      } catch (Exception ex) {
        if (logger != null && logger.severeEnabled()) {
          logger.severe(
              LocalizedStrings.CUSTOM_EVENT_ERROR_HANDLER_FAILED_0_1_2,
              new Object[] { ev, e, ex });
        }
      }
    }
  }
  
  private EventImpl getEventImpl(EntryEvent ev) {
    EntryEventImpl entryEvent = (EntryEventImpl)ev;
    Operation op = entryEvent.getOperation();
    Event.Type type = null;
    if (op.equals(Operation.CREATE)) {
      type = Event.Type.AFTER_INSERT;
    }
    else if (op.equals(Operation.UPDATE)) {
      type = Event.Type.AFTER_UPDATE;
    }
    else if (op.equals(Operation.DESTROY)) {
      type = Event.Type.AFTER_DELETE;
    }
    else if (op.equals(Operation.BULK_DML_OP)) {
      type = Event.Type.BULK_DML;
    }
    // TODO: Handle bulk insert
    EventImpl eventImpl = new EventImpl(entryEvent, type);
    return eventImpl;
  }
  
  /**
   * Set by SYS.ATTACH_GATEWAY_EVENT_ERROR_HANDLER procedure 
   */
  public void setCustomGatewayEventErrorHandler(GatewayEventErrorHandler handler) {
    customGatewayHandler = handler;
  }
  
}
