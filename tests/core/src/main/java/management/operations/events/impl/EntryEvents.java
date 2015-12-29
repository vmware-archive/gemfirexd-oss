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
package management.operations.events.impl;

import static management.util.HydraUtil.logInfo;

import java.util.List;
import java.util.Map;

import management.operations.OperationEvent;
import management.operations.OperationPrms;
import management.operations.events.EntryOperationEvents;

public class EntryEvents extends AbstractEvents implements EntryOperationEvents {
  
  public EntryEvents(){
    
  }

  public EntryEvents(Map<String, List<OperationEvent>> eventMap) {
    super(eventMap);
    // TODO Auto-generated constructor stub
  }

  @Override
  public void entryAdded(String regionName, String key, Object value) {
    if (OperationPrms.recordEntryOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_ENTRY_ADDED;
      event.data = key;
      addEvent(event);
    }
    logInfo("EntryOperations: region:" + regionName + " Finished with adding entry : " + key);
  }

  @Override
  public void entryInvalidated(String regionName, String key) {
    if (OperationPrms.recordEntryOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_ENTRY_INVALIDATED;
      event.data = key;
      addEvent(event);
    }
    logInfo("EntryOperations: region:" + regionName + " Finished with invalidating entry : " + key);
  }

  @Override
  public void entryDestroyed(String regionName, String key) {
    if (OperationPrms.recordEntryOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_ENTRY_DESTROYED;
      event.data = key;
      addEvent(event);
    }
    logInfo("EntryOperations: region:" + regionName + " Finished with destroying entry : " + key);
  }

  @Override
  public void entryUpdated(String regionName, String key, Object value) {
    if (OperationPrms.recordEntryOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_ENTRY_UPDATED;
      event.data = key;
      addEvent(event);
    }
    logInfo("EntryOperations: region:" + regionName + " Finished with updating entry : " + key);
  }

  @Override
  public void entryRead(String regionName, String key) {
    if (OperationPrms.recordEntryOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_ENTRY_READ;
      event.data = key;
      addEvent(event);
    }
    logInfo("EntryOperations: region:" + regionName + " Finished with reading entry : " + key);
  }

  @Override
  public void entryInvalidatedLocal(String regionName, String key) {
    if (OperationPrms.recordEntryOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_ENTRY_INVALIDATEDLOCAL;
      event.data = key;
      addEvent(event);
    }
    logInfo("EntryOperations: region:" + regionName + " Finished with invalidating locally entry : " + key);
  }

  @Override
  public void entryDestroyedLocal(String regionName, String key) {
    if (OperationPrms.recordEntryOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_ENTRY_DESTROYEDLOCAL;
      event.data = key;
      addEvent(event);
    }
    logInfo("EntryOperations: region:" + regionName + " Finished with destroying locally entry : " + key);
  }

  @Override
  public void entryPutIfAbsent(String regionName, String key, Object value) {
    if (OperationPrms.recordEntryOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_ENTRY_PUTIFABSENT;
      event.data = key;
      addEvent(event);
    }
    logInfo("EntryOperations: region:" + regionName + " Finished with puting IFAbsent entry : " + key);
  }

  @Override
  public void entryRemoved(String regionName, String key) {
    if (OperationPrms.recordEntryOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_ENTRY_REMOVED;
      event.data = key;
      addEvent(event);
    }
    logInfo("EntryOperations: region:" + regionName + " Finished with removing entry : " + key);
  }

  @Override
  public void entryReplaced(String regionName, String key, Object value) {
    if (OperationPrms.recordEntryOperations()) {
      OperationEvent event = new OperationEvent();
      event.name = EVENT_ENTRY_REPLACED;
      event.data = key;
      addEvent(event);
    }
    logInfo("EntryOperations: region:" + regionName + " Finished with replacing entry : " + key);
  }

}
