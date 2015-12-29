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
package com.pivotal.gemfirexd.callbacks.impl;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.delta.Delta;
import com.pivotal.gemfirexd.callbacks.impl.GatewayConflictHelper;
import com.pivotal.gemfirexd.internal.engine.ddl.EventImpl;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer.SerializableDelta;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;

/**
 * Implementation of GemFireXD's GatewayConflictHelper interface. Holds a
 * reference to GFE's GatewayConflictHelper.
 * 
 * The GatewayConflictResolverWrapper creates and holds an instance of this
 * class, and must set the GFE's helper on it before use.
 * 
 * The GFE helper should ideally have been passed through the constructor, but
 * its reference is available only during onEvent() of the resolver, and we
 * don't want to create GemFireXD's helper instance with every onEvent() call.
 * Hence the need to explicity set GFE helper.
 * 
 * @author sjigyasu
 * 
 */
public class GatewayConflictHelperImpl implements GatewayConflictHelper{

  // The gateway event to process 
  private GatewayEventImpl gatewayEvent;
  
  // GFE conflict helper 
  private com.gemstone.gemfire.cache.util.GatewayConflictHelper gfeConflictHelper;
  
  // Changes due to setColumn() calls are saved into this
  private SerializableDelta changes;
  
  public void setGFEConflictHelper(com.gemstone.gemfire.cache.util.GatewayConflictHelper gfeConflictHelper) {
    this.gfeConflictHelper = gfeConflictHelper;
  }
  
  public void setGatewayEvent(GatewayEventImpl event) {
    this.gatewayEvent = event;
  }
  
  /*
   * Applies all changes to the GFE conflict helper
   */
  public void applyChanges() throws StandardException {
    // If there was no change, don't modify the event value on the GFE helper
    if(changes == null) {
      return;
    }
    EntryEventImpl event = gatewayEvent.getEntryEvent();
    LogWriterI18n logger = event.getRegion().getLogWriterI18n();

    if(logger.fineEnabled()) {
      logger.fine("GatewayConflictHelperImpl::Applying changes");
    }
    Object collectedChanges = null;
    if(event.hasDelta()) {
      collectedChanges = changes;
    } 
    else {
      // It is a case of insert.
      collectedChanges = changes.apply(event.getRegion(), event.getKey(), event.getRawNewValue(), false);
    }
    if (logger.fineEnabled()) {
      logger.fine("GatewayConflictHelperImpl::Setting collected changes on GFE conflict helper");
    }
    gfeConflictHelper.changeEventValue(collectedChanges);
  }
  
  /*
   * Saves a change
   */
  private void saveChange(Region<?,?> region, SerializableDelta delta) {
    // TODO: Change the way we get the logger here
    LogWriterI18n logger = GemFireCacheImpl.getExisting().getLoggerI18n();
    if(logger.fineEnabled()) {
      logger.fine("GatewayConflictHelperImpl::Saving change:" + delta);
    }
    if(changes == null) {
      changes = delta;
    } else {
      changes = (SerializableDelta) changes.merge(region, delta);
    }
    logger.fine("GatewayConflictHelperImpl::Collected changes:" + changes);
  }
  
  @Override
  public void disallowEvent() {
    assert(gfeConflictHelper != null);
    gfeConflictHelper.disallowEvent();
  }

  /*
   * Unless the user specifically sets a value on a column, the event's row
   * should be used.  So any modifications are actually on the new row.
   * The old row should be looked up only when we want to update a column that doesn't exist 
   * in the new row.
   * Cases:
   * 1. Insert:
   * Here we get the entire new row.  So setColumn should update DVDs in this row.
   * 2. Update:
   * Here we get a delta in the event. So take the old row, apply the delta and get a new row first.
   * Then set the value on the particular DVD.
   * 3. Delete:
   * User can't do a setColumn.  (?)
   * 
   * For each case, disallow setColumn operation on PK column or partition column
   * 
   */
  @Override
  public void setColumnValue(int index, Object value) throws Exception {
    assert(gfeConflictHelper != null);
    assert(gatewayEvent != null);

    LogWriterI18n logger = GemFireCacheImpl.getExisting().getLoggerI18n();
    if(logger.fineEnabled()) {
      logger.fine("GatewayConflictHelperImpl::Attempting to set column-"
          + index + ", value=" + value.toString());
    }
    
    EntryEventImpl event = gatewayEvent.getEntryEvent();
    EventImpl eventImpl = gatewayEvent.getEventImpl();
 
    GemFireContainer container = eventImpl.getContainer();
    int numColumns = container.getNumColumns();

    FormatableBitSet changedColumnsBitSet = new FormatableBitSet(numColumns);
    changedColumnsBitSet.grow(numColumns);
    changedColumnsBitSet.set(index - 1);
    
    // Construct a new DVD for the changed column
    DataValueDescriptor[] changedRow = new DataValueDescriptor[numColumns];
    RowFormatter rf = container.getCurrentRowFormatter();
    DataValueDescriptor columnDVD = rf.getNewNull(index);

    // Initialize the DVD with the new value
    columnDVD.setObjectForCast(value, true, value.getClass().getName());
    changedRow[index-1] = columnDVD;
    
    // Construct a a new delta with this DVD
    SerializableDelta newDelta = new SerializableDelta(changedRow, changedColumnsBitSet);
    
    // If the event has a delta (UPDATE case), merge our new delta with it
    if(event.hasDelta()) {
      SerializableDelta eventDelta = eventImpl.getSerializableDelta();
      if(eventDelta != null) {
        if(logger.fineEnabled()) {
          logger.fine("Event's Delta="+eventDelta);
        }
        SerializableDelta mergedDelta = (SerializableDelta) eventDelta.merge(event.getRegion(), newDelta);
        if(logger.fineEnabled()) {
          logger.fine("Merged Delta as New Value ="+mergedDelta);
        }
        newDelta = mergedDelta;
      }
    }
    saveChange(event.getRegion(), newDelta);
  }
  public static void dummy() {
  }

}
