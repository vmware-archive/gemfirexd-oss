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

import java.sql.ResultSet;

import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.TimestampedEntryEventImpl;
import com.pivotal.gemfirexd.callbacks.Event.Type;
import com.pivotal.gemfirexd.callbacks.impl.GatewayEvent;
import com.pivotal.gemfirexd.callbacks.TableMetaData;
import com.pivotal.gemfirexd.callbacks.impl.GatewayEvent.GatewayEventType;
import com.pivotal.gemfirexd.internal.engine.ddl.EventImpl;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer.SerializableDelta;
import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;

public class GatewayEventImpl implements GatewayEvent {

  private TimestampedEntryEventImpl entryEvent;
  private EventImpl eventImpl;
  private GatewayEvent.GatewayEventType type;
  
  public GatewayEventImpl(TimestampedEntryEventImpl entryEvent) {
    this.entryEvent = entryEvent;
    // We've to manage three representations of operations -
    // Operation, Type and GatewayEventType
    Type eventType = null;
    Operation op = entryEvent.getOperation();
    if(op.equals(Operation.CREATE)) {
      eventType = Type.AFTER_INSERT;
      this.type = GatewayEventType.INSERT;
    } else if (op.equals(Operation.UPDATE)) {
      eventType = Type.AFTER_UPDATE;
      this.type = GatewayEventType.UPDATE;
    } else if(op.equals(Operation.DESTROY)) {
      eventType = Type.AFTER_DELETE;
      this.type = GatewayEventType.DELETE;
    } else {
      throw new AssertionError("Unexpected operation type " + op + " for gateway event: " + entryEvent);
    }
    this.eventImpl = new EventImpl(entryEvent, eventType);
  }
  
  
  public TimestampedEntryEventImpl getEntryEvent(){
    return entryEvent;
  }
  
  public EventImpl getEventImpl() {
    return eventImpl;
  }
  
  @Override
  public int getNewDistributedSystemID() {
    return entryEvent.getNewDistributedSystemID();
  }

  @Override
  public int getOldDistributedSystemID() {
    return entryEvent.getOldDistributedSystemID();
  }

  @Override
  public long getNewTimestamp() {
    return entryEvent.getNewTimestamp();
  }

  @Override
  public long getOldTimestamp() {
    return entryEvent.getOldTimestamp();
  }

  @Override
  public GatewayEventType getType() {
    return this.type;
  }

  @Override
  public ResultSet getNewRow() {
    return eventImpl.getNewRowsAsResultSet();
  }

  @Override
  public int[] getModifiedColumns() {
    return eventImpl.getModifiedColumns();
  }
  
  @Override
  public ResultSet getOldRow() {
    return eventImpl.getOldRowAsResultSet();
  }

  @Override
  public TableMetaData getTableMetaData() {
    return eventImpl.getResultSetMetaData();
  }

  @Override
  public String getSchemaName() {
    return eventImpl.getSchemaName();
  }

  @Override
  public String getTableName() {
    return eventImpl.getTableName();
  }

  @Override
  public ResultSet getPrimaryKeys() {
    return eventImpl.getPrimaryKeysAsResultSet();
  }

  @Override
  public boolean isTransactional() {
    return false;
  }
  
  @Override
  public String toString() {
    return entryEvent.toString() + ",GatewayEventType=" + type;
  }
  
  public static void dummy() {
    GatewayConflictResolverWrapper.dummy();
  }

}
