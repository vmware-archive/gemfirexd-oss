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

package com.pivotal.gemfirexd.internal.engine.ddl.wan.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EnumListenerEvent;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.Token;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;

/**
 * 
 * @author Asif, Yogesh
 */
public final class BulkDBSynchronizerMessage extends
    AbstractDBSynchronizerMessage {

  // These fields are not written directly
  private transient GfxdCBArgForSynchPrms cbArg;

  private boolean skipListeners;

  // reusing an otherwise unused bitmask
  private static final short SKIP_LISTENERS = UNRESERVED_FLAGS_START;

  public BulkDBSynchronizerMessage() {
  }

  public BulkDBSynchronizerMessage(LocalRegion rgn, String dmlString,
      boolean skipListeners, GfxdCBArgForSynchPrms arg)
      throws StandardException {
    super(rgn);
    this.cbArg = arg;
    // setting Token.INVALID rather than null since old code relied on
    // this.cbArg being set there and need to make sure that we are no longer
    // relying on value carrying this.cbArg (i.e. we get a ClassCastException
    // somewhere otherwise)
    this.getEntryEventImpl().setNewValue(Token.INVALID);
    this.getEntryEventImpl().setCallbackArgument(this.cbArg);
    this.getEntryEventImpl().disallowOffHeapValues();
    this.skipListeners = skipListeners;
  }

  @Override
  public byte getGfxdID() {
    return BULK_DB_SYNCH_MESSG;
  }

  @Override
  public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.skipListeners = (flags & SKIP_LISTENERS) != 0;
    try {
      // Read EventID
      EntryEventImpl event = this.getEntryEventImpl();
      if (event != null) {
        this.cbArg = new GfxdCBArgForSynchPrms();
        InternalDataSerializer.invokeFromData(this.cbArg, in);
        event.setNewValue(Token.INVALID);
        event.setCallbackArgument(this.cbArg);
      }
    } catch (IOException ioe) {
      throw ioe;
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  public void toData(DataOutput out)
      throws IOException {
    super.toData(out);
    try {
      InternalDataSerializer.invokeToData(this.cbArg, out);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  @Override
  protected short computeCompressedShort(short flags) {
    flags = super.computeCompressedShort(flags);
    if (this.skipListeners) flags |= SKIP_LISTENERS;
    return flags;
  }

  public GfxdCBArgForSynchPrms getGfxdCBArgForSynchPrms() {
    return this.cbArg;  
  }

  @Override
  protected void appendFields(final StringBuilder sb) {
    super.appendFields(sb);
    sb.append(";GfxdCBArgForSynchPrms=").append(this.cbArg)
        .append(";skipListeners=").append(this.skipListeners);
  }

  @Override
  Operation getOperation() {
    return Operation.BULK_DML_OP;
  }

  @Override
  EnumListenerEvent getListenerEvent() {
    return EnumListenerEvent.AFTER_BULK_DML_OP;
  }

  @Override
  boolean skipListeners() {
    return this.skipListeners;
  }
}
