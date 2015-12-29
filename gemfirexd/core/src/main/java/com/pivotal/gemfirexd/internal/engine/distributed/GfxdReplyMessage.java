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

package com.pivotal.gemfirexd.internal.engine.distributed;

import java.io.DataOutput;

import com.gemstone.gemfire.distributed.internal.ReplyMessage;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.cache.AbstractOperationMessage;
import com.pivotal.gemfirexd.internal.engine.GfxdDataSerializable;
import com.pivotal.gemfirexd.internal.engine.GfxdSerializable;

/**
 * Base class for GemFireXD distribution reply messages. This handles
 * registration and proper serialization of GemFireXD reply messages.
 * 
 * To add a new {@link GfxdReplyMessage} implementation, add the classId for it
 * in {@link GfxdSerializable} which needs to be returned by
 * {@link #getGfxdID()} and its registration in
 * {@link GfxdDataSerializable#initTypes()}. Also override the
 * {@link #fromData(java.io.DataInput)} and {@link #toData(DataOutput)} with a
 * call to the base class method at the very beginning.
 * 
 * @author Sumedh Wale
 * @since 6.0
 */
public abstract class GfxdReplyMessage extends ReplyMessage implements
    GfxdSerializable {

  public GfxdReplyMessage() {
  }

  public GfxdReplyMessage(final AbstractOperationMessage sourceMessage,
      final boolean sendTXChanges, final boolean finishTXRead,
      final boolean flushPendingOps) {
    super(sourceMessage, sendTXChanges, finishTXRead, flushPendingOps);
  }

  @Override
  public final int getDSFID() {
    return DataSerializableFixedID.GFXD_TYPE;
  }

  public abstract byte getGfxdID();

  public abstract GfxdResponseCode getResponseCode();

  /** no inline processing by default */
  @Override
  public boolean getInlineProcess() {
    return false;
  }
}
