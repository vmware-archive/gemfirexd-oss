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
package com.pivotal.gemfirexd.internal.engine.ddl.callbacks.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.LogWriter;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdSerializable;
import com.pivotal.gemfirexd.internal.engine.ddl.callbacks.CallbackProcedures;
import com.pivotal.gemfirexd.internal.engine.ddl.wan.messages.AbstractGfxdReplayableMessage;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;

public class GfxdRemoveLoaderMessage extends AbstractGfxdReplayableMessage {

  private static final long serialVersionUID = 9042246545142608810L;

  private String schema;

  private String table;

  public GfxdRemoveLoaderMessage() {
  }

  public GfxdRemoveLoaderMessage(String schema, String table) {
    this.schema = schema;
    this.table = table;
  }

  @Override
  public void execute() throws StandardException {
    LogWriter logger = Misc.getGemFireCache().getLoggerI18n()
        .convertToLogWriter();
    if (logger.infoEnabled()) {
      logger.info("GfxdRemoveLoaderMessage: Executing with fields as: "
          + this.toString());
    }
    CallbackProcedures.removeGfxdCacheLoaderLocally(CallbackProcedures
        .getContainerForTable(this.schema, this.table));
  }

  @Override
  public byte getGfxdID() {
    return GfxdSerializable.REMOVE_LOADER_MSG;
  }

  @Override
  public void toData(DataOutput out)
      throws IOException {
    super.toData(out);
    DataSerializer.writeString(this.schema, out);
    DataSerializer.writeString(this.table, out);
  }

  @Override
  public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.schema = DataSerializer.readString(in);
    this.table = DataSerializer.readString(in);
  }

  @Override
  public void appendFields(final StringBuilder sb) {
    super.appendFields(sb);
    sb.append("; schema = ");
    sb.append(this.schema);
    sb.append("; table = ");
    sb.append(this.table);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean shouldBeConflated() {
    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getRegionToConflate() {
    return this.schema + '.' + this.table;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object getKeyToConflate() {
    return GfxdSetLoaderMessage.CONFLATION_KEY_PREFIX;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object getValueToConflate() {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getSQLStatement() {
    final StringBuilder sb = new StringBuilder();
    return sb.append("CALL SYS.REMOVE_LOADER('").append(this.schema)
        .append("','").append(this.table).append("')").toString();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getSchemaName() {
    return this.schema;
  }
}
