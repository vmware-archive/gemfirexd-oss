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
import java.sql.SQLException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.cache.execute.InternalFunctionInvocationTargetException;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.GfxdSerializable;
import com.pivotal.gemfirexd.internal.engine.ddl.callbacks.CallbackProcedures;
import com.pivotal.gemfirexd.internal.engine.ddl.wan.messages.AbstractGfxdReplayableMessage;
import com.pivotal.gemfirexd.internal.engine.distributed.FunctionExecutionException;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;

/**
 * 
 * @author kneeraj
 * 
 */
public class GfxdAddListenerMessage extends AbstractGfxdReplayableMessage {

  private static final long serialVersionUID = -5118777374426905270L;

  private String id;

  private String schema;

  private String table;

  private String implementation;

  private String initInfoStr;

  private String serverGroups;

  static final String CONFLATION_KEY_PREFIX = "__GFXD_INTERNAL_GFXDLISTENER.";

  protected static final short HAS_SERVER_GROUPS = UNRESERVED_FLAGS_START;

  public GfxdAddListenerMessage() {
  }

  public GfxdAddListenerMessage(String id, String schema, String table,
      String implementation, String initInfo, String serverGroups) {
    this.id = id;
    this.schema = schema;
    this.table = table;
    this.implementation = implementation;
    this.initInfoStr = initInfo;
    this.serverGroups = serverGroups;
  }

  @Override
  public void execute() throws StandardException {
    EmbedConnection conn = null;
    boolean contextSet = false;
    try {
      conn = GemFireXDUtils.getTSSConnection(true, true, false);
      conn.getTR().setupContextStack();
      contextSet = true;
      SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_QUERYDISTRIB,
          "GfxdAddListenerMessage: Executing with fields as: "
              + this.toString());
      CallbackProcedures.addGfxdCacheListenerLocally(this.id,
          CallbackProcedures.getContainerForTable(this.schema, this.table),
          this.implementation, this.initInfoStr, this.serverGroups);
    } catch (Exception ex) {
      if (GemFireXDUtils.TraceApplicationJars) {
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_QUERYDISTRIB,
            "GfxdAddListenerMessage#execute: exception encountered", ex);
      }
      if (GemFireXDUtils.retryToBeDone(ex)) {
        throw new InternalFunctionInvocationTargetException(ex);
      }
      throw new FunctionExecutionException(ex);
    } finally {
      if (contextSet) {
        try {
          conn.internalCommit();
        } catch (SQLException ex) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_QUERYDISTRIB,
              "GfxdAddListenerMessage#execute: exception "
                  + "encountered in commit", ex);
        }
        conn.getTR().restoreContextStack();
      }
    }
  }

  @Override
  public byte getGfxdID() {
    return GfxdSerializable.ADD_LISTENER_MSG;
  }

  @Override
  public void toData(DataOutput out)
      throws IOException {
    super.toData(out);
    DataSerializer.writeString(this.id, out);
    DataSerializer.writeString(this.schema, out);
    DataSerializer.writeString(this.table, out);
    DataSerializer.writeString(this.implementation, out);
    DataSerializer.writeString(this.initInfoStr, out);
    if (this.serverGroups != null) {
      DataSerializer.writeString(this.serverGroups, out);
    }
  }

  @Override
  public void fromData(DataInput in)
      throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.id = DataSerializer.readString(in);
    this.schema = DataSerializer.readString(in);
    this.table = DataSerializer.readString(in);
    this.implementation = DataSerializer.readString(in);
    this.initInfoStr = DataSerializer.readString(in);
    if ((flags & HAS_SERVER_GROUPS) != 0) {
      this.serverGroups = DataSerializer.readString(in);
    }
  }

  @Override
  protected short computeCompressedShort(short flags) {
    flags = super.computeCompressedShort(flags);
    if (this.serverGroups != null) flags |= HAS_SERVER_GROUPS;
    return flags;
  }

  @Override
  public void appendFields(final StringBuilder sb) {
    super.appendFields(sb);
    sb.append("; id=").append(this.id);
    sb.append("; schema=").append(this.schema);
    sb.append("; table=").append(this.table);
    sb.append("; implementation=").append(this.implementation);
    sb.append("; initinfostr=").append(this.initInfoStr);
    if (this.serverGroups != null) {
      sb.append("; serverGroups=").append(this.serverGroups);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean shouldBeConflated() {
    return false;
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
    return CONFLATION_KEY_PREFIX + this.id;
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
    sb.append("CALL SYS.ADD_LISTENER('").append(this.id).append("','")
        .append(this.schema).append("','").append(this.table).append("','")
        .append(this.implementation).append("','").append(this.initInfoStr)
        .append("',");
    if (this.serverGroups != null) {
      sb.append('\'').append(this.serverGroups).append('\'');
    }
    else {
      sb.append("NULL");
    }
    return sb.append(')').toString();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getSchemaName() {
    return this.schema;
  }
}
