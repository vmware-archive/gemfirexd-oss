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

package com.pivotal.gemfirexd.internal.engine.ddl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import com.gemstone.gemfire.DataSerializer;
import com.pivotal.gemfirexd.internal.engine.GfxdDataSerializable;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

/**
 * This is an object to persist and send the maximum value determined for
 * IDENTITY columns when using ALTER TABLE ... SET GENERATED ALWAYS AS IDENTITY
 * from query node to other nodes. Since this goes into the DDL queue, it allows
 * every node to reliably get the same value when recovering from persisted data
 * without having to evaluate it again.
 * 
 * @author swale
 * @since 7.0
 */
public final class PersistIdentityStart extends GfxdDataSerializable {

  private String schemaName;
  private String tableName;
  private String columnName;
  private long startValue;

  /** for deserialization */
  public PersistIdentityStart() {
  }

  public PersistIdentityStart(final String schemaName, final String tableName,
      final String columnName, long startValue) {
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.columnName = columnName;
    this.startValue = startValue;
  }

  public void checkMatchingColumn(final String schemaName,
      final String tableName, final String columnName) {
    if (!schemaName.equals(this.schemaName)
        || !tableName.equals(this.tableName)
        || !columnName.equals(this.columnName)) {
      SanityManager.THROWASSERT("Expected table=" + schemaName + '.'
          + tableName + " column=" + columnName + " but got " + toString());
    }
  }

  public long getStartValue() {
    return this.startValue;
  }

  @Override
  public void toData(final DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeString(this.schemaName, out);
    DataSerializer.writeString(this.tableName, out);
    DataSerializer.writeString(this.columnName, out);
    out.writeLong(this.startValue);
  }

  @Override
  public void fromData(DataInput in) throws IOException,
          ClassNotFoundException {
    super.fromData(in);
    this.schemaName = DataSerializer.readString(in);
    this.tableName = DataSerializer.readString(in);
    this.columnName = DataSerializer.readString(in);
    this.startValue = in.readLong();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public byte getGfxdID() {
    return PERSIST_IDENTITY_START;
  }

  @Override
  public String toString() {
    return new StringBuilder().append("PersistIdentityStart:table=")
        .append(this.schemaName).append('.').append(this.tableName)
        .append(";column=").append(this.columnName).append(";startValue=")
        .append(this.startValue).toString();
  }
}
