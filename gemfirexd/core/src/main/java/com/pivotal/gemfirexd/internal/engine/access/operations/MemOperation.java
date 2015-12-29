
/*

 Derived from source files from the Derby project.

 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to you under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

 */

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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

/*
 This file was based on the MemStore patch written by Knut Magne, published
 under the Derby issue DERBY-2798 and released under the same license,
 ASF, as described above. The MemStore patch was in turn based on Derby source
 files.
*/

package com.pivotal.gemfirexd.internal.engine.access.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.gemstone.gemfire.internal.cache.Conflatable;
import com.gemstone.gemfire.internal.cache.EventID;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.LimitObjectInput;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Compensation;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Loggable;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Transaction;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Undoable;
import com.pivotal.gemfirexd.internal.iapi.store.raw.log.LogInstant;
import com.pivotal.gemfirexd.internal.iapi.util.ByteArray;

/**
 * DOCUMENT ME!
 */
public abstract class MemOperation implements Loggable, Undoable, Compensation,
    Conflatable {

  /**
   * The container for the current operation.
   */
  protected GemFireContainer memcontainer;

  /**
   * Creates a new $class.name$ object.
   * 
   * @param memcontainer
   *          DOCUMENT ME!
   * @param slotId
   *          DOCUMENT ME!
   */
  protected MemOperation(GemFireContainer memcontainer) {
    this.memcontainer = memcontainer;
  }

  public final void readExternal(ObjectInput in) throws IOException,
      ClassNotFoundException {
    throw new UnsupportedOperationException("unexpected call");
  }

  public final void writeExternal(ObjectOutput out) throws IOException {
    throw new UnsupportedOperationException("unexpected call");
  }

  public final int getTypeFormatId() {
    throw new UnsupportedOperationException("unexpected call");
  }

  public final boolean needsRedo(Transaction xact) throws StandardException {
    throw new UnsupportedOperationException("unexpected call");
  }

  public abstract void doMe(Transaction xact, LogInstant instant,
      LimitObjectInput in) throws StandardException, IOException;

  public abstract Compensation generateUndo(Transaction xact,
      LimitObjectInput in) throws StandardException, IOException;

  /**
   * Returns true if the actual operation has to be performed at commit or
   * abort.
   */
  public boolean doAtCommitOrAbort() {
    return false;
  }

  /**
   * the default for prepared log is always null for all the operations that
   * don't have optionalData. If an operation has optional data, the operation
   * need to prepare the optional data for this method. A MemOperation has no
   * optional data to write out
   */
  public final ByteArray getPreparedLog() throws StandardException {
    throw new UnsupportedOperationException("unexpected call");
  }

  public final int group() {
    throw new UnsupportedOperationException("unexpected call");
  }

  public final void releaseResource(Transaction xact) {
    throw new UnsupportedOperationException("unexpected call");
  }

  public final void setUndoOp(Undoable op) {
    throw new UnsupportedOperationException("unexpected call");
  }

  public boolean shouldBeConflated() {
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean shouldBeMerged() {
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean merge(Conflatable existing) {
    throw new AssertionError("not expected to be invoked");
  }

  public final String getRegionToConflate() {
    return (this.memcontainer != null ? this.memcontainer
        .getQualifiedTableName() : null);
  }

  public Object getKeyToConflate() {
    return null;
  }

  public Object getValueToConflate() {
    return null;
  }

  public final void setLatestValue(Object value) {
    throw new AssertionError("MemOperation#setLatestValue "
        + "not expected to be invoked");
  }

  public final EventID getEventId() {
    throw new AssertionError("MemOperation#getEventId "
        + "not expected to be invoked");
  }

  @Override
  public final String toString() {
    String regionName = (this.memcontainer != null ? this.memcontainer
        .getQualifiedTableName() : "null");
    return toStringBuilder(new StringBuilder(), regionName).toString();
  }

  protected StringBuilder toStringBuilder(StringBuilder sb, String regionName) {
    return sb.append(this.getClass().getName()).append(" region=").append(
        regionName).append(" key=").append(getKeyToConflate())
        .append(" value=").append(getValueToConflate());
  }
  
  public boolean isIndexMemOperation() {
    return false;
  }
}
