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

package com.gemstone.gemfire.internal.cache;

import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.internal.cache.delta.Delta;

/**
 * Wraps a {@link CacheWriterException} for transactions. Used to undo the
 * changes for current operation on other nodes that did not have the failure.
 * 
 * @author swale
 * @since 7.0
 */
public final class TXCacheWriterException extends CacheWriterException {

  private static final long serialVersionUID = 1885220451811584517L;

  /**
   * The original operation of the failed entry.
   * 
   * Transient to avoid default serialization from kicking in.
   */
  private transient byte originalOp;

  /**
   * The original destroy operation flag of the failed entry.
   * 
   * Transient to avoid default serialization from kicking in.
   */
  private transient byte destroy;

  /**
   * The original bulk operation flag of the failed entry.
   * 
   * Transient to avoid default serialization from kicking in.
   */
  private transient boolean bulkOp;

  /**
   * The original value of the failed entry.
   * 
   * Transient to avoid default serialization from kicking in.
   */
  private transient Object originalValue;

  /**
   * The original delta of the failed entry.
   * 
   * Transient to avoid default serialization from kicking in.
   */
  private transient Delta originalDelta;

  /**
   * Creates a new instance of <code>TXCacheWriterException</code>.
   */
  public TXCacheWriterException() {
  }

  /**
   * Constructs an instance of <code>TXCacheWriterException</code> with the
   * specified detail message.
   * 
   * @param msg
   *          the detail message
   */
  public TXCacheWriterException(String msg) {
    super(msg);
  }

  /**
   * Constructs an instance of <code>TXCacheWriterException</code> with the
   * specified detail message and cause.
   * 
   * @param msg
   *          the detail message
   * @param cause
   *          the causal Throwable
   */
  public TXCacheWriterException(String msg, Throwable cause) {
    super(msg, cause);
  }

  /**
   * Constructs an instance of <code>TXCacheWriterException</code> with the
   * specified cause.
   * 
   * @param cause
   *          the causal Throwable
   */
  public TXCacheWriterException(Throwable cause) {
    super(cause);
  }

  /**
   * Constructs an instance of <code>TXCacheWriterException</code> for the
   * specified <code>CacheWriterException</code>.
   * 
   * @param cwe
   *          the causal CacheWriterException
   * @param op
   *          the previous operation for the current entry that failed
   * @param destroy
   *          the previous destroy op flag for the current entry that failed
   * @param bulkOp
   *          the previous bulk op flags for the current entry that failed
   * @param value
   *          the value of the current entry that failed
   * @param delta
   *          the delta of the current entry that failed
   */
  public TXCacheWriterException(final CacheWriterException cwe, final byte op,
      final byte destroy, final boolean bulkOp, final Object value,
      final Delta delta) {
    super(cwe.getMessage(), cwe);
    this.originalOp = op;
    this.destroy = destroy;
    this.bulkOp = bulkOp;
    this.originalValue = value;
    this.originalDelta = delta;
  }

  /**
   * Get the original operation of the failed entry.
   */
  public final byte getOriginalOp() {
    return this.originalOp;
  }

  /**
   * Get the original destroy operation of the failed entry.
   */
  public final byte getOriginalDestroy() {
    return this.destroy;
  }

  /**
   * Get the original bulk operation flag of the failed entry.
   */
  public final boolean getOriginalBulkOp() {
    return this.bulkOp;
  }

  /**
   * Get the original value of the failed entry.
   */
  public final Object getOriginalValue() {
    return this.originalValue;
  }

  /**
   * Get the original value of the failed entry.
   */
  public final Delta getOriginalDelta() {
    return this.originalDelta;
  }

  // Overrides for Serializable to serialize currentValue using DataSerializer

  private synchronized void writeObject(final java.io.ObjectOutputStream out)
      throws IOException {
    getStackTrace(); // Ensure that stackTrace field is initialized.
    out.defaultWriteObject();
    // now write the original ops and value
    out.writeByte(this.originalOp);
    out.writeByte(this.destroy);
    out.writeBoolean(this.bulkOp);
    DataSerializer.writeObject(this.originalValue, out);
    DataSerializer.writeObject(this.originalDelta, out);
  }

  private void readObject(final java.io.ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    // now read the original ops and value
    this.originalOp = in.readByte();
    this.destroy = in.readByte();
    this.bulkOp = in.readBoolean();
    this.originalValue = DataSerializer.readObject(in);
    this.originalDelta = DataSerializer.readObject(in);
  }
}
