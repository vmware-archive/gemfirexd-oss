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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import com.gemstone.gemfire.internal.InternalDataSerializer;

/**
 * Class for response codes for {@link GfxdReplyMessage}s. Also allows for
 * processing of the reply as per the response code using the given
 * {@link GfxdReplyMessageProcessor}.
 * 
 * @author swale
 */
public final class GfxdResponseCode implements Serializable {

  private static final long serialVersionUID = -3947107718381507844L;

  private static final int C_TIMEOUT = 0;

  private static final int C_EXCEPTION = 1;

  private static final int C_WAITING_START = 2;

  private static final int MIN_CACHED_ORD = Byte.MIN_VALUE;

  private static final int MAX_CACHED_ORD = Byte.MAX_VALUE;

  private static final GfxdResponseCode[] codeCache;

  public static final GfxdResponseCode TIMEOUT;

  public static final GfxdResponseCode EXCEPTION;

  private final int ordinal;

  static {
    codeCache = new GfxdResponseCode[MAX_CACHED_ORD - MIN_CACHED_ORD + 1];
    for (int code = MIN_CACHED_ORD; code <= MAX_CACHED_ORD; ++code) {
      codeCache[code - MIN_CACHED_ORD] = new GfxdResponseCode(code);
    }
    TIMEOUT = codeCache[C_TIMEOUT - MIN_CACHED_ORD];
    EXCEPTION = codeCache[C_EXCEPTION - MIN_CACHED_ORD];
  }

  private GfxdResponseCode(int ordinal) {
    this.ordinal = ordinal;
  }

  public boolean isTimeout() {
    return (this.ordinal == C_TIMEOUT);
  }

  public boolean isException() {
    return (this.ordinal == C_EXCEPTION);
  }

  public static GfxdResponseCode WAITING(int sequenceId) {
    assert sequenceId >= 0;
    int ordinal = sequenceId + C_WAITING_START;
    if (ordinal <= MAX_CACHED_ORD) {
      return codeCache[ordinal - MIN_CACHED_ORD];
    }
    return new GfxdResponseCode(ordinal);
  }

  public boolean isWaiting() {
    return (this.ordinal >= C_WAITING_START);
  }

  public int waitingSequenceId() {
    return (this.ordinal - C_WAITING_START);
  }

  public static GfxdResponseCode GRANT(int sequenceId) {
    assert sequenceId > 0;
    int ordinal = -sequenceId;
    if (ordinal >= MIN_CACHED_ORD) {
      return codeCache[ordinal - MIN_CACHED_ORD];
    }
    return new GfxdResponseCode(ordinal);
  }

  public boolean isGrant() {
    return (this.ordinal < 0);
  }

  public int grantedSequenceId() {
    return -this.ordinal;
  }

  public void toData(DataOutput out) throws IOException {
    InternalDataSerializer.writeUnsignedVL(this.ordinal, out);
  }

  public static GfxdResponseCode fromData(DataInput in) throws IOException {
    int ordinal = (int)InternalDataSerializer.readUnsignedVL(in);
    if (ordinal >= MIN_CACHED_ORD && ordinal <= MAX_CACHED_ORD) {
      return codeCache[ordinal - MIN_CACHED_ORD];
    }
    return new GfxdResponseCode(ordinal);
  }

  @Override
  public boolean equals(final Object other) {
    if (other instanceof GfxdResponseCode) {
      return (((GfxdResponseCode)other).ordinal == this.ordinal);
    }
    return false;
  }

  public boolean equals(final GfxdResponseCode other) {
    return (other.ordinal == this.ordinal);
  }

  @Override
  public int hashCode() {
    return this.ordinal;
  }

  @Override
  public String toString() {
    switch (this.ordinal) {
      case C_TIMEOUT:
        return "TIMEOUT";
      case C_EXCEPTION:
        return "EXCEPTION";
      default:
        if (isWaiting()) {
          return "WAITING[" + waitingSequenceId() + ']';
        }
        else {
          return "GRANT[" + grantedSequenceId() + ']';
        }
    }
  }
}
