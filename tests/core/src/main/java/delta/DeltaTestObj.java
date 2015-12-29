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
package delta;

import hydra.TestConfig;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DeltaTestImpl;
import com.gemstone.gemfire.InvalidDeltaException;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;

/**
 * 
 * Introduced for strict validation
 * 
 * @author aingle
 * @since 6.1
 */
public class DeltaTestObj extends DeltaTestImpl {
  /**
   * to keep track of how many time toDelta called
   */
  private transient long toDeltaCounter;

  /**
   * to keep track of how many time fromDelta called
   */
  private transient long fromDeltaCounter;
  
  public static final boolean enableFailure = TestConfig.tab().booleanAt(
      DeltaPropagationPrms.enableFailure, false);

  public DeltaTestObj(int intVal, String str) {
    super(intVal, str);
  }

  public DeltaTestObj(int intVal, String str, long toDeltaCounter) {
    super(intVal, str);
    this.toDeltaCounter = toDeltaCounter;
  }

  public DeltaTestObj(DeltaTestObj obj) {
    super(obj.getIntVar(), obj.getStr(), obj.getDoubleVar(), obj.getByteArr(),
        obj.getTestObj());
    this.toDeltaCounter = obj.getToDeltaCounter();
    this.fromDeltaCounter = obj.getFromDeltaCounter();
  }

  public DeltaTestObj() {
    super();
  }

  public long getFromDeltaCounter() {
    return fromDeltaCounter;
  }

  public long getToDeltaCounter() {
    return toDeltaCounter;
  }

  public void toDelta(DataOutput out) throws IOException {
    this.toDeltaCounter++;
    super.toDelta(out);
    if (GemFireCacheImpl.getInstance().getLogger().fineEnabled())
      GemFireCacheImpl.getInstance().getLogger().fine(
          "toDeltaCounter: " + this.toDeltaCounter);
  }

  public void fromDelta(DataInput in) throws IOException {
    this.fromDeltaCounter++;
    super.fromDelta(in);
    if (GemFireCacheImpl.getInstance().getLogger().fineEnabled())
      GemFireCacheImpl.getInstance().getLogger().fine(
          "fromDeltaCounter: " + this.fromDeltaCounter);
  }
  
  public void setFromDeltaCounter(int counter){
    this.fromDeltaCounter=counter;
  }

  public void setToDeltaCounter(int counter){
    this.toDeltaCounter=counter;
  }

  protected void checkInvalidInt2(int intVal) {
    if (enableFailure) {
      if (intVal%30 == 27) {
        throw new InvalidDeltaException("Delta could not be applied. " + this);
      }
    }
  }

  public String toString() {
    StringBuffer bytes = new StringBuffer("");
    byte[] byteArr = super.getByteArr();
    if (byteArr != null) {
      for (int i = 0; i < byteArr.length; i++) {
        bytes.append(byteArr[i]);
      }
    }
    return "DeltaTestObj[int=" + super.getIntVar() + ",double="
        + super.getDoubleVar() + ",str=" + super.getStr() + ",bytes=("
        + bytes.toString() + "),testObj="
        + ((super.getTestObj() != null) ? super.getTestObj().hashCode() : "")
        + ",toDeltaCounter=" + this.toDeltaCounter + ",fromDeltaCounter="
        + this.fromDeltaCounter + "]";
  }
}
