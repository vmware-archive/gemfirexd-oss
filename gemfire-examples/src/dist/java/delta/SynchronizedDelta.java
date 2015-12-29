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

import com.gemstone.gemfire.Delta;
import com.gemstone.gemfire.InvalidDeltaException;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Sample implementation of Delta
 * 
 * @author GemStone Systems, Inc.
 * @since 6.1
 */
public class SynchronizedDelta implements Delta, Serializable {

  private long longVal;
  private double doubleVal;

  
  public SynchronizedDelta(long i, double d) {
    this.longVal = i;
    this.doubleVal = d;
  }
//one boolean per field to manage changed status
  private transient boolean lgFldChd = false;
  private transient boolean dblFldChd = false;
  public boolean hasDelta() {
    return this.lgFldChd || this.dblFldChd;
  }

  public synchronized void toDelta(DataOutput out) throws IOException {
    System.out.println("Extracting delta in synchronized block from " + this.toString());

    out.writeBoolean(lgFldChd);
    if (lgFldChd) {
      out.writeLong(this.longVal);
      this.lgFldChd = false;
      System.out.println(" Extracted delta from field 'longVal' = "
                        + this.longVal);
    }
    out.writeBoolean(dblFldChd);
    if (dblFldChd) {
      out.writeDouble(this.doubleVal);
      this.dblFldChd = false;
      System.out.println(" Extracted delta from field 'doubleVal' = "
                        + this.doubleVal);
    }
  }

  public synchronized void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
    System.out.println("Applying delta in synchronized block to " + this.toString());

    if (in.readBoolean()) {
      this.longVal = in.readLong();
      System.out.println(" Applied delta to field 'longVal' = "
            + this.longVal);
    }
    if (in.readBoolean()) {
      this.doubleVal = in.readDouble();
      System.out.println(" Applied delta to field 'doubleVal' = "
            + this.doubleVal);
    }
  }

  public void setLongVal(long aLongVal) {
    this.lgFldChd = true;
    this.longVal = aLongVal;
  }

  public void setDoubleVal(double aDoubleVal) {
    this.dblFldChd = true;
    this.doubleVal = aDoubleVal;
  }

  @Override
  public String toString() {
    return "SynchronizedDelta [ hasDelta = " + hasDelta() + ", longVal = " + this.longVal
        + ", doubleVal = {" + this.doubleVal + "} ]";
  }
}
