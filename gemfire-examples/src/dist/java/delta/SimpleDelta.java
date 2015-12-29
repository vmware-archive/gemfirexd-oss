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
 * Sample implementation of Delta. This class is not threadsafe, so multiple
 * threads cannot be putting deltas into the cache unless cloning is enabled on
 * the region.
 * 
 * For a delta object that is thread safe with cloning disabled, see
 * {@link SynchronizedDelta}
 * 
 * @author GemStone Systems, Inc.
 * @since 6.1
 */
public class SimpleDelta implements Delta, Serializable {

  private int intVal;
  private double doubleVal;
  
  public SimpleDelta(){}
  
  public SimpleDelta(int i, double d){
    this.intVal = i;
    this.doubleVal = d;
  }
  // one boolean per field to manage changed status
  private transient boolean intFldChd = false;
  private transient boolean dblFldChd = false;

  public boolean hasDelta() {
    return this.intFldChd || this.dblFldChd;
  }

  public void toDelta(DataOutput out) throws IOException {
    System.out.println("Extracting delta from " + this.toString());
    out.writeBoolean(intFldChd);
    if (intFldChd) {
      out.writeInt(this.intVal);
      this.intFldChd = false;
      System.out.println(" Extracted delta from field 'intVal' = "
                        + this.intVal);
    }
    out.writeBoolean(dblFldChd);
    if (dblFldChd) {
      out.writeDouble(this.doubleVal);
      this.dblFldChd = false;
      System.out.println(" Extracted delta from field 'doubleVal' = "
                        + this.doubleVal);
    }
  }

  public void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
    System.out.println("Applying delta to " + this.toString());
    if (in.readBoolean()) {
      this.intVal = in.readInt();
      System.out.println(" Applied delta to field 'intVal' = "
            + this.intVal);
    }
    if (in.readBoolean()) {
      this.doubleVal = in.readDouble();
      System.out.println(" Applied delta to field 'doubleVal' = "
            + this.doubleVal);
    }
  }

  public void setIntVal(int anIntVal) {
    this.intFldChd = true;
    this.intVal = anIntVal;
  }
  
  public void setDoubleVal(double aDoubleVal) {
    this.dblFldChd = true;
    this.doubleVal = aDoubleVal;
  }

  @Override
  public String toString() {
    return "SimpleDelta [ hasDelta = " + hasDelta() + ", intVal = " + this.intVal
        + ", doubleVal = {" + this.doubleVal + "} ]";
  }
}
