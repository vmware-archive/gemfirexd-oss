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

import hydra.Log;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import objects.Portfolio;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.Delta;
import com.gemstone.gemfire.InvalidDeltaException;
/**
 * To tests delta propagation in concurrent enviroment
 * 
 * @author aingle
 * @since 6.1
 */
public class DeltaPortfolio extends Portfolio implements Delta {

  /* constructor */
  public DeltaPortfolio() {
    super();
  }

  public DeltaPortfolio(int i) {
    super(i);
  }

  public void fromDelta(DataInput in) throws IOException, InvalidDeltaException {
    try {
      this.ID = DataSerializer.readPrimitiveInt(in);
      this.status = DataSerializer.readString(in);
      this.type = DataSerializer.readString(in);
      Log.getLogWriter().info("DeltaPortfolio.fromDelta() called " + toString());
    }
    catch (IOException ioe) {
      throw ioe;
    }
    catch (IllegalArgumentException iae) {
      throw new InvalidDeltaException(iae);
    }
  }

  public boolean hasDelta() {
    return true;
  }

  public void toDelta(DataOutput out) throws IOException {
    try {
      DataSerializer.writePrimitiveInt(this.ID, out);
      DataSerializer.writeString(this.status, out);
      DataSerializer.writeString(this.type, out);
      Log.getLogWriter().info("DeltaPortfolio.toDelta() called " + toString());
    }
    catch (IOException ioe) {
      throw ioe;
    }
    catch (IllegalArgumentException iae) {
      throw new InvalidDeltaException(iae);
    }
  }

  public String toString() {
    String out = "DeltaPortfolio [ID=" + super.ID + " status=" + super.status
        + " type=" + super.type + "\n ";
    return out + "\n]";
  }
}
