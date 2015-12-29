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

package objects;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.Delta;
//import com.gemstone.gemfire.Instantiator;
import com.gemstone.gemfire.InvalidDeltaException;
import com.gemstone.gemfire.cache.util.ObjectSizer;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A {@link objects.PSTObject} that implements the Delta interface.
 */
public class DeltaPSTObject extends PSTObject implements ConfigurableObject, TimestampedObject, ObjectSizer, DataSerializable, Delta, Cloneable
{

  // INSTANTIATORS DISABLED due to bug 35646
  //
  //static {
  //  Instantiator.register(new Instantiator(DeltaPSTObject.class, (byte)42) {
  //    public DataSerializable newInstance() {
  //      return new DeltaPSTObject();
  //    }
  //  });
  //}

  public DeltaPSTObject() {
  }

//------------------------------------------------------------------------------
// Delta

  public boolean hasDelta() {
    return true;
  }

  public void toDelta(DataOutput out)
  throws IOException {
    out.writeInt(this.field1);
    out.writeLong(this.timestamp);
  }

  public void fromDelta(DataInput in)
  throws IOException {
    this.field1 = in.readInt();
    this.timestamp = in.readLong();
  }

  /**
   * Two <code>DeltaPSTObject</code>s are considered to be equal if they have
   * the same values for field1, field2 and timestamp.
   * This provides stronger validation than the {@link #validate}
   * method that only considers the index.
   */
  public boolean equals(Object o) {
    if (o instanceof DeltaPSTObject) {
      DeltaPSTObject other = (DeltaPSTObject) o;
      if (this.timestamp == other.timestamp) {
	if ((this.field1 == other.field1) &&
	    (this.field2 == other.field2) ) {
	  return true;
	}
      }
    }

    return false;
  }

//------------------------------------------------------------------------------
// Cloneable

  /**
   * Makes a deep copy of this object.
   */
  public Object clone() throws CloneNotSupportedException {
    DeltaPSTObject obj = (DeltaPSTObject)super.clone();
    this.byteArray = new byte[this.byteArray.length];
    System.arraycopy(this.byteArray, 0, obj.byteArray, 0,
                     this.byteArray.length);
    return obj;
  }
}
