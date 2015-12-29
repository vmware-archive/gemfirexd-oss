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
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.util.Sizeof;
import com.gemstone.gemfire.cache.util.ObjectSizer;
import hydra.*;
import java.io.*;

/**
 * An object containing an index, a timestamp, a batch number such that the
 * first {@link BatchStringPrms#batchSize} instances are in batch 0, the second
 * are in batch 1, etc., and a byte array of size {@link BatchObjectPrms#size}.
 */
public class BatchObject implements ConfigurableObject, TimestampedObject,
ObjectSizer, DataSerializable
{
  private int index;
  private long timestamp;
  private int batch;
  private byte[] byteArray;

  // INSTANTIATORS DISABLED due to bug 35646
  //
  //static {
  //                                TBD: WHAT SHOULD 23 BE REPLACED WITH???
  //  Instantiator.register(new Instantiator(BatchObject.class, (byte)23) {
  //    public DataSerializable newInstance() {
  //      return new BatchObject();
  //    }
  //  });
  //}

  public BatchObject() {
  }

  /**
   * Initializes a BatchObject.
   * @param index the value to encode into the BatchObject.
   *
   * @throws ObjectCreationException
   *         <code>index</code> cannot be encoded
   */
  public void init(int anIndex) {
    this.index = anIndex;
    this.timestamp = NanoTimer.getTime() - RemoteTestModule.getClockSkew();
    this.batch = anIndex/BatchStringPrms.getBatchSize();
    this.byteArray = new byte[BatchObjectPrms.getSize()];
  }

  /**
   * Returns the index.
   */
  public int getIndex() {
    return this.index;
  }

  /**
   * Returns the batch.
   */
  public int getBatch() {
    return this.batch;
  }

  /**
   * Validates that the given index is encoded in the BatchObject.
   *
   * @param anIndex the expected encoded value 
   *
   * @throws ObjectValidationException
   *         If <code>index</code> is not the same as <code>index</code>
   */
  public void validate(int anIndex) {
    int encodedIndex = this.getIndex();
    if (encodedIndex != anIndex) {
      String s = "Expected index " + anIndex + ", got " + encodedIndex;
      throw new ObjectValidationException(s);
    }
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  public void resetTimestamp() {
    this.timestamp = NanoTimer.getTime() - RemoteTestModule.getClockSkew();
  }

  public String toString() {
    if (this.byteArray == null) {
      return "BatchObject@" + this.timestamp;
    } else {
      return "BatchObject(" + this.getIndex() + ", " + this.getBatch() + ")@"
                            + this.timestamp;
    }
  }

  /**
   * Two <code>BatchObject</code>s are considered to be equal if they have
   * the same values for index, timestamp, and batch.
   * This provides stronger validation than the {@link #validate}
   * method that only considers the index.
   *
   */
  public boolean equals(Object o) {
    if (o instanceof BatchObject) {
      BatchObject other = (BatchObject) o;
      if (this.index == other.index) {
        if (this.timestamp == other.timestamp) {
          if (this.batch == other.batch) {
	    return true;
	  }
	}
      }
    }

    return false;
  }

  // ObjectSizer
  public int sizeof(Object o) {
    if (o instanceof BatchObject) {
      BatchObject obj = (BatchObject)o;
      return Sizeof.sizeof(obj.index)
           + Sizeof.sizeof(obj.timestamp)
           + Sizeof.sizeof(obj.batch)
           + Sizeof.sizeof(obj.byteArray);
    } else {
      return Sizeof.sizeof(o);
    }
  }

  // DataSerializable
  public void toData(DataOutput out)
  throws IOException {
    out.writeInt(this.index);
    out.writeLong(this.timestamp);
    out.writeInt(this.batch);
    DataSerializer.writeByteArray(this.byteArray, out);
  }
  public void fromData(DataInput in)
  throws IOException, ClassNotFoundException {
    this.index = in.readInt();
    this.timestamp = in.readLong();
    this.batch = in.readInt();
    this.byteArray = DataSerializer.readByteArray(in);
  }
}
