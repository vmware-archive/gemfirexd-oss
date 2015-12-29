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
//import com.gemstone.gemfire.Instantiator;
import com.gemstone.gemfire.internal.NanoTimer;
import com.gemstone.gemfire.internal.util.Sizeof;
import com.gemstone.gemfire.cache.util.ObjectSizer;
import hydra.*;
import java.io.*;

/**
 * An object containing a timestamp, a byte array of configurable size and primitive fields.
 * 
 * @author Jean Farris
 * @since 5.0
 */

public class PSTObject implements ConfigurableObject, TimestampedObject, UpdatableObject, ObjectSizer, DataSerializable
{
  protected long timestamp;
  protected int field1;
  protected char field2;
  protected byte[] byteArray;

  // INSTANTIATORS DISABLED due to bug 35646
  //
  //static {
  //  Instantiator.register(new Instantiator(PSTObject.class, (byte)23) {
  //    public DataSerializable newInstance() {
  //      return new PSTObject();
  //    }
  //  });
  //}

  public PSTObject() {
  }

  /**
   * Initializes a PSTObject.
   * @param index the value to encode into the PSTObject.
   *
   * @throws ObjectCreationException
   *         <code>index</code> cannot be encoded
   */
  public void init( int index ) {
    int size = PSTObjectPrms.getSize();
    if ( size == 0 ) {
      this.byteArray = null;
    } else {
      boolean encodeKey = true;
      this.byteArray = ArrayOfByte.init(index, size, encodeKey, false);
    }
    this.timestamp = NanoTimer.getTime() - RemoteTestModule.getClockSkew();
    GsRandom random = TestConfig.tab().getRandGen();
    this.field1 = random.nextInt();
    this.field2 = random.nextChar();
  }

  /**
   * Returns the index encoded in the PSTObject's byte array.
   *
   * @throws ObjectAccessException
   */
  public int getIndex() {
    if ( this.byteArray == null ) {
      throw new ObjectAccessException( "No index is encoded when " + BasePrms.nameForKey( PSTObjectPrms.size ) + " is 0" );
    } else {
       try {
	   return ( ArrayOfByte.getIndex (byteArray) );
      } catch( ObjectAccessException e ) {
        throw new ObjectAccessException( "PSTObject.byteArray  does not contain an encoded integer index" );
      }
    }
  }

  /**
   * Validates that the given index is encoded in the PSTObject's
   * byte array.
   *
   * @param index the expected encoded value 
   *
   * @throws ObjectValidationException
   *         If <code>index</code> is not encoded in <code>bytes</code>
   */
  public void validate( int index ) {
    if ( this.byteArray == null ) {
      Log.getLogWriter().info( "Cannot validate encoded index of object " + index + ", it has no byteArray" );
    } else {
      int encodedIndex = this.getIndex();
      if ( encodedIndex != index ) {
        throw new ObjectValidationException( "Expected index " + index + ", got " + encodedIndex );
      }
    }
  }

  public void incrementField1() {
    ++this.field1;
  }

  public synchronized void update() {
    incrementField1();
    resetTimestamp();
  }

  public long getTimestamp() {
    return this.timestamp;
  }

  public void resetTimestamp() {
    this.timestamp = NanoTimer.getTime() - RemoteTestModule.getClockSkew();
  }

  public String toString() {
    if ( this.byteArray == null ) {
      return this.getClass().getName() + "@" + this.timestamp;
    } else {
      return this.getClass().getName() + "(" + this.getIndex() + ")@" + this.timestamp;
    }
  }

  /**
   * Two <code>PSTObject</code>s are considered to be equal if they have
   * the same values for field1, field2 and timestamp.
   * This provides stronger validation than the {@link #validate}
   * method that only considers the index.
   */
  public boolean equals(Object o) {
    if (o instanceof PSTObject) {
      PSTObject other = (PSTObject) o;
      if (this.timestamp == other.timestamp) {
	if ((this.field1 == other.field1) &&
	    (this.field2 == other.field2) ) {
	  return true;
	}
      }
    }

    return false;
  }

  // ObjectSizer
  public int sizeof(Object o) {
    if (o instanceof PSTObject) {
      PSTObject obj = (PSTObject)o;
      return Sizeof.sizeof(obj.timestamp)
                   + Sizeof.sizeof(obj.field1)
                   + Sizeof.sizeof(obj.field2)
                   + Sizeof.sizeof(obj.byteArray);
    } else {
      return Sizeof.sizeof(o);
    }
  }
  public int hashCode() {
    int result = 17;
    result = 37 * result + (int)timestamp;
    result = 37 * result + field1;
    result = 37 * result + field2;
    return result;
  }
  // DataSerializable
  public void toData( DataOutput out )
  throws IOException {
    out.writeLong( this.timestamp );
    out.writeInt( this.field1 );
    out.writeChar( this.field2 );
    DataSerializer.writeByteArray(this.byteArray, out);
  }
  public void fromData( DataInput in )
  throws IOException, ClassNotFoundException {
    this.timestamp = in.readLong();
    this.field1 = in.readInt();
    this.field2 = in.readChar();
    this.byteArray = DataSerializer.readByteArray( in );
  }
}
