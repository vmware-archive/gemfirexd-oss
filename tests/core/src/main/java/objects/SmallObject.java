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
import com.gemstone.gemfire.Instantiator;
import hydra.*;
import java.io.*;

/**
 * An object containing an int and a String.
 */
public class SmallObject implements ConfigurableObject, DataSerializable
{
  protected int id;
  protected String name;

  static {
    Instantiator.register(new Instantiator(SmallObject.class, (byte)23) {
      public DataSerializable newInstance() {
        return new SmallObject();
      }
    });
  }

  public SmallObject() {
  }

  /**
   * Initializes a SmallObject.
   * @param index the value to encode into the SmallObject.
   *
   * @throws ObjectCreationException
   *         <code>index</code> cannot be encoded
   */
  public void init( int index ) {
    this.id = index;
    this.name = "joe";
  }

  /**
   * Returns the index.
   * @throws ObjectAccessException
   */
  public int getIndex() {
    return this.id;
  }

  /**
   * Validates that the given index coincides with the SmallObject.
   * @param index the expected value 
   *
   * @throws ObjectValidationException
   *         If <code>index</code> does not match.
   */
  public void validate( int index ) {
    int actualIndex = this.getIndex();
    if ( actualIndex != index ) {
      throw new ObjectValidationException( "Expected index " + index + ", got " + actualIndex );
    }
  }

  public String toString() {
    return String.valueOf(this.id);
  }

  /**
   * Two <code>SmallObject</code>s are considered to be equal if they have
   * the same id.
   */
  public boolean equals(Object o) {
    if (o instanceof SmallObject) {
      SmallObject other = (SmallObject) o;
      if (this.id == other.id) {
	return true;
      }
    }
    return false;
  }

  // DataSerializable
  public void toData( DataOutput out )
  throws IOException {
    out.writeInt(this.id);
    DataSerializer.writeString(this.name, out);
  }
  public void fromData( DataInput in )
  throws IOException, ClassNotFoundException {
    this.id = in.readInt();
    this.name = DataSerializer.readString(in);
  }
}
