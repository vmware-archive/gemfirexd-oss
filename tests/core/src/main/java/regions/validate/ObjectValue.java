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
package regions.validate;

/**
 * This <code>Value</code> indicates that a region entry has a valid
 * value. 
 *
 * @author David Whitlock
 * @since 3.5
 */
public class ObjectValue extends Value {

  /** The value object associated with the region entry */
  private Object value;

  //////////////////////  Constructors  //////////////////////

  /**
   * Creates a new <code>ObjectValue</code> for the given value of a
   * region entry.
   *
   * @throws IllegalArgumentException
   *         If <code>value</code> is <code>null</code>
   */
  public ObjectValue(Object value) {
    if (value == null) {
      String s = "An ObjectValue cannot be created for a null value";
      throw new IllegalArgumentException(s);
    }

    this.value = value;
  }

  ////////////////////  Instance Methods  ////////////////////

  /**
   * Returns the value of the region entry associated with this
   * <code>ObjectValue</code>.
   */
  public Object getValue() {
    return this.value;
  }

  /**
   * Two <code>ObjectValue</code>s are the same if they have the same
   * {@linkplain #getValue value}.
   */
  public boolean equals(Object o) {
    if (o instanceof ObjectValue) {
      ObjectValue other = (ObjectValue) o;
      return this.value.equals(other.getValue());

    } else {
      return false;
    }
  }

  public String toString() {
    return "ObjectValue with value " + this.getValue() + " at " +
      this.formatTimestamp();
  }

}
