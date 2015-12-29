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

package com.gemstone.gemfire.cache;

import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.util.ArrayUtils;

/** Thrown when attempting to create a <code>Region.Entry</code> that already
 * exists in the <code>Region</code>.
 * @author Eric Zoerner
 *
 * @see com.gemstone.gemfire.cache.Region#create(Object, Object)
 * @see Region.Entry
 * @since 3.0
 */
public class EntryExistsException extends CacheException {

  private static final long serialVersionUID = 2925082493103537925L;

  /**
   * The old value for the key of current operation.
   * 
   * Transient to avoid default serialization from kicking in.
   */
  private transient Object oldValue;

  /**
   * Constructs an instance of <code>EntryExistsException</code> with the
   * specified detail message.
   * 
   * @param msg
   *          the detail message
   * @since 6.5
   */
  public EntryExistsException(String msg, Object oldValue) {
    super(oldValue == null ? msg : (msg + ", with oldValue: " + ArrayUtils
        .objectStringNonRecursive(oldValue)));
    this.oldValue = oldValue;
  }

  /**
   * Returns the old existing value that caused this exception.
   */
  public final Object getOldValue() {
    return this.oldValue;
  }

  /**
   * Sets the old existing value that caused this exception.
   */
  public final void setOldValue(Object oldValue) {
    this.oldValue = oldValue;
  }

  // Overrides for Serializable to serialize oldValue using DataSerializer

  private synchronized void writeObject(final java.io.ObjectOutputStream out)
      throws IOException {
    getStackTrace(); // Ensure that stackTrace field is initialized.
    out.defaultWriteObject();
    // now write the oldValue
    // if it is a RegionEntry then cannot serialize it
    if (this.oldValue instanceof RegionEntry) {
      DataSerializer.writeObject(
          ArrayUtils.objectStringNonRecursive(this.oldValue), out);
    }
    else {
      DataSerializer.writeObject(this.oldValue, out);
    }
  }

  private void readObject(final java.io.ObjectInputStream in)
      throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    // now read the oldValue
    this.oldValue = DataSerializer.readObject(in);
  }
}
