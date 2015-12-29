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

package com.gemstone.gemfire.internal.cache;

import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;

/**
 * Interface for index key objects persisted to oplogs which should be able to
 * write its data as a byte[].
 * 
 * @author swale
 * @since gfxd 1.0
 */
public interface SortedIndexKey {

  /**
   * Write the data as a byte[] to the given output stream that can be read by
   * an invocation of {@link DataSerializer#readByteArray}.
   */
  public void writeKeyBytes(DataOutput out) throws IOException;

  /** Set a transient value field of the key. */
  public void setTransientValue(Object value);

  /**
   * Get the transient value field of key set previously by
   * {@link #setTransientValue(Object)}.
   */
  public Object getTransientValue();

  public byte[] snapshotKeyFromValue();
}
