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
package com.pivotal.gemfirexd.internal.iapi.types;

import com.pivotal.gemfirexd.internal.engine.store.CompactCompositeKey;
import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;

/**
 * This interface enables ExecRow binary comparison with a BinarySQLHybridType
 * without de-serializing the column.
 * 
 * @author soubhikc, swale
 * @since 7.0
 */
public interface BinaryComparable {

  public boolean compare(int op, ExecRow row, boolean byteArrayStore,
      int logicalPosition, boolean orderedNulls, boolean unknownRV)
      throws StandardException;

  public boolean compare(int op, CompactCompositeKey key, int logicalPosition,
      boolean orderedNulls, boolean unknownRV) throws StandardException;

  /**
   * Returns true if this type can compare (or is advantageous) serialized
   * byte[] to byte[] compare in which case a BinarySQLHybridType will be
   * created for incoming DVD.
   */
  public boolean canCompareBytesToBytes();

  /**
   * Compare this value against column embedded in serialized row passed as
   * bytes. A negative return value indicates that comparison has failed else a
   * positive one gives the new position in the byte[] when the passed one is
   * key bytes (isKeyBytes is true).
   */
  public int equals(RowFormatter rf, byte[] bytes, boolean isKeyBytes,
      int logicalPosition, int keyBytesPos, final DataValueDescriptor[] outDVD)
      throws StandardException;
}
