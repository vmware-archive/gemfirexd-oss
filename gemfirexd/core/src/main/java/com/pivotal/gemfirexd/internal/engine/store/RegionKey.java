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
package com.pivotal.gemfirexd.internal.engine.store;

import java.io.Serializable;

import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.cache.KeyWithRegionContext;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;

/**
 * An interface that represents a Gemfire region key. This interface extends
 * {@link DataSerializableFixedID} so that region keys or individual
 * <code>DataValueDescriptor</code> are always data serializable.
 * 
 * @author rdubey
 * 
 */
public interface RegionKey extends KeyWithRegionContext,
    DataSerializableFixedID, Serializable {

  /**
   * The number of columns in this key.
   */
  public int nCols();

  /**
   * Returns the <code>DataValueDescriptor</code> representing the region key at
   * given 0-based index.
   * 
   * @return a <code>DataValueDescriptor</code>.
   */
  public DataValueDescriptor getKeyColumn(int index);

  /**
   * Fill in the <code>DataValueDescriptor</code> array representing the region.
   */
  public void getKeyColumns(DataValueDescriptor[] keys);

  /**
   * Fill in the Object array representing the region key.
   */
  public void getKeyColumns(Object[] keys) throws StandardException;
}
