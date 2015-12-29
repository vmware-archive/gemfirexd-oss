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

package com.pivotal.gemfirexd.internal.engine;

import com.pivotal.gemfirexd.internal.engine.sql.execute.UpdatableResultSet;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ColumnDescriptorList;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.TableDescriptor;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;

/**
 * This extends {@link GfxdVTITemplate} for VTIs that need to be updatable.
 * 
 * @author swale
 */
public abstract class UpdateVTITemplate extends GfxdVTITemplate implements
    UpdatableResultSet {

  /**
   * Set the {@link ColumnDescriptorList} for the given {@link TableDescriptor}.
   */
  public abstract void setColumnDescriptorList(TableDescriptor td);

  /**
   * Set the updated row for current cursor. This must have a valid
   * implementation for columns in rows that are updatable.
   * 
   * @param row
   *          the new updated row
   * 
   * @throws StandardException
   *           on an error
   */
  @Override
  public abstract void updateRow(ExecRow row) throws StandardException;

  @Override
  public void deleteRowDirectly() throws StandardException {
    throw new UnsupportedOperationException(
        "This method should not have been invoked");
  }
  
  /**
   * @see UpdatableResultSet#isForUpdate()
   */
  @Override
  public boolean isForUpdate() {
    return true;
  }

  /**
   * @see UpdatableResultSet#canUpdateInPlace()
   */
  @Override
  public boolean canUpdateInPlace() {
    return true;
  }

  /**
   * Set the activation being used for this instance.
   */
  public abstract void setActivation(Activation act);
}
