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

package com.pivotal.gemfirexd.internal.engine.sql.execute;

import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;

/**
 * Interface for ResultSets that are updatable.
 * 
 * @author swale
 * @since 7.0
 */
public interface UpdatableResultSet {

  /**
   * Get the Activation associated with this ResultSet.
   */
  public Activation getActivation();

  /**
   * Returns true if this ResultSet or it's source result set can be updated.
   */
  public boolean isForUpdate();

  /**
   * Return true if an updatable result set can be updated in place using
   * {@link #updateRow(ExecRow)} instead of creating a new SQL for update.
   */
  public boolean canUpdateInPlace();

  /**
   * Updates the resultSet's current row with it's new values after an update
   * has been issued either using positioned update or JDBC's udpateRow method.
   * 
   * @param row
   *          new values for the currentRow
   * 
   * @exception StandardException
   *              thrown on failure.
   */
  public void updateRow(ExecRow row) throws StandardException;
  
  public void deleteRowDirectly() throws StandardException;
}
