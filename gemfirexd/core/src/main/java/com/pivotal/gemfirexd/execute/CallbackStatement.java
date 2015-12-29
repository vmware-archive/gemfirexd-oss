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

package com.pivotal.gemfirexd.execute;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * An extension to {@link Statement} when passed to {@link QueryObserver} and
 * others to allow overriding results sent back by the statement execution.
 * 
 * @author swale
 */
public interface CallbackStatement extends Statement {

  /**
   * Override the {@link ResultSet} for this statement execution with the given
   * {@link ResultSet}.
   * 
   * Only one of {@link #setResultSet(ResultSet)} or
   * {@link #setUpdateCount(int)} can be invoked by a {@link QueryObserver}.
   * 
   * @param rs
   *          the new {@link ResultSet} that will be returned by this statement
   */
  public void setResultSet(ResultSet rs);

  /**
   * Override the update count for this statement execution with given value.
   * 
   * Only one of {@link #setResultSet(ResultSet)} or
   * {@link #setUpdateCount(int)} can be invoked by a {@link QueryObserver}.
   * 
   * @param count
   *          the new update count that will be returned by this statement
   */
  public void setUpdateCount(int count);

  /**
   * Returns the query/DML string of this statement.
   */
  public String getSQLText();

  /**
   * Returns true if this is a {@link PreparedStatement} or
   * {@link CallableStatement}, and false otherwise.
   */
  public boolean isPrepared();
}
