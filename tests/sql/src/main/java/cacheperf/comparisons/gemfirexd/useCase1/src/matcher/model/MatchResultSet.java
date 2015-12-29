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
package cacheperf.comparisons.gemfirexd.useCase1.src.matcher.model;

import java.io.Serializable;
import java.sql.ResultSet;

public class MatchResultSet implements Serializable {

  private ResultSet resultSet;
  private boolean isErrorState;

  public MatchResultSet() {
    this.resultSet = null;
    this.isErrorState = false;
  }

  public MatchResultSet(ResultSet resultSet, boolean isErrorState) {
    this.resultSet = resultSet;
    this.isErrorState = isErrorState;
  }

  public ResultSet getResultSet() {
    return resultSet;
  }
  public void setResultSet(ResultSet resultSet) {
    this.resultSet = resultSet;
  }
  public boolean isErrorState() {
    return isErrorState;
  }

  public void setErrorState(boolean isErrorState) {
    this.isErrorState = isErrorState;
  }

  @Override
  public String toString() {
  return "MatchResultSet isErrorState="+this.isErrorState+" resultSet"+this.resultSet;
  }
}
