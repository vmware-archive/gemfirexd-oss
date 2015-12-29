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
import java.util.List;

public class Query implements Serializable {

  protected String queryString;
  protected List<Object> bindVariableValueList;

  public Query() {
    super();
  }

  public Query(String queryString, List<Object> bindVariableValueList) {
    super();
    this.queryString = queryString;
    this.bindVariableValueList = bindVariableValueList;
  }

  public String getQueryString() {
    return queryString;
  }

  public void setQueryString(String queryString) {
    this.queryString = queryString;
  }

  public List<Object> getBindVariableValueList() {
    return bindVariableValueList;
  }

  public void setBindVariableValueList(List<Object> bindVariableValueList) {
    this.bindVariableValueList = bindVariableValueList;
  }

  @Override
  public String toString() {
    return "queryString="+queryString +" bindVariableValueList="+bindVariableValueList;
  }
}
