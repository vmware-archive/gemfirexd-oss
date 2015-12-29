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
package objects.query;

public abstract class BaseQueryFactory {

  protected boolean logIndexes;
  protected boolean logQueries;
  protected boolean logUpdates;

  protected boolean logQueryResults;
  protected boolean logQueryResultSize;

  protected boolean logWarnings;

  protected boolean validateResults;

  /**
   * Initializes the factory.
   */
  public void init() {
    logIndexes = QueryPrms.logIndexes();
    logQueries = QueryPrms.logQueries();
    logUpdates = QueryPrms.logUpdates();

    logQueryResults = QueryPrms.logQueryResults();
    logQueryResultSize = QueryPrms.logQueryResultSize();

    logWarnings = QueryPrms.logWarnings();

    validateResults = QueryPrms.validateResults();
  }

  public boolean getLogQueries() {
    return logQueries;
  }

  public void setLogQueries(boolean b) {
    logQueries = b;
  }
}
