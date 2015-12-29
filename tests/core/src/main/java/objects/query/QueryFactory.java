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

/**
 * Interface for objects that support queries.
 */
public interface QueryFactory {

  /**
   * (Re)initializes the singleton factory.
   */
  public void init();
  
  /**
   * Returns the number of the query configured using the parameter class for
   * the factory.
   *
   * @throws QueryFactoryException if the query type is not configured.
   */
  public int getQueryType();
  
  /**
   * Returns the number of the update query configured using the parameter class for
   * the factory.
   *
   * @throws QueryFactoryException if the update query type is not configured.
   */
  public int getUpdateQueryType();
  
  /**
   * Returns the number of the delete query configured using the parameter class for
   * the factory.
   *
   * @throws QueryFactoryException if the delete query type is not configured.
   */
  public int getDeleteQueryType();

  /**
   * Returns a query string for the given query type, parameterized by the
   * given integer value.
   *
   * @throws QueryFactoryException if the queryType is not found.
   */
  public String getQuery(int queryType, int i);
  
  /**
   * Returns a query string for a prepared statement for the given query type, parameterized by the
   * given integer value.
   *
   * @throws QueryFactoryException if the queryType is not found.
   */
  public String getPreparedQuery(int queryType);
  
  
  
  
}
