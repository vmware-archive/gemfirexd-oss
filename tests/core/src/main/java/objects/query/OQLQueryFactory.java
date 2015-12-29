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

import com.gemstone.gemfire.cache.query.FunctionDomainException;
import com.gemstone.gemfire.cache.query.IndexExistsException;
import com.gemstone.gemfire.cache.query.IndexNameConflictException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.cache.query.TypeMismatchException;

import java.util.List;
import java.util.Map;

/**
 * Interface for objects that support OQL queries.
 */
public interface OQLQueryFactory extends QueryFactory {

  /**
   * Creates the regions used by the query object type.
   */
  public void createRegions();

  public List getPreparedInsertObjects();

  /**
   * inserts objects into region
   * @param pobjs A list of objects for insertion.  The implementing class needs to know what to do with this list
   * @param i the index of the object inserted
   * @return a map of all objects inserted into region for relation purposes
   * @throws QueryObjectException
   */
  public Map fillAndExecutePreparedInsertObjects(List pobjs, int i)
      throws QueryObjectException;

  /**
   * creates a Query object from a given OQL query string
   * @param stmt
   */
//  public Query prepareOQLStatement(String stmt);

  /**
   * given the prepared query object, will determine how best to fill parameters, based on the queryType
   * @param query
   * @param queryType
   * @param id
   * @return
   */
  public Object fillAndExecutePreparedQueryStatement(Query query,
      int queryType, int id) throws NameResolutionException,
      TypeMismatchException, FunctionDomainException,
      QueryInvocationTargetException;

  /**
   * Reads the OQL results from the result object
   * @param resultSet
   */
  public void readResultSet(int queryType, Object resultSet);

  public void createIndexes() throws NameResolutionException,RegionNotFoundException,
      IndexExistsException, IndexNameConflictException;
  
}
