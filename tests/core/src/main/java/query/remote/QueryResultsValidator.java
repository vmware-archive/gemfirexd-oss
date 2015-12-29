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
package query.remote;


/**
 * This interface should be implemented if the developer needs to validate
 * query result dynamically.
 * A default implementation of this class will be created , if the predicatble
 * results for queries is passed via conf file.
 * 
 * @author ashahid
 *
 */
public interface QueryResultsValidator
{
  /**
   * Function to be implemented, which will be invoked after
   * every query execution. The results should be authenticated
   * in it
   * @param resultSet  Query results
   * @param queryIndex The position of the query as passed in the conf file
   * @param queryString Query String
   * 
   * @return boolean true if results were as expected or false
   */
   public boolean validateQueryResults(Object resultSet, int queryIndex, String queryString);

}
