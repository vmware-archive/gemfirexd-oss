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

import com.gemstone.gemfire.cache.Region;

import java.util.List;
import java.util.Map;

public interface GFEQueryFactory extends QueryFactory {
  
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
   * Get's the object from the Region
   * Used for direct get performance test
   */
  public Object directGet(Object key, Region region);
  
  public Region getRegionForQuery(int queryType);
  
  /**
   * updates the object into the correct region
   * Used for direct update performance test
   */
  public void directUpdate(int i, int queryType);
  
  public void readResultSet(int queryType, Object rs);
  
}
