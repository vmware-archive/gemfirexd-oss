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
//
//  DistinctAndNonDistinctQueryJUnitTest.java
//
package com.gemstone.gemfire.cache.query.functional;

import java.util.*;
import junit.framework.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.query.*;

public class DistinctAndNonDistinctQueryJUnitTest extends TestCase {

  static List data = Arrays.asList(new String[] {
    "abcd", "bcdd", "cde", "de", "abcd", "bcdd", "cde", "de" });

  public DistinctAndNonDistinctQueryJUnitTest(String name) {
    super(name);
  }
  
  public void testDistinct() throws Exception {
    String queryString = "select distinct * from $1";
    Query q = CacheUtils.getQueryService().newQuery(queryString);
    SelectResults results = (SelectResults)q.execute(new Object[] { data });
    assertEquals(4, results.size());
    for (Iterator itr = data.iterator(); itr.hasNext(); ) {
      Object element = itr.next();
      assertTrue(results.contains(element));
      assertEquals(1, results.occurrences(element));
    }
    CacheUtils.closeCache();
  }
    
  public void testNonDistinct() throws Exception {
    String queryString = "select * from $1";
    Query q = CacheUtils.getQueryService().newQuery(queryString);
    SelectResults results = (SelectResults)q.execute(new Object[] { data });
    assertEquals(8, results.size());
    for (Iterator itr = data.iterator(); itr.hasNext(); ) {
      Object element = itr.next();
      assertTrue(results.contains(element));
      assertEquals(2, results.occurrences(element));
    }

    queryString = "select ALL * from $1";
    q = CacheUtils.getQueryService().newQuery(queryString);
    results = (SelectResults)q.execute(new Object[] { data });
    assertEquals(8, results.size());
    for (Iterator itr = data.iterator(); itr.hasNext(); ) {
      Object element = itr.next();
      assertTrue(results.contains(element));
      assertEquals(2, results.occurrences(element));
    }
    CacheUtils.closeCache();
  }
  
  public void testDistinctNonDistinctWithIndexes() throws Exception {
    CacheUtils.startCache();
    Region rgn = CacheUtils.createRegion("testDistinctNonDistinctWithIndexes",
                                    String.class, Scope.LOCAL);
    QueryService qs = CacheUtils.getQueryService();
    qs.createIndex("length", IndexType.FUNCTIONAL, "length", "/testDistinctNonDistinctWithIndexes");
    
    List filtered = new ArrayList();
    int i = 0;
    for (Iterator itr = data.iterator(); itr.hasNext(); ) {
      String s = (String)itr.next();
      if (s.length() <= 3) {
        rgn.put(new Integer(i++), s);
        filtered.add(s);
      }
    }
    
    String queryString = "select distinct * from /testDistinctNonDistinctWithIndexes s "
      + " where 3 >= s.length";
    Query q = CacheUtils.getQueryService().newQuery(queryString);
    SelectResults results = (SelectResults)q.execute();
    assertEquals(2, results.size());
    for (Iterator itr = filtered.iterator(); itr.hasNext(); ) {
      Object element = itr.next();
      assertTrue(results.contains(element));
      assertEquals(1, results.occurrences(element));
    }
    
    queryString = "select distinct * from /testDistinctNonDistinctWithIndexes "
      + "where 3 >= length";
    q = CacheUtils.getQueryService().newQuery(queryString);
    results = (SelectResults)q.execute();
    assertEquals(2, results.size());
    for (Iterator itr = filtered.iterator(); itr.hasNext(); ) {
      Object element = itr.next();
      assertTrue(results.contains(element));
      assertEquals(1, results.occurrences(element));
    }
    
    queryString = "select * from /testDistinctNonDistinctWithIndexes "
      + "where 3 >= length";
    q = CacheUtils.getQueryService().newQuery(queryString);
    results = (SelectResults)q.execute(new Object[] { data });
    assertEquals(4, results.size());
    for (Iterator itr = filtered.iterator(); itr.hasNext(); ) {
      Object element = itr.next();
      assertTrue(results.contains(element));
      assertEquals(2, results.occurrences(element));
    }
    
    queryString = "select ALL * from /testDistinctNonDistinctWithIndexes "
      + "where 3 >= length";
    q = CacheUtils.getQueryService().newQuery(queryString);
    results = (SelectResults)q.execute(new Object[] { data });
    assertEquals(4, results.size());
    for (Iterator itr = filtered.iterator(); itr.hasNext(); ) {
      Object element = itr.next();
      assertTrue(results.contains(element));
      assertEquals(2, results.occurrences(element));
    }
    
    CacheUtils.closeCache();
  } 
}
