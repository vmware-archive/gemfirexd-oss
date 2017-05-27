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
 * ReservedKeywordsTest.java
 * JUnit based test
 *
 * Created on March 10, 2005, 7:14 PM
 */
package com.gemstone.gemfire.cache.query.functional;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.data.Keywords;
//import com.gemstone.gemfire.cache.query.data.Portfolio;
import java.util.Collection;
import junit.framework.*;

/**
 * @author vaibhav
 */
public class ReservedKeywordsTest extends TestCase {

  public ReservedKeywordsTest(String testName) {
    super(testName);
  }

  protected void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
  }

  protected void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  public void testReservedKeywords() throws Exception {
    String keywords[] = { "select", "distinct", "from", "where", "TRUE",
        "FALSE", "undefined", "element", "not", "and", "or", "type"};
    Region region = CacheUtils.createRegion("Keywords", Keywords.class);
    region.put("0", new Keywords());
    Query query;
    Collection result;
    for (int i = 0; i < keywords.length; i++) {
      String qStr = "SELECT DISTINCT * FROM /Keywords where \"" + keywords[i]
          + "\"";
      System.out.println(qStr);
      query = CacheUtils.getQueryService().newQuery(qStr);
      result = (Collection) query.execute();
      if (result.size() != 1) fail(query.getQueryString());
    }
    for (int i = 0; i < keywords.length; i++) {
      String qStr = "SELECT DISTINCT * FROM /Keywords where \""
          + keywords[i].toUpperCase() + "\"()";
      System.out.println(qStr);
      query = CacheUtils.getQueryService().newQuery(qStr);
      result = (Collection) query.execute();
      if (result.size() != 1) fail(query.getQueryString());
    }
  }
}
