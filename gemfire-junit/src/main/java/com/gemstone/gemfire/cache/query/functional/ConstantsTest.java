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
 * ConstantsTest.java
 * JUnit based test
 *
 * Created on March 10, 2005, 6:26 PM
 */

package com.gemstone.gemfire.cache.query.functional;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import java.util.Collection;
import junit.framework.*;

/**
 *
 * @author vaibhav
 */
public class ConstantsTest extends TestCase {
  
  public ConstantsTest(String testName) {
    super(testName);
  }
  
  protected void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
    Region region = CacheUtils.createRegion("Portfolios", Portfolio.class);
    region.put("0",new Portfolio(0));
    region.put("1",new Portfolio(1));
    region.put("2",new Portfolio(2));
    region.put("3",new Portfolio(3));
  }
  
  protected void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }
  
  public void testTRUE() throws Exception{
    Query query = CacheUtils.getQueryService().newQuery("SELECT DISTINCT * FROM /Portfolios where TRUE");
    Object result = query.execute();
    if(!(result instanceof Collection) || ((Collection)result).size() != 4)
      fail(query.getQueryString());
  }
  
  public void testFALSE() throws Exception{
    Query query = CacheUtils.getQueryService().newQuery("SELECT DISTINCT * FROM /Portfolios where FALSE");
    Object result = query.execute();
    if(!(result instanceof Collection) || ((Collection)result).size() != 0)
      fail(query.getQueryString());
  }
  
  public void testUNDEFINED() throws Exception{
    Query query = CacheUtils.getQueryService().newQuery("SELECT DISTINCT * FROM /Portfolios where UNDEFINED");
    Object result = query.execute();
    if(!(result instanceof Collection) || ((Collection)result).size() != 0)
      fail(query.getQueryString());
    
    query = CacheUtils.getQueryService().newQuery("SELECT DISTINCT * FROM UNDEFINED");
    result = query.execute();
    if(!result.equals(QueryService.UNDEFINED))
      fail(query.getQueryString());
  }
  
  public void testNULL() throws Exception{
    Query query = CacheUtils.getQueryService().newQuery("SELECT DISTINCT * FROM /Portfolios where NULL");
    Object result = query.execute();
    if(!(result instanceof Collection) || ((Collection)result).size() != 0)
      fail(query.getQueryString());
    
    query = CacheUtils.getQueryService().newQuery("SELECT DISTINCT * FROM NULL");
    result = query.execute();
    if(!result.equals(QueryService.UNDEFINED))
      fail(query.getQueryString());
  }
}
