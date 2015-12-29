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
package com.gemstone.gemfire.cache.query;

import com.gemstone.gemfire.cache.*;
import java.util.*;
import junit.framework.*;

// for internal access test
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.cache.RegionEntry;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.EntrySnapshot;
import com.gemstone.gemfire.cache.query.data.Portfolio;

/*
 * RegionTest.java
 *
 * Created on January 31, 2005, 3:54 PM
 */

/**
 *
 * @author vaibhav
 */
public class RegionTest extends TestCase{

  static String queries[] = {
    "status = 'active'",
            "status <> 'active'",
            "ID > 2",
            "ID < 1",
            "ID >= 2",
            "ID <= 1",
            "status = 'active' AND ID = 0",
            "status = 'active' AND ID = 1",
            "status = 'active' OR ID = 1",
            "isActive",
            "isActive()",
            "testMethod(true)",
            "NOT isActive",
            "P1.secId = 'SUN'",
            "status = 'active' AND ( ID = 1 OR P1.secId = 'SUN')",
  };

  Region region;
  QueryService qs;
  Cache cache;


  public RegionTest(String testName) {
    super(testName);
  }

  public static Test suite(){
    TestSuite suite = new TestSuite(RegionTest.class);
    return suite;
  }

  public void testShortcutMethods() throws Exception {
    for(int i=0;i<queries.length;i++){
      System.out.println("Query = "+queries[i]);
      Object r = region.query(queries[i]);
      CacheUtils.getLogger().fine(Utils.printResult(r));
    }
  }

  public void testQueryServiceInterface() throws Exception {
    for(int i=0;i<queries.length;i++){
      System.out.println("Query = select distinct * from /pos where "+queries[i]);
      Query q = qs.newQuery("select distinct * from /pos where "+queries[i]);
      Object r = q.execute();
      CacheUtils.getLogger().fine(Utils.printResult(r));
    }
  }


  public void testParameterBinding() throws Exception {
    Query q = qs.newQuery("select distinct * from /pos where ID = $1");
    Object [] params = new Object[]{new Integer(0)};//{"active"};
    Object r = q.execute(params);
    CacheUtils.getLogger().fine(Utils.printResult(r));

    q = qs.newQuery("select distinct * from $1 where status = $2 and ID = $3");
    params = new Object[]{this.region, "active", new Integer(0)};
    r = q.execute(params);
    CacheUtils.getLogger().fine(Utils.printResult(r));
  }



  public void testQRegionInterface() throws Exception {
    String queries[] = {
      "select distinct * from /pos.keys where toString = '1'",
              "select distinct * from /pos.values where status = 'active'",
              "select distinct * from /pos.entries where key = '1'",
              "select distinct * from /pos.entries where value.status = 'active'"
    };

    for(int i=0;i<queries.length;i++){
      System.out.println("Query = "+queries[i]);
      Query q = qs.newQuery(queries[i]);
      Object r = q.execute();
      CacheUtils.getLogger().fine(Utils.printResult(r));
    }
  }


  public void testInvalidEntries() throws Exception {
    region.invalidate("1");
    region.invalidate("3");
    Query q = qs.newQuery("select distinct * from /pos");
    SelectResults results = (SelectResults)q.execute();
    assertEquals(2, results.size()); // should not include NULLs
  }

  public void testRegionEntryAccess() throws Exception {
    Iterator entriesIter = region.entries(false).iterator();
    while(entriesIter.hasNext()){
      Region.Entry entry = (Region.Entry)entriesIter.next();
      RegionEntry regionEntry = null;
      if (entry instanceof LocalRegion.NonTXEntry) {
        regionEntry = ((LocalRegion.NonTXEntry)entry).getRegionEntry();
      }
      else {
        regionEntry = ((EntrySnapshot)entry).getRegionEntry();
      }
      assertNotNull(regionEntry);
    }


    LocalRegion lRegion = (LocalRegion)region;
    Iterator keysIterator = lRegion.keys().iterator();
    while(keysIterator.hasNext()){
      Object key = keysIterator.next();
      Region.Entry rEntry = lRegion.getEntry(key);
      RegionEntry regionEntry = null;
      if (rEntry instanceof LocalRegion.NonTXEntry) {
        regionEntry = ((LocalRegion.NonTXEntry)rEntry).getRegionEntry();
      }
      else {
        regionEntry = ((EntrySnapshot)rEntry).getRegionEntry();
      }
      assertNotNull(regionEntry);
    }
  }

  public void testRegionNames() {

    String queryStrs[] = new String[] {
        "SELECT * FROM /pos",
        "SELECT * FROM /pos where status='active'",
        "SELECT * FROM /p:os", // Using ':' in region name.
        "SELECT * FROM /p@os", // Using '@' in region name.
        "SELECT * FROM /p-os", // Using '-' in region name.
        "SELECT * FROM /p#os", // Using '#' in region name.
        "SELECT * FROM /p+os", // Using '+' in region name.
    };

    CacheUtils.startCache();
    cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setValueConstraint(Portfolio.class);
    RegionAttributes regionAttributes = attributesFactory.create();

    cache.createRegion("p:os",regionAttributes);
    cache.createRegion("p@os",regionAttributes);
    cache.createRegion("p-os",regionAttributes);
    cache.createRegion("p#os",regionAttributes);
    cache.createRegion("p+os",regionAttributes);
    cache.createRegion("p \"ou\"\"inner\"\"ter\"s",regionAttributes);

    for(int i=0; i<queryStrs.length;++i) {
       Query q = CacheUtils.getQueryService().newQuery(queryStrs[i]);
       try {
         q.execute();
       } catch (Exception ex) {
         // Failed - Any other exception.
         fail("Failed to execute the query. '" +  queryStrs[i] + "' Error: " + ex.getMessage());
       }
    }
  }

  protected void setUp() throws Exception {
    CacheUtils.startCache();
    cache = CacheUtils.getCache();
    AttributesFactory attributesFactory = new AttributesFactory();
    attributesFactory.setValueConstraint(Portfolio.class);
    RegionAttributes regionAttributes = attributesFactory.create();

    region = cache.createRegion("pos",regionAttributes);
    region.put("0",new Portfolio(0));
    region.put("1",new Portfolio(1));
    region.put("2",new Portfolio(2));
    region.put("3",new Portfolio(3));

    qs = cache.getQueryService();
  }

  public void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

}
