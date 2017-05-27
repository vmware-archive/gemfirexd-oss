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
package com.gemstone.gemfire.cache.query.functional;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.data.Position;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache.query.internal.ResultsCollectionWrapper;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * 
 * @author ashahid
 * 
 */
public class OrderByTest extends TestCase {

  public OrderByTest(String testName) {
    super(testName);
  }

  protected void setUp() throws java.lang.Exception {
    CacheUtils.startCache();

  }

  protected void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }

  public static void main(String[] args) {
    junit.textui.TestRunner.run(suite());
  }

  public static Test suite() {
    TestSuite suite = new TestSuite(OrderByTest.class);
    return suite;
  }

  public void testOrderByWithIndexResultDefaultProjection() throws Exception {
    String queries[] = {
        // Test case No. IUMR021
        "SELECT  distinct * FROM /portfolio1 pf1 where ID > 10 order by ID desc ",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID > 10 order by ID asc ",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID asc ",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID desc ",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID desc ",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID asc",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID != 10 order by ID asc ",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID != 10 order by ID desc ",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID > 10 order by ID desc limit 5",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID > 10 order by ID asc limit 5",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID asc limit 5 ",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID desc limit 5",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID desc limit 5",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID asc limit 5",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID != 10 order by ID asc limit 10",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID != 10 order by ID desc limit 10",
        
    };
    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions

    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);

    for (int i = 0; i < 50; i++) {
      r1.put(i + "", new Portfolio(i));
    }

    
    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create Indexes

    qs.createIndex("IDIndexPf1", IndexType.FUNCTIONAL, "ID", "/portfolio1");

    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        ResultsCollectionWrapper rcw = (ResultsCollectionWrapper)r[i][1];
        int indexLimit =  queries[i].indexOf("limit") ;
        int limit=-1;
        boolean limitQuery = indexLimit!= -1;
        if(limitQuery) {
          limit = Integer.parseInt(queries[i].substring(indexLimit+5).trim());
        }
        assertTrue("Result size is "+rcw.size()+" and limit is "+limit, !limitQuery || rcw.size() <= limit);
        String colType = rcw.getCollectionType().getSimpleClassName();
        if (!(colType.equals("SortedSet") || colType.equals("LinkedHashSet"))){
          fail("The collection type " + colType + " is not expexted");
        }
        if (!observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }

        Iterator itr = observer.indexesUsed.iterator();
        while (itr.hasNext()) {
          if (!(itr.next().toString()).equals("IDIndexPf1")) {
            fail("<IDIndexPf1> was expected but found "
                + itr.next().toString());
          }
          // assertEquals("statusIndexPf1",itr.next().toString());
        }

        int indxs = observer.indexesUsed.size();

        System.out
            .println("**************************************************Indexes Used :::::: "
                + indxs + " Index Name: " + observer.indexName);

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length,true,queries);
  }
  public void testOrderByWithIndexResultWithProjection() throws Exception {
    String queries[] = {
        // Test case No. IUMR021
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID > 10 order by ID desc ",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID > 10 order by ID asc ",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID asc ",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID desc ",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID desc ",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID asc",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID != 10 order by ID asc ",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID != 10 order by ID desc ",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID > 10 order by ID desc limit 5",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID > 10 order by ID asc limit 5",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID asc limit 5 ",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID desc limit 5",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID desc limit 5",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID asc limit 5",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID != 10 order by ID asc limit 10",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID != 10 order by ID desc limit 10",
        
    };
    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions

    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);

    for (int i = 0; i < 50; i++) {
      r1.put(i + "", new Portfolio(i));
    }

    
    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create Indexes

    qs.createIndex("IDIndexPf1", IndexType.FUNCTIONAL, "ID", "/portfolio1");

    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        ResultsCollectionWrapper rcw = (ResultsCollectionWrapper)r[i][1];
        int indexLimit =  queries[i].indexOf("limit") ;
        int limit=-1;
        boolean limitQuery = indexLimit!= -1;
        if(limitQuery) {
          limit = Integer.parseInt(queries[i].substring(indexLimit+5).trim());
        }
        assertTrue(!limitQuery || rcw.size() <= limit);
        //assertEquals("Set",rcw.getCollectionType().getSimpleClassName());
        String colType = rcw.getCollectionType().getSimpleClassName();
        if (!(colType.equals("SortedSet") || colType.equals("LinkedHashSet"))){
          fail("The collection type " + colType + " is not expexted");
        }
        if (!observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }

        Iterator itr = observer.indexesUsed.iterator();
        while (itr.hasNext()) {
          if (!(itr.next().toString()).equals("IDIndexPf1")) {
            fail("<IDIndexPf1> was expected but found "
                + itr.next().toString());
          }
          // assertEquals("statusIndexPf1",itr.next().toString());
        }

        int indxs = observer.indexesUsed.size();

        System.out
            .println("**************************************************Indexes Used :::::: "
                + indxs + " Index Name: " + observer.indexName);

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length,true,queries);
  }
  
  public void testMultiColOrderByWithIndexResultDefaultProjection() throws Exception {
    String queries[] = {
        // Test case No. IUMR021
        "SELECT  distinct * FROM /portfolio1 pf1 where ID > 10 order by ID desc, pkid asc ",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID > 10 order by ID asc, pkid desc ",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID asc, pkid asc ",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID desc , pkid desc",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID desc, pkid desc ",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID asc, pkid asc",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID != 10 order by ID asc, pkid asc ",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID != 10 order by ID desc, pkid desc ",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID > 10 order by ID desc, pkid asc limit 5",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID > 10 order by ID asc, pkid desc limit 5",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID asc, pkid asc limit 5 ",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID desc, pkid desc limit 5",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID desc, pkid asc limit 5",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID asc, pkid desc limit 5",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID != 10 order by ID asc, pkid asc limit 10",
        "SELECT  distinct * FROM /portfolio1 pf1 where ID != 10 order by ID desc, pkid desc limit 10",
        
    };
    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions

    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);

    for (int i = 0; i < 50; i++) {
      r1.put(i + "", new Portfolio(i));
    }

    
    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create Indexes

    qs.createIndex("IDIndexPf1", IndexType.FUNCTIONAL, "ID", "/portfolio1");

    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        ResultsCollectionWrapper rcw = (ResultsCollectionWrapper)r[i][1];
        int indexLimit =  queries[i].indexOf("limit") ;
        int limit=-1;
        boolean limitQuery = indexLimit!= -1;
        if(limitQuery) {
          limit = Integer.parseInt(queries[i].substring(indexLimit+5).trim());
        }
        assertTrue(!limitQuery || rcw.size() <= limit);
        assertEquals("SortedSet",rcw.getCollectionType().getSimpleClassName());
        if (!observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }

        Iterator itr = observer.indexesUsed.iterator();
        while (itr.hasNext()) {
          if (!(itr.next().toString()).equals("IDIndexPf1")) {
            fail("<IDIndexPf1> was expected but found "
                + itr.next().toString());
          }
          // assertEquals("statusIndexPf1",itr.next().toString());
        }

        int indxs = observer.indexesUsed.size();

        System.out
            .println("**************************************************Indexes Used :::::: "
                + indxs + " Index Name: " + observer.indexName);

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length,true,queries);
  }
  public void testMultiColOrderByWithIndexResultWithProjection() throws Exception {
    String queries[] = {
        // Test case No. IUMR021
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID > 10 order by ID desc, pkid desc ",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID > 10 order by ID asc, pkid asc ",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID asc, pkid asc ",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID desc , pkid desc",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID desc, pkid asc ",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID asc, pkid desc",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID != 10 order by ID asc , pkid desc",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID != 10 order by ID desc, pkid asc ",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID > 10 order by ID desc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID > 10 order by ID asc, pkid asc limit 5",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID asc, pkid desc limit 5 ",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID > 10 and ID < 20 order by ID desc, pkid asc limit 5",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID desc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID >= 10 and ID <= 20 order by ID asc, pkid asc limit 5",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID != 10 order by ID asc , pkid desc limit 10",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where ID != 10 order by ID desc, pkid desc limit 10",
        
    };
    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions

    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);

    for (int i = 0; i < 50; i++) {
      r1.put(i + "", new Portfolio(i));
    }

    
    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create Indexes

    qs.createIndex("IDIndexPf1", IndexType.FUNCTIONAL, "ID", "/portfolio1");

    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        ResultsCollectionWrapper rcw = (ResultsCollectionWrapper)r[i][1];
        int indexLimit =  queries[i].indexOf("limit") ;
        int limit=-1;
        boolean limitQuery = indexLimit!= -1;
        if(limitQuery) {
          limit = Integer.parseInt(queries[i].substring(indexLimit+5).trim());
        }
        assertTrue(!limitQuery || rcw.size() <= limit);
        assertEquals("SortedSet",rcw.getCollectionType().getSimpleClassName());
        if (!observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }

        Iterator itr = observer.indexesUsed.iterator();
        while (itr.hasNext()) {
          if (!(itr.next().toString()).equals("IDIndexPf1")) {
            fail("<IDIndexPf1> was expected but found "
                + itr.next().toString());
          }
          // assertEquals("statusIndexPf1",itr.next().toString());
        }

        int indxs = observer.indexesUsed.size();

        System.out
            .println("**************************************************Indexes Used :::::: "
                + indxs + " Index Name: " + observer.indexName);

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length,true,queries);
  }
  
  public void testMultiColOrderByWithMultiIndexResultDefaultProjection() throws Exception {
    String queries[] = {
        // Test case No. IUMR021
        "SELECT  distinct * FROM /portfolio1 pf1 where pkid = '12' and ID > 10 order by ID desc, pkid asc ",
        "SELECT  distinct * FROM /portfolio1 pf1 where pkid > '1' and ID > 10 order by ID asc, pkid desc ",
        "SELECT  distinct * FROM /portfolio1 pf1 where pkid = '13'and  ID > 10 and ID < 20 order by ID asc, pkid asc ",
        "SELECT  distinct * FROM /portfolio1 pf1 where pkid <'9' and ID > 10 and ID < 20 order by ID desc , pkid desc",
        "SELECT  distinct * FROM /portfolio1 pf1 where pkid = '15' and ID >= 10 and ID <= 20 order by ID desc, pkid desc ",
        "SELECT  distinct * FROM /portfolio1 pf1 where pkid > '1' and pkid <='9' and ID >= 10 and ID <= 20 order by ID asc, pkid asc",
        "SELECT  distinct * FROM /portfolio1 pf1 where pkid > 'a' and ID != 10 order by ID asc, pkid asc ",
        "SELECT  distinct * FROM /portfolio1 pf1 where pkid > '1' and ID != 10 order by ID desc, pkid desc ",
        "SELECT  distinct * FROM /portfolio1 pf1 where pkid = '17' and ID > 10 order by ID desc, pkid asc limit 5",
        "SELECT  distinct * FROM /portfolio1 pf1 where pkid > '17' and ID > 10 order by ID asc, pkid desc limit 5",
        "SELECT  distinct * FROM /portfolio1 pf1 where pkid < '7' and ID > 10 and ID < 20 order by ID asc, pkid asc limit 5 ",
        "SELECT  distinct * FROM /portfolio1 pf1 where pkid = '18' and ID > 10 and ID < 20 order by ID desc, pkid desc limit 5",
        "SELECT  distinct * FROM /portfolio1 pf1 where pkid > 'a' and ID >= 10 and ID <= 20 order by ID desc, pkid asc limit 5",
        "SELECT  distinct * FROM /portfolio1 pf1 where pkid != '17' and ID >= 10 and ID <= 20 order by ID asc, pkid desc limit 5",
        "SELECT  distinct * FROM /portfolio1 pf1 where pkid > '0' and ID != 10 order by ID asc, pkid asc limit 10",
        "SELECT  distinct * FROM /portfolio1 pf1 where pkid > '9' and ID != 10 order by ID desc, pkid desc limit 10",
        
    };
    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions

    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);

    for (int i = 0; i < 50; i++) {
      r1.put(i + "", new Portfolio(i));
    }

    
    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create Indexes

    qs.createIndex("IDIndexPf1", IndexType.FUNCTIONAL, "ID", "/portfolio1");
    qs.createIndex("PKIDIndexPf1", IndexType.FUNCTIONAL, "pkid", "/portfolio1");
    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        
        ResultsCollectionWrapper rcw = (ResultsCollectionWrapper)r[i][1];
        
        int indexLimit =  queries[i].indexOf("limit") ;
        int limit=-1;
        boolean limitQuery = indexLimit!= -1;
        if(limitQuery) {
          limit = Integer.parseInt(queries[i].substring(indexLimit+5).trim());
        }
        assertTrue(!limitQuery || rcw.size() <= limit);
        assertEquals("SortedSet",rcw.getCollectionType().getSimpleClassName());
        if (!observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }

        Iterator itr = observer.indexesUsed.iterator();
        while (itr.hasNext()) {
          String indexUsed = itr.next().toString(); 
          if (!(indexUsed).equals("IDIndexPf1")) {
            fail("<IDIndexPf1> was expected but found "
                + indexUsed);
          }
          // assertEquals("statusIndexPf1",itr.next().toString());
        }

        int indxs = observer.indexesUsed.size();

        System.out
            .println("**************************************************Indexes Used :::::: "
                + indxs + " Index Name: " + observer.indexName);

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length,true,queries);
  }
  
  public void testMultiColOrderByWithMultiIndexResultProjection() throws Exception {
    String queries[] = {
        // Test case No. IUMR021
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid = '12' and ID > 10 order by ID desc, pkid asc ",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid > '1' and ID > 10 order by ID asc, pkid desc ",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid = '13'and  ID > 10 and ID < 20 order by ID asc, pkid asc ",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid <'9' and ID > 10 and ID < 20 order by ID desc , pkid desc",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid = '15' and ID >= 10 and ID <= 20 order by ID desc, pkid desc ",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid > '1' and pkid <='9' and ID >= 10 and ID <= 20 order by ID asc, pkid asc",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid > 'a' and ID != 10 order by ID asc, pkid asc ",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid > '1' and ID != 10 order by ID desc, pkid desc ",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid = '17' and ID > 10 order by ID desc, pkid asc limit 5",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid > '17' and ID > 10 order by ID asc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid < '7' and ID > 10 and ID < 20 order by ID asc, pkid asc limit 5 ",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid = '18' and ID > 10 and ID < 20 order by ID desc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid > 'a' and ID >= 10 and ID <= 20 order by ID desc, pkid asc limit 5",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid != '17' and ID >= 10 and ID <= 20 order by ID asc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid > '0' and ID != 10 order by ID asc, pkid asc limit 10",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid > '9' and ID != 10 order by ID desc, pkid desc limit 10"
        
    };

    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions

    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);

    for (int i = 0; i < 50; i++) {
      r1.put(i + "", new Portfolio(i));
    }

    
    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create Indexes

    qs.createIndex("IDIndexPf1", IndexType.FUNCTIONAL, "ID", "/portfolio1");
    qs.createIndex("PKIDIndexPf1", IndexType.FUNCTIONAL, "pkid", "/portfolio1");
    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        int indexLimit =  queries[i].indexOf("limit") ;
        int limit=-1;
        boolean limitQuery = indexLimit!= -1;
        if(limitQuery) {
          limit = Integer.parseInt(queries[i].substring(indexLimit+5).trim());
        }
        ResultsCollectionWrapper rcw = (ResultsCollectionWrapper)r[i][1];
        assertEquals("SortedSet",rcw.getCollectionType().getSimpleClassName());
        if (!observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }
        assertTrue(!limitQuery || rcw.size() <= limit);

        Iterator itr = observer.indexesUsed.iterator();
        while (itr.hasNext()) {
          String indexUsed = itr.next().toString();          
          if (!(indexUsed).equals("IDIndexPf1")) {
            fail("<IDIndexPf1> was expected but found "
                + indexUsed);
          }
          // assertEquals("statusIndexPf1",itr.next().toString());
        }

        int indxs = observer.indexesUsed.size();

        System.out
            .println("**************************************************Indexes Used :::::: "
                + indxs + " Index Name: " + observer.indexName);

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length,true,queries);
  }
  
  
  public void testLimitNotAppliedIfOrderByNotUsingIndex() throws Exception {
    String queries[] = {
        // Test case No. IUMR021
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid = '12' and ID > 10 order by ID desc, pkid asc ",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid > '1' and ID > 10 order by ID asc, pkid desc ",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid = '13'and  ID > 10 and ID < 20 order by ID asc, pkid asc ",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid <'9' and ID > 10 and ID < 20 order by ID desc , pkid desc",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid = '15' and ID >= 10 and ID <= 20 order by ID desc, pkid desc ",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid > '1' and pkid <='9' and ID >= 10 and ID <= 20 order by ID asc, pkid asc",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid > 'a' and ID != 10 order by ID asc, pkid asc ",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid > '1' and ID != 10 order by ID desc, pkid desc ",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid = '17' and ID > 10 order by ID desc, pkid asc limit 5",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid > '17' and ID > 10 order by ID asc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid < '7' and ID > 10 and ID < 20 order by ID asc, pkid asc limit 5 ",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid = '18' and ID > 10 and ID < 20 order by ID desc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid > 'a' and ID >= 10 and ID <= 20 order by ID desc, pkid asc limit 5",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid != '17' and ID >= 10 and ID <= 20 order by ID asc, pkid desc limit 5",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid > '0' and ID != 10 order by ID asc, pkid asc limit 10",
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pkid > '9' and ID != 10 order by ID desc, pkid desc limit 10"
        
    };

    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions

    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);

    for (int i = 0; i < 50; i++) {
      r1.put(i + "", new Portfolio(i));
    }

    
    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create Indexes

  
    qs.createIndex("PKIDIndexPf1", IndexType.FUNCTIONAL, "pkid", "/portfolio1");
    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        int indexLimit =  queries[i].indexOf("limit") ;
        int limit=-1;
        boolean limitQuery = indexLimit!= -1;
        if(limitQuery) {
          limit = Integer.parseInt(queries[i].substring(indexLimit+5).trim());
        }
        ResultsCollectionWrapper rcw = (ResultsCollectionWrapper)r[i][1];
        assertEquals("SortedSet",rcw.getCollectionType().getSimpleClassName());
        if (!observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }
        assertTrue(!limitQuery || !observer.limitAppliedAtIndex);

        Iterator itr = observer.indexesUsed.iterator();
        while (itr.hasNext()) {
          String indexUsed = itr.next().toString();          
          if (!(indexUsed).equals("PKIDIndexPf1")) {
            fail("<PKIDIndexPf1> was expected but found "
                + indexUsed);
          }
          // assertEquals("statusIndexPf1",itr.next().toString());
        }

        int indxs = observer.indexesUsed.size();

        System.out
            .println("**************************************************Indexes Used :::::: "
                + indxs + " Index Name: " + observer.indexName);

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length,true,queries);
  }
  
  public void testLimitAndOrderByApplicationOnPrimaryKeyIndexQuery() throws Exception {
    String queries[] = {
        //The PK index should  be used but limit should not be applied as order by cannot be applied while data is fetched
        // from index
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pf1.ID != '10' order by ID desc limit 5 " ,
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pf1.ID != $1 order by ID "
        
    };

    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions

    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);

    for (int i = 0; i < 50; i++) {
      r1.put(i + "", new Portfolio(i));
    }

    
    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute(new Object[]{new Integer(10)});
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create Indexes

  
    qs.createIndex("PKIDIndexPf1", IndexType.PRIMARY_KEY, "ID", "/portfolio1");
    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute(new Object[]{"10"});
        int indexLimit =  queries[i].indexOf("limit") ;
        int limit=-1;
        boolean limitQuery = indexLimit!= -1;
        if(limitQuery) {
          limit = Integer.parseInt(queries[i].substring(indexLimit+5).trim());
        }
        boolean orderByQuery =  queries[i].indexOf("order by") != -1 ;
        ResultsCollectionWrapper rcw = (ResultsCollectionWrapper)r[i][1];
        if(orderByQuery) {
          assertEquals("SortedSet",rcw.getCollectionType().getSimpleClassName());
        }
        if (!observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }
        if(limitQuery) {
          if(orderByQuery) {
            assertFalse(observer.limitAppliedAtIndex);
          }else {
            assertTrue(observer.limitAppliedAtIndex);            
          }
        }else {
          assertFalse(observer.limitAppliedAtIndex);
        }        

        Iterator itr = observer.indexesUsed.iterator();
        while (itr.hasNext()) {
          String indexUsed = itr.next().toString();          
          if (!(indexUsed).equals("PKIDIndexPf1")) {
            fail("<PKIDIndexPf1> was expected but found "
                + indexUsed);
          }
          // assertEquals("statusIndexPf1",itr.next().toString());
        }

        int indxs = observer.indexesUsed.size();

        System.out
            .println("**************************************************Indexes Used :::::: "
                + indxs + " Index Name: " + observer.indexName);

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length,true,queries);
  }
  
  public void testLimitApplicationOnPrimaryKeyIndex() throws Exception{

    String queries[] = {
        //The PK index should  be used but limit should not be applied as order by cannot be applied while data is fetched
        // from index       
        "SELECT  distinct ID, description, createTime FROM /portfolio1 pf1 where pf1.ID != $1 limit 10",
    };

    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions

    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);

    for (int i = 0; i < 50; i++) {
      r1.put(i + "", new Portfolio(i));
    }

    
    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute(new Object[]{new Integer(10)});
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create Indexes

  
    qs.createIndex("PKIDIndexPf1", IndexType.PRIMARY_KEY, "ID", "/portfolio1");
    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute(new Object[]{"10"});
        int indexLimit =  queries[i].indexOf("limit") ;
        int limit=-1;
        boolean limitQuery = indexLimit!= -1;
        if(limitQuery) {
          limit = Integer.parseInt(queries[i].substring(indexLimit+5).trim());
        }
        boolean orderByQuery =  queries[i].indexOf("order by") != -1 ;
        ResultsCollectionWrapper rcw = (ResultsCollectionWrapper)r[i][1];
        if(orderByQuery) {
          assertEquals("SortedSet",rcw.getCollectionType().getSimpleClassName());
        }
        if (!observer.isIndexesUsed) {
          fail("Index is NOT uesd");
        }
        int indexDistinct =  queries[i].indexOf("distinct") ;
        boolean distinctQuery = indexDistinct!= -1;
        
        if(limitQuery) {
          if(orderByQuery) {
            assertFalse(observer.limitAppliedAtIndex);
          }else {
            assertTrue(observer.limitAppliedAtIndex);            
          }
        }else {
          assertFalse(observer.limitAppliedAtIndex);
        }        

        Iterator itr = observer.indexesUsed.iterator();
        while (itr.hasNext()) {
          String indexUsed = itr.next().toString();          
          if (!(indexUsed).equals("PKIDIndexPf1")) {
            fail("<PKIDIndexPf1> was expected but found "
                + indexUsed);
          }
          // assertEquals("statusIndexPf1",itr.next().toString());
        }

        int indxs = observer.indexesUsed.size();

        System.out
            .println("**************************************************Indexes Used :::::: "
                + indxs + " Index Name: " + observer.indexName);

      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    
    //Result set verification
    Collection coll1 = null;
    Collection coll2 = null;
    Iterator itert1 = null;
    Iterator itert2 = null;
    ObjectType type1, type2;
    type1 = ((SelectResults)r[0][0]).getCollectionType().getElementType();
    type2 = ((SelectResults)r[0][1]).getCollectionType().getElementType();
    if ((type1.getClass().getName()).equals(type2.getClass().getName())) {
      System.out.println("Both SelectResults are of the same Type i.e.--> "
          + ((SelectResults)r[0][0]).getCollectionType().getElementType());
    }
    else {
      System.out.println("Classes are : " + type1.getClass().getName() + " "
          + type2.getClass().getName());
      fail("FAILED:Select result Type is different in both the cases."+ "; failed query="+queries[0]);
    }
    if (((SelectResults)r[0][0]).size() == ((SelectResults)r[0][1]).size()) {
      System.out.println("Both SelectResults are of Same Size i.e.  Size= "
          + ((SelectResults)r[0][1]).size());
    }
    else {
      fail("FAILED:SelectResults size is different in both the cases. Size1="
          + ((SelectResults)r[0][0]).size() + " Size2 = "
          + ((SelectResults)r[0][1]).size()+"; failed query="+queries[0]);
    }
      coll2 = (((SelectResults)r[0][1]).asSet());
      coll1 = (((SelectResults)r[0][0]).asSet());
    
    itert1 = coll1.iterator();
    itert2 = coll2.iterator();
    while (itert1.hasNext()) {
      Object[] values1 = ((Struct)itert1.next()).getFieldValues();
      Object[] values2 = ((Struct)itert2.next()).getFieldValues();
      assertEquals(values1.length, values2.length);
      assertTrue((((Integer)values1[0]).intValue() != 10));
      assertTrue((((Integer)values2[0]).intValue() != 10));
    }
  
  }
  
  public void testOrderedResultsReplicatedRegion() throws Exception {
    String queries[] = {
        // Test case No. IUMR021
        /*"select distinct status as st from /portfolio1 where ID > 0 order by status",*/
        "select distinct p.status as st from /portfolio1 p where ID > 0 and status = 'inactive' order by p.status",   
        /*"select distinct p.position1.secId as st from /portfolio1 p where p.ID > 0 and p.position1.secId != 'IBM' order by p.position1.secId",        
        "select distinct  key.status as st from /portfolio1 key where key.ID > 5 order by key.status",
        "select distinct  key.status as st from /portfolio1 key where key.status = 'inactive' order by key.status desc, key.ID"
        */
    };
    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions

    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);

    for (int i = 0; i < 50; i++) {
      r1.put(i + "", new Portfolio(i));
    }

    
    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create Indexes
    qs.createIndex("i1",IndexType.FUNCTIONAL, "p.status","/portfolio1 p" );
    qs.createIndex("i2",IndexType.FUNCTIONAL, "p.ID","/portfolio1 p" );
    qs.createIndex("i3",IndexType.FUNCTIONAL, "p.position1.secId","/portfolio1 p" );
    
    
    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        
        
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length,true,queries);
  }
  
  public void testOrderByWithNullValues() throws Exception {
    // IN ORDER BY NULL values are treated as smallest. E.g For an ascending order by field
    // its null values are reported first and then the values in ascending order.
    String queries[] = {
        "SELECT  distinct * FROM /portfolio1 pf1 order by pkid", // 0 null values are first in the order.
        "SELECT  distinct * FROM /portfolio1 pf1  order by pkid asc", //1 same as above.
        "SELECT  distinct * FROM /portfolio1 order by pkid desc", //2 null values are last in the order.
        "SELECT  distinct pkid FROM /portfolio1 pf1 order by pkid", //3 null values are first in the order.
        "SELECT  distinct pkid FROM /portfolio1 pf1 where pkid != 'XXXX' order by pkid asc", //4
        "SELECT  distinct pkid FROM /portfolio1 pf1 where pkid != 'XXXX' order by pkid desc", //5 null values are last in the order.
        "SELECT  distinct ID FROM /portfolio1 pf1 where ID < 1000 order by pkid", //6
        "SELECT  distinct ID FROM /portfolio1 pf1 where ID > 3 order by pkid", //7 
        "SELECT  distinct ID, pkid FROM /portfolio1 pf1 where ID < 1000 order by pkid", //8
        "SELECT  distinct ID, pkid FROM /portfolio1 pf1 where ID > 0 order by pkid", //9 
        "SELECT  distinct ID, pkid FROM /portfolio1 pf1 where ID > 0 order by pkid, ID asc", //10
        "SELECT  distinct ID, pkid FROM /portfolio1 pf1 where ID > 0 order by pkid, ID desc", //11
    };

    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();

    // Create Regions
    final int size = 9;
    final int numNullValues = 3;
    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);
    for (int i = 1; i <= size; i++) {
      Portfolio pf = new Portfolio(i);
      // Add numNullValues null values.
      if(i <= numNullValues) {
        pf.pkid = null;
        pf.status = "a" + i;
      }
      r1.put(i + "", pf);
    }

    Query q = null;
    SelectResults results = null;
    List list = null;
    String str = "";
    try {
      // Query 0 - null values are first in the order.
      str = queries[0];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults)q.execute();
      r[0][0] = results;
      list = results.asList();
      for (int i = 1; i <= size; i++) {
        Portfolio p = (Portfolio)list.get((i-1));
        if (i <= numNullValues) {
          assertNull( "Expected null value for pkid, p: " + p, p.pkid);
        } else {
          assertNotNull("Expected not null value for pkid", p.pkid);
          if (!p.pkid.equals("" + i)) {
            fail(" Value of pkid is not in expected order.");  
          }
        }
      }
      
      // Query 1 - null values are first in the order.
      str = queries[1];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults)q.execute();
      list = results.asList();
      for (int i = 1; i <= size; i++) {
        Portfolio p = (Portfolio)list.get((i-1));
        if (i <= numNullValues) {
          assertNull( "Expected null value for pkid", p.pkid);
        } else {
          assertNotNull("Expected not null value for pkid", p.pkid);
          if (!p.pkid.equals("" + i)) {
            fail(" Value of pkid is not in expected order.");  
          }
        }
      }
      
      // Query 2 - null values are last in the order.
      str = queries[2];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults)q.execute();
      list = results.asList();
      for (int i = 1; i <= size; i++) {
        Portfolio p = (Portfolio)list.get((i-1));
        if (i > (size - numNullValues )) {
          assertNull( "Expected null value for pkid", p.pkid);
        } else {
          assertNotNull("Expected not null value for pkid", p.pkid);
          if (!p.pkid.equals("" + (size - (i-1)))) {
            fail(" Value of pkid is not in expected order.");  
          }
        }
      }

      // Query 3 - 1 distinct null value with pkid.
      str = queries[3];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults)q.execute();
      list = results.asList();
      for (int i = 1; i <= list.size(); i++) {
        String pkid = (String)list.get((i-1));
        if (i == 1) {
          assertNull( "Expected null value for pkid", pkid);
        } else {
          assertNotNull("Expected not null value for pkid", pkid);
          if (!pkid.equals("" + (numNullValues + (i-1)))) {
            fail(" Value of pkid is not in expected order.");  
          }
        }
      }
      
      // Query 4 - 1 distinct null value with pkid.
      str = queries[4];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults)q.execute();
      list = results.asList();
      for (int i = 1; i <= list.size(); i++) {
        String pkid = (String)list.get((i-1));
        if (i == 1) {
          assertNull( "Expected null value for pkid", pkid);
        } else {
          assertNotNull("Expected not null value for pkid", pkid);
          if (!pkid.equals("" + (numNullValues + (i-1)))) {
            fail(" Value of pkid is not in expected order.");  
          }
        }
      }
      
      // Query 5 -  1 distinct null value with pkid at the end.
      str = queries[5];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults)q.execute();
      list = results.asList();
      for (int i = 1; i <= list.size(); i++) {
        String pkid = (String)list.get((i-1));
        if (i == (list.size())) {
          assertNull( "Expected null value for pkid", pkid);
        } else {
          assertNotNull("Expected not null value for pkid", pkid);
          if (!pkid.equals("" + (size - (i-1)))) {
            fail(" Value of pkid is not in expected order.");  
          }
        }
      }
      
      // Query 6 - ID field values should be in the same order.
      str = queries[6];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults)q.execute();
      list = results.asList();
      for (int i = 1; i <= size; i++) {
        int id = ((Integer)list.get((i-1))).intValue();
        // ID should be one of 1, 2, 3  because of distinct
        if (i <= numNullValues) {
          if (!(id == 1 || id == 2 || id == 3)) {
            fail(" Value of ID is not as expected " + id);            
          }
        } else {
          if (id != i) {
            fail(" Value of ID is not as expected " + id);             
          }
        }
      }
      
      // Query 7 - ID field values should be in the same order.
      str = queries[7];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults)q.execute();
      list = results.asList();
      for (int i = 1; i <= list.size(); i++) {
        int id = ((Integer)list.get((i-1))).intValue();
        if (id != (numNullValues + i)) {
          fail(" Value of ID is not as expected, " + id);             
        }
      }
      
       // Query 8 - ID, pkid field values should be in the same order.
      str = queries[8];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults)q.execute();
      list = results.asList();
      for (int i = 1; i <= size; i++) {
        Struct vals = (Struct)list.get((i-1));
        int id  = ((Integer)vals.get("ID")).intValue();
        String pkid = (String)vals.get("pkid");

        // ID should be one of 1, 2, 3  because of distinct
        if (i <= numNullValues) {
          if (!(id == 1 || id == 2 || id == 3)) {
            fail(" Value of ID is not as expected " + id);            
          }
          assertNull( "Expected null value for pkid", pkid);
        } else {
          if (id != i) {
            fail(" Value of ID is not as expected " + id);             
          }
          assertNotNull( "Expected not null value for pkid", pkid);
          if (!pkid.equals("" + i)) {
            fail(" Value of pkid is not in expected order.");  
          }
        }
      }
      
      // Query 9 - ID, pkid field values should be in the same order.
      str = queries[9];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults)q.execute();
      list = results.asList();
      
      for (int i = 1; i <= list.size(); i++) {      
        Struct vals = (Struct)list.get((i-1));
        int id  = ((Integer)vals.get("ID")).intValue();
        String pkid = (String)vals.get("pkid");

        if (i <= numNullValues) {
          assertNull( "Expected null value for pkid, " + pkid, pkid);
          if (!(id == 1 || id == 2 || id == 3)) {
            fail(" Value of ID is not as expected " + id);            
          }
        } else {
          if (!pkid .equals("" + i)) {
            fail(" Value of pkid is not as expected, " + pkid);             
          }
          if (id != i) {
            fail(" Value of ID is not as expected, " + id);             
          }
        }
      }
      
      // Query 10 - ID asc, pkid field values should be in the same order.
      str = queries[10];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults)q.execute();
      list = results.asList();
      
      for (int i = 1; i <= list.size(); i++) {      
        Struct vals = (Struct)list.get((i-1));
        int id  = ((Integer)vals.get("ID")).intValue();
        String pkid = (String)vals.get("pkid");

        if (i <= numNullValues) {
          assertNull( "Expected null value for pkid, " + pkid, pkid);
          if (id != i) {
            fail(" Value of ID is not as expected, it is: " + id + " expected :" + i);            
          }
        } else {
          if (!pkid .equals("" + i)) {
            fail(" Value of pkid is not as expected, " + pkid);             
          }
          if (id != i) {
            fail(" Value of ID is not as expected, " + id);             
          }
        }
      }
      
    // Query 11 - ID desc, pkid field values should be in the same order.
      str = queries[11];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults)q.execute();
      list = results.asList();
      
      for (int i = 1; i <= list.size(); i++) {      
        Struct vals = (Struct)list.get((i-1));
        int id  = ((Integer)vals.get("ID")).intValue();
        String pkid = (String)vals.get("pkid");

        if (i <= numNullValues) {
          assertNull( "Expected null value for pkid, " + pkid, pkid);
          if (id != (numNullValues - (i -1))) {
            fail(" Value of ID is not as expected " + id);            
          }
        } else {
          if (!pkid .equals("" + i)) {
            fail(" Value of pkid is not as expected, " + pkid);             
          }
          if (id != i) {
            fail(" Value of ID is not as expected, " + id);             
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(q.getQueryString());
    }
  }

  public void testOrderByWithNullValuesUseIndex() throws Exception {
    // IN ORDER BY NULL values are treated as smallest. E.g For an ascending order by field
    // its null values are reported first and then the values in ascending order.
    String queries[] = {
        "SELECT  distinct * FROM /portfolio1 pf1 where ID > 0 order by pkid", // 0 null values are first in the order.
        "SELECT  distinct * FROM /portfolio1 pf1 where ID > 0 order by pkid asc", //1 same as above.
        "SELECT  distinct * FROM /portfolio1 where ID > 0 order by pkid desc", //2 null values are last in the order.
        "SELECT  distinct pkid FROM /portfolio1 pf1 where ID > 0 order by pkid", //3 null values are first in the order.
        "SELECT  distinct pkid FROM /portfolio1 pf1 where ID > 0 order by pkid asc", //4
        "SELECT  distinct pkid FROM /portfolio1 pf1 where ID > 0 order by pkid desc", //5 null values are last in the order.
        "SELECT  distinct ID FROM /portfolio1 pf1 where ID < 1000 order by pkid", //6
        "SELECT  distinct ID FROM /portfolio1 pf1 where ID > 3 order by pkid", //7 
        "SELECT  distinct ID, pkid FROM /portfolio1 pf1 where ID < 1000 order by pkid", //8
        "SELECT  distinct ID, pkid FROM /portfolio1 pf1 where ID > 0 order by pkid", //9 
    };

    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    
    // Create Regions
    final int size = 9;
    final int numNullValues = 3;
    Region r1 = CacheUtils.createRegion("portfolio1", Portfolio.class);
    for (int i = 1; i <= size; i++) {
      Portfolio pf = new Portfolio(i);
      // Add numNullValues null values.
      if(i <= numNullValues) {
        pf.pkid = null;
        pf.status = "a" + i;
      }
      r1.put(i + "", pf);
    }

    // Create Indexes
    qs.createIndex("IDIndexPf1", IndexType.FUNCTIONAL, "ID", "/portfolio1");
    qs.createIndex("PKIDIndexPf1", IndexType.FUNCTIONAL, "pkid", "/portfolio1");

    Query q = null;
    SelectResults results = null;
    List list = null;
    String str = "";
    try {
      // Query 0 - null values are first in the order.
      QueryObserverImpl observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      
      str = queries[0];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults)q.execute();
      
      if (!observer.isIndexesUsed) {
        fail("Index is NOT uesd");
      }
      
      r[0][0] = results;
      list = results.asList();
      for (int i = 1; i <= size; i++) {
        Portfolio p = (Portfolio)list.get((i-1));
        if (i <= numNullValues) {
          assertNull( "Expected null value for pkid, p: " + p, p.pkid);
        } else {
          assertNotNull("Expected not null value for pkid", p.pkid);
          if (!p.pkid.equals("" + i)) {
            fail(" Value of pkid is not in expected order.");  
          }
        }
      }
      
      // Query 1 - null values are first in the order.
      str = queries[1];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults)q.execute();
      list = results.asList();
      for (int i = 1; i <= size; i++) {
        Portfolio p = (Portfolio)list.get((i-1));
        if (i <= numNullValues) {
          assertNull( "Expected null value for pkid", p.pkid);
        } else {
          assertNotNull("Expected not null value for pkid", p.pkid);
          if (!p.pkid.equals("" + i)) {
            fail(" Value of pkid is not in expected order.");  
          }
        }
      }
      
      // Query 2 - null values are last in the order.
      str = queries[2];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults)q.execute();
      list = results.asList();
      for (int i = 1; i <= size; i++) {
        Portfolio p = (Portfolio)list.get((i-1));
        if (i > (size - numNullValues )) {
          assertNull( "Expected null value for pkid", p.pkid);
        } else {
          assertNotNull("Expected not null value for pkid", p.pkid);
          if (!p.pkid.equals("" + (size - (i-1)))) {
            fail(" Value of pkid is not in expected order.");  
          }
        }
      }

      // Query 3 - 1 distinct null value with pkid.
      str = queries[3];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults)q.execute();
      list = results.asList();
      for (int i = 1; i <= list.size(); i++) {
        String pkid = (String)list.get((i-1));
        if (i == 1) {
          assertNull( "Expected null value for pkid", pkid);
        } else {
          assertNotNull("Expected not null value for pkid", pkid);
          if (!pkid.equals("" + (numNullValues + (i-1)))) {
            fail(" Value of pkid is not in expected order.");  
          }
        }
      }
      
      // Query 4 - 1 distinct null value with pkid.
      observer = new QueryObserverImpl();
      QueryObserverHolder.setInstance(observer);
      
      str = queries[4];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults)q.execute();
      
      if (!observer.isIndexesUsed) {
        fail("Index is NOT uesd");
      }
      
      list = results.asList();
      for (int i = 1; i <= list.size(); i++) {
        String pkid = (String)list.get((i-1));
        if (i == 1) {
          assertNull( "Expected null value for pkid", pkid);
        } else {
          assertNotNull("Expected not null value for pkid", pkid);
          if (!pkid.equals("" + (numNullValues + (i-1)))) {
            fail(" Value of pkid is not in expected order.");  
          }
        }
      }
      
      // Query 5 -  1 distinct null value with pkid at the end.
      str = queries[5];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults)q.execute();
      list = results.asList();
      for (int i = 1; i <= list.size(); i++) {
        String pkid = (String)list.get((i-1));
        if (i == (list.size())) {
          assertNull( "Expected null value for pkid", pkid);
        } else {
          assertNotNull("Expected not null value for pkid", pkid);
          if (!pkid.equals("" + (size - (i-1)))) {
            fail(" Value of pkid is not in expected order.");  
          }
        }
      }
      
      // Query 6 - ID field values should be in the same order.
      str = queries[6];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults)q.execute();
      list = results.asList();
      for (int i = 1; i <= size; i++) {
        int id = ((Integer)list.get((i-1))).intValue();
        // ID should be one of 1, 2, 3  because of distinct
        if (i <= numNullValues) {
          if (!(id == 1 || id == 2 || id == 3)) {
            fail(" Value of ID is not as expected " + id);            
          }
        } else {
          if (id != i) {
            fail(" Value of ID is not as expected " + id);             
          }
        }
      }
      
      // Query 7 - ID field values should be in the same order.
      str = queries[7];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults)q.execute();
      list = results.asList();
      for (int i = 1; i <= list.size(); i++) {
        int id = ((Integer)list.get((i-1))).intValue();
        if (id != (numNullValues + i)) {
          fail(" Value of ID is not as expected, " + id);             
        }
      }
      
       // Query 8 - ID, pkid field values should be in the same order.
      str = queries[8];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults)q.execute();
      list = results.asList();
      for (int i = 1; i <= size; i++) {
        Struct vals = (Struct)list.get((i-1));
        int id  = ((Integer)vals.get("ID")).intValue();
        String pkid = (String)vals.get("pkid");

        // ID should be one of 1, 2, 3  because of distinct
        if (i <= numNullValues) {
          if (!(id == 1 || id == 2 || id == 3)) {
            fail(" Value of ID is not as expected " + id);            
          }
          assertNull( "Expected null value for pkid", pkid);
        } else {
          if (id != i) {
            fail(" Value of ID is not as expected " + id);             
          }
          assertNotNull( "Expected not null value for pkid", pkid);
          if (!pkid.equals("" + i)) {
            fail(" Value of pkid is not in expected order.");  
          }
        }
      }
      
      // Query 9 - ID, pkid field values should be in the same order.
      str = queries[9];
      q = CacheUtils.getQueryService().newQuery(str);
      CacheUtils.getLogger().info("Executing query: " + str);
      results = (SelectResults)q.execute();
      list = results.asList();
      
      for (int i = 1; i <= list.size(); i++) {      
        Struct vals = (Struct)list.get((i-1));
        int id  = ((Integer)vals.get("ID")).intValue();
        String pkid = (String)vals.get("pkid");

        if (i <= numNullValues) {
          assertNull( "Expected null value for pkid, " + pkid, pkid);
          if (!(id == 1 || id == 2 || id == 3)) {
            fail(" Value of ID is not as expected " + id);            
          }
        } else {
          if (!pkid .equals("" + i)) {
            fail(" Value of pkid is not as expected, " + pkid);             
          }
          if (id != i) {
            fail(" Value of ID is not as expected, " + id);             
          }
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail(q.getQueryString());
    }
    
  }
  
  public void testOrderedResultsPartitionedRegion_Bug43514_1() throws Exception {
    String queries[] = {
        // Test case No. IUMR021
        "select distinct * from /portfolio1 p order by status, ID desc",
        "select distinct * from /portfolio1 p, p.positions.values val order by p.ID, val.secId desc",
        "select distinct p.status from /portfolio1 p order by p.status",
        "select distinct status, ID from /portfolio1 order by status, ID",
        "select distinct p.status, p.ID from /portfolio1 p order by p.status, p.ID",      
        "select distinct key.ID from /portfolio1.keys key order by key.ID",
        "select distinct key.ID, key.status from /portfolio1.keys key order by key.status, key.ID",
        "select distinct key.ID, key.status from /portfolio1.keys key order by key.status desc, key.ID",
        "select distinct key.ID, key.status from /portfolio1.keys key order by key.status, key.ID desc",
        "select distinct p.status, p.ID from /portfolio1 p order by p.status asc, p.ID",
        "select distinct p.ID, p.status from /portfolio1 p order by p.ID desc, p.status asc",
        "select distinct p.ID from /portfolio1 p, p.positions.values order by p.ID",
        "select distinct p.ID, p.status from /portfolio1 p, p.positions.values order by p.status, p.ID",
        "select distinct pos.secId from /portfolio1 p, p.positions.values pos order by pos.secId",
        "select distinct p.ID, pos.secId from /portfolio1 p, p.positions.values pos order by pos.secId, p.ID",
        "select distinct p.iD from /portfolio1 p order by p.iD",
        "select distinct p.iD, p.status from /portfolio1 p order by p.iD",
        "select distinct iD, status from /portfolio1 order by iD",
        "select distinct p.getID() from /portfolio1 p order by p.getID()",
        "select distinct p.names[1] from /portfolio1 p order by p.names[1]",
        "select distinct p.position1.secId, p.ID from /portfolio1 p order by p.position1.secId desc, p.ID",
        "select distinct p.ID, p.position1.secId from /portfolio1 p order by p.position1.secId, p.ID",
        "select distinct e.key.ID from /portfolio1.entries e order by e.key.ID",
        "select distinct e.key.ID, e.value.status from /portfolio1.entries e order by e.key.ID",
        "select distinct e.key.ID, e.value.status from /portfolio1.entrySet e order by e.key.ID desc , e.value.status desc",
        "select distinct e.key, e.value from /portfolio1.entrySet e order by e.key.ID, e.value.status desc",
        "select distinct e.key from /portfolio1.entrySet e order by e.key.ID desc, e.key.pkid desc",
        "select distinct p.ID, pos.secId from /portfolio1 p, p.positions.values pos order by p.ID, pos.secId",
        "select distinct p.ID, pos.secId from /portfolio1 p, p.positions.values pos order by p.ID desc, pos.secId desc",
        "select distinct p.ID, pos.secId from /portfolio1 p, p.positions.values pos order by p.ID desc, pos.secId",
              
    };
    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(paf.create());
    Region r1 = CacheUtils.createRegion("portfolio1", af.create(),false);

    for (int i = 0; i < 50; i++) {      
      r1.put(new Portfolio(i), new Portfolio(i));
    }

    
    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create Indexes
    qs.createIndex("i1",IndexType.FUNCTIONAL, "p.status","/portfolio1 p" );
    qs.createIndex("i2",IndexType.FUNCTIONAL, "p.ID","/portfolio1 p" );
    qs.createIndex("i3",IndexType.FUNCTIONAL, "p.position1.secId","/portfolio1 p" );
    qs.createIndex("i4",IndexType.FUNCTIONAL, "key.ID","/portfolio1.keys key" );
    qs.createIndex("i5",IndexType.FUNCTIONAL, "key.status","/portfolio1.keys key" );
    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        
        
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length,true,queries);
  }
  
  
  public void testOrderedResultsPartitionedRegion_Bug43514_2() throws Exception {
    String queries[] = {
        // Test case No. IUMR021
      "select distinct status as st from /portfolio1 where ID > 0 order by status",
        "select distinct p.status as st from /portfolio1 p where ID > 0 and status = 'inactive' order by p.status",   
        "select distinct p.position1.secId as st from /portfolio1 p where p.ID > 0 and p.position1.secId != 'IBM' order by p.position1.secId",        
        "select distinct  key.status as st from /portfolio1 key where key.ID > 5 order by key.status",
        "select distinct key.ID,key.status as st from /portfolio1 key where key.status = 'inactive' order by key.status desc, key.ID",
        "select distinct  status, ID from /portfolio1 order by status",
            "select distinct  p.status, p.ID from /portfolio1 p order by p.status",      
            "select distinct p.position1.secId, p.ID from /portfolio1 p order by p.position1.secId",            
            "select distinct p.status, p.ID from /portfolio1 p order by p.status asc, p.ID",
            
            "select distinct p.ID from /portfolio1 p, p.positions.values order by p.ID",
            
            "select distinct * from /portfolio1 p, p.positions.values order by p.ID",
            "select distinct p.iD, p.status from /portfolio1 p order by p.iD",
            "select distinct iD, status from /portfolio1 order by iD",
            "select distinct * from /portfolio1 p order by p.getID()",
            "select distinct * from /portfolio1 p order by p.getP1().secId",
            "select distinct  p.position1.secId  as st from /portfolio1 p order by p.position1.secId",
           
            "select distinct p, pos from /portfolio1 p, p.positions.values pos order by p.ID",
            "select distinct p, pos from /portfolio1 p, p.positions.values pos order by pos.secId",          
            "select distinct status from /portfolio1 where ID > 0 order by status",
            "select distinct p.status as st from /portfolio1 p where ID > 0 and status = 'inactive' order by p.status",      
            "select distinct p.position1.secId as st from /portfolio1 p where p.ID > 0 and p.position1.secId != 'IBM' order by p.position1.secId"        
            
    };
    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    AttributesFactory af = new AttributesFactory();
    af.setPartitionAttributes(paf.create());
    Region r1 = CacheUtils.createRegion("portfolio1", af.create(),false);

    for (int i = 0; i < 50; i++) {
      
      r1.put(i + "", new Portfolio(i));
    }

    
    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute();
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    // Create Indexes
    qs.createIndex("i1",IndexType.FUNCTIONAL, "p.status","/portfolio1 p" );
    qs.createIndex("i2",IndexType.FUNCTIONAL, "p.ID","/portfolio1 p" );
    qs.createIndex("i3",IndexType.FUNCTIONAL, "p.position1.secId","/portfolio1 p" );
   
    
    
    // Execute Queries with Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        QueryObserverImpl observer = new QueryObserverImpl();
        QueryObserverHolder.setInstance(observer);
        r[i][1] = q.execute();
        
        
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
    StructSetOrResultsSet ssOrrs = new StructSetOrResultsSet();
    ssOrrs.CompareQueryResultsWithoutAndWithIndexes(r, queries.length,true,queries);
  }
  
  
  public void testBug() throws Exception {
    
    String queries[] = {"SELECT DISTINCT * FROM /test WHERE id < $1 ORDER BY $2" };
    Object r[][] = new Object[queries.length][2];
    QueryService qs;
    qs = CacheUtils.getQueryService();
    Position.resetCounter();
    // Create Regions
    PartitionAttributesFactory paf = new PartitionAttributesFactory();
    AttributesFactory af = new AttributesFactory();
   // af.setPartitionAttributes(paf.create());
    Region r1 = CacheUtils.createRegion("test", af.create(),false);
   
    for (int i=0; i<100; i++) {
      r1.put("key-"+i, new TestObject(i, "ibm"));
    }

    
    // Execute Queries without Indexes
    for (int i = 0; i < queries.length; i++) {
      Query q = null;
      try {
        q = CacheUtils.getQueryService().newQuery(queries[i]);
        CacheUtils.getLogger().info("Executing query: " + queries[i]);
        r[i][0] = q.execute(new Object[]{new Integer(101),"id"});
        assertEquals(100, ((SelectResults)r[i][0]).size());
      } catch (Exception e) {
        e.printStackTrace();
        fail(q.getQueryString());
      }
    }
   
    
   
  }
  
  class QueryObserverImpl extends QueryObserverAdapter {
    boolean isIndexesUsed = false;

    ArrayList indexesUsed = new ArrayList();

    String indexName;
    private boolean limitAppliedAtIndex = false;

    public void beforeIndexLookup(Index index, int oper, Object key) {
      indexName = index.getName();
      indexesUsed.add(index.getName());
    }

    public void afterIndexLookup(Collection results) {
      if (results != null) {
        isIndexesUsed = true;
      }
    }
    
    public void limitAppliedAtIndexLevel(Index index, int limit , Collection indexResult){
      this.limitAppliedAtIndex = true;
    }
    
  }

  public static class TestObject implements DataSerializable {
    protected String _ticker;
    protected int _price;
    public int id;
    public int important;
    public int selection;
    public int select;

    public TestObject() {}

    public TestObject(int id, String ticker) {
      this.id = id;
      this._ticker = ticker;
      this._price = id;
      this.important = id;
      this.selection =id;
      this.select =id;
    }

    public int getId() {
      return this.id;
    }

    public String getTicker() {
      return this._ticker;
    }

    public int getPrice() {
      return this._price;
    }

    public void toData(DataOutput out) throws IOException
    {
      //System.out.println("Is serializing in WAN: " + GatewayEventImpl.isSerializingValue());
      out.writeInt(this.id);
      DataSerializer.writeString(this._ticker, out);
      out.writeInt(this._price);
    }

    public void fromData(DataInput in) throws IOException, ClassNotFoundException
    {
      //System.out.println("Is deserializing in WAN: " + GatewayEventImpl.isDeserializingValue());
      this.id = in.readInt();
      this._ticker = DataSerializer.readString(in);
      this._price = in.readInt();
    }

    @Override
    public String toString() {
      StringBuffer buffer = new StringBuffer();
      buffer
          .append("TestObject [")
          .append("id=")
          .append(this.id)
          .append("; ticker=")
          .append(this._ticker)
          .append("; price=")
          .append(this._price)
          .append("]");
      return buffer.toString();
    }

    @Override
    public boolean equals(Object o){
//      getLogWriter().info("In TestObject.equals()");
      TestObject other = (TestObject)o;
      if ((id == other.id) && (_ticker.equals(other._ticker))) {
        return true;
      } else {
//        getLogWriter().info("NOT EQUALS");
        return false;
      }
    }

    @Override
    public int hashCode(){
      return this.id;
    }
  }
}
