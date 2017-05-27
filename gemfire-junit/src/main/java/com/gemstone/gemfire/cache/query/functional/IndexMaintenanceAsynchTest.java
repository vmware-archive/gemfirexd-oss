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
 * IndexMaintenanceAsynchTest.java
 *
 * Created on May 10, 2005, 5:26 PM
 */

package com.gemstone.gemfire.cache.query.functional;


import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.cache.query.internal.index.IndexProtocol;
import com.gemstone.gemfire.cache.query.data.Portfolio;

import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;

import io.snappydata.test.dunit.DistributedTestBase;
import io.snappydata.test.dunit.DistributedTestBase.WaitCriterion;

import java.util.ArrayList;
import java.util.Collection;


/**
 *
 * @author Ketan
 */
public class IndexMaintenanceAsynchTest extends TestCase {
    public IndexMaintenanceAsynchTest(String testName){
        super(testName);
    }
    
    protected void setUp() throws Exception {
        if(!isInitDone){
            init();
        }
        System.out.println("Running "+this.getName());
    }
    
    protected void tearDown() throws Exception {
    }
    
    public static Test suite(){
        TestSuite suite = new TestSuite(IndexMaintenanceAsynchTest.class);
        return suite;
    }
    
    static QueryService qs;
    static boolean isInitDone = false;
    static Region region;
    static IndexProtocol index;
    private static void init(){
        try{
            String queryString;
            Query query;
            Object result;
            Cache cache = CacheUtils.getCache();
            region = CacheUtils.createRegion("portfolios",Portfolio.class, false);
            for (int i = 0; i < 4; i++){
                region.put(""+i,new Portfolio(i));
            }
            qs = cache.getQueryService();
            index = (IndexProtocol)qs.createIndex("statusIndex", IndexType.FUNCTIONAL,"status","/portfolios");
            IndexStatistics stats = index.getStatistics();
            assertEquals(4, stats.getNumUpdates());
            
           // queryString= "SELECT DISTINCT * FROM /portfolios p, p.positions.values pos where pos.secId='IBM'";
            queryString= "SELECT DISTINCT * FROM /portfolios";
            query = CacheUtils.getQueryService().newQuery(queryString);
                     
            result = query.execute();
            System.out.println(Utils.printResult(result));
            
        }catch(Exception e){
            e.printStackTrace();
        }
        isInitDone = true;
    }
    
    public void testAddEntry() throws Exception {
        
        new NewThread(region, index);
        //assertEquals(5, stats.getNumberOfValues());
        Thread.sleep(12000);
    }
    
    
    class NewThread implements Runnable {
        String queryString;
        Query query;
        Object result;
        Thread t;
        Region region;
        IndexProtocol index;
        NewThread(Region region, IndexProtocol index) {
            t = new Thread(this,"Demo");
            this.region = region;
            this.index = index;
            t.setPriority(10);
            t.start();
        }
        public void run() {
            try {
                IndexStatistics stats = index.getStatistics();
                for (int i = 5; i < 9; i++){
                    region.put(""+i,new Portfolio(i));
                }
                final IndexStatistics st = stats;
                WaitCriterion ev = new WaitCriterion() {
                  public boolean done() {
                    return st.getNumUpdates() == 8;
                  }
                  public String description() {
                    return "index updates never became 8";
                  }
                };
                DistributedTestBase.waitForCriterion(ev, 5000, 200, true);
                
                //queryString= "SELECT DISTINCT * FROM /portfolios p, p.positions.values pos where pos.secId='IBM'";
                queryString= "SELECT DISTINCT * FROM /portfolios where status = 'active'";
                query = CacheUtils.getQueryService().newQuery(queryString);
                QueryObserverImpl observer = new QueryObserverImpl();
                QueryObserverHolder.setInstance(observer);
                
                result = query.execute();
                if(!observer.isIndexesUsed){
                    fail("NO INDEX USED");
                }
                System.out.println(Utils.printResult(result));
                if (((Collection)result).size() != 4 ) {
                    fail("Did not obtain expected size of result for the query");
                }
                // Task ID: IMA 1
                
            } catch (Exception e) {
                e.printStackTrace();
                
            }
        }
    }
    class QueryObserverImpl extends QueryObserverAdapter{
        boolean isIndexesUsed = false;
        ArrayList indexesUsed = new ArrayList();
        
        public void beforeIndexLookup(Index index, int oper, Object key) {
            indexesUsed.add(index.getName());
        }
        
        public void afterIndexLookup(Collection results) {
            if(results != null){
                isIndexesUsed = true;
            }
        }
    }
}
