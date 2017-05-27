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
 * NegativeNumberQueriesTest.java
 *
 * Created on October 5, 2005, 2:44 PM
 */

package com.gemstone.gemfire.cache.query.functional;

/**
 *
 * @author  prafulla
 */

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.cache.query.data.*;
import com.gemstone.gemfire.cache.query.*;
import junit.framework.*;
import java.util.*;

public class NegativeNumberQueriesTest extends TestCase {
    
    /** Creates a new instance of NegativeNumberQueriesTest */
    public NegativeNumberQueriesTest(String testName) {
        super(testName);
    }//end of constructors
    
    static Cache cache;
    static Region region;
    static Index index;
    static DistributedSystem ds;
    static Properties props = new Properties();    
    static QueryService qs;     
    
    
    static int cnt = 1;
    
    /////////// create cache and regions in static block /////////
    static{
        try{
            ds = DistributedSystem.connect(props);
            cache = CacheFactory.create(ds);
            /*create region with to contain Portfolio objects*/
            
            AttributesFactory factory  = new AttributesFactory();            
            factory.setScope(Scope.DISTRIBUTED_ACK);            
            factory.setValueConstraint(Numbers.class);            
            factory.setIndexMaintenanceSynchronous(true);            
            region = cache.createRegion("numbers", factory.create());
            
        }catch(Exception e){
            e.printStackTrace();
        }
    }//end of static block
    
    protected void setUp() throws java.lang.Exception {
        
    }//end of setUp
    
    protected void tearDown() throws java.lang.Exception {
    }//end of tearDown
    
    
    public static Test suite(){
        TestSuite suite = new TestSuite(NegativeNumberQueriesTest.class);
        return suite;
    }//end of suite
    
    
    ///////////// test cases /////////////
    public void testBug33474() throws Exception {
        
        populateRegionsWithNumbers();
        //createIndexOnNumbers();
        
        QueryService qs;
        qs = cache.getQueryService();        
        String queryStr = "SELECT DISTINCT * FROM /numbers num WHERE num.id1 >= -200";
        Query q = qs.newQuery(queryStr);
        SelectResults rs = (SelectResults) q.execute();                
        System.out.println("--------------------- Size of Result Set is: -------------------------"+rs.size());
        
    }//end of testGetQueryTimes
    
    public void populateRegionsWithNumbers() throws Exception{
        System.out.println("--------------------- Populating Data -------------------------");
        for (int i = 0; i < 100; i++) {
            region.put(String.valueOf(i), new Numbers(i));
        }        
        for (int i = -100; i > -200; i--) {
            region.put(String.valueOf(i), new Numbers(i));
        }        
        System.out.println("--------------------- Data Populatio done -------------------------");
    }//end of populateRegions
    
    public void createIndexOnNumbers() throws Exception {        
            System.out.println("--------------------- Creating Indices -------------------------");
            QueryService qs;
            qs = cache.getQueryService();
            qs.createIndex("id", IndexType.FUNCTIONAL, "num.id", "/numbers num");
            qs.createIndex("id1", IndexType.FUNCTIONAL, "num.id1", "/numbers num");
            qs.createIndex("avg", IndexType.FUNCTIONAL, "num.max1", "/numbers num");
            qs.createIndex("l", IndexType.FUNCTIONAL, "num.l", "/numbers num");            
            System.out.println("--------------------- Index Creation Done -------------------------");            
    }// end of createIndex
    
    ////////// main method ///////////
    public static void main(java.lang.String[] args) {
        junit.textui.TestRunner.run(suite());
    }//end of main method  
    
}//end of NegativeNumberQueriesTest
