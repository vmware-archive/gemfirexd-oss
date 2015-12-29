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
 * IUMTest.java
 *@author vikramj
 *@ TASK IUM 4 & IUM 3
 * Created on April 29, 2005, 10:14 AM
 */

package com.gemstone.gemfire.cache.query.functional;
import com.gemstone.gemfire.cache.Region;
import junit.framework.TestCase;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.data.Position;
import com.gemstone.gemfire.cache.query.internal.QueryObserverAdapter;
import com.gemstone.gemfire.cache.query.internal.QueryObserverHolder;
import com.gemstone.gemfire.cache.query.types.StructType;
import java.util.*;

public class IUMTest extends TestCase{
    StructType resType1=null;
    StructType resType2= null;
    StructType resType3= null;
    
    String[] strg1 = null;
    String[] strg2= null;
    String[] strg3= null;
    
    int resSize1=0;
    int resSize2=0;
    int resSize3=0;
    
    Object valPf1=null;
    Object valPos1=null;
    
    Object valPf2=null;
    Object valPos2=null;
    
    Object valPf3=null;
    Object valPos3=null;
    
    Iterator itert1=null;
    Iterator itert2=null;
    Iterator itert3=null;
    
    Set set1=null;
    Set set2=null;
    Set set3=null;
       
    boolean isActive1=false;
    boolean isActive2=false;
    boolean isActive3=true;
    
    public IUMTest(String testName){
        super(testName);
    }
    
    protected void setUp() throws java.lang.Exception {
        CacheUtils.startCache();
    }
    
    protected void tearDown() throws java.lang.Exception {
        CacheUtils.closeCache();
    }
    
    public void testComparisonBetnWithAndWithoutIndexCreation() throws Exception {
        
        Region region = CacheUtils.createRegion("pos", Portfolio.class);
        
        for(int i=0;i<4;i++){
            region.put(""+i, new Portfolio(i));
        }
        QueryService qs;
        qs = CacheUtils.getQueryService();
        String queries[] = {
            "SELECT DISTINCT * FROM /pos,  positions.values where status='active'"
                    //TASK IUM4
        };
        SelectResults r[][] = new SelectResults[queries.length][2];
        for (int i = 0; i < queries.length; i++) {
            Query q = null;
            try {
                q = CacheUtils.getQueryService().newQuery(queries[i]);
                QueryObserverImpl observer = new QueryObserverImpl();
                QueryObserverHolder.setInstance(observer);
                r[i][0] = (SelectResults)q.execute();
                
                if(!observer.isIndexesUsed){
                    System.out.println("NO INDEX USED");
                }               
            } catch (Exception e) {
                e.printStackTrace();
                fail(q.getQueryString());
            }
        }
        
        //  Create an Index on status and execute the same query again.
        
        qs = CacheUtils.getQueryService();
        qs.createIndex("statusIndex", IndexType.FUNCTIONAL,"status","/pos");
        
        for (int i = 0; i < queries.length; i++) {
            Query q = null;
            try {
                q = CacheUtils.getQueryService().newQuery(queries[i]);
                QueryObserverImpl observer2 = new QueryObserverImpl();
                QueryObserverHolder.setInstance(observer2);
                r[i][1] = (SelectResults)q.execute();
                
                if(observer2.isIndexesUsed){
                    System.out.println("YES INDEX IS USED!");
                } else {
                    fail("Index NOT Used");
                }               
                   
            } catch (Exception e) {
                e.printStackTrace();
                fail(q.getQueryString());
            }
        }
        CacheUtils.compareResultsOfWithAndWithoutIndex(r, this);
        // BUG : Types are not Equal in both the cases as when Indexes are used the Iterator Names
        //       are getting Overwritten as iter1,iter2 and so on instead of the complied values of the iterator names used in the Query.
        
//        if ((resType1).equals(resType2)){
//            System.out.println("Both Search Results are of the same Type i.e.--> "+resType1);
//        }else {
//            fail("FAILED:Search result Type is different in both the cases");
//        }
//        if (resSize1==resSize2 || resSize1 != 0 ){
//            System.out.println("Both Search Results are non-zero and of Same Size i.e.  Size= "+resSize1);
//        }else {
//            fail("FAILED:Search result Type is different in both the cases");
//        }
//        
   }
    public void testWithOutIndexCreatedMultiCondQueryTest() throws Exception {
        Region region = CacheUtils.createRegion("portfolios", Portfolio.class);
        for(int i=0;i<4;i++){
            region.put(""+i, new Portfolio(i));
            // System.out.println(new Portfolio(i));
        }
        CacheUtils.getQueryService();
        
        String queries[] = {
            "SELECT DISTINCT * from /portfolios pf , pf.positions.values pos where pos.getSecId = 'IBM' and status = 'inactive'"
                    //TASK IUM3
        };
        for (int i = 0; i < queries.length; i++) {
            Query q = null;
            try {
                q = CacheUtils.getQueryService().newQuery(queries[i]);
                Object r3 = q.execute();
                System.out.println(Utils.printResult(r3));
                resType3 =(StructType)((SelectResults)r3).getCollectionType().getElementType();
                resSize3 =(((SelectResults)r3).size());
                //         System.out.println(resType3);
                strg3=resType3.getFieldNames();
                //         System.out.println(strg3[0]);
                //         System.out.println(strg2[1]);
                
                set3=(((SelectResults)r3).asSet());
                Iterator iter=set3.iterator();
                while (iter.hasNext()){
                    Struct stc3=(Struct)iter.next();
                    valPf2=stc3.get(strg3[0]);
                    valPos2=stc3.get(strg3[1]);
                    isActive3=((Portfolio)stc3.get(strg3[0])).isActive();
                    //        System.out.println(valPf2);
                    //        System.out.println(valPos2);
                }
                
            } catch (Exception e) {
                e.printStackTrace();
                fail(q.getQueryString());
            }
        }
        
        itert3 = set3.iterator();
        while (itert3.hasNext()){
            Struct stc3 = (Struct)itert3.next();
            if(!((Position)stc3.get(strg3[1])).secId.equals("IBM"))
                fail("FAILED:  secId found is not IBM");
            if (((Portfolio)stc3.get(strg3[0])).isActive() != false)
                fail("FAILED:Portfolio in Search result is Active");
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
