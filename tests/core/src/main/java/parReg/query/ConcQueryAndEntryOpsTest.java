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
package parReg.query; 

//import event.*;
//import java.math.*;
import java.util.*;
//import objects.*;
//import query.QueryTest;
//import java.lang.reflect.*;
import util.*;
import hydra.*;
import query.*;
import parReg.query.index.IndexTest;
//import hydra.blackboard.*;
import com.gemstone.gemfire.cache.*;
//import com.gemstone.gemfire.internal.NanoTimer;
//import com.gemstone.gemfire.cache.query.Utils;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryInvocationTargetException;
import pdx.PdxTestVersionHelper;
/**
 * A version of the <code>QueryTask</code> that performs operations
 * on partitioned region.  
 */
public class ConcQueryAndEntryOpsTest extends QueryTest {
  private Object numOfQueriesLock = new Object();
  private int numOfQueries = 0;
  
  /* hydra task methods */
  /* ======================================================================== */
  public synchronized static void HydraTask_initialize() {
     if (queryTest == null) {
        queryTest = new ConcQueryAndEntryOpsTest();
        queryTest.initialize();
     }
  }
  
  public static void HydraTask_getNumOfQueries() {
    if (queryTest == null) {
      queryTest = new ConcQueryAndEntryOpsTest();
    }
    ((ConcQueryAndEntryOpsTest)queryTest).printNumOfQueries();
  }
  
  /* override methods */
  /* ======================================================================== */
  
  protected void printNumOfQueries(){
    Log.getLogWriter().info("Number of queries executed is " + numOfQueries);
  }
  
  protected void createIndex() {
    String objectType = TestConfig.tab().stringAt(QueryPrms.objectType, "");
    if(!objectType.equals("parReg.query.NewPortfolio")) {
        return;
    }
    IndexTest indexTest = new IndexTest();
    int regionNumber = (new Random()).nextInt(numOfRegions); 
    indexTest.createIndex(REGION_NAME + ("" + regionNumber));
 }

 protected void removeIndex() {
    String objectType = TestConfig.tab().stringAt(QueryPrms.objectType, "");
    if(!objectType.equals("parReg.query.NewPortfolio")) {
        return;
    }
    IndexTest indexTest = new IndexTest();
    int regionNumber = (new Random()).nextInt(numOfRegions); 
    indexTest.removeIndex(REGION_NAME + ("" + regionNumber));
 }  
  
  protected Object getObjectToAdd(String name) {
    
    boolean useRandomValues = false;
    useRandomValues =  TestConfig.tab().booleanAt(QueryPrms.useRandomValues, false);
    if (useRandomValues) { 
      BaseValueHolder anObj = new ValueHolder(name, randomValues);
      return anObj;
    } else {
      long i = NameFactory.getPositiveNameCounter();
      int index = (int)(i % maxObjects);
  
      NewPortfolio val = new NewPortfolio(name, index);
          
      Log.getLogWriter().info("set portfolio ");
      return val;
    }
  }
  
  /* ======================================================================== */
  protected Object getUpdateObject(String name) {
    return getObjectToAdd(name);
  }
  
  /* ======================================================================== */
  protected void doQuery(boolean logAddition) {
    String region1 = randRgn();
  
    String queries[] = { 
       "select distinct * from " + region1 + " where name = $1 ",   

       "select distinct * from " + region1 + " where status = 'active'",
       "select distinct * from " + region1 + " where id != 0 AND status = 'active'",        
       "select distinct * from " + region1 + " where id <= 1 AND status <> 'active'",
       "select distinct * from " + region1 + " where NOT (id <= 5 AND status <> 'active')",
       "select distinct * from " + region1 + " where id < 5 OR status != 'active'",      
       "import parReg.\"query\".Position; select distinct * from " + region1 + " r, " +
          "r.positions.values pVal TYPE Position where r.status = 'active' AND pVal.mktValue >= 25.00", 
       "import parReg.\"query\".Position; select distinct r.name, pVal, r.status from " + region1 + " r, " +
         "r.positions.values pVal TYPE Position where r.status = 'active' AND pVal.mktValue >= 25.00 AND pVal.qty >= 500",
        "select distinct * from " + randRgn() + ".keys",
         "select distinct e.value from " + randRgn() + ".entries e"
  
  //      The following queries are not supported in parReg -- for multiple regions reference.       
  //  "select distinct * from " + region1 + " p where p.id IN " + 
  //       "(select distinct np.id from " + randRgn() + " np where np.status = 'active')",        
  //      "select distinct * from " + randRgn() + ", " + randRgn(),        
  //      "select distinct * from " + randRgn() + ", " + randRgn() + ", " + randRgn(),               
    };
   
    //Add the limitClause if configured to do so
    if (queryLimit != -1) {
      for(int q =0; q < queries.length; q++) {
        queries[q] += " limit " + queryLimit;
      }       
    }  
    int i = ((new Random()).nextInt(queries.length));
    if (i == 0) {
      doQuery1(logAddition, region1);
      return;
    }
   
    Query query = CacheHelper.getCache().getQueryService().newQuery(queries[i]);
    if(logAddition) {
       Log.getLogWriter().info(" query = " + query);
    }   
    try {
       Object result = query.execute();
       validateQueryLimit(result); 
  //     Log.getLogWriter().info(Utils.printResult(result));
       if ((result instanceof Collection) && logAddition) {
         synchronized (numOfQueriesLock) {
           numOfQueries++;
           //Log.getLogWriter().info("number of queries executed is " + numOfQueries);
         }
         Log.getLogWriter().info("Size of result is :" + ((Collection)result).size());
         
       }
    } catch (Exception e) {
       throw new TestException("Caught exception during query execution" + TestHelper.getStackTrace(e));
    }
  
  }
  
  /*
   * This method tests query with param based on key (name) on partition region.
   * Will throw exception if result.size() is not 0 or 1.
   */
  protected void doQuery1(boolean logAddition, String region1) {
  
    String queryString = "select distinct * from " + region1 + " where name = $1 ";
  
    Query query = CacheHelper.getCache().getQueryService().newQuery(queryString);
    if(logAddition) {
       Log.getLogWriter().info(" query = " + query.toString());
    }   
    
    int regionNum = 0;            
    for (int i=0; i<numOfRegions; i++) {
      if (region1.equals("/" + REGION_NAME + i)) {
        regionNum = i;
        Log.getLogWriter().info(" region num = " + regionNum);
      }
    }
    //get the region thru region number
    Region aRegion = CacheHelper.getCache().getRegion(REGION_NAME + ("" + regionNum));
    
    Set keys = aRegion.keySet();  
    Iterator itr = keys.iterator();
    
    Object [] params = new Object[1];
    if (itr.hasNext()) {
      params[0] = itr.next();
      Log.getLogWriter().info("param $1 is " + params[0]);
      NewPortfolio aNP = (NewPortfolio) PdxTestVersionHelper.toBaseObject(aRegion.get(params[0]));
      Log.getLogWriter().info("Rgion name is " + aRegion.getName());
      Log.getLogWriter().info("The object in region with key " + params[0] + " is " + aNP);
      
  
      try {
         Object result = query.execute(params);
         validateQueryLimit(result); 
    //     Log.getLogWriter().info(Utils.printResult(result));
         if ((result instanceof Collection) && logAddition) {
           synchronized (numOfQueriesLock) {
             numOfQueries++;
             //Log.getLogWriter().info("number of queries executed is " + numOfQueries);
           }
           Log.getLogWriter().info("Size of result is :" + ((Collection)result).size());
           
           if (((Collection)result).size() > 1) 
             throw new TestException ("Query result when checking key (name) can not be more than 1");
             
         }
      } catch (QueryInvocationTargetException ite) {
         if (QueryPrms.allowQueryInvocationTargetException()) {
          // ignore for PR Query HA tests
          Log.getLogWriter().info("Caught " + ite + " (expected with concurrent execution); continuing with test");
         } else {
          throw new TestException("Caught exception during query execution" + TestHelper.getStackTrace(ite));
         }
      } catch (Exception e) {
         throw new TestException("Caught exception during query execution" + TestHelper.getStackTrace(e));
      }
      
      Log.getLogWriter().info("Finished checking query 1.");
    }
  
  }
    
}



