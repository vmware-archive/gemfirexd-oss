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

package com.gemstone.gemfire.cache.query.facets.lang;
import com.gemstone.gemfire.cache.Region;
//import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import java.util.*;
import java.io.*;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TestQuery2 extends TestCase{
  static boolean _useJNDI = false;
  static boolean _random = true;
  static final String APP_NAME = "TestApp";
  
  
  public TestQuery2(String testName) {
    super(testName);
  }
  
  public static Test suite(){
    TestSuite suite = new TestSuite(TestQuery2.class);
    return suite;
  }
  
  
  static void printResults(Object result) {
    if(result instanceof Collection){
      Collection r = (Collection)result;
      int size = r.size();
      Utils.log("size=" + size);
      Utils.log("--------------------");
      
      Iterator i = r.iterator();
      while(i.hasNext()) {
        Employee e = (Employee)i.next();
        Utils.log(e);
      }
    }else{
      Utils.log(result);
    }
    Utils.log("\n");
  }
  
  public void testQuery() throws Throwable {
    Utils.installObserver();
    //Map namespace = null;
    QueryService queryService = Utils.getQueryService();
    
        /*Utils.log("getting connection...");
        Connection cxn = SessionFactory.getInstance().getSession();
        Utils.log("beginning txn...");
        cxn.begin();
        Utils.log("creating indexable set...");*/
    Region set = Utils.createRegion("Employees",Employee.class);//queryService.newIndexableSet(Employee.class);
    queryService.createIndex("nameIndex",IndexType.FUNCTIONAL,"name","/Employees");
    Utils.log("populating...");
    for (int i = 0; i < 100; i++){
      //set.add(new Employee());
      set.put("emp"+i,new Employee());
    }
    /*if (_useJNDI) {
      Utils.log("binding emps...");
      cxn.getInitialContext().rebind("emps", set);
      Utils.log("committing...");
      cxn.commit();
      Utils.log("beginning txn...");
      cxn.begin();
    } else {
      namespace = new HashMap();
      namespace.put("emps", set);
    }*/
    
    
    String str = "select distinct * from /Employees where name='Adam'";
    Query query = queryService.newQuery(str);
    
    Object results = null;
    int numQueries = 10000;
    for (int i = 0; i < numQueries; i++) {
      Utils.log("Running query " + i + " of 10000");
      results =  query.execute();
      //Commented out due to excessive logging, if uncommented 
      //junit will require additional memory for logging.
      //printResults(results);
      
    }
    
    
    /*if (_useJNDI)
      cxn.commit();
    cxn.close();
     */
  }
  
  
  private static String readln(InputStream in)
  throws Exception {
    StringBuffer buf = new StringBuffer();
    int ch;
    while (true) {
      ch = in.read();
      if (ch < 0)
        return null;
      if (ch == '\n' || ch == '\r') {
        String s = buf.toString().trim();
        if (s.length() == 0)
          return readln(in);
        return s;
      }
      buf.append((char)ch);
    }
  }
  
  
}
