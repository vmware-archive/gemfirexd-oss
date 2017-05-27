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
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.facets.lang.Employee;
import java.util.*;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
//import com.gemstone.gemfire.internal.util.DebuggerSupport;


public class TestQuery extends TestCase{
  
  public TestQuery(String testName) {
    super(testName);
  }
  
  public static Test suite(){
    TestSuite suite = new TestSuite(TestQuery.class);
    return suite;
  }
  
  protected void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
  }
  
  protected void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }
  
  static void printResults(Object result) {
    if(result instanceof Collection){
      Collection r = (Collection)result;
      int size = r.size();
      CacheUtils.log("size=" + size);
      CacheUtils.log("--------------------");
      
      Iterator i = r.iterator();
      while(i.hasNext()) {
        Employee e = (Employee)i.next();
        CacheUtils.log(e);
      }
    }else{
      CacheUtils.log(result);
    }
    CacheUtils.log("\n");
  }
  
  public void testQuery()  throws Throwable {
    Region region = CacheUtils.createRegion("Employees",Employee.class);
    QueryService queryService = CacheUtils.getQueryService();
    CacheUtils.log("indexing path name");
    queryService.createIndex("nameIndex", IndexType.FUNCTIONAL, "name", "/Employees");
    CacheUtils.log("indexing path f");
    queryService.createIndex("fIndex", IndexType.FUNCTIONAL, "f", "/Employees");
    CacheUtils.log("populating...");
    
    for (int i = 0; i < 100; i++) {
      Employee emp = i % 2 == 0 ? new Employee() : new DerivedEmployee();
      //emp.collect = set;
      emp.empId = i;
      region.put("emp"+i,emp);
    }
    
    // test no namespace
    CacheUtils.log("No Namespace:");
    printResults((queryService.newQuery("select distinct * from $1 where salary > 100000")).execute(new Object[]  { region }));
    
    CacheUtils.log("Object Not Found");
    try {
      printResults((queryService.newQuery("select distinct * from emps where salary > 100000")).execute());
      CacheUtils.log("Did not throw exception");
      System.exit(0);
    } catch(Exception e) {
      CacheUtils.log("Properly threw exception");
    }
    
    
    
    // test the query methods defined in Queriable
    CacheUtils.log("Testing: query(\"salary > 100000\")");
    printResults(region.query("salary > 100000"));
    
    //        CacheUtils.log("...selectElement: salary > 100000");
    //        CacheUtils.log(set.selectElement("salary > 100000"));
    //        CacheUtils.log("...select: salary > 100000");
    //        CacheUtils.log(set.select("salary > 100000"));
    CacheUtils.log("...existsElement: salary > 100000");
    CacheUtils.log(""+region.existsValue("this.salary > 100000"));
    
    
    //InputStream in = new FileInputStream(filename);
    String queryString;
    for(int i=0;i<queries.length;i++){
      queryString = queries[i];
      queryString = queryString.trim();
      if (queryString.length() == 0 || queryString.startsWith("#"))  // comment
        continue;
      
      CacheUtils.log("Running query on : " + queryString);
      Query query = queryService.newQuery(queryString);
      Object results=null;
//      if (i == 1) DebuggerSupport.waitForJavaDebugger(CacheUtils.getLogger());
      results = query.execute(new Object[] { new Integer(100000), region });
      printResults(results);
    }
  }
  
  
  static String queries[]={
    "#select distinct * from /Employees where testMethod(null, 'hello') > 100000",
            "select distinct * from /Employees where not (select distinct * from collect).isEmpty",
            "42 or true",
            "#select distinct * from /Employees where element(select distinct * from /Employees e where e.empId = 1).name = 'A'",
            "select distinct * from /Employees where address = null",
            "select distinct * from /Employees where fieldAddress = null",
            "select distinct * from /Employees where address() = null",
            "select distinct * from /Employees -- this is a comment",
            "select distinct * from /Employees where f = 0.9F",
            "select distinct * from /Employees where f <= 0.9F;",
            "select distinct * from /Employees where 0.9F >= f;",
            "select distinct * from /Employees where d = 0.1;",
            "select distinct * from /Employees where 1 > d; ",
            "select distinct * from /Employees where false",
            "select distinct * from /Employees where true",
            "select distinct * from /Employees where name = 'Bob'",
            "select distinct * from /Employees where salary < 10000 and name = 'Bob'",
            "#select distinct * from /Employees where emps.salary <= 100000",
            "#select distinct * from /Employees WHERE salary < 100000 AND emps.name = 'Bob'",
            "select distinct * from /Employees where 100000 < salary OR name = 'Bob'",
            "select distinct * from /Employees where f = 0.9F or name = 'Bob'",
            "select distinct * from /Employees where salary > 100000 and name = 'Bob'",
            "select distinct * from /Employees where not(salary > 100000 and name = 'Bob')",
            "select distinct * from /Employees where salary <= 100000",
            "select distinct * from /Employees where 100000 > salary",
            "select distinct * from /Employees where salary >= 100000",
            "select distinct * from /Employees where salary = 100000",
            "select distinct * from /Employees where salary <> 100000",
            "select distinct * from /Employees where not (salary > 100000)",
            "select distinct * from /Employees /* this is a comment */ where not (salary > 100000)",
            "(select distinct * from /Employees where hireDate.getTime > 5.0e18).iterator",
            "select distinct * from /Employees where hireDate.getTime > 5.0e18",
            "select distinct * from /Employees where hireDate > hireDate.getClass.newInstance",
            "select distinct * from /Employees where name = 'A'.concat('dam')",
            "select distinct * from /Employees where name = 'Ada'.concat('m');",
            "select distinct * from /Employees where name.startsWith('Bo')",
            "select distinct * from /Employees emps where emps.testMethod(5,'x') > 100000",
            "select distinct * from /Employees where testMethod(5,'x') > 100000",
            "select distinct * from /Employees where testMethod(99999999999L, 'y') > 100000",
            "select distinct * from /Employees where testMethod(1.0F, 'z') > 100000;",
            "#select distinct * from /Employees where testMethod(-1.0e10F, 'z') > 100000",
            "select distinct * from /Employees where testMethod(1.0e-67, 'a') > 100000",
            "select distinct * from /Employees where testMethod(0, 'b') > 100000",
            "select distinct * from /Employees where testMethod(DATE '2000-01-07', 'c') > 100000",
            "select distinct * from /Employees where testMethod(date '1999-12-31', 'd') > 100000",
            "select distinct * from /Employees where testMethod(TIME '12:00:0', 'e') > 100000",
            "select distinct * from /Employees where testMethod(TIMEstAmp '2000-01-07 12:00:0.5', 'e') > 100000",
            "select distinct * from /Employees where testMethod(TIMESTAMP '1776-07-04 16:22:32.101010101', 'e') > 100000",
            "select distinct * from /Employees where testMethod(TIMESTAMP '1776-07-04 16:22:32.101910101', 'e') > 100000",
            "select distinct * from /Employees where NOT name.startsWith('Bo')",
            "select distinct * from /Employees where name='Bob'",
            "select distinct * from /Employees where address = undefined",
            "select distinct * from /Employees where address = null",
            "select distinct * from /Employees where address.postalCode = '97006'",
            "select distinct * from /Employees where salary > $1",
            "select distinct * from $2 where salary > $1",
            "select distinct * from /Employees where name.charAt(0) = char 'B'",
            "select distinct * from /Employees where name.charAt(0) = char 'R'",
            "select distinct * from /Employees where is_undefined(address.postalCode)",
            "select distinct * from /Employees where is_defined(address.postalCode)",
            "select distinct * from /Employees where address <> null",
            "select distinct * from /Employees where address() <> null",
            "select distinct * from /Employees where address = null;",
            "select distinct * from /Employees where address.street = null",
            "select distinct * from /Employees where address.city() = 'Beaverton'"
  };
  
}
