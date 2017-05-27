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
 * ComparisonOperatorsTest.java
 * JUnit based test
 *
 * Created on March 10, 2005, 3:14 PM
 */

package com.gemstone.gemfire.cache.query.functional;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.CacheUtils;
//import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import java.util.Collection;
import java.util.Iterator;
import junit.framework.*;

/**
 *
 * @author vaibhav
 */
public class ComparisonOperatorsTest extends TestCase {
  
  public ComparisonOperatorsTest(String testName) {
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
  
  String operators[] ={"=","<>","!=","<","<=",">",">="};
  // TODO add test methods here. The name must begin with 'test'. For example:
  // public void testHello() {}
  
  public void testCompareWithInt() throws Exception{
    String var = "ID";
    int value = 2;
    QueryService qs = CacheUtils.getQueryService();
    for(int i=0;i<operators.length;i++){
      Query query = qs.newQuery("SELECT DISTINCT * FROM /Portfolios where "+var+operators[i]+value);
      Object result = query.execute();
      if(result instanceof Collection){
        Iterator iter = ((Collection)result).iterator();
        while(iter.hasNext()){
          boolean isPassed = false;
          Portfolio p = (Portfolio)iter.next();
          switch(i){
            case 0 : isPassed = (p.getID() == value); break;
            case 1 : isPassed = (p.getID() != value); break;
            case 2 : isPassed = (p.getID() != value); break;
            case 3 : isPassed = (p.getID() < value); break;
            case 4 : isPassed = (p.getID() <= value); break;
            case 5 : isPassed = (p.getID() > value); break;
            case 6 : isPassed = (p.getID() >= value); break;
          }
          if(!isPassed)
            fail(this.getName()+" failed for operator "+operators[i]);
        }
      }else{
        fail(this.getName()+" failed for operator "+operators[i]);
      }
    }
  }
  
  public void testCompareWithString() throws Exception{
    String var = "P1.secId";
    String value = "DELL";
    QueryService qs = CacheUtils.getQueryService();
    for(int i=0;i<operators.length;i++){
      Query query = qs.newQuery("SELECT DISTINCT * FROM /Portfolios where "+var+operators[i]+"'"+value+"'");
      Object result = query.execute();
      if(result instanceof Collection){
        Iterator iter = ((Collection)result).iterator();
        while(iter.hasNext()){
          boolean isPassed = false;
          Portfolio p = (Portfolio)iter.next();
          switch(i){
            case 0 : isPassed = (p.getP1().getSecId().compareTo(value) == 0); break;
            case 1 : isPassed = (p.getP1().getSecId().compareTo(value) != 0); break;
            case 2 : isPassed = (p.getP1().getSecId().compareTo(value) != 0); break;
            case 3 : isPassed = (p.getP1().getSecId().compareTo(value) < 0); break;
            case 4 : isPassed = (p.getP1().getSecId().compareTo(value) <= 0); break;
            case 5 : isPassed = (p.getP1().getSecId().compareTo(value) > 0); break;
            case 6 : isPassed = (p.getP1().getSecId().compareTo(value) >= 0); break;
          }
          if(!isPassed)
            fail(this.getName()+" failed for operator "+operators[i]);
        }
      }else{
        fail(this.getName()+" failed for operator "+operators[i]);
      }
    }
  }
  
  public void testCompareWithNULL() throws Exception{
    String var ="P2";
    Object value = null;
    QueryService qs = CacheUtils.getQueryService();
    for(int i=0;i<operators.length;i++){
      Query query = qs.newQuery("SELECT DISTINCT * FROM /Portfolios where "+var+operators[i]+value);
      Object result = query.execute();
      if(result instanceof Collection){
        Iterator iter = ((Collection)result).iterator();
        while(iter.hasNext()){
          boolean isPassed = false;
          Portfolio p = (Portfolio)iter.next();
          switch(i){
            case 0 : isPassed = (p.getP2() == value); break;
            default : isPassed = (p.getP2() != value); break;
          }
          if(!isPassed)
            fail(this.getName()+" failed for operator "+operators[i]);
        }
      }else{
        fail(this.getName()+" failed for operator "+operators[i]);
      }
    }
  }
  
  public void testCompareWithUNDEFINED() throws Exception{
    String var = "P2.secId";
    QueryService qs = CacheUtils.getQueryService();
    for(int i=0;i<operators.length;i++){
      Query query = qs.newQuery("SELECT DISTINCT * FROM /Portfolios where "+var+operators[i]+" UNDEFINED");
      Object result = query.execute();
      if(result instanceof Collection){
        if(((Collection)result).size() != 0)
          fail(this.getName()+" failed for operator "+operators[i]);
      }else{
        fail(this.getName()+" failed for operator "+operators[i]);
      }
    }
  }
}
