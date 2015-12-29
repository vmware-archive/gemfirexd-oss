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
 * ResultsDataSerializabilityTest.java
 * JUnit based test
 *
 * Created on March 8, 2007
 */

package com.gemstone.gemfire.cache.query.functional;

import java.util.*;
import java.io.*;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.cache.query.data.*;
import com.gemstone.gemfire.cache.query.internal.*;
import com.gemstone.gemfire.cache.query.internal.types.*;
import com.gemstone.gemfire.*;
import com.gemstone.gemfire.internal.*;
import com.gemstone.gemfire.cache.*;

import junit.framework.*;

/**
 * Test whether query results are DataSerializable
 *
 * @author ezoerner
 */
public class ResultsDataSerializabilityTest extends TestCase {
  
  public ResultsDataSerializabilityTest(String testName) {
    super(testName);
  }
  
  protected void setUp() throws java.lang.Exception {
    CacheUtils.startCache();
  }
  
  protected void tearDown() throws java.lang.Exception {
    CacheUtils.closeCache();
  }
  
  /* In the absence of some kind of hook into the DataSerializer,
   * just test to see if the known implementation classes that are part of query
   * results implement the  DataSerializable interface
   */
  public void testImplementsDataSerializable() throws Exception {
    Class[] classes = new Class[] {
      SortedResultSet.class,
      ResultsCollectionWrapper.class,
      ResultsSet.class,
      SortedStructSet.class,
      StructImpl.class,
      StructSet.class,
      Undefined.class,
//      QRegion.class, // QRegions remain unserializable
      CollectionTypeImpl.class,
      MapTypeImpl.class,
      ObjectTypeImpl.class,
      StructTypeImpl.class,
    };
    
    List list = new ArrayList();
    for (int i = 0; i < classes.length; i++) {
      Class nextClass = classes[i];
      if (!DataSerializable.class.isAssignableFrom(nextClass)) {
        if (!DataSerializableFixedID.class.isAssignableFrom(nextClass)) {
          list.add(nextClass.getName());
        }
      }
    }
    
    assertTrue(list + " are not DataSerializable",
               list.isEmpty());
  }
  
  
/* test DataSerializability of a simple query result */
  public void testDataSerializability() throws Exception {
              
    Region region = CacheUtils.createRegion("Portfolios", Portfolio.class);
    for(int i = 0; i < 10000; i++) {
      region.put(i+"",new Portfolio(i));
    }
    
    String queryStr = "SELECT DISTINCT * FROM /Portfolios";
    Query q = CacheUtils.getQueryService().newQuery(queryStr);
    
    SelectResults res1 = (SelectResults)q.execute();
        
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);
    DataSerializer.writeObject(res1, out, false); // false prevents Java serialization
    out.close();
    
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));    
    SelectResults res2 = (SelectResults)DataSerializer.readObject(in);
    in.close();
    
    assertEquals(res2.size(), res1.size());
  }
  
}
