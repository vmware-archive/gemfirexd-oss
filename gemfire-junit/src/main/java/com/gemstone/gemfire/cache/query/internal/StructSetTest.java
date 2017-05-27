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
 * IndexCreationInternalsTest.java
 * JUnit based test
 *
 * Created on February 22, 2005, 11:24 AM
 */

package com.gemstone.gemfire.cache.query.internal;

import java.util.*;
import junit.framework.*;
import com.gemstone.gemfire.cache.query.*;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.types.*;
import com.gemstone.gemfire.cache.query.internal.types.*;

/**
 *
 * @author ericz
 */
public class StructSetTest extends TestCase {
  
  public StructSetTest(String testName) {
    super(testName);
  }
  
  
  public void testIntersectionAndRetainAll() {
    String names[] = {"p","pos"};
    ObjectType types[] = {TypeUtils.OBJECT_TYPE, TypeUtils.OBJECT_TYPE};
    StructTypeImpl sType = new StructTypeImpl(names, types);
    StructSet set1 = new StructSet(sType);
    Portfolio ptf = new Portfolio(0);
    Iterator pIter = ptf.positions.values().iterator();
    while(pIter.hasNext()){
      Object arr[] = {ptf, pIter.next()};
      set1.addFieldValues(arr);
    }
    
    StructSet set2 = new StructSet(sType);
    pIter = ptf.positions.values().iterator();
    while(pIter.hasNext()){
      Object arr[] = {ptf, pIter.next()};
      set2.addFieldValues(arr);
    }
    
    assertEquals(2, set1.size());
    assertEquals(2, set2.size());
    // tests that retainAll does not modify set1
    assertTrue(!set1.retainAll(set2));
    assertEquals(2, set1.size());
    assertEquals(2, set2.size());
    SelectResults sr = QueryUtils.intersection(set1, set2, null);
    assertEquals(2, sr.size());
  }  
}
