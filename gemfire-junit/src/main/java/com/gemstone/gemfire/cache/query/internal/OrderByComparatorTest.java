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

package com.gemstone.gemfire.cache.query.internal;

import java.util.ArrayList;
import java.util.Iterator;


import junit.framework.TestCase;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.internal.types.StructTypeImpl;
import com.gemstone.gemfire.cache.query.internal.types.TypeUtils;
import com.gemstone.gemfire.cache.query.types.ObjectType;

/**
 * @author Yogesh Mahajan
 *
 */
public class OrderByComparatorTest  extends TestCase{

        public OrderByComparatorTest(String testName) {
           super(testName);
         }

         public void testOredrByComparator() throws Exception {

               ObjectType types[] = {TypeUtils.OBJECT_TYPE, TypeUtils.OBJECT_TYPE};
               String names[] = {"pf","pos"};
               StructTypeImpl structTypeImpl = new StructTypeImpl(names , types);

               SortedStructSet sss = new SortedStructSet(new OrderByComparator(structTypeImpl), structTypeImpl);

               String status[] = {"active", "inactive"};
               Boolean criteria[] = {new Boolean(false), new Boolean(true)};
               ArrayList list = null;
               for (int i=0; i<10 ; i++){
                       list = new ArrayList();
                       Object[] arr = new Object[2];

               arr[0] = status[i%1] ;
               arr[1] = criteria[i%2];
               list.add(arr);

               Portfolio ptf = new Portfolio(i);
                   Iterator pIter = ptf.positions.values().iterator();
                   while(pIter.hasNext()){
                     Object values[] = {ptf, pIter.next()};
                    // StructImpl s = new StructImpl(structTypeImpl, values);
                     ((OrderByComparator)sss.comparator()).orderByMap.put(values, list);
                     sss.addFieldValues(values);
                   }
               }
         }
}