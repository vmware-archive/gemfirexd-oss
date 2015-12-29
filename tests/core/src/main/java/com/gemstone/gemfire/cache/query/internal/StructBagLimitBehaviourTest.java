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
/**
 * 
 */
package com.gemstone.gemfire.cache.query.internal;

import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.types.ObjectType;
import com.gemstone.gemfire.cache.query.types.StructType;
import com.gemstone.gemfire.cache.query.internal.types.*;

import java.util.Iterator;
import java.util.List;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test StructsBag Limit behaviour
 * 
 * @author Asif
 */
public class StructBagLimitBehaviourTest extends ResultsBagLimitBehaviourTest {

  public StructBagLimitBehaviourTest(String testName) {
    super(testName);
  }

  public static Test suite() {
    TestSuite suite = new TestSuite(StructBagLimitBehaviourTest.class);
    return suite;
  }

  public static void main(java.lang.String[] args) {
    junit.textui.TestRunner.run(suite());
  }

  public ResultsBag getBagObject(Class clazz) {
    ObjectType[] types = new ObjectType[] { new ObjectTypeImpl(clazz),
        new ObjectTypeImpl(clazz) };
    StructType type = new StructTypeImpl(new String[] { "field1", "field2" },
        types);
    return new StructBag(type, null);
  }

  public Object wrap(Object obj, ObjectType type) {
    StructTypeImpl stype = (StructTypeImpl)type;
    if (obj == null) {
      return new StructImpl(stype, null);
    }
    else {
      return new StructImpl(stype, new Object[] { obj, obj });
    }
  }

  public void testRemoveAllStructBagSpecificMthod() {
    StructBag bag1 = (StructBag)getBagObject(Integer.class);
    // Add Integer & null Objects
    bag1.add(wrap(null, bag1.getCollectionType().getElementType()));
    bag1.add(wrap(null, bag1.getCollectionType().getElementType()));
    bag1.add(wrap(new Integer(1), bag1.getCollectionType().getElementType()));
    bag1.add(wrap(new Integer(2), bag1.getCollectionType().getElementType()));
    bag1.add(wrap(new Integer(2), bag1.getCollectionType().getElementType()));
    bag1.add(wrap(new Integer(3), bag1.getCollectionType().getElementType()));
    bag1.add(wrap(new Integer(3), bag1.getCollectionType().getElementType()));
    bag1.add(wrap(new Integer(4), bag1.getCollectionType().getElementType()));
    bag1.applyLimit(4);
    StructBag bag2 = (StructBag)getBagObject(Integer.class);
    bag2.addAll(bag1);
    // Now remove the first element & it occurnece completelt from bag2
    Iterator itr2 = bag2.iterator();
    Struct first = (Struct)itr2.next();
    int occrnce = 0;
    while (itr2.hasNext()) {
      if (itr2.next().equals(first)) {
        itr2.remove();
        ++occrnce;
      }
    }
    assertTrue(bag1.removeAll(bag2));
    assertEquals(occrnce, bag1.size());
    Iterator itr = bag1.iterator();
    for (int i = 0; i < occrnce; ++i) {
      itr.next();
    }
    assertFalse(itr.hasNext());
  }

  public void testRetainAllStructBagSpecific() {
    StructBag bag1 = (StructBag)getBagObject(Integer.class);
    // Add Integer & null Objects
    // Add Integer & null Objects
    bag1.add(wrap(new Integer(1), bag1.getCollectionType().getElementType()));
    bag1.add(wrap(new Integer(2), bag1.getCollectionType().getElementType()));
    bag1.add(wrap(new Integer(2), bag1.getCollectionType().getElementType()));
    bag1.add(wrap(new Integer(3), bag1.getCollectionType().getElementType()));
    bag1.add(wrap(new Integer(3), bag1.getCollectionType().getElementType()));
    bag1.add(wrap(new Integer(4), bag1.getCollectionType().getElementType()));
    bag1.add(wrap(null, bag1.getCollectionType().getElementType()));
    bag1.add(wrap(null, bag1.getCollectionType().getElementType()));
    bag1.applyLimit(4);
    StructBag bag2 = (StructBag)getBagObject(Integer.class);
    bag2.addAll(bag1);
    // Now remove the first element & it occurnece completelt from bag2
    Iterator itr2 = bag2.iterator();
    Struct first = (Struct)itr2.next();
    int occrnce = 0;
    while (itr2.hasNext()) {
      if (itr2.next().equals(first)) {
        itr2.remove();
        ++occrnce;
      }
    }
    bag1.retainAll(bag2);
    assertEquals(4, bag1.size());
    Iterator itr = bag1.iterator();
    for (int i = 0; i < 4; ++i) {
      itr.next();
    }
    assertFalse(itr.hasNext());
  }

}
