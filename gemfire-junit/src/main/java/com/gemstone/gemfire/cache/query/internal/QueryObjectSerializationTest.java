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

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.Instantiator;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.internal.cache.CachePerfStats;
import com.gemstone.gemfire.cache.query.types.ObjectType;

import java.io.*;
import java.util.*;
import junit.framework.*;

/**
 * Tests the Serialization of the Query related class.
 *
 * @author kbanks
 * @since 3.0
 */
public class QueryObjectSerializationTest extends TestCase
  implements Serializable {
  /** A <code>ByteArrayOutputStream</code> that data is serialized to */
  private transient ByteArrayOutputStream baos;

  public QueryObjectSerializationTest(String name) {
    super(name);
  }

  ////////  Helper Class
  public static class SimpleObjectType implements ObjectType {
     public SimpleObjectType() {}
     public boolean isCollectionType() { return false; }
     public boolean isMapType() { return false; }
     public boolean isStructType() { return false; }
     public String getSimpleClassName() { return "java.lang.Object"; }
     public Class resolveClass() { return Object.class; }
     public void toData(DataOutput out) {}
     public void fromData(DataInput in) {}
     public boolean equals(Object o) { return o instanceof SimpleObjectType; }
  }

  ////////  Helper methods

  /**
   * Creates a new <code>ByteArrayOutputStream</code> for this test to
   * work with.
   */
  public void setUp() {
    this.baos = new ByteArrayOutputStream();
  }

  public void tearDown() {
    this.baos = null;
  }

  /**
   * Returns a <code>DataOutput</code> to write to
   */
  protected DataOutputStream getDataOutput() {
    return new DataOutputStream(this.baos);
  }

  /**
   * Returns a <code>DataInput</code> to read from
   */
  protected DataInputStream getDataInput() {
    ByteArrayInputStream bais =
      new ByteArrayInputStream(this.baos.toByteArray());
    return new DataInputStream(bais);
  }

  ////////  Test methods

  /**
   * Data serializes and then data de-serializes the given object and
   * asserts that the two objects satisfy o1.equals(o2)
   */
  private void checkRoundTrip(Object o1)
    throws IOException, ClassNotFoundException {

    DataOutputStream out = getDataOutput();
    DataSerializer.writeObject(o1, out);
    out.flush();
    DataInput in = getDataInput();
    assertEquals(o1, DataSerializer.<Object>readObject(in));
    this.baos = new ByteArrayOutputStream();
  }

  /**
   * Tests the serialization of many, but not all of the possible ResultSets
   */
  public void testSerializationOfQueryResults()
    throws IOException, ClassNotFoundException {
    Collection data = new java.util.ArrayList();
    data.add(null);
    data.add(null);
    data.add("some string");
    data.add(Long.MAX_VALUE);
    data.add(45);
    data.add(QueryService.UNDEFINED);

    ObjectType elementType = new SimpleObjectType();
    // Undefined
    checkRoundTrip(QueryService.UNDEFINED); 
    //ResultsBag
    ResultsBag rbWithoutData = new ResultsBag(); 
    rbWithoutData.setElementType(elementType); //avoid NPE in equals
    checkRoundTrip(rbWithoutData); 
    ResultsBag rbWithData = new ResultsBag(data, (CachePerfStats)null); 
    rbWithData.setElementType(elementType); //avoid NPE in equals
    checkRoundTrip(rbWithData); 
    /*
    Set rbWithoutDataAsSet = new ResultsBag().asSet(); 
    ResultsCollectionWrapper rcw = new ResultsCollectionWrapper(elementType, rbWithoutDataAsSet, -1);
    checkRoundTrip(rcw); 
    Set rbWithDataAsSet = new ResultsBag(data, (CachePerfStats)null).asSet(); 
    ResultsCollectionWrapper rcwWithData = new ResultsCollectionWrapper(elementType, rbWithDataAsSet, -1);
    checkRoundTrip(rcwWithData); 
    */
    //SortedResultSet
    SortedResultSet srsWithoutData = new SortedResultSet();
    srsWithoutData.setElementType(elementType); //avoid NPE in equals
    checkRoundTrip(srsWithoutData); 
    SortedResultSet srsWithData = new SortedResultSet();
    srsWithData.setElementType(elementType); //avoid NPE in equals
    checkRoundTrip(srsWithData); 

    //SortedStructSet
    //SortedStructSet sssWithoutData = new SortedStructSet();
    //checkRoundTrip(sssWithoutData); 

  }

  public static Test suite(){
    TestSuite suite = new TestSuite(QueryObjectSerializationTest.class);
    return suite;
  }

  public static void main(java.lang.String[] args) {
    junit.textui.TestRunner.run(suite());
  }
}
