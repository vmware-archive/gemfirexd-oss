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
package com.pivotal.gemfirexd.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.PartitionResolver;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdRangePartitionResolver;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.SQLInteger;

public class RangePartitionResolverTest extends JdbcTestBase {

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(RangePartitionResolverTest.class));
  }

  public RangePartitionResolverTest(String name) {
    super(name);
  }

  public void testRange_bug42950() throws SQLException, StandardException {
    // Create a schema
    Connection conn = getConnection();      
    Statement s = conn.createStatement();
    s.execute("create schema trade");
    s.execute("create table trade.portfolio (cid int not null, " +
    		"sid int not null)  " +
    		"partition by range (cid) " +
    		"(VALUES BETWEEN 0 AND 1666, VALUES BETWEEN 1666 AND 3332, " +
    		"VALUES BETWEEN 3332 AND 4998, VALUES BETWEEN 4998 AND 6664, " +
    		"VALUES BETWEEN 6664 AND 8330, VALUES BETWEEN 8330 AND 10000)  BUCKETS 7  REDUNDANCY 3");

    GfxdRangePartitionResolver erpr = (GfxdRangePartitionResolver)((PartitionedRegion)Misc
        .getGemFireCache().getRegion("/TRADE/PORTFOLIO")).getPartitionResolver();
    
    Object[] robjs = erpr.getRoutingObjectsForRange(new SQLInteger(0), false, new SQLInteger(10000), false);
    assertNotNull(robjs);
    assertEquals(1, robjs[0]);
    assertEquals(2, robjs[1]);
    assertEquals(3, robjs[2]);
    assertEquals(4, robjs[3]);
    assertEquals(5, robjs[4]);
    assertEquals(6, robjs[5]);
  }
  
  public void testRanges() throws SQLException, StandardException {
    // Create a schema
    Connection conn = getConnection();      
    Statement s = conn.createStatement();
    s.execute("create schema EMP");

    // Get the cache;
    Cache cache = CacheFactory.getAnyInstance();

    // Check for PARTITION BY RANGE
    s.execute(
        "create table EMP.PARTITIONTESTTABLE (ID int not null, "
            + " DESCRIPTION varchar(1024) not null, primary key (ID))"
            + "PARTITION BY RANGE ( ID )"
            + " ( VALUES BETWEEN 20 and 40, VALUES BETWEEN 40 and 60, "
            + "VALUES BETWEEN 60 and 80, VALUES BETWEEN 90 and 100 )");

    Region regtwo = cache.getRegion("/EMP/PARTITIONTESTTABLE");  
    RegionAttributes rattr = regtwo.getAttributes();
    PartitionResolver pr = rattr.getPartitionAttributes().getPartitionResolver();
    GfxdRangePartitionResolver rpr = (GfxdRangePartitionResolver)pr;
    assertNotNull(rpr);
    
    DataValueDescriptor lowerBound = null;
    DataValueDescriptor upperBound = null;
    Object[] routingObjects = null;
    // case 1 - all inclusive bounds well inside.
    // array should contain 1, 2 and 3
    lowerBound = new SQLInteger(25);
    upperBound = new SQLInteger(75);
    routingObjects = rpr.getRoutingObjectsForRange(lowerBound, true,
        upperBound, true);
    assertEquals(3, routingObjects.length);
    assertEquals(1, routingObjects[0]);
    assertEquals(2, routingObjects[1]);
    assertEquals(3, routingObjects[2]);
    
    // case 2.1 - lower inclusive upper exclusive.
    // array should contain 1, 2 and 3
    routingObjects = rpr.getRoutingObjectsForRange(lowerBound, true, upperBound, false);
    assertEquals(3, routingObjects.length);
    assertEquals(1, routingObjects[0]);
    assertEquals(2, routingObjects[1]);
    assertEquals(3, routingObjects[2]);
    
    // case 2.2 - lower inclusive upper exclusive and one of the boundaries.
    // array should contain 1 and 2
    upperBound = new SQLInteger(60);
    routingObjects = rpr.getRoutingObjectsForRange(lowerBound, true, upperBound, false);
    assertEquals(2, routingObjects.length);
    assertEquals(1, routingObjects[0]);
    assertEquals(2, routingObjects[1]);
    // case 2.3 - lower inclusive upper exclusive and in between some range.
    // same as case 2.1

    // case 3.1 - lower exclusive upper inclusive.
    // array should contain 1, 2 and 3
    lowerBound = new SQLInteger(25);
    upperBound = new SQLInteger(75);
    routingObjects = rpr.getRoutingObjectsForRange(lowerBound, false, upperBound, true);
    assertEquals(3, routingObjects.length);
    assertEquals(1, routingObjects[0]);
    assertEquals(2, routingObjects[1]);
    assertEquals(3, routingObjects[2]);

    // case 3.2 - lower exclusive and one of the boundaries and upper inclusive.
    // array should contain 2 and 3
    lowerBound = new SQLInteger(40);
    upperBound = new SQLInteger(75);
    routingObjects = rpr.getRoutingObjectsForRange(lowerBound, true, upperBound, true);
    assertEquals(2, routingObjects.length);
    assertEquals(2, routingObjects[0]);
    assertEquals(3, routingObjects[1]);

    // case 3.3 - lower exclusive and in between some range upper inclusive.
    // array should contain 1, 2 and 3
    lowerBound = new SQLInteger(25);
    upperBound = new SQLInteger(75);
    routingObjects = rpr.getRoutingObjectsForRange(lowerBound, true, upperBound, true);
    assertEquals(3, routingObjects.length);
    assertEquals(1, routingObjects[0]);
    assertEquals(2, routingObjects[1]);
    assertEquals(3, routingObjects[2]);
   
    // case 4.1 - Both exclusive
    // array should contain null in this case as 15 to 20 not there and
    // so it is discontinuous.
    lowerBound = new SQLInteger(15);
    upperBound = new SQLInteger(75);
    routingObjects = rpr.getRoutingObjectsForRange(lowerBound, true, upperBound, true);
    assertNull(routingObjects);

    // case 4.2 - Both exclusive and boundary values.
    // array should contain only 2
    lowerBound = new SQLInteger(40);
    upperBound = new SQLInteger(60);
    routingObjects = ((GfxdRangePartitionResolver)pr)
        .getRoutingObjectsForRange(lowerBound, false, upperBound, false);
    assertEquals(1, routingObjects.length);
    assertEquals(2, routingObjects[0]);
    
    //Discontinuous range so null expected.
    lowerBound = new SQLInteger(40);
    upperBound = new SQLInteger(95);
    routingObjects = ((GfxdRangePartitionResolver)pr)
        .getRoutingObjectsForRange(lowerBound, true, upperBound, true);
    assertNull(routingObjects);
    
    //When lower bound and upper bound are in same range and well within.
    lowerBound = new SQLInteger(20);
    upperBound = new SQLInteger(30);
    routingObjects = ((GfxdRangePartitionResolver)pr)
        .getRoutingObjectsForRange(lowerBound, true, upperBound, true);
    assertEquals(1, routingObjects.length);
    assertEquals(1, routingObjects[0]);
    
    // uupper = lower and a boundary condition for bug #39530
    lowerBound = new SQLInteger(60);
    upperBound = new SQLInteger(60);
    routingObjects = ((GfxdRangePartitionResolver)pr)
        .getRoutingObjectsForRange(lowerBound, true, upperBound, true);
    assertEquals(1, routingObjects.length);
    assertEquals(3, routingObjects[0]);
    
    s.close();
    conn.close();
  }

  public void testRangesInfinity() throws SQLException, StandardException {
    Connection conn = getConnection();      
    Statement s = conn.createStatement();
    
    s.execute("create table EMP.PARTITIONTESTTABLE_ONE (ID int not null, "
        + " DESCRIPTION varchar(1024) not null, primary key (ID))"
        + "PARTITION BY RANGE ( ID )"
        + " ( VALUES BETWEEN -INFINITY and 40, VALUES BETWEEN 50 and +INFINITY )");
    
 // Get the cache;
    Cache cache = CacheFactory.getAnyInstance();

    Region regtwo = cache.getRegion("/EMP/PARTITIONTESTTABLE_ONE");  
    RegionAttributes rattr = regtwo.getAttributes();
    PartitionResolver pr = rattr.getPartitionAttributes().getPartitionResolver();
    GfxdRangePartitionResolver rpr = (GfxdRangePartitionResolver)pr;
    assertNotNull(rpr);
    
    DataValueDescriptor lowerBound = null;
    DataValueDescriptor upperBound = null;
    Object[] routingObjects = null;
    
    lowerBound = new SQLInteger(-10);
    upperBound = new SQLInteger(30);
    routingObjects = rpr.getRoutingObjectsForRange(lowerBound, true, upperBound, true);
    assertEquals(1, routingObjects.length);
    assertEquals(1, routingObjects[0]);

    lowerBound = new SQLInteger(10);
    upperBound = new SQLInteger(45);
    routingObjects = rpr.getRoutingObjectsForRange(lowerBound, true, upperBound, true);
    assertNull(routingObjects);
    
    lowerBound = new SQLInteger(40);
    upperBound = new SQLInteger(60);
    routingObjects = rpr.getRoutingObjectsForRange(lowerBound, true, upperBound, true);
    assertNull(routingObjects);
    
    lowerBound = new SQLInteger(50);
    upperBound = new SQLInteger(70);
    routingObjects = rpr.getRoutingObjectsForRange(lowerBound, true, upperBound, true);
    assertEquals(1, routingObjects.length);
    assertEquals(2, routingObjects[0]);
    
    // Few conditions where actually passing null as bounds. This means
    // actually passing -infinity and +infinity as bounds
    lowerBound = null;
    upperBound = new SQLInteger(30);
    routingObjects = rpr.getRoutingObjectsForRange(lowerBound, false, upperBound, true);
    assertEquals(1, routingObjects.length);
    assertEquals(1, routingObjects[0]);
    
    lowerBound = null;
    upperBound = new SQLInteger(45);
    routingObjects = rpr.getRoutingObjectsForRange(lowerBound, false, upperBound, true);
    assertNull(routingObjects);
    
    lowerBound = new SQLInteger(50);
    upperBound = null;
    routingObjects = rpr.getRoutingObjectsForRange(lowerBound, true, upperBound, false);
    assertEquals(1, routingObjects.length);
    assertEquals(2, routingObjects[0]);
    
    s.close();
    conn.close();
  }

  public void testRanges_colocated() throws SQLException, StandardException {
    // Create a schema
    Connection conn = getConnection();      
    Statement s = conn.createStatement();
    s.execute("create schema EMP");

    // Get the cache;
    Cache cache = CacheFactory.getAnyInstance();

    // Check for PARTITION BY RANGE
    s.execute(
        "create table EMP.PARTITIONTESTTABLE (ID int not null, "
            + " DESCRIPTION varchar(1024) not null, primary key (ID))"
            + "PARTITION BY RANGE ( ID )"
            + " ( VALUES BETWEEN 20 and 40, VALUES BETWEEN 40 and 60, "
            + "VALUES BETWEEN 60 and 80, VALUES BETWEEN 90 and 100 )");

    s.execute(
        "create table EMP.PARTITIONTESTTABLE_COLOCATED (ID int not null, "
            + " DESCRIPTION varchar(1024) not null, primary key (ID))"
            + "PARTITION BY RANGE ( ID )"
            + " ( VALUES BETWEEN 20 and 40, VALUES BETWEEN 40 and 60, "
            + "VALUES BETWEEN 60 and 80, VALUES BETWEEN 90 and 100 ) "
            + "COLOCATE WITH (EMP.PARTITIONTESTTABLE)");
    
    Region regtwo = cache.getRegion("/EMP/PARTITIONTESTTABLE_COLOCATED");  
    RegionAttributes rattr = regtwo.getAttributes();
    PartitionResolver pr = rattr.getPartitionAttributes().getPartitionResolver();
    GfxdRangePartitionResolver rpr = (GfxdRangePartitionResolver)pr;
    assertNotNull(rpr);
    
    DataValueDescriptor lowerBound = null;
    DataValueDescriptor upperBound = null;
    Object[] routingObjects = null;
    // case 1 - all inclusive bounds well inside.
    // array should contain 1, 2 and 3
    lowerBound = new SQLInteger(25);
    upperBound = new SQLInteger(75);
    routingObjects = rpr.getRoutingObjectsForRange(lowerBound, true,
        upperBound, true);
    assertEquals(3, routingObjects.length);
    assertEquals(1, routingObjects[0]);
    assertEquals(2, routingObjects[1]);
    assertEquals(3, routingObjects[2]);
    
    // case 2.1 - lower inclusive upper exclusive.
    // array should contain 1, 2 and 3
    routingObjects = rpr.getRoutingObjectsForRange(lowerBound, true, upperBound, false);
    assertEquals(3, routingObjects.length);
    assertEquals(1, routingObjects[0]);
    assertEquals(2, routingObjects[1]);
    assertEquals(3, routingObjects[2]);
    
    // case 2.2 - lower inclusive upper exclusive and one of the boundaries.
    // array should contain 1 and 2
    upperBound = new SQLInteger(60);
    routingObjects = rpr.getRoutingObjectsForRange(lowerBound, true, upperBound, false);
    assertEquals(2, routingObjects.length);
    assertEquals(1, routingObjects[0]);
    assertEquals(2, routingObjects[1]);
    // case 2.3 - lower inclusive upper exclusive and in between some range.
    // same as case 2.1

    // case 3.1 - lower exclusive upper inclusive.
    // array should contain 1, 2 and 3
    lowerBound = new SQLInteger(25);
    upperBound = new SQLInteger(75);
    routingObjects = rpr.getRoutingObjectsForRange(lowerBound, false, upperBound, true);
    assertEquals(3, routingObjects.length);
    assertEquals(1, routingObjects[0]);
    assertEquals(2, routingObjects[1]);
    assertEquals(3, routingObjects[2]);

    // case 3.2 - lower exclusive and one of the boundaries and upper inclusive.
    // array should contain 2 and 3
    lowerBound = new SQLInteger(40);
    upperBound = new SQLInteger(75);
    routingObjects = rpr.getRoutingObjectsForRange(lowerBound, true, upperBound, true);
    assertEquals(2, routingObjects.length);
    assertEquals(2, routingObjects[0]);
    assertEquals(3, routingObjects[1]);

    // case 3.3 - lower exclusive and in between some range upper inclusive.
    // array should contain 1, 2 and 3
    lowerBound = new SQLInteger(25);
    upperBound = new SQLInteger(75);
    routingObjects = rpr.getRoutingObjectsForRange(lowerBound, true, upperBound, true);
    assertEquals(3, routingObjects.length);
    assertEquals(1, routingObjects[0]);
    assertEquals(2, routingObjects[1]);
    assertEquals(3, routingObjects[2]);
   
    // case 4.1 - Both exclusive
    // array should contain null in this case as 15 to 20 not there and
    // so it is discontinuous.
    lowerBound = new SQLInteger(15);
    upperBound = new SQLInteger(75);
    routingObjects = rpr.getRoutingObjectsForRange(lowerBound, true, upperBound, true);
    assertNull(routingObjects);

    // case 4.2 - Both exclusive and boundary values.
    // array should contain only 2
    lowerBound = new SQLInteger(40);
    upperBound = new SQLInteger(60);
    routingObjects = ((GfxdRangePartitionResolver)pr)
        .getRoutingObjectsForRange(lowerBound, false, upperBound, false);
    assertEquals(1, routingObjects.length);
    assertEquals(2, routingObjects[0]);
    
    //Discontinuous range so null expected.
    lowerBound = new SQLInteger(40);
    upperBound = new SQLInteger(95);
    routingObjects = ((GfxdRangePartitionResolver)pr)
        .getRoutingObjectsForRange(lowerBound, true, upperBound, true);
    assertNull(routingObjects);
    
    //When lower bound and upper bound are in same range and well within.
    lowerBound = new SQLInteger(20);
    upperBound = new SQLInteger(30);
    routingObjects = ((GfxdRangePartitionResolver)pr)
        .getRoutingObjectsForRange(lowerBound, true, upperBound, true);
    assertEquals(1, routingObjects.length);
    assertEquals(1, routingObjects[0]);
    
    // uupper = lower and a boundary condition for bug #39530
    lowerBound = new SQLInteger(60);
    upperBound = new SQLInteger(60);
    routingObjects = ((GfxdRangePartitionResolver)pr)
        .getRoutingObjectsForRange(lowerBound, true, upperBound, true);
    assertEquals(1, routingObjects.length);
    assertEquals(3, routingObjects[0]);
    
    s.close();
    conn.close();
  }

  public void testRangesInfinity_colocated() throws SQLException, StandardException {
    Connection conn = getConnection();      
    Statement s = conn.createStatement();
    
    s.execute("create table EMP.PARTITIONTESTTABLE_ONE (ID int not null, "
        + " DESCRIPTION varchar(1024) not null, primary key (ID))"
        + "PARTITION BY RANGE ( ID )"
        + " ( VALUES BETWEEN -INFINITY and 40, VALUES BETWEEN 50 and +INFINITY )");
  
    s.execute(
        "create table EMP.PARTITIONTESTTABLE_ONE_COLOCATED (ID int not null, "
            + " DESCRIPTION varchar(1024) not null, primary key (ID))"
            + "PARTITION BY RANGE ( ID )"
            + " ( VALUES BETWEEN -INFINITY and 40, VALUES BETWEEN 50 and +INFINITY ) "
            + "COLOCATE WITH (EMP.PARTITIONTESTTABLE_ONE)");
 // Get the cache;
    Cache cache = CacheFactory.getAnyInstance();

    Region regtwo = cache.getRegion("/EMP/PARTITIONTESTTABLE_ONE_COLOCATED");  
    RegionAttributes rattr = regtwo.getAttributes();
    PartitionResolver pr = rattr.getPartitionAttributes().getPartitionResolver();
    GfxdRangePartitionResolver rpr = (GfxdRangePartitionResolver)pr;
    assertNotNull(rpr);
    
    DataValueDescriptor lowerBound = null;
    DataValueDescriptor upperBound = null;
    Object[] routingObjects = null;
    
    lowerBound = new SQLInteger(-10);
    upperBound = new SQLInteger(30);
    routingObjects = rpr.getRoutingObjectsForRange(lowerBound, true, upperBound, true);
    assertEquals(1, routingObjects.length);
    assertEquals(1, routingObjects[0]);

    lowerBound = new SQLInteger(10);
    upperBound = new SQLInteger(45);
    routingObjects = rpr.getRoutingObjectsForRange(lowerBound, true, upperBound, true);
    assertNull(routingObjects);
    
    lowerBound = new SQLInteger(40);
    upperBound = new SQLInteger(60);
    routingObjects = rpr.getRoutingObjectsForRange(lowerBound, true, upperBound, true);
    assertNull(routingObjects);
    
    lowerBound = new SQLInteger(50);
    upperBound = new SQLInteger(70);
    routingObjects = rpr.getRoutingObjectsForRange(lowerBound, true, upperBound, true);
    assertEquals(1, routingObjects.length);
    assertEquals(2, routingObjects[0]);
    
    // Few conditions where actually passing null as bounds. This means
    // actually passing -infinity and +infinity as bounds
    lowerBound = null;
    upperBound = new SQLInteger(30);
    routingObjects = rpr.getRoutingObjectsForRange(lowerBound, true, upperBound, true);
    assertEquals(1, routingObjects.length);
    assertEquals(1, routingObjects[0]);
    
    lowerBound = null;
    upperBound = new SQLInteger(45);
    routingObjects = rpr.getRoutingObjectsForRange(lowerBound, true, upperBound, true);
    assertNull(routingObjects);
    
    lowerBound = new SQLInteger(50);
    upperBound = null;
    routingObjects = rpr.getRoutingObjectsForRange(lowerBound, true, upperBound, true);
    assertEquals(1, routingObjects.length);
    assertEquals(2, routingObjects[0]);
    
    s.close();
    conn.close();
  }

  /**
   * Test for the case when updates are being done via a table scan.
   * (also see bug #40070)
   */
  public void testUpdateTableScan() throws Exception {
    setupConnection();

    Cache cache = Misc.getGemFireCache();
    // Check for PARTITION BY RANGE
    sqlExecute("create table EMP.PARTITIONTESTTABLE (ID int not null, "
        + " TID int not null)"
        + "PARTITION BY RANGE ( TID )"
        + " ( VALUES BETWEEN 20 and 40, VALUES BETWEEN 40 and 60, "
        + "VALUES BETWEEN 60 and 80, VALUES BETWEEN 90 and 100 )", false);

    Region reg = cache.getRegion("/EMP/PARTITIONTESTTABLE");
    RegionAttributes rattr = reg.getAttributes();
    PartitionResolver pr = rattr.getPartitionAttributes()
        .getPartitionResolver();
    GfxdRangePartitionResolver rpr = (GfxdRangePartitionResolver)pr;
    assertNotNull(rpr);

    String query1 = "select * from EMP.PARTITIONTESTTABLE";
    String query2 = "select * from EMP.PARTITIONTESTTABLE where ID = 5";
    String query3 = "select * from EMP.PARTITIONTESTTABLE where ID = 8";
    String query4 = "select * from EMP.PARTITIONTESTTABLE where ID = 10";

    // insert some values with duplicate IDs
    sqlExecute("insert into EMP.PARTITIONTESTTABLE values (4, 20), (5, 30), "
        + "(3, 40), (3, 5), (6, 30), (5, 200), (8, 5)", true);
    sqlExecuteVerifyText(query1, getResourcesDir()
        + "/lib/checkQuery.xml", "scan_query1", false, false);
    sqlExecuteVerifyText(query2, getResourcesDir()
        + "/lib/checkQuery.xml", "scan_query2", false, false);
    sqlExecuteVerifyText(query3, getResourcesDir()
        + "/lib/checkQuery.xml", "scan_query3", false, false);
    sqlExecuteVerifyText(query4, getResourcesDir()
        + "/lib/checkQuery.xml", "empty", false, false);

    // now do an update and check again
    sqlExecute("update EMP.PARTITIONTESTTABLE set ID = 5 where TID = 30",
        true);
    sqlExecuteVerifyText(query1, getResourcesDir()
        + "/lib/checkQuery.xml", "scan_query1_update", false, false);
    sqlExecuteVerifyText(query2, getResourcesDir()
        + "/lib/checkQuery.xml", "scan_query2_update", true, false);
    sqlExecuteVerifyText(query3, getResourcesDir()
        + "/lib/checkQuery.xml", "scan_query3", false, false);
    sqlExecuteVerifyText(query4, getResourcesDir()
        + "/lib/checkQuery.xml", "empty", false, false);

    // another update and check again
    sqlExecute("update EMP.PARTITIONTESTTABLE set ID = 10 where TID = 5",
        true);
    sqlExecuteVerifyText(query1, getResourcesDir()
        + "/lib/checkQuery.xml", "scan_query1_update2", true, false);
    sqlExecuteVerifyText(query2, getResourcesDir()
        + "/lib/checkQuery.xml", "scan_query2_update", true, false);
    sqlExecuteVerifyText(query3, getResourcesDir()
        + "/lib/checkQuery.xml", "empty", false, false);
    sqlExecuteVerifyText(query4, getResourcesDir()
        + "/lib/checkQuery.xml", "scan_query4_update", false, false);
  }
}
