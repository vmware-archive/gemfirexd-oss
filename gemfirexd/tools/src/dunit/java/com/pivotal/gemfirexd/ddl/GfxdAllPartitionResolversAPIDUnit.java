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
package com.pivotal.gemfirexd.ddl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.PartitionResolver;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdListPartitionResolver;
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdPartitionByExpressionResolver;
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdRangePartitionResolver;
import com.pivotal.gemfirexd.internal.iapi.types.DataTypeDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.SQLInteger;
import io.snappydata.test.dunit.SerializableRunnable;
import io.snappydata.test.dunit.VM;
import io.snappydata.test.util.TestException;

@SuppressWarnings("serial")
public class GfxdAllPartitionResolversAPIDUnit extends DistributedSQLTestBase {

  public GfxdAllPartitionResolversAPIDUnit(String name) {
    super(name);
  }

  void checkResolver(int[] clientNums, int[] serverNums, String verifyFucntion) {
    ArrayList<VM> vmlist = getVMs(clientNums, serverNums);
    assertNotNull(vmlist);
    assertTrue(vmlist.size() > 0);
    for (int i = 0; i < vmlist.size(); i++) {
      VM executeVM = vmlist.get(i);
      if (executeVM != null) {
        executeVM.invoke(GfxdAllPartitionResolversAPIDUnit.class,
            verifyFucntion);
      }
    }
  }

  public static void verifyRangeResolver() {
    Cache cache = CacheFactory.getAnyInstance();
    Region regtwo = cache.getRegion("/EMP/PARTITIONTESTTABLE");  
    RegionAttributes rattr = regtwo.getAttributes();
    PartitionResolver pr = rattr.getPartitionAttributes().getPartitionResolver();
    GfxdRangePartitionResolver rpr = (GfxdRangePartitionResolver)pr;
    assertNotNull(rpr);
    
    Integer robj = (Integer)rpr.getRoutingKeyForColumn(new SQLInteger(200));
    assertEquals(200, robj.intValue());
    robj = (Integer)rpr.getRoutingKeyForColumn(new SQLInteger(20));
    assertEquals(1, robj.intValue());
    robj = (Integer)rpr.getRoutingKeyForColumn(new SQLInteger(60));
    assertEquals(3, robj.intValue());
    
    assertNull(rpr.getRoutingObjectsForRange(new SQLInteger(10), true, new SQLInteger(20), true));
    Object [] robjs1 = rpr.getRoutingObjectsForRange(new SQLInteger(25), true, new SQLInteger(75), true);
    assertEquals(3, robjs1.length);
    
    Object [] robjs2 = rpr.getRoutingObjectsForList(new DataValueDescriptor[] {new SQLInteger(25), new SQLInteger(75)});
    assertEquals(2, robjs2.length);
    assertEquals(1, robjs2[0].hashCode());
    assertEquals(3, robjs2[1].hashCode());
    
    robjs2 = rpr.getRoutingObjectsForList(new DataValueDescriptor[] {new SQLInteger(15), new SQLInteger(75)});
    assertEquals(2, robjs2.length);
    assertEquals(15, robjs2[0].hashCode());
    assertEquals(3, robjs2[1].hashCode());
    
    robj = (Integer)rpr.getRoutingObjectsForPartitioningColumns(new DataValueDescriptor[] {new SQLInteger(0) });
    assertEquals(0, robj.hashCode());
    String[] arr = rpr.getColumnNames();
    assertEquals(1, arr.length);
    assertEquals("ID", arr[0]);
    
    String tbname = rpr.getTableName();
    assertEquals("PARTITIONTESTTABLE", tbname);
    String scname = rpr.getSchemaName();
    assertEquals("EMP", scname);
    String master = rpr.getMasterTable(false /* immediate master*/);
    assertEquals("/EMP/PARTITIONTESTTABLE", master);
    assertTrue(rpr.isUsedInPartitioning("ID"));
    assertEquals(0, rpr.getPartitioningColumnIndex("ID"));
    assertEquals(1, rpr.getPartitioningColumnsCount());
  }
  
  public void testRangePartitionResolver() throws Exception {
    // Start one client and one server
    startVMs(1, 2);

    clientSQLExecute(1, "create schema EMP");
    clientSQLExecute(1,
        "create table EMP.PARTITIONTESTTABLE (ID int not null, "
        + " ID2 int not null, primary key (ID))"
        + "PARTITION BY RANGE ( ID )"
        + " ( VALUES BETWEEN 20 and 40, VALUES BETWEEN 40 and 60, "
        + "VALUES BETWEEN 60 and 80, VALUES BETWEEN 90 and 100 )");

    clientSQLExecute(1,
        "insert into EMP.PARTITIONTESTTABLE values (1, 1), (2, 2), (3, 3) ");
    checkResolver(new int[] { 1 }, new int[] { 1 , 2 }, "verifyRangeResolver");
  }
  
  public static void verifyListResolver() {
    Cache cache = CacheFactory.getAnyInstance();
    Region regtwo = cache.getRegion("/EMP/PARTITIONTESTTABLE");  
    RegionAttributes rattr = regtwo.getAttributes();
    PartitionResolver pr = rattr.getPartitionAttributes().getPartitionResolver();
    GfxdListPartitionResolver rpr = (GfxdListPartitionResolver)pr;
    assertNotNull(rpr);

    final DataValueDescriptor notInList = new SQLInteger(200);
    DataTypeDescriptor dtd = DataTypeDescriptor
        .getBuiltInDataTypeDescriptor(Types.INTEGER);
    Integer robj = (Integer)rpr.getRoutingKeyForColumn(notInList);
    assertEquals(notInList.computeHashCode(dtd.getMaximumWidth(), 0),
        robj.intValue());
    robj = (Integer)rpr.getRoutingKeyForColumn(new SQLInteger(20));
    assertEquals(0, robj.intValue());
    robj = (Integer)rpr.getRoutingKeyForColumn(new SQLInteger(60));
    assertEquals(1, robj.intValue());
    
    assertNull(rpr.getRoutingObjectsForRange(new SQLInteger(10), true, new SQLInteger(20), true));
    assertNull(rpr.getRoutingObjectsForRange(new SQLInteger(25), true, new SQLInteger(75), true));
    
    Object[] onerobj = rpr.getRoutingObjectsForRange(new SQLInteger(25), true, new SQLInteger(25), true);
    assertEquals(1, onerobj.length);
    assertEquals(25, onerobj[0].hashCode());
    
    onerobj = rpr.getRoutingObjectsForRange(new SQLInteger(20), true, new SQLInteger(20), true);
    assertEquals(1, onerobj.length);
    assertEquals(0, onerobj[0].hashCode());
    
    Object [] robjs2 = rpr.getRoutingObjectsForList(new DataValueDescriptor[] {new SQLInteger(25), new SQLInteger(20)});
    assertEquals(2, robjs2.length);
    assertEquals(25, robjs2[0].hashCode());
    assertEquals(0, robjs2[1].hashCode());
    
    robj = (Integer)rpr.getRoutingObjectsForPartitioningColumns(new DataValueDescriptor[] {new SQLInteger(0) });
    assertEquals(0, robj.hashCode());
    
    robj = (Integer)rpr.getRoutingObjectsForPartitioningColumns(new DataValueDescriptor[] {new SQLInteger(40) });
    assertEquals(0, robj.hashCode());
    
    String[] arr = rpr.getColumnNames();
    assertEquals(1, arr.length);
    assertEquals("ID", arr[0]);
    
    String tbname = rpr.getTableName();
    assertEquals("PARTITIONTESTTABLE", tbname);
    String scname = rpr.getSchemaName();
    assertEquals("EMP", scname);
    String master = rpr.getMasterTable(false /* immediate master*/);
    assertEquals("/EMP/PARTITIONTESTTABLE", master);
    assertTrue(rpr.isUsedInPartitioning("ID"));
    assertEquals(0, rpr.getPartitioningColumnIndex("ID"));
    assertEquals(1, rpr.getPartitioningColumnsCount());
  }
  
  public void testListPartitionResolver() throws Exception {
    // Start one client and one server
    startVMs(1, 2);

    clientSQLExecute(1, "create schema EMP");
    clientSQLExecute(1,
        "create table EMP.PARTITIONTESTTABLE (ID int not null, "
        + " ID2 int not null, primary key (ID))"
        + "PARTITION BY LIST ( ID )"
        + " ( VALUES (20, 40), VALUES (50, 60) , "
        + "VALUES (70, 80), VALUES (90, 100 ))");

    clientSQLExecute(1,
        "insert into EMP.PARTITIONTESTTABLE values (1, 1), (2, 2), (3, 3) ");
    checkResolver(new int[] { 1 }, new int[] { 1 , 2 }, "verifyListResolver");
  }
  
  public static void verifyPartitionByColumnResolver() {
    Cache cache = CacheFactory.getAnyInstance();
    Region regtwo = cache.getRegion("/EMP/PARTITIONTESTTABLE");  
    RegionAttributes rattr = regtwo.getAttributes();
    PartitionResolver pr = rattr.getPartitionAttributes().getPartitionResolver();
    GfxdPartitionByExpressionResolver rpr = (GfxdPartitionByExpressionResolver)pr;
    assertNotNull(rpr);

    DataValueDescriptor dvd = new SQLInteger(200);
    Integer robj = (Integer)rpr.getRoutingKeyForColumn(dvd);
    assertEquals(GfxdPartitionByExpressionResolver.computeHashCode(dvd,
        null, 0), robj.intValue());
    dvd = new SQLInteger(20);
    robj = (Integer)rpr.getRoutingKeyForColumn(dvd);
    assertEquals(GfxdPartitionByExpressionResolver.computeHashCode(dvd,
        null, 0), robj.intValue());
    dvd = new SQLInteger(60);
    robj = (Integer)rpr.getRoutingKeyForColumn(new SQLInteger(60));
    assertEquals(GfxdPartitionByExpressionResolver.computeHashCode(dvd,
        null, 0), robj.intValue());

    assertNull(rpr.getRoutingObjectsForRange(new SQLInteger(10), true,
        new SQLInteger(20), true));
    assertNull(rpr.getRoutingObjectsForRange(new SQLInteger(25), true,
        new SQLInteger(75), true));

    dvd = new SQLInteger(25);
    Object[] onerobj = rpr.getRoutingObjectsForRange(new SQLInteger(25), true,
        dvd, true);
    assertEquals(1, onerobj.length);
    assertEquals(GfxdPartitionByExpressionResolver.computeHashCode(dvd,
        null, 0), onerobj[0].hashCode());

    dvd = new SQLInteger(20);
    Object[] robjs2 = rpr.getRoutingObjectsForList(new DataValueDescriptor[] {
        new SQLInteger(25), dvd });
    assertEquals(2, robjs2.length);
    assertEquals(GfxdPartitionByExpressionResolver.computeHashCode(
        new SQLInteger(25), null, 0), robjs2[0].hashCode());
    assertEquals(GfxdPartitionByExpressionResolver.computeHashCode(dvd,
        null, 0), robjs2[1].hashCode());

    robj = (Integer)rpr.getRoutingObjectsForPartitioningColumns(
        new DataValueDescriptor[] { new SQLInteger(0) });
    assertEquals(GfxdPartitionByExpressionResolver.computeHashCode(
        new SQLInteger(0), null, 0), robj.hashCode());

    dvd = new SQLInteger(40);
    robj = (Integer)rpr.getRoutingObjectsForPartitioningColumns(
        new DataValueDescriptor[] { new SQLInteger(40) });
    assertEquals(GfxdPartitionByExpressionResolver.computeHashCode(dvd,
        null, 0), robj.hashCode());

    String[] arr = rpr.getColumnNames();
    assertEquals(1, arr.length);
    assertEquals("ID2", arr[0]);

    String tbname = rpr.getTableName();
    assertEquals("PARTITIONTESTTABLE", tbname);
    String scname = rpr.getSchemaName();
    assertEquals("EMP", scname);
    String master = rpr.getMasterTable(false /* immediate master*/);
    assertEquals("/EMP/PARTITIONTESTTABLE", master);
    assertFalse(rpr.isUsedInPartitioning("ID"));
    assertTrue(rpr.isUsedInPartitioning("ID2"));
    assertEquals(0, rpr.getPartitioningColumnIndex("ID2"));
    assertEquals(1, rpr.getPartitioningColumnsCount());
    assertFalse(rpr.isPartitioningKeyThePrimaryKey());
  }

  public void testPartitionByColumnPartitionResolver() throws Exception {
    // Start one client and one server
    startVMs(1, 2);

    clientSQLExecute(1, "create schema EMP");
    clientSQLExecute(1,
        "create table EMP.PARTITIONTESTTABLE (ID int not null, "
        + " ID2 int not null, ID3 int not null, primary key (ID))"
        + "PARTITION BY COLUMN (ID2)");

    clientSQLExecute(1,
        "insert into EMP.PARTITIONTESTTABLE values (1, 1, 1), (2, 2, 2 ), (3, 3, 3) ");
    checkResolver(new int[] { 1 }, new int[] { 1 , 2 }, "verifyPartitionByColumnResolver");
  }
  
  public static void verifyPartitionByPrimaryResolver() {
    Cache cache = CacheFactory.getAnyInstance();
    Region regtwo = cache.getRegion("/EMP/PARTITIONTESTTABLE");  
    RegionAttributes rattr = regtwo.getAttributes();
    PartitionResolver pr = rattr.getPartitionAttributes().getPartitionResolver();
    GfxdPartitionByExpressionResolver rpr = (GfxdPartitionByExpressionResolver)pr;
    assertNotNull(rpr);

    DataValueDescriptor dvd = new SQLInteger(200);
    int maxWidth = DataTypeDescriptor.getBuiltInDataTypeDescriptor(
        Types.INTEGER).getMaximumWidth();
    Integer robj = (Integer)rpr.getRoutingKeyForColumn(dvd);
    assertEquals(dvd.computeHashCode(maxWidth, 0), robj.intValue());
    dvd = new SQLInteger(20);
    robj = (Integer)rpr.getRoutingKeyForColumn(dvd);
    assertEquals(dvd.computeHashCode(maxWidth, 0), robj.intValue());
    dvd = new SQLInteger(60);
    robj = (Integer)rpr.getRoutingKeyForColumn(dvd);
    assertEquals(dvd.computeHashCode(maxWidth, 0), robj.intValue());

    assertNull(rpr.getRoutingObjectsForRange(new SQLInteger(10), true,
        new SQLInteger(20), true));
    assertNull(rpr.getRoutingObjectsForRange(new SQLInteger(25), true,
        new SQLInteger(75), true));

    dvd = new SQLInteger(25);
    Object[] onerobj = rpr.getRoutingObjectsForRange(dvd, true,
        new SQLInteger(25), true);
    assertEquals(1, onerobj.length);
    assertEquals(dvd.computeHashCode(maxWidth, 0), onerobj[0].hashCode());

    DataValueDescriptor dvd2 = new SQLInteger(20);
    Object[] robjs2 = rpr.getRoutingObjectsForList(new DataValueDescriptor[] {
        dvd, dvd2 });
    assertEquals(2, robjs2.length);
    assertEquals(dvd.computeHashCode(maxWidth, 0), robjs2[0].hashCode());
    assertEquals(dvd2.computeHashCode(maxWidth, 0), robjs2[1].hashCode());

    dvd = new SQLInteger(0);
    robj = (Integer)rpr.getRoutingObjectsForPartitioningColumns(
        new DataValueDescriptor[] { dvd });
    assertEquals(dvd.computeHashCode(maxWidth, 0), robj.hashCode());

    dvd = new SQLInteger(40);
    robj = (Integer)rpr.getRoutingObjectsForPartitioningColumns(
        new DataValueDescriptor[] {new SQLInteger(40) });
    assertEquals(dvd.computeHashCode(maxWidth, 0), robj.hashCode());

    String[] arr = rpr.getColumnNames();
    assertEquals(1, arr.length);
    assertEquals("ID", arr[0]);

    String tbname = rpr.getTableName();
    assertEquals("PARTITIONTESTTABLE", tbname);
    String scname = rpr.getSchemaName();
    assertEquals("EMP", scname);
    String master = rpr.getMasterTable(false /* immediate master*/);
    assertEquals("/EMP/PARTITIONTESTTABLE", master);
    assertTrue(rpr.isUsedInPartitioning("ID"));
    assertFalse(rpr.isUsedInPartitioning("ID2"));
    assertEquals(0, rpr.getPartitioningColumnIndex("ID"));
    assertEquals(1, rpr.getPartitioningColumnsCount());
    assertTrue(rpr.isPartitioningKeyThePrimaryKey());
  }

  public void testPartitionByPrimaryPartitionResolver() throws Exception {
    // Start one client a one server
    startVMs(1, 2);

    clientSQLExecute(1, "create schema EMP");
    clientSQLExecute(1,
        "create table EMP.PARTITIONTESTTABLE (ID int not null, "
        + " ID2 int not null, ID3 int not null, primary key (ID))"
        + "PARTITION BY PRIMARY KEY");

    clientSQLExecute(1,
        "insert into EMP.PARTITIONTESTTABLE values (1, 1, 1), (2, 2, 2 ), (3, 3, 3) ");
    checkResolver(new int[] { 1 }, new int[] { 1 , 2 }, "verifyPartitionByPrimaryResolver");
  }

  public void testDefaultPartitionResolver_40101() throws Exception {
    // Start one client a three servers
    startVMs(1, 3);

    clientSQLExecute(1, "create table trade.txhistory (cid int, sid int, "
        + "qty int, price decimal (30, 20), type varchar(10), "
        + "tid int,  constraint type_ch check (type in ('buy', 'sell')))");

    String query1 = "select cid, sid, type, tid from trade.txhistory "
        + "where cid > 4 and sid < 200 and qty > 100 and tid < 40";
    String query2 = "select count(*) from trade.txhistory  where "
        + "cid > 3 and sid < 100 and tid > 20";
    String query3 = "select max(price) from trade.txhistory where tid = 5";
    String query4 = "select sum(qty * price) as amount from trade.txhistory "
        + "where sid < 100 and tid > 20";

    // insert some values with duplicate CIDs
    clientSQLExecute(1, "insert into trade.txhistory values "
        + "(4, 50, 150, 30.0, 'buy', 20), "
        + "(5, 160, 120, 40.0, 'sell', 30), "
        + "(3, 80, 80, 70.0, 'sell', 40), "
        + "(3, 20, 120, 45.0, 'buy', 5), "
        + "(6, 80, 200, 80.0, 'buy', 30), "
        + "(5, 10, 60, 75.0, 'sell', 200), "
        + "(8, 70, 180, 30.0, 'sell', 5)");

    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 }, query1, TestUtil.getResourcesDir()
        + "/lib/checkQuery.xml", "hist_query1");
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 }, query2, null, "2");
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 }, query3, null,
        "45.00000000000000000000");
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 }, query4, null,
        "26100.00000000000000000000");

    // now do an update and check again
    //serverSQLExecute(1, "update trade.txhistory set ID = 5 where TID = 30");
  }

  // For range partitioning
  public void testDefaultPartitionResolverRange_40290() throws Exception {
    // Start one client a three servers
    AsyncVM async1 = invokeStartServerVM(1, 0, "SG1, SG2", null);
    AsyncVM async2 = invokeStartServerVM(2, 0, "SG2, SG3, SG4", null);
    AsyncVM async3 = invokeStartServerVM(3, 0, null, null);
    startClientVMs(1, 0, null);
    joinVMs(true, async1, async2, async3);

    clientSQLExecute(1, "create schema trade DEFAULT SERVER GROUPS (SG2,SG3,SG4)");
    
    clientSQLExecute(1, "create table trade.customers (cid int not null, " +
    		"cust_name varchar(100), addr varchar(100), ctid int, primary key (cid))  " +
    		"partition by range (cid) ( VALUES BETWEEN 0 AND 699, VALUES BETWEEN 699 AND 1102, " +
    		"VALUES BETWEEN 1102 AND 1251, VALUES BETWEEN 1251 AND 1577, VALUES BETWEEN 1577 AND 1800, " +
    		"VALUES BETWEEN 1800 AND 10000)");

    clientSQLExecute(1, "create table trade.networth (cid int not null, cash int not null, " +
    		"securities int not null, loanlimit int, availloan int not null, tid int not null, " +
    		"constraint netw_pk primary key (cid), " +
    		"constraint cust_newt_fk foreign key (cid) references trade.customers (cid) on delete restrict, " +
    		"constraint cash_ch check (cash>=0), constraint sec_ch check (securities >=0), " +
    		"constraint availloan_ck check (loanlimit>=availloan and availloan >=0))");

    clientSQLExecute(1, "insert into trade.customers values(52, 'NAME52', 'ADDRESS IS NAME52', 4 ), " +
    		"(51, 'NAME51', 'ADDRESS IS NAME51', 4), " +
    		"(415, 'NAME415', 'ADDRESS IS NAME415', 12), " +
    		"(408, 'NAME408', 'ADDRESS IS NAME408', 12), " +
    		"(383, 'NAME383', 'ADDRESS IS NAME383', 16), " +
    		"(380, 'NAME380', 'ADDRESS IS NAME380', 16)");
    
    clientSQLExecute(1, "insert into trade.networth values(51, 51, 0, 51000, 10000, 2 ), " +
        "(415, 415, 0, 41500, 10000, 2), " + "(408, 408, 0, 40800, 10000, 2), " +  "(383, 383, 0, 38300, 10000, 2), " +
        "(380, 380, 0, 38000, 10000, 2)");
    
    String query1 = "select * from trade.customers c, trade.networth n where n.cid = c.cid and n.tid = 2";
    
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 }, query1, TestUtil.getResourcesDir()
        + "/lib/checkQuery.xml", "test_40290");
  }
  
  public void testDefaultPartitionResolverList_40290() throws Exception {
    // Start one client a three servers
    AsyncVM async1 = invokeStartServerVM(1, 0, "SG1, SG2", null);
    AsyncVM async2 = invokeStartServerVM(2, 0, "SG2, SG3, SG4", null);
    AsyncVM async3 = invokeStartServerVM(3, 0, null, null);
    startClientVMs(1, 0, null);
    joinVMs(true, async1, async2, async3);

    clientSQLExecute(1, "create schema trade DEFAULT SERVER GROUPS (SG2,SG3,SG4)");
    
    clientSQLExecute(1, "create table trade.customers (cid int not null, " +
                "cust_name varchar(100), addr varchar(100), ctid int, primary key (cid))  " +
                "partition by list (cid) ( VALUES (0, 10, 100, 200), VALUES (51, 415, 408, 383, 380), " +
                "VALUES (1500, 2000))");

    clientSQLExecute(1, "create table trade.networth (cid int not null, cash int not null, " +
                "securities int not null, loanlimit int, availloan int not null, tid int not null, " +
                "constraint netw_pk primary key (cid), " +
                "constraint cust_newt_fk foreign key (cid) references trade.customers (cid) on delete restrict, " +
                "constraint cash_ch check (cash>=0), constraint sec_ch check (securities >=0), " +
                "constraint availloan_ck check (loanlimit>=availloan and availloan >=0))");

    clientSQLExecute(1, "insert into trade.customers values(52, 'NAME52', 'ADDRESS IS NAME52', 4 ), " +
                "(51, 'NAME51', 'ADDRESS IS NAME51', 4), " +
                "(415, 'NAME415', 'ADDRESS IS NAME415', 12), " +
                "(408, 'NAME408', 'ADDRESS IS NAME408', 12), " +
                "(383, 'NAME383', 'ADDRESS IS NAME383', 16), " +
                "(380, 'NAME380', 'ADDRESS IS NAME380', 16)");
    
    clientSQLExecute(1, "insert into trade.networth values(51, 51, 0, 51000, 10000, 2 ), " +
        "(415, 415, 0, 41500, 10000, 2), " + "(408, 408, 0, 40800, 10000, 2), " +  "(383, 383, 0, 38300, 10000, 2), " +
        "(380, 380, 0, 38000, 10000, 2)");
    
    String query1 = "select * from trade.customers c, trade.networth n where n.cid = c.cid and n.tid = 2";
    
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 }, query1, TestUtil.getResourcesDir()
        + "/lib/checkQuery.xml", "test_40290");
  }

  public void testDefaultPartitionResolverColumn_40290() throws Exception {
    // Start one client a three servers
    AsyncVM async1 = invokeStartServerVM(1, 0, "SG1, SG2", null);
    AsyncVM async2 = invokeStartServerVM(2, 0, "SG2, SG3, SG4", null);
    AsyncVM async3 = invokeStartServerVM(3, 0, null, null);
    startClientVMs(1, 0, null);
    joinVMs(true, async1, async2, async3);

    clientSQLExecute(1, "create schema trade DEFAULT SERVER GROUPS (SG2,SG3,SG4)");
    
    clientSQLExecute(1, "create table trade.customers (cid int not null, " +
                "cust_name varchar(100), addr varchar(100), ctid int, primary key (cid))  " +
                "partition by column (cid)");

    clientSQLExecute(1, "create table trade.networth (cid int not null, cash int not null, " +
                "securities int not null, loanlimit int, availloan int not null, tid int not null, " +
                "constraint netw_pk primary key (cid), " +
                "constraint cust_newt_fk foreign key (cid) references trade.customers (cid) on delete restrict, " +
                "constraint cash_ch check (cash>=0), constraint sec_ch check (securities >=0), " +
                "constraint availloan_ck check (loanlimit>=availloan and availloan >=0))");

    clientSQLExecute(1, "insert into trade.customers values(52, 'NAME52', 'ADDRESS IS NAME52', 4 ), " +
                "(51, 'NAME51', 'ADDRESS IS NAME51', 4), " +
                "(415, 'NAME415', 'ADDRESS IS NAME415', 12), " +
                "(408, 'NAME408', 'ADDRESS IS NAME408', 12), " +
                "(383, 'NAME383', 'ADDRESS IS NAME383', 16), " +
                "(380, 'NAME380', 'ADDRESS IS NAME380', 16)");
    
    clientSQLExecute(1, "insert into trade.networth values(51, 51, 0, 51000, 10000, 2 ), " +
        "(415, 415, 0, 41500, 10000, 2), " + "(408, 408, 0, 40800, 10000, 2), " +  "(383, 383, 0, 38300, 10000, 2), " +
        "(380, 380, 0, 38000, 10000, 2)");
    
    String query1 = "select * from trade.customers c, trade.networth n where n.cid = c.cid and n.tid = 2";
    
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 }, query1, TestUtil.getResourcesDir()
        + "/lib/checkQuery.xml", "test_40290");
  }

  public void testDefaultPartitionResolverColumnPK_40290() throws Exception {
    // Start one client a three servers
    AsyncVM async1 = invokeStartServerVM(1, 0, "SG1, SG2", null);
    AsyncVM async2 = invokeStartServerVM(2, 0, "SG2, SG3, SG4", null);
    AsyncVM async3 = invokeStartServerVM(3, 0, null, null);
    startClientVMs(1, 0, null);
    joinVMs(true, async1, async2, async3);

    clientSQLExecute(1, "create schema trade DEFAULT SERVER GROUPS (SG2,SG3,SG4)");
    
    clientSQLExecute(1, "create table trade.customers (cid int not null, " +
                "cust_name varchar(100), addr varchar(100), ctid int, primary key (cid))  " +
                "partition by primary key");

    clientSQLExecute(1, "create table trade.networth (cid int not null, cash int not null, " +
                "securities int not null, loanlimit int, availloan int not null, tid int not null, " +
                "constraint netw_pk primary key (cid), " +
                "constraint cust_newt_fk foreign key (cid) references trade.customers (cid) on delete restrict, " +
                "constraint cash_ch check (cash>=0), constraint sec_ch check (securities >=0), " +
                "constraint availloan_ck check (loanlimit>=availloan and availloan >=0))");

    clientSQLExecute(1, "insert into trade.customers values(52, 'NAME52', 'ADDRESS IS NAME52', 4 ), " +
                "(51, 'NAME51', 'ADDRESS IS NAME51', 4), " +
                "(415, 'NAME415', 'ADDRESS IS NAME415', 12), " +
                "(408, 'NAME408', 'ADDRESS IS NAME408', 12), " +
                "(383, 'NAME383', 'ADDRESS IS NAME383', 16), " +
                "(380, 'NAME380', 'ADDRESS IS NAME380', 16)");
    
    clientSQLExecute(1, "insert into trade.networth values(51, 51, 0, 51000, 10000, 2 ), " +
        "(415, 415, 0, 41500, 10000, 2), " + "(408, 408, 0, 40800, 10000, 2), " +  "(383, 383, 0, 38300, 10000, 2), " +
        "(380, 380, 0, 38000, 10000, 2)");
    
    String query1 = "select * from trade.customers c, trade.networth n where n.cid = c.cid and n.tid = 2";
    
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 }, query1, TestUtil.getResourcesDir()
        + "/lib/checkQuery.xml", "test_40290");
  }
  
  public void testDefaultPartitionResolverDefaultRslvr_40290() throws Exception {
    // Start one client a three servers
    AsyncVM async1 = invokeStartServerVM(1, 0, "SG1, SG2", null);
    AsyncVM async2 = invokeStartServerVM(2, 0, "SG2, SG3, SG4", null);
    AsyncVM async3 = invokeStartServerVM(3, 0, null, null);
    startClientVMs(1, 0, null);
    joinVMs(true, async1, async2, async3);

    clientSQLExecute(1, "create schema trade DEFAULT SERVER GROUPS (SG2,SG3,SG4)");
    
    clientSQLExecute(1, "create table trade.customers (cid int not null, " +
                "cust_name varchar(100), addr varchar(100), ctid int, primary key (cid))");

    clientSQLExecute(1, "create table trade.networth (cid int not null, cash int not null, " +
                "securities int not null, loanlimit int, availloan int not null, tid int not null, " +
                "constraint netw_pk primary key (cid), " +
                "constraint cust_newt_fk foreign key (cid) references trade.customers (cid) on delete restrict, " +
                "constraint cash_ch check (cash>=0), constraint sec_ch check (securities >=0), " +
                "constraint availloan_ck check (loanlimit>=availloan and availloan >=0))");

    clientSQLExecute(1, "insert into trade.customers values(52, 'NAME52', 'ADDRESS IS NAME52', 4 ), " +
                "(51, 'NAME51', 'ADDRESS IS NAME51', 4), " +
                "(415, 'NAME415', 'ADDRESS IS NAME415', 12), " +
                "(408, 'NAME408', 'ADDRESS IS NAME408', 12), " +
                "(383, 'NAME383', 'ADDRESS IS NAME383', 16), " +
                "(380, 'NAME380', 'ADDRESS IS NAME380', 16)");
    
    clientSQLExecute(1, "insert into trade.networth values(51, 51, 0, 51000, 10000, 2 ), " +
        "(415, 415, 0, 41500, 10000, 2), " + "(408, 408, 0, 40800, 10000, 2), " +  "(383, 383, 0, 38300, 10000, 2), " +
        "(380, 380, 0, 38000, 10000, 2)");
    
    String query1 = "select * from trade.customers c, trade.networth n where n.cid = c.cid and n.tid = 2";
    
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 }, query1, TestUtil.getResourcesDir()
        + "/lib/checkQuery.xml", "test_40290");
  }

  public void testExpression_bug41002() throws Exception {
    // Start a couple of servers
    // Use this VM as the client
    startVMs(1, 2);

    clientSQLExecute(1, "CREATE TABLE COFFEES (COF_NAME VARCHAR(32) "
        + "CONSTRAINT COF_PK PRIMARY KEY, SUP_ID INTEGER, PRICE FLOAT, "
        + "SALES INTEGER, TOTAL INTEGER) PARTITION BY (COF_NAME)");
    // expect syntax error here without parens
    try {
      clientSQLExecute(1, "CREATE TABLE SUPPLIERS (SUP_ID INTEGER CONSTRAINT "
          + "SUP_PK PRIMARY KEY, SUP_NAME VARCHAR(40), STREET VARCHAR(40), "
          + "CITY VARCHAR(20), STATE VARCHAR(2), ZIP CHAR(5)) PARTITION BY "
          + "SUP_ID COLOCATE WITH (COFFEES)");
    } catch (SQLException ex) {
      if (!"42X01".equals(ex.getSQLState())) {
        throw ex;
      }
    }
    clientSQLExecute(1, "CREATE TABLE SUPPLIERS (SUP_ID INTEGER CONSTRAINT "
        + "SUP_PK PRIMARY KEY, SUP_NAME VARCHAR(40), STREET VARCHAR(40), "
        + "CITY VARCHAR(20), STATE VARCHAR(2), ZIP CHAR(5)) PARTITION BY "
        + "(SUP_ID) COLOCATE WITH (COFFEES)");

    // few inserts but no real colocation guarantee since the two tables are
    // partitioned by different columns
    serverSQLExecute(1, "INSERT INTO COFFEES VALUES('French Roast', "
        + "49, 8.99, 0, 0)");
    serverSQLExecute(2, "INSERT INTO SUPPLIERS VALUES(49, 'Nestle', "
        + "'Street1', 'City1', 'OR', '40000')");

    // queries
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 },
        "SELECT * FROM COFFEES", TestUtil.getResourcesDir()
            + "/lib/checkQuery.xml", "bug41002_coffees");
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 },
        "SELECT * FROM SUPPLIERS", TestUtil.getResourcesDir()
            + "/lib/checkQuery.xml", "bug41002_suppliers");

    // drop the tables and close the connection

    // exepect exception when dropping parent before child
    addExpectedException(new int[] { 1 }, null,
        UnsupportedOperationException.class);
    try {
      clientSQLExecute(1, "drop table COFFEES");
    } catch (SQLException ex) {
      if (!"X0Y98".equals(ex.getSQLState())) {
        throw ex;
      }
    }
    removeExpectedException(new int[] { 1 }, null,
        UnsupportedOperationException.class);

    clientSQLExecute(1, "drop table SUPPLIERS");
    clientSQLExecute(1, "drop table COFFEES");
  }

  public void testExpressionNetworkClient_bug41002() throws Exception {
    // Start a network server
    final int netPort = startNetworkServer(1, null, null);
    final VM serverVM = this.serverVMs.get(0);

    // Use this VM as the network client
    final Connection conn = TestUtil.getNetConnection(netPort, null, null);

    Statement stmt = conn.createStatement();
    stmt.execute("CREATE TABLE COFFEES (COF_NAME VARCHAR(32) "
        + "CONSTRAINT COF_PK PRIMARY KEY, SUP_ID INTEGER, PRICE FLOAT, "
        + "SALES INTEGER, TOTAL INTEGER) PARTITION BY (COF_NAME)");

    // expect syntax error here without parens
    try {
      stmt.execute("CREATE TABLE SUPPLIERS (SUP_ID INTEGER CONSTRAINT "
          + "SUP_PK PRIMARY KEY, SUP_NAME VARCHAR(40), STREET VARCHAR(40), "
          + "CITY VARCHAR(20), STATE VARCHAR(2), ZIP CHAR(5)) PARTITION BY "
          + "SUP_ID COLOCATE WITH (COFFEES)");
    } catch (SQLException ex) {
      if (!"42X01".equals(ex.getSQLState())) {
        throw ex;
      }
    }

    stmt.execute("CREATE TABLE SUPPLIERS (SUP_ID INTEGER CONSTRAINT "
        + "SUP_PK PRIMARY KEY, SUP_NAME VARCHAR(40), STREET VARCHAR(40), "
        + "CITY VARCHAR(20), STATE VARCHAR(2), ZIP CHAR(5)) PARTITION BY "
        + "(SUP_ID) COLOCATE WITH (COFFEES)");

    // few inserts but no real colocation guarantee since the two tables are
    // partitioned by different columns
    stmt.execute("INSERT INTO COFFEES VALUES('French Roast', "
        + "49, 8.99, 0, 0)");
    stmt.execute("INSERT INTO SUPPLIERS VALUES(49, 'Nestle', "
        + "'Street1', 'City1', 'OR', '40000')");

    // queries
    TestUtil.jdbcConn = conn;
    TestUtil.sqlExecuteVerifyText("SELECT * FROM COFFEES", TestUtil.getResourcesDir()
        + "/lib/checkQuery.xml", "bug41002_coffees", true, false);
    TestUtil.sqlExecuteVerifyText("SELECT * FROM SUPPLIERS", TestUtil.getResourcesDir()
        + "/lib/checkQuery.xml", "bug41002_suppliers", false, false);

    // drop the tables and close the connection

    // exepect exception when dropping parent before child
    addExpectedException(serverVM,
        new Object[] { UnsupportedOperationException.class });
    try {
      stmt.execute("drop table COFFEES");
    } catch (SQLException ex) {
      if (!"X0Y98".equals(ex.getSQLState())) {
        throw ex;
      }
    }
    removeExpectedException(serverVM,
        new Object[] { UnsupportedOperationException.class });

    stmt.execute("drop table SUPPLIERS");
    stmt.execute("drop table COFFEES");

    stmt.close();
    conn.close();
  }

  /**
   * test for "delete should have happened" with servers going down and
   * redundancy 0 or not satisfied
   */
  public void testDataLoss_44547() throws Exception {
    startVMs(1, 3);

    clientSQLExecute(1, "create table trade.sellorders (oid int not null "
        + "constraint orders_pk primary key, cid int, sid int) "
        + "partition by column (sid)");

    // dummy puts on the global index region to force bucket creation
    serverExecute(1, new SerializableRunnable() {
      @Override
      public void run() {
        Region<Object, Object> globalIndexRegion = Misc
            .getRegion("/TRADE/2__SELLORDERS__OID", true, false);
        for (int i = 1; i <= 100; i++) {
          globalIndexRegion.put(new SQLInteger(10000 + i), "dummy");
        }
      }
    });

    // inserts on server2 so that global index region buckets and region buckets
    // are not colocated
    serverExecute(2, new SerializableRunnable() {
      @Override
      public void run() {
        try {
          Connection conn = TestUtil.getConnection();
          PreparedStatement pstmt = conn
              .prepareStatement("insert into trade.sellorders values (?, ?, ?)");
          for (int i = 1; i <= 100; i++) {
            pstmt.setInt(1, i);
            pstmt.setInt(2, i);
            pstmt.setInt(3, i * 3);
            pstmt.execute();
          }
        } catch (Exception ex) {
          throw new TestException("failed in inserts", ex);
        }
      }
    });

    // now deletes should throw "delete should have happened" if server1 goes
    // down
    Connection conn = TestUtil.getConnection();
    Statement stmt = conn.createStatement();
    for (int i = 1; i <= 50; i++) {
      stmt.execute("delete from trade.sellorders where cid = " + i);
    }
    stopVMNum(-1);
    try {
      for (int i = 51; i <= 100; i++) {
        stmt.execute("delete from trade.sellorders where cid = " + i);
      }
      fail("expected an assertion failure during deletes");
    } catch (SQLException sqle) {
      if (!sqle.getMessage().contains(
          "Delete did not happen on index: TRADE.2__SELLORDERS__OID")) {
        throw sqle;
      }
    }
  }

//  public static void verifyPartitionByMultipleColumnResolver() {
//    Cache cache = CacheFactory.getAnyInstance();
//    Region regtwo = cache.getRegion("/EMP/PARTITIONTESTTABLE");  
//    RegionAttributes rattr = regtwo.getAttributes();
//    PartitionResolver pr = rattr.getPartitionAttributes().getPartitionResolver();
//    GfxdPartitionByExpressionResolver rpr = (GfxdPartitionByExpressionResolver)pr;
//    assertNotNull(rpr);
//  
//    boolean exceptionGot = false;
//    try {
//      Integer robj = (Integer)rpr.getRoutingKeyForColumn(Integer.valueOf(200));
//    } catch (Exception e) {
//      exceptionGot = true;
//    }
//    assertTrue(exceptionGot);
//    
//    exceptionGot = false;
//    try {
//      rpr.getRoutingObjectsForRange(new Integer(10), true, new Integer(20), true);
//    } catch (Exception e) {
//      exceptionGot = true;
//    }
//    assertTrue(exceptionGot);
//    
//    exceptionGot = false;
//    try {
//      rpr.getRoutingObjectsForList(new Object[] {new Integer(25), new Integer(20)});
//    } catch (Exception e) {
//      exceptionGot = true;
//    }
//    assertTrue(exceptionGot);
//    
//    robj = (Integer)rpr.getRoutingObjectsForPartitioningColumns(new Object[] {new Integer(0) });
//    assertEquals(0, robj.hashCode());
//    
//    robj = (Integer)rpr.getRoutingObjectsForPartitioningColumns(new Object[] {new Integer(40) });
//    assertEquals(40, robj.hashCode());
//    
//    String[] arr = rpr.getColumnNames();
//    assertEquals(1, arr.length);
//    assertEquals("ID2", arr[0]);
//    
//    String tbname = rpr.getTableName();
//    assertEquals("PARTITIONTESTTABLE", tbname);
//    String scname = rpr.getSchemaName();
//    assertEquals("EMP", scname);
//    String master = rpr.getMasterTable(false /* immediate master*/);
//    assertEquals("/EMP/PARTITIONTESTTABLE", master);
//    assertFalse(rpr.isUsedInPartitioning("ID"));
//    assertTrue(rpr.isUsedInPartitioning("ID2"));
//    assertEquals(0, rpr.getPartitioningColumnIndex("ID2"));
//    assertEquals(1, rpr.getPartitioningColumnsCount());
//    assertFalse(rpr.isPartitionedOnPrimary());
//  }
//  
//  public void testPartitionByMultipleColumnPartitionResolver() throws Exception {
//    // Start one client and one server
//    startServerVMs(2);
//    startClientVMs(1);
//
//    clientSQLExecute(1, "create schema EMP");
//    clientSQLExecute(1,
//        "create table EMP.PARTITIONTESTTABLE (ID int not null, "
//        + " ID2 int not null, ID3 int not null, primary key (ID))"
//        + "PARTITION BY COLUMN (ID2, ID3)");
//
//    clientSQLExecute(1,
//        "insert into EMP.PARTITIONTESTTABLE values (1, 1, 1), (2, 2, 2 ), (3, 3, 3) ");
//    checkResolver(new int[] { 1 }, new int[] { 1 , 2 }, "verifyPartitionByMultipleColumnResolver");
//  }
//  
//  public static void verifyPartitionByPrimaryCompositeResolver() {
//    Cache cache = CacheFactory.getAnyInstance();
//    Region regtwo = cache.getRegion("/EMP/PARTITIONTESTTABLE");  
//    RegionAttributes rattr = regtwo.getAttributes();
//    PartitionResolver pr = rattr.getPartitionAttributes().getPartitionResolver();
//    GfxdPartitionByExpressionResolver rpr = (GfxdPartitionByExpressionResolver)pr;
//    assertNotNull(rpr);
//    
//    Integer robj = (Integer)rpr.getRoutingKeyForColumn(Integer.valueOf(200));
//    assertEquals(200, robj.intValue());
//    robj = (Integer)rpr.getRoutingKeyForColumn(Integer.valueOf(20));
//    assertEquals(20, robj.intValue());
//    robj = (Integer)rpr.getRoutingKeyForColumn(Integer.valueOf(60));
//    assertEquals(60, robj.intValue());
//    
//    assertNull(rpr.getRoutingObjectsForRange(new Integer(10), true, new Integer(20), true));
//    assertNull(rpr.getRoutingObjectsForRange(new Integer(25), true, new Integer(75), true));
//    
//    Object[] onerobj = rpr.getRoutingObjectsForRange(new Integer(25), true, new Integer(25), true);
//    assertEquals(1, onerobj.length);
//    assertEquals(25, onerobj[0].hashCode());
//    
//    Object [] robjs2 = rpr.getRoutingObjectsForList(new Object[] {new Integer(25), new Integer(20)});
//    assertEquals(2, robjs2.length);
//    assertEquals(25, robjs2[0].hashCode());
//    assertEquals(20, robjs2[1].hashCode());
//    
//    robj = (Integer)rpr.getRoutingObjectsForPartitioningColumns(new Object[] {new Integer(0) });
//    assertEquals(0, robj.hashCode());
//    
//    robj = (Integer)rpr.getRoutingObjectsForPartitioningColumns(new Object[] {new Integer(40) });
//    assertEquals(40, robj.hashCode());
//    
//    String[] arr = rpr.getColumnNames();
//    assertEquals(1, arr.length);
//    assertEquals("ID", arr[0]);
//    
//    String tbname = rpr.getTableName();
//    assertEquals("PARTITIONTESTTABLE", tbname);
//    String scname = rpr.getSchemaName();
//    assertEquals("EMP", scname);
//    String master = rpr.getMasterTable(false /* immediate master*/);
//    assertEquals("/EMP/PARTITIONTESTTABLE", master);
//    assertTrue(rpr.isUsedInPartitioning("ID"));
//    assertFalse(rpr.isUsedInPartitioning("ID2"));
//    assertEquals(0, rpr.getPartitioningColumnIndex("ID"));
//    assertEquals(1, rpr.getPartitioningColumnsCount());
//    assertTrue(rpr.isPartitionedOnPrimary());
//  }
//  
//  public void testPartitionByPrimaryCompositePartitionResolver() throws Exception {
//    // Start one client and one server
//    startServerVMs(2);
//    startClientVMs(1);
//
//    clientSQLExecute(1, "create schema EMP");
//    clientSQLExecute(1,
//        "create table EMP.PARTITIONTESTTABLE (ID int not null, "
//        + " ID2 int not null, ID3 int not null, primary key (ID))"
//        + "PARTITION BY PRIMARY KEY");
//
//    clientSQLExecute(1,
//        "insert into EMP.PARTITIONTESTTABLE values (1, 1, 1), (2, 2, 2 ), (3, 3, 3) ");
//    checkResolver(new int[] { 1 }, new int[] { 1 , 2 }, "verifyPartitionByPrimaryCompositeResolver");
//  }
}
