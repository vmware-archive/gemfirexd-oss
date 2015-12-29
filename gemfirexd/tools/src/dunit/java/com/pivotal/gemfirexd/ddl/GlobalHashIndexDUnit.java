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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.distributed.internal.ReplyException;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedPreparedStatement;
/***
 * Test for the global hash index creation and query
 * With respect to regard the creation of global index, several
 * scenario are considered:
 * 1) implicitly create.
 *    I. If the primary key or unique key is not a partition 
 *    key, a global index is created for them.
 *    II. If the partition key is a subset of the primary key
 *    or unique key, a global index is created.
 *    III. Otherwise, no global index creation.
 *    
 *    
 * 2) explicitly create. 
 *    Using create index statement. if the index fields should be
 *    unique?
 * 
 * @see GlobalHashIndexTest 
 * @author yjing
 */
public class GlobalHashIndexDUnit extends DistributedSQLTestBase {

  private static final long serialVersionUID = 3615764749940350805L;

  public GlobalHashIndexDUnit(String name) {
    super(name);
  }

  public void testGlobalHashIndexCreate() throws Exception {
    // Start one client and two servers
    startVMs(1, 2);

    final String schemaName = getCurrentDefaultSchemaName();
    // Create a table
    clientSQLExecute(1, "create table " + schemaName
        + ".ORDERS (ID int primary key, VOL int NOT NULL unique, "
        + "SECURITY_ID varchar(10)) partition by Primary Key redundancy 1");

    // partition by Primary Key redundancy 1
    for (int i = 0; i < 100; i++) {
      String statement = "insert into ORDERS values (" + i + "," + i + ",'char"
          + i + "')";
      clientSQLExecute(1, statement);
    }
    sqlExecuteVerify(null, new int[] { 1, 2 },
        "VALUES SYSCS_UTIL.CHECK_TABLE('" + schemaName + "','ORDERS')",
        null, "1");

   /* String deleteQuery="Delete from APP.ORDERS where ID=?";
   TestUtil.setupConnection();
   EmbedConnection mcon = (EmbedConnection)TestUtil.jdbcConn;
   EmbedPreparedStatement es = (EmbedPreparedStatement)TestUtil.jdbcConn.prepareStatement(deleteQuery);
    
   for(int i=0; i<100; i+=10) {
    // clientSQLExecute(1, "Delete from APP.ORDERS where ID="+i);
     es.setInt(1, i);
     int n = es.executeUpdate();
   }
    sqlExecuteVerify(null, new int[] { 1, 2 },
        "VALUES SYSCS_UTIL.CHECK_TABLE('" + schemaName + "','ORDERS')",
        null, "1");
   */
    String updateQuery = "update " + schemaName + ".ORDERS set vol=vol+101";
    TestUtil.setupConnection();
    EmbedPreparedStatement es = (EmbedPreparedStatement)TestUtil.jdbcConn
        .prepareStatement(updateQuery);

    // for(int i=0; i<100; i+=10) {
    es.execute();

    // }
    sqlExecuteVerify(null, new int[] { 1, 2 },
        "VALUES SYSCS_UTIL.CHECK_TABLE('" + schemaName + "','ORDERS')",
        null, "1");
  }

  public void testGlobalHashIndexDuplicateWithMultiColumns() throws Exception {
    // Start one client and two servers
    startVMs(1, 2);

    // Create a table
    clientSQLExecute(1, "create table ORDERS (ID int primary key, VOL int "
        + "NOT NULL, NAME varchar(10) NOT NULL, SECURITY_ID varchar(10), "
        + "UNIQUE (VOL, NAME))");

    clientSQLExecute(1,
        "insert into ORDERS values (1, 2, 'GEMSTONE', 'GEMSTONE')");

    clientSQLExecute(1,
        "insert into ORDERS values (2, 2, 'GEMSTONE1', 'GEMSTONE')");
  }

public void testBug39692() throws Exception {
//Start one client and one servers
  startVMs(1, 1);
//Create a table
  clientSQLExecute(1, "create table APP.SECURITIES (sec_id int not null, symbol varchar(10) not null, " +
  		"price decimal (30, 20), exchange varchar(10) not null, tid int," +
  		"constraint sec_pk primary key (sec_id), constraint sec_uq unique (symbol, exchange), " +
  		"constraint exc_ch check (exchange in ('nasdaq', 'nye', 'amex', 'lse', 'fse', 'hkse', 'tse')))");
  clientSQLExecute(1, "create table APP.CUSTOMERS (cid int not null, cust_name varchar(100)," +
  		  "since date, addr varchar(100), tid int, primary key (cid))");

  /*clientSQLExecute(1,"create table trade.networth (cid int not null, cash decimal (30, 20), " +
  		"securities decimal (30, 20), loanlimit int, availloan decimal (30, 20),  tid int, " +
  		"constraint netw_pk primary key (cid), " +
  		"constraint cust_newt_fk foreign key (cid) references trade.customers (cid) on delete cascade, " +
  		"constraint cash_ch check (cash>=0), constraint sec_ch check (securities >=0), " +
  		"constraint availloan_ck check (loanlimit>=availloan and availloan >=0))");
  clientSQLExecute(1,"create table trade.portfolio (cid int not null, sid int not null, qty int not null," +
  		 "availQty int not null, subTotal decimal(30,20), tid int, " +
  		 "constraint portf_pk primary key (cid, sid), constraint cust_fk foreign key (cid) references trade.customers (cid) on delete cascade, " +
  		 "constraint sec_fk foreign key (sid) references trade.securities (sec_id), " +
  		 "constraint qty_ck check (qty>=0), constraint avail_ch check (availQty>=0 and availQty<=qty))");*/
  clientSQLExecute(1, "create table APP.BUYORDERS (oid int not null constraint"
          + " buyorders_pk primary key, cid int, sid int, qty int, bid decimal"
          + "(30, 20), ordertime timestamp, status varchar(10), tid int, "
          + "constraint bo_cust_fk foreign key (cid) references app.customers "
          + "(cid) on delete RESTRICT, constraint bo_sec_fk foreign key (sid) "
          + "references app.securities (sec_id), "
          + "constraint bo_qty_ck check (qty>=0))");
  clientSQLExecute(1, "insert into APP.SECURITIES VALUES(1, 'abc', 12.23, 'nasdaq', 20)");
  clientSQLExecute(1, "insert into APP.SECURITIES VALUES(2, 'bbc', 13.23, 'nye', 30)");
  clientSQLExecute(1, "insert into APP.SECURITIES VALUES(3, 'cbc', 14.23, 'amex', 40)");
  clientSQLExecute(1, "insert into APP.SECURITIES VALUES(4, 'dbc', 15.23, 'lse', 50)");
  clientSQLExecute(1, "insert into APP.SECURITIES VALUES(5, 'ebc', 16.23, 'fse', 60)");
  
  clientSQLExecute(1, "insert into APP.CUSTOMERS VALUES(6, 'andy', '2008-11-03','gemstone AVE',70)");
  clientSQLExecute(1, "insert into APP.CUSTOMERS VALUES(7, 'andy', '2008-11-03','gemstone AVE',80)");
  clientSQLExecute(1, "insert into APP.CUSTOMERS VALUES(8, 'andy', '2008-11-03','gemstone AVE',90)");
  clientSQLExecute(1, "insert into APP.CUSTOMERS VALUES(9, 'andy', '2008-11-03','gemstone AVE',100)");
  clientSQLExecute(1, "insert into APP.CUSTOMERS VALUES(10, 'andy', '2008-11-03','gemstone AVE',110)");
  clientSQLExecute(1, "insert into APP.CUSTOMERS VALUES(11, 'andy', '2008-11-03','gemstone AVE',120)");
  clientSQLExecute(1, "insert into APP.CUSTOMERS VALUES(12, 'andy', '2008-11-03','gemstone AVE',130)");
  
  clientSQLExecute(1, "insert into APP.BUYORDERS VALUES(13, 6, 1,100,12.34,'2008-11-03 12:34:56','F',10)");
  clientSQLExecute(1, "insert into APP.BUYORDERS VALUES(14, 7, 2,100,12.34,'2008-11-03 12:34:56','F',10)");
  clientSQLExecute(1, "insert into APP.BUYORDERS VALUES(15, 8, 3,100,12.34,'2008-11-03 12:34:56','F',10)");
  clientSQLExecute(1, "insert into APP.BUYORDERS VALUES(16, 9, 4,100,12.34,'2008-11-03 12:34:56','F',10)");
  clientSQLExecute(1, "insert into APP.BUYORDERS VALUES(17, 10, 5,100,12.34,'2008-11-03 12:34:56','F',10)");
  clientSQLExecute(1, "insert into APP.BUYORDERS VALUES(18, 11, 1,100,12.34,'2008-11-03 12:34:56','F',10)");
  clientSQLExecute(1, "insert into APP.BUYORDERS VALUES(19, 12, 2,100,12.34,'2008-11-03 12:34:56','F',10)");

    // Rahul : This is to supress exception in server log.
    addExpectedException(new int[] { 1 }, new int[] { 1 }, new Object[] {
        ReplyException.class, SQLException.class, FunctionException.class });

    try {
      clientSQLExecute(1,
          "UPDATE APP.BUYORDERS SET sid = 0 where cid =6 and sid=1 ");
    } catch (SQLException ex) {
      if (!"23503".equals(ex.getSQLState())) {
        throw ex;
      }
    } finally {
      // Rahul : remove supress expression in server log.
      removeExpectedException(new int[] { 1 }, new int[] { 1 }, new Object[] {
          ReplyException.class, SQLException.class, FunctionException.class });
    }

  //} catch (Exception e) {

  //}
 // assert j==7:"The expected exceptions are 7 but the actual exception are"+j;
//  clientSQLExecute(1, "UPDATE BUYORDERS SET sid = 1 where cid = 2 and sid= 7");

}

public void testBug_40826() throws Exception {

  startVMs(1, 1);
//Create a table
  clientSQLExecute(1,"create table trade.portfolio (cid int not null, sid int not null, qty int not null, " +
              "availQty int not null, subTotal decimal(30,20), tid int, " +
              "constraint portf_pk primary key (cid, sid)," +
              "constraint qty_ck check (qty>=0), " +
              "constraint avail_ch check (availQty>=0 and availQty<=qty))");
  clientSQLExecute(1, "INSERT INTO TRADE.PORTFOLIO VALUES(2, 3, 10, 10, 1.0, 1)");
  Connection conn = TestUtil.getConnection();
  PreparedStatement ps=conn.prepareStatement("update trade.portfolio set subTotal=(?*qty) where cid=? and sid=? and tid=?");
  ps.setBigDecimal(1, new BigDecimal(1.1));
  ps.setInt(2, 2);
  ps.setInt(3, 3);
  ps.setInt(4, 4);
  ps.execute();
  conn.close();
}  

public void testGlobalUniqueIndexWithRedundancy() throws Exception {

  startVMs(1, 2);
//Create a table
  final String schemaName = getCurrentDefaultSchemaName();
  // Create a table
  clientSQLExecute(1, "create table " + schemaName
      + ".ORDERS (ID int primary key, VOL int NOT NULL unique, "
      + "SECURITY_ID varchar(10)) partition by Primary Key redundancy 1");
  // Insert a single row
  clientSQLExecute(1, "INSERT INTO "+schemaName+".ORDERS VALUES(2, 3, 'ABC')");
  // Try to create a globally unique index
  // Previously, this failed as secondaries were also scanned during index creation
  //  and CREATE INDEX thought there were two identical rows in the table!
  clientSQLExecute(1,"create global hash index i1 on "+schemaName+".orders(VOL)");
  }  
}
