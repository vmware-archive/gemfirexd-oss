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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

import org.apache.derbyTesting.junit.JDBC;

import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.monitor.Monitor;

/**
 * @author yjing
 *
 */
public class GlobalHashIndexTest extends JdbcTestBase {
  
  public static void main(String[] args) {
    TestRunner.run(new TestSuite(GlobalHashIndexTest.class));
  }
  
  public GlobalHashIndexTest(String name) {
    super(name);
  }
  
  public void testCreateGlobalHashIndexForPrimaryKey() throws SQLException, StandardException {
    // Create a schema
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create schema EMP");

    // Check for PARTITION BY COLUMN
    s.execute("create table EMP.PARTITIONTESTTABLE (ID int not null, "
        + " SECONDID int not null, THIRDID int not null, primary key (ID, SECONDID))"
        + " PARTITION BY COLUMN (ID)"+ getOverflowSuffix());
    
    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE values(1,2,3)");
    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE values(4,5,6)");
    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE values(7,8,9)");

    AlterTableTest.checkIndexType("EMP", "PARTITIONTESTTABLE",
        GfxdConstants.LOCAL_HASH1_INDEX_TYPE, "ID", "SECONDID");

    String[][] expectedRows = { { "1", "2", "3" } };
    ResultSet rs = s.executeQuery("Select * from EMP.PARTITIONTESTTABLE "
        + "where id=1 and secondid=2");
    JDBC.assertFullResultSet(rs, expectedRows);
  }

  /**
   * Check for different combinations where primary key is superset of
   * partitioning columns (see EricS's comments towards the end of #41218)
   */
  public void testCreateLocalIndexForPKSupersetPartitionColumns()
      throws SQLException, StandardException {
    // Create a schema
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create schema EMP");

    // Check for PARTITION BY COLUMN with PK superset of partitioning columns
    s.execute("create table EMP.PARTITIONTESTTABLE (ID int not null, "
        + " SECONDID int not null, THIRDID int not null, primary key "
        + "(ID, SECONDID)) PARTITION BY COLUMN (SECONDID)"
        + getOverflowSuffix());

    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE values(1, 2, 3)");
    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE values(4, 5, 6)");
    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE values(7, 8, 9)");

    AlterTableTest.checkIndexType("EMP", "PARTITIONTESTTABLE",
        GfxdConstants.LOCAL_HASH1_INDEX_TYPE, "ID", "SECONDID");
    AlterTableTest.checkNoIndex("EMP", "PARTITIONTESTTABLE", "SECONDID");

    String[][] expectedRows = { { "1", "2", "3" } };
    ResultSet rs = s.executeQuery("select * from EMP.PARTITIONTESTTABLE "
        + "where id=1 and secondid=2");
    JDBC.assertFullResultSet(rs, expectedRows);

    // now add more columns with a non-contiguous subset
    s.execute("create table EMP.PARTITIONTESTTABLE2 (ID int not null, "
        + " SECONDID int not null, THIRDID int not null, FOURTHID int "
        + "not null, primary key (THIRDID, SECONDID, ID, FOURTHID)) "
        + "PARTITION BY COLUMN (FOURTHID, SECONDID)" + getOverflowSuffix());

    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE2 values(1, 2, 3, 4)");
    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE2 values(4, 5, 6, 7)");
    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE2 values(7, 8, 9, 10)");

    AlterTableTest.checkIndexType("EMP", "PARTITIONTESTTABLE2",
        GfxdConstants.LOCAL_HASH1_INDEX_TYPE, "ID", "SECONDID", "THIRDID",
        "FOURTHID");
    AlterTableTest.checkNoIndex("EMP", "PARTITIONTESTTABLE2", "ID", "SECONDID");

    String[][] expectedRows2 = { { "1", "2", "3", "4" } };
    rs = s.executeQuery("select * from EMP.PARTITIONTESTTABLE2 "
        + "where id=1 and secondid=2");
    JDBC.assertFullResultSet(rs, expectedRows2);
  }

  public void testCreateGlobalHashIndexWithPrimaryKey() throws SQLException,
      StandardException {
    // Create a schema
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create schema EMP");

    // Check for PARTITION BY COLUMN
    s.execute("create table EMP.PARTITIONTESTTABLE (ID int primary key, "
        + " SECONDID int not null, THIRDID int not null, Unique (SECONDID))"
        + " PARTITION BY COLUMN (ID, SECONDID)" + getOverflowSuffix());

    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE values(1,2,3)");
    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE values(4,5,6)");
    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE values(7,8,9)");

    AlterTableTest.checkIndexType("EMP", "PARTITIONTESTTABLE",
        GfxdConstants.GLOBAL_HASH_INDEX_TYPE, "ID");
    AlterTableTest.checkIndexType("EMP", "PARTITIONTESTTABLE",
        GfxdConstants.GLOBAL_HASH_INDEX_TYPE, "SECONDID");

    String[][] expectedRows = { { "1", "2", "3" } };
    ResultSet rs = s.executeQuery("Select * from EMP.PARTITIONTESTTABLE "
        + "where id=1 and secondid=2");
    JDBC.assertFullResultSet(rs, expectedRows);
  }

  public void testCreateGlobalHashIndexForUniqueKey() throws SQLException, StandardException {
    // Create a schema
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create schema EMP");

    // Check for PARTITION BY RANGE
    s.execute("create table EMP.PARTITIONTESTTABLE (ID int Primary KEY,"
        + " SECONDID int not null, THIRDID int not null, unique (SECONDID, THIRDID))"
        + " PARTITION BY COLUMN (ID)"+getOverflowSuffix());

    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE values(1,2,3)");
    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE values(4,5,6)");
    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE values(7,8,9)");

    AlterTableTest.checkIndexType("EMP", "PARTITIONTESTTABLE",
        GfxdConstants.LOCAL_HASH1_INDEX_TYPE, "ID");
    AlterTableTest.checkIndexType("EMP", "PARTITIONTESTTABLE",
        GfxdConstants.GLOBAL_HASH_INDEX_TYPE, "SECONDID", "THIRDID");

    ResultSet rs = s.executeQuery("VALUES SYSCS_UTIL.CHECK_TABLE('EMP',"
        + "'PARTITIONTESTTABLE')");
    JDBC.assertSingleValueResultSet(rs, "1");

       // JDBC.assertDrainResults(rs, 1);
    String[][] expectedRows={{"1","2","3"}};
    rs=s.executeQuery("Select * from EMP.PARTITIONTESTTABLE where thirdid=3 and secondid=2");
    JDBC.assertFullResultSet(rs, expectedRows);
    
    
    s.execute("DELETE from EMP.PARTITIONTESTTABLE where ID=1");
    rs=s.executeQuery("Select * from EMP.PARTITIONTESTTABLE where thirdid=3 and secondid=2");
    
    JDBC.assertDrainResults(rs, 0);
    
    try {
    	Monitor.getStream().println(
    	          "<ExpectedException action=add>"
    	              + "com.gemstone.gemfire.cache.EntryExistsException"
    	              + "</ExpectedException>");
    	       Monitor.getStream().flush();
       
      s.execute("INSERT INTO EMP.PARTITIONTESTTABLE values(10,5,6)");
    }
    catch (Exception e) {
       return;
    }
    finally {
    	Monitor.getStream().println("<ExpectedException action=remove>"
    	          + "com.gemstone.gemfire.cache.EntryExistsException"
    	          + "</ExpectedException>");
    	      Monitor.getStream().flush();
    }
    throw StandardException
        .newException("The duplicate unique key exception is supposed to be thrown!");
  }

  public void testCreateGlobalHashIndexWithPartialUniqueKeyAsBaseTablePartitionKey() 
      throws SQLException, StandardException {
    // Create a schema
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create schema EMP");

    // Check for PARTITION BY RANGE
    s.execute("create table EMP.PARTITIONTESTTABLE (ID int Primary KEY,"
        + " SECONDID int not null, THIRDID int not null, unique (SECONDID, THIRDID))"
        + " PARTITION BY COLUMN (SECONDID)"+getOverflowSuffix());

    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE values(1,2,3)");
    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE values(4,5,6)");
    s.execute("INSERT INTO EMP.PARTITIONTESTTABLE values(7,8,9)");

    AlterTableTest.checkIndexType("EMP", "PARTITIONTESTTABLE",
        GfxdConstants.GLOBAL_HASH_INDEX_TYPE, "ID");
    AlterTableTest.checkIndexType("EMP", "PARTITIONTESTTABLE",
        GfxdConstants.LOCAL_SORTEDMAP_INDEX_TYPE, "SECONDID", "THIRDID");

    String[][] expectedRows = { { "1", "2", "3" } };
    ResultSet rs = s.executeQuery("Select * from EMP.PARTITIONTESTTABLE "
        + "where thirdid=3 and secondid=2");
    JDBC.assertFullResultSet(rs, expectedRows);
  }

  public void testBug39750() throws SQLException, StandardException {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create table trade.customers (cid int not null, cust_name varchar(100), since date, addr varchar(100), tid int, primary key (cid))"+getOverflowSuffix());
    s.execute("create table emp.employees (eid int not null constraint employees_pk primary key, emp_name varchar(100), since date, addr varchar(100), ssn varchar(9))"+getOverflowSuffix());
    s.execute("create table trade.trades (tid int, cid int, eid int, tradedate date, primary Key (tid), foreign key (cid) references trade.customers (cid) ON DELETE RESTRICT, constraint emp_fk foreign key (eid) references emp.employees (eid) ON DELETE RESTRICT)"+getOverflowSuffix());
    s.execute("insert into trade.customers values (1,'abc','2008-10-20','kdkd',1)");
    s.execute("insert into trade.customers values (2,'abc','2008-10-20','kdkd',2)");
    s.execute("insert into trade.customers values (3,'abc','2008-10-20','kdkd',3)");
    s.execute("insert into trade.customers values (4,'abc','2008-10-20','kdkd',4)");
    s.execute("insert into trade.customers values (5,'abc','2008-10-20','kdkd',5)");
    s.execute("insert into trade.customers values (6,'abc','2008-10-20','kdkd',6)");
    s.execute("insert into emp.employees   values (1,'abc','2008-10-20','kdkd','1')");
    s.execute("insert into emp.employees   values (2,'abc','2008-10-20','kdkd','2')");
    s.execute("insert into emp.employees   values (3,'abc','2008-10-20','kdkd','3')");
    s.execute("insert into emp.employees   values (4,'abc','2008-10-20','kdkd','4')");
    s.execute("insert into emp.employees   values (5,'abc','2008-10-20','kdkd','5')");
    s.execute("insert into emp.employees   values (6,'abc','2008-10-20','kdkd','6')");
    
    s.execute("insert into trade.trades  values (7,1,6,'2008-10-20')");
    s.execute("insert into trade.trades  values (8,2,5,'2008-10-20')");
    s.execute("insert into trade.trades  values (9,3,4,'2008-10-20')");
    s.execute("insert into trade.trades  values (10,4,3,'2008-10-20')");
    s.execute("insert into trade.trades  values (11,5,2,'2008-10-20')");
    s.execute("insert into trade.trades  values (12,6,1,'2008-10-20')");
    try {
        Monitor.getStream().println(
                "<ExpectedException action=add>"
                    + "java.sql.SQLIntegrityConstraintViolationException"
                    + "</ExpectedException>");
             Monitor.getStream().flush();
             
             Monitor.getStream().println(
                 "<ExpectedException action=add>"
                     + "java.sql.SQLException"
                     + "</ExpectedException>");
              Monitor.getStream().flush();
              
              Monitor.getStream().println(
                  "<ExpectedException action=add>"
                      + "com.pivotal.gemfirexd.internal.iapi.error.StandardException"
                      + "</ExpectedException>");
               Monitor.getStream().flush();
      s.execute( "delete from trade.customers where cid=4");
    }catch(Exception e) {
    	
    }
    finally {
    	  Monitor.getStream().println(
                  "<ExpectedException action=remove>"
                      + "com.pivotal.gemfirexd.internal.iapi.error.StandardException"
                      + "</ExpectedException>");
          Monitor.getStream().flush();
          
          Monitor.getStream().println(
                  "<ExpectedException action=remove>"
                      + "java.sql.SQLException"
                      + "</ExpectedException>");
         Monitor.getStream().flush();
         
         Monitor.getStream().println(
                 "<ExpectedException action=remove>"
                     + "java.sql.SQLIntegrityConstraintViolationException"
                     + "</ExpectedException>");
         Monitor.getStream().flush();
    }
    
    
  }
  

  public String getOverflowSuffix() {
    return  "";
  }

}
