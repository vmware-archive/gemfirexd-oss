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
import java.sql.Statement;
import java.util.Properties;

import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.iapi.reference.Attribute;
import com.pivotal.gemfirexd.jdbc.GfxdCallbacksTest;

@SuppressWarnings("serial")
public class GfxdListenerWriterDUnit extends DistributedSQLTestBase {

  public GfxdListenerWriterDUnit(String name) {
    super(name);
  }

  public void testListenerCallback() throws Exception {
    // Start one client and one server
    startVMs(1, 3);
    clientSQLExecute(1, "create schema EMP");
    clientSQLExecute(1, "create table EMP.TESTTABLE (ID int not null,"
        + " SECONDID int not null, THIRDID int not null,"
        + " primary key (ID))");
    clientSQLExecute(1, "create table EMP.TESTTABLE_ONE (ID int not null,"
        + " SECONDID int not null, THIRDID int not null,"
        + " primary key (ID))");
    GfxdCallbacksTest.addListener(
        "ID1",
        "emp",
        "testtable",
        "com.pivotal.gemfirexd.ddl.EventCallBackFactorImpl",
        "emp.testtable_one:"+2, "");
    clientSQLExecute(1, "insert into emp.testtable values(1,2,3)");
    clientSQLExecute(1, "insert into emp.testtable values(2,3,4)");
    clientSQLExecute(1, "insert into emp.testtable values(3,4,5)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "listenerWriter_Test1");
    
    GfxdCallbacksTest.removeListener("ID1", "emp", "testtable");
    
    clientSQLExecute(1, "insert into emp.testtable values(4,5,6)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "listenerWriter_Test1");
    
    GfxdCallbacksTest.addListener(
        "ID1",
        "emp",
        "testtable",
        "com.pivotal.gemfirexd.ddl.EventCallBackFactorImpl",
        "emp.testtable_one:"+3, "");
    
    clientSQLExecute(1, "insert into emp.testtable values(5,6,7)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "listenerWriter_Test2");
  }
  
  public void testListenerCallback_replicate() throws Exception {
    // Start one client and one server
    startVMs(1, 3);
    clientSQLExecute(1, "create schema EMP");
    clientSQLExecute(1, "create table EMP.TESTTABLE (ID int not null,"
        + " SECONDID int not null, THIRDID int not null,"
        + " primary key (ID)) replicate");
    clientSQLExecute(1, "create table EMP.TESTTABLE_ONE (ID int not null,"
        + " SECONDID int not null, THIRDID int not null,"
        + " primary key (ID))");
    GfxdCallbacksTest.addListener(
        "ID1",
        "emp",
        "testtable",
        "com.pivotal.gemfirexd.ddl.EventCallBackFactorImpl",
        "emp.testtable_one:"+2, "");
    clientSQLExecute(1, "insert into emp.testtable values(1,2,3)");
    clientSQLExecute(1, "insert into emp.testtable values(2,3,4)");
    clientSQLExecute(1, "insert into emp.testtable values(3,4,5)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "listenerWriter_Test1");
    
    GfxdCallbacksTest.removeListener("ID1", "emp", "testtable");
    
    clientSQLExecute(1, "insert into emp.testtable values(4,5,6)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "listenerWriter_Test1");
    
    GfxdCallbacksTest.addListener(
        "ID1",
        "emp",
        "testtable",
        "com.pivotal.gemfirexd.ddl.EventCallBackFactorImpl",
        "emp.testtable_one:"+3, "");
    
    clientSQLExecute(1, "insert into emp.testtable values(5,6,7)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "listenerWriter_Test2");
  }
  
  public void testWriterCallback() throws Exception {
    // Start one client and one server
    startVMs(1, 3);
    clientSQLExecute(1, "create schema EMP");
    clientSQLExecute(1, "create table EMP.TESTTABLE (ID int not null,"
        + " SECONDID int not null, THIRDID int not null,"
        + " primary key (ID))");
    clientSQLExecute(1, "create table EMP.TESTTABLE_ONE (ID int not null,"
        + " SECONDID int not null, THIRDID int not null,"
        + " primary key (ID))");
    GfxdCallbacksTest.addWriter(
        "emp",
        "testtable",
        "com.pivotal.gemfirexd.ddl.EventCallBackFactorImpl",
        "emp.testtable_one:"+2, null);
    clientSQLExecute(1, "insert into emp.testtable values(1,2,3)");
    clientSQLExecute(1, "insert into emp.testtable values(2,3,4)");
    clientSQLExecute(1, "insert into emp.testtable values(3,4,5)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "listenerWriter_Test1");
    
    GfxdCallbacksTest.removeWriter("emp", "testtable");
    
    clientSQLExecute(1, "insert into emp.testtable values(4,5,6)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "listenerWriter_Test1");
    
    GfxdCallbacksTest.addWriter(
        "emp",
        "testtable",
        "com.pivotal.gemfirexd.ddl.EventCallBackFactorImpl",
        "emp.testtable_one:"+3, "");
    
    clientSQLExecute(1, "insert into emp.testtable values(5,6,7)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "listenerWriter_Test2");
  }
  
  public void testWriterCallback_replicate() throws Exception {
    // Start one client and one server
    startVMs(1, 3);
    clientSQLExecute(1, "create schema EMP");
    clientSQLExecute(1, "create table EMP.TESTTABLE (ID int not null,"
        + " SECONDID int not null, THIRDID int not null,"
        + " primary key (ID)) replicate");
    clientSQLExecute(1, "create table EMP.TESTTABLE_ONE (ID int not null,"
        + " SECONDID int not null, THIRDID int not null,"
        + " primary key (ID))");
    GfxdCallbacksTest.addWriter(
        "emp",
        "testtable",
        "com.pivotal.gemfirexd.ddl.EventCallBackFactorImpl",
        "emp.testtable_one:"+2, null);
    clientSQLExecute(1, "insert into emp.testtable values(1,2,3)");
    clientSQLExecute(1, "insert into emp.testtable values(2,3,4)");
    clientSQLExecute(1, "insert into emp.testtable values(3,4,5)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "listenerWriter_Test1");
    
    GfxdCallbacksTest.removeWriter("emp", "testtable");
    
    clientSQLExecute(1, "insert into emp.testtable values(4,5,6)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "listenerWriter_Test1");
    
    GfxdCallbacksTest.addWriter(
        "emp",
        "testtable",
        "com.pivotal.gemfirexd.ddl.EventCallBackFactorImpl",
        "emp.testtable_one:"+3, "");
    
    clientSQLExecute(1, "insert into emp.testtable values(5,6,7)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "listenerWriter_Test2");
  }
  

  public void testListenerCallbackSupressionForSkipListener() throws Exception {
    // Start one client and one server
    startVMs(1, 3);
    clientSQLExecute(1, "create schema EMP");
    clientSQLExecute(1, "create table EMP.TESTTABLE (ID int not null,"
        + " SECONDID int not null, THIRDID int not null,"
        + " primary key (ID))");
    clientSQLExecute(1, "create table EMP.TESTTABLE_ONE (ID int not null,"
        + " SECONDID int not null, THIRDID int not null,"
        + " primary key (ID))");
    GfxdCallbacksTest.addListener(
        "ID1",
        "emp",
        "testtable",
        "com.pivotal.gemfirexd.ddl.EventCallBackFactorImpl",
        "emp.testtable_one:"+2, "");
    Properties props = new Properties();
    props.put(com.pivotal.gemfirexd.Attribute.SKIP_LISTENERS, "true");
    Connection conn = TestUtil.getConnection(props);
    Statement stmt = conn.createStatement();
    stmt.execute("insert into emp.testtable values(1,2,3)");
    stmt.execute("insert into emp.testtable values(2,3,4)");
    stmt.execute("insert into emp.testtable values(3,4,5)");    
    stmt.execute("update emp.testtable set secondid =7");
    stmt.execute("delete from  emp.testtable where ID = 1");
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "empty");
    
    GfxdCallbacksTest.removeListener("ID1", "emp", "testtable");
    
    clientSQLExecute(1, "insert into emp.testtable values(4,5,6)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "empty");
    
    GfxdCallbacksTest.addListener(
        "ID1",
        "emp",
        "testtable",
        "com.pivotal.gemfirexd.ddl.EventCallBackFactorImpl",
        "emp.testtable_one:"+3, "");
    
    stmt.execute( "insert into emp.testtable values(5,6,7)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "empty");
  }
  
  public void testListenerCallbackSuppression_replicateForSkipListeners() throws Exception {
    // Start one client and one server
    startVMs(1, 3);
    clientSQLExecute(1, "create schema EMP");
    clientSQLExecute(1, "create table EMP.TESTTABLE (ID int not null,"
        + " SECONDID int not null, THIRDID int not null,"
        + " primary key (ID)) replicate");
    clientSQLExecute(1, "create table EMP.TESTTABLE_ONE (ID int not null,"
        + " SECONDID int not null, THIRDID int not null,"
        + " primary key (ID))");
    GfxdCallbacksTest.addListener(
        "ID1",
        "emp",
        "testtable",
        "com.pivotal.gemfirexd.ddl.EventCallBackFactorImpl",
        "emp.testtable_one:"+2, "");
    Properties props = new Properties();
    props.put(com.pivotal.gemfirexd.Attribute.SKIP_LISTENERS, "true");
    Connection conn = TestUtil.getConnection(props);
    Statement stmt = conn.createStatement();
    stmt.execute( "insert into emp.testtable values(1,2,3)");
    stmt.execute( "insert into emp.testtable values(2,3,4)");
    stmt.execute( "insert into emp.testtable values(3,4,5)");
    stmt.execute("update emp.testtable set secondid =7");
    stmt.execute("delete from  emp.testtable where ID = 1");
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "empty");
    
    GfxdCallbacksTest.removeListener("ID1", "emp", "testtable");
    
    clientSQLExecute(1, "insert into emp.testtable values(4,5,6)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "empty");
    
    GfxdCallbacksTest.addListener(
        "ID1",
        "emp",
        "testtable",
        "com.pivotal.gemfirexd.ddl.EventCallBackFactorImpl",
        "emp.testtable_one:"+3, "");
    
    stmt.execute( "insert into emp.testtable values(5,6,7)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "empty");
  }
  
  
  public void testWriterCallbackSuppressionForSkipListeners() throws Exception {
    // Start one client and one server
    startVMs(1, 3);
    clientSQLExecute(1, "create schema EMP");
    clientSQLExecute(1, "create table EMP.TESTTABLE (ID int not null,"
        + " SECONDID int not null, THIRDID int not null,"
        + " primary key (ID))");
    clientSQLExecute(1, "create table EMP.TESTTABLE_ONE (ID int not null,"
        + " SECONDID int not null, THIRDID int not null,"
        + " primary key (ID))");
    GfxdCallbacksTest.addWriter(
        "emp",
        "testtable",
        "com.pivotal.gemfirexd.ddl.EventCallBackFactorImpl",
        "emp.testtable_one:"+2, null);
    Properties props = new Properties();
    props.put(com.pivotal.gemfirexd.Attribute.SKIP_LISTENERS, "true");
    Connection conn = TestUtil.getConnection(props);
    Statement stmt = conn.createStatement();
    
    stmt.execute( "insert into emp.testtable values(1,2,3)");
    stmt.execute( "insert into emp.testtable values(2,3,4)");
    stmt.execute("insert into emp.testtable values(3,4,5)");
    stmt.execute("update emp.testtable set secondid =7");
    stmt.execute("delete from  emp.testtable where ID = 1");
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "empty");
    
    GfxdCallbacksTest.removeWriter("emp", "testtable");
    
    clientSQLExecute(1,"insert into emp.testtable values(4,5,6)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "empty");
    
    GfxdCallbacksTest.addWriter(
        "emp",
        "testtable",
        "com.pivotal.gemfirexd.ddl.EventCallBackFactorImpl",
        "emp.testtable_one:"+3, "");
    
    stmt.execute("insert into emp.testtable values(5,6,7)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "empty");
  }
  
  

  public void testWriterCallbackSuppressionForSkipListener_replicate() throws Exception {
    // Start one client and one server
    startVMs(1, 3);
    clientSQLExecute(1, "create schema EMP");
    clientSQLExecute(1, "create table EMP.TESTTABLE (ID int not null,"
        + " SECONDID int not null, THIRDID int not null,"
        + " primary key (ID)) replicate");
    clientSQLExecute(1, "create table EMP.TESTTABLE_ONE (ID int not null,"
        + " SECONDID int not null, THIRDID int not null,"
        + " primary key (ID))");
    GfxdCallbacksTest.addWriter(
        "emp",
        "testtable",
        "com.pivotal.gemfirexd.ddl.EventCallBackFactorImpl",
        "emp.testtable_one:"+2, null);
    
    Properties props = new Properties();
    props.put(com.pivotal.gemfirexd.Attribute.SKIP_LISTENERS, "true");
    Connection conn = TestUtil.getConnection(props);
    Statement stmt = conn.createStatement();
    
    stmt.execute("insert into emp.testtable values(1,2,3)");
    stmt.execute("insert into emp.testtable values(2,3,4)");
    stmt.execute("insert into emp.testtable values(3,4,5)");
    stmt.execute("update emp.testtable set secondid =7");
    stmt.execute("delete from  emp.testtable where ID = 1");
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "empty");
    
    GfxdCallbacksTest.removeWriter("emp", "testtable");
    
    clientSQLExecute(1,"insert into emp.testtable values(4,5,6)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "empty");
    
    GfxdCallbacksTest.addWriter(
        "emp",
        "testtable",
        "com.pivotal.gemfirexd.ddl.EventCallBackFactorImpl",
        "emp.testtable_one:"+3, "");
    
    stmt.execute("insert into emp.testtable values(5,6,7)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "empty");
  }

}
