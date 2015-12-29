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

import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.jdbc.GfxdCallbacksTest;

public class GfxdEventCallbackDUnit extends DistributedSQLTestBase {

  public GfxdEventCallbackDUnit(String name) {
    super(name);
  }
  
  public void testEventCallbackWriter() throws Exception {
    // Start one client and three server
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
        "com.pivotal.gemfirexd.ddl.EventCallbackWriter",
        "|url=jdbc|primary-keys=ID", null);
    clientSQLExecute(1, "insert into emp.testtable values(1,2,3)");
    clientSQLExecute(1, "insert into emp.testtable values(2,3,4)");
    clientSQLExecute(1, "insert into emp.testtable values(3,4,5)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkEventCallback.xml", "listenerWriter_Test_1");
    
    GfxdCallbacksTest.removeWriter("emp", "testtable");
    
    clientSQLExecute(1, "insert into emp.testtable values(4,5,6)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkEventCallback.xml", "listenerWriter_Test_1");
    
    GfxdCallbacksTest.addWriter(
        "emp",
        "testtable",
        "com.pivotal.gemfirexd.ddl.EventCallbackWriter",
        "|url=jdbc|primary-keys=ID", "");
    
    clientSQLExecute(1, "insert into emp.testtable values(5,6,7)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkEventCallback.xml", "listenerWriter_Test_2");
    
    clientSQLExecute(1, "insert into emp.testtable values(6,7,8)");
    clientSQLExecute(1, "update emp.testtable set thirdid=7 where secondid=2");
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkEventCallback.xml", "listenerWriter_Test_3");

    clientSQLExecute(1, "delete from emp.testtable where thirdid=7");
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkEventCallback.xml", "listenerWriter_Test_4");

  }
  

  public void testEventCallbackListener() throws Exception {
    // Start one client and three server
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
        "com.pivotal.gemfirexd.ddl.EventCallbackListener",
        "|url=jdbc|primary-keys=ID", null);
    clientSQLExecute(1, "insert into emp.testtable values(1,2,3)");
    clientSQLExecute(1, "insert into emp.testtable values(2,3,4)");
    clientSQLExecute(1, "insert into emp.testtable values(3,4,5)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkEventCallback.xml", "listenerWriter_Test_1");
    
    GfxdCallbacksTest.removeListener("ID1", "emp", "testtable");
    
    clientSQLExecute(1, "insert into emp.testtable values(4,5,6)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkEventCallback.xml", "listenerWriter_Test_1");
    
    GfxdCallbacksTest.addListener(
        "ID1",
        "emp",
        "testtable",
        "com.pivotal.gemfirexd.ddl.EventCallbackListener",
        "|url=jdbc|primary-keys=ID", "");
    
    clientSQLExecute(1, "insert into emp.testtable values(5,6,7)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkEventCallback.xml", "listenerWriter_Test_2");
    
    clientSQLExecute(1, "insert into emp.testtable values(6,7,8)");
    clientSQLExecute(1, "update emp.testtable set thirdid=7 where secondid=2");
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkEventCallback.xml", "listenerWriter_Test_3");

    clientSQLExecute(1, "delete from emp.testtable where thirdid=7");
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkEventCallback.xml", "listenerWriter_Test_4");
  }
  
  //This test will fail because the listener will be invoked on all replicated testtable
  public void _testEventCallbackListener_replicate() throws Exception {
    // Start one client and three server
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
        "com.pivotal.gemfirexd.ddl.EventCallbackListener",
        "|url=jdbc|primary-keys=ID|max-connections=10", null);
    clientSQLExecute(1, "insert into emp.testtable values(1,2,3)");
    clientSQLExecute(1, "insert into emp.testtable values(2,3,4)");
    clientSQLExecute(1, "insert into emp.testtable values(3,4,5)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkEventCallback.xml", "listenerWriter_Test_1");
    
    GfxdCallbacksTest.removeListener("ID1", "emp", "testtable");
    
    clientSQLExecute(1, "insert into emp.testtable values(4,5,6)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkEventCallback.xml", "listenerWriter_Test_1");
    
    GfxdCallbacksTest.addListener(
        "ID1",
        "emp",
        "testtable",
        "com.pivotal.gemfirexd.ddl.EventCallbackListener",
        "|url=jdbc|primary-keys=ID|max-connections=10", "");
    
    clientSQLExecute(1, "insert into emp.testtable values(5,6,7)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkEventCallback.xml", "listenerWriter_Test_2");
    
    clientSQLExecute(1, "insert into emp.testtable values(6,7,8)");
    clientSQLExecute(1, "update emp.testtable set thirdid=7 where secondid=2");
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkEventCallback.xml", "listenerWriter_Test_3");

    clientSQLExecute(1, "delete from emp.testtable where thirdid=7");
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkEventCallback.xml", "listenerWriter_Test_4");
  }
  
  public void testEventCallbackWriter_replicate() throws Exception {
    // Start one client and three server
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
        "com.pivotal.gemfirexd.ddl.EventCallbackWriter",
        "|url=jdbc|primary-keys=ID|max-connections=10", null);
    clientSQLExecute(1, "insert into emp.testtable values(1,2,3)");
    clientSQLExecute(1, "insert into emp.testtable values(2,3,4)");
    clientSQLExecute(1, "insert into emp.testtable values(3,4,5)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkEventCallback.xml", "listenerWriter_Test_1");
    
    GfxdCallbacksTest.removeWriter("emp", "testtable");
    
    clientSQLExecute(1, "insert into emp.testtable values(4,5,6)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkEventCallback.xml", "listenerWriter_Test_1");
    
    GfxdCallbacksTest.addWriter(        
        "emp",
        "testtable",
        "com.pivotal.gemfirexd.ddl.EventCallbackWriter",
        "|url=jdbc|primary-keys=ID|max-connections=10", "");
    
    clientSQLExecute(1, "insert into emp.testtable values(5,6,7)");
    
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkEventCallback.xml", "listenerWriter_Test_2");
    
    clientSQLExecute(1, "insert into emp.testtable values(6,7,8)");
    clientSQLExecute(1, "update emp.testtable set thirdid=7 where secondid=2");
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkEventCallback.xml", "listenerWriter_Test_3");

    clientSQLExecute(1, "delete from emp.testtable where thirdid=7");
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.TESTTABLE_ONE", TestUtil.getResourcesDir()
            + "/lib/checkEventCallback.xml", "listenerWriter_Test_4");
  }
  
}
