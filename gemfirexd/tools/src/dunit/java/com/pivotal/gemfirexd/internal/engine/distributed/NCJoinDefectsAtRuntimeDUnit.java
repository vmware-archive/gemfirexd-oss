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
package com.pivotal.gemfirexd.internal.engine.distributed;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;

import io.snappydata.test.dunit.SerializableRunnable;

/**
 * Non Collocated Join Functional Test.
 * 
 * Test NC Join between two Non Collocated tables
 * 
 * @author vivekb
 */
@SuppressWarnings("serial")
public class NCJoinDefectsAtRuntimeDUnit extends DistributedSQLTestBase {

  public NCJoinDefectsAtRuntimeDUnit(String name) {
    super(name);
  }
  
  @Override
  public void setUp() throws Exception {
    System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "true");
    invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "true");
      }
    });
    super.setUp();
  }

  @Override
  public void tearDown2() throws java.lang.Exception {
    System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "false");
    invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "false");
      }
    });
    super.tearDown2();
  }

  public void testCharConstant() throws Exception {
    // Start one client and three servers
    startServerVMs(3, 0, "SG1");
    startClientVMs(1, 0, null);

    clientSQLExecute(1, "create table tpk ( id int primary key, "
        + "vid varchar(10), sid int) partition by primary key");
    clientSQLExecute(1, "create table tdriver ( id int primary key, "
        + "vid varchar(10), sid int) partition by column(sid)");
    
    clientSQLExecute(1, "create schema EMP");
    clientSQLExecute(1, "create table EMP.PARTTABLE1 (IDP1 int not null, "
        + " DESCRIPTIONP1 varchar(1024) not null, "
        + "ADDRESSP1 varchar(1024) not null, primary key (DESCRIPTIONP1))"
        + "PARTITION BY primary key")
        ;
    clientSQLExecute(1, "create table EMP.PARTTABLE2 (IDP2 int not null, "
        + " DESCRIPTIONP2 varchar(1024) not null, "
        + "ADDRESSP2 varchar(1024) not null, primary key (DESCRIPTIONP2))"
        + "PARTITION BY primary key")
        ;

    for (int i = 1; i <= 4; ++i) {
      clientSQLExecute(1, "insert into EMP.PARTTABLE1 values (" + i
          + ", 'First1" + i + "', 'J1 604')");
      clientSQLExecute(1, "insert into EMP.PARTTABLE2 values (" + i
          + ", 'First2" + i + "', 'J2 604')");
    }

    {
      HashSet<Integer> expected = new HashSet<Integer>();
      expected.add(3);
      {
        String query = "select * from EMP.PARTTABLE1 p1, EMP.PARTTABLE2 p2 "
            + " where " + " IDP1 = IDP2 " + " and IDP2 = 3 "
            + " and p1.ADDRESSP1 = 'J1 604'";

        Connection conn = TestUtil.getConnection();
        Statement s1 = conn.createStatement();
        ResultSet rs = s1.executeQuery(query);
        while (rs.next()) {
          assertTrue(expected.remove(rs.getInt(1)));
        }
        assertTrue(expected.isEmpty());
      }
    }
  }
}  

