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

import com.gemstone.gemfire.cache.CacheException;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import io.snappydata.test.dunit.SerializableRunnable;

@SuppressWarnings("serial")
public class DeltaMergeAndIndexUpdateDUnit extends DistributedSQLTestBase {

  public DeltaMergeAndIndexUpdateDUnit(String name) {
    super(name);
  }

  public void testIndexCreateDuringGII() throws Exception {
    // Start one client and two servers
    startVMs(2, 2);

    // Create a table
    clientSQLExecute(
        1,
        "create table ORDERS (ID int not null, VOL int, "
            + "SEC_ID varchar(10), CUST_ID varchar(10), constraint id_pk primary key (ID), "
            + "constraint seccust_id unique (SEC_ID, CUST_ID)) REDUNDANCY 1");
    for (int i = 0; i < 1000; i++) {
      String insertStmnt = getInsertStmnt(i);
      clientSQLExecute(1, insertStmnt);
    }

    stopVMNums(-1);
    final SerializableRunnable doUpdates = new SerializableRunnable(
        "doUpdates") {
      @Override
      public void run() throws CacheException {
        for (int i = 0; i < 1000; i++) {
          String updateStmnt = "update ORDERS set SEC_ID='nid"+i+"' where ID=100";
          try {
            clientSQLExecute(1, updateStmnt);
          } catch (Exception e) {
            fail("unexpected exception while executing update", e);
          }
        }
      }
    };
    Thread t = new Thread(doUpdates);
    t.start();
    restartVMNums(-1);
    if (t.isAlive()) {
      t.join();
    }

    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from ORDERS where ID = 100", TestUtil.getResourcesDir()
            + "/lib/checkIndex.xml", "DeltaMergeAndIndexUpdate");
  }

  private String getInsertStmnt(int i) {
    String stmnt = "insert into orders values(" + i + "," + i * 10 + ", 'sid"
        + i + "', 'cust" + i + "')";
    return stmnt;
  }
}
