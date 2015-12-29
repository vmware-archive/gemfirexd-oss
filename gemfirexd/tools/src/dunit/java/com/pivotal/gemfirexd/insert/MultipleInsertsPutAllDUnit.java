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
package com.pivotal.gemfirexd.insert;

import java.sql.SQLException;

import com.gemstone.gemfire.cache.EntryExistsException;
import com.gemstone.gemfire.cache.PartitionedRegionDistributionException;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;

@SuppressWarnings("serial")
public class MultipleInsertsPutAllDUnit extends DistributedSQLTestBase {

  public MultipleInsertsPutAllDUnit(String name) {
    super(name);
  }

  @Override
  protected String reduceLogging() {
    return "config";
  }

  public void testMultInsertPK() throws Exception {
    startVMs(1, 2);

    GemFireXDQueryObserver putAllObserver = new GemFireXDQueryObserverAdapter() {
      @Override
      public void putAllCalledWithMapSize(int size) {
        assertEquals(3, size);
      }
    };
    GemFireXDQueryObserverHolder.setInstance(putAllObserver);
    clientSQLExecute(1, "create schema emp");
    clientSQLExecute(1,
        "create table EMP.PARTITIONTESTTABLE_PARENT (ID int primary key,"
            + " SECONDID int , THIRDID int )");
    clientSQLExecute(1,
        "insert into EMP.PARTITIONTESTTABLE_PARENT values(2,4,6), (4,6,8), (6,8,10)");
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.PARTITIONTESTTABLE_PARENT", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "listenerWriter_Test1");
  }

  public void testMultInsertNoPK() throws Exception {
    startVMs(1, 2);
    GemFireXDQueryObserver putAllObserver = new GemFireXDQueryObserverAdapter() {
      @Override
      public void putAllCalledWithMapSize(int size) {
        assertEquals(3, size);
      }
    };
    GemFireXDQueryObserverHolder.setInstance(putAllObserver);
    clientSQLExecute(1, "create schema emp");
    clientSQLExecute(1, "create table EMP.PARTITIONTESTTABLE_PARENT (ID int,"
        + " SECONDID int , THIRDID int )");
    clientSQLExecute(1,
        "insert into EMP.PARTITIONTESTTABLE_PARENT values(2,4,6), (4,6,8), (6,8,10)");
    sqlExecuteVerify(new int[] { 1 }, null,
        "select * from EMP.PARTITIONTESTTABLE_PARENT", TestUtil.getResourcesDir()
            + "/lib/checkCreateTable.xml", "listenerWriter_Test1");
  }

  public void testMultInsertConstraintViolation() throws Exception {
    // start a client and two servers
    startVMs(1, 2);

    GemFireXDQueryObserver putAllObserver = new GemFireXDQueryObserverAdapter() {
      @Override
      public void putAllCalledWithMapSize(int size) {
        assertEquals(3, size);
      }
    };
    GemFireXDQueryObserverHolder.setInstance(putAllObserver);
    clientSQLExecute(1, "create schema emp");
    clientSQLExecute(1,
        "create table EMP.PARTITIONTESTTABLE_PARENT (ID int primary key,"
            + " SECONDID int , THIRDID int )");
    clientSQLExecute(1,
        "insert into EMP.PARTITIONTESTTABLE_PARENT values(6,8,10)");
    addExpectedException(new int[] { 1 }, new int[] { 1, 2 }, new Object[] {
        SQLException.class, EntryExistsException.class,
        PartitionedRegionDistributionException.class });
    checkKeyViolation(1, "insert into EMP.PARTITIONTESTTABLE_PARENT "
        + "values(2,4,6), (4,6,8), (6,9,11)");
    checkKeyViolation(-1, "insert into EMP.PARTITIONTESTTABLE_PARENT "
        + "values(2,4,6), (4,6,8), (6,9,11)");
    checkKeyViolation(-2, "insert into EMP.PARTITIONTESTTABLE_PARENT "
        + "values(2,4,6), (4,6,8), (6,9,11)");
    removeExpectedException(new int[] { 1 }, new int[] { 1, 2 }, new Object[] {
        SQLException.class, EntryExistsException.class,
        PartitionedRegionDistributionException.class });
  }
}
