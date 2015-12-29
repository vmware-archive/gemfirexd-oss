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

import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;

/**
 * Tests for "CREATE INDEX" including check for distributed index creation and
 * check for Gemfire "GLOBAL HASH" extension.
 * 
 * @author swale
 * @since 6.0
 */
@SuppressWarnings("serial")
public class CreateIndexDUnit extends DistributedSQLTestBase {

  public CreateIndexDUnit(String name) {
    super(name);
  }

  /**
   * This tests whether a normal local index can be created successfully in a
   * distributed manner.
   */
  public void testDistributedIndexCreate() throws Exception {
    // Start one client and two servers
    startVMs(1, 2);

    // Create a table
    clientSQLExecute(1, "create table ORDERS (ID int not null, VOL int, "
        + "SECURITY_ID varchar(10), NUM int, CUST_ID int)");

    // Create an index on the table
    clientSQLExecute(1, "create index VOLUMEIDX on ORDERS(VOL)");

    // Test the fields from SYSCONGLOMERATES on client and servers
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 },
        "select CONGLOMERATENAME, DESCRIPTOR from SYS.SYSCONGLOMERATES "
            + "where CONGLOMERATENAME = 'VOLUMEIDX'", TestUtil.getResourcesDir()
            + "/lib/checkIndex.xml", "orders_nogh");

    // Test the fields from SYSCONGLOMERATES on client and servers with
    // non-index table scan
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 },
        "select CONGLOMERATENAME, DESCRIPTOR from SYS.SYSCONGLOMERATES "
            + "where CONGLOMERATENAME like 'VOLUMEIDX%'", TestUtil.getResourcesDir()
            + "/lib/checkIndex.xml", "orders_nogh");
  }

  /**
   * This tests whether a "GLOBAL HASH" index can be created successfully in a
   * distributed manner.
   */
  public void testDistributedGlobalHashIndexCreate() throws Exception {
    // Start one client and two servers
    startVMs(1, 2);

    // Create a table
    clientSQLExecute(1, "create table ORDERS (ID int not null, VOL int, "
        + "SECURITY_ID varchar(10), NUM int, CUST_ID int)");

    // Create an index on the table
    clientSQLExecute(1, "create global hash index VOLUMEIDX on ORDERS(VOL)");

    // set of expected region attributes for the GLOBAL HASH index region
    RegionAttributesCreation expectedAttrs = new RegionAttributesCreation();
    expectedAttrs.setDataPolicy(DataPolicy.PARTITION);
    expectedAttrs.setConcurrencyChecksEnabled(false);
    PartitionAttributes pa = new PartitionAttributesFactory().create();
    expectedAttrs.setPartitionAttributes(pa);
    expectedAttrs.setInitialCapacity(TestUtil.TEST_DEFAULT_INITIAL_CAPACITY);
    expectedAttrs.setConcurrencyChecksEnabled(false);
    expectedAttrs.setAllHasFields(true);
    expectedAttrs.setHasScope(false);
    expectedAttrs.setHasDiskDirs(false);
    expectedAttrs.setHasDiskWriteAttributes(false);

    // Check the table attributes on the client and servers
    serverVerifyRegionProperties(1, null, "VOLUMEIDX", expectedAttrs);
    serverVerifyRegionProperties(2, null, "VOLUMEIDX", expectedAttrs);

    // Check that the local-max-memory PR attribute is zero on the client
    pa = new PartitionAttributesFactory().setLocalMaxMemory(0).create();
    expectedAttrs.setPartitionAttributes(pa);
    clientVerifyRegionProperties(1, null, "VOLUMEIDX", expectedAttrs);

    // Test the fields from SYSCONGLOMERATES on client and servers
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 },
        "select CONGLOMERATENAME, DESCRIPTOR from SYS.SYSCONGLOMERATES "
            + "where CONGLOMERATENAME='VOLUMEIDX'", TestUtil.getResourcesDir()
            + "/lib/checkIndex.xml", "orders_gh");

    // Test the fields from SYSCONGLOMERATES on client and servers with
    // non-index table scan
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 },
        "select CONGLOMERATENAME, DESCRIPTOR from SYS.SYSCONGLOMERATES "
            + "where CONGLOMERATENAME like 'VOLUMEIDX%'", TestUtil.getResourcesDir()
            + "/lib/checkIndex.xml", "orders_gh");
  }
}
