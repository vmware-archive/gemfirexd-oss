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

import java.util.Properties;

import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;
import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdPartitionByExpressionResolver;

/**
 * Tests for "CREATE SCHEMA" including check for distributed schema creation
 * and check for Gemfire "DEFAULT SERVER GROUPS" extension.
 * 
 * @author swale
 * @since 6.0
 */
public class CreateSchemaDUnit extends DistributedSQLTestBase {
  private static final long serialVersionUID = 2039821312825185649L;

  public CreateSchemaDUnit(String name) {
    super(name);
  }

  public void testDistributedCreateAndServerGroup() throws Exception {
    // disable username for this test to enable checking for 'APP'
    TestUtil.currentUserName = null;
    // Start one client and two servers
    startServerVMs(2, 0, "sg1");
    startClientVMs(1, 0, null);

    // Create a schema
    clientSQLExecute(1, "create schema EMP");

    // Create a set of expected region attributes for the schema
    RegionAttributesCreation expectedAttrs = new RegionAttributesCreation();
    expectedAttrs.setScope(Scope.DISTRIBUTED_NO_ACK);
    expectedAttrs.setDataPolicy(DataPolicy.EMPTY);
    expectedAttrs.setConcurrencyChecksEnabled(false);
    expectedAttrs.setAllHasFields(true);
    expectedAttrs.setHasDiskDirs(false);
    expectedAttrs.setHasDiskWriteAttributes(false);
    // Test the schema region attributes on client and servers
    clientVerifyRegionProperties(1, "EMP", null, expectedAttrs);
    serverVerifyRegionProperties(1, "EMP", null, expectedAttrs);
    serverVerifyRegionProperties(2, "EMP", null, expectedAttrs);

    // Check for both index and non-index lookup in SYSSCHEMAS

    // Test the fields from SYSSCHEMAS on client and servers with index lookup
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 },
        "select schemaname, authorizationid, defaultservergroups "
            + "from SYS.SYSSCHEMAS where schemaname='EMP'", TestUtil.getResourcesDir()
            + "/lib/checkSchema.xml", "emp_nosg");

    // Test the fields from SYSSCHEMAS on client and servers with non-index
    // table scan
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 },
        "select schemaname, authorizationid, defaultservergroups "
            + "from SYS.SYSSCHEMAS where schemaname like 'EMP%'", TestUtil.getResourcesDir()
            + "/lib/checkSchema.xml", "emp_nosg");

    // Drop the schema
    clientSQLExecute(1, "drop schema EMP restrict");

    // Test that the schema region has been destroyed on client and servers
    clientVerifyRegionProperties(1, "EMP", null, null);
    serverVerifyRegionProperties(1, "EMP", null, null);
    serverVerifyRegionProperties(2, "EMP", null, null);

    // Test that there is nothing in the SYSSCHEMAS table as well.
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 },
        "select * from SYS.SYSSCHEMAS where schemaname like 'EMP%'", TestUtil.getResourcesDir()
            + "/lib/checkSchema.xml", "empty");

    // Re-create the schema with default server groups GemFire extension
    clientSQLExecute(1, "create schema EMP default server groups (SG1)");

    // Test the schema region attributes on client and servers
    clientVerifyRegionProperties(1, "EMP", null, expectedAttrs);
    serverVerifyRegionProperties(1, "EMP", null, expectedAttrs);
    serverVerifyRegionProperties(2, "EMP", null, expectedAttrs);

    // Check for both index and non-index lookup in SYSSCHEMAS

    // Test the fields from SYSSCHEMAS on client and servers with index lookup
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 },
        "select schemaname, defaultservergroups "
            + "from SYS.SYSSCHEMAS where schemaname='EMP'", TestUtil.getResourcesDir()
            + "/lib/checkSchema.xml", "emp_sg");

    // Test the fields from SYSSCHEMAS on client and servers with non-index
    // table scan
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 },
        "select schemaname, defaultservergroups "
            + "from SYS.SYSSCHEMAS where defaultservergroups='SG1'", TestUtil.getResourcesDir()
            + "/lib/checkSchema.xml", "emp_sg");

    // Drop the schema
    clientSQLExecute(1, "drop schema EMP restrict");
  }

  public void testDistributedSchemaTableCreate() throws Exception {
    // Start one client and two servers
    startVMs(1, 1);
    startVMs(0, 1, 0, "SG1", null);

    // Create a schema with default server groups GemFire extension
    clientSQLExecute(1, "create schema EMP default server groups (SG1)");

    // Create the table and insert a row
    clientSQLExecute(1, "create table EMP.TESTTABLE (ID int not null, "
        + "DESCRIPTION varchar(1024) not null, primary key (ID))");
    clientSQLExecute(1, "insert into EMP.TESTTABLE values (1, 'First')");

    // Create a set of expected region attributes for the table
    RegionAttributesCreation expectedAttrs = new RegionAttributesCreation();
    expectedAttrs.setDataPolicy(DataPolicy.PARTITION);
    expectedAttrs.setConcurrencyChecksEnabled(false);
    PartitionAttributes pa = new PartitionAttributesFactory()
        .setPartitionResolver(new GfxdPartitionByExpressionResolver()).create();
    expectedAttrs.setPartitionAttributes(pa);
    expectedAttrs.setInitialCapacity(TestUtil.TEST_DEFAULT_INITIAL_CAPACITY);
    expectedAttrs.setConcurrencyChecksEnabled(false);
    expectedAttrs.setAllHasFields(true);
    expectedAttrs.setHasScope(false);
    expectedAttrs.setHasDiskDirs(false);
    expectedAttrs.setHasDiskWriteAttributes(false);
    // Check the table attributes on the server with SG1 group
    serverVerifyRegionProperties(2, "EMP", "TESTTABLE", expectedAttrs);

    // Check that the local-max-memory PR attribute is zero on the client and
    // first server
    pa = new PartitionAttributesFactory(expectedAttrs
        .getPartitionAttributes()).setLocalMaxMemory(0).create();
    expectedAttrs.setPartitionAttributes(pa);
    clientVerifyRegionProperties(1, "EMP", "TESTTABLE", expectedAttrs);
    serverVerifyRegionProperties(1, "EMP", "TESTTABLE", expectedAttrs);

    // Check the inserted row on the client.
    // Due to the table scan nature of the query, it shall succeed even on
    // the client even though client has only an accessor region
    // and distributed query may not work.
    sqlExecuteVerify(new int[] { 1 }, null,
        "select ID, DESCRIPTION from EMP.TESTTABLE", TestUtil.getResourcesDir()
            + "/lib/checkSchema.xml", "emp_tab_sel",
        false /* do not use prep stmt */,
        false /* do not check for type information */);

    // Drop the table
    clientSQLExecute(1, "drop table EMP.TESTTABLE");

    // Test that the table has been destroyed on client and servers
    clientVerifyRegionProperties(1, "EMP", "TESTTABLE", null);
    serverVerifyRegionProperties(1, "EMP", "TESTTABLE", null);
    serverVerifyRegionProperties(2, "EMP", "TESTTABLE", null);

    // Test that there is nothing in the SYSTABLES as well.
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 },
        "select * from SYS.SYSTABLES where tablename='TESTTABLE'", TestUtil.getResourcesDir()
            + "/lib/checkSchema.xml", "empty");

    // Drop the schema
    clientSQLExecute(1, "drop schema EMP restrict");

    // Test that the schema region has been destroyed on client and servers
    clientVerifyRegionProperties(1, "EMP", null, null);
    serverVerifyRegionProperties(1, "EMP", null, null);
    serverVerifyRegionProperties(2, "EMP", null, null);

    // Test that there is nothing in the SYSSCHEMAS table as well.
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2 },
        "select * from SYS.SYSSCHEMAS where schemaname='EMP'", TestUtil.getResourcesDir()
            + "/lib/checkSchema.xml", "empty");
  }

  public void testDistributedSchemaCreateWithMultipleServerGroups()
      throws Exception {
    // Start one client and four servers
    final Properties props = new Properties();
    props.setProperty(TestUtil.TEST_SKIP_DEFAULT_INITIAL_CAPACITY, "true");
    AsyncVM async1 = invokeStartServerVM(1, 0, null, props);
    AsyncVM async2 = invokeStartServerVM(2, 0, "SG1", props);
    AsyncVM async3 = invokeStartServerVM(3, 0, "SG1, Sg2", props);
    AsyncVM async4 = invokeStartServerVM(4, 0, "sg2", props);
    startClientVMs(1, 0, null, props);
    joinVMs(true, async1, async2, async3, async4);

    // Create a schema with default server groups GemFire extension
    clientSQLExecute(1, "create schema EMP default server groups (SG1, SG2)");

    // Create the table and insert a row
    clientSQLExecute(1, "create table EMP.TESTTABLE (ID int not null, "
        + "DESCRIPTION varchar(1024) not null, primary key (ID))");
    clientSQLExecute(1, "insert into EMP.TESTTABLE values (1, 'First')");

    // Create a set of expected region attributes for the table
    RegionAttributesCreation expectedAttrs = new RegionAttributesCreation();
    expectedAttrs.setDataPolicy(DataPolicy.PARTITION);
    expectedAttrs.setConcurrencyChecksEnabled(false);
    PartitionAttributes pa = new PartitionAttributesFactory()
        .setPartitionResolver(new GfxdPartitionByExpressionResolver()).create();
    expectedAttrs.setPartitionAttributes(pa);
    expectedAttrs.setInitialCapacity(GfxdConstants.DEFAULT_INITIAL_CAPACITY);
    expectedAttrs.setConcurrencyChecksEnabled(false);
    expectedAttrs.setAllHasFields(true);
    expectedAttrs.setHasScope(false);
    expectedAttrs.setHasDiskDirs(false);
    expectedAttrs.setHasDiskWriteAttributes(false);
    // Check the table attributes on the servers in SG1 and SG2 groups
    serverVerifyRegionProperties(2, "EMP", "TESTTABLE", expectedAttrs);
    serverVerifyRegionProperties(3, "EMP", "TESTTABLE", expectedAttrs);
    serverVerifyRegionProperties(4, "EMP", "TESTTABLE", expectedAttrs);

    // Check that the local-max-memory PR attribute is zero on the client and
    // other servers
    PartitionAttributes pa2 = new PartitionAttributesFactory(expectedAttrs
        .getPartitionAttributes()).setLocalMaxMemory(0).create();
    expectedAttrs.setPartitionAttributes(pa2);
    clientVerifyRegionProperties(1, "EMP", "TESTTABLE", expectedAttrs);
    serverVerifyRegionProperties(1, "EMP", "TESTTABLE", expectedAttrs);

    // Check the inserted row on the client and servers.
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2, 3, 4 },
        "select ID, DESCRIPTION from EMP.TESTTABLE", TestUtil.getResourcesDir()
            + "/lib/checkSchema.xml", "emp_tab_sel",
        false /* do not use prep stmt */,
        false /* do not check for type information */);

    // Drop the table
    clientSQLExecute(1, "drop table EMP.TESTTABLE");

    // Test that the table has been destroyed on client and servers
    clientVerifyRegionProperties(1, "EMP", "TESTTABLE", null);
    serverVerifyRegionProperties(1, "EMP", "TESTTABLE", null);
    serverVerifyRegionProperties(2, "EMP", "TESTTABLE", null);
    serverVerifyRegionProperties(3, "EMP", "TESTTABLE", null);
    serverVerifyRegionProperties(4, "EMP", "TESTTABLE", null);

    // Test that there is nothing in the SYSTABLES as well.
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2, 3, 4 },
        "select * from SYS.SYSTABLES where tablename='TESTTABLE'", TestUtil.getResourcesDir()
            + "/lib/checkSchema.xml", "empty");

    //--------------- Now create the table in one server group overriding
    //--------------- the schema specification

    // Create the table and insert a row
    serverSQLExecute(1, "create table EMP.TESTTABLE (ID int not null, "
        + "DESCRIPTION varchar(1024) not null, primary key (ID)) "
        + "server groups (SG2)");
    serverSQLExecute(2, "insert into EMP.TESTTABLE values (1, 'First')");

    // Check that the local-max-memory PR attribute is zero on the client and
    // other servers
    clientVerifyRegionProperties(1, "EMP", "TESTTABLE", expectedAttrs);
    serverVerifyRegionProperties(1, "EMP", "TESTTABLE", expectedAttrs);
    serverVerifyRegionProperties(2, "EMP", "TESTTABLE", expectedAttrs);

    // Check the table attributes on the server in appropriate group
    expectedAttrs.setPartitionAttributes(pa);
    expectedAttrs.setInitialCapacity(GfxdConstants.DEFAULT_INITIAL_CAPACITY);
    expectedAttrs.setConcurrencyChecksEnabled(false);
    serverVerifyRegionProperties(3, "EMP", "TESTTABLE", expectedAttrs);
    serverVerifyRegionProperties(4, "EMP", "TESTTABLE", expectedAttrs);

    // Check the inserted row on the client and servers.
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2, 3, 4 },
        "select ID, DESCRIPTION from EMP.TESTTABLE", TestUtil.getResourcesDir()
            + "/lib/checkSchema.xml", "emp_tab_sel",
        false /* do not use prep stmt */,
        false /* do not check for type information */);

    // Drop the table
    serverSQLExecute(2, "drop table EMP.TESTTABLE");

    // Test that the table has been destroyed on client and servers
    clientVerifyRegionProperties(1, "EMP", "TESTTABLE", null);
    serverVerifyRegionProperties(1, "EMP", "TESTTABLE", null);
    serverVerifyRegionProperties(2, "EMP", "TESTTABLE", null);
    serverVerifyRegionProperties(3, "EMP", "TESTTABLE", null);
    serverVerifyRegionProperties(4, "EMP", "TESTTABLE", null);

    // Test that there is nothing in the SYSTABLES as well.
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2, 3, 4 },
        "select * from SYS.SYSTABLES where tablename='TESTTABLE'", TestUtil.getResourcesDir()
            + "/lib/checkSchema.xml", "empty");

    //--------------- Now do the above test for replicated table

    // Create the table and insert a row
    serverSQLExecute(1, "create table EMP.TESTTABLE (ID int not null, "
        + "DESCRIPTION varchar(1024) not null, primary key (ID)) "
        + "replicate server groups (SG2)");
    clientSQLExecute(1, "insert into EMP.TESTTABLE values (1, 'First')");

    expectedAttrs = new RegionAttributesCreation();
    expectedAttrs.setDataPolicy(DataPolicy.REPLICATE);
    expectedAttrs.setScope(Scope.DISTRIBUTED_ACK);
    expectedAttrs.setInitialCapacity(GfxdConstants.DEFAULT_INITIAL_CAPACITY);
    expectedAttrs.setConcurrencyChecksEnabled(false);
    expectedAttrs.setAllHasFields(true);
    expectedAttrs.setHasDiskDirs(false);
    expectedAttrs.setHasDiskWriteAttributes(false);
    // Check the table attributes on the server in appropriate group
    serverVerifyRegionProperties(3, "EMP", "TESTTABLE", expectedAttrs);
    serverVerifyRegionProperties(4, "EMP", "TESTTABLE", expectedAttrs);

    // Check that the data policy is empty on the client and other servers
    expectedAttrs.setDataPolicy(DataPolicy.EMPTY);
    clientVerifyRegionProperties(1, "EMP", "TESTTABLE", expectedAttrs);
    serverVerifyRegionProperties(1, "EMP", "TESTTABLE", expectedAttrs);
    serverVerifyRegionProperties(2, "EMP", "TESTTABLE", expectedAttrs);

    // Check the inserted row on the client and servers.
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2, 3, 4 },
        "select ID, DESCRIPTION from EMP.TESTTABLE", TestUtil.getResourcesDir()
            + "/lib/checkSchema.xml", "emp_tab_sel",
        false /* do not use prep stmt */,
        false /* do not check for type information */);

    // Drop the table
    serverSQLExecute(4, "drop table EMP.TESTTABLE");

    // Test that the table has been destroyed on client and servers
    clientVerifyRegionProperties(1, "EMP", "TESTTABLE", null);
    serverVerifyRegionProperties(1, "EMP", "TESTTABLE", null);
    serverVerifyRegionProperties(2, "EMP", "TESTTABLE", null);
    serverVerifyRegionProperties(3, "EMP", "TESTTABLE", null);
    serverVerifyRegionProperties(4, "EMP", "TESTTABLE", null);

    // Test that there is nothing in the SYSTABLES as well.
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2, 3, 4 },
        "select * from SYS.SYSTABLES where tablename='TESTTABLE'", TestUtil.getResourcesDir()
            + "/lib/checkSchema.xml", "empty");

    //--------------- cleanup at end of test

    // Drop the schema
    clientSQLExecute(1, "drop schema EMP restrict");

    // Test that the schema region has been destroyed on client and servers
    clientVerifyRegionProperties(1, "EMP", null, null);
    serverVerifyRegionProperties(1, "EMP", null, null);
    serverVerifyRegionProperties(2, "EMP", null, null);
    serverVerifyRegionProperties(3, "EMP", null, null);
    serverVerifyRegionProperties(4, "EMP", null, null);

    // Test that there is nothing in the SYSSCHEMAS table as well.
    sqlExecuteVerify(new int[] { 1 }, new int[] { 1, 2, 3, 4 },
        "select * from SYS.SYSSCHEMAS where schemaname='EMP'", TestUtil.getResourcesDir()
            + "/lib/checkSchema.xml", "empty");
  }
}
