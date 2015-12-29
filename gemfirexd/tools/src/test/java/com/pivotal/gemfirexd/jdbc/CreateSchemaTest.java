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
//
//  CreateSchemaTest.java
//  gemfire
//
//  Created by Eric Zoerner on 6/24/08.

package com.pivotal.gemfirexd.jdbc;

import java.sql.*;
import java.util.Properties;

import junit.textui.TestRunner;
import junit.framework.TestSuite;

import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdPartitionByExpressionResolver;

public class CreateSchemaTest extends JdbcTestBase {

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(CreateSchemaTest.class));
  }

  public CreateSchemaTest(String name) {
    super(name);
  }

  public void testCreateAndDropSchema() throws SQLException {
    setupConnection();

    // verify that the schema region "EMP" doesn't exist
    assertNull(Misc.getRegionForTable("EMP", false));

    sqlExecute("CREATE SCHEMA EMP", true);

    // verify that the schema region "EMP" exists
    assertNotNull(Misc.getRegionForTable("EMP", false));

    sqlExecute("CREATE TABLE EMP.AVAILABILITY "
        + "(HOTEL_ID INT NOT NULL, BOOKING_DATE DATE NOT NULL, "
        + "ROOMS_TAKEN INT, CONSTRAINT HOTELAVAIL_PK PRIMARY KEY "
        + "(HOTEL_ID, BOOKING_DATE))", false);

    // now try to create the same schema again and get an expected exception
    try {
      sqlExecute("CREATE SCHEMA EMP", true);
      fail("Unexpectedly did not get exception when trying to create a"
          + " schema that already exists");
    } catch (SQLException ex) {
      if (!"X0Y68".equals(ex.getSQLState())) {
        throw ex;
      }
    }

    // delete the table
    sqlExecute("drop table EMP.AVAILABILITY", true);

    // drop the schema
    sqlExecute("DROP SCHEMA EMP RESTRICT", true);

    // verify that the table region doesn't exist
    assertNull(Misc.getRegionForTable("EMP.AVAILABILITY", false));

    // verify that the schema region "EMP" doesn't exist
    assertNull(Misc.getRegionForTable("EMP", false));

    // now try to create a table in the dropped schema.
    // schema should be automatically created
    sqlExecute("CREATE TABLE EMP.AVAILABILITY "
        + "(HOTEL_ID INT NOT NULL, BOOKING_DATE DATE NOT NULL, "
        + "ROOMS_TAKEN INT, CONSTRAINT HOTELAVAIL_PK PRIMARY KEY "
        + "(HOTEL_ID, BOOKING_DATE))", false);

    // verify that the schema region "EMP" exists
    assertNotNull(Misc.getRegionForTable("EMP", false));

    // delete the table
    sqlExecute("drop table EMP.AVAILABILITY", true);

    // drop the schema
    sqlExecute("DROP SCHEMA EMP RESTRICT", false);

    // verify that the table region doesn't exist
    assertNull(Misc.getRegionForTable("EMP.AVAILABILITY", false));

    // verify that the schema region "EMP" doesn't exist
    assertNull(Misc.getRegionForTable("EMP", false));
  }
  // Test whether the entry in SYSSCHEMAS is as expected
  public void testSYSSCHEMAS() throws Exception {
    setupConnection();
    sqlExecute("CREATE SCHEMA EMP DEFAULT SERVER GROUPS (SG1)",
        Boolean.TRUE /* use prep statement */);

    RegionAttributesCreation expectedAttrs = new RegionAttributesCreation();
    expectedAttrs.setScope(Scope.DISTRIBUTED_NO_ACK);
    expectedAttrs.setDataPolicy(DataPolicy.EMPTY);
    expectedAttrs.setConcurrencyChecksEnabled(false);
    expectedAttrs.setAllHasFields(true);
    expectedAttrs.setHasDiskDirs(false);
    expectedAttrs.setHasDiskWriteAttributes(false);
    // Test the schema region attributes on client
    verifyRegionProperties("EMP", null, TestUtil
        .regionAttributesToXML(expectedAttrs));

    // Check for both index and non-index lookup in SYSSCHEMAS

    // Test the fields from SYSSCHEMAS on client with index lookup
    sqlExecuteVerifyText("SELECT SCHEMANAME,"
        + " DEFAULTSERVERGROUPS FROM SYS.SYSSCHEMAS WHERE SCHEMANAME='EMP'",
        TestUtil.getResourcesDir() + "/lib/checkSchema.xml",
        "emp_sg", Boolean.TRUE /* use prep statement */,
        Boolean.FALSE /* do not check for type info */);

    // Test the fields from SYSSCHEMAS on client with non-index table scan
    sqlExecuteVerifyText(
        "SELECT SCHEMANAME, DEFAULTSERVERGROUPS FROM SYS.SYSSCHEMAS "
            + "WHERE DEFAULTSERVERGROUPS='SG1'", getResourcesDir()
            + "/lib/checkSchema.xml", "emp_sg",
        Boolean.TRUE /* use prep statement */,
        Boolean.FALSE /* do not check for type info */);
  }

  // Test for server group in standalone VM
  public void testDefaultServerGroup() throws Exception {
    Properties props = new Properties();
    props.setProperty("server-groups", "sg1");
    setupConnection(props);
    sqlExecute("CREATE SCHEMA EMP DEFAULT SERVER GROUPS (SG1)",
        Boolean.TRUE /* use prep statement */);

    RegionAttributesCreation expectedAttrs = new RegionAttributesCreation();
    expectedAttrs.setScope(Scope.DISTRIBUTED_NO_ACK);
    expectedAttrs.setDataPolicy(DataPolicy.EMPTY);
    expectedAttrs.setConcurrencyChecksEnabled(false);
    expectedAttrs.setAllHasFields(true);
    expectedAttrs.setHasDiskDirs(false);
    expectedAttrs.setHasDiskWriteAttributes(false);
    // Test the schema region attributes on VM
    verifyRegionProperties("EMP", null,
        TestUtil.regionAttributesToXML(expectedAttrs));

    //--- Check for both index and non-index lookup in SYSSCHEMAS

    // Test the fields from SYSSCHEMAS on client with index lookup
    sqlExecuteVerifyText("SELECT SCHEMANAME,"
        + " DEFAULTSERVERGROUPS FROM SYS.SYSSCHEMAS WHERE SCHEMANAME='EMP'",
        TestUtil.getResourcesDir() + "/lib/checkSchema.xml",
        "emp_sg", Boolean.TRUE /* use prep statement */,
        Boolean.FALSE /* do not check for type info */);

    // Test the fields from SYSSCHEMAS on client with non-index table scan
    sqlExecuteVerifyText(
        "SELECT SCHEMANAME, DEFAULTSERVERGROUPS FROM SYS.SYSSCHEMAS "
            + "WHERE DEFAULTSERVERGROUPS='SG1'", getResourcesDir()
            + "/lib/checkSchema.xml", "emp_sg",
        Boolean.TRUE /* use prep statement */,
        Boolean.FALSE /* do not check for type info */);

    //--- Create a table and verify that the default server groups is inherited

    sqlExecute("create table EMP.PARTITIONTESTTABLE (ID int not null, "
        + " SECONDID int not null, THIRDID int not null, primary key "
        + "(SECONDID)) PARTITION BY COLUMN (ID, SECONDID)",
        Boolean.TRUE /* use prep statement */);

    expectedAttrs = new RegionAttributesCreation();
    expectedAttrs.setDataPolicy(DataPolicy.PARTITION);
    expectedAttrs.setInitialCapacity(TestUtil.TEST_DEFAULT_INITIAL_CAPACITY);
    expectedAttrs.setConcurrencyChecksEnabled(false);
    expectedAttrs.setAllHasFields(true);
    expectedAttrs.setHasScope(false);
    expectedAttrs.setHasDiskDirs(false);
    expectedAttrs.setHasDiskWriteAttributes(false);
    PartitionAttributes pattrs = new PartitionAttributesFactory()
        .setPartitionResolver(new GfxdPartitionByExpressionResolver()).create();
    expectedAttrs.setPartitionAttributes(pattrs);

    // Test the table region attributes on VM
    verifyRegionProperties("EMP", "PARTITIONTESTTABLE", TestUtil
        .regionAttributesToXML(expectedAttrs));

    //--- Same test for replicated table

    sqlExecute("create table EMP.REPLICATEDTESTTABLE (ID int not null, "
        + " SECONDID int not null, THIRDID int not null, primary key "
        + "(SECONDID)) REPLICATE", Boolean.TRUE /* use prep statement */);

    expectedAttrs = new RegionAttributesCreation();
    expectedAttrs.setDataPolicy(DataPolicy.REPLICATE);
    expectedAttrs.setScope(Scope.DISTRIBUTED_ACK);
    expectedAttrs.setInitialCapacity(TestUtil.TEST_DEFAULT_INITIAL_CAPACITY);
    expectedAttrs.setConcurrencyChecksEnabled(false);
    expectedAttrs.setAllHasFields(true);
    expectedAttrs.setHasDiskDirs(false);
    expectedAttrs.setHasDiskWriteAttributes(false);
    // Test the table region attributes on VM
    verifyRegionProperties("EMP", "REPLICATEDTESTTABLE", TestUtil
        .regionAttributesToXML(expectedAttrs));
  }

}
