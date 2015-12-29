/*

Derby - Class org.apache.derbyTesting.functionTests.tests.lang.unarySimpleTest

Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/

/*
 * Changes for GemFireXD distributed data platform (some marked by "GemStone changes")
 *
 * Portions Copyright (c) 2010-2015 Pivotal Software, Inc. All rights reserved.
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



package org.apache.derbyTesting.functionTests.tests.lang;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;
  /**
   * This test was written as a simple unary plus/minus test for GemFireXD.
   */

public class UnarySimpleTest extends BaseJDBCTestCase {
  /**
     * Public constructor required for running test as standalone JUnit.
     */
  public UnarySimpleTest(String name) {
    super(name);
  }
  /**
     * Create a suite of tests.
     */
  public static Test suite() {
    return   TestConfiguration.defaultSuite(UnarySimpleTest.class);
  }
   /**
     * Set the fixture up with tables
     */
  protected void setUp() throws SQLException {
  }
  /**
     * Tear-down the fixture by removing the tables
     */
  protected void tearDown() throws Exception {
// GemStone changes BEGIN
          super.preTearDown();
// GemStone changes END
    super.tearDown();
  }

  public void testSimpleUnaryPlus() throws SQLException {
    getConnection().setAutoCommit(false);
    Statement s = createStatement();
    s.execute("create table t (c1 int)");
    s.execute("insert into t values (222),(11),(-55)");
    Integer[][] expectedRows={{322},{111},{45}};
    JDBC.assertUnorderedResultSet(s.executeQuery("select c1+100 from t"), expectedRows, false);
    s.executeUpdate("drop table t");
    s.close();
  }
  public void testSimpleUnaryMinus() throws SQLException {
    getConnection().setAutoCommit(false);
    Statement s = createStatement();
    s.execute("create table t (c1 int)");
    s.execute("insert into t values (222),(11),(-55)");
    Integer[][] expectedRows={{122},{-89},{-155}};
    JDBC.assertUnorderedResultSet(s.executeQuery("select c1-100 from t"), expectedRows, false);
    s.executeUpdate("drop table t");
    s.close();
  }
}

