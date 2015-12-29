/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang._Suite

       Licensed to the Apache Software Foundation (ASF) under one
       or more contributor license agreements.  See the NOTICE file
       distributed with this work for additional information
       regarding copyright ownership.  The ASF licenses this file
       to you under the Apache License, Version 2.0 (the
       "License"); you may not use this file except in compliance
       with the License.  You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

       Unless required by applicable law or agreed to in writing,
       software distributed under the License is distributed on an
       "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
       KIND, either express or implied.  See the License for the
       specific language governing permissions and limitations
       under the License
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

import org.apache.derbyTesting.functionTests.suites.XMLSuite;
import org.apache.derbyTesting.functionTests.tests.nist.NistScripts;
import org.apache.derbyTesting.junit.BaseTestCase;
import org.apache.derbyTesting.junit.JDBC;

import junit.framework.Test; 
import junit.framework.TestSuite;

/**
 * Suite to run all JUnit tests in this package:
 * org.apache.derbyTesting.functionTests.tests.lang
 * <P>
 * All tests are run "as-is", just as if they were run
 * individually. Thus this test is just a collection
 * of all the JUNit tests in this package (excluding itself).
 * While the old test harness is in use, some use of decorators
 * may be required.
 *
 */
public class _Suite extends BaseTestCase  {

	/**
	 * Use suite method instead.
	 */
	private _Suite(String name) {
		super(name);
	}

	public static Test suite() {

		TestSuite suite = new TestSuite("lang");
        // Gemstone changes BEGIN
		
        // DERBY-1315 and DERBY-1735 need to be addressed
        // before re-enabling this test as it's memory use is
        // different on different vms leading to failures in
        // the nightly runs.
        // suite.addTest(largeCodeGen.suite());

        suite.addTest(AnsiTrimTest.suite());
        suite.addTest(GroupByExpressionTest.suite());
        suite.addTest(DistinctTest.suite());
        suite.addTest(CreateTableFromQueryTest.suite());
        suite.addTest(CoalesceTest.suite());
        
        // disabled due to #41425
        // needs a file handler to work, not a
        // feature targeted for gemfirexd 1.0.
        // Caused by: java.lang.UnsupportedOperationException
        // at com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction.getFileHandler(GemFireTransaction.java:2661)
        //suite.addTest(DatabaseClassLoadingTest.suite());
 
        suite.addTest(DynamicLikeOptimizationTest.suite());
        suite.addTest(ExistsWithSubqueriesTest.suite());
    // FIXME #48136 Disabled LangScripts due to
    // SQLNonTransientConnectionException
    // suite.addTest(LangScripts.suite());
        suite.addTest(MathTrigFunctionsTest.suite());
        
        // PrepareExecuteDDL does not clean itself up
        // and has schema-matching issues
        //suite.addTest(PrepareExecuteDDL.suite());
        // RoutineSecurityTest is disabled for GemFireXD as 
        // It does not support security properties 
        //suite.addTest(RoutineSecurityTest.suite());
        suite.addTest(RoutineTest.suite());
        // SQLAuthorizationPropTest fails because stop/start of instance
        // resets the property
        //suite.addTest(SQLAuthorizationPropTest.suite());
        // GemFireXD does not work identical to Derby via StatementCache
        // Commenting out this test
        //suite.addTest(StatementPlanCacheTest.suite());
        suite.addTest(StreamsTest.suite());
        // GemFireXD does not support INSERT with a subselect containing aggregation
        // Also, there are assertions while throwing expected 22005 (data type mismatch)
        // errors here. Commenting out this test.
        //suite.addTest(TimeHandlingTest.suite());
        // GemFireXD does not support statement triggers
        // And this test calculates the order in which randomly-generated triggers
        // should fire. Commenting out this test.
        //suite.addTest(TriggerTest.suite());
        suite.addTest(TruncateTableTest.suite());
        suite.addTest(VTITest.suite());
        suite.addTest(SysDiagVTIMappingTest.suite());
        // GemFireXD does not support scrolling sensitive updatable cursors
        // Nor is the ordering of results from a cursor always known
        // This test is complex, but needs rework to handle GemFireXD's different semantics
        //suite.addTest(UpdatableResultSetTest.suite());
        // GemFireXD does not support UPDATE or DELETE WHERE CURRENT OF
        // which is all this test does. Comment out for now.
        //suite.addTest(CurrentOfTest.suite());
	    suite.addTest(CursorTest.suite());
	    // GemFireXD allows BLOB->BIT DATA casts and assignments
	    // This tests needs some tweaking
        //suite.addTest(CastingTest.suite());
        suite.addTest(ScrollCursors2Test.suite());
        
    // FIXME #48136 Disabled NullifTest due to
    // SQLNonTransientConnectionException
    // suite.addTest(NullIfTest.suite());
        // This test looks at the internal query plan, which differs
        // in GemFireXD plans
        //suite.addTest(InListMultiProbeTest.suite());
        // This test fiddles with underlying security policy information
        // and causes GemFireXD to be unable to restart the database
        // Comment out for now
        //suite.addTest(SecurityPolicyReloadingTest.suite());
        // Disable the failing test
        suite.addTest(UnaryArithmeticParameterTest.suite());
        //GemFireXD does not handle holdable cursors correctly
        // This test fails to drop objects because result sets are not closed
        //suite.addTest(HoldCursorTest.suite());
        //GemFireXD shuts down its database specially in the cluster
        // This test is not valid for GemFireXD - comment out
        //suite.addTest(ShutdownDatabaseTest.suite());
        // This test requires the Derby statement plan internal workings
        // to be unchanged. Comment out for now.
        //suite.addTest(StalePlansTest.suite());
        suite.addTest(SystemCatalogTest.suite());
    // FIXME #48136 Disabled ForBitDataTest (throws
    // SQLNonTransientConnectionException)
    // suite.addTest(ForBitDataTest.suite());
        suite.addTest(GroupByTest.suite());
        //GemFireXD does not support updatable scrollable cursors or
        // UPDATE WHERE CURRENT OF...
        // Comment this test out
        //suite.addTest(UpdateCursorTest.suite());
        suite.addTest(CoalesceTest.suite());
        //GemFireXD does not support statement triggers
        // Comment this test out
        //suite.addTest(ProcedureInTriggerTest.suite());
        //GemFireXD does not support UPDATE WHERE CURRENT OF
        // Comment this test out
        //suite.addTest(ForUpdateTest.suite());
        //GemFireXD collation support needs to be tested
        // For now, it causes database connect errors
        // Comment this test out
        //suite.addTest(CollationTest.suite());
        //suite.addTest(CollationTest2.suite());
        // FIXME #48136 Disabled ScrollCursors1Test (throws
        // SQLNonTransientConnectionException)
//        suite.addTest(ScrollCursors1Test.suite());
        suite.addTest(SimpleTest.suite());
        // This long complex series of DDL operations is not
        // yet ready for GemFireXD. It needs tweaking to add
        // results for the new GemFireXD system tables
        // Comment out for now
        //suite.addTest(GrantRevokeDDLTest.suite());
        //This test uses LOCK_TABLE VTI, there is a known bug that
        // GemFireXD crashes when the VTI is selected from
        // Comment out this test
        //suite.addTest(ReleaseCompileLocksTest.suite());
        //This test tries to drop a schema from another user's schema
        // GemFireXD schema authorization throws an exception here
        //suite.addTest(LazyDefaultSchemaCreationTest.suite());
        //GemFireXD error codes are not exactly equal to Derby ones
        // Comment out this test, needs to be tweaked to handle new Derby error codes
        //suite.addTest(ErrorCodeTest.suite());
        suite.addTest(TimestampArithTest.suite());
        //GemFireXD returns slightly different results
        // This needs to be looked at but comment out for now
        //FIXME
        //suite.addTest(SpillHashTest.suite());
        suite.addTest(CaseExpressionTest.suite());
        suite.addTest(CharUTF8Test.suite());
        suite.addTest(AggregateClassLoadingTest.suite());
        //GemFireXD returns different results for the ODBC calls here
        // Comment out for now
        //suite.addTest(TableFunctionTest.suite());
        suite.addTest(DeclareGlobalTempTableJavaTest.suite());
        suite.addTest(PrimaryKeyTest.suite());
        //GemFireXD does not support RENAME DDL
        // Comment these two tests out
        //suite.addTest(RenameTableTest.suite());
        //suite.addTest(RenameIndexTest.suite());
        suite.addTest(Bug5052rtsTest.suite());
        // This bug test uses UPDATE WHERE CURRENT OF
        // Currently unsupported by GemFireXD
        //suite.addTest(Bug5054Test.suite());
        suite.addTest(Bug4356Test.suite());
        suite.addTest(SynonymTest.suite());
        // This test uses SET_DATABASE_PROPERTY, unsupported in GemFireXD
        //suite.addTest(CommentTest.suite());
        suite.addTest(NestedWhereSubqueryTest.suite());
        //GemFireXD has different rules for conglomerate use
        // Some mismatches in this testcase, comment out for now
        //suite.addTest(ConglomerateSharingTest.suite());
        //GemFireXD has different rules for nullable unique constraints
        // than Derby. Needs to be tweaked, comment out for now
        //suite.addTest(NullableUniqueConstraintTest.suite());
        suite.addTest(OLAPTest.suite());
        //GemFireXD does not support ALTER TABLE SET NULL yet
        // Comment this out for now
        //suite.addTest(UniqueConstraintSetNullTest.suite());

        // Add the XML tests, which exist as a separate suite
        // so that users can "run all XML tests" easily.
        suite.addTest(XMLSuite.suite());

        // Add the NIST suite in from the nist package since
        // it is a SQL language related test.
        //suite.addTest(NistScripts.suite());
        
        // Add the java tests that run using a master
        // file (ie. partially converted).
        suite.addTest(LangHarnessJavaTest.suite());
        		
        suite.addTest(ResultSetsFromPreparedStatementTest.suite());

        // tests that do not run with JSR169
        if (JDBC.vmSupportsJDBC3())  
        {
            // test uses triggers interwoven with other tasks
            // triggers may cause a generated class which calls 
            // java.sql.DriverManager, which will fail with JSR169.
            // also, test calls procedures which use DriverManager
            // to get the default connection.
            //suite.addTest(GrantRevokeDDLTest.suite());

            // test uses regex classes that are not available in Foundation 1.1
            suite.addTest(ErrorMessageTest.suite());
            // Test uses DriverManager to connect to database in jar.
            //suite.addTest(DBInJarTest.suite());
        }
        suite.addTest(UDTTest.suite());
        // FIXME #48136 Disabled OffsetFetchNextTest (throws 
        // SQLNonTransientConnectionException)
//        suite.addTest(OffsetFetchNextTest.suite());
        // FIXME #48136 Disabled GrantRevokeTest (throws 
        // SQLNonTransientConnectionException)
//        suite.addTest(GrantRevokeTest.suite());
        // Gemstone changes END
        
        return suite;
	}
}
