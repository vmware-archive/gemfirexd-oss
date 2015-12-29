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

//import org.apache.derbyTesting.functionTests.suites.XMLSuite;
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
 * GemStone QA: This file differs from the _Suite.java file in that
 * it contains several more tests that we know might fail.  This
 * suite file is not appropriate for nightly testing, where we expect
 * 100% pass rates.
 */
public class _QASuite extends BaseTestCase  {

  /**
   * Use suite method instead.
   */
  private _QASuite(String name) {
    super(name);
  }

  public static Test suite() {

    TestSuite suite = new TestSuite("lang");

        // Note: several tests are called out for RUN INDENPENDENTLY
        //  on gemfirexd.  These tests have at times exhibited side
        // effects on other tests, especially due to poor test cleanup
        // in failure paths.

        // DERBY-1315 and DERBY-1735 need to be addressed
        // before re-enabling this test as it's memory use is
        // different on different vms leading to failures in
        // the nightly runs.
        // suite.addTest(largeCodeGen.suite());
        // RUN INDEPENDENTLY suite.addTest(ScrollCursors2Test.suite());

        //suite.addTest(GrantRevokeTest.suite());
        suite.addTest(AnsiTrimTest.suite());
        //[sb] remove this while enabling below list where its already there.
        suite.addTest(GroupByExpressionTest.suite());
        // GemStone changes BEGIN
        // disabled due to #41425
     /*  suite.addTest(CreateTableFromQueryTest.suite()); */

        // needs a file handler to work, not a
        // feature targeted for gemfirexd 1.0.
        // Caused by: java.lang.UnsupportedOperationException
        // at com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction.getFileHandler(GemFireTransaction.java:2661)
     /*  suite.addTest(DatabaseClassLoadingTest.suite()); */
        // GemStone changes END

        suite.addTest(DynamicLikeOptimizationTest.suite());
        suite.addTest(ExistsWithSubqueriesTest.suite());
        suite.addTest(GroupByExpressionTest.suite());
        // The following test suite is big and does not clean
        // up after itself completely.  Run it independently.
        //suite.addTest(LangScripts.suite());
        suite.addTest(MathTrigFunctionsTest.suite());
        // PrepareExecuteDDL is one monolithic jumble of code,
        //  not worth fighting through test failures herein.
        // suite.addTest(PrepareExecuteDDL.suite());
        // RUN_INDEPENDENTLY suite.addTest(RoutineSecurityTest.suite());
        suite.addTest(RoutineTest.suite());
        // RUN_INDEPENDENTLY suite.addTest(SQLAuthorizationPropTest.suite());
        // No support for SYSCS_DIAG.STATEMENT_CACHE
        //suite.addTest(StatementPlanCacheTest.suite());
        suite.addTest(StreamsTest.suite());
        suite.addTest(TimeHandlingTest.suite());
        suite.addTest(TriggerTest.suite());
        suite.addTest(TruncateTableTest.suite());
        //suite.addTest(VTITest.suite());
        //suite.addTest(SysDiagVTIMappingTest.suite());
        // GemStone changes BEGIN
        // disable since GFXD does not currently allow updatable result sets
        // suite.addTest(UpdatableResultSetTest.suite());
        // disable due to #41295: Locking problems in derby junit suites
        /*
         * suite.addTest(CurrentOfTest.suite());
         */
        // GemStone changes END
      suite.addTest(CursorTest.suite());
        suite.addTest(CastingTest.suite());
        suite.addTest(NullIfTest.suite());
        suite.addTest(InListMultiProbeTest.suite());
        // RUN_INDEPENDENTLY suite.addTest(SecurityPolicyReloadingTest.suite());
        // GemStone changes BEGIN
        // disable since GFXD does not currently allow updatable result sets
        // suite.addTest(UpdatableResultSetTest.suite());
        //suite.addTest(CurrentOfTest.suite());
        // GemStone changes END
        suite.addTest(UnaryArithmeticParameterTest.suite());
        // FIX_ME: HANGING ON TEARDOWN suite.addTest(HoldCursorTest.suite());
        suite.addTest(ShutdownDatabaseTest.suite());
        // GemStone changes BEGIN
        // disable since GFXD does not support SYSCS_UTIL.SET_RUNTIMESTATISTICS
        //suite.addTest(StalePlansTest.suite());
        // GemStone changes END
        suite.addTest(SystemCatalogTest.suite());
        suite.addTest(ForBitDataTest.suite());
        suite.addTest(DistinctTest.suite());
        suite.addTest(GroupByTest.suite());
        suite.addTest(UpdateCursorTest.suite());
        // RUN INDEPENDENTLY suite.addTest(CoalesceTest.suite());
        suite.addTest(ProcedureInTriggerTest.suite());
        suite.addTest(ForUpdateTest.suite());
        // The CollationTest seems to have a few issues, starting with ticket 42908
        //suite.addTest(CollationTest.suite());
        suite.addTest(CollationTest2.suite());
        suite.addTest(ScrollCursors1Test.suite());
        suite.addTest(SimpleTest.suite());
        // RUN GrantRevokeDDLTest INDEPENDENTLY
        // suite.addTest(GrantRevokeDDLTest.suite());
        suite.addTest(ReleaseCompileLocksTest.suite());
        // LazyDefaultSchema test commented out because
        // it uses unsupported transaction isolations, plus
        // our transactions are per thread rather than per connection.
        //suite.addTest(LazyDefaultSchemaCreationTest.suite());
        suite.addTest(ErrorCodeTest.suite());
        suite.addTest(TimestampArithTest.suite());
        suite.addTest(SpillHashTest.suite());
        suite.addTest(CaseExpressionTest.suite());
        suite.addTest(CharUTF8Test.suite());
        suite.addTest(AggregateClassLoadingTest.suite());
        suite.addTest(TableFunctionTest.suite());
        suite.addTest(DeclareGlobalTempTableJavaTest.suite());
        suite.addTest(PrimaryKeyTest.suite());
        suite.addTest(RenameTableTest.suite());
        suite.addTest(RenameIndexTest.suite());
        suite.addTest(Bug5052rtsTest.suite());
        suite.addTest(Bug5054Test.suite());
        suite.addTest(Bug4356Test.suite());
        suite.addTest(SynonymTest.suite());
        suite.addTest(CommentTest.suite());
        suite.addTest(NestedWhereSubqueryTest.suite());
        // GemStone changes BEGIN
        // Test peeks in sys$conglomerates table, where we have different contents
        //suite.addTest(ConglomerateSharingTest.suite());
        // GemStone changes END
        suite.addTest(NullableUniqueConstraintTest.suite());
        suite.addTest(OLAPTest.suite());
        suite.addTest(UniqueConstraintSetNullTest.suite());

        // Add the XML tests, which exist as a separate suite
        // so that users can "run all XML tests" easily.
        suite.addTest(XMLBindingTest.suite());
        suite.addTest(XMLTypeAndOpsTest.suite());
        // Add the NIST suite in from the nist package since
        // it is a SQL language related test.
        // GemStone changes BEGIN
        // Disable since scripts are long sets of sql and frail
        //suite.addTest(NistScripts.suite());
        // GemStone changes END

        // Add the java tests that run using a master
        // file (ie. partially converted).
        // IO redirection from running junit tests in ant messes up
        // the stdout capture of the old harness tests, so we run those
        // separately.
        //suite.addTest(LangHarnessJavaTest.suite());

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


        return suite;
  }
}
