/*
 *
 * Derby - Class org.apache.derbyTesting.functionTests.tests.lang.LangScripts
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, 
 * software distributed under the License is distributed on an 
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
 * either express or implied. See the License for the specific 
 * language governing permissions and limitations under the License.
 */
package org.apache.derbyTesting.functionTests.tests.lang;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.apache.derbyTesting.functionTests.util.ScriptTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * LangScripts runs SQL scripts (.sql files) in the lang package
 * and compares the output to a canon file in the
 * standard master package.
 * <BR>
 * Its suite() method returns a set of tests where each test is an instance of
 * this class for an individual script wrapped in a clean database decorator.
 * <BR>
 * It can also be used as a command line program to run one or more
 * language based SQL scripts as tests.
 *
 */
public final class LangScripts extends ScriptTestCase {

    /**
     * Language SQL scripts (.sql files) that run under Derby's client
     * and emebedded configurations. SQL tests are put in this category
     * if they are likely to have some testing of the protocol,
     * typically tests related to data types.
     * 
     */
	
	// These tests will be converted to JUnit if desirable
    private static final String[] CLIENT_AND_EMBEDDED_TESTS = {
        //"LOB",   			//Converted to JUnit
        //"bit",   			//Converted to JUnit
        "bit2",    			//Not to be converted
        //"nulls", 			//Converted to JUnit
        //"stringtypes",	//Converted to JUnit
        };
    
    /**
     * Language SQL scripts (.sql files) that only run in embedded.
     * Most tests that are testing SQL functionality can just 
     * run in emebdded.
     */
    private static final String[] EMBEDDED_TESTS = {
        //"aggbuiltin",   	//Converted to JUnit
        //"aggregate",		//Converted to JUnit
    	// altertable
        //"arithmetic",		//Converted to JUnit
        // autoincrement
        "case",				//Not to be converted
        "cast",
        // checkConstraint
        // columnDefaults
        //"comparisons",	//Converted to JUnit
        "constantExpression",
        // datetime
        "delete",
        //"derived",		//Converted to JUnit
        //"depend",
        // dropTable		//Converted to JUnit
        "ejbql",
        // ejbql_nonSPS
        // fk_nonSPS
        //"floattypes",		//Converted to JUnit
        //"functions",		//Converted to JUnit
        "identifier",
        //"implicitConversions",	//Converted to JUnit
        // inbetween
        //"innerjoin",		//Converted to JUnit
        //"insert",			//Converted to JUnit
        //"intersect",		//Converted to JUnit
        //"joins",			//Converted to JUnit
        //"logop",			//Converted to JUnit
        //lojreorder		//In-progress Converted to JUnit (many tests fail)
        //"miscerrors",		//Converted to JUnit
        "modifyColumn",		//Not converted as GemFireXD does not fully support ALTER TABLE yet
        //"nonreserved",	//Converted to JUnit
        //"orderby",		//Converted to JUnit
        // outerjoin		//Converted to JUnit
        "positionedDelUpd",	//Not converted as uses prepared cursor operations
        // predicatePushdown	//Converted to JUnit
        // predicatesIntoViews	
        // refActions		// Not converted as GemFireXD does not support CASCADE delete
        "refActions1",		// Not converted as GemFireXD does not support CASCADE delete
        // refActions2		// Not converted as GemFireXD does not support CASCADE delete
        // refActions3		// Not converted as GemFireXD does not support CASCADE delete - convert once supported
        // reopenScan		//Converted to JUnit
        //"schemas",		//Converted to JUnit
        //"select",			//Converted to JUnit
        // subquery			//Converted to JUnit
        // subquery2		//Converted to JUnit
        // subqueryFlattening //Converted to JUnit
        "synonym",			// Not converted as needs prepared stmts
        //"tempRestrictions",  // Simple test of aggregates in where clause, covered elsewhere
        //triggerBeforeTrig
        //triggerGeneral
        //triggerRecursion
        //triggerRefClause		// All trigger tests not converted or run due to GemFireXD trigger restrictions (no stmt-level, no cascade)
        //"union",			//Converted to JUnit
        "update"			// Not converted as GemFireXD does not support WHERE CURRENT OF and requires transactional rollback testing
        //"valuesclause",	//Converted to JUnit
        };
    
    /**
     * Tests that run in embedded and require JDBC3_TESTS
     * (ie. can not run on JSR169).
     */
    private static final String[] JDBC3_TESTS = {
    };

	/**
	 * Run a set of language SQL scripts (.sql files) passed in on the
	 * command line. Note the .sql suffix must not be provided as
     * part of the script name.
	 * <code>
	 * example
	 * java org.apache.derbyTesting.functionTests.tests.lang.LangScripts case union
	 * </code>
	 */
	public static void main(String[] args)
	{
		junit.textui.TestRunner.run(getSuite(args));
	}

	/**
	 * Return the suite that runs all the langauge SQL scripts.
	 */
	public static Test suite() {
        
        TestSuite suite = new TestSuite("LangScripts");
        suite.addTest(getSuite(CLIENT_AND_EMBEDDED_TESTS));
        suite.addTest(getSuite(EMBEDDED_TESTS));
        if (JDBC.vmSupportsJDBC3())
            suite.addTest(getSuite(JDBC3_TESTS));
        
        // Set up the scripts run with the network client
        TestSuite clientTests = new TestSuite("LangScripts:client");
        clientTests.addTest(getSuite(CLIENT_AND_EMBEDDED_TESTS));
        Test client = TestConfiguration.clientServerDecorator(clientTests);
        
        // add those client tests into the top-level suite.
        suite.addTest(client);

        return suite;
    }
    
	/*
	 * A single JUnit test that runs a single language SQL script.
	 */
	private LangScripts(String langTest){
		super(langTest);
	}
	
    /**
     * Return a suite of language SQL tests from the list of
     * script names. Each test is surrounded in a decorator
     * that cleans the database.
      */
	private static Test getSuite(String[] list)
	{
        TestSuite suite = new TestSuite("SQL scripts");
        for (int i = 0; i < list.length; i++)
            suite.addTest(
            		new CleanDatabaseTestSetup(
            		new LangScripts(list[i])));

        return getIJConfig(suite);
    }
}
