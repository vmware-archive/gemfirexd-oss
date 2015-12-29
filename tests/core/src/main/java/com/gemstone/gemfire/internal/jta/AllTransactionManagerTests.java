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
package com.gemstone.gemfire.internal.jta;

/*
 * Run all JTA Junit  tests from here
 * 
 * @author Mitul Bid
 */

import junit.framework.Test;
import junit.framework.TestSuite;

public class AllTransactionManagerTests {
	public static Test suite() {
		TestSuite suite = new TestSuite(
				"Test for com.gemstone.gemfire.internal.jta");
		//$JUnit-BEGIN$
		suite.addTestSuite(UserTransactionImplTest.class);
		suite.addTestSuite(TransactionManagerImplTest.class);
		suite.addTestSuite(TransactionImplTest.class);
		suite.addTestSuite(ExceptionTest.class);	
		//$JUnit-END$
		return suite;
	}
}
