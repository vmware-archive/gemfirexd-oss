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
package com.gemstone.gemfire.memcached;

import java.io.IOException;
import java.util.logging.Logger;
import java.util.logging.StreamHandler;

import junit.framework.TestCase;

public class RubyJUnitTest extends TestCase {

	private final static int port = 11212;
	private static final Logger logger = Logger.getLogger(GemcachedDevelopmentJUnitTest.class.getCanonicalName());
	
//	@Override
//	protected void setUp() throws Exception {
//		GemFireMemcachedServer server = new GemFireMemcachedServer(port);
//		server.start();
//		logger.addHandler(new StreamHandler());
//	    logger.info("SWAP:Running test:"+getName());
//	}
	
	public void testRubyScript() throws IOException, InterruptedException {
//		Process p = Runtime.getRuntime().exec("tests/com/gemstone/gemfire/memcached/gemcachedTest.rb");
//		assertEquals(0, p.waitFor());
	}
}
