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
package com.pivotal.gemfirexd.jdbc;

import com.pivotal.gemfirexd.internal.engine.GfxdConstants;

import junit.framework.TestSuite;
import junit.textui.TestRunner;

public class NCJoin_LangScripts_InnerJoinTest extends LangScripts_InnerJoinTest {

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(NCJoin_LangScripts_InnerJoinTest.class));
  }
  
  public NCJoin_LangScripts_InnerJoinTest(String name) {
    super(name); 
  }
  
  @Override
  protected void setUp() throws Exception {
    System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "true");
    super.setUp();
  }

  @Override
  protected void tearDown() throws java.lang.Exception {
    System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "false");
    super.tearDown();
  }

  @Override
  protected String reduceLogging() {
    return "config";
  }

  @Override
  public void testLangScript_InnerJoinTestNoPartitioning() throws Exception
  {
    // DO Nothing for NCJ
  }
}
