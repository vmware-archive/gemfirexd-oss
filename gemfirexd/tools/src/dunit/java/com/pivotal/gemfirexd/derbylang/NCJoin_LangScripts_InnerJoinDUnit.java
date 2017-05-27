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
package com.pivotal.gemfirexd.derbylang;

import com.pivotal.gemfirexd.internal.engine.GfxdConstants;

import io.snappydata.test.dunit.SerializableRunnable;

/*
 * Porting @com.pivotal.gemfirexd.derbylang.LangScripts_InnerJoinDUnit
 * 
 * to NCJ
 */
public class NCJoin_LangScripts_InnerJoinDUnit extends
    LangScripts_InnerJoinDUnit {

  public NCJoin_LangScripts_InnerJoinDUnit(String name) {
    super(name);
    // TODO Auto-generated constructor stub
  }

  @Override
  public void setUp() throws Exception {
    System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "true");
    invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "true");
      }
    });
    super.setUp();
  }

  @Override
  public void tearDown2() throws java.lang.Exception {
    System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "false");
    invokeInEveryVM(new SerializableRunnable() {
      @Override
      public void run() {
        System.setProperty(GfxdConstants.OPTIMIZE_NON_COLOCATED_JOIN, "false");
      }
    });
    super.tearDown2();
  }

  protected String colocateT2() {
    return " ";
  }

  /*
   * For NCJ, Outer join is not supported, as in
   * @see com.pivotal.gemfirexd.derbylang.LangScripts_InnerJoinDUnit#outerJoin1()
   */
  @Override
  protected String outerJoin1() {
    return "noexec";
  }

  /*
   * For NCJ, Outer join is not supported, as in
   * @see com.pivotal.gemfirexd.derbylang.LangScripts_InnerJoinDUnit#outerJoin2()
   */
  @Override
  protected String outerJoin2() {
    return "noexec";
  }

  /*
   * TODO: For NCJ, currently insert select is not working, THAT NEED TO BE FIXED
   * @see com.pivotal.gemfirexd.derbylang.LangScripts_InnerJoinDUnit#insertSelect1()
   */
  @Override
  protected String insertSelect1() {
    return "insert into insert_test values (1,1,2),(1,1,3),(1,1,5),(1,1,7),(3,3,2),(3,3,5),(3,3,7)";
  }

  /*
   * TODO: For NCJ, currently insert select is not working, that need to be fixed
   * @see com.pivotal.gemfirexd.derbylang.LangScripts_InnerJoinDUnit#insertSelect2()
   * 
   * We may ultimately not support insert select with sub-query, but then proper exception 
   * i.e. "Not SUpported exception" should be fixed.
   */
  @Override
  protected String insertSelect2() {
    return "insert into insert_test values (1,1,2),(1,1,3),(1,1,5),(1,1,7),(3,3,2),(3,3,5),(3,3,7)";
  }
}
