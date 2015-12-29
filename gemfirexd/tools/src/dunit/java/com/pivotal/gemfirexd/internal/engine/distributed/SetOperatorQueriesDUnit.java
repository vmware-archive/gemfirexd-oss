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
package com.pivotal.gemfirexd.internal.engine.distributed;

import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.internal.engine.distributed.SetQueriesDUnitHelper.CreateTableDUnitF;

public class SetOperatorQueriesDUnit extends DistributedSQLTestBase {
  private static SetQueriesDUnitHelper setQueriesDUnitHelper = new SetQueriesDUnitHelper();

  public SetOperatorQueriesDUnit(String name) {
    super(name); 
  }

  @Override
  protected String reduceLogging() {
    // these tests generate lots of logs, so reducing them
    return "config";
  }

  /*
   * Handle scenarios where table have been created with primary keys
   */
  // If changing this, also change 
  // doRunThisCaseForSetOpAll_withPK and doRunThisCaseForSetOpAll_withPK
  private final CreateTableDUnitF[] createTableFunctions_noColoc_withPK =
    new SetQueriesDUnitHelper.CreateTableDUnitF[]
                                                {
      setQueriesDUnitHelper.new CreateTable_withPK_PR_onPK_1(), //0
      setQueriesDUnitHelper.new CreateTable_withPK_PR_onCol_1(), //1
      setQueriesDUnitHelper.new CreateTable_withPK_PR_onRange_ofPK_1(), //2
      setQueriesDUnitHelper.new CreateTable_withPK_PR_onRange_ofCol_1(), //3
      setQueriesDUnitHelper.new CreateTable_withPK_Replicated_1() //4
                                                };
  static boolean[][] doRunThisCaseForSetOpDistinct_withPK_noColoc =
  {
    // Any failures combination of tables can be disabled from here
    // Reflect any change in createTableFunctions
    // 5 x 5
    // 00 01 02 03 04
    // 10 11 12 13 14
    // 20 21 22 23 24
    // 30 31 32 33 34
    // 40 41 42 43 44
    {true, true, true, true, true},
    {true, true, true, true, true},
    {true, true, true, true, true},
    {true, true, true, true, true},
    {true, true, true, true, true}
  };
  static boolean[][] doRunThisCaseForSetOpAll_withPK_noColoc =
  {
    // Any failures combination of tables can be disabled from here
    // Reflect any change in createTableFunctions
    // 5 x 5
    // 00 01 02 03 04
    // 10 11 12 13 14
    // 20 21 22 23 24
    // 30 31 32 33 34
    // 40 41 42 43 44
    {true, true, true, true, true},
    {true, true, true, true, true},
    {true, true, true, true, true},
    {true, true, true, true, true},
    {true, true, true, true, true}
  };

  /*
   * Non-colocated, no Index, with PK
   */
  public void testSetOperators_noColoc_withPK_noIndex_red0() throws Exception {
    startVMs(1, 3);
    int redundancy = 0;
    for(int i=createTableFunctions_noColoc_withPK.length-1 ; i >= 0; i--) {
      for(int j=createTableFunctions_noColoc_withPK.length-1 ; j >= 0; j--) {
          boolean setOPDistinct = doRunThisCaseForSetOpDistinct_withPK_noColoc[i][j];
          boolean setOpAll =  doRunThisCaseForSetOpAll_withPK_noColoc[i][j];
          SetQueriesDUnitHelper.caseSetOperators_noColoc_withPK_scenario1(getLogWriter(),
              i, j, false, redundancy,
              setOPDistinct, setOpAll, createTableFunctions_noColoc_withPK);
      }
    }
  } /**/
  
  /*
   * Non-colocated, no Index, with PK
   */
  public void testSetOperators_noColoc_withPK_noIndex_red1() throws Exception {
    startVMs(1, 3);
    int redundancy = 1;
    for(int i=createTableFunctions_noColoc_withPK.length-1 ; i >= 0; i--) {
      for(int j=createTableFunctions_noColoc_withPK.length-1 ; j >= 0; j--) {
          boolean setOPDistinct = doRunThisCaseForSetOpDistinct_withPK_noColoc[i][j];
          boolean setOpAll =  doRunThisCaseForSetOpAll_withPK_noColoc[i][j];
          SetQueriesDUnitHelper.caseSetOperators_noColoc_withPK_scenario1(getLogWriter(),
              i, j, false, redundancy,
              setOPDistinct, setOpAll, createTableFunctions_noColoc_withPK);
      }
    }
  } /**/
  
  /*
   * Non-colocated, no Index, with PK
   */
  public void testSetOperators_noColoc_withPK_noIndex_red2() throws Exception {
    startVMs(1, 3);
    int redundancy = 2;
    for(int i=createTableFunctions_noColoc_withPK.length-1 ; i >= 0; i--) {
      for(int j=createTableFunctions_noColoc_withPK.length-1 ; j >= 0; j--) {
          boolean setOPDistinct = doRunThisCaseForSetOpDistinct_withPK_noColoc[i][j];
          boolean setOpAll =  doRunThisCaseForSetOpAll_withPK_noColoc[i][j];
          SetQueriesDUnitHelper.caseSetOperators_noColoc_withPK_scenario1(getLogWriter(),
              i, j, false, redundancy,
              setOPDistinct, setOpAll, createTableFunctions_noColoc_withPK);
      }
    }
  } /**/
}
