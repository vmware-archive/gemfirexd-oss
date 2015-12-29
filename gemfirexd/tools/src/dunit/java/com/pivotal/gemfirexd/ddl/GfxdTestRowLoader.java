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
package com.pivotal.gemfirexd.ddl;

import java.sql.SQLException;

import com.pivotal.gemfirexd.callbacks.RowLoader;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;

public class GfxdTestRowLoader implements RowLoader {

  private final int arg1;

  private final int arg2;

  private final int arg3;

  private String params;

  public static GfxdTestRowLoader createGfxdLoader() {
    return new GfxdTestRowLoader();
  }

  public static GfxdTestRowLoader createGfxdLoader(Integer arg1, Integer arg2,
      Integer arg3) {
    return new GfxdTestRowLoader(arg1.intValue(), arg2.intValue(),
        arg3.intValue());
  }

  public GfxdTestRowLoader(int arg1, int arg2, int arg3) {
    this.arg1 = arg1;
    this.arg2 = arg2;
    this.arg3 = arg3;
  }

  public GfxdTestRowLoader() {
    this.arg1 = 1;
    this.arg2 = 2;
    this.arg3 = 3;
  }

  int getArg1() {
    return 222222;
  }

  int getArg2() {
    return this.arg2;
  }

  int getArg3() {
    return this.getArg3();
  }

  /**
   * Very simple implementation which will load a value of 1 for all the columns
   * of the table.
   */
  public Object getRow(String schemaName, String tableName, Object[] primarykey)
      throws SQLException {
    SanityManager.DEBUG_PRINT("GfxdTestRowLoader", "load called with key="
        + primarykey[0] + " in VM "
        + Misc.getDistributedSystem().getDistributedMember());
    Integer num = (Integer)primarykey[0];
    Object[] values = new Object[3];
    for (int i = 0; i < 3; i++) {
      values[i] = num;
    }
    return values;
  }

  public String getParams() {
    return this.params;
  }

  public void init(String initStr) throws SQLException {
    this.params = initStr;
  }
}
