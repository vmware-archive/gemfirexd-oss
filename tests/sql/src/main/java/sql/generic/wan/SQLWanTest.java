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
package sql.generic.wan;

import hydra.ConfigHashtable;
import hydra.Log;
import hydra.TestConfig;
import sql.generic.SQLTestDecorator;
import sql.generic.SQLTestable;
import sql.wan.SQLWanPrms;
import util.TestException;

public class SQLWanTest extends SQLTestDecorator {
  public ConfigHashtable conftab = TestConfig.tab();
  public int numOfWanSites = conftab.intAt(SQLWanPrms.numOfWanSites);

  public SQLWanTest(SQLTestable sqlTest) {    
    super(sqlTest);
    Log.getLogWriter().info("Decorated SQLWanTest on SQLGenericTest");
  }

  @Override
  public void initialize() {
    super.initialize();
    initWanTest();
  }

  protected void initWanTest() {
    if (numOfWanSites > 5)
      throw new TestException("test could not handle more than 5 wan sites yet");
  }
}
