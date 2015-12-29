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
package com.pivotal.gemfirexd.delete;

import java.sql.Statement;

import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
/**
 * Test class for deletes on replicated tables.
 * 
 * @author rdubey
 * 
 */
@SuppressWarnings("serial")
public class DeleteOnReplicatedTableDUnit extends DistributedSQLTestBase{
  
  public DeleteOnReplicatedTableDUnit(String name) {
    super(name);
  }
  
  /**
   * Bug test for 40145.
   * @throws Exception
   */
  public void test40145WithReplicatedTable() throws Exception {
    AsyncVM async1 = invokeStartServerVM(1, 0, "SG1,SG2", null);
    AsyncVM async2 = invokeStartServerVM(2, 0, "SG1", null);
    AsyncVM async3 = invokeStartServerVM(3, 0, "SG2", null);
    startClientVMs(1, 0, null);
    joinVMs(true, async1, async2, async3);

    clientSQLExecute(1,
        "create table t1(id int not null, partitionId int not null, "
            + "primary key(id)) replicate server groups (sg1)");
    clientSQLExecute(1, "insert into t1 values(1,1)");
    Statement st = TestUtil.jdbcConn.createStatement();
    int numDel = st.executeUpdate("delete from t1 where t1.id=1");
    assertEquals("Delete should delete one row ",1,numDel);
  }

}
