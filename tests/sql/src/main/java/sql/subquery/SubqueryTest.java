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
package sql.subquery;

import hydra.TestConfig;

import java.sql.Connection;
import java.sql.SQLException;

import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;

public class SubqueryTest extends SQLTest {
  protected static SubqueryTest sbt = new SubqueryTest();
  protected static boolean independentSubqueryOnly = TestConfig.tab().
      booleanAt(SQLPrms.independentSubqueryOnly, true);
  protected boolean limitedSubquery = TestConfig.tab().booleanAt(SQLPrms.limitNumberOfSubquery, false);
  
  public static void HydraTask_doSubquery() {
    sbt.doSubquery();
  }
  
  //Independent query 
  protected void doSubquery() {
    if (limitedSubquery && getMyTid() % 37 != 0) return; 
    Connection dConn =null;
    if (hasDerbyServer) dConn = getDiscConnection(); 
    Connection gConn = getGFEConnection();  
    
    if (setCriticalHeap) resetCanceledFlag();
    if (setTx && isHATest) resetNodeFailureFlag();
    if (setTx && testEviction) resetEvictionConflictFlag();
    
    doSubquery(dConn, gConn);  
    
    commit(gConn);
    commit(dConn);
    
    if (hasDerbyServer) closeDiscConnection(dConn);
    closeGFEConnection(gConn);
  }
  
  protected void doSubquery(Connection dConn, Connection gConn) {
    if (independentSubqueryOnly) {
      int num = random.nextInt(numOfWorkers);
      if (num == 0 && !isHATest) 
        new Subquery().subqueryUpdate(dConn, gConn); 
      else if (num == 1)
        new Subquery().subqueryDelete(dConn, gConn);
      else
        new Subquery().query(dConn, gConn); 
    } else {
      if (random.nextInt(numOfWorkers) != 1)
        new ColocatedSubquery().query(dConn, gConn);
      //if (random.nextBoolean()) new ColocatedSubquery().query(dConn, gConn);
      //else new Subquery().query(dConn, gConn);
      else  
        new ColocatedSubquery().subqueryDelete(dConn, gConn);
    }    
  }

}
