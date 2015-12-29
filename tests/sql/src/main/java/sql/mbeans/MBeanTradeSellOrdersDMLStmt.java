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
package sql.mbeans;

import hydra.Log;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import sql.dmlStatements.TradeSellOrdersDMLStmt;
import sql.mbeans.listener.CallBackListener;
import sql.sqlutil.ResultSetHelper;
import util.TestException;

public class MBeanTradeSellOrdersDMLStmt extends TradeSellOrdersDMLStmt {

  private List<CallBackListener> listeners = new ArrayList<CallBackListener>();

  public void query(Connection gConn) throws SQLException {

    int numOfNonUniq = select.length / 2; // how many query statement is for non
                                          // unique keys, non uniq query must be
                                          // at the end
    int whichQuery = getWhichOne(numOfNonUniq, select.length); // randomly
                                                               // select one
                                                               // query sql
                                                               // based on test
                                                               // uniq or not

    String[] status = new String[2];
    BigDecimal[] ask = new BigDecimal[2];
    int[] cid = new int[5]; // test In for 5
    int[] oid = new int[5];
    int tid = getMyTid();
    Timestamp orderTime = getRandTime();
    getStatus(status);
    getAsk(ask);

    getCids(gConn, cid);
    getOids(oid);

    ResultSet gfeRS = null;
    ArrayList<SQLException> exceptionList = new ArrayList<SQLException>();

    executeListeners(MBeanTest.SELECT_QUERY_TAG, select[whichQuery]);
    try {
      gfeRS = query(gConn, whichQuery, status, ask, cid, oid, orderTime, tid);
    } catch (SQLException e) {
      if(e.getSQLState().equals("XCL54")) {
        Log.getLogWriter().warning("Error occurred while executing query for query: " + select[whichQuery] + ", Low Memory Exception");
        return;
      } else {
        throw e;
      }
    }

    if (gfeRS != null)
      ResultSetHelper.asList(gfeRS, false);
    else if (isHATest)
      Log.getLogWriter().info(
          "could not get gfxd query results after retry due to HA");
    else if (setCriticalHeap)
      Log.getLogWriter().info(
          "could not get gfxd query results after retry due to XCL54");
    else
      throw new TestException("gfxd query returns null and not a HA test");
  }

  private void executeListeners(String listenerName, Object param) {
    for (CallBackListener listener : listeners) {
      if (listenerName.equals(listener.getName())) {
        listener.execute(param);
      }
    }
  }

  public void addListener(CallBackListener callBackListener) {
    listeners.add(callBackListener);
  }
}
