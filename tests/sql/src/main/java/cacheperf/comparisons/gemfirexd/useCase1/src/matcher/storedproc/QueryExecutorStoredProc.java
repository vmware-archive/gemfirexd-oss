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
package cacheperf.comparisons.gemfirexd.useCase1.src.matcher.storedproc;

import hydra.Log;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import cacheperf.comparisons.gemfirexd.useCase1.src.matcher.LogUtils;

import com.pivotal.gemfirexd.procedure.ProcedureExecutionContext;

public class QueryExecutorStoredProc {

  public QueryExecutorStoredProc() {
    super();
  }

  // GemFireXD does not support Boolean type, so integer used (1=true, 0=false)
  // GemFireXD does not suport Generic types, so raw types used

  //@SuppressWarnings("unchecked")
  /**
   * QueryExecutorStoredProc can be executed with <I>ON ALL/ON TABLE</I>
   * to execute a SQL SELECT QUERY.
   *
   * We intentionally catch throwable for distributed query execution.
   *   
   * @param inQuery SQL SELECT Query to be executed
   * @param paramValueList List of values for bind parameters (i.e. ? marks)
   * @param errorStateValue Output parameter of error status
   * @param resultSet Output resultset
   * @param pCtx ProcedureExecutionContext internally passed by GemFireXD driver 
   */
   @SuppressWarnings("PMD.AvoidCatchingThrowable")
  public static void executeSelect(
    String inQuery,
    @SuppressWarnings("rawtypes") List paramValueList,
    int[] errorStateValue,
    ResultSet[] resultSet,
    ProcedureExecutionContext pCtx)
  throws SQLException {

    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine(
        "QueryExecutorStoredProc-executeSelect entering" +
        " inQuery=" + inQuery +
        " paramValueList=" + paramValueList +
        " errorStateValue=" + LogUtils.getErrorStateValueArrayStr(errorStateValue) +
        " resultSet=" + LogUtils.getResultSetArrayStr(resultSet, 20));
    }
    Connection conn = pCtx.getConnection();
    PreparedStatement pStmt;
    try {
      pStmt = conn.prepareStatement(inQuery);
      int columnPosition = 1;
      for (Object paramValue : paramValueList) {
        pStmt.setObject(columnPosition, paramValue);
        columnPosition++;
      }
      resultSet[0] = pStmt.executeQuery();

    } catch (Throwable e) {
      errorStateValue[0] = 1;
      if (Log.getLogWriter().fineEnabled()) {
        Log.getLogWriter().fine(
          "QueryExecutorStoredProc-executeSelect" +
          " ERROR=" + e);
      }
    }
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine(
        "QueryExecutorStoredProc-executeSelect exiting" +
        " errorStateValue[0]=" + errorStateValue[0]);
    }
  }
}
