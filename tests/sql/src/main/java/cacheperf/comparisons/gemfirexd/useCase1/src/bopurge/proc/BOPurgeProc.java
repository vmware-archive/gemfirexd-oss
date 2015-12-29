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
package cacheperf.comparisons.gemfirexd.useCase1.src.bopurge.proc;

import hydra.Log;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import util.TestHelper;

import com.pivotal.gemfirexd.procedure.ProcedureExecutionContext;

public class BOPurgeProc {

  private static final int maxBatchSize = 10000;

  private static final int EXPIRED_DATA_LIFE_STATUS = 7;

  private static final String DELETE_EXPIRED_OFAC_CHUNKED_REC =
    "DELETE FROM SECL_OFAC_CHUNKED_MESSAGE where BACKOFFICE_TXN_ID =?";

  private static final String DELETE_EXPIRED_OFAC_REC =
    "DELETE FROM SECL_OFAC_MESSAGE where BO_TXN_ID=? ";

  private static final String DELETE_BO_RAW_DATA =
    "DELETE FROM SECL_BO_RAW_DATA where BO_TXN_ID =? ";

  private static final String SELECT_BO_TXN_ID_READY_FOR_PURGE =
    "SELECT DISTINCT(BO_TXN_ID) AS BO_TXN_ID" +
    " from SECL_BO_DATA_STATUS_HIST_TEMP" +
    " where DATA_LIFE_STATUS = ?";

  private static final String DELETE_BO_MATCHED_DATA =
    "DELETE FROM SECL_MATCHED_DATA where BACKOFFICE_TXN_ID = ?";

  private static final String DELETE_ALL_RECORDS_FROM_TEMP_TABLE =
    "DELETE FROM SECL_BO_DATA_STATUS_HIST_TEMP";

  private static final String DELETE_EXPIRED_BO_LOG =
    "DELETE FROM SECL_BO_DATA_STATUS_HIST where BO_TXN_ID = ?";

  private static BOPurgeExecutorService executorService = BOPurgeExecutorFactory.getInstance();

  public BOPurgeProc() {
    super();
  }

  @SuppressWarnings("unchecked")
  public static void purge(ResultSet[] resultSet,
      ProcedureExecutionContext pCtx) throws SQLException {
    Connection connection = null;
    try {
      connection = pCtx.getConnection();
      markAndDeleteExpiredTransactions(connection);
    } catch (Exception e) {
      Log.getLogWriter().error("Component: UseCase1-SECURITAS:appstat|Event Severity: Fatal|Event Class: MatchingEngine|Description: Issue while mark and delete expired transactions. Summary: " + TestHelper.getStackTrace(e));
    } finally {
      connection.close();
    }
  }

  private static void markAndDeleteExpiredTransactions(Connection pCtx)
  throws SQLException {
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("Entering markAndDeleteExpiredTransactions..");
    }
    int expiredBOTrans = 0;
    try {
      expiredBOTrans = markExpiredBOTransactionsAndLog(pCtx);
    } catch (SQLException e) {
      Log.getLogWriter().error("Component: UseCase1-SECURITAS:appstat|Event Severity: Fatal|Event Class: MatchingEngine|Description: Issue while mark and delete expired transactions. Summary: Error while marking the expired BO Transactions. " + TestHelper.getStackTrace(e));
    }

    if (expiredBOTrans > 0) {
      try {
        deleteExpiredFircosoftTransactions(pCtx);
      } catch (SQLException e) {
        Log.getLogWriter().error("Component: UseCase1-SECURITAS:appstat|Event Severity: Fatal|Event Class: MatchingEngine|Description: Issue while mark and delete expired transactions-deleteExpiredFircosoftTransactions Summary: Error while deleting the corresponding Fircosoft Transactions, So not deleting the expired BO and Channel transactions. " + TestHelper.getStackTrace(e));
        throw e;
      }
    }

    if (expiredBOTrans > 0) {
      try {
        deleteExpiredBOTransactions(pCtx);
      } catch (SQLException e) {
        Log.getLogWriter().error("Component: UseCase1-SECURITAS:appstat|Event Severity: Fatal|Event Class: MatchingEngine|Description: Issue while mark and delete expired transactions- deleteExpiredBOTransactions Summary: Error while deleting the expired BO Transactions. " + TestHelper.getStackTrace(e));
      }
    }

    try {
      purgeTempTable(pCtx);
    } catch (Exception e) {
      Log.getLogWriter().error("Component: UseCase1-SECURITAS:appstat|Event Severity: Fatal|Event Class: MatchingEngine|Description: Issue while mark and delete expired transactions- purgeTempTable Summary: Error while deleting the temp records. " + TestHelper.getStackTrace(e));
    }
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("Exiting markAndDeleteExpiredTransactions...");
    }
  }

  private static void purgeTempTable(Connection pCtx) throws SQLException {
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("Entering into purgeTempTable");
    }
    PreparedStatement prepareStatement = pCtx.prepareStatement(DELETE_ALL_RECORDS_FROM_TEMP_TABLE);
    prepareStatement.execute();
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("Exiting into purgeTempTable");
    }
  }

  private static int markExpiredBOTransactionsAndLog(Connection pCtx)
      throws SQLException {
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("Entering markExpiredBOTransactionsAndLog..");
    }
    executorService.insertAllPurgeRecordsToTempTable(pCtx);
    int recordsMarked = executorService.markAllRecordsForPurge(pCtx);
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("Exiting markExpiredBOTransactionsAndLog with " + recordsMarked + " rows marked expired");
    }
    return  recordsMarked;
  }

  private static void deleteExpiredFircosoftTransactions(Connection pCtx)
      throws SQLException {
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("Entering deleteExpiredFircosoftTransactions..");
    }
    int deleteExpiredOfacChunkedMessage = deleteExpiredOfacChunkedMessage(pCtx);
    if (deleteExpiredOfacChunkedMessage > 0) {
      deleteExpiredOfaceMessage(pCtx);
    }
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("Exiting deleteExpiredFircosoftTransactions..");
    }
  }

  private static void deleteExpiredOfaceMessage(Connection pCtx) throws SQLException {
    int currentBatchSize = 0;
    ResultSet result = getExpiredBOIds(pCtx);
    PreparedStatement deleteStmt = pCtx
        .prepareStatement(DELETE_EXPIRED_OFAC_REC);
    while (result.next()) {
      deleteStmt.setString(1, result.getString("BO_TXN_ID"));
      deleteStmt.addBatch();
      currentBatchSize++;
      if (currentBatchSize > maxBatchSize) {
        deleteStmt.executeBatch();
        pCtx.commit();
      }
    }
    deleteStmt.executeBatch();
    pCtx.commit();
  }

  private  static int deleteExpiredOfacChunkedMessage(Connection pCtx)
  throws SQLException {
    int currentBatchSize = 0;
    int flag = 0;
    ResultSet result = getExpiredBOIds(pCtx);
    PreparedStatement deleteStmt = pCtx
        .prepareStatement(DELETE_EXPIRED_OFAC_CHUNKED_REC);
    while (result.next()) {
      deleteStmt.setString(1, result.getString("BO_TXN_ID"));
      deleteStmt.addBatch();
      currentBatchSize++;
      if (currentBatchSize > maxBatchSize) {
        deleteStmt.executeBatch();
        pCtx.commit();
        flag=1;
      }
    }

    int[] executeBatch = deleteStmt.executeBatch();
    pCtx.commit();
    if (null != executeBatch && executeBatch.length > 0){
      flag=1;
    }
    return flag;
  }

  private static ResultSet getExpiredBOIds(Connection pCtx)
      throws SQLException {
    PreparedStatement prepareStatement = pCtx.prepareStatement(SELECT_BO_TXN_ID_READY_FOR_PURGE);
    prepareStatement.setInt(1, 7);
    ResultSet result = prepareStatement.executeQuery();
    return result;
  }

  private static void deleteExpiredBOTransactions(Connection pCtx)
  throws SQLException {
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("Entering deleteExpiredBOTransactions..");
    }
    deleteBOMatchedData(pCtx);
    deleteBORawData(pCtx);
    deleteBOLogData(pCtx);
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("Exiting deleteExpiredBOTransactions..");
    }
  }

  private static void deleteBOMatchedData(Connection pCtx)
  throws SQLException {
    int currentBatchSize = 0;
    ResultSet result = getExpiredBOIds(pCtx);
    PreparedStatement deleteStmt=pCtx.prepareStatement(DELETE_BO_MATCHED_DATA);
    while (result.next()){
      deleteStmt.setString(1, result.getString("BO_TXN_ID"));
      deleteStmt.addBatch();
      currentBatchSize++;
      if (currentBatchSize > maxBatchSize){
        deleteStmt.executeBatch();
        pCtx.commit();
      }
      deleteStmt.executeBatch();
      pCtx.commit();
    }
  }

  private static void deleteBORawData(Connection pCtx) throws SQLException {
    int currentBatchSize = 0;
    ResultSet result = getExpiredBOIds(pCtx);
    PreparedStatement deleteStmt=pCtx.prepareStatement(DELETE_BO_RAW_DATA);
    while (result.next()) {
      deleteStmt.setString(1, result.getString("BO_TXN_ID"));
      deleteStmt.addBatch();
      currentBatchSize++;
      if (currentBatchSize > maxBatchSize) {
        deleteStmt.executeBatch();
        pCtx.commit();
      }
      deleteStmt.executeBatch();
      pCtx.commit();
    }
  }

  private static void deleteBOLogData(Connection pCtx) throws SQLException {
    int currentBatchSize = 0;
    ResultSet result = getExpiredBOIds(pCtx);
    PreparedStatement deleteStmt=pCtx.prepareStatement(DELETE_EXPIRED_BO_LOG);
    while (result.next()) {
      deleteStmt.setString(1, result.getString("BO_TXN_ID"));
      deleteStmt.addBatch();
      currentBatchSize++;
      if (currentBatchSize > maxBatchSize) {
        deleteStmt.executeBatch();
        pCtx.commit();
      }
      deleteStmt.executeBatch();
      pCtx.commit();
    }
  }
}
