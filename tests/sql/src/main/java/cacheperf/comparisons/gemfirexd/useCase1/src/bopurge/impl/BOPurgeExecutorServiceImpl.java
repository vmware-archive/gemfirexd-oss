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
package cacheperf.comparisons.gemfirexd.useCase1.src.bopurge.impl;

import hydra.Log;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import util.TestHelper;

import cacheperf.comparisons.gemfirexd.useCase1.src.bopurge.proc.BOPurgeExecutorService;

public class BOPurgeExecutorServiceImpl implements BOPurgeExecutorService {

  private static final int maxBatchSize = 1000;

  private static final int MARK_AS_TO_PURGE = 7;
  private static final int NON_FINAL_STATUS_ACK_HIT = 1;
  private static final int NON_FINAL_STATUS_OUT_RECHECK = 5;
  private static final int NON_FINAL_STATUS_OUT_PENDING = 12;

  private static final String INSERT_INTO_SECL_BO_DATA_TEMP_BO_TXN_ID =
    "INSERT INTO SECL_BO_DATA_STATUS_HIST_TEMP" +
    " (BO_TXN_ID, VALUE_DATE, LAST_UPDATE_TIME)" +
    " VALUES(?, ?, ?)";

  private static final String GET_LIST_OF_EXPIRED_BO_IDS =
    "select BACKOFFICE_TXN_ID, BACKOFFICE_CODE, CHANNEL_NAME, TXN_TYPE," +
          " DATA_LIFE_STATUS, MATCH_STATUS, MATCH_CATEG_ID, HIT_STATUS," +
          " CURRENT_TIMESTAMP" +
    " AS LAST_UPDATE_TIME, ACTUAL_VALUE_DATE" +
    " from SECT_BACKOFFICE_DATA" +
    " where DATA_LIFE_STATUS = 7";

  private static final String INSERT_INTO_LOG_TABLE_FOR_PURGED_RECORDS =
    "INSERT INTO SECL_BO_DATA_STATUS_HIST" +
    " (BO_TXN_ID, BACKOFFICE_CODE, CHANNEL_NAME, TXN_TYPE, DATA_LIFE_STATUS," +
     " MATCH_STATUS, MATCH_CATEG_ID, HIT_STATUS, LAST_UPDATE_TIME," +
     " ACTUAL_VALUE_DATE, SCREENING_TIME)" +
    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?)";

  private static final String GET_LIST_OF_DATA_TO_INSERT_IN_TEMP_TABLE =
    "SELECT boData.BO_TXN_ID," +
          " MAX(boData.ACTUAL_VALUE_DATE) AS MAX_VALUE_DATE," +
          " MAX(boData.LAST_UPDATE_TIME) AS LAST_UPDATED" +
    " FROM SECL_BO_DATA_STATUS_HIST boData," +
         " SECM_TXN_MANAGEMENT txnMgmt" +
    " WHERE ((boData.HIT_STATUS is not null" +
               " AND boData.HIT_STATUS not in (?,?,?))" +
        " AND txnMgmt.backoffice_code = boData.backoffice_code" +
        " AND boData.txn_type=txnMgmt.txn_type" +
        " AND ({fn TIMESTAMPADD(SQL_TSI_DAY, txnMgmt.TXN_HOUSEKEEPING, boData.ACTUAL_VALUE_DATE)} < CURRENT_TIMESTAMP))" +
      "OR ((boData.HIT_STATUS is not null" +
             " AND boData.HIT_STATUS not in (?,?,?))" +
         " AND txnMgmt.backoffice_code=boData.backoffice_code" +
         " AND (boData.txn_type NOT IN (" +
                " SELECT txn_type" +
                " FROM secm_txn_management" +
                " WHERE backoffice_code=boData.backoffice_code)" +
            " AND txnMgmt.txn_type='UNKNOWN')" +
         " AND ({fn TIMESTAMPADD(SQL_TSI_DAY, txnMgmt.TXN_HOUSEKEEPING, boData.ACTUAL_VALUE_DATE)} < CURRENT_TIMESTAMP))" +
    " GROUP BY BO_TXN_ID";

  private static final String MARK_EXPIRED_BO_TRANS =
    "UPDATE SECT_BACKOFFICE_DATA t" +
    " SET data_life_status= ?" +
    " WHERE t.BACKOFFICE_TXN_ID in (" +
          " SELECT boData.BO_TXN_ID" +
          " FROM SECL_BO_DATA_STATUS_HIST boData," +
               " SECM_TXN_MANAGEMENT txnMgmt," +
               " SECL_BO_DATA_STATUS_HIST_TEMP TMP" +
          " WHERE (boData.BO_TXN_ID=TMP.BO_TXN_ID" +
                    " AND boData.LAST_UPDATE_TIME=TMP.LAST_UPDATE_TIME" +
                    " AND (boData.HIT_STATUS is not null" +
                            " AND boData.HIT_STATUS not in (?,?,?))" +
                    " AND txnMgmt.backoffice_code=boData.backoffice_code" +
                    " AND boData.txn_type=txnMgmt.txn_type" +
                    " AND ({fn TIMESTAMPADD(SQL_TSI_DAY, txnMgmt.TXN_HOUSEKEEPING, TMP.VALUE_DATE)} < CURRENT_TIMESTAMP))" +
          " OR (boData.BO_TXN_ID=TMP.BO_TXN_ID" +
                 " AND boData.LAST_UPDATE_TIME=TMP.LAST_UPDATE_TIME" +
                 " AND (boData.HIT_STATUS is not null" +
                         " AND boData.HIT_STATUS not in (?,?,?))" +
                         " AND txnMgmt.backoffice_code=boData.backoffice_code" +
                         " AND (boData.txn_type NOT IN (" +
                                    " SELECT txn_type" +
                                    " FROM secm_txn_management" +
                                    " WHERE backoffice_code=boData.backoffice_code)" +
                                " AND txnMgmt.txn_type='UNKNOWN')" +
                         " AND ({fn TIMESTAMPADD(SQL_TSI_DAY, txnMgmt.TXN_HOUSEKEEPING, TMP.VALUE_DATE)} < CURRENT_TIMESTAMP))" +
    " GROUP BY boData.BO_TXN_ID)";

  private static final String GET_EXPIRED_BO_IDS =
    "SELECT boData.BO_TXN_ID" +
    " FROM SECL_BO_DATA_STATUS_HIST boData," +
         " SECM_TXN_MANAGEMENT txnMgmt," +
         " SECL_BO_DATA_STATUS_HIST_TEMP TMP" +
    " WHERE (boData.BO_TXN_ID=TMP.BO_TXN_ID" +
              " AND boData.LAST_UPDATE_TIME=TMP.LAST_UPDATE_TIME" +
              " AND (boData.HIT_STATUS is not null" +
                      " AND boData.HIT_STATUS not in (?,?,?))" +
              " AND txnMgmt.backoffice_code=boData.backoffice_code" +
              " AND boData.txn_type=txnMgmt.txn_type" +
              " AND ({fn TIMESTAMPADD(SQL_TSI_DAY, txnMgmt.TXN_HOUSEKEEPING, TMP.VALUE_DATE)} < CURRENT_TIMESTAMP))" +
      " OR (boData.BO_TXN_ID=TMP.BO_TXN_ID" +
             " AND boData.LAST_UPDATE_TIME=TMP.LAST_UPDATE_TIME" +
             " AND (boData.HIT_STATUS is not null" +
                     " AND boData.HIT_STATUS not in (?,?,?))" +
             " AND txnMgmt.backoffice_code = boData.backoffice_code" +
             " AND (boData.txn_type NOT IN (" +
                         "SELECT txn_type" +
                         " FROM secm_txn_management" +
                         " WHERE backoffice_code = boData.backoffice_code)" +
                   " AND txnMgmt.txn_type='UNKNOWN')" +
             " AND ({fn TIMESTAMPADD(SQL_TSI_DAY, txnMgmt.TXN_HOUSEKEEPING, TMP.VALUE_DATE)} < CURRENT_TIMESTAMP))" +
    " GROUP BY boData.BO_TXN_ID";

  private static final String GET_EXPIRED_BO_IDS_1 =
    "SELECT DISTINCT (boData.BO_TXN_ID)" +
    " FROM SECL_BO_DATA_STATUS_HIST boData," +
          " SECM_TXN_MANAGEMENT txnMgmt," +
          " SECL_BO_DATA_STATUS_HIST_TEMP TMP" +
    " WHERE boData.BO_TXN_ID=TMP.BO_TXN_ID" +
      " AND boData.LAST_UPDATE_TIME=TMP.LAST_UPDATE_TIME" +
      " AND (boData.HIT_STATUS is not null AND boData.HIT_STATUS not in (?,?,?))" +
      " AND txnMgmt.backoffice_code = boData.backoffice_code" +
      " AND boData.txn_type = txnMgmt.txn_type" +
      " AND ({fn TIMESTAMPADD (SQL_TSI_DAY, txnMgmt.TXN_HOUSEKEEPING, TMP.VALUE_DATE)} < CURRENT_TIMESTAMP)" +
    " GROUP BY boData.BO_TXN_ID";

   private static final String GET_EXPIRED_BO_IDS_2 =
     "SELECT DISTINCT (boData.BO_TXN_ID)" +
     " FROM SECL_BO_DATA_STATUS_HIST boData," +
          " SECM_TXN_MANAGEMENT txnMgmt," +
          " SECL_BO_DATA_STATUS_HIST_TEMP TMP" +
     " WHERE  boData.BO_TXN_ID=TMP.BO_TXN_ID" +
        " AND boData.LAST_UPDATE_TIME=TMP.LAST_UPDATE_TIME" +
        " AND (boData.HIT_STATUS is not null" +
                " AND boData.HIT_STATUS not in (?,?,?))" +
        " AND txnMgmt.backoffice_code = boData.backoffice_code" +
        " AND (boData.txn_type NOT IN (" +
                    "SELECT txn_type" +
                    " FROM secm_txn_management" +
                    " WHERE backoffice_code = boData.backoffice_code)" +
                " AND txnMgmt.txn_type='UNKNOWN')" +
        " AND ({fn TIMESTAMPADD (SQL_TSI_DAY, txnMgmt.TXN_HOUSEKEEPING, TMP.VALUE_DATE)} < CURRENT_TIMESTAMP)" +
     " GROUP BY boData.BO_TXN_ID";

  private static final String UPDATE_TEMP_TABLE_FOR_DATA_LIFE_STATUS =
    "UPDATE SECL_BO_DATA_STATUS_HIST_TEMP" +
    " SET DATA_LIFE_STATUS= ?" +
    " WHERE BO_TXN_ID = ?";

  private static final String SELECT_LAST_UPDATED_BO_LOG_RECORD =
    "select BO_TXN_ID, BACKOFFICE_CODE, CHANNEL_NAME, TXN_TYPE," +
          " DATA_LIFE_STATUS, MATCH_STATUS, MATCH_CATEG_ID, HIT_STATUS," +
          " LAST_UPDATE_TIME, ACTUAL_VALUE_DATE, SCREENING_TIME" +
    " from SECL_BO_DATA_STATUS_HIST" +
    " where BO_TXN_ID=?" +
    " ORDER BY LAST_UPDATE_TIME DESC";

  private static final String SELECT_PURGE_READY_BO_IDS_FROM_TEMP_TABLE =
    "select BO_TXN_ID" +
    " from SECL_BO_DATA_STATUS_HIST_TEMP" +
    " where DATA_LIFE_STATUS = 7";

  public int updateData(String query, Integer[] params, Connection conn)
  throws SQLException {
    PreparedStatement createStatement = conn.prepareStatement(query);
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("Entering updateData with query: " + query);
    }
    for (int i = 0; i < params.length; i++) {
      Integer param = params[i];
      createStatement.setInt(i + 1, param);
    }
    int updatedRows = createStatement.executeUpdate();
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("Exiting updateData with " + updatedRows + " rows updated");
    }

    return updatedRows;
  }

  public int insertAllPurgeRecordsToTempTable(Connection pCtx) {
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("Entering into insertAllPurgeRecordsToTempTable ...");
    }
    String boTxnId;
    int insertCount=0;
    try {
      PreparedStatement prepareStatement = pCtx
          .prepareStatement(GET_LIST_OF_DATA_TO_INSERT_IN_TEMP_TABLE);
      prepareStatement.setInt(1, NON_FINAL_STATUS_ACK_HIT );
      prepareStatement.setInt(2, NON_FINAL_STATUS_OUT_RECHECK);
      prepareStatement.setInt(3, NON_FINAL_STATUS_OUT_PENDING);
      prepareStatement.setInt(4, NON_FINAL_STATUS_ACK_HIT );
      prepareStatement.setInt(5, NON_FINAL_STATUS_OUT_RECHECK);
      prepareStatement.setInt(6, NON_FINAL_STATUS_OUT_PENDING);

      PreparedStatement insertStmt = pCtx
          .prepareStatement(INSERT_INTO_SECL_BO_DATA_TEMP_BO_TXN_ID);

      ResultSet rs = prepareStatement.executeQuery();
      while (rs.next()) {
        boTxnId = rs.getString("BO_TXN_ID");
        Timestamp maxValueDate = rs.getTimestamp("MAX_VALUE_DATE");
        Timestamp lastUpdateDate = rs.getTimestamp("LAST_UPDATED");
        insertStmt.setString(1, boTxnId);
        insertStmt.setTimestamp(2, maxValueDate);
        insertStmt.setTimestamp(3, lastUpdateDate);
        insertCount = insertStmt.executeUpdate();
      }
    } catch (SQLException e) { Log.getLogWriter().error("Component: UseCase1-SECURITAS:appstat|Event Severity: Fatal|Event Class: MatchingEngine|Description: Issue while purging transactions.-insertAllPurgeRecordsToTempTable Summary: Error occurred while inserting data to temp table: " + TestHelper.getStackTrace(e));
    }
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("Exiting into insertAllPurgeRecordsToTempTable...insertCount=" + insertCount);
    }
    return insertCount;
  }

  public int markAllRecordsForPurge(Connection pCtx) {
    int updateTempTable = updateTempTable(pCtx);
    insertLogData(pCtx);
    return updateTempTable;
  }

  private void insertLogData(Connection pCtx) {
    int currentBatchSize = 0;
    try {
      PreparedStatement prepareStatement = pCtx
          .prepareStatement(SELECT_LAST_UPDATED_BO_LOG_RECORD);
      PreparedStatement insertStmt = pCtx
          .prepareStatement(INSERT_INTO_LOG_TABLE_FOR_PURGED_RECORDS);
      ResultSet listOfAllPurgeReadyBOIds = getListOfAllPurgeReadyBOIdsFromTempTable(pCtx);
      if (null != listOfAllPurgeReadyBOIds) {
        while (listOfAllPurgeReadyBOIds.next()) {
          String boId = listOfAllPurgeReadyBOIds.getString("BO_TXN_ID");
          prepareStatement.setString(1, boId);
          ResultSet resultSet = prepareStatement.executeQuery();
          while (resultSet.next()) {
            insertStmt.setString(1, resultSet.getString("BO_TXN_ID"));
            insertStmt.setString(2, resultSet.getString("BACKOFFICE_CODE"));
            insertStmt.setString(3, resultSet.getString("CHANNEL_NAME"));
            insertStmt.setString(4, resultSet.getString("TXN_TYPE"));
            insertStmt.setInt(5, MARK_AS_TO_PURGE);
            insertStmt.setString(6, resultSet.getString("MATCH_STATUS"));
            insertStmt.setString(7, resultSet.getString("MATCH_CATEG_ID"));
            insertStmt.setString(8, resultSet.getString("HIT_STATUS"));
            insertStmt.setString(9, resultSet.getString("ACTUAL_VALUE_DATE"));
            insertStmt.setString(10, "SCREENING_TIME");
            insertStmt.addBatch();
            currentBatchSize++;
            if (currentBatchSize > maxBatchSize) {
              insertStmt.executeBatch();
              pCtx.commit();
              currentBatchSize = 0;
            }
            break;
          }
        }
        insertStmt.executeBatch();
        pCtx.commit();
      }
    } catch (Exception e) {
      Log.getLogWriter().error("Component: UseCase1-SECURITAS:appstat|Event Severity: Fatal|Event Class: MatchingEngine|Description: Issue while insering raw data.-insertLogData Summary:  " + TestHelper.getStackTrace(e));
    }
  }

  private ResultSet getListOfAllPurgeReadyBOIdsFromTempTable(Connection pCtx) {
    ResultSet executeQuery = null;
    try {
      PreparedStatement selectPurgeReadyBOIds = pCtx
          .prepareStatement(SELECT_PURGE_READY_BO_IDS_FROM_TEMP_TABLE);
      executeQuery = selectPurgeReadyBOIds.executeQuery();
      return executeQuery;
    } catch (SQLException e) {
      Log.getLogWriter().error("Component: UseCase1-SECURITAS:appstat|Event Severity: Fatal|Event Class: MatchingEngine|Description: Issue while purging transactions.-getListOfAllPurgeReadyBOIdsFromTempTable Summary:  " + TestHelper.getStackTrace(e));
    }
    return executeQuery;
  }

  private int updateTempTable(Connection pCtx) {
    int count1 = 0;
    int count2 = 0;
    try {
      ResultSet listOfAllPurgeReadyBOIds = getListOfAllPurgeReadyBOIdPartOne(pCtx);
      ResultSet listOfAllPurgeReadyBOIdsTwo = getListOfAllPurgeReadyBOIdPartTwo(pCtx);
      PreparedStatement updateTempTableStatement = pCtx
          .prepareStatement(UPDATE_TEMP_TABLE_FOR_DATA_LIFE_STATUS);
      if (null != listOfAllPurgeReadyBOIds) {
        count1 = updateTempTableHelper(pCtx, listOfAllPurgeReadyBOIds,
            updateTempTableStatement);
      }
      if (null != listOfAllPurgeReadyBOIdsTwo) {
        count2 = updateTempTableHelper(pCtx,
            listOfAllPurgeReadyBOIdsTwo, updateTempTableStatement);
      }
    } catch (Exception e) {
      Log.getLogWriter().error("Component: UseCase1-SECURITAS:appstat|Event Severity: Fatal|Event Class: MatchingEngine|Description: Issue while purging transactions.-updateTempTable Summary:  " + TestHelper.getStackTrace(e));
    }
    return count1 + count2;
  }

  private int updateTempTableHelper(Connection pCtx,
                                    ResultSet listOfAllPurgeReadyBOIds,
                                    PreparedStatement updateTempTableStatement)
  throws SQLException {
    int count=0;
    int currentBatchSize=0;
    while (listOfAllPurgeReadyBOIds.next()) {
      count++;
      String boId = listOfAllPurgeReadyBOIds.getString("BO_TXN_ID");
      updateTempTableStatement.setInt(1, MARK_AS_TO_PURGE);
      updateTempTableStatement.setString(2, boId);
      updateTempTableStatement.addBatch();
      currentBatchSize++;
      if (currentBatchSize > maxBatchSize) {
        updateTempTableStatement.executeBatch();
        pCtx.commit();
        currentBatchSize = 0;
      }
    }
    updateTempTableStatement.executeBatch();
    pCtx.commit();
    return count;
  }

  public void updateLogForBOExpiredRecord(Connection pCtx) {
    Log.getLogWriter().info("Enterting into updateLogForBOExpiredRecord");
    try {
      insertIntoLog(pCtx);
    } catch (SQLException e) {
      Log.getLogWriter().error("Component: UseCase1-SECURITAS:appstat|Event Severity: Fatal|Event Class: MatchingEngine|Description: Issue while purging transactions.-updateLogForBOExpiredRecord Summary: ERROR occurred while updating logs for purge: " + TestHelper.getStackTrace(e));
    }
    Log.getLogWriter().info("Exiting from updateLogForBOExpiredRecord");
  }

  private static void insertIntoLog(Connection pCtx) throws SQLException {
    int currentBatchSize = 0;
    PreparedStatement prepareStatement = pCtx
        .prepareStatement(GET_LIST_OF_EXPIRED_BO_IDS);
    ResultSet result = prepareStatement.executeQuery();
    PreparedStatement insertStmt = pCtx
        .prepareStatement(INSERT_INTO_LOG_TABLE_FOR_PURGED_RECORDS);
    while(result.next()){
      insertStmt.setString(1, result.getString("BACKOFFICE_TXN_ID"));
      insertStmt.setString(2, result.getString("BACKOFFICE_CODE"));
      insertStmt.setString(3, result.getString("CHANNEL_NAME"));
      insertStmt.setString(4, result.getString("TXN_TYPE"));
      insertStmt.setString(5, result.getString("DATA_LIFE_STATUS"));
      insertStmt.setString(6, result.getString("MATCH_STATUS"));
      insertStmt.setString(7, result.getString("MATCH_CATEG_ID"));
      insertStmt.setString(8, result.getString("HIT_STATUS"));
      insertStmt.setString(9, result.getString("LAST_UPDATE_TIME"));
      insertStmt.setString(10, result.getString("ACTUAL_VALUE_DATE"));
      insertStmt.addBatch();
      currentBatchSize++;
      if (currentBatchSize>maxBatchSize) {
        insertStmt.executeBatch();
        pCtx.commit();
        currentBatchSize = 0;
      }
    }
    insertStmt.executeBatch();
    pCtx.commit();
  }

  private ResultSet getListOfAllPurgeReadyBOIdPartOne(Connection pCtx) {
    try {
      PreparedStatement prepareStatement = pCtx
          .prepareStatement(GET_EXPIRED_BO_IDS_1);
      prepareStatement.setInt(1, NON_FINAL_STATUS_ACK_HIT);
      prepareStatement.setInt(2, NON_FINAL_STATUS_OUT_RECHECK);
      prepareStatement.setInt(3, NON_FINAL_STATUS_OUT_PENDING);
      ResultSet rs = prepareStatement.executeQuery();
      return rs;
    } catch (SQLException e) {
      Log.getLogWriter().error("Component: UseCase1-SECURITAS:appstat|Event Severity: Fatal|Event Class: MatchingEngine|Description: Issue while mark and delete expired transactions.-getListOfAllPurgeReadyBOIds Summary: Error occurred while getting the list of all purge ready BOIds:" + TestHelper.getStackTrace(e));
      return null;
    }
  }

  private ResultSet getListOfAllPurgeReadyBOIdPartTwo(Connection pCtx) {
    try {
      PreparedStatement prepareStatement = pCtx
          .prepareStatement(GET_EXPIRED_BO_IDS_2);
      prepareStatement.setInt(1, NON_FINAL_STATUS_ACK_HIT);
      prepareStatement.setInt(2, NON_FINAL_STATUS_OUT_RECHECK);
      prepareStatement.setInt(3, NON_FINAL_STATUS_OUT_PENDING);
      ResultSet rs = prepareStatement.executeQuery();
      return rs;
    } catch (SQLException e) {
      Log.getLogWriter().error("Component: UseCase1-SECURITAS:appstat|Event Severity: Fatal|Event Class: MatchingEngine|Description: Issue while mark and delete expired transactions.-getListOfAllPurgeReadyBOIds Summary: Error occurred while getting the list of all purge ready BOIds:" + TestHelper.getStackTrace(e));
      return null;
    }
  }
}
