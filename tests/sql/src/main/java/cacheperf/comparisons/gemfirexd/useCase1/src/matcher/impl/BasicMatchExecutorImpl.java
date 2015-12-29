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
package cacheperf.comparisons.gemfirexd.useCase1.src.matcher.impl;

import static cacheperf.comparisons.gemfirexd.useCase1.src.matcher.utils.MatchStoredProcParams.channelPrimaryKey;
import static cacheperf.comparisons.gemfirexd.useCase1.src.matcher.utils.MatchStoredProcParams.channelTableName;
import static cacheperf.comparisons.gemfirexd.useCase1.src.matcher.utils.MatchStoredProcParams.queryExecutorStoredProcName;
import static cacheperf.comparisons.gemfirexd.useCase1.src.matcher.utils.MatchStoredProcParams.resultProcessorName;
import static cacheperf.comparisons.gemfirexd.useCase1.src.matcher.utils.MatchStoredProcParams.schemaNameString;

import cacheperf.comparisons.gemfirexd.useCase1.src.matcher.MatchExecutor;
import cacheperf.comparisons.gemfirexd.useCase1.src.matcher.model.MatchingInfo;
import cacheperf.comparisons.gemfirexd.useCase1.src.matcher.model.MatchResultSet;
import cacheperf.comparisons.gemfirexd.useCase1.src.matcher.model.Query;
import cacheperf.comparisons.gemfirexd.useCase1.src.matcher.utils.StoredProcUtils;

import com.pivotal.gemfirexd.procedure.ProcedureExecutionContext;

import hydra.Log;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BasicMatchExecutorImpl implements MatchExecutor {

  /**
   * This method is to get the queryString from buildQuery method and execute the query using the connection. Based on the number of resultset, match status is decided.
   */
  @Override
  public MatchResultSet match(List<Object> prevResultList, Map<String, Map<String, String>> backOfficeMsg, Set<MatchingInfo> matchingKeySet, ProcedureExecutionContext pCtx){
    
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine(
        "BasicMatchExecutorImpl-match entering" +
        " prevResultList=" + prevResultList +
        " backOfficeMsg=" + backOfficeMsg +
        " matchingKeySet=" + matchingKeySet);
    }

    MatchResultSet matchResultSet = new MatchResultSet();
    
    /* ITR-2
     * 1. <<start>>
     * 2. call buildInClause() to create inClause out of prevResultList
     * 3. call buildQuery() to create Query (SQL, ListofParamValues)
     * 4. call buildCallableStatementString to create a string to be passed to callablestatement   
     * 5. Invoke QueryExecutorStoredProc by passing the SQL query and  ListofParamValues
     * 6. ResultSet returned by QueryExecutorStoredProc will be the result of query executed in different nodes of GemFireXD
     * 7. Set the resultSet in MatchResultSet and pass
     * 8. In case of error, setErrorStatus in MatchResultSet
     */
    
    //based on previous matches build a string of format SEC_OWNER.SECT_CHANNEL_DATA.CHANNEL_TXN_ID IN (?,?,...)
    String inClause = buildInClause(prevResultList);
    
    //Build Query (SQL Query String and List of values to be set to "?" marks
    Query query = buildQuery(prevResultList, inClause, backOfficeMsg, matchingKeySet);
    
    if (query == null) {
      if (Log.getLogWriter().fineEnabled()) {
        Log.getLogWriter().fine(
           "BasicMatchExecutorImpl-match" +
           " query=" + query +
           " returning MatchingResultSet with null JDBC resultset");
      }
      
      matchResultSet.setErrorState(false);
      //returnung MatchingResultSet with null jdbc resultset
      matchResultSet.setResultSet(null);
      return matchResultSet;
    }
    
    //ITR-8 change ends
    
    //build the string to invoke queryExecutorStoredProcedure through callable statement
    String callableStmtString = buildCallableStatementString(inClause);
    
    CallableStatement callableStmt;
    try {
      //create a callable statement and have the INPUT params set and output param registered
      callableStmt= prepareCall(callableStmtString, query, prevResultList, pCtx);
      if (Log.getLogWriter().fineEnabled()) {
        Log.getLogWriter().fine(
          "BasicMatchExecutorImpl-match executing" +
          " storedproc=" + callableStmtString +
          " query=" + query);
      }
      callableStmt.execute();
      
      matchResultSet.setResultSet(callableStmt.getResultSet());
      matchResultSet.setErrorState(StoredProcUtils.toBoolean(callableStmt.getInt(3)));
    } catch (SQLException e) {
      matchResultSet.setErrorState(true);
      if (Log.getLogWriter().fineEnabled()) {
        Log.getLogWriter().fine("BasicMatchExecutorImpl-match error=" + e);
      }
      
    }
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("BasicMatchExecutorImpl-match exiting" +
        " matchResultSet=" + matchResultSet);
    }
    return matchResultSet;
  }
  
  public String buildInClause(List<Object> prevResultList) {
    //previous result is null or empty, return empty string
    if(prevResultList==null || prevResultList.size()==0)
    {
      return "";
    }
    
    //otherwise build inclause of form-  SEC_OWNER.SECT_CHANNEL_DATA.CHANNEL_TXN_ID IN (?,?,...)
    StringBuilder inClause = new StringBuilder(schemaNameString);
      inClause.append(channelTableName);
      inClause.append("." );
      inClause.append(channelPrimaryKey);
      inClause.append(" IN (" );
      
      
      int numberOfRecords = 0;
      
      for(Object txnId : prevResultList)
      {
        //any subsequent record, add "," as separator
        if(numberOfRecords>0)
        {
          inClause.append(",");
        }
        
        inClause.append("?");
                
        numberOfRecords++;
        
      }
      
      //close the in clause
      inClause.append(") ");

    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine(
        "BasicMatchExecutorImpl-buildInClause" +
        " numberOfRecords=" + numberOfRecords +
        " sb=" + inClause);
    }
    return   inClause.toString();  
  }
  
  /**
   * Based on the BOMessage and MatchingInfo, query will be framed with where condition.
   * 
   * @param backOfficeMsg
   * @param matchingKeySet
   * @return
   */
  private Query buildQuery(List<Object> prevResultList, String inClause, Map<String, Map<String, String>> backOfficeMsg, Set<MatchingInfo> matchingKeySet) {
    
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine(
        "BasicMatchExecutorImpl-buildQuery" +
        " backOfficeMsg=" + backOfficeMsg +
        " matchingKeySet=" + matchingKeySet);
    }
    
    /*
     * Purpose : query to be executed against channel data and its extension tables (extra key). Values of keys come from backOfficeMsg
     * sample query: select chn_txnid from t1, t2, t3 where t1.chn_txnid=t2.chn_txn_id and t1.chn_txnid=t3.chn_txn_id and t1.col1=? and t2.col2=? and t3.col2=? 
     * 
     * 1. backOfficeMsg = Map { bo_tablename , Map {col, val} }
     * 2. matchingKeySet = Set { MatchingInfo () }
     * 3. Loop
     *     matchingKeySet.nextval
     *     get col, bo_table, chn_table
     *     get value of col using col, bo_table name
     *     append to query
     */
    //define variable to build SQL query
    StringBuilder queryString = new StringBuilder();
    
    //define variable to build bindVariable values
    List<Object> bindVariableValueList = new ArrayList<Object>();
    
    //if previous result list is non empty, then populate bindVariableValueList with values from prevResultList
    if(prevResultList!=null && prevResultList.size()!=0)
    {
      bindVariableValueList.addAll(prevResultList);
    }
    
    
    queryString.append("<local> SELECT ");
    queryString.append(schemaNameString).append(channelTableName).append(".").append(channelPrimaryKey).append(" FROM ");
    
    // prepare the list of tables which are part of the query.
    Set<String> chnlTableNames = new HashSet<String>();
    for (MatchingInfo matchingInfo : matchingKeySet) {
      chnlTableNames.add(matchingInfo.getChnDataTable());
    }
    
    int tableCount = 0;
    // Add the list tables to the from part of the query
    for (String chnlTableName : chnlTableNames) {

      if(tableCount == 0){
        queryString.append(schemaNameString).append(chnlTableName);
      } else{
        queryString.append(", ").append(schemaNameString).append(chnlTableName);
      }
      tableCount++;
    }
    
    if(matchingKeySet.size() > 0){
      queryString.append(" WHERE ");
    }
    
    //add In clause
    queryString.append(inClause);
    
    
    // Based on each MatchingInfo, creating the WHERE condition part of the query.
    //TODO: For the secondary key matching, joining of the Channel_tables should be added part of the where condition.
    int columnCount = 0;
    for (MatchingInfo matchingInfo : matchingKeySet) {
      
      Map<String, String> boTable = backOfficeMsg.get(matchingInfo.getBoDataTable());
      // If there is no boTable for the given MatchingInfo.boDataTable value, Don't consider this matchinginfo
      if(boTable == null) 
      {
        continue;
      }
      
      Object boCurrentColumnValue = boTable.get(matchingInfo.getKeyName());
      // If there is no BackOffice column exist in 'matchingInfo' object, Don't consider this column
      if(boCurrentColumnValue == null) 
      {
        if (Log.getLogWriter().fineEnabled()) {
          Log.getLogWriter().fine(
            "BasicMatchExecutorImpl-buildQuery returning null due to " +
            " key=" + matchingInfo.getKeyName() +
            " value=" + boCurrentColumnValue);
        }
        return null;
      }
      
      //" and " has to be added if there an inclause or for any subsequent query critreia addition
      if(columnCount > 0 || ! "".equals(inClause)){
        
        queryString.append(" and ");
        }
      
      //ITR-6: making uppercase to make the query case insensitive
      //build string of format <schema>.<tableName>.<columnName>=?
      //In case, column if of type VARCHAR/LONG VARCHAR/CHAR, format: upper(<schema>.<tableName>.<columnName>=upper(?) 
      queryString.append(buildFieldCriteria(matchingInfo));
      
      //also add the param value into bindVariableValueList
      bindVariableValueList.add(boCurrentColumnValue);
      columnCount++;
    }

    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("BasicMatchExecutorImpl-buildQuery sb=" + queryString);
    }
    return new Query(queryString.toString(), bindVariableValueList);
  }
  
  
  /**
   * @param matchingInfo
   * @return
   */
  
  
  private String buildFieldCriteria(MatchingInfo matchingInfo) {
    
    /*
     * 1. if(dataType.equalsIgnoreCase("VARCHAR")||dataType.equalsIgnoreCase("CHAR")||dataType.equalsIgnoreCase("LONG VARCHAR")), set isUpperFlag true
     * 2. if isUpperFlag=true, use upper() SQL function to convert both table field [upper(columnName)]] and value [upper(?)]
     */
    
    //define variable to build fieldString
    StringBuilder fieldStringBuilder = new StringBuilder();
    
    String upperStringBegin = null;
    String upperStringEnd = null;
    
    //get dataType of the matching key
    String dataType = matchingInfo.getDataType();
    
    // For charachter type data both column value and bo message field value are to be converted to upper for comparision
        
    if("VARCHAR".equalsIgnoreCase(dataType)||"CHAR".equalsIgnoreCase(dataType)||"LONG VARCHAR".equalsIgnoreCase(dataType))
    {
      /**
       * Commenting out the UPPER function, as the Case insensitive index feature is in place. 
       * In order to revert
       * Uncomment the next two lines and comment the third and fourth line. Drop the case insensitive index
       * 03/07/2013
       */    
//      upperStringBegin="UPPER(";
//      upperStringEnd=")";
      upperStringBegin="";
      upperStringEnd="";
    }
    else
    {
      upperStringBegin="";
      upperStringEnd="";
    }
    
    
    /* Build a string of format :
     * For char,varchar,long varchar type
     * UPPER(schemaNameString.chnDataTable.KeyName)=UPPER(?)
     * 
     *For all other types:
     * 
     * schemaNameString.chnDataTable.KeyName)=?
     */
    fieldStringBuilder.append(upperStringBegin)
    .append(schemaNameString)
    .append(matchingInfo.getChnDataTable()).append(".").append(matchingInfo.getKeyName())
    .append(upperStringEnd)
    .append("=")
    .append(upperStringBegin)
    .append("?")
    .append(upperStringEnd);
    
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine("BasicMatchExecutorImpl-buildFieldCriteria fieldStringBuilder=" + fieldStringBuilder);
    }
    return fieldStringBuilder.toString();
  }


  /**
   * @param inClause
   * @return
   */
  //This method has been intentionally made public so that we can junit test various types of scenarios
  // In future, this method can be moved to a helper class
  public String buildCallableStatementString(String inClause)
  {
    String whereClause;
    if("".equals(inClause))
    {
      whereClause="";
    }
    else
    {
      whereClause = "WHERE " + inClause;
    }
    
    StringBuilder callableStmtFormat = new StringBuilder("{CALL ");
    callableStmtFormat.append(schemaNameString)
      .append(queryExecutorStoredProcName)
      .append("(?,?,?)");
    callableStmtFormat.append("  WITH RESULT PROCESSOR ")
      .append(resultProcessorName)
      .append(" ON TABLE ")
      .append(schemaNameString)
      .append(channelTableName)
      .append(" ")
      .append(whereClause)
      .append("}");
    
    if (Log.getLogWriter().fineEnabled()) {
      Log.getLogWriter().fine(
        "BasicMatchExecutorImpl-buildCallableStatementString sb=" + callableStmtFormat);
    }
    return callableStmtFormat.toString();
    
  }
  /**
   * @param callableStmtString
   * @param query
   * @param prevResultList
   * @param pCtx
   * @return
   * @throws SQLException
   */
  //This method has been intentionally made public so that we can junit test various types of scenarios
  // In future, this method can be moved to a helper class
  public CallableStatement prepareCall(String callableStmtString, Query query, List prevResultList, ProcedureExecutionContext pCtx) throws SQLException
  {
    /*CallableStatement callableStmt = conn
        .prepareCall("{CALL SEC_OWNER.queryExecutorStoredProc(?, ?, ?) WITH RESULT PROCESSOR com.jpmorgan.tss.securitas.strategic.matchingengine.db.gemfirexd.storedproc.MatchResultProcessor ON TABLE SEC_OWNER.SECT_CHANNEL_DATA WHERE CHANNEL_TXN_ID IN ('326e843a-3f32-44f6-a482-e740c738b679',  '80e4064b-732b-40f4-9b38-2dc97b101c26',  'd0aa59b1-79d7-432e-967d-88928c9af1b9') }");
    
    */
    Connection conn = pCtx.getConnection();
    CallableStatement callableStmt = conn
        .prepareCall(callableStmtString);
  
    
    callableStmt.registerOutParameter(3, Types.INTEGER);
    callableStmt.setObject(1, query.getQueryString());
    callableStmt.setObject(2, query.getBindVariableValueList());
    
    if(prevResultList == null || prevResultList.size()==0)
    {
      return callableStmt;
    }
    
    int bindVariablePosition = 4;
    for (Object txnId: prevResultList)
    {
      if (Log.getLogWriter().fineEnabled()) {
        Log.getLogWriter().fine(" prepareCall: txnId=" + txnId );
      }
      callableStmt.setObject(bindVariablePosition, txnId);
      bindVariablePosition++;
    }
    return callableStmt;
  }
}
