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
package com.pivotal.gemfirexd.tools.gfxdtop;

import com.pivotal.gemfirexd.tools.gfxdtop.GfxdConstants;

public class Statement implements Comparable<Statement> {

  private String queryDefn;
  private long numTimesCompiled;
  private long numExecution;
  private long numExecutionsInProgress;
  private long numTimesGlobalIndexLookup;
  private long numRowsModified;
  private long parseTime;
  private long bindTime;
  private long optimizeTime;
  private long routingInfoTime;
  private long generateTime;
  private long totalCompilationTime;
  private long executionTime;
  private long projectionTime;
  private long totalExecutionTime;
  private long rowsModificationTime;
  private long qNNumRowsSeen;
  private long qNMsgSendTime;
  private long qNMsgSerTime;
  private long qNRespDeSerTime;

  public static String[] getGridColumnNames() {
    String[] colNames = new String[] {
        GfxdConstants.MBEAN_COLNAME_QUERYDEFINITION,
        GfxdConstants.MBEAN_COLNAME_NUMEXECUTION,
        GfxdConstants.MBEAN_COLNAME_TOTALEXECUTIONTIME,
        GfxdConstants.MBEAN_COLNAME_NUMEXECUTIONSINPROGRESS,
        GfxdConstants.MBEAN_COLNAME_NUMTIMESCOMPILED,
        GfxdConstants.MBEAN_COLNAME_NUMTIMESGLOBALINDEXLOOKUP,
        GfxdConstants.MBEAN_COLNAME_NUMROWSMODIFIED,
        GfxdConstants.MBEAN_COLNAME_PARSETIME,
        GfxdConstants.MBEAN_COLNAME_BINDTIME,
        GfxdConstants.MBEAN_COLNAME_OPTIMIZETIME,
        GfxdConstants.MBEAN_COLNAME_ROUTINGINFOTIME,
        GfxdConstants.MBEAN_COLNAME_GENERATETIME,
        GfxdConstants.MBEAN_COLNAME_TOTALCOMPILATIONTIME,
        GfxdConstants.MBEAN_COLNAME_EXECUTIONTIME,
        GfxdConstants.MBEAN_COLNAME_PROJECTIONTIME,
        GfxdConstants.MBEAN_COLNAME_ROWSMODIFICATIONTIME,
        GfxdConstants.MBEAN_COLNAME_QNNUMROWSSEEN,
        GfxdConstants.MBEAN_COLNAME_QNMSGSENDTIME,
        GfxdConstants.MBEAN_COLNAME_QNMSGSERTIME,
        GfxdConstants.MBEAN_COLNAME_QNRESPDESERTIME };
    return colNames;
  }

  public static String[] getGridColumnAttributes() {
    String[] colAttributes = new String[] {
        GfxdConstants.MBEAN_ATTRIBUTE_QUERYDEFINITION,
        GfxdConstants.MBEAN_ATTRIBUTE_NUMEXECUTION,
        GfxdConstants.MBEAN_ATTRIBUTE_TOTALEXECUTIONTIME,
        GfxdConstants.MBEAN_ATTRIBUTE_NUMEXECUTIONSINPROGRESS,
        GfxdConstants.MBEAN_ATTRIBUTE_NUMTIMESCOMPILED,
        GfxdConstants.MBEAN_ATTRIBUTE_NUMTIMESGLOBALINDEXLOOKUP,
        GfxdConstants.MBEAN_ATTRIBUTE_NUMROWSMODIFIED,
        GfxdConstants.MBEAN_ATTRIBUTE_PARSETIME,
        GfxdConstants.MBEAN_ATTRIBUTE_BINDTIME,
        GfxdConstants.MBEAN_ATTRIBUTE_OPTIMIZETIME,
        GfxdConstants.MBEAN_ATTRIBUTE_ROUTINGINFOTIME,
        GfxdConstants.MBEAN_ATTRIBUTE_GENERATETIME,
        GfxdConstants.MBEAN_ATTRIBUTE_TOTALCOMPILATIONTIME,
        GfxdConstants.MBEAN_ATTRIBUTE_EXECUTIONTIME,
        GfxdConstants.MBEAN_ATTRIBUTE_PROJECTIONTIME,
        
        GfxdConstants.MBEAN_ATTRIBUTE_ROWSMODIFICATIONTIME,
        GfxdConstants.MBEAN_ATTRIBUTE_QNNUMROWSSEEN,
        GfxdConstants.MBEAN_ATTRIBUTE_QNMSGSENDTIME,
        GfxdConstants.MBEAN_ATTRIBUTE_QNMSGSERTIME,
        GfxdConstants.MBEAN_ATTRIBUTE_QNRESPDESERTIME };
    return colAttributes;
  }

  public static int[] getGridColumnWidths() {
    int[] colWidths = new int[] { 300, 150, 160, 180, 150, 200, 150, 130, 130,
        160, 140, 180, 170, 160, 130,  190, 170, 170, 170, 200 };
    return colWidths;
  }

  public String getQueryDefinition() {
    return queryDefn;
  }

  
  public void setQueryDefinition(String queryDefn) {
    this.queryDefn = queryDefn;
  }

  
  public long getNumTimesCompiled() {
    return numTimesCompiled;
  }

  
  public void setNumTimesCompiled(long numTimesCompiled) {
    this.numTimesCompiled = numTimesCompiled;
  }

  
  public long getNumExecution() {
    return numExecution;
  }

  
  public void setNumExecution(long numExecution) {
    this.numExecution = numExecution;
  }

  
  public long getNumExecutionsInProgress() {
    return numExecutionsInProgress;
  }

  
  public void setNumExecutionsInProgress(long numExecutionsInProgress) {
    this.numExecutionsInProgress = numExecutionsInProgress;
  }

  
  public long getNumTimesGlobalIndexLookup() {
    return numTimesGlobalIndexLookup;
  }

  
  public void setNumTimesGlobalIndexLookup(long numTimesGlobalIndexLookup) {
    this.numTimesGlobalIndexLookup = numTimesGlobalIndexLookup;
  }

  
  public long getNumRowsModified() {
    return numRowsModified;
  }

  
  public void setNumRowsModified(long numRowsModified) {
    this.numRowsModified = numRowsModified;
  }

  
  public long getParseTime() {
    return parseTime;
  }

  
  public void setParseTime(long parseTime) {
    this.parseTime = parseTime;
  }

  
  public long getBindTime() {
    return bindTime;
  }

  
  public void setBindTime(long bindTime) {
    this.bindTime = bindTime;
  }

  
  public long getOptimizeTime() {
    return optimizeTime;
  }

  
  public void setOptimizeTime(long optimizeTime) {
    this.optimizeTime = optimizeTime;
  }

  
  public long getRoutingInfoTime() {
    return routingInfoTime;
  }

  
  public void setRoutingInfoTime(long routingInfoTime) {
    this.routingInfoTime = routingInfoTime;
  }

  
  public long getGenerateTime() {
    return generateTime;
  }

  
  public void setGenerateTime(long generateTime) {
    this.generateTime = generateTime;
  }

  
  public long getTotalCompilationTime() {
    return totalCompilationTime;
  }

  
  public void setTotalCompilationTime(long totalCompilationTime) {
    this.totalCompilationTime = totalCompilationTime;
  }

  
  public long getExecutionTime() {
    return executionTime;
  }

  
  public void setExecutionTime(long executionTime) {
    this.executionTime = executionTime;
  }

  
  public long getProjectionTime() {
    return projectionTime;
  }

  
  public void setProjectionTime(long projectionTime) {
    this.projectionTime = projectionTime;
  }

  
  public long getTotalExecutionTime() {
    return totalExecutionTime;
  }

  
  public void setTotalExecutionTime(long totalExecutionTime) {
    this.totalExecutionTime = totalExecutionTime;
  }

  
  public long getRowsModificationTime() {
    return rowsModificationTime;
  }

  
  public void setRowsModificationTime(long rowsModificationTime) {
    this.rowsModificationTime = rowsModificationTime;
  }

  
  public long getqNNumRowsSeen() {
    return qNNumRowsSeen;
  }

  
  public void setqNNumRowsSeen(long qNNumRowsSeen) {
    this.qNNumRowsSeen = qNNumRowsSeen;
  }

  
  public long getqNMsgSendTime() {
    return qNMsgSendTime;
  }

  
  public void setqNMsgSendTime(long qNMsgSendTime) {
    this.qNMsgSendTime = qNMsgSendTime;
  }

  
  public long getqNMsgSerTime() {
    return qNMsgSerTime;
  }

  
  public void setqNMsgSerTime(long qNMsgSerTime) {
    this.qNMsgSerTime = qNMsgSerTime;
  }

  
  public long getqNRespDeSerTime() {
    return qNRespDeSerTime;
  }
  
  public void setqNRespDeSerTime(long qNRespDeSerTime) {
    this.qNRespDeSerTime = qNRespDeSerTime;
  }

  
  
  //TODO : Externalize to another class to make it configurable the sort criteria
  @Override
  public int compareTo(Statement otherStmt) {
    
    if(otherStmt.numExecutionsInProgress == numExecutionsInProgress) {
      if(otherStmt.numExecution == numExecution) {
        if(otherStmt.executionTime == executionTime) {
          if(otherStmt.totalExecutionTime == totalExecutionTime) {
            return (int) (otherStmt.numTimesGlobalIndexLookup - numTimesGlobalIndexLookup);
            /*
            if(otherStmt.numTimesGlobalIndexLookup == numTimesGlobalIndexLookup) {
              
            }else{
              return (int) (numTimesGlobalIndexLookup - otherStmt.numTimesGlobalIndexLookup);  
            }*/
          }else{
            return (int) (otherStmt.totalExecutionTime - totalExecutionTime);  
          } 
        }else{
          return (int) (otherStmt.executionTime - executionTime);  
        } 
      }else{
        return (int) (otherStmt.numExecution - numExecution);  
      }
    }else 
      return (int) (otherStmt.numExecutionsInProgress -numExecutionsInProgress);
  }
  
  
}
