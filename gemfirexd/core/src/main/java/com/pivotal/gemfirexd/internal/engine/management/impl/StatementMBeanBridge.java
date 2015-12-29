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
package com.pivotal.gemfirexd.internal.engine.management.impl;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.pivotal.gemfirexd.internal.impl.sql.GenericStatement;
import com.pivotal.gemfirexd.internal.impl.sql.StatementStats;

/**
 * Bridge / implementation of statement attributes
 * 
 * @author ajayp
 * 
 */

public class StatementMBeanBridge implements Cleanable {
  private boolean isQueryNode = false;
  private Statistics statistics;
  private String statementSource;

  /**
   * @param args
   */
  public static void main(String[] args) {

  }

  public StatementMBeanBridge(GenericStatement statement) {
    this.update(statement);
  }

  public void update(GenericStatement statement) {
    StatementStats statementStats = statement.getPreparedStatement().getStatementStats();
    this.statistics = statementStats.getStatistics(); // we probably needn't do this as Statistics Object remains the same??
    this.isQueryNode = statementStats.isQueryNode();
    this.statementSource = statement.getSource();
  }
 
  
  public long getQNNumTimesCompiled() {
      return this.statistics.getLong("QNNumTimesCompiled");   
  }
  
  public long getDNNumTimesCompiled() {
    return this.statistics.getLong("DNNumTimesCompiled");
  }
  
  
  

  public long getQNNumExecutions() {
      return this.statistics.getLong("QNNumExecutions"); 
  }
  

  public long getDNNumExecution() {
      return this.statistics.getLong("DNNumExecutions");
  }
  
  
  public long getQNNumExecutionsInProgress() {
    return this.statistics.getLong("QNNumExecutionsInProgress");
  }

  public long getDNNumExecutionsInProgress() {
    return this.statistics.getLong("DNNumExecutionsInProgress");
  }

  public long getQNNumTimesGlobalIndexLookup() {   
      return this.statistics.getLong("QNNumTimesGlobalIndexLookup");
  }
  
  public long getDNNumTimesGlobalIndexLookup() {
      return this.statistics.getLong("DNNumTimesGlobalIndexLookup");
  }
  
  

  public long getQNNumRowsModified() {
      return this.statistics.getLong("QNNumRowsModified");
  }
  
  public long getDNNumRowsModified() {
      return this.statistics.getLong("DNNumRowsModified");
  }
  
  

  public long getQNParseTime() {
      return this.statistics.getLong("QNParseTime");

  }
  public long getDNParseTime() {
      return this.statistics.getLong("DNParseTime");
  }
  
  

  public long getQNBindTime() {
      return this.statistics.getLong("QNBindTime");
  }
  
  public long getDNBindTime() {
      return this.statistics.getLong("DNBindTime");
  }
  
  

  public long getQNOptimizeTime() {
      return this.statistics.getLong("QNOptimizeTime");

  }
  public long getDNOptimizeTime() {
      return this.statistics.getLong("DNOptimizeTime");
  }
  
  

  public long getQNRoutingInfoTime() {
      return this.statistics.getLong("QNRoutingInfoTime");

  }
  public long getDNRoutingInfoTime() {
      return this.statistics.getLong("DNRoutingInfoTime");
  }
  
  

  public long getQNGenerateTime() {
      return this.statistics.getLong("QNGenerateTime");

  }
  public long getDNGenerateTime() {
      return this.statistics.getLong("DNGenerateTime");
  }

  public long getQNTotalCompilationTime() {
      return this.statistics.getLong("QNTotalCompilationTime");
  }
  public long getDNTotalCompilationTime() {
      return this.statistics.getLong("DNTotalCompilationTime");
  }

  
  
  public long getQNExecuteTime() {
      return this.statistics.getLong("QNExecuteTime");
  }
  public long getDNExecutionTime() {
      return this.statistics.getLong("DNExecuteTime");
  }
  
  

  public long getQNProjectionTime() {
      return this.statistics.getLong("QNProjectionTime");
  }
  public long getDNProjectionTime() {
      return this.statistics.getLong("DNProjectionTime");
  }
  
  

  public long getQNTotalExecutionTime() {
      return this.statistics.getLong("QNTotalExecutionTime");
  }
  public long getDNTotalExecutionTime() {
      return this.statistics.getLong("DNTotalExecutionTime");
  }

  public long getQNRowsModificationTime() {
      return this.statistics.getLong("QNRowsModificationTime");
  }
  public long getDNRowsModificationTime() {
      return this.statistics.getLong("DNRowsModificationTime");
    
  }
  
  public long getDNNumProjectedRows() {
     return this.statistics.getLong("DNNumProjectedRows");
  }

  public long getDNNumNLJoinRowsReturned() {
    return this.statistics.getLong("DNNumNLJoinRowsReturned");
  }

  public long getDNNumHASHJoinRowsReturned() {
    return this.statistics.getLong("DNNumHASHJoinRowsReturned");
  }

  public long getDNNumTableRowsScanned() {
      return this.statistics.getLong("DNNumTableRowsScanned");
  }

  public long getDNNumRowsHashScanned() {
      return this.statistics.getLong("DNNumRowsHashScanned");
  }

  public long getDNNumIndexRowsScanned() {
     return this.statistics.getLong("DNNumIndexRowsScanned");
  }

  public long getDNNumRowsSorted() {
    return this.statistics.getLong("DNNumRowsSorted");
  }

  public long getDNNumSortRowsOverflowed() {
      return this.statistics.getLong("DNNumSortRowsOverflowed");
  }

  public long getDNNumSingleHopExecutions() {
     return this.statistics.getLong("DNNumSingleHopExecutions");
  }

  public long getDNSubQueryNumRowsSeen() {
      return this.statistics.getLong("DNSubQueryNumRowsSeen");
  }

  public long getQNNumRowsSeen() {
     return this.statistics.getLong("QNNumRowsSeen");
  }

  public long getQNMsgSendTime() {
      return this.statistics.getLong("QNMsgSendTime");
  }

  public long getDNMsgDeSerTime() {
      return this.statistics.getLong("DNMsgDeSerTime");
  }

  public long getQNMsgSerTime() {
     return this.statistics.getLong("QNMsgSerTime");
  }

  public long getDNMsgProcessTime() {
     return this.statistics.getLong("DNMsgProcessTime");
  }

  public long getDNResultIterationTime() {
     return this.statistics.getLong("DNResultIterationTime");
  }

  public long getDNRespSerTime() {
    return this.statistics.getLong("DNRespSerTime");
  }

  public long getDNRespDeSerTime() {
    return this.statistics.getLong("DNRespDeSerTime");

  }

  public long getDNThrottleTime() {
     return this.statistics.getLong("DNThrottleTime");
  }

  public long getDNNLJoinTime() {
    return this.statistics.getLong("DNNLJoinTime");

  }

  public long getDNHASHJoinTime() {
    return this.statistics.getLong("DNHASHJoinTime");

  }

  public long getDNTableScanTime() {
    return this.statistics.getLong("DNTableScanTime");
  }

  public long getDNHashScanTime() {
    return this.statistics.getLong("DNHashScanTime");

  }

  public long getDNIndexScanTime() {
     return this.statistics.getLong("DNIndexScanTime");
  }

  public long getDNSortTime() {
    return this.statistics.getLong("DNSortTime");

  }

  public long getDNSubQueryExecutionTime() {
     return this.statistics.getLong("DNSubQueryExecutionTime");
  }

  public String getString() {
    return this.statementSource;
  }

  @Override
  public void cleanUp() {
    this.statistics = null;
  }

  public long getQNNumRowsSeenId() {
     return this.statistics.getLong("QNNumRowsSeen");
  }

  public long getQNRespDeSerTime() {
    return this.statistics.getLong("QNRespDeSerTime");
  }

}