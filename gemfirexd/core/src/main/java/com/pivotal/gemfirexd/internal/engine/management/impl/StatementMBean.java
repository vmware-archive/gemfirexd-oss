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

import java.util.concurrent.atomic.AtomicInteger;

import com.pivotal.gemfirexd.internal.engine.management.StatementMXBean;
import com.pivotal.gemfirexd.internal.impl.sql.GenericStatement;

/**
 * Mbean for Statement
 *
 * @author ajayp
 *
 */

public class StatementMBean implements StatementMXBean {

  private final StatementMBeanBridge bridge;
  private final AtomicInteger refCount;


  public StatementMBean(StatementMBeanBridge bridge) {
    this.bridge   = bridge;
    this.refCount = new AtomicInteger();
  }

  public void updateBridge(GenericStatement statement) {
    this.refCount.incrementAndGet();
    this.bridge.update(statement);
  }

  @Override
  public long getQNNumTimesCompiled() {
    return bridge.getQNNumTimesCompiled();
  }
  
  @Override
  public long getDNNumTimesCompiled() {
    return bridge.getDNNumTimesCompiled();
  }



  @Override
  public long getQNNumExecutions() {
    return bridge.getQNNumExecutions();
  }
  
  @Override
  public long getDNNumExecution() {
    return bridge.getDNNumExecution();
  }



  @Override
  public long getQNNumExecutionsInProgress() {
   return bridge.getQNNumExecutionsInProgress();
  }
  
  @Override
  public long getDNNumExecutionsInProgress() {
   return bridge.getDNNumExecutionsInProgress();
  }



  @Override
  public long getQNNumTimesGlobalIndexLookup() {
   return bridge.getQNNumTimesGlobalIndexLookup();
  }
  
  @Override
  public long getDNNumTimesGlobalIndexLookup() {
   return bridge.getDNNumTimesGlobalIndexLookup();
  }



  @Override
  public long getQNNumRowsModified() {
    return bridge.getQNNumRowsModified();
  }
  
  @Override
  public long getDNNumRowsModified() {
    return bridge.getDNNumRowsModified();
  }



  @Override
  public long getQNParseTime() {
    return bridge.getQNParseTime();
  }
  @Override
  public long getDNParseTime() {
    return bridge.getDNParseTime();
  }



  @Override
  public long getQNBindTime() {
    return bridge.getQNBindTime();
  }
  @Override
  public long getDNBindTime() {
    return bridge.getDNBindTime();
  }



  @Override
  public long getQNOptimizeTime() {
    return bridge.getQNOptimizeTime();
  }
  @Override
  public long getDNOptimizeTime() {
    return bridge.getDNOptimizeTime();
  }



  @Override
  public long  getQNRoutingInfoTime() {
    return bridge.getQNRoutingInfoTime();
  }
  @Override
  public long  getDNRoutingInfoTime() {
    return bridge.getDNRoutingInfoTime();
  }



  @Override
  public long  getQNGenerateTime() {
    return bridge.getQNGenerateTime();
  }
  @Override
  public long  getDNGenerateTime() {
    return bridge.getDNGenerateTime();
  }



  @Override
  public long getQNTotalCompilationTime() {
    return bridge.getQNTotalCompilationTime();
  }
  @Override
  public long getDNTotalCompilationTime() {
    return bridge.getDNTotalCompilationTime();
  }



  @Override
  public long getQNExecuteTime() {
    return bridge.getQNExecuteTime();
  }
  @Override
  public long getDNExecutionTime() {
    return bridge.getDNExecutionTime();
  }



  @Override
  public long getQNProjectionTime() {
    return bridge.getQNProjectionTime();
  }
  @Override
  public long getDNProjectionTime() {
    return bridge.getDNProjectionTime();
  }



  @Override
  public long getQNTotalExecutionTime() {
    return bridge.getQNTotalExecutionTime();
  }
  @Override
  public long getDNTotalExecutionTime() {
    return bridge.getDNTotalExecutionTime();
  }
  



  @Override
  public long getQNRowsModificationTime() {
    return bridge.getQNRowsModificationTime();
  }
  @Override
  public long getDNRowsModificationTime() {
    return bridge.getDNRowsModificationTime();
  }

  
  //only for Query node  
  @Override
  public long getQNNumRowsSeen(){
    return bridge.getQNNumRowsSeenId();
  }
  
  @Override
  public long getQNMsgSendTime(){
    return bridge.getQNMsgSendTime();
  }
  
  @Override
  public long getQNMsgSerTime(){
    return bridge.getQNMsgSerTime();
    
  }
  
  @Override
  public long getQNRespDeSerTime(){
    return bridge.getQNRespDeSerTime();
  }
  
  
  //Only for data nodes
  @Override
  public long  getNumProjectedRows() {
   return bridge.getDNNumProjectedRows();
  }



  @Override
  public long getNumNLJoinRowsReturned() {
    return bridge.getDNNumNLJoinRowsReturned();
  }



  @Override
  public long getNumHASHJoinRowsReturned() {
    return bridge.getDNNumHASHJoinRowsReturned();
  }



  @Override
  public long getNumTableRowsScanned() {
    return bridge.getDNNumTableRowsScanned();
  }



  @Override
  public long  getNumRowsHashScanned() {
    return bridge.getDNNumRowsHashScanned();
  }



  @Override
  public long  getNumIndexRowsScanned() {
    return bridge.getDNNumIndexRowsScanned();
  }



  @Override
  public long getNumRowsSorted() {
    return bridge.getDNNumRowsSorted();
  }



  @Override
  public long  getNumSortRowsOverflowed() {
    return bridge.getDNNumSortRowsOverflowed();
  }



  @Override
  public long  getNumSingleHopExecutions() {
    return bridge.getDNNumSingleHopExecutions();
  }



  @Override
  public long  getSubQueryNumRowsSeen() {
    return bridge.getDNSubQueryNumRowsSeen();
  }



  @Override
  public long getMsgDeSerTime() {
    return bridge.getDNMsgDeSerTime();
  }



  @Override
  public long getMsgProcessTime() {
    return bridge.getDNMsgProcessTime();
  }



  @Override
  public long getResultIterationTime() {
    return bridge.getDNResultIterationTime();
  }



  @Override
  public long getRespSerTime() {
    return bridge.getDNRespSerTime();
  }



  @Override
  public long getThrottleTime() {
    return bridge.getDNThrottleTime();
  }



  @Override
  public long getNLJoinTime() {
    return bridge.getDNNLJoinTime();
  }



  @Override
  public long getHASHJoinTime() {
    return bridge.getDNHASHJoinTime();
  }



  @Override
  public long getTableScanTime() {
   return bridge.getDNTableScanTime();
  }



  @Override
  public long  getHashScanTime() {
    return bridge.getDNHashScanTime();
  }



  @Override
  public long getIndexScanTime() {
    return bridge.getDNIndexScanTime();
  }



  @Override
  public long getSortTime() {
    return bridge.getDNSortTime();
  }



  @Override
  public long getSubQueryExecutionTime() {
    return bridge.getDNSubQueryExecutionTime();
  }



  @Override
  public String retrievString() {
    return bridge.getString();
  }



  /**
   * @return the refCount
   */
  public int getRefCount() {
    return this.refCount.get();
  }

  public int decRefCount() {
    return this.refCount.decrementAndGet();
  }

}