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

import com.pivotal.gemfirexd.internal.engine.management.AggregateStatementMXBean;

/**
 * 
 * @author rishim
 *
 */
public class AggregateStatementMBean implements AggregateStatementMXBean {
  
  
  private AggregateStatementMBeanBridge bridge;
  
  public AggregateStatementMBean(AggregateStatementMBeanBridge bridge){
    this.bridge = bridge;
  }

  @Override
  public long getBindTime() {
    return bridge.getBindTime();
  }

  @Override
  public long getExecutionTime() {
    return bridge.getExecutionTime();
  }

  @Override
  public long getGenerateTime() {
    return bridge.getGenerateTime();
  }

  @Override
  public long getNumExecution() {
    return bridge.getNumExecution();
  }

  @Override
  public long getNumExecutionsInProgress() {
    return bridge.getNumExecutionsInProgress();
  }

  @Override
  public long getNumRowsModified() {
    return bridge.getNumRowsModified();
  }

  @Override
  public long getNumTimesCompiled() {
    return bridge.getNumTimesCompiled();
  }

  @Override
  public long getNumTimesGlobalIndexLookup() {
    return bridge.getNumTimesGlobalIndexLookup();
  }

  @Override
  public long getOptimizeTime() {
    return bridge.getOptimizeTime();
  }

  @Override
  public long getParseTime() {
    return bridge.getParseTime();
  }

  @Override
  public long getProjectionTime() {
    return bridge.getProjectionTime();
  }

  @Override
  public long getQNMsgSendTime() {
    return bridge.getQNMsgSendTime();
  }

  @Override
  public long getQNMsgSerTime() {
    return bridge.getQNMsgSerTime();
  }

  @Override
  public long getQNNumRowsSeen() {
    return bridge.getQNNumRowsSeen();
  }

  @Override
  public long getRoutingInfoTime() {
    return bridge.getRoutingInfoTime();
  }

  @Override
  public long getRowsModificationTime() {
    return bridge.getRowsModificationTime();
  }

  @Override
  public long getTotalCompilationTime() {
    return bridge.getTotalCompilationTime();
  }

  @Override
  public long getTotalExecutionTime() {
    return bridge.getTotalExecutionTime();
  }
  
  @Override
  public long getQNRespDeSerTime() {
    return bridge.getQNRespDeSerTime();
  }

  public AggregateStatementMBeanBridge getBridge(){
    return this.bridge;
  }


}
