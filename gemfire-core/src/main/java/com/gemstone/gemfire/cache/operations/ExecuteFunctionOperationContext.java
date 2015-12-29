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
package com.gemstone.gemfire.cache.operations;

import java.io.Serializable;
import java.util.Set;

/**
 * OperationContext for Function execution operation. This is for the pre-operation case
 * 
 * @author Yogesh Mahajan
 * @since 6.0
 *
 */
public class ExecuteFunctionOperationContext extends OperationContext {

  private String functionId;

  private String regionName;

  private boolean optizeForWrite;

  private boolean isPostOperation;
  
  private Set keySet;

  private Object result;

  public ExecuteFunctionOperationContext(String functionName,
      String regionName, Set keySet, boolean optimizeForWrite,
      boolean isPostOperation) {
    this.functionId = functionName;
    this.regionName = regionName;
    this.keySet = keySet;
    this.optizeForWrite = optimizeForWrite;
    this.isPostOperation = isPostOperation;
  }

  @Override
  public OperationCode getOperationCode() {
    return OperationCode.EXECUTE_FUNCTION;
  }

  @Override
  public boolean isPostOperation() {
    return this.isPostOperation;
  }

  public String getFunctionId() {
    return this.functionId;
  }

  public String getRegionName() {
    return this.regionName;
  }

  public boolean isOptimizeForWrite() {
    return this.optizeForWrite;
  }
  
  public Object getResult() {
    return this.result;
  }

  public Set getKeySet() {
    return this.keySet;
  }

  public void setResult(Object oneResult) {
    this.result = oneResult;
  }
  
  public void setIsPostOperation(boolean isPostOperation) {
    this.isPostOperation = isPostOperation;
  }
}
