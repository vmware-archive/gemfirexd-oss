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

package com.pivotal.gemfirexd.internal.engine.distributed;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * A dummy ResultCollector that will ignore all results and only used to signal
 * that results from all nodes have been received.
 * 
 * Since this ResultCollector has no state, use the
 * {@link AckResultCollector#INSTANCE} to get the instance to be used.
 * 
 * @author swale
 * @since 7.0
 */
public class AckResultCollector implements ResultCollector<Object, Object> {

  public static final AckResultCollector INSTANCE = new AckResultCollector();

  /** use singleton instance */
  private AckResultCollector() {
  }

  @Override
  public Serializable getResult() throws FunctionException {
    return null;
  }

  @Override
  public Serializable getResult(long timeout, TimeUnit unit)
      throws FunctionException, InterruptedException {
    return null;
  }

  @Override
  public void addResult(DistributedMember memberID,
      Object resultOfSingleExecution) {
  }

  @Override
  public void clearResults() {
  }

  @Override
  public void endResults() {
  }
}
