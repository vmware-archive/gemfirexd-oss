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
package com.gemstone.gemfire.internal.tools.gfsh.aggregator;

import java.util.Properties;

//import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.internal.InternalEntity;
import com.gemstone.gemfire.internal.tools.gfsh.aggregator.AggregateFunction;

/**
 * The Aggregator partition function. Used internally.
 * 
 * @author dpark
 */
public class AggregatorPartitionFunction implements /*Declarable, */Function, InternalEntity {
  private static final long serialVersionUID = 1L;

  public final static String ID = "__gfsh_aggregator";

  public AggregatorPartitionFunction() {
  }

  public String getId() {
    return ID;
  }

  public void execute(FunctionContext context) {
    AggregateFunction aggregator = (AggregateFunction) context.getArguments();
    context.getResultSender().lastResult(aggregator.run(context));
  }

  public void init(Properties p) {
  }

  public boolean hasResult() {
    return true;
  }

  public boolean optimizeForWrite() {
    return true;
  }

  public boolean isHA() {
    return false;
  }
}
