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
package com.gemstone.gemfire.internal.cache.execute;

import java.util.HashMap;
import java.util.Set;

import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.cache.DistributedRegion;
import com.gemstone.gemfire.internal.cache.DistributedRegionFunctionStreamingMessage;
/**
 * 
 * @author ymahajan
 *
 */
public class DistributedRegionFunctionResultWaiter extends
    StreamingFunctionOperation {

  private final Set filter;

  private final DistributedRegion region;

  public DistributedRegionFunctionResultWaiter(InternalDistributedSystem sys,
      DistributedRegion region, ResultCollector rc, final Function function,
      final Set filter, Set recipients,
      final HashMap<InternalDistributedMember, Object> memberArgs,
      ResultSender resultSender) {
    super(sys, rc, function, memberArgs, recipients,resultSender);
    this.region = region;
    this.filter = filter;
  }

  @Override
  protected DistributionMessage createRequestMessage(Set recipients,
      FunctionStreamingResultCollector processor, boolean isReExecute,
      boolean isFnSerializationReqd) {
    DistributedRegionFunctionStreamingMessage msg =
      new DistributedRegionFunctionStreamingMessage(
        this.region.getFullPath(), this.functionObject,
        processor.getProcessorId(), this.filter, this.memberArgs.get(recipients
            .toArray()[0]), isReExecute, isFnSerializationReqd,
        this.region.getTXState());
    msg.setRecipients(recipients);
    return msg;
  }
}
