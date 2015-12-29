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

package com.gemstone.gemfire.distributed.internal;

/**
 * A PooledDistributionMessage passes processing off to a thread that
 * executes on the DistributionManager's getThreadPool().  This is
 * sufficient for messages that don't need to be processed serially
 * with respect to any other message.
 */
public abstract class PooledDistributionMessage extends DistributionMessage {

  @Override
  final public int getProcessorType() {
    return DistributionManager.STANDARD_EXECUTOR;
  }

}

