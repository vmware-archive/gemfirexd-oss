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

import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.internal.ReplyProcessor21;

/**
 * Internal interface for ResultCollectors used for streaming etc.
 */
public interface InternalResultCollector<T, S> extends ResultCollector<T, S> {

  /** keep a reference of processor, if required, to avoid it getting GCed */
  void setProcessor(ReplyProcessor21 processor);

  /**
   * get the {@link ReplyProcessor21}, if any, set in the collector by a
   * previous call to {@link #setProcessor(ReplyProcessor21)}
   */
  ReplyProcessor21 getProcessor();
}
