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

import com.gemstone.gemfire.cache.execute.ResultSender;

/**
 * 
 * @author ymahajan
 *
 */
public interface InternalResultSender extends ResultSender<Object> {

  public void enableOrderedResultStreaming(boolean enable);

  public boolean isLocallyExecuted();

  public boolean isLastResultReceived();
  
  public void setException(Throwable t);

  /**
   * Like {@link #lastResult(Object)} but allows for an additional TX related
   * arguments which will determine if the message end processing for
   * transaction should be performed or not (e.g. shipping back TX changes,
   * flushing TX batch etc).
   * 
   * Useful for messages which do processing in toData serialization (like GFXD
   * streaming messages) and thus determine the TX changes etc after the result
   * has been serialized for writing to wire.
   */
  public void lastResult(Object lastResult, boolean doTXFlush,
      boolean sendTXChanges, boolean finishTXRead);
}
