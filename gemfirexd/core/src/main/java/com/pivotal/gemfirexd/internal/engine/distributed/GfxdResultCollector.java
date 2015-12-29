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

import java.util.Collection;
import java.util.Set;

import com.gemstone.gemfire.cache.execute.ResultCollector;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.execute.LocalResultCollector;
import com.pivotal.gemfirexd.internal.engine.access.GemFireTransaction;
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;

/**
 * {@link ResultCollector} interface used by GemFireXD that adds a couple of
 * methods to get and set the {@link GfxdReplyMessageProcessor}. This avoids
 * GCing of the processor for the case of client side streaming of results.
 * Currently the above functionality is provided by {@link LocalResultCollector}
 * but the interface has been retained for any GemFireXD specific enhancements.
 * 
 * @author swale
 */
public interface GfxdResultCollector<T> extends LocalResultCollector<Object, T> {

  /**
   * Provide a set to be populated with members that had non-fatal execution of
   * the function.
   */
  public void setResultMembers(Set<DistributedMember> members);

  /**
   * Return the members that had non-fatal execution of the function as set by
   * previous invocation of {@link #setResultMembers(Set)}.
   */
  public Set<DistributedMember> getResultMembers();

  /**
   * The list of containers to be closed, if any, once all the results have been
   * received.
   * 
   * @return true if the ResultCollector will handle closing of containers in
   *         its {@link #endResults()} and false otherwise.
   */
  public boolean setupContainersToClose(
      Collection<GemFireContainer> containers, GemFireTransaction tran)
      throws StandardException;

  /**
   * Set the total number of recipients for this message.
   */
  public void setNumRecipients(int n);

  /**
   * If this is a streaming ResultCollector then return its
   * {@link GfxdResultCollectorHelper} else return null.
   */
  public GfxdResultCollectorHelper getStreamingHelper();
  /**
   * Would be mainly used in retries
   */
  public GfxdResultCollector<T> cloneCollector();
}
