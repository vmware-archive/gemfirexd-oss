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

package com.gemstone.gemfire.internal.cache;

/**
 * Store the result of {@link VMIdAdvisor#newUUID(ClusterUUID)} to encapsulate a
 * unique ID across this distributed system as well as other distributed systems
 * across WAN to which this cluster is connected.
 * 
 * @author swale
 * @since gfxd 2.0
 */
public interface ClusterUUID {

  /**
   * get the distributed system wide unique ID (could be unique even across
   * other clusters to which this is connected on WAN or not) for this node
   */
  public long getMemberId();

  /**
   * Get the local generated unique ID of this node. Together with
   * {@link #getMemberId()} this provides a cluster and WAN wide unique UUID.
   */
  public int getUniqId();

  /**
   * Set the member ID and uniq ID returned by {@link #getMemberId()} and
   * {@link #getUniqId()} respectively. To be used only by UUID generators like
   * {@link VMIdAdvisor}.
   */
  public void setUUID(long memberId, int uniqId, VMIdAdvisor advisor);
}
