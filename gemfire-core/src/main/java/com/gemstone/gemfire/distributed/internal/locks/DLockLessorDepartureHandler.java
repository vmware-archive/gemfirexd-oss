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

package com.gemstone.gemfire.distributed.internal.locks;

import com.gemstone.gemfire.distributed.internal.locks.DLockGrantor.DLockGrantToken;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * Provides hook for handling departure of a lease holder (lessor).
 * <p>
 * Implementation is optional and will be called from 
 * <code>DLockGrantor.handleDepartureOf(Serializable)</code>
 *
 * @author Kirk Lund
 */
public interface DLockLessorDepartureHandler {

  public void handleDepartureOf(InternalDistributedMember owner,
      DLockGrantor grantor);

  /**
   * Callback to handle departure of a lease holder for each
   * {@link DLockGrantToken} held by that member or for lease timeout of a
   * token.
   * 
   * @since 7.0
   */
  public void handleDLockTokenRelease(InternalDistributedMember owner,
      DLockGrantor grantor, DLockGrantToken grantToken, ReleaseEvent event);

  /**
   * An event that lead to release of {@link DLockGrantToken} apart from normal
   * lock release by the lease holder.
   * 
   * @author swale
   * @since 7.0
   */
  public static enum ReleaseEvent {
    /**
     * DLock token being released due to departure of lock owner from
     * distributed system.
     */
    LESSOR_DEPARTURE,
    /**
     * DLock token being released due to crash of lock owner.
     */
    LESSOR_CRASH,
    /** DLock token being release due to lease expiration of the token */
    LEASE_TIMEOUT
  }
}
