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
package com.gemstone.gemfire.cache.util;

import com.gemstone.gemfire.distributed.DistributedMember;

/**
 * An event delivered to a {@link BridgeMembershipListener} when this 
 * process detects connection changes to BridgeServers or bridge clients.
 *
 * @author Kirk Lund
 * @since 4.2.1
 */
public interface BridgeMembershipEvent {
 
  /**
   * Returns the the member that connected or disconnected.
   *
   * @see com.gemstone.gemfire.distributed.DistributedSystem#getDistributedMember
   */
  public DistributedMember getMember();

  /**
   * Returns the id of the member that connected or disconnected.
   *
   * @see com.gemstone.gemfire.distributed.DistributedSystem#getMemberId
   */
  public String getMemberId();
  
  /**
   * Returns true if the member is a bridge client to a BridgeServer hosted
   * by this process. Returns false if the member is a BridgeServer that this
   * process is connected to.
   */
  public boolean isClient();
  
}

