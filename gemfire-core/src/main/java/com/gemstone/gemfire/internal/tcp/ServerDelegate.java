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
package com.gemstone.gemfire.internal.tcp;

import com.gemstone.gemfire.distributed.internal.DistributionMessage;
import com.gemstone.gemfire.distributed.internal.membership.*;
import com.gemstone.gemfire.i18n.LogWriterI18n;


/** <p>ServerDelegate is a conduit plugin that receives
    {@link com.gemstone.gemfire.distributed.internal.DistributionMessage}
    objects received from other conduits.</p>

    @see com.gemstone.gemfire.distributed.internal.direct.DirectChannel

    @author Bruce Schuchardt
    @since 2.0
   
  */
public interface ServerDelegate {

  public void receive( DistributionMessage message, int bytesRead,
                       Stub connId );

  public LogWriterI18n getLogger();

  /**
   * Called when a possibly new member is detected by receiving a direct channel
   * message from him.
   */
  public void newMemberConnected(InternalDistributedMember member, Stub id);
}
