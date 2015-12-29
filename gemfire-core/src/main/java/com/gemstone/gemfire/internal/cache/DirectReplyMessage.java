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

import com.gemstone.gemfire.distributed.internal.DirectReplyProcessor;
import com.gemstone.gemfire.distributed.internal.DistributionMessage;

/**
 * A message that can reply directly to the sender
 * 
 * 
 * @author dsmith
 *
 */
public interface DirectReplyMessage {
  /**
   * Called on the sending side. This reply processor
   * will be handed the responses from the message.
   */
  DirectReplyProcessor getDirectReplyProcessor();
  
  /**
   * Indicates whether the message could send an acknowledgement
   * back on the connection the request was sent on.  This flag 
   * only takes effect when {@link com.gemstone.gemfire.distributed.DistributedSystem#setThreadsSocketPolicy(boolean)} 
   * is set to <code>false</code>
   * If this flag is set to true, the process method <b> must </b> reply
   * by calling {@link DistributionMessage#getReplySender(com.gemstone.gemfire.distributed.internal.DM)} and using
   * the result to send the reply. the ReplySender determines whether to reply
   * directly or through the shared channel.
   * @return true if a direct acknowledgement is allowed
   * @see com.gemstone.gemfire.distributed.internal.direct.DirectChannel
   */
  boolean supportsDirectAck();
  
  /**
   * Called on the sending side. This method is invoked
   * if the message will end up using the shared channel.
   * The message is expected to register the processor
   * and send it's id to the receiving side.
   * 
   */
  void registerProcessor();
}
