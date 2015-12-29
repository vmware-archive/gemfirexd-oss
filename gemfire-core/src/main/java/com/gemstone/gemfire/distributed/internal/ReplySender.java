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

import java.util.Set;

import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.DirectReplyMessage;

/**
 * This interface is used by direct ack messages to send a reply
 * to the original sender of the message. Any message which implements
 * {@link DirectReplyMessage} must reply by calling putOutgoing on the
 * ReplySender returned by {@link DistributionMessage#getReplySender(DM)}
 * 
 * The reply sender may be the distribution manager itself, or it may send
 * the reply directly back on the same socket the message as received on.
 * @author dsmith
 *
 */
public interface ReplySender {
  
  public Set putOutgoing(DistributionMessage msg);

  public LogWriterI18n getLoggerI18n();

}
