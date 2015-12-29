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
package cacheRunner;

import com.gemstone.gemfire.cache.*;
import java.util.*;

/**
 * A GemFire <code>TransactionListener</code> that logs information
 * about the events it receives.
 *
 * @author GemStone Systems, Inc.
 * @since 4.0
 */
public class LoggingTransactionListener extends LoggingCacheCallback
  implements TransactionListener {

  /**
   * Zero-argument constructor required for declarative cache XML
   * file. 
   */
  public LoggingTransactionListener() {
    super();
  }

  //////////////////////  Instance Methods  //////////////////////

  /**
   * Logs information about a <code>TransactionEvent</code>
   *
   * @param kind
   *        The kind of event to be logged
   */
  protected void log(String kind, TransactionEvent event) {
    StringBuffer sb = new StringBuffer();
    sb.append(kind);
    sb.append(" in transaction ");
    sb.append(event.getTransactionId());
    sb.append("\n");

    List<CacheEvent<?, ?>> events = event.getEvents();
    sb.append(events.size());
    sb.append(" Events\n");
    for (CacheEvent<?, ?> entryEvent : events) {
    	sb.append(format((EntryEvent<?, ?>) entryEvent));
    }
    sb.append("\n");

    log(sb.toString(), event.getCache());
  }

  public void afterCommit(TransactionEvent event) {
    log("TransactionListener.afterCommit", event);
  }

  public void afterFailedCommit(TransactionEvent event) {
    log("TransactionListener.afterFailedCommit", event);
  }

  public void afterRollback(TransactionEvent event) {
    log("TransactionListener.afterRollback", event);
  }

}
