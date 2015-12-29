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

package cacheperf.gemfire;

import cacheperf.AbstractLatencyListener;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.util.GatewayEvent;
import com.gemstone.gemfire.cache.util.GatewayEventListener;
import java.util.Iterator;
import java.util.List;

/**
 * A listener that records message latency statistics on gateway events.
 */

public class GatewayLatencyListener extends AbstractLatencyListener
                                    implements GatewayEventListener {

  //----------------------------------------------------------------------------
  // Constructors
  //----------------------------------------------------------------------------

  /**
   * Creates a latency listener.
   */
  public GatewayLatencyListener() {
    super();
  }

  //----------------------------------------------------------------------------
  // GatewayEventListener API
  //----------------------------------------------------------------------------

  /**
   * Processes events by recording their latencies.
   */
  public boolean processEvents(List events) {
    for (Iterator i = events.iterator(); i.hasNext();) {
      GatewayEvent event = (GatewayEvent)i.next();
      Operation op = event.getOperation();
      if (op.isUpdate()) {
        if (!processEvent(event)) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Processes the event by recording the latency.  Returns false if there
   * is a problem.
   */
  private boolean processEvent(GatewayEvent event) {
    try {
      Object value = event.getDeserializedValue();
      recordLatency(value);
      return true;
    } catch (RuntimeException ex) {
      return false;
    }
  }

  public void close() {
  }
}
