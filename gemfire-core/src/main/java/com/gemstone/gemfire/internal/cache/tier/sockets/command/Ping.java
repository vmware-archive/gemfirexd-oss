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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.sockets.*;
import com.gemstone.gemfire.distributed.internal.DistributionStats;

import java.io.IOException;

public class Ping extends BaseCommand {

  private final static Ping singleton = new Ping();

  public static Command getCommand() {
    return singleton;
  }

  private Ping() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException {
    if (logger.fineEnabled()) {
      logger.fine(servConn.getName() + ": rcv tx: " + msg.getTransactionId()
          + " from " + servConn.getSocketString() + " rcvTime: "
          + (DistributionStats.getStatTime() - start));
    }
    ClientHealthMonitor chm = ClientHealthMonitor.getInstance();
    if (chm != null)
      chm.receivedPing(servConn.getProxyID());
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    if (crHelper.emulateSlowServer() > 0) {
      // this.logger.fine("SlowServer", new Exception());
      boolean interrupted = Thread.interrupted();
      try {
        Thread.sleep(crHelper.emulateSlowServer());
      }
      catch (InterruptedException ugh) {
        interrupted = true;
        servConn.getCachedRegionHelper().getCache().getCancelCriterion()
            .checkCancelInProgress(ugh);
      }
      finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
    writeReply(msg, servConn);
    servConn.setAsTrue(RESPONDED);
    if (logger.fineEnabled()) {
      logger.fine(servConn.getName() + ": Sent ping reply to "
          + servConn.getSocketString());
    }
  }

}
