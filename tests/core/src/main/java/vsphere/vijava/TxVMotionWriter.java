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
package vsphere.vijava;

import tx.TxBB;
import tx.TxWriter;
import hydra.Log;

import com.gemstone.gemfire.cache.TransactionEvent;
import com.gemstone.gemfire.cache.TransactionWriterException;

public class TxVMotionWriter extends TxWriter {

  @Override
  public void beforeCommit(TransactionEvent event)
      throws TransactionWriterException {
    super.beforeCommit(event);

    Boolean bool = (Boolean)TxBB.getBB().getSharedMap()
        .get(TxBB.VMOTION_TRIGGERED);
    if (bool == null || !bool) {
      try {
        TxBB.getBB().getSharedMap().put(TxBB.VMOTION_TRIGGERED, Boolean.TRUE);
        TxBB.getBB().getSharedMap().put(TxBB.VMOTION_TRIGGERED_TIME, System.currentTimeMillis());
        Log.getLogWriter().info("Migrating a VM in beforeCommit()");
        VIJavaUtil.doMigrateVM();
      } catch (Exception e) {
        Log.getLogWriter().info("Failed migrating a VM in beforeCommit()", e);
      }
    } else {
      long lastTrigger = (Long)TxBB.getBB().getSharedMap()
          .get(TxBB.VMOTION_TRIGGERED_TIME);
      if ((System.currentTimeMillis() - lastTrigger) > (5 * 60 * 1000)) {
        // It's been 5 min since last vMotion occurred, trigger it again
        try {
          TxBB.getBB().getSharedMap()
              .put(TxBB.VMOTION_TRIGGERED_TIME, System.currentTimeMillis());
          Log.getLogWriter().info("Migrating a VM in beforeCommit()");
          VIJavaUtil.doMigrateVM();
        } catch (Exception e) {
          Log.getLogWriter().info("Failed migrating a VM in beforeCommit()", e);
        }
      }
    }

  }

}
