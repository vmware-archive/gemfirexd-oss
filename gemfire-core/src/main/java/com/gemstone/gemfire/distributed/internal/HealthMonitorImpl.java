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

import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.admin.GemFireHealth;
import com.gemstone.gemfire.admin.GemFireHealthConfig;
import com.gemstone.gemfire.admin.internal.GemFireHealthEvaluator;
import com.gemstone.gemfire.internal.LogWriterImpl;
import com.gemstone.gemfire.internal.admin.remote.*;
import com.gemstone.gemfire.distributed.internal.membership.*;

/**
 * Implements a thread that monitors the health of the vm it lives in.
 * @author Darrel Schneider
 * @since 3.5
 */
public class HealthMonitorImpl implements HealthMonitor, Runnable {
  private final InternalDistributedMember owner;
  private final int id;
  private final DistributionManager dm;
  private final GemFireHealthEvaluator eval;

  /** The current health status
   *
   * @see GemFireHealth#OKAY_HEALTH */ 
  private GemFireHealth.Health currentStatus;
  private final Thread t;
  private volatile boolean stopRequested = false;

  private static int idCtr = 0;
  
  /********** Constructors *********/
  /**
   * Creates a health monitor given its owner, configuration, and its dm
   */
  public HealthMonitorImpl(InternalDistributedMember owner,
                           GemFireHealthConfig config,
                           DistributionManager dm) {
    this.owner = owner;
    this.id = getNewId();
    this.dm = dm;
    this.eval = new GemFireHealthEvaluator(config, dm);
    this.currentStatus = GemFireHealth.GOOD_HEALTH;
    ThreadGroup tg = LogWriterImpl.createThreadGroup("HealthMonitor Threads", dm.getLoggerI18n());
    this.t = new Thread(tg, this, LocalizedStrings.HealthMonitorImpl_HEALTH_MONITOR_OWNED_BY_0.toLocalizedString(owner));
    this.t.setDaemon(true);
  }
  
  /************** HealthMonitor interface implementation ******************/
  public int getId() {
    return this.id;
  }
  public void resetStatus() {
    this.currentStatus = GemFireHealth.GOOD_HEALTH;
    this.eval.reset();
  }
  public String[] getDiagnosis(GemFireHealth.Health healthCode) {
    return this.eval.getDiagnosis(healthCode);
  }
  public void stop() {
    if (this.t.isAlive()) {
      this.stopRequested = true;
      this.t.interrupt();
    }
  }

  /********** HealthMonitorImpl public methods **********/
  /**
   * Starts the monitor so that it will periodically do health checks. 
   */
  public void start() {
    if (this.stopRequested) {
      throw new RuntimeException(LocalizedStrings.HealthMonitorImpl_A_HEALTH_MONITOR_CAN_NOT_BE_STARTED_ONCE_IT_HAS_BEEN_STOPPED.toLocalizedString());
    }
    if (this.t.isAlive()) {
      // it is already running
      return;
    }
    this.t.start();
  }

  /********** Runnable interface implementation **********/

  public void run() {
    final int sleepTime = this.eval.getEvaluationInterval() * 1000;
    if (this.dm.getLoggerI18n().fineEnabled()) {
      String s = LocalizedStrings.HealthMonitorImpl_STARTING_HEALTH_MONITOR_HEALTH_WILL_BE_EVALUATED_EVERY_0_SECONDS.toLocalizedString(Integer.valueOf((sleepTime/1000)));
      this.dm.getLoggerI18n().fine(s);
    }
    try {
      while (!this.stopRequested) {
//        SystemFailure.checkFailure(); dm's stopper will do this
        this.dm.getCancelCriterion().checkCancelInProgress(null);
        Thread.sleep(sleepTime);
        if (!this.stopRequested) {
          GemFireHealth.Health newStatus = this.eval.evaluate();
          if (newStatus != this.currentStatus) {
            this.currentStatus = newStatus;
            HealthListenerMessage msg = HealthListenerMessage.create(getId(), newStatus);
            msg.setRecipient(this.owner);
            this.dm.putOutgoing(msg);
          }
        }
      }

    } catch (InterruptedException ex) {
      // No need to reset interrupt bit, we're exiting.
      if (!this.stopRequested) {
        this.dm.getLoggerI18n().warning(LocalizedStrings.HealthMonitorImpl_UNEXPECTED_STOP_OF_HEALTH_MONITOR, ex);
      }
    } finally {
      this.eval.close();
      this.stopRequested = true;
      if (this.dm.getLoggerI18n().fineEnabled()) {
        this.dm.getLoggerI18n().fine("Stopping health monitor");
      }
    }
  }
  
  /********** Internal implementation **********/

  private static synchronized int getNewId() {
    idCtr += 1;
    return idCtr;
  }

}
