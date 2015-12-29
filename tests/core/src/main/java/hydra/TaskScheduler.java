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

package hydra;

import java.util.*;

/** 
 *  Interface for task schedulers.
 */

public interface TaskScheduler { 

  /**
   *  Asks the scheduler to create vm and client records based on the client
   *  descriptions and map clients to threadgroups.
   *  @param cds the ClientDescription describes the client vms and threads
   *  @param tgs the thread groups to map clients to.
   */
  public void createClients( Map cds, Map tgs );

  /** 
   *  Asks the scheduler to start clients previously created but not running.
   *  @throws HydraInternalException if there are running clients
   *  @throws HydraTimeoutException if the clients do not start within
   *                                {@link Prms#maxClientStartupWaitSec}
   */
  public void startClients();

  /**
   *  Asks the scheduler to start clients previously created.  Does not
   *  complain if there are running clients, but does remove them from the
   *  active list.
   *  @throws HydraTimeoutException if the clients do not start within
   *                                {@link Prms#maxClientStartupWaitSec}
   */
  public void startClientsForced();

  /**
   *  Asks the scheduler to stop the clients, rather than leaving them running
   *  for another scheduler to use later.
   *  @throws HydraTimeoutException if the clients do not stop within
   *                                {@link Prms#maxClientShutdownWaitSec}
   */
  public void stopClients();

  /**
   *  Asks the scheduler to execute its tasks using the existing clients.
   *  @param haltIfBadResult whether to stop on a fatal task error.
   *  @param maxResultWaitSec the maximum amount of time to wait for any one
   *                          task to complete.
   *  @return true if all tasks completed within the time limit and without
   *               error.
   *  @throws HydraInternalException if there are tasks, but no existing clients
   */
  public boolean executeTasks( boolean haltIfBadResult, long maxResultWaitSec );

  /**
   *  Asks the scheduler to log the result of executing its tasks.
   */
  public void printReport();
}
