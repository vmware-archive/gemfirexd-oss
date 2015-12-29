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

/**
 *  Throw this exception from a client to stop the master from scheduling the
 *  task on the client henceforth.  No error is reported.
 *  <p>
 *  When a client throws this from a TASK, it can still be scheduled with other
 *  tasks for which it is eligible.
 *  <p>
 *  This exception can also be thrown from a batched INITTASK or CLOSETASK.  In
 *  this case, all eligible clients are scheduled repeatedly with the batched
 *  task until all clients executing it have thrown this exception.  Then the
 *  next task in the list is scheduled onto eligible clients.
 *  <p>
 *  For example, suppose a test wants to run a fixed workload on each client,
 *  say N iterations of an operation.  Each client throws this exception when it
 *  reaches N.  To avoid doing all N iterations in a single task, it does the
 *  work in batches, and uses a HydraThreadLocal to keep track of how many it's
 *  done.  That allows totalTaskTimeSec to be set arbitrarily large, but still
 *  keep maxResultWaitSec small to detect hangs early in long-running tests.
 *  If there are multiple workloads, be aware that a large totalTaskTimeSec
 *  means that termination must be achieved some other way, e.g., via additional
 *  stop scheduling orders or maxTimesToRun task attributes.
 */

public class StopSchedulingTaskOnClientOrder extends SchedulingOrder {

    public StopSchedulingTaskOnClientOrder() {
        super();
    }
    public StopSchedulingTaskOnClientOrder(String s) {
        super(s);
    }
}
