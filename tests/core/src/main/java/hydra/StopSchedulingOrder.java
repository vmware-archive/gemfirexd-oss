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
 *  Throw this exception from a hydra task of type "TASK" to stop the master
 *  from scheduling any more TASK work on any clients.
 *  <p>
 *  When a client throws this, master will quit scheduling tasks and wait for
 *  currently executing tasks to finish, then go on to do any closetasks and
 *  endtasks.  No error is reported.
 *  <p>
 *  For example, suppose a test wants to run until it has put 2GB of data into
 *  a region, but the data used in the test is random and its size is hard
 *  to pin down.  The test can monitor the region and throw this exception
 *  when the condition is reached.  Task granularity can be finer, with a small
 *  maxResultWaitSec, and totalTaskTimeSec can be very large to ensure that the
 *  test doesn't terminate before the 2GB goal is reached.
 */

public class StopSchedulingOrder extends SchedulingOrder {

    public StopSchedulingOrder() {
        super();
    }
    public StopSchedulingOrder(String s) {
        super(s);
    }
}
