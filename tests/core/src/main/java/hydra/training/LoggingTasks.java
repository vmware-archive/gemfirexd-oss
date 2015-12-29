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
package hydra.training;

import hydra.Log;
import hydra.TestConfig;

/**
 * Contains simple Hydra test code that demonstrate the five kinds of
 * Hydra tasks (START, INIT, TASK, CLOSE, and END) and generates
 * various kinds of logging output.
 *
 * @author David Whitlock
 * @since 4.0
 */
public class LoggingTasks {

  /**
   * A START task that logs to standard out, standard error, and the
   * Hydra logger.
   */
  public static void loggingStartTask() {
    Log.getLogWriter().info("Begin logging START task");
    System.out.println("Standard out from START task in thread " +
                       Thread.currentThread().getName() + "\n\n");
    System.err.println("Standard err from START task\n\n");
    Log.getLogWriter().info("End logging START task");
  }

  /**
   * An INIT task that logs to standard out, standard error, and the
   * Hydra logger in each client VM thread.
   */
  public static void loggingInitTask() {
    Log.getLogWriter().info("Begin logging INIT task");
    System.out.println("Standard out from INIT task in thread " +
                       Thread.currentThread().getName() + "\n\n");
    System.err.println("Standard err from INIT task\n\n");
    Log.getLogWriter().info("End logging INIT task");
  }

  /**
   * A test TASK that logs to standard out, standard error, and the
   * Hydra logger in each client VM thread.  This method sleeps for
   * a random amount of time to simulate work being done.
   *
   * @throws InterruptedException
   *         If interrupted while "working".  Since we do not expect
   *         this method to be interrupted, we propagate the exception
   *         back to Hydra to indicate that an error occurred.
   */
  public static void loggingTestTask() throws InterruptedException {
    Log.getLogWriter().info("Begin logging test TASK");
    System.out.println("Standard out from test TASK in thread " +
                       Thread.currentThread().getName() + "\n\n");
    System.err.println("Standard err from test TASK\n\n");

    // Pretend to do some work
    long sleep =
      TestConfig.tab().getRandGen().nextInt(250, 750);
    Thread.sleep(500);

    Log.getLogWriter().info("End logging test task");
  }

  /**
   * A CLOSE task that logs to standard out, standard error, and the
   * Hydra logger in each client VM thread.
   */
  public static void loggingCloseTask() {
    Log.getLogWriter().info("Begin logging CLOSE task");
    System.out.println("Standard out from CLOSE task in thread " +
                       Thread.currentThread().getName() + "\n\n");
    System.err.println("Standard err from CLOSE task\n\n");
    Log.getLogWriter().info("End logging CLOSE task");
  }

  /**
   * A END task that logs to standard out, standard error, and the
   * Hydra logger.
   */
  public static void loggingEndTask() {
    Log.getLogWriter().info("Begin logging END task");
    System.out.println("Standard out from END task in thread " +
                       Thread.currentThread().getName() + "\n\n");
    System.err.println("Standard err from END task\n\n");
    Log.getLogWriter().info("End logging END task");
  }

}
