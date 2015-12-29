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

package cacheperf.poc.useCase3_2;

import hydra.blackboard.*;

/**
 *  Manages the blackboard used to synchronize client threads for a task.
 *  Defines counters used to synchronize clients executing particular tasks.
 *  Also provides a signal that can be used by a clients to signal others.
 */
public class TaskSyncBlackboard extends Blackboard {

  public static final String RECOVERY_KEY = "recovery";

  public static int createDataTask;
  public static int putDataTask;
  public static int signal;

  private static TaskSyncBlackboard blackboard;

  /**
   *  Zero-arg constructor for remote method invocations.
   */
  public TaskSyncBlackboard() {
  }
  /**
   *  Creates a blackboard using the specified name and transport type.
   */
  public TaskSyncBlackboard( String name, String type ) {
    super( name, type, TaskSyncBlackboard.class );
  }
  /**
   *  Creates a blackboard.
   */
  public static synchronized TaskSyncBlackboard getInstance() {
    if (blackboard == null) {
      blackboard = new TaskSyncBlackboard("TaskSyncBlackboard", "rmi");
    }
    return blackboard;
  }
}
