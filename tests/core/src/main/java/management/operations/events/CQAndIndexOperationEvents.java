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
package management.operations.events;

public interface CQAndIndexOperationEvents {
  
  /* CQ and Index Operations Events */
  public void cqCreated(String name, String query, String listeners);

  public void cqStopped(String name, String query);

  public void indexCreated(String name, String expression, String fromClause);

  public void indexRemoved(String name);

  public static String EVENT_CQ_CREATED = "EVENT_CQ_CREATED";
  public static String EVENT_CQ_STOP = "EVENT_CQ_STOP";
  public static String EVENT_INDEX_CREATED = "EVENT_INDEX_CREATED";
  public static String EVENT_INDEX_REMOVED = "EVENT_INDEX_REMOVED";

}
