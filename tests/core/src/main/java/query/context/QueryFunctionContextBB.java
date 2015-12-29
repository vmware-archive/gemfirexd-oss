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

package query.context;

// import util.*;
// import hydra.*;

import query.index.IndexValidator;
import hydra.blackboard.Blackboard;

// import com.gemstone.gemfire.cache.*;

public class QueryFunctionContextBB extends Blackboard {

  // Blackboard creation variables
  static String QUERY_BB_NAME = "QueryFunctionContext_Blackboard";

  static String QUERY_BB_TYPE = "RMI";

  // singleton instance of blackboard
  static public QueryFunctionContextBB bbInstance = null;

  public static int NUM_NODES;

  /**
   * Get the QueryFunctionContextBB
   */
  public static QueryFunctionContextBB getBB() {
    if (bbInstance == null) {
      synchronized (QueryFunctionContextBB.class) {
        if (bbInstance == null)
          bbInstance = new QueryFunctionContextBB(QUERY_BB_NAME, QUERY_BB_TYPE);
      }
    }
    return bbInstance;
  }

  /**
   * Initialize the QueryFunctionContextBB This saves caching attributes in the
   * blackboard that must only be read once per test run.
   */
  public static void HydraTask_initialize() {
    hydra.blackboard.SharedMap aMap = getBB().getSharedMap();
  }

  // Zero-arg constructor for remote method invocations.
  public QueryFunctionContextBB() {
  }

  /**
   * Creates a sample blackboard using the specified name and transport type.
   */
  public QueryFunctionContextBB(String name, String type) {
    super(name, type, QueryFunctionContextBB.class);
  }

}
