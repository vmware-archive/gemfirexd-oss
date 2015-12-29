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
package sql.sqlTx;

import hydra.blackboard.Blackboard;

public class SQLTxBatchingNonDeleteHoldKeysBB extends Blackboard {
//Blackboard creation variables
  static String SQL_BB_NAME = "SQLTxBatchingNonDeleteHoldKeysBB_Blackboard";
  static String SQL_BB_TYPE = "RMI";
  
  public static SQLTxBatchingNonDeleteHoldKeysBB bbInstance = null;
  
  public static synchronized SQLTxBatchingNonDeleteHoldKeysBB getBB() {
    if (bbInstance == null) {
        bbInstance = new SQLTxBatchingNonDeleteHoldKeysBB(SQL_BB_NAME, SQL_BB_TYPE);
    }      
    return bbInstance;  
  }
  
  public SQLTxBatchingNonDeleteHoldKeysBB() {
    
  }
  
  public SQLTxBatchingNonDeleteHoldKeysBB(String name, String type) {
    super(name, type, SQLTxBatchingNonDeleteHoldKeysBB.class);
  }

}
