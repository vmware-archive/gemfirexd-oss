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
/**
 *  Gfxd memscale blackboard
 */
package sql.memscale;

import hydra.blackboard.Blackboard;

public class SqlMemScaleBB extends Blackboard {

  //Blackboard creation variables
  static String SQL_BB_NAME = "SqlMemScale_Blackboard";
  static String SQL_BB_TYPE = "RMI";
  
  // shared counters
  public static int rowCounter;
  public static int leader;
  public static int pause1;
  public static int pause2;
  public static int pause3;
  public static int pause4;
  public static int pause5;
  public static int verifyOffHeapMemory;
  public static int executionNumber;
  public static int timeToStop;

  public static SqlMemScaleBB bbInstance = null;

  public static synchronized SqlMemScaleBB getBB() {
    if (bbInstance == null) {
      bbInstance = new SqlMemScaleBB(SQL_BB_NAME, SQL_BB_TYPE);
    }      
    return bbInstance;  
  }

  public SqlMemScaleBB() {

  }

  public SqlMemScaleBB(String name, String type) {
    super(name, type, SqlMemScaleBB.class);
  }

}
