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
package sql.wan;

import hydra.blackboard.Blackboard;

public class SQLWanBB extends Blackboard {
//Blackboard creation variables
  static String SQL_WAN_BB_NAME = "SQLWANBB_Blackboard";
  static String SQL_WAN_BB_TYPE = "RMI";
  
  public static SQLWanBB bbInstance = null;
  
  public static synchronized SQLWanBB getBB() {
    if (bbInstance == null) {
        bbInstance = new SQLWanBB(SQL_WAN_BB_NAME, SQL_WAN_BB_TYPE);
    }      
    return bbInstance;  
  }
  
  public SQLWanBB() {
    
  }
  
  public SQLWanBB(String name, String type) {
    super(name, type, SQLWanBB.class);
  }
  
  //index of 0
  public static int synchWanEndPorts; 
  
  //currently plan to configure 5 wan sites
  //index of 1 to 5
  public static int stopStartVmsInWanSite1;
  public static int stopStartVmsInWanSite2;
  public static int stopStartVmsInWanSite3;
  public static int stopStartVmsInWanSite4;
  public static int stopStartVmsInWanSite5;

  
  public static int synchWanSiteParitionKeys;
  public static int synchWanSiteSchemas;

}
