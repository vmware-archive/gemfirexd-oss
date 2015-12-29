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

public class SQLWanSenderBB extends Blackboard {
//Blackboard creation variables
  static String SQL_WAN_BB_NAME = "SQLWANSenderBB_Blackboard";
  static String SQL_WAN_BB_TYPE = "RMI";
  
  public static SQLWanSenderBB bbInstance = null;
  
  public static synchronized SQLWanSenderBB getBB() {
    if (bbInstance == null) {
        bbInstance = new SQLWanSenderBB(SQL_WAN_BB_NAME, SQL_WAN_BB_TYPE);
    }      
    return bbInstance;  
  }
  
  public SQLWanSenderBB() {
    
  }
  
  public SQLWanSenderBB(String name, String type) {
    super(name, type, SQLWanSenderBB.class);
  }
  
  //index of 0
  public static int dummyIndex;  //not used
  
  //currently plan to configure 5 wan sites
  //index of 1 to 5
  public static int stopStartSenderInWanSite1;
  public static int stopStartSenderInWanSite2;
  public static int stopStartSenderInWanSite3;
  public static int stopStartSenderInWanSite4;
  public static int stopStartSenderInWanSite5;


}
