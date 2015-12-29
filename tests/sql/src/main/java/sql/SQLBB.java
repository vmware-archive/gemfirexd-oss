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
 * 
 */
package sql;

import hydra.blackboard.Blackboard;

/**
 * @author eshu
 *
 */
public class SQLBB extends Blackboard {
//Blackboard creation variables
  static String SQL_BB_NAME = "SQLBB_Blackboard";
  static String SQL_BB_TYPE = "RMI";
  
  public static SQLBB bbInstance = null;
  
  public static synchronized SQLBB getBB() {
    if (bbInstance == null) {
        bbInstance = new SQLBB(SQL_BB_NAME, SQL_BB_TYPE);
    }      
    return bbInstance;  
  }
  
  public SQLBB() {
    
  }
  
  public SQLBB(String name, String type) {
    super(name, type, SQLBB.class);
  }
  
  public static int syncBootProcess; //temperally work around of bug 39239
  
  public static int firstToCreateDerby; //used to allow only one thread to create derby database
  
  //primary key count for each table
  public static int tradeCustomersPrimary;
  public static int tradeSecuritiesPrimary;
  public static int tradeSellOrdersPrimary;
  public static int tradeBuyOrdersPrimary;
  public static int indexCount;
  public static int dataStoreCount;
  public static int show_customers;
  public static int addInterest;
  public static int testInOutParam;
  public static int stopStartVms;
  public static int numOfPRs;  
  public static int calculatedNumOfPPs;
  public static int createTableDone;
  public static int triggerInvocationCounter;
  public static int populatePortfolioUsingFunction;
  public static int wanDerbyDDLThread;
  public static int empEmployeesPrimary;
  public static int initServerPort;
  public static int asynchDBTargetVm;
  public static int defaultEmployeesPrimary;
  public static int populateThruLoader;
  public static int firstInRound;
  public static int perfLimitedIndexDDL; // used to track whether concurrent index op can be performed
  public static int perfLimitedProcDDL; //used to track whether concurrent procedure op can be perfromed.
  public static int addNewDataNodes;   
  public static int testLevelConfiguration= 0;
  
}
