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
package sql.joinStatements;

import hydra.RemoteTestModule;
import hydra.TestConfig;
import hydra.blackboard.SharedMap;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.util.Random;

import sql.SQLPrms;
import sql.SQLTest;
import sql.SQLBB;
import sql.dmlStatements.AbstractDMLStmt;

/**
 * @author eshu
 *
 */
public abstract class AbstractJoinStmt implements JoinTableStmtIF {

  public static final Random rand = SQLTest.random;
  protected static boolean testUniqueKeys = SQLTest.testUniqueKeys;
  public static boolean testServerGroup = TestConfig.tab().booleanAt(SQLPrms.testServerGroups, false);
  public static SharedMap map = SQLBB.getBB().getSharedMap(); 
  protected static int maxNumOfTries = 2;
  protected static boolean isHATest = TestConfig.tab().longAt(util.StopStartPrms.numVMsToStop, 0) > 0? true: false;
  protected static String tradeSchemaSG = SQLTest.tradeSchemaSG;
  /* (non-Javadoc)
   * @see sql.joinStatements.JoinTableStmtIF#query(java.sql.Connection, java.sql.Connection)
   */
  public abstract void query(Connection dConn, Connection gConn);

  /**
   * to get my ThreadID used for update and delete record, so only particular thread can
   * update or delete a record it has inserted.
   * @return The threadId of the current hydra Thread.
   */
  protected int getMyTid() {
    int myTid = RemoteTestModule.getCurrentThread().getThreadId();
    return myTid;
  }
  
  /**
   * randomly generated a string of char (from 'a' to 'z' ) 
   * @param length -- the max number of characters in the string
   * @return a string of characters (from 'a' to 'z' )
   */
  protected String getRandVarChar(int length) {
    int aVal = 'a';
    int symbolLength = rand.nextInt(length) + 1;
    char[] charArray = new char[symbolLength];
    for (int j = 0; j<symbolLength; j++) {
      charArray[j] = (char) (rand.nextInt(26) + aVal); //one of the char in a-z
    }
    return new String(charArray);
  }
  
  //get a price between .01 to 100.00
  protected BigDecimal getPrice() {
    return new BigDecimal (Double.toString((rand.nextInt(10000)+1) * .01));
    //return new BigDecimal (((rand.nextInt(10000)+1) * .01));  //to reproduce bug 39418
  } 
  
  protected Date getSince() {
  	return AbstractDMLStmt.getSince();
  }
  
  protected int getSid() {
    return rand.nextInt((int) SQLBB.getBB().getSharedCounters().read(SQLBB.tradeSecuritiesPrimary))+1;
  }
}
