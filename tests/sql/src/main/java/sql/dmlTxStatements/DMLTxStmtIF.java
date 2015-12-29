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
package sql.dmlTxStatements;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author eshu
 *
 */
public interface DMLTxStmtIF {
  
  //first connection is to derby disc and second connection is to gfe db. It is possible for passing in 
  //only the gfe connection when concurrently performing dml statements without unique keys constraints
  //public void insertTx(Connection dConn, Connection gConn, int size);
  public void queryTx(Connection dConn, Connection gConn);
  
  //insert one record to both dbs if derby exists
  public void insertTx(Connection dConn, Connection gConn, 
  		HashMap<String, Integer> modifiedKeys, 
  		ArrayList<SQLException>dExList, ArrayList<SQLException> sExList);
  public void updateTx(Connection dConn, Connection gConn, 
   		HashMap<String, Integer> modifiedKeys, 
  		ArrayList<SQLException>dExList, ArrayList<SQLException> sExList);
  public void deleteTx(Connection dConn, Connection gConn, 
   		HashMap<String, Integer> modifiedKeys, 
  		ArrayList<SQLException>dExList, ArrayList<SQLException> sExList);
  
  //not a dmlStatment but used by test for populate table
  public void populateTx(Connection dConn, Connection gConn);
  //perform derby ops 
  public void performDerbyTxOps(Connection dConn, Object[]data, ArrayList<SQLException> dExList);
  
  public String getMyTableName();
}
