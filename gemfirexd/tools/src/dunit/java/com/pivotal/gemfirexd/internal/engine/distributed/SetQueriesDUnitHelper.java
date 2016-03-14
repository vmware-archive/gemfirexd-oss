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
package com.pivotal.gemfirexd.internal.engine.distributed;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;
import org.apache.log4j.Logger;

public class SetQueriesDUnitHelper {
  static String schemaName = "setopqry";
  static int currentCount = 0;
  private static final int MAXQUERIES = 35;
  static final String[] queryStringsLeft = new String[MAXQUERIES];
  static final String[] queryStringsRight = new String[MAXQUERIES];
  
  /*
   * Handle scenarios where table have been created with primary keys
   */
  static final int[] unionResult_withPK = new int[MAXQUERIES];
  static final int[] unionAllResult_withPK = new int[MAXQUERIES];
  static final int[] intersectResult_withPK = new int[MAXQUERIES];
  static final int[] intersectAllResult_withPK = new int[MAXQUERIES];
  static final int[] exceptResult_withPK = new int[MAXQUERIES];
  static final int[] exceptAllResult_withPK = new int[MAXQUERIES];
  private static void setResults_withPK(int unionCount, int unionAllCount,
      int intersectCount, int intersectAllCount,
      int exceptCount, int exceptAllCount) {
    unionResult_withPK[currentCount] = unionCount;
    unionAllResult_withPK[currentCount] = unionAllCount;
    intersectResult_withPK[currentCount] = intersectCount;
    intersectAllResult_withPK[currentCount] = intersectAllCount;
    exceptResult_withPK[currentCount] = exceptCount;
    exceptAllResult_withPK[currentCount] = exceptAllCount;
  }
  
  /*
   * Handle scenarios where table have NOT been created with primary keys
   * This means, there would be DUPLICATE values in tables
   */
  static final int[] unionResult_noPK = new int[MAXQUERIES];
  static final int[] unionAllResult_noPK = new int[MAXQUERIES];
  static final int[] intersectResult_noPK = new int[MAXQUERIES];
  static final int[] intersectAllResult_noPK = new int[MAXQUERIES];
  static final int[] exceptResult_noPK = new int[MAXQUERIES];
  static final int[] exceptAllResult_noPK = new int[MAXQUERIES];
  private static void setResults_noPK(int unionCount, int unionAllCount,
      int intersectCount, int intersectAllCount,
      int exceptCount, int exceptAllCount) {
    unionResult_noPK[currentCount] = unionCount;
    unionAllResult_noPK[currentCount] = unionAllCount;
    intersectResult_noPK[currentCount] = intersectCount;
    intersectAllResult_noPK[currentCount] = intersectAllCount;
    exceptResult_noPK[currentCount] = exceptCount;
    exceptAllResult_noPK[currentCount] = exceptAllCount;
  }
  
  /*
   * Write Queries, and set their expected count
   * Queries will not be execute queries if count exceed MAXQUERIES
   */
  static { // 0-3

    // Projection Queries

    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1";
      queryStringsRight[currentCount] =  "Select * from " + schemaName + ".orders2";
      setResults_withPK(30 /*union*/, 40 /*union all*/, 
          10 /*intersect*/, 10 /*intersect all*/, 
          10 /*except*/, 10 /*except all*/);
      setResults_noPK(30 /*union*/, 100 /*union all*/, 
          10 /*intersect*/, 20 /*intersect all*/, 
          10 /*except*/, 40 /*except all*/);
      currentCount++;
    } 

    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1";
      queryStringsRight[currentCount] = "Select id, cust_name, vol, security_id, num, addr from " + schemaName + ".orders2";
      setResults_withPK(30 /*union*/, 40 /*union all*/, 
          10 /*intersect*/, 10 /*intersect all*/, 
          10 /*except*/, 10 /*except all*/);
      setResults_noPK(30 /*union*/, 100 /*union all*/, 
          10 /*intersect*/, 20 /*intersect all*/, 
          10 /*except*/, 40 /*except all*/);
      currentCount++;
    } 

    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select id, cust_name, vol, security_id, num, addr from " + schemaName + ".orders1";
      queryStringsRight[currentCount] =  "Select * from " + schemaName + ".orders2";
      setResults_withPK(30 /*union*/, 40 /*union all*/, 
          10 /*intersect*/, 10 /*intersect all*/, 
          10 /*except*/, 10 /*except all*/);
      setResults_noPK(30 /*union*/, 100 /*union all*/, 
          10 /*intersect*/, 20 /*intersect all*/, 
          10 /*except*/, 40 /*except all*/);
      currentCount++;
    } 

    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1";
      queryStringsRight[currentCount] =  "Select * from " + schemaName + ".orders2";
      setResults_withPK(30 /*union*/, 40 /*union all*/, 
          10 /*intersect*/, 10 /*intersect all*/, 
          10 /*except*/, 10 /*except all*/);
      setResults_noPK(30 /*union*/, 100 /*union all*/, 
          10 /*intersect*/, 20 /*intersect all*/, 
          10 /*except*/, 40 /*except all*/);
      currentCount++;
    } 
  }
  
  static { // 4-15

    // Equality on PK
    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1" + " where id = " + 9;
      queryStringsRight[currentCount] = "Select * from " + schemaName + ".orders2" + " where id = " + 9;
      setResults_withPK(1 /*union*/, 1 /*union all*/,
          0 /*intersect*/, 0 /*intersect all*/, 
          1 /*except*/, 1 /*except all*/);
      setResults_noPK(1 /*union*/, 3 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          1 /*except*/, 3 /*except all*/);
      currentCount++;
    } 
    
    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1" + " where id = " + 18;
      queryStringsRight[currentCount] = "Select * from " + schemaName + ".orders2" + " where id = " + 18;
      setResults_withPK(1 /*union*/, 2 /*union all*/,
          1 /*intersect*/, 1 /*intersect all*/, 
          0 /*except*/, 0 /*except all*/);
      setResults_noPK(1 /*union*/, 5 /*union all*/, 
          1 /*intersect*/, 2 /*intersect all*/, 
          0 /*except*/, 1 /*except all*/);
      currentCount++;
    } 
    
    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1" + " where id = " + 28;
      queryStringsRight[currentCount] = "Select * from " + schemaName + ".orders2" + " where id = " + 28;
      setResults_withPK(1 /*union*/, 1 /*union all*/,
          0 /*intersect*/, 0 /*intersect all*/, 
          0 /*except*/, 0 /*except all*/);
      setResults_noPK(1 /*union*/, 2 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          0 /*except*/, 0 /*except all*/);
      currentCount++;
    } 
    
    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1" + " where id = " + 38;
      queryStringsRight[currentCount] = "Select * from " + schemaName + ".orders2" + " where id = " + 38;
      setResults_withPK(0 /*union*/, 0 /*union all*/,
          0 /*intersect*/, 0 /*intersect all*/, 
          0 /*except*/, 0 /*except all*/);
      setResults_noPK(0 /*union*/, 0 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          0 /*except*/, 0 /*except all*/);
      currentCount++;
    } 
    
    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1" + " where id = " + 9;
      queryStringsRight[currentCount] = "Select * from " + schemaName + ".orders2" + " where id = " + 28;
      setResults_withPK(2 /*union*/, 2 /*union all*/,
          0 /*intersect*/, 0 /*intersect all*/, 
          1 /*except*/, 1 /*except all*/);
      setResults_noPK(2 /*union*/, 5 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          1 /*except*/, 3 /*except all*/);
      currentCount++;
    } 

    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1" + " where id = " + 9;
      queryStringsRight[currentCount] = "Select * from " + schemaName + ".orders2" + " where id = " + 18;
      setResults_withPK(2 /*union*/, 2 /*union all*/,
          0 /*intersect*/, 0 /*intersect all*/, 
          1 /*except*/, 1 /*except all*/);
      setResults_noPK(2 /*union*/, 5 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          1 /*except*/, 3 /*except all*/);
      currentCount++;
    } 

    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1" + " where id = " + 18;
      queryStringsRight[currentCount] = "Select * from " + schemaName + ".orders2" + " where id = " + 38;
      setResults_withPK(1 /*union*/, 1 /*union all*/,
          0 /*intersect*/, 0 /*intersect all*/, 
          1 /*except*/, 1 /*except all*/);
      setResults_noPK(1 /*union*/, 3 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          1 /*except*/, 3 /*except all*/);
      currentCount++;
    } 

    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1" + " where id = " + 38;
      queryStringsRight[currentCount] = "Select * from " + schemaName + ".orders2" + " where id = " + 25;
      setResults_withPK(1 /*union*/, 1 /*union all*/,
          0 /*intersect*/, 0 /*intersect all*/, 
          0 /*except*/, 0 /*except all*/);
      setResults_noPK(1 /*union*/, 2 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          0 /*except*/, 0 /*except all*/);
      currentCount++;
    } 

    // now change in projection list
    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select id, cust_name, vol, security_id, num, addr from " + schemaName + ".orders1" + " where id = " + 9;
      queryStringsRight[currentCount] = "Select id, cust_name, vol, security_id, num, addr from " + schemaName + ".orders2" + " where id = " + 28;
      setResults_withPK(2 /*union*/, 2 /*union all*/,
          0 /*intersect*/, 0 /*intersect all*/, 
          1 /*except*/, 1 /*except all*/);
      setResults_noPK(2 /*union*/, 5 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          1 /*except*/, 3 /*except all*/);
      currentCount++;
    } 

    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select id, cust_name, vol, security_id, num, addr from " + schemaName + ".orders1" + " where id = " + 9;
      queryStringsRight[currentCount] = "Select * from " + schemaName + ".orders2" + " where id = " + 18;
      setResults_withPK(2 /*union*/, 2 /*union all*/,
          0 /*intersect*/, 0 /*intersect all*/, 
          1 /*except*/, 1 /*except all*/);
      setResults_noPK(2 /*union*/, 5 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          1 /*except*/, 3 /*except all*/);
      currentCount++;
    } 

    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select id, cust_name, vol, security_id, num, addr from " + schemaName + ".orders1" + " where id = " + 18;
      queryStringsRight[currentCount] = "Select * from " + schemaName + ".orders2" + " where id = " + 38;
      setResults_withPK(1 /*union*/, 1 /*union all*/,
          0 /*intersect*/, 0 /*intersect all*/, 
          1 /*except*/, 1 /*except all*/);
      setResults_noPK(1 /*union*/, 3 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          1 /*except*/, 3 /*except all*/);
      currentCount++;
    } 

    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1" + " where id = " + 38;
      queryStringsRight[currentCount] = "Select id, cust_name, vol, security_id, num, addr from " + schemaName + ".orders2" + " where id = " + 25;
      setResults_withPK(1 /*union*/, 1 /*union all*/,
          0 /*intersect*/, 0 /*intersect all*/, 
          0 /*except*/, 0 /*except all*/);
      setResults_noPK(1 /*union*/, 2 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          0 /*except*/, 0 /*except all*/);
      currentCount++;
    }     
  }

  static { //16-20

    // where clause: inequality on PK
    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1" + " where id < " + 18;
      queryStringsRight[currentCount] = "Select * from " + schemaName + ".orders2" + " where id < " + 18;
      setResults_withPK(18 /*union*/, 26 /*union all*/, 
          8 /*intersect*/, 8 /*intersect all*/, 
          10 /*except*/, 10 /*except all*/);
      setResults_noPK(18 /*union*/, 70 /*union all*/, 
          8 /*intersect*/, 16 /*intersect all*/, 
          10 /*except*/, 38 /*except all*/);
      currentCount++;
    } 
    
    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1" + " where id > " + 18;
      queryStringsRight[currentCount] = "Select * from " + schemaName + ".orders2" + " where id > " + 18;
      setResults_withPK(11 /*union*/, 12 /*union all*/, 
          1 /*intersect*/, 1 /*intersect all*/, 
          0 /*except*/, 0 /*except all*/);
      setResults_noPK(11 /*union*/, 25 /*union all*/, 
          1 /*intersect*/, 2 /*intersect all*/, 
          0 /*except*/, 1 /*except all*/);
      currentCount++;
    } 
    
    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1" + " where id < " + 9;
      queryStringsRight[currentCount] = "Select * from " + schemaName + ".orders2" + " where id > " + 28;
      setResults_withPK(10 /*union*/, 10 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          9 /*except*/, 9 /*except all*/);
      setResults_noPK(10 /*union*/, 29 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          9 /*except*/, 27 /*except all*/);
      currentCount++;
    } 

    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1" + " where id > " + 9;
      queryStringsRight[currentCount] = "Select * from " + schemaName + ".orders2" + " where id < " + 18;
      setResults_withPK(10 /*union*/, 18 /*union all*/, 
          8 /*intersect*/, 8 /*intersect all*/, 
          2 /*except*/, 2 /*except all*/);
      setResults_noPK(10 /*union*/, 46 /*union all*/, 
          8 /*intersect*/, 16 /*intersect all*/, 
          2 /*except*/, 14 /*except all*/);
      currentCount++;
    } 

    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1" + " where id < " + 18;
      queryStringsRight[currentCount] = "Select * from " + schemaName + ".orders2" + " where id > " + 38;
      setResults_withPK(18 /*union*/, 18 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          18 /*except*/, 18 /*except all*/);
      setResults_noPK(18 /*union*/, 54 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          18 /*except*/, 54 /*except all*/);
      currentCount++;
    } 
  }

  static { //21-28

    // where clause: equality on other column
    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1" + " where vol = " + 9;
      queryStringsRight[currentCount] = "Select * from " + schemaName + ".orders2" + " where vol = " + 9;
      setResults_withPK(1 /*union*/, 1 /*union all*/,
          0 /*intersect*/, 0 /*intersect all*/, 
          1 /*except*/, 1 /*except all*/);
      setResults_noPK(1 /*union*/, 3 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          1 /*except*/, 3 /*except all*/);
      currentCount++;
    } 
    
    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1" + " where vol = " + 18;
      queryStringsRight[currentCount] = "Select * from " + schemaName + ".orders2" + " where vol = " + 18;
      setResults_withPK(1 /*union*/, 2 /*union all*/,
          1 /*intersect*/, 1 /*intersect all*/, 
          0 /*except*/, 0 /*except all*/);
      setResults_noPK(1 /*union*/, 5 /*union all*/, 
          1 /*intersect*/, 2 /*intersect all*/, 
          0 /*except*/, 1 /*except all*/);
      currentCount++;
    } 
    
    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1" + " where vol = " + 28;
      queryStringsRight[currentCount] = "Select * from " + schemaName + ".orders2" + " where vol = " + 28;
      setResults_withPK(1 /*union*/, 1 /*union all*/,
          0 /*intersect*/, 0 /*intersect all*/, 
          0 /*except*/, 0 /*except all*/);
      setResults_noPK(1 /*union*/, 2 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          0 /*except*/, 0 /*except all*/);
      currentCount++;
    } 
    
    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1" + " where vol = " + 38;
      queryStringsRight[currentCount] = "Select * from " + schemaName + ".orders2" + " where vol = " + 38;
      setResults_withPK(0 /*union*/, 0 /*union all*/,
          0 /*intersect*/, 0 /*intersect all*/, 
          0 /*except*/, 0 /*except all*/);
      setResults_noPK(0 /*union*/, 0 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          0 /*except*/, 0 /*except all*/);
      currentCount++;
    } 

    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1" + " where vol = " + 9;
      queryStringsRight[currentCount] = "Select * from " + schemaName + ".orders2" + " where vol = " + 28;
      setResults_withPK(2 /*union*/, 2 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          1 /*except*/, 1 /*except all*/);
      setResults_noPK(2 /*union*/, 5 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          1 /*except*/, 3 /*except all*/);
      currentCount++;
    } 

    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1" + " where vol = " + 9;
      queryStringsRight[currentCount] = "Select * from " + schemaName + ".orders2" + " where vol = " + 18;
      setResults_withPK(2 /*union*/, 2 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          1 /*except*/, 1 /*except all*/);
      setResults_noPK(2 /*union*/, 5 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          1 /*except*/, 3 /*except all*/);
      currentCount++;
    } 

    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1" + " where vol = " + 18;
      queryStringsRight[currentCount] = "Select * from " + schemaName + ".orders2" + " where vol = " + 38;
      setResults_withPK(1 /*union*/, 1 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          1 /*except*/, 1 /*except all*/);
      setResults_noPK(1 /*union*/, 3 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          1 /*except*/, 3 /*except all*/);
      currentCount++;
    } 

    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1" + " where vol = " + 38;
      queryStringsRight[currentCount] = "Select * from " + schemaName + ".orders2" + " where vol = " + 25;
      setResults_withPK(1 /*union*/, 1 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          0 /*except*/, 0 /*except all*/);
      setResults_noPK(1 /*union*/, 2 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          0 /*except*/, 0 /*except all*/);
      currentCount++;
    } 
  }

  static { //29-33

    // where clause: inequality on other column
    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1" + " where vol < " + 18;
      queryStringsRight[currentCount] = "Select * from " + schemaName + ".orders2" + " where vol < " + 18;
      setResults_withPK(18 /*union*/, 26 /*union all*/, 
          8 /*intersect*/, 8 /*intersect all*/, 
          10 /*except*/, 10 /*except all*/);
      setResults_noPK(18 /*union*/, 70 /*union all*/, 
          8 /*intersect*/, 16 /*intersect all*/, 
          10 /*except*/, 38 /*except all*/);
      currentCount++;
    } 
    
    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1" + " where vol > " + 18;
      queryStringsRight[currentCount] = "Select * from " + schemaName + ".orders2" + " where vol > " + 18;
      setResults_withPK(11 /*union*/, 12 /*union all*/, 
          1 /*intersect*/, 1 /*intersect all*/, 
          0 /*except*/, 0 /*except all*/);
      setResults_noPK(11 /*union*/, 25 /*union all*/, 
          1 /*intersect*/, 2 /*intersect all*/, 
          0 /*except*/, 1 /*except all*/);
      currentCount++;
    } 
    
    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1" + " where vol < " + 9;
      queryStringsRight[currentCount] = "Select * from " + schemaName + ".orders2" + " where vol > " + 28;
      setResults_withPK(10 /*union*/, 10 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          9 /*except*/, 9 /*except all*/);
      setResults_noPK(10 /*union*/, 29 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          9 /*except*/, 27 /*except all*/);
      currentCount++;
    } 

    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1" + " where vol > " + 9;
      queryStringsRight[currentCount] = "Select * from " + schemaName + ".orders2" + " where vol < " + 18;
      setResults_withPK(10 /*union*/, 18 /*union all*/, 
          8 /*intersect*/, 8 /*intersect all*/, 
          2 /*except*/, 2 /*except all*/);
      setResults_noPK(10 /*union*/, 46 /*union all*/, 
          8 /*intersect*/, 16 /*intersect all*/, 
          2 /*except*/, 14 /*except all*/);
      currentCount++;
    } 

    assert (currentCount < MAXQUERIES); {
      queryStringsLeft[currentCount] = "Select * from " + schemaName + ".orders1" + " where vol < " + 18;
      queryStringsRight[currentCount] = "Select * from " + schemaName + ".orders2" + " where vol > " + 38;
      setResults_withPK(18 /*union*/, 18 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          18 /*except*/, 18 /*except all*/);
      setResults_noPK(18 /*union*/, 54 /*union all*/, 
          0 /*intersect*/, 0 /*intersect all*/, 
          18 /*except*/, 54 /*except all*/);
      currentCount++;
    } 
  }
  
  public SetQueriesDUnitHelper() {
  }
  
  /*
   * Set of table create functions
   */
  public abstract class CreateTableDUnitF
  {
    public abstract void createTableFunction(Connection conn, 
        String tabname,
        int redundancy) throws SQLException; 
    public abstract void createColocatedTableFunction(Connection conn, 
        String tabname,
        int redundancy) throws SQLException; 
  }

  /*
   * Partition on col
   */
  public class CreateTable_withPK_PR_onPK_1 extends CreateTableDUnitF
  {
    String mainTableName = null;
    
    @Override
    public void createTableFunction(Connection conn, 
        String tabname,
        int redundancy) throws SQLException {
      // We create a table...
      String tablename = schemaName + "." + tabname;
      mainTableName = tablename;
      Statement s = conn.createStatement();

      if (redundancy == 0) {
        s.execute("create table " + tablename
            + "(id int not null , cust_name varchar(200), vol int, "
            + "security_id varchar(10), num int, addr varchar(100)"
            + ", primary key (id))  partition by column (id)"); // no redundancy
      }
      else {
        s.execute("create table " + tablename
            + "(id int not null , cust_name varchar(200), vol int, "
            + "security_id varchar(10), num int, addr varchar(100)"
            + ", primary key (id))  partition by column (id)" 
            + " redundancy " + redundancy); // redundancy x?
      }
    }

    @Override
    public void createColocatedTableFunction(Connection conn, String tabname,
        int redundancy) throws SQLException {
      // We create a table...
      String tablename = schemaName + "." + tabname;
      Statement s = conn.createStatement();

      if (redundancy == 0) {
        s.execute("create table " + tablename
            + "(id int not null , cust_name varchar(200), vol int, "
            + "security_id varchar(10), num int, addr varchar(100)"
            + ", primary key (id))  partition by column (id)"
            +  " colocate with (" + mainTableName + ")");// no redundancy
      }
      else {
        s.execute("create table " + tablename
            + "(id int not null , cust_name varchar(200), vol int, "
            + "security_id varchar(10), num int, addr varchar(100)"
            + ", primary key (id))  partition by column (id)" 
            + " colocate with (" + mainTableName + ")"
            + " redundancy " + redundancy); // redundancy x?
      }
    }
  }

  /*
   * Partition on col
   */
  public class CreateTable_withPK_PR_onCol_1 extends CreateTableDUnitF
  {
    String mainTableName = null;
    
    @Override
    public void createTableFunction(Connection conn, String tabname,
        int redundancy) throws SQLException {
      // We create a table...
      String tablename = schemaName + "." + tabname;
      mainTableName = tablename;
      Statement s = conn.createStatement();

      if (redundancy == 0) {
        s.execute("create table " + tablename
            + "(id int not null , cust_name varchar(200), vol int, "
            + "security_id varchar(10), num int, addr varchar(100)"
            + ", primary key (id))  partition by column (vol)"); // no
        // redundancy
      }
      else {
        s.execute("create table " + tablename
            + "(id int not null , cust_name varchar(200), vol int, "
            + "security_id varchar(10), num int, addr varchar(100)"
            + ", primary key (id))  partition by column (vol)" 
            + " redundancy " + redundancy); // redundancy x?
      }
    }

    @Override
    public void createColocatedTableFunction(Connection conn, String tabname,
        int redundancy) throws SQLException {
      // We create a table...
      String tablename = schemaName + "." + tabname;
      Statement s = conn.createStatement();

      if (redundancy == 0) {
        s.execute("create table " + tablename
            + "(id int not null , cust_name varchar(200), vol int, "
            + "security_id varchar(10), num int, addr varchar(100)"
            + ", primary key (id))  partition by column (vol)"
            + " colocate with (" + mainTableName + ")"); // no redundancy
      }
      else {
        s.execute("create table " + tablename
            + "(id int not null , cust_name varchar(200), vol int, "
            + "security_id varchar(10), num int, addr varchar(100)"
            + ", primary key (id))  partition by column (vol)" 
            + " colocate with (" + mainTableName + ")"
            + " redundancy " + redundancy); // redundancy x?
      }
    }
  }

  /*
   * Range on PK
   */
  public class CreateTable_withPK_PR_onRange_ofPK_1 extends CreateTableDUnitF
  {
    String mainTableName = null;
    
    @Override
    public void createTableFunction(Connection conn, String tabname,
        int redundancy) throws SQLException {
      // We create a table...
      String tablename = schemaName + "." + tabname;
      mainTableName = tablename;
      Statement s = conn.createStatement();

      s.execute("create table " + tablename
          + " (id int not null , cust_name varchar(200), vol int, "
          + " security_id varchar(10), num int, addr varchar(100)"
          + ", primary key ( id )) " + " partition by range ( id ) "
          + " (VALUES BETWEEN 0 AND 9, VALUES BETWEEN 10 AND 20)  "
          + "BUCKETS 2 " + " REDUNDANCY " + redundancy);
    }

    @Override
    public void createColocatedTableFunction(Connection conn, String tabname,
        int redundancy) throws SQLException {
      // We create a table...
      String tablename = schemaName + "." + tabname;
      Statement s = conn.createStatement();

      s.execute("create table " + tablename
          + " (id int not null , cust_name varchar(200), vol int, "
          + " security_id varchar(10), num int, addr varchar(100)"
          + ", primary key ( id )) " + " partition by range ( id ) "
          + " (VALUES BETWEEN 0 AND 9, VALUES BETWEEN 10 AND 20)  "
          + " colocate with (" + mainTableName + ")"
          + " REDUNDANCY " + redundancy);
    }
  }

  /*
   * Range on other column
   */
  public class  CreateTable_withPK_PR_onRange_ofCol_1 extends CreateTableDUnitF
  {
    String mainTableName = null;
    
    @Override
    public void createTableFunction( Connection conn, 
        String tabname,
        int redundancy) throws SQLException {
      // We create a table...
      String tablename = schemaName + "." + tabname;
      mainTableName = tablename;
      Statement s = conn.createStatement();

      s.execute("create table " + tablename
          + " (id int not null , cust_name varchar(200), vol int, "
          + " security_id varchar(10), num int, addr varchar(100)"
          + ", primary key ( id )) " + " partition by range ( vol ) "
          + " (VALUES BETWEEN 0 AND 9, VALUES BETWEEN 10 AND 20)  "
          + " BUCKETS 2 " + " REDUNDANCY " + redundancy);
    }

    @Override
    public void createColocatedTableFunction(Connection conn, String tabname,
        int redundancy) throws SQLException {
      // We create a table...
      String tablename = schemaName + "." + tabname;
      Statement s = conn.createStatement();

      s.execute("create table " + tablename
          + " (id int not null , cust_name varchar(200), vol int, "
          + " security_id varchar(10), num int, addr varchar(100)"
          + ", primary key ( id )) " + " partition by range ( vol ) "
          + " (VALUES BETWEEN 0 AND 9, VALUES BETWEEN 10 AND 20)  "
          + " colocate with (" + mainTableName + ")" 
          + " REDUNDANCY " + redundancy);
    }
  }
  
  /*
   * Replicated table
   * redundant redundancy
   */
  public class CreateTable_withPK_Replicated_1 extends CreateTableDUnitF
  {
    @Override
    public void createTableFunction(Connection conn, String tabname,
        int redundancy) throws SQLException {
      // We create a table...
      String tablename = schemaName + "." + tabname;
      Statement s = conn.createStatement();

      s.execute("create table " + tablename
          + "(id int not null , cust_name varchar(200), vol int, "
          + "security_id varchar(10), num int, addr varchar(100)"
          + ", primary key (id))  replicate");
    }

    @Override
    public void createColocatedTableFunction(Connection conn, String tabname,
        int redundancy) throws SQLException {
      throw new SQLException("Colocated tables on replicated table not supported");
    }
  }
  
  /*
   * Partition on col
   */
  public class CreateTable_noPK_PR_onCol_1 extends CreateTableDUnitF
  {
    String mainTableName = null;
    
    @Override
    public void createTableFunction(Connection conn, String tabname,
        int redundancy) throws SQLException {
      // We create a table...
      String tablename = schemaName + "." + tabname;
      mainTableName = tablename;
      Statement s = conn.createStatement();

      if (redundancy == 0) {
        s.execute("create table " + tablename
            + "(id int not null , cust_name varchar(200), vol int, "
            + "security_id varchar(10), num int, addr varchar(100))"
            + " partition by column (vol)"); // no
        // redundancy
      }
      else {
        s.execute("create table " + tablename
            + "(id int not null , cust_name varchar(200), vol int, "
            + "security_id varchar(10), num int, addr varchar(100))"
            + " partition by column (vol)" 
            + " redundancy " + redundancy); // redundancy x?
      }
    }

    @Override
    public void createColocatedTableFunction(Connection conn, String tabname,
        int redundancy) throws SQLException {
      throw new SQLException("Colocated tables test has not been added yet");
    }
  }

  /*
   * Range on other column
   */
  public class  CreateTable_noPK_PR_onRange_ofCol_1 extends CreateTableDUnitF
  {
    String mainTableName = null;
    
    @Override
    public void createTableFunction( Connection conn, 
        String tabname,
        int redundancy) throws SQLException {
      // We create a table...
      String tablename = schemaName + "." + tabname;
      mainTableName = tablename;
      Statement s = conn.createStatement();

      s.execute("create table " + tablename
          + " (id int not null , cust_name varchar(200), vol int, "
          + " security_id varchar(10), num int, addr varchar(100))"
          + " partition by range ( vol ) "
          + " (VALUES BETWEEN 0 AND 9, VALUES BETWEEN 10 AND 20)  "
          + " BUCKETS 2 " + " REDUNDANCY " + redundancy);
    }

    @Override
    public void createColocatedTableFunction(Connection conn, String tabname,
        int redundancy) throws SQLException {
      throw new SQLException("Colocated tables test has not been added yet");
    }
  }
  
  static void createLocalIndex(Connection conn, String tabname, String columnname) throws SQLException {
    String tablename = schemaName + "." + tabname;
    String indexName = tabname+columnname+columnname.length();

    Statement s = conn.createStatement();
    s.execute("create INDEX " + indexName + " on " + tablename + "(" + columnname + ")");
    s.close();
  }

  static void loadSampleData(Logger logw, Connection conn, int numOfRows, String tabname, int startVal) throws SQLException {
    String tablename = schemaName + "." + tabname;
    logw.info("Loading data (" 
        + numOfRows + " rows..."
        + startVal + " as start value)...");
    String[] securities = { "IBM", "INTC", "MOT", "TEK", "AMD", "CSCO", "DELL",
        "HP", "SMALL1", "SMALL2" };

    PreparedStatement psInsert = conn
    .prepareStatement("insert into " + tablename + " values (?, ?, ?, ?, ?, ?)");
    for (int i = 0; i < numOfRows; i++) {
      int baseId = i + startVal;
      String custName = "CustomerWithaLongName" + baseId;
      psInsert.setInt(1, baseId);
      psInsert.setString(2, custName);
      psInsert.setInt(3, baseId); // max volume is 500
      psInsert.setString(4, securities[baseId%10]);
      psInsert.setInt(5, 0);
      String queryString = "Emperors club for desperate men, "
        + "Washington DC, District of Columbia";
      psInsert.setString(6, queryString);
      psInsert.executeUpdate();
    }
  }

  static void dropTable(Connection conn, String tabname) throws SQLException {
    String tablename = schemaName + "." + tabname;

    Statement s = conn.createStatement();
    s.execute("drop table " + tablename);
    s.close();
  }

  static void createSchema(Connection conn) throws SQLException {
    Statement s = conn.createStatement();
    s.execute("create schema " + schemaName);
    s.close();
  }

  static void dropSchema(Connection conn) throws SQLException {
    Statement s = conn.createStatement();
    s.execute("drop schema " + schemaName + " RESTRICT");
    s.close();
  }
  
  /*
   * Execute given query
   */
  static void executeSetOp_1(Logger logw, String queryType,
      int expectedRows,
      String query1,
      String query2, 
      Connection conn,
      String failureMessage,
      int failIndex) throws Exception {
    SanityManager.ASSERT(query1 != null, "Query1 is null");
    SanityManager.ASSERT(query2 != null, "Query2 is null");
    String execQuery = query1 + " " + queryType + " " + query2;
    logw.info("Execute Query["+failIndex+"]: " + execQuery);
    ResultSet rs = conn.createStatement().executeQuery(execQuery);
    int rowCount = 0;
    Map<Integer, Integer> resultMap = new TreeMap<Integer, Integer>(); 
    while(rs.next()) {
      rowCount++;
      resultMap.put(rs.getInt(1), (resultMap.get(rs.getInt(1)) == null? 1 : resultMap.get(rs.getInt(1)) + 1));
    }
    rs.close();

    logw.info("expectedRows : " + expectedRows + " Got : " + rowCount );
    logw.info(resultMap.toString());
    junit.framework.Assert.assertEquals(failureMessage + execQuery, expectedRows, rowCount);
  } /**/

  /*
   * Help execute given query in PK case
   */
  static void callExecuteSetOp_withPK(Logger logw,
      boolean doSetOp,
      boolean doSetOpDistinct,
      boolean doSetOpAll, 
      Connection conn,
      String failureMessage) throws Exception {
    SanityManager.ASSERT(conn != null, "Connection is null");
    if (doSetOp) {
      for(int i=0;  i<currentCount; i++){
        String onFailMessage = failureMessage 
        + " query: " 
        + "(" + i + ") ";
        executeSetOp_1(logw, "union", 
            unionResult_withPK[i],
            queryStringsLeft[i],
            queryStringsRight[i], 
            conn,
            onFailMessage, i);
        executeSetOp_1(logw, "intersect", 
            intersectResult_withPK[i],
            queryStringsLeft[i],
            queryStringsRight[i], 
            conn,
            onFailMessage, i);
        executeSetOp_1(logw, "except", 
            exceptResult_withPK[i],
            queryStringsLeft[i],
            queryStringsRight[i], 
            conn,
            onFailMessage, i);
      }
    }
    if (doSetOpDistinct) {
      for(int i=0;  i<currentCount; i++){
        String onFailMessage = failureMessage 
        + " query: " 
        + "(" + i + ") ";
        executeSetOp_1(logw, "union distinct", 
            unionResult_withPK[i],
            queryStringsLeft[i],
            queryStringsRight[i], 
            conn,
            onFailMessage, i);
        executeSetOp_1(logw, "intersect distinct", 
            intersectResult_withPK[i],
            queryStringsLeft[i],
            queryStringsRight[i], 
            conn,
            onFailMessage, i);
        executeSetOp_1(logw, "except distinct", 
            exceptResult_withPK[i],
            queryStringsLeft[i],
            queryStringsRight[i], 
            conn,
            onFailMessage, i);
      }
    }
    if (doSetOpAll) {
      for(int i=0;  i< currentCount; i++){
        String onFailMessage = failureMessage 
        + " query: " 
        + "(" + i + ") ";
        executeSetOp_1(logw, "union all", 
            unionAllResult_withPK[i],
            queryStringsLeft[i],
            queryStringsRight[i], 
            conn,
            onFailMessage, i);
        executeSetOp_1(logw, "intersect all", 
            intersectAllResult_withPK[i],
            queryStringsLeft[i],
            queryStringsRight[i], 
            conn,
            onFailMessage, i);
        executeSetOp_1(logw, "except all", 
            exceptAllResult_withPK[i],
            queryStringsLeft[i],
            queryStringsRight[i], 
            conn,
            onFailMessage, i);
      }
    }
  }/**/ 

  /*
   * Help execute given query in no PK case
   */
  static void callExecuteSetOp_noPK(Logger logw,
      boolean doSetOp,
      boolean doSetOpDistinct,
      boolean doSetOpAll, 
      Connection conn,
      String failureMessage) throws Exception {
    SanityManager.ASSERT(conn != null, "Connection is null");
    if (doSetOp) {
      for(int i=0;  i< currentCount; i++){
        String onFailMessage = failureMessage 
        + " query: " 
        + "(" + i + ") ";
        executeSetOp_1(logw, "union", 
            unionResult_noPK[i],
            queryStringsLeft[i],
            queryStringsRight[i], 
            conn,
            onFailMessage, i);
        executeSetOp_1(logw, "intersect", 
            intersectResult_noPK[i],
            queryStringsLeft[i],
            queryStringsRight[i], 
            conn,
            onFailMessage, i);
        executeSetOp_1(logw, "except", 
            exceptResult_noPK[i],
            queryStringsLeft[i],
            queryStringsRight[i], 
            conn,
            onFailMessage, i);
      }
    }
    if (doSetOpDistinct) {
      for(int i=0;  i< currentCount; i++){
        String onFailMessage = failureMessage 
        + " query: " 
        + "(" + i + ") ";
        executeSetOp_1(logw, "union distinct", 
            unionResult_noPK[i],
            queryStringsLeft[i],
            queryStringsRight[i], 
            conn,
            onFailMessage, i);
        executeSetOp_1(logw, "intersect distinct", 
            intersectResult_noPK[i],
            queryStringsLeft[i],
            queryStringsRight[i], 
            conn,
            onFailMessage, i);
        executeSetOp_1(logw, "except distinct", 
            exceptResult_noPK[i],
            queryStringsLeft[i],
            queryStringsRight[i], 
            conn,
            onFailMessage, i);
      }
    }
    if (doSetOpAll) {
      for(int i=0;  i< currentCount; i++){
        String onFailMessage = failureMessage 
        + " query: " 
        + "(" + i + ") ";
        executeSetOp_1(logw, "union all", 
            unionAllResult_noPK[i],
            queryStringsLeft[i],
            queryStringsRight[i], 
            conn,
            onFailMessage, i);
        executeSetOp_1(logw, "intersect all", 
            intersectAllResult_noPK[i],
            queryStringsLeft[i],
            queryStringsRight[i], 
            conn,
            onFailMessage, i);
        executeSetOp_1(logw, "except all", 
            exceptAllResult_noPK[i],
            queryStringsLeft[i],
            queryStringsRight[i], 
            conn,
            onFailMessage, i);
      }
    }
  }/**/
  
  /*
   * Non-colocated
   * Test various combinations of PR PR and RR 
   */
  static void caseSetOperators_noColoc_withPK_scenario1(Logger logw,
      int leftTableIndex,
      int rightTableIndex,
      boolean createIndex,
      int redundancy,
      boolean setOPDistinct,
      boolean setOpAll,
      CreateTableDUnitF[] createTableFunctions_noColoc_withPK) throws Exception {

    if (setOPDistinct || setOpAll) {
      Properties props = new Properties();
      Connection conn = TestUtil.getConnection(props);    

      createSchema(conn);

      logw.info("SetOperatorQueriesDUnit.caseSetOperators_noColoc_withPK_scenario1: "
          + "With Index " + createIndex
          + " with redundancy " + redundancy); 

      logw.info("Execute left create table: " 
          + "["
          + leftTableIndex
          + "] "
          + createTableFunctions_noColoc_withPK[leftTableIndex].getClass().getName());
      createTableFunctions_noColoc_withPK[leftTableIndex].createTableFunction(conn, "orders1", redundancy);

      logw.info("Execute right create table: " 
          + "["
          + rightTableIndex
          + "] "
          + createTableFunctions_noColoc_withPK[rightTableIndex].getClass().getName());
      createTableFunctions_noColoc_withPK[rightTableIndex].createTableFunction(conn, "orders2", redundancy);

      if (createIndex) {
        logw.info("Execute create index on left table: "); 
        createLocalIndex(conn, "orders1", "vol");
        logw.info("Execute create index on right table: "); 
        createLocalIndex(conn, "orders2", "vol");
      } 

      loadSampleData(logw, conn, 20, "orders1", 0);
      loadSampleData(logw, conn, 20, "orders2", 10);

      // execute
      logw.info("call execute with: "
          + "doUnion/Intersect/Except " + setOPDistinct
          + " doUnion/Intersect/ExceptDistinct " + setOPDistinct
          + " doUnion/Intersect/ExceptAll " + setOpAll);
      String onFailMessage = "create table: " 
        + "[" + leftTableIndex + "]"
        + "[" + rightTableIndex+ "] "
        + "with index" + "(" + createIndex + ") "
        + "redundancy" + "(" + redundancy + ") ";

      callExecuteSetOp_withPK(logw, setOPDistinct, setOPDistinct, setOpAll, 
            conn, onFailMessage);

        // drop tables
        dropTable(conn, "orders2");
        dropTable(conn, "orders1");
        dropSchema(conn);
    } 
    else {
      logw.info("SetOperatorQueriesDUnit.caseSetOperators_noColoc_withPK_scenario1: "
          + "With Index " + createIndex
          + " with redundancy " + redundancy
          + "Did not execute for being disabled"); 
    }
  } /**/
  
}
