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
package com.pivotal.gemfirexd.query;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Random;

import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.engine.sql.execute.GemFireDistributedResultSet;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.ResultSet;
import io.snappydata.test.dunit.VM;

@SuppressWarnings("serial")
public class SingleTablePredicatesCheckDUnit extends DistributedSQLTestBase {
  
  public static boolean testRunForReplicatedTablesOnly = false; 

  public static final int byrange = QueryEvaluationHelper.byrange, 
                          bylist = QueryEvaluationHelper.bylist, 
                          bycolumn = QueryEvaluationHelper.bycolumn, 
                          ANDing = QueryEvaluationHelper.ANDing, 
                          ORing = QueryEvaluationHelper.ORing, 
                          Contains = QueryEvaluationHelper.Contains, 
                          Ordered = QueryEvaluationHelper.Ordered,
                          TimesOccur = QueryEvaluationHelper.TimesOccur;
  
  public static final int noCheck = QueryEvaluationHelper.noCheck, 
                          fixLater = QueryEvaluationHelper.fixLater, 
                          fixImmediate = QueryEvaluationHelper.fixImmediate, 
                          wontFix = QueryEvaluationHelper.wontFix, 
                          fixDepend = QueryEvaluationHelper.fixDepend, 
                          fixInvestigate = QueryEvaluationHelper.fixInvestigate;

  public SingleTablePredicatesCheckDUnit(String name) {
    super(name);
  }

  @Override
  protected String reduceLogging() {
    // these tests generate lots of logs, so reducing them
    return "config";
  }

  public static final Object orderbyQueries[][][][] = {
         { {{ "select ID, DESCRIPTION from TESTTABLE where ID  >= ? And ID <= ? Order By ID" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(1) }
            ,{ new Integer(Types.INTEGER), new Integer(3) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(3), new Integer(1), new Integer(2) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(Ordered), new Integer(1), new Integer[] {1,2,3} } 
           }
          ,{//getRoutingParameter inputs
             { new Integer(byrange), new Integer(1), true, new Integer(3), true, 1 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
           }
         } //1
        ,{ {{ "select ID, DESCRIPTION from TESTTABLE where ID  >= ? And ID <= ? Order By ID DESC" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(1) }
            ,{ new Integer(Types.INTEGER), new Integer(3) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(3), new Integer(1), new Integer(2) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(Ordered), new Integer(1), new Integer[] {3,2,1} } 
           }
          ,{//getRoutingParameter inputs
            { new Integer(byrange), new Integer(1), true, new Integer(3), true, 1 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
           }
         } //2
        ,{ {{ "select ID, DESCRIPTION from TESTTABLE where ID  >= ? And ID <= ? Order By DESCRIPTION ASC NULLS FIRST" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(5) }
            ,{ new Integer(Types.INTEGER), new Integer(7) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(3), new Integer(1), new Integer(2) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(Contains), new Integer(1), new Integer[] {6,5,7} } 
            ,{ new Integer(Ordered), new Integer(2), new String[] {null,"First5","First7"} } 
           }
          ,{//getRoutingParameter inputs
             { new Integer(byrange), new Integer(5), true, new Integer(7), true, 1 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
           }
         } //3
        ,{ {{"select ID, DESCRIPTION from TESTTABLE where ID  >= ? And ID <= ? Order By DESCRIPTION ASC NULLS LAST" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(5) }
            ,{ new Integer(Types.INTEGER), new Integer(7) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(3), new Integer(1), new Integer(2) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(Contains), new Integer(1), new Integer[] {5,7,6} } 
            ,{ new Integer(Ordered), new Integer(2), new String[] {"First5","First7",null} } 
           }
          ,{//getRoutingParameter inputs
             { new Integer(byrange), new Integer(5), true, new Integer(7), true, 1 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
           }
         } //4
        ,{ {{new Integer(fixImmediate), "select ID, DESCRIPTION from TESTTABLE where ID = ? or ID = ? or ID = ? Order By DESCRIPTION DESC NULLS FIRST" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(5) }
            ,{ new Integer(Types.INTEGER), new Integer(6) }
            ,{ new Integer(Types.INTEGER), new Integer(7) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(3), new Integer(1), new Integer(2) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(Contains), new Integer(1), new Integer[] {5,6,7} } 
            ,{ new Integer(Ordered), new Integer(2), new String[] {null,"First7","First5"} } 
           }
          ,{//getRoutingParameter inputs
            //TODO soubhik need to convert this into List
             { new Integer(byrange), new Integer(5), true, new Integer(7), true, 1 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
           }
         } //5
        ,{ {{new Integer(fixImmediate), "select ID, DESCRIPTION from TESTTABLE where ID IN (?,?,?) Order By DESCRIPTION DESC NULLS LAST" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(5) }
            ,{ new Integer(Types.INTEGER), new Integer(6) }
            ,{ new Integer(Types.INTEGER), new Integer(7) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(3), new Integer(1), new Integer(2) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(Contains), new Integer(1), new Integer[] {5,6,7} } 
            ,{ new Integer(Ordered), new Integer(2), new String[] {"First7","First5",null} } 
           }
          ,{//getRoutingParameter inputs
            //TODO soubhik need to convert this into List
             { new Integer(byrange), new Integer(1), true, new Integer(3), true, 1 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
           }
         } //6
        ,{ {{ "select ID, ADDRESS from TESTTABLE where ID  >= ? And ID <= ? Order By ADDRESS" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(1) }
            ,{ new Integer(Types.INTEGER), new Integer(3) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(3), new Integer(1), new Integer(2) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(Ordered), new Integer(2), new String[] {"J 604","J 987","J 987"} } 
           }
          ,{//getRoutingParameter inputs
             { new Integer(byrange), new Integer(1), true, new Integer(3), true, 1 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
           }
         } //7
        ,{ {{ "select ID, ADDRESS from TESTTABLE where ID  >= ? And ID <= ? Order By ADDRESS DESC" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(1) }
            ,{ new Integer(Types.INTEGER), new Integer(3) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(3), new Integer(1), new Integer(2) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(Ordered), new Integer(2), new String[] {"J 987","J 987","J 604"} } 
           }
          ,{//getRoutingParameter inputs
             { new Integer(byrange), new Integer(1), true, new Integer(3), true, 1 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
           }
         } //8
        ,{ {{"select ID, COUNTRY, ADDRESS from TESTTABLE where ID  >= ? And ID <= ? Order By 2" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(1) }
            ,{ new Integer(Types.INTEGER), new Integer(6) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(6), new Integer(2), new Integer(1) } //expectedRows, prunedNodes, noExecQueryNodes
             ,{ new Integer(Contains), new Integer(1), new Integer[]{1,2,3,4,5,6} }
             ,{ new Integer(Ordered) , new Integer(2), new String[] {"INDIA","ROWLD","ROWLD","ROWLD","US","US"} }
           }
          ,{//getRoutingParameter inputs
             { new Integer(byrange), new Integer(1), true, new Integer(6), true, 2 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
           }
         } //9 
        ,{ {{"select ID, COUNTRY, ADDRESS from TESTTABLE where ID  >= ? And ID <= ? Order By 2 DESC" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(1) }
            ,{ new Integer(Types.INTEGER), new Integer(6) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(6), new Integer(2), new Integer(1) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(Contains), new Integer(1), new Integer[]{1,2,3,4,5,6} }
            ,{ new Integer(Ordered) , new Integer(2), new String[] {"US","US","ROWLD","ROWLD","ROWLD","INDIA"} }
           }
          ,{//getRoutingParameter inputs
             { new Integer(byrange), new Integer(1), true, new Integer(6), true, 2 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
           }
         } //10 
        ,{ {{ "select ID, COUNTRY, ADDRESS from TESTTABLE where ID  >= ? And ID <= ? Order By 2,1" }}
           ,{//input parameters
              { new Integer(Types.INTEGER), new Integer(1) }
             ,{ new Integer(Types.INTEGER), new Integer(6) }
            }
           ,{//output (atleast 1 row for Nodes reconciliation)
              { new Integer(6), new Integer(2), new Integer(1) } //expectedRows, prunedNodes, noExecQueryNodes
             ,{ new Integer(Ordered), new Integer(2), new String[] {"INDIA","ROWLD","ROWLD","ROWLD","US","US"} }
             ,{ new Integer(Ordered), new Integer(1), new Integer[] {5,1,2,4,3,6} }
            }
           ,{//getRoutingParameter inputs
              { new Integer(byrange), new Integer(1), true, new Integer(6), true, 2 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
            }
         } //11
        ,{ {{ "select ID, COUNTRY, ADDRESS from TESTTABLE where ID  >= ? And ID <= ? Order By 2 ASC, 1 DESC" }}
           ,{//input parameters
              { new Integer(Types.INTEGER), new Integer(1) }
             ,{ new Integer(Types.INTEGER), new Integer(6) }
            }
           ,{//output (atleast 1 row for Nodes reconciliation)
              { new Integer(6), new Integer(2), new Integer(1) } //expectedRows, prunedNodes, noExecQueryNodes
             ,{ new Integer(Ordered), new Integer(2), new String[] {"INDIA","ROWLD","ROWLD","ROWLD","US","US"} }
             ,{ new Integer(Ordered), new Integer(1), new Integer[] {5,4,2,1,6,3} }
            }
           ,{//getRoutingParameter inputs
              { new Integer(byrange), new Integer(1), true, new Integer(6), true, 2 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
            }
         } //12
        ,{ {{ "select ID, COUNTRY, ADDRESS from TESTTABLE where ID  >= ? And ID <= ? Order By 2 DESC, 1 DESC" }}
           ,{//input parameters
              { new Integer(Types.INTEGER), new Integer(1) }
             ,{ new Integer(Types.INTEGER), new Integer(6) }
            }
           ,{//output (atleast 1 row for Nodes reconciliation)
              { new Integer(6), new Integer(2), new Integer(1) } //expectedRows, prunedNodes, noExecQueryNodes
             ,{ new Integer(Ordered), new Integer(2), new String[] {"US","US","ROWLD","ROWLD","ROWLD","INDIA"} }
             ,{ new Integer(Ordered), new Integer(1), new Integer[] {6,3,4,2,1,5} }
            }
           ,{//getRoutingParameter inputs
              { new Integer(byrange), new Integer(1), true, new Integer(6), true, 2 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
            }
         } //13
        ,{ {{ "select ID, COUNTRY, ADDRESS from TESTTABLE where ID  >= ? And ID <= ? Order By 2,ID" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(1) }
            ,{ new Integer(Types.INTEGER), new Integer(6) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(6), new Integer(2), new Integer(1) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(Ordered), new Integer(2), new String[] {"INDIA","ROWLD","ROWLD","ROWLD","US","US"} }
            ,{ new Integer(Ordered), new Integer(1), new Integer[] {5,1,2,4,3,6} }
           }
          ,{//getRoutingParameter inputs
             { new Integer(byrange), new Integer(1), true, new Integer(6), true, 2 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
           }
         } //14
        ,{ {{ "select ID, COUNTRY, ADDRESS from TESTTABLE where ID  >= ? And ID <= ? Order By COUNTRY DESC,ID" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(1) }
            ,{ new Integer(Types.INTEGER), new Integer(6) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(6), new Integer(2), new Integer(1) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(Ordered), new Integer(2), new String[] {"US","US","ROWLD","ROWLD","ROWLD","INDIA"} }
            ,{ new Integer(Ordered), new Integer(1), new Integer[] {3,6,1,2,4,5} }
           }
          ,{//getRoutingParameter inputs
             { new Integer(byrange), new Integer(1), true, new Integer(6), true, 2 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
           }
         } //15
        ,{ {{new Integer(fixInvestigate), "select ADDRESS as Alias3, COUNTRY as Alias2, ID as Alias1 from TESTTABLE where ID < ? Order By Alias3, Alias1" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(10) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(9), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(Ordered), new Integer(3), new Integer[]{6,8,3,9,1,2,4,5,7} }
            ,{ new Integer(Ordered), new Integer(1), new String[] {null,null,"J 604","J 604","J 987","J 987","J 987","J 987","J 987"} }
           }
          ,{//getRoutingParameter inputs
             { new Integer(byrange), null, true, new Integer(10), true, new Integer(3) } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
           }
         } //16
        ,{ {{"select ADDRESS as Alias3, COUNTRY as Alias2, ID as Alias1 from TESTTABLE where ID < ? Order By Alias3 ASC, Alias1 ASC NULLS LAST" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(10) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(9), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(Ordered), new Integer(1), new String[] {"J 604","J 604","J 987","J 987","J 987","J 987","J 987",null,null} }
            ,{ new Integer(Ordered), new Integer(3), new Integer[] {3,9,1,2,4,5,7,6,8} }
           }
          ,{//getRoutingParameter inputs
             { new Integer(byrange), null, true, new Integer(10), true, new Integer(3) } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
           }
         } //17
        ,{ {{"select ID as id , ADDRESS as address, COUNTRY as country from TESTTABLE where ID >= ? Order By id, country" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(1) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(13), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(Ordered), new Integer(1), new Integer[] {1,2,3,4,5,6,7,8,9,10,11,12,13} }
            ,{ new Integer(Ordered), new Integer(3), new String[]  {"ROWLD","ROWLD","US","ROWLD","INDIA","US","ROWLD","ROWLD","US","INDIA","ROWLD","US","ROWLD"} }
           }
          ,{//getRoutingParameter inputs
             { new Integer(byrange), new Integer(1), true, new Integer(10), true, new Integer(3) } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
           }
         } //18
        ,{ {{ "select ID as alias1, ADDRESS as alias3, COUNTRY as alias2 from TESTTABLE where ID > ? OR COUNTRY in (?) Order By alias2, alias1" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(13) }
            ,{ new Integer(Types.VARCHAR), new String("US") }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(4), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(Ordered), new Integer(1), new Integer[]{3,6,9,12} }
            ,{ new Integer(Ordered), new Integer(3), new String[] {"US","US","US","US"} }
           }
          ,{//getRoutingParameter inputs
             { new Integer(noCheck) } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
           }
         } //19
        ,{ {{new Integer(fixInvestigate), "select ID as alias1, ADDRESS as alias3, COUNTRY as alias2 from TESTTABLE where ID < ? Order By ADDRESS, 3, 1" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(10) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(9), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
            /*,{ new Integer(Ordered), new Integer(2), new String[] {null,null,"J 604","J 604","J 987","J 987","J 987","J 987","J 987"} }
            ,{ new Integer(Ordered), new Integer(3), new String[] {"ROWLD","ROWLD","US","US","ROWLD","ROWLD","ROWLD","ROWLD","INDIA"} }
            ,{ new Integer(Ordered), new Integer(1), new Integer[] {6,8,3,9,1,2,4,7,5} }*/
           }
          ,{//getRoutingParameter inputs
             { new Integer(byrange), new Integer(1), true, new Integer(10), true, new Integer(3) } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
           }
         } //20
        ,{ {{ new Integer(fixImmediate), "select ID, DESCRIPTION from TESTTABLE where ID  >= ? And ID <= ? Order By DESCRIPTION" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(1) }
            ,{ new Integer(Types.INTEGER), new Integer(13) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(13), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(Contains), new Integer(1), new Integer[] {1,2,3,4,5,6,7,8,9,10,11,12,13} } 
            ,{ new Integer(Ordered), new Integer(2), new String[] {null,null,null,null,null,null,"First1","First11","First13","First3","First5","First7","First9"} } 
           }
          ,{//getRoutingParameter inputs
             { new Integer(byrange), new Integer(1), true, new Integer(13), true, 3 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
           }
         } //21
        ,{ {{"select ID, DESCRIPTION from TESTTABLE where (ID = ? or ID = ? or ID = ?) AND ID >= 0 Order By DESCRIPTION DESC NULLS FIRST" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(5) }
            ,{ new Integer(Types.INTEGER), new Integer(6) }
            ,{ new Integer(Types.INTEGER), new Integer(7) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(3), new Integer(1), new Integer(2) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(Contains), new Integer(1), new Integer[] {5,6,7} } 
            ,{ new Integer(Ordered), new Integer(2), new String[] {null,"First7","First5"} } 
           }
          ,{//getRoutingParameter inputs
            //TODO soubhik need to convert this into List
             { new Integer(byrange), new Integer(5), true, new Integer(7), true, 1 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
           }
         } //22
        ,{ {{"select ID, DESCRIPTION from TESTTABLE where ID IN (?,?,?) AND ID >= 0 Order By DESCRIPTION DESC NULLS LAST" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(5) }
            ,{ new Integer(Types.INTEGER), new Integer(6) }
            ,{ new Integer(Types.INTEGER), new Integer(7) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(3), new Integer(1), new Integer(2) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(Contains), new Integer(1), new Integer[] {5,6,7} } 
            ,{ new Integer(Ordered), new Integer(2), new String[] {"First7","First5",null} } 
           }
          ,{//getRoutingParameter inputs
            //TODO soubhik need to convert this into List
             { new Integer(byrange), new Integer(1), true, new Integer(3), true, 1 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
           }
         } //23
        ,{ {{"select discount, quantity+discount, quantity from testtable where ID IN (?,?,?) AND ID between 0 and 14 Order By discount+quantity" }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(5) }
            ,{ new Integer(Types.INTEGER), new Integer(6) }
            ,{ new Integer(Types.INTEGER), new Integer(7) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(3), new Integer(1), new Integer(2) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(Contains), new Integer(1), new Integer[] {45,54,63} } 
            ,{ new Integer(Ordered), new Integer(2), new Long[] {545L,654L,763L} } 
            ,{ new Integer(Ordered), new Integer(3), new Long[] {500L,600L,700L} } 
           }
          ,{//getRoutingParameter inputs
             { new Integer(noCheck) }
           }
         } //24
        ,{ {{"select discount, quantity+discount, quantity from testtable where ID IN (?,?,?) AND ID between 0 and 14 FETCH FIRST ROW ONLY " }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(5) }
            ,{ new Integer(Types.INTEGER), new Integer(6) }
            ,{ new Integer(Types.INTEGER), new Integer(7) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(1), new Integer(1), new Integer(2) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(Contains), new Integer(1), new Integer[] {45,54,63} } 
            ,{ new Integer(Contains), new Integer(2), new Long[] {545L,654L,763L} } 
            ,{ new Integer(Contains), new Integer(3), new Long[] {500L,600L,700L} } 
           }
          ,{//getRoutingParameter inputs
             { new Integer(noCheck) }
           }
         } //25
        ,{ {{"select ID, DESCRIPTION from TESTTABLE where ID >= 0 Order By DESCRIPTION DESC NULLS LAST FETCH FIRST ROW ONLY " }}
          ,{//input parameters
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(1), new Integer(1), new Integer(2) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(Ordered), new Integer(2), new String[] {"First9"} } 
           }
          ,{//getRoutingParameter inputs
             { new Integer(noCheck) }
           }
         } //26
        ,{ {{"select ADDRESS as Alias3, COUNTRY as Alias2, ID as Alias1 from TESTTABLE where ID < ? Order By Alias3 ASC, Alias1 ASC NULLS LAST FETCH FIRST 3 ROWS ONLY " }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(10) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(3), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(Ordered), new Integer(1), new String[] {"J 604","J 604","J 987"} }
            ,{ new Integer(Ordered), new Integer(3), new Integer[] {3,9,1} }
           }
          ,{//getRoutingParameter inputs
             { new Integer(noCheck) }
           }
         } //27
        ,{ {{ "select ID, COUNTRY, ADDRESS from TESTTABLE where ID  >= ? And ID <= ? Order By 2 ASC, 1 DESC OFFSET 3 ROWS " }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(1) }
            ,{ new Integer(Types.INTEGER), new Integer(6) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(3), new Integer(2), new Integer(1) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(Ordered), new Integer(2), new String[] {"ROWLD","US","US"} }
            ,{ new Integer(Ordered), new Integer(1), new Integer[] {1,6,3} }
           }
          ,{//getRoutingParameter inputs
             { new Integer(byrange), new Integer(1), true, new Integer(6), true, 2 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
           }
        } //28
       ,{ {{"select ID, COUNTRY, ADDRESS from TESTTABLE where ID  >= ? And ID <= ? Order By 2 ASC, 1 DESC OFFSET 4 ROWS FETCH NEXT ROW ONLY " }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(1) }
            ,{ new Integer(Types.INTEGER), new Integer(6) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(1), new Integer(2), new Integer(1) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(Ordered), new Integer(2), new String[] {"US"} }
            ,{ new Integer(Ordered), new Integer(1), new Integer[] {6} }
           }
          ,{//getRoutingParameter inputs
             { new Integer(byrange), new Integer(1), true, new Integer(6), true, 2 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
           }
        } //29
       ,{ {{"select ID, COUNTRY, ADDRESS from TESTTABLE where ID  >= ? And ID <= ? Order By 2 ASC, 1 DESC OFFSET 5 ROWS FETCH NEXT 2 ROWS ONLY " }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(1) }
            ,{ new Integer(Types.INTEGER), new Integer(6) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(1), new Integer(2), new Integer(1) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(Ordered), new Integer(2), new String[] {"US"} }
            ,{ new Integer(Ordered), new Integer(1), new Integer[] {3} }
           }
          ,{//getRoutingParameter inputs
             { new Integer(byrange), new Integer(1), true, new Integer(6), true, 2 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
           }
        } //30
       ,{ {{"select ID, COUNTRY, ADDRESS from TESTTABLE where ID  >= ? And ID <= ? Order By 2 ASC, 1 DESC OFFSET 3 ROWS FETCH NEXT 2 ROWS ONLY " }}
         ,{//input parameters
            { new Integer(Types.INTEGER), new Integer(1) }
           ,{ new Integer(Types.INTEGER), new Integer(6) }
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(2), new Integer(2), new Integer(1) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(Ordered), new Integer(2), new String[] {"ROWLD","US"} }
           ,{ new Integer(Ordered), new Integer(1), new Integer[] {1,6} }
          }
         ,{//getRoutingParameter inputs
            { new Integer(byrange), new Integer(1), true, new Integer(6), true, 2 } //getRoutingFromXXX,lowerBound,lboundinclusive,upperBound,uboundinclusive,prunedNodes
          }
       } //31
  };

  /**
   * Tests different combinations of Ordered clause
   * on range partitioned Table. 
   * 
   */
  public void testOrderByClause_1()
      throws Exception {
    startVMs(2, 3);
    
    Double dblPrice = Random.class.newInstance().nextDouble();
    this.clientVMs.get(1).invoke(this.getClass(), "prepareTable", new Object[] {dblPrice, false, false} );
    
    Object queries[][][][] = orderbyQueries;
    
    if (testRunForReplicatedTablesOnly) {
      queries = new Object[27][][][];
      for (int i = 0; i < 27; i++) {
        queries[i] = orderbyQueries[i];
      }
    }
    
      try {
        
        VM[] vms = new VM[] {this.serverVMs.get(0), this.serverVMs.get(1), this.serverVMs.get(2)};
        QueryEvaluationHelper.evaluate4DQueryArray(queries, vms, this);
        
      } finally {
        GemFireXDQueryObserverHolder
        .setInstance(new GemFireXDQueryObserverAdapter());
        //invokeInEveryVM(QueryEvaluationHelper.class, "reset");

        this.clientVMs.get(1).invoke(this.getClass(), "clearTables", new Object[] {false, false} );
        TestUtil.clearStatementCache();
     }
  } //end of OrderByTest_1

  /**
   * Tests different combinations of Ordered clause on range partitioned Table
   * with single server VM.
   */
  public void testOrderByClause_1_1() throws Exception {
    startVMs(2, 1);

    Double dblPrice = Random.class.newInstance().nextDouble();
    this.clientVMs.get(1).invoke(this.getClass(), "prepareTable",
        new Object[] { dblPrice, false, false });

    Object queries[][][][] = orderbyQueries;
    
    if (testRunForReplicatedTablesOnly) {
      queries = new Object[27][][][];
      for (int i = 0; i < 27; i++) {
        queries[i] = orderbyQueries[i];
      }
    }

    try {

      VM[] vms = new VM[] { this.serverVMs.get(0) };
      QueryEvaluationHelper.evaluate4DQueryArray(queries, vms, this);

    } finally {
      GemFireXDQueryObserverHolder.setInstance(new GemFireXDQueryObserverAdapter());
      //invokeInEveryVM(QueryEvaluationHelper.class, "reset");

      this.clientVMs.get(1).invoke(this.getClass(), "clearTables",
          new Object[] { false, false });
      TestUtil.clearStatementCache();
    }

  } // end of OrderByTest_1_1

  /**
   * Tests Distinct projections
   * on range partitioned Table. 
   * 
   */
  public void testDistinctProjection_1() throws Exception {
    startVMs(2, 3);

    Double dblPrice = Random.class.newInstance().nextDouble();
    this.clientVMs.get(1).invoke(this.getClass(), "prepareTable",
        new Object[] { dblPrice, false, false });

    try {
      VM[] servervms = new VM[] { this.serverVMs.get(0), this.serverVMs.get(1),
          this.serverVMs.get(2) };
      VM[] clientvms = new VM[] { this.clientVMs.get(0), this.clientVMs.get(1) };
      invokeDistinctQueries(servervms, clientvms);
    } finally {
      this.clientVMs.get(1).invoke(this.getClass(), "clearTables",
          new Object[] { false, false });
      TestUtil.clearStatementCache();
    }
  }

  public void testDistinctProjection_2() throws Exception {
    startVMs(2, 3);

    Double dblPrice = Random.class.newInstance().nextDouble();
    this.clientVMs.get(1).invoke(this.getClass(), "prepareTable",
        new Object[] { dblPrice, true, false });

    try {
      this.serverVMs.get(1).invoke(
          this,
          "invokeDistinctQueries",
          new Object[] {
              new VM[] { this.serverVMs.get(0), this.serverVMs.get(1),
                  this.serverVMs.get(2) },
              new VM[] { this.clientVMs.get(0), this.clientVMs.get(1) } });
    } finally {
      this.clientVMs.get(1).invoke(this.getClass(), "clearTables",
          new Object[] { true, false });
      TestUtil.clearStatementCache();
    }
  }

  /** on a single server VM */
  public void testDistinctProjection_1_1() throws Exception {
    startVMs(2, 1);

    Double dblPrice = Random.class.newInstance().nextDouble();
    this.clientVMs.get(1).invoke(this.getClass(), "prepareTable",
        new Object[] { dblPrice, false, false });

    try {
      VM[] servervms = new VM[] { this.serverVMs.get(0) };
      VM[] clientvms = new VM[] { this.clientVMs.get(0), this.clientVMs.get(1) };
      invokeDistinctQueries(servervms, clientvms);
    } finally {
      this.clientVMs.get(1).invoke(this.getClass(), "clearTables",
          new Object[] { false, false });
      TestUtil.clearStatementCache();
    }
  }

  /** on a single server VM */
  public void testDistinctProjection_2_1() throws Exception {
    startVMs(2, 1);

    Double dblPrice = Random.class.newInstance().nextDouble();
    this.clientVMs.get(1).invoke(this.getClass(), "prepareTable",
        new Object[] { dblPrice, true, false });

    try {
      this.serverVMs.get(0).invoke(
          this,
          "invokeDistinctQueries",
          new Object[] {
              new VM[] { this.serverVMs.get(0) },
              new VM[] { this.clientVMs.get(0), this.clientVMs.get(1) } });
    } finally {
      this.clientVMs.get(1).invoke(this.getClass(), "clearTables",
          new Object[] { true, false });
      TestUtil.clearStatementCache();
    }
  }

  public void invokeDistinctQueries(VM[] servervms, VM[] clientvms) 
    throws Exception {

    Object queries[][][][] = { 
         { {{ "select distinct DESCRIPTION from TESTTABLE" }}
          ,{//input parameters
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(8), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(TimesOccur), new Integer(1), new String[] {"First1","First11","First13","First3","First5","First7","First9", null}
                                                      , new Integer[] {1,1,1,1,1,1,1,1} } 
           }
          ,{//getRoutingParameter inputs
             { new Integer(noCheck) }
           }
         } //1
        ,{ {{ "select distinct ADDRESS from TESTTABLE" }}
           ,{//input parameters
            }
           ,{//output (atleast 1 row for Nodes reconciliation)
              { new Integer(3), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
             ,{ new Integer(TimesOccur), new Integer(1), new String[] {"J 604", "J 987", null}, new Integer[] {1,1,1}  } 
            }
           ,{//getRoutingParameter inputs
              { new Integer(noCheck) }
            }
          } //2
         ,{ {{ "select distinct ID from TESTTABLE" }}
            ,{//input parameters
             }
            ,{//output (atleast 1 row for Nodes reconciliation)
               { new Integer(13), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
              ,{ new Integer(TimesOccur), new Integer(1), new Integer[] {1,2,3,4,5,6,7,8,9,10,11,12,13}, new Integer[] {1,1,1,1,1,1,1,1,1,1,1,1,1} } 
             }
            ,{//getRoutingParameter inputs
               { new Integer(noCheck) }
             }
           } //3
         ,{ {{ "select distinct * from TESTTABLE" }}
           ,{//input parameters
            }
           ,{//output (atleast 1 row for Nodes reconciliation)
              { new Integer(13), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
             ,{ new Integer(TimesOccur), new Integer(1), new Integer[] {1,2,3,4,5,6,7,8,9,10,11,12,13}, new Integer[] {1,1,1,1,1,1,1,1,1,1,1,1,1} } 
            }
           ,{//getRoutingParameter inputs
              { new Integer(noCheck) }
            }
          } //4
         ,{ {{ "select distinct ID from TESTTABLE order by ID" }}
           ,{//input parameters
            }
           ,{//output (atleast 1 row for Nodes reconciliation)
              { new Integer(13), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
             ,{ new Integer(Ordered), new Integer(1), new Integer[] {1,2,3,4,5,6,7,8,9,10,11,12,13} } 
            }
           ,{//getRoutingParameter inputs
              { new Integer(noCheck) }
            }
          } //5
         ,{ {{ "select distinct * from TESTTABLE order by ID" }}
           ,{//input parameters
            }
           ,{//output (atleast 1 row for Nodes reconciliation)
              { new Integer(13), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
             ,{ new Integer(Ordered), new Integer(1), new Integer[] {1,2,3,4,5,6,7,8,9,10,11,12,13} } 
            }
           ,{//getRoutingParameter inputs
              { new Integer(noCheck) }
            }
          } //6
         ,{ {{ "select distinct description, id from TESTTABLE order by ID" }}
           ,{//input parameters
            }
           ,{//output (atleast 1 row for Nodes reconciliation)
              { new Integer(13), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
             ,{ new Integer(Ordered), new Integer(1), new String[] {"First1",null,"First3",null,"First5",null,"First7",null,"First9",null,
                                                                    "First11",null,"First13" } } 
             ,{ new Integer(Ordered), new Integer(2), new Integer[] {1,2,3,4,5,6,7,8,9,10,11,12,13} } 
            }
           ,{//getRoutingParameter inputs
              { new Integer(noCheck) }
            }
          } //7
         ,{ {{ "select distinct address, country from TESTTABLE" }}
           ,{//input parameters
            }
           ,{//output (atleast 1 row for Nodes reconciliation)
              { new Integer(6), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
             ,{ new Integer(Contains), new Integer(1), new String[] {"J 604", "J 987", null} } 
             ,{ new Integer(Contains), new Integer(2), new String[] {"INDIA", "US", "ROWLD"} } 
            }
           ,{//getRoutingParameter inputs
              { new Integer(noCheck) }
            }
          } //8
         ,{ {{ "select distinct address, country from TESTTABLE order by country" }}
           ,{//input parameters
            }
           ,{//output (atleast 1 row for Nodes reconciliation)
              { new Integer(6), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
             ,{ new Integer(Contains), new Integer(1), new String[] {"J 604", "J 987", null} } 
             ,{ new Integer(Ordered), new Integer(2), new String[] {"INDIA","INDIA", "ROWLD", "ROWLD", "US", "US" } } 
            }
           ,{//getRoutingParameter inputs
              { new Integer(noCheck) }
            }
          } //9
         ,{ {{ "select distinct country, address from TESTTABLE order by address nulls first" }}
           ,{//input parameters
            }
           ,{//output (atleast 1 row for Nodes reconciliation)
              { new Integer(6), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
             ,{ new Integer(Contains), new Integer(1), new String[] {"INDIA","ROWLD","US" } } 
             ,{ new Integer(Ordered), new Integer(2), new String[] {null,null,null,"J 604","J 987","J 987"} } 
            }
           ,{//getRoutingParameter inputs
              { new Integer(noCheck) }
            }
          } //10
         ,{ {{ "select distinct * from TESTTABLE where SRC > 1" }}
           ,{//input parameters
            }
           ,{//output (atleast 1 row for Nodes reconciliation)
              { new Integer(11), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
             ,{ new Integer(TimesOccur), new Integer(1), new Integer[] {1,2,3,4,6,7,8,9,11,12,13}, new Integer[] {1,1,1,1,1,1,1,1,1,1,1} } 
            }
           ,{//getRoutingParameter inputs
              { new Integer(noCheck) }
            }
          } //11
         ,{ {{ "select distinct * from TESTTABLE where SRC > 1 order by address desc nulls first, id desc" }}
           ,{//input parameters
            }
           ,{//output (atleast 1 row for Nodes reconciliation)
              { new Integer(11), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
             ,{ new Integer(TimesOccur), new Integer(1), new Integer[] {12,8,6,13,11,7,4,2,1,9,3}, new Integer[] {1,1,1,1,1,1,1,1,1,1,1} }
             ,{ new Integer(Ordered)   , new Integer(2), new String[] {null,null,null,"First13","First11","First7",null,null,"First1","First9","First3"} }
            }
           ,{//getRoutingParameter inputs
              { new Integer(noCheck) }
            }
          } //12
    };

      try {

        getLogWriter().info("number of client VMs : " + clientvms.length);

        QueryEvaluationHelper
          .setObserver ( clientvms, "Client", "", 
              new GemFireXDQueryObserverAdapter() {
                  @Override
                  public void createdGemFireXDResultSet(ResultSet rs) {
                    if( ! (rs instanceof GemFireDistributedResultSet) )
                      throw new GemFireXDRuntimeException("didn't received GemfireResultSet");
                    
                    GemFireDistributedResultSet gfrs = (GemFireDistributedResultSet) rs;
                    
                    if( ! gfrs.isIterator( GemFireDistributedResultSet.ORDERED |  
                                        GemFireDistributedResultSet.DISTINCT|
                                        GemFireDistributedResultSet.FETCH_EARLY
                       ) ) {
                      throw new GemFireXDRuntimeException("Wrong Iterator Acquired by GemfireDistributedResultSet");
                    }
                  };
              } 
          );

        QueryEvaluationHelper.evaluate4DQueryArray(queries, servervms, this);

      } finally {
        GemFireXDQueryObserverHolder
        .setInstance(new GemFireXDQueryObserverAdapter());
        //invokeInEveryVM(QueryEvaluationHelper.class, "reset");         
     }
  } //end of Distinct

  public void testScalarAggregates_1()
    throws Exception {
    startVMs(2, 3);
    
    Double dblPrice = Random.class.newInstance().nextDouble();
    this.clientVMs.get(1).invoke(this.getClass(), "prepareTable", new Object[] {dblPrice, true, false} );
    
    try {
      VM[] servervms = new VM[] {this.serverVMs.get(0), this.serverVMs.get(1), this.serverVMs.get(2)};
      invokeScalarAggregates(servervms, dblPrice);
    }
    finally {
      this.clientVMs.get(1).invoke(this.getClass(), "clearTables", new Object[] {true, false} );
      TestUtil.clearStatementCache();
    }
    
  }
  
  public void testScalarAggregates_2() throws Exception {
    startVMs(2, 3);
    
    Double dblPrice = Random.class.newInstance().nextDouble();
    this.clientVMs.get(1).invoke(this.getClass(), "prepareTable", new Object[] {dblPrice, true, false} );

    try {
      this.serverVMs.get(1).invoke(
          this,
          "invokeScalarAggregates",
          new Object[] { new VM[] { this.serverVMs.get(0),
              this.serverVMs.get(1), this.serverVMs.get(2) }, dblPrice });
    } finally {
      this.clientVMs.get(1).invoke(this.getClass(), "clearTables", new Object[] { true, false });
      TestUtil.clearStatementCache();
    }
  }

  public void invokeScalarAggregates(VM[] servervms, Double dblPrice) 
  throws Exception {
    
    Object queries[][][][] = { 
    { {{ "select count(*) from TESTTABLE" }}
     ,{//input parameters
      }
     ,{//output (atleast 1 row for Nodes reconciliation)
        { new Integer(1), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
       ,{ new Integer(Contains), new Integer(1), new Integer[]{13} } 
      }
     ,{//getRoutingParameter inputs
        { new Integer(noCheck) }
      }
    } //1
   ,{ {{ "select count(1) from TESTTABLE where id >= 10" }}
     ,{//input parameters
      }
     ,{//output (atleast 1 row for Nodes reconciliation)
        { new Integer(1), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
       ,{ new Integer(Contains), new Integer(1), new Integer[]{4} } 
      }
     ,{//getRoutingParameter inputs
        { new Integer(noCheck) }
      }
    } //2
   ,{ {{ "select max(ALL id), min(id), sum(id), sum(id/2), sum(cast(id/2 as REAL)) from TESTTABLE where id <= 13" }}
     ,{//input parameters
      }
     ,{//output (atleast 1 row for Nodes reconciliation)
        { new Integer(1), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
       ,{ new Integer(Contains), new Integer(1), new Integer[]{13} } 
       ,{ new Integer(Contains), new Integer(2), new Integer[]{1} } 
       ,{ new Integer(Contains), new Integer(3), new Integer[]{91} } 
       ,{ new Integer(Contains), new Integer(4), new Integer[]{42} } 
       ,{ new Integer(Contains), new Integer(5), new Float[]{42.0F} } 
      }
     ,{//getRoutingParameter inputs
        { new Integer(noCheck) }
      }
    } //3
   ,{ {{ new Integer(fixImmediate), "select min(id), max(id), count(1), sum(id) from TESTTABLE where id = 13" }}
     ,{//input parameters
      }
     ,{//output (atleast 1 row for Nodes reconciliation)
        { new Integer(1), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
       ,{ new Integer(Contains), new Integer(1), new Integer[]{13} } 
       ,{ new Integer(Contains), new Integer(2), new Integer[]{13} } 
       ,{ new Integer(Contains), new Integer(3), new Integer[]{1}  } 
       ,{ new Integer(Contains), new Integer(4), new Integer[]{13} } 
      }
     ,{//getRoutingParameter inputs
        { new Integer(noCheck) }
      }
    } //4
   ,{ {{ "select avg(testtable.quantity), avg(testtable.price), max(testtable.price), avg(ALL testtable.amount), " +
   		"min(ALL testtable.price), count(price), sum(ALL price) from TESTTABLE" }}
     ,{//input parameters
      }
     ,{//output (atleast 1 row for Nodes reconciliation)
        { new Integer(1), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
       ,{ new Integer(Ordered), new Integer(1), new Long[]{700L} } 
       ,{ new Integer(Ordered), new Integer(2), new Double[]{ (dblPrice*13D)/13D } } 
       ,{ new Integer(Ordered), new Integer(3), new Double[]{ dblPrice } } 
       ,{ new Integer(Ordered), new Integer(4), new Double[]{ 24.150002 } } 
       ,{ new Integer(Ordered), new Integer(5), new Double[]{ dblPrice } } 
       ,{ new Integer(Ordered), new Integer(6), new Integer[]{ 13 } } 
       ,{ new Integer(Ordered), new Integer(7), new Double[]{ (dblPrice*13D) } } 
      }
     ,{//getRoutingParameter inputs
        { new Integer(noCheck) }
      }
    } //5
   ,{ {{ "select avg(quantity*discount), avg(cast((quantity*discount) as REAL)), avg(amount*multiply), avg(amount*discount) from TESTTABLE" }}
     ,{//input parameters
      }
     ,{//output (atleast 1 row for Nodes reconciliation)
        { new Integer(1), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
       ,{ new Integer(Ordered), new Integer(1), new Long[]{ 56700L } } 
       ,{ new Integer(Ordered), new Integer(2), new Double[]{ 56700D } } 
       ,{ new Integer(Ordered), new Integer(3), new Double[]{ 241.197165228617D } } 
       ,{ new Integer(Ordered), new Integer(4), new Double[]{ 1956.1501 } }
      }
     ,{//getRoutingParameter inputs
        { new Integer(noCheck) }
      }
    } //6
   ,{ {{ "select avg(quantity+amount*discount-multiply/7), sum(quantity+amount*discount-multiply/7), " +
   		"max(quantity+amount*discount-multiply/7), min(quantity+amount*discount-multiply/7)," +
   		"avg(int_overflow+amount/discount), sum(int_overflow+amount/discount) from TESTTABLE" }}
     ,{//input parameters
      }
     ,{//output (atleast 1 row for Nodes reconciliation)
        { new Integer(1), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
       ,{ new Integer(Ordered), new Integer(1), new Double[]{ 2655.00897395856D } } 
       ,{ new Integer(Ordered), new Integer(2), new Double[]{ 34515.116661461354D } } 
       ,{ new Integer(Ordered), new Integer(3), new Double[]{ 6545.451986741072D } } 
       ,{ new Integer(Ordered), new Integer(4), new Double[]{ 130.76608019461494D } } 
       ,{ new Integer(Ordered), new Integer(5), new Double[]{ 2.147483648e9 } } 
       ,{ new Integer(Ordered), new Integer(6), new Float[]{ 2.79172874e10F } } 
      }
     ,{//getRoutingParameter inputs
        { new Integer(noCheck) }
      }
    } //7
   ,{ {{ "select sum(cast(int_overflow as BIGINT)), avg(cast(int_overflow as BIGINT)), " +
   		"sum(cast(int_overflow as DOUBLE)), avg(cast(int_overflow as DOUBLE)), " +
   		"sum(cast(int_overflow as REAL)), avg(cast(int_overflow as REAL)) from TESTTABLE" }}
     ,{//input parameters
      }
     ,{//output (atleast 1 row for Nodes reconciliation)
        { new Integer(1), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
       ,{ new Integer(Contains), new Integer(1), new Long[]{ 27917287411L } } 
       ,{ new Integer(Ordered), new Integer(2), new Long[]{ 2147483647L } } 
       ,{ new Integer(Contains), new Integer(3), new Double[]{ 27917287411D } } 
       ,{ new Integer(Ordered), new Integer(4), new Double[]{ 2.147483647e9 } } 
       ,{ new Integer(Contains), new Integer(5), new Float[]{ 27917287411F } } 
       ,{ new Integer(Ordered), new Integer(6), new Double[]{ 2.147483647e9D } } 
      }
     ,{//getRoutingParameter inputs
        { new Integer(noCheck) }
      }
    } //8
   ,{ {{ "select sum(cast(quantity as BIGINT))/count(quantity), avg(cast(quantity as BIGINT)) from TESTTABLE" }}
     ,{//input parameters
      }
     ,{//output (atleast 1 row for Nodes reconciliation)
        { new Integer(1), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
        ,{ new Integer(Ordered), new Integer(1), new Long[]{ 700L } } 
        ,{ new Integer(Ordered), new Integer(2), new Long[]{ 700L } } 
      }
     ,{//getRoutingParameter inputs
        { new Integer(noCheck) }
      }
    } //9
   ,{ {{ "select cast( avg(amount) as REAL), cast( sum(amount) as REAL), count(discount), sum(amount)/max(discount) from TESTTABLE" }}
      ,{//input parameters
       }
      ,{//output (atleast 1 row for Nodes reconciliation)
         { new Integer(1), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
         ,{ new Integer(Ordered), new Integer(1), new Float[]{ 24.149998F } } 
         ,{ new Integer(Ordered), new Integer(2), new Float[]{ 313.94998F } } 
         ,{ new Integer(Ordered), new Integer(3), new Integer[]{ 13 } } 
         ,{ new Integer(Ordered), new Integer(4), new Float[]{ 2.6833332F } } 
         
       }
      ,{//getRoutingParameter inputs
         { new Integer(noCheck) }
       }
    } //10
   ,{ {{ "select avg(amount)/max(discount), sum(amount)/count(amount)/max(discount), sum(amount)/(cast(count(amount) as REAL)/max(discount)) from TESTTABLE" }}
      ,{//input parameters
       }
      ,{//output (atleast 1 row for Nodes reconciliation)
         { new Integer(1), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
         ,{ new Integer(Ordered), new Integer(1), new Double[]{ 0.20641024 } } 
         ,{ new Integer(Ordered), new Integer(2), new Float[]{ 0.20641024F } } 
         ,{ new Integer(Ordered), new Integer(3), new Float[]{ 2825.5498F } } 
         
       }
      ,{//getRoutingParameter inputs
         { new Integer(noCheck) }
       }
    } //11
   ,{ {{ "select case when sum(amount) > 0 then cast( sum(cast( INT_OVERFLOW as DOUBLE)) as REAL) else 1 end, count(discount) from TESTTABLE" }}
      ,{//input parameters
       }
      ,{//output (atleast 1 row for Nodes reconciliation)
         { new Integer(1), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
        ,{ new Integer(Ordered), new Integer(1), new Double[]{ 2.7917287424E10D } } // case when will cast additionally to Double. 
        ,{ new Integer(Ordered), new Integer(2), new Integer[]{ 13 } } 
       }
      ,{//getRoutingParameter inputs
         { new Integer(noCheck) }
       }
    } //12
   ,{ {{ "select sum(ID/?), avg(quantity * ?/amount) from testtable" }}
      ,{//input parameters
         { new Integer(Types.INTEGER), new Integer(1) }
        ,{ new Integer(Types.INTEGER), new Integer(2) }
       }
      ,{//output (atleast 1 row for Nodes reconciliation)
         { new Integer(1), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
        ,{ new Integer(Ordered), new Integer(1), new Integer[]{ 91 } } 
        ,{ new Integer(Ordered), new Integer(2), new Double[]{ 57.971012D } } 
       }
      ,{//getRoutingParameter inputs
         { new Integer(noCheck) }
       }
    } //13
   ,{ {{ "select sum((id/?+discount*?)- -?+1-1)+?, sum( ((id/cast(? as REAL))+discount*?)- -?+1-1) + ? from testtable" }}
       ,{//input parameters
          { new Integer(Types.FLOAT)  , new Float  (5) }
         ,{ new Integer(Types.INTEGER), new Integer(2) }
         ,{ new Integer(Types.INTEGER), new Integer(5) }
         ,{ new Integer(Types.INTEGER), new Integer(6) }

         ,{ new Integer(Types.INTEGER), new Integer(5) }
         ,{ new Integer(Types.INTEGER), new Integer(2) }
         ,{ new Integer(Types.INTEGER), new Integer(5) }
         ,{ new Integer(Types.INTEGER), new Integer(6) }
        }
       ,{//output (atleast 1 row for Nodes reconciliation)
          { new Integer(1), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
         ,{ new Integer(Ordered), new Integer(1), new Integer[]{ 1722 } }  
         ,{ new Integer(Ordered), new Integer(2), new Float[]{ 1727.2F } } 
        }
       ,{//getRoutingParameter inputs
          { new Integer(noCheck) }
        }
     } //14
    ,{ {{ "select (max(INT_OVERFLOW)-cast(? as DOUBLE))/avg( ((id/?)+discount*?)- -?+1-1) + ? from testtable" }}
       ,{//input parameters
          { new Integer(Types.DOUBLE) , new Double (5.98D) }
         ,{ new Integer(Types.INTEGER), new Integer(99) }
         ,{ new Integer(Types.INTEGER), new Integer(2) }
         ,{ new Integer(Types.INTEGER), new Integer(5) }
         ,{ new Integer(Types.INTEGER), new Integer(6) }
        }
       ,{//output (atleast 1 row for Nodes reconciliation)
          { new Integer(1), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
         ,{ new Integer(Ordered), new Integer(1), new Double[]{ 1.6393010893282443e7 } }  
        }
       ,{//getRoutingParameter inputs
          { new Integer(noCheck) }
        }
     } //15
    ,{ {{ "select sum(times(id)+?), sum(multiply + length(description) /  (times(discount)*?) ) from testtable" }}
       ,{//input parameters
          { new Integer(Types.INTEGER), new Integer(2) }
         ,{ new Integer(Types.INTEGER), new Integer(5) }
        }
       ,{//output (atleast 1 row for Nodes reconciliation)
          { new Integer(1), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
         ,{ new Integer(Ordered), new Integer(1), new Integer[]{ 208 } } 
         ,{ new Integer(Ordered), new Integer(2), new Double[]{ 55.91222D } }  
        }
       ,{//getRoutingParameter inputs
          { new Integer(noCheck) }
        }
     } //16
    ,{ {{ "select avg(distinct quantity) from testtable" }}
       ,{//input parameters
        }
       ,{//output (atleast 1 row for Nodes reconciliation)
          { new Integer(1), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
         ,{ new Integer(Ordered), new Integer(1), new Long[]{ 700L } } 
        }
       ,{//getRoutingParameter inputs
          { new Integer(noCheck) }
        }
     } //17
    ,{ {{ "select count(distinct quantity) from testtable" }}
       ,{//input parameters
        }
       ,{//output (atleast 1 row for Nodes reconciliation)
          { new Integer(1), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
         ,{ new Integer(Ordered), new Integer(1), new Integer[]{ 13 } } 
        }
       ,{//getRoutingParameter inputs
          { new Integer(noCheck) }
        }
     } //18
    ,{ {{ "select sum(src), sum(distinct src) from testtable where src < ?" }}
       ,{//input parameters
          { new Integer(Types.INTEGER), new Integer(4) }
        }
       ,{//output (atleast 1 row for Nodes reconciliation)
          { new Integer(1), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
         ,{ new Integer(Ordered), new Integer(1), new Integer[]{ 17 } } 
         ,{ new Integer(Ordered), new Integer(2), new Integer[]{ 6  } } 
        }
       ,{//getRoutingParameter inputs
          { new Integer(noCheck) }
        }
     } //19
    ,{ {{ "select sum(src)/ avg(distinct cast(src2 as DECIMAL(4)) ) from testtable" }}
      ,{//input parameters
       }
      ,{//output (atleast 1 row for Nodes reconciliation)
         { new Integer(1), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
        ,{ new Integer(Ordered), new Integer(1), new BigDecimal[]{ new BigDecimal(26).setScale(14) } } 
       }
      ,{//getRoutingParameter inputs
           { new Integer(noCheck) }
         }
     } //20
   };
  
    try {
      
      QueryEvaluationHelper.evaluate4DQueryArray(queries, servervms, this);
    
    } finally {
      GemFireXDQueryObserverHolder
      .setInstance(new GemFireXDQueryObserverAdapter());
      //invokeInEveryVM(QueryEvaluationHelper.class, "reset");
    }
  } // end of ScalarAggregates

  /**
   * Tests GroupBy projections
   * on range partitioned Table. 
   * 
   */
  public void testGroupByClause_1()
      throws Exception {
    startVMs(2, 3);

    Double dblPrice = Random.class.newInstance().nextDouble();
    this.clientVMs.get(1).invoke(this.getClass(), "prepareTable",
        new Object[] { dblPrice, false, false });

    VM[] serverVMs = new VM[] { this.serverVMs.get(0), this.serverVMs.get(1),
        this.serverVMs.get(2) };

    try {
      invokeGroupByQueries(serverVMs);
    }
    finally {
      this.clientVMs.get(1).invoke(this.getClass(), "clearTables", new Object[] {false, false} );
      TestUtil.clearStatementCache();
    }
  }
  
  public void testGroupByClause_2()
      throws Exception {
    startVMs(2, 3);

    Double dblPrice = Random.class.newInstance().nextDouble();
    this.clientVMs.get(1).invoke(this.getClass(), "prepareTable", new Object[] {dblPrice, false, true} );

    VM[] serverVMs = new VM[] { this.serverVMs.get(0), this.serverVMs.get(1),
        this.serverVMs.get(2) };

    try {
      invokeGroupByQueries(serverVMs);
    }
    finally {
      this.clientVMs.get(1).invoke(this.getClass(), "clearTables", new Object[] {false, true} );
      TestUtil.clearStatementCache();
    }
  }

  /**
   * Tests GroupBy projections on range partitioned Table on single server VM.
   */
  public void testGroupByClause_1_1() throws Exception {
    startVMs(2, 1);

    Double dblPrice = Random.class.newInstance().nextDouble();
    this.clientVMs.get(1).invoke(this.getClass(), "prepareTable",
        new Object[] { dblPrice, false, false });

    VM[] serverVMs = new VM[] { this.serverVMs.get(0) };

    try {
      invokeGroupByQueries(serverVMs);
    } finally {
      this.clientVMs.get(1).invoke(this.getClass(), "clearTables",
          new Object[] { false, false });
      TestUtil.clearStatementCache();
    }
  }

  public void testGroupByClause_2_1() throws Exception {
    startVMs(2, 1);

    Double dblPrice = Random.class.newInstance().nextDouble();
    this.clientVMs.get(1).invoke(this.getClass(), "prepareTable",
        new Object[] { dblPrice, false, true });

    VM[] serverVMs = new VM[] { this.serverVMs.get(0) };

    try {
      invokeGroupByQueries(serverVMs);
    } finally {
      this.clientVMs.get(1).invoke(this.getClass(), "clearTables",
          new Object[] { false, true });
      TestUtil.clearStatementCache();
    }
  }

  private void invokeGroupByQueries(VM[] vms) throws Exception {
    Object queries[][][][] = { 
        { {{ "select id from TESTTABLE group by id" }}
          ,{//input parameters
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(13), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(TimesOccur), new Integer(1), new Integer[] {1,2,3,4,5,6,7,8,9,10,11,12,13}, new Integer[] {1,1,1,1,1,1,1,1,1,1,1,1,1} } 
           }
          ,{//getRoutingParameter inputs
             { new Integer(noCheck) }
           }
        } //1
       ,{ {{ "select COUNTRY from TESTTABLE group by COUNTRY" }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(3), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(Contains), new Integer(1), new String[] {"INDIA","ROWLD","US"} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //2
       ,{ {{ "select COUNTRY, ADDRESS from TESTTABLE group by COUNTRY, ADDRESS" }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(6), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(TimesOccur), new Integer(1), new String[] {"INDIA","ROWLD","US"}, new Integer[] {2,2,2} } 
           ,{ new Integer(TimesOccur), new Integer(2), new String[] {"J 987","J 604",null}, new Integer[] {2,1,3} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //3
       ,{ {{ "select ADDRESS, COUNTRY from TESTTABLE group by COUNTRY, ADDRESS" }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(6), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(TimesOccur), new Integer(1), new String[] {"J 987","J 604",null}, new Integer[] {2,1,3} } 
           ,{ new Integer(TimesOccur), new Integer(2), new String[] {"INDIA","ROWLD","US"}, new Integer[] {2,2,2} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //4
       ,{ {{ "select ADDRESS from TESTTABLE group by COUNTRY, ADDRESS" }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(6), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(TimesOccur), new Integer(1), new String[] {"J 987","J 604",null}, new Integer[] {2,1,3} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //5
       ,{ {{ "select COUNTRY from TESTTABLE group by COUNTRY, ADDRESS" }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(6), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(TimesOccur), new Integer(1), new String[] {"INDIA","ROWLD","US"}, new Integer[] {2,2,2} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //6
       ,{ {{ "select ADDRESS, COUNTRY from TESTTABLE group by ADDRESS, COUNTRY" }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(6), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(TimesOccur), new Integer(1), new String[] {"J 987","J 604",null}, new Integer[] {2,1,3} } 
           ,{ new Integer(TimesOccur), new Integer(2), new String[] {"INDIA","ROWLD","US"}, new Integer[] {2,2,2} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //7
       ,{ {{ "select COUNTRY, ADDRESS from TESTTABLE group by ADDRESS, COUNTRY" }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(6), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(TimesOccur), new Integer(1), new String[] {"INDIA","ROWLD","US"}, new Integer[] {2,2,2} } 
           ,{ new Integer(TimesOccur), new Integer(2), new String[] {"J 987","J 604",null}, new Integer[] {2,1,3} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //8
       ,{ {{ "select COUNTRY from TESTTABLE group by ADDRESS, COUNTRY" }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(6), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(TimesOccur), new Integer(1), new String[] {"INDIA","ROWLD","US"}, new Integer[] {2,2,2} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //9
       ,{ {{ "select ADDRESS from TESTTABLE group by ADDRESS, COUNTRY" }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(6), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(TimesOccur), new Integer(1), new String[] {"J 987","J 604",null}, new Integer[] {2,1,3} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //10
       ,{ {{ "select COUNTRY, ADDRESS from TESTTABLE group by COUNTRY, ADDRESS order by ADDRESS, COUNTRY" }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(6), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(Ordered), new Integer(1), new String[] {"US","INDIA","ROWLD","INDIA","ROWLD","US"} } 
           ,{ new Integer(Ordered), new Integer(2), new String[] {"J 604","J 987","J 987",null,null,null} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //11
       ,{ {{ "select address, country from TESTTABLE group by address, country order by country, address" }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(6), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(Ordered), new Integer(1), new String[] {"J 987",null,"J 987",null,"J 604",null} } 
           ,{ new Integer(Ordered), new Integer(2), new String[] {"INDIA","INDIA","ROWLD","ROWLD","US","US"} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //12
       ,{ {{ "select COUNTRY, ADDRESS from TESTTABLE group by COUNTRY, ADDRESS order by ADDRESS NULLS FIRST, COUNTRY" }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(6), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(Ordered), new Integer(1), new String[] {"INDIA","ROWLD","US","US","INDIA","ROWLD"} } 
           ,{ new Integer(Ordered), new Integer(2), new String[] {null,null,null,"J 604","J 987","J 987"} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //13
       ,{ {{ "select address, country from TESTTABLE group by COUNTRY, ADDRESS order by ADDRESS NULLS FIRST" }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(6), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(Ordered), new Integer(1), new String[] {null,null,null,"J 604","J 987","J 987"} } 
           ,{ new Integer(Contains), new Integer(2), new String[] {"INDIA","ROWLD","US","US","INDIA","ROWLD"} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //14
       ,{ {{ "select address, country from TESTTABLE group by address, country order by country, address nulls first" }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(6), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(Ordered), new Integer(1), new String[] {null,"J 987",null,"J 987",null,"J 604"} } 
           ,{ new Integer(Ordered), new Integer(2), new String[] {"INDIA","INDIA","ROWLD","ROWLD","US","US"} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //15
       ,{ {{"select case when address is null then 'NULL ADDR' else address end from testtable group by address having address is null" }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(1), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(Ordered), new Integer(1), new String[] {"NULL ADDR"} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //16
       ,{ {{ "select ADDRESS || '  ' || COUNTRY from TESTTABLE group by COUNTRY, ADDRESS having address is not null order by ADDRESS, COUNTRY" }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(3), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(Ordered), new Integer(1), new String[] {"J 604  US", "J 987  INDIA", "J 987  ROWLD"} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //17
       ,{ {{"select discount+quantity from testtable group by quantity, discount order by quantity+discount" }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(13), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(Ordered), new Integer(1), new Long[] {109L,218L,327L,436L,545L,654L,763L,872L,981L,1090L,1199L,1308L,1417L} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //18
       ,{ {{"select src+src2 from testtable group by src, src2" }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(10), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(Ordered), new Integer(1), new Integer[] {2,3,3,4,4,5,5,6,6,7} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //19
       ,{ {{"select src+src2 from testtable group by src+src2 having src+src2 > 1" }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(6), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(Ordered), new Integer(1), new Integer[] {2,3,4,5,6,7} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //20
       ,{ {{"select distinct src from testtable group by src, src2" }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(5), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(Ordered), new Integer(1), new Integer[] {1,2,3,4,5} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //21
       ,{ {{"select country from testtable group by country order by substr(country, 3, 4)" }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(3), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(Ordered), new Integer(1), new String[] {"US","INDIA","ROWLD",} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //22
       ,{ {{"select count(1), sum(ID), min(ID), max(id), substr(DESCRIPTION,2,length(DESCRIPTION)-2) from TESTTABLE Group By substr(DESCRIPTION,2,length(DESCRIPTION)-2)" }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(3), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(Ordered), new Integer(1), new Integer[] {5,2,6} }
           ,{ new Integer(Ordered), new Integer(2), new Integer[] {25,24,42} }
           ,{ new Integer(Ordered), new Integer(3), new Integer[] {1,11,2} }
           ,{ new Integer(Ordered), new Integer(4), new Integer[] {9,13,12} }
           ,{ new Integer(Ordered), new Integer(5), new String[] {"irst","irst1",null} }
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //23
       ,{ {{"select distinct address from testtable group by description, address order by 1 desc nulls last" }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(3), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(Ordered), new Integer(1), new String[] {"J 987","J 604",null} }
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //24
    };
    
      try {
        QueryEvaluationHelper.evaluate4DQueryArray(queries, vms, this);
        
      } finally {
        GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter());
        //invokeInEveryVM(QueryEvaluationHelper.class, "reset");         
     }
  } // end of GroupBy

  public void testGroupByAndAggregates_1()
      throws Exception {
    startVMs(2, 3);
    
    Double dblPrice = Random.class.newInstance().nextDouble();
    this.clientVMs.get(1).invoke(this.getClass(), "prepareTable", new Object[] {dblPrice, false, false} );

    try {
      invokeGroupByAndAggregatesQueries();
    }
    finally {
      this.clientVMs.get(1).invoke(this.getClass(), "clearTables", new Object[] {false, false} );
      TestUtil.clearStatementCache();
    }
    
  }

  public void testGroupByAndAggregates_2()
     throws Exception {
    startVMs(2, 3);
    
    Double dblPrice = Random.class.newInstance().nextDouble();
    this.clientVMs.get(1).invoke(this.getClass(), "prepareTable", new Object[] {dblPrice, false, true} );

    try {
      invokeGroupByAndAggregatesQueries();
    }
    finally {
      this.clientVMs.get(1).invoke(this.getClass(), "clearTables", new Object[] {false, true} );
      TestUtil.clearStatementCache();
    }
    
  }

  private void invokeGroupByAndAggregatesQueries()
     throws Exception
  {
    
    Object queries[][][][] = { 
         //aggregates with group by 
        { {{ "select COUNTRY, count(1) from TESTTABLE group by COUNTRY" }}
          ,{//input parameters
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(3), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(Ordered), new Integer(1), new String[] {"INDIA","ROWLD","US"} } 
            ,{ new Integer(Ordered), new Integer(2), new Integer[]{2,7,4} } 
           }
          ,{//getRoutingParameter inputs
             { new Integer(noCheck) }
           }
        } //2
       ,{ {{ "select COUNTRY, count(*), sum(id), min(id), max(id) from TESTTABLE group by COUNTRY order by 1" }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(3), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(Ordered), new Integer(1), new String[] {"INDIA","ROWLD","US"} } 
           ,{ new Integer(Ordered), new Integer(2), new Integer[]{2,7,4} } 
           ,{ new Integer(Ordered), new Integer(3), new Integer[]{15,46,30} } 
           ,{ new Integer(Ordered), new Integer(4), new Integer[]{5,1,3} } 
           ,{ new Integer(Ordered), new Integer(5), new Integer[]{10,13,12} }
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //3
       ,{ {{ "select COUNTRY, count(id), sum(id), min(id), max(id) from TESTTABLE group by COUNTRY order by 2,1" }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(3), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(Ordered), new Integer(1), new String[] {"INDIA","US","ROWLD"} } 
           ,{ new Integer(Ordered), new Integer(2), new Integer[]{2,4,7} } 
           ,{ new Integer(Ordered), new Integer(3), new Integer[]{15,30,46} } 
           ,{ new Integer(Ordered), new Integer(4), new Integer[]{5,3,1} } 
           ,{ new Integer(Ordered), new Integer(5), new Integer[]{10,12,13} }
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //4
       ,{ {{ "select min(id) from TESTTABLE group by COUNTRY order by 1, 1" }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(3), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(Ordered), new Integer(1), new Integer[]{1,3,5} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //5
       ,{ {{ "select min(id) from TESTTABLE group by COUNTRY order by min(id)" }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(3), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(Ordered), new Integer(1), new Integer[]{1,3,5} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //6
       ,{ {{ "select avg(src), avg(src2) from testtable group by id having avg(src) = avg(src2)" }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(3), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(Ordered), new Integer(1), new Integer[]{2,1,2} } 
           ,{ new Integer(Ordered), new Integer(2), new Integer[]{2,1,2} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //7
       ,{ {{ new Integer(fixImmediate), "select sum( (id/?)+(discount-?) ) from testtable " +
             " having avg(id)+?-count(id)/? >= avg(discount)*?/count(id)-cast(? as float) and max(id) not in (?,?,?,?) " }}
         ,{//input parameters
              { new Integer(Types.INTEGER), new Integer(10) }
             ,{ new Integer(Types.INTEGER), new Integer(210) }
             
             ,{ new Integer(Types.INTEGER), new Integer(1) }
             ,{ new Integer(Types.INTEGER), new Integer(100) }
             ,{ new Integer(Types.INTEGER), new Integer(20) }
             ,{ new Integer(Types.FLOAT)  , new Float(0.1F) }
             
             ,{ new Integer(Types.INTEGER), new Integer(11) }
             ,{ new Integer(Types.INTEGER), new Integer(12) }
             ,{ new Integer(Types.INTEGER), new Integer(14) }
             ,{ new Integer(Types.INTEGER), new Integer(15) }
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(1), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(Contains), new Integer(1), new Integer[]{-1907} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //8
       ,{ {{ "select count(1), substr(DESCRIPTION,2,length(DESCRIPTION)-2), sum(ID), min(ID), max(id) from TESTTABLE " +
             "  Group By substr(DESCRIPTION,2,length(DESCRIPTION)-2)" + 
             "  ORDER BY substr(DESCRIPTION,2,length(DESCRIPTION)-2), count(1)" }}
          ,{//input parameters
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
               { new Integer(3), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
              ,{ new Integer(Ordered), new Integer(1), new Integer[]{5,2,6} } 
              ,{ new Integer(Ordered), new Integer(2), new String[]{"irst","irst1",null} } 
              ,{ new Integer(Ordered), new Integer(3), new Integer[]{25,24,42} } 
              ,{ new Integer(Ordered), new Integer(4), new Integer[]{1,11,2} } 
              ,{ new Integer(Ordered), new Integer(5), new Integer[]{9,13,12} } 
           }
          ,{//getRoutingParameter inputs
             { new Integer(noCheck) }
           }
        } //9
       ,{ {{ "select address, case when ADDRESS is null then 'NULL ADD' else address end || '  ' || COUNTRY  from TESTTABLE group by COUNTRY, ADDRESS " +
       		"having address is not null or address is null order by ADDRESS NULLS FIRST, COUNTRY" }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(6), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(Ordered), new Integer(1), new String[]{null,null,null,"J 604","J 987","J 987"} } 
           ,{ new Integer(Ordered), new Integer(2), new String[]{"NULL ADD  INDIA","NULL ADD  ROWLD","NULL ADD  US","J 604  US","J 987  INDIA","J 987  ROWLD"} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //10
       ,{ {{ "select count(quantity+discount), sum(quantity), sum(quantity+discount) from testtable where ID IN (?,?,?) AND ID between 0 and 14 " +
                " group by description Order By avg(discount+quantity)" }}
         ,{//input parameters
            { new Integer(Types.INTEGER), new Integer(5) }
           ,{ new Integer(Types.INTEGER), new Integer(6) }
           ,{ new Integer(Types.INTEGER), new Integer(7) }
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(3), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(Ordered), new Integer(1), new Integer[]{1,1,1} } 
           ,{ new Integer(Ordered), new Integer(2), new Long[]{500L,600L,700L} } 
           ,{ new Integer(Ordered), new Integer(3), new Long[]{545L,654L,763L} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //11
       ,{ {{ "select " +
             " case when count( mod(id,2) ) + ? >= 0 and -sum( mod(discount,discount-1) ) < -? then " +
             "         case when max( length(id) ) > 1 then char(?,3) " +
             "             when max( length(id) ) >= 1 then char(-2) " +
             "         else char(10) end " +
             "      when ( count( mod(id,2) ) = 2 and -sum( mod(discount,discount-1) ) = -2 ) " +
             "        or (max(id) != 10 or 1!=1) and (1!=1 or 2 > min(discount)) " +
             "        then case when max( length(id) ) < 1 then char(-1)" +
             "                  when min( id ) = 1 then char(-2)" +
             "                  when max( length(id) ) > 1 then char(?,3)" +
             "             else char(20) end" +
             "      else char( count(mod(id,2)), 4 ) || ' - ' || char( sum( mod(discount, discount-1)) ) " +
             " end" +
             " from testtable group by address "}}
         ,{//input parameters
            { new Integer(Types.INTEGER), new Integer(7) }
           ,{ new Integer(Types.INTEGER), new Integer(4) }
           ,{ new Integer(Types.INTEGER), new Integer(-50) }
           ,{ new Integer(Types.INTEGER), new Integer(200) }
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(3), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(Ordered), new Integer(1), new String[]{"200","-50","4    - 4          "} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //12
       ,{ {{ "select sum(ID*?), count(1) from TESTTABLE " +
             " where id in (?,?,?) or (description is not null and length(description) > ?) or description is null" +
             " group by case when length(DESCRIPTION) > ? and length(DESCRIPTION) < ? then " +
             "                       substr(DESCRIPTION,1,length(DESCRIPTION)-1) " +
             "               when length(DESCRIPTION) > ? then " +
             "                       substr(DESCRIPTION,1,length(DESCRIPTION)-2) " +
             "          end, address " +
             " having avg(quantity) > 300 " +
             " order by avg(src), count(1) desc " }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(19) }
            ,{ new Integer(Types.INTEGER), new Integer(1) }
            ,{ new Integer(Types.INTEGER), new Integer(2) }
            ,{ new Integer(Types.INTEGER), new Integer(3) }
            ,{ new Integer(Types.INTEGER), new Integer(4) }
            
            ,{ new Integer(Types.INTEGER), new Integer(5) }
            ,{ new Integer(Types.INTEGER), new Integer(7) }
            ,{ new Integer(Types.INTEGER), new Integer(6) }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(3), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(Ordered), new Integer(1), new Integer[]{703,684,228} } 
            ,{ new Integer(Ordered), new Integer(2), new Integer[]{5,4,2} } 
           }
          ,{//getRoutingParameter inputs
             { new Integer(noCheck) }
           }
        } //13
       ,{ {{ "select avg(ID*?), count(1) from TESTTABLE " +
             " where id in (?,?,?) or (description is not null and length(description) > ?) or description is null" +
             " group by substr(DESCRIPTION,1,locate('st',DESCRIPTION)), address " +
             " having address like ? " +
             "    or case when avg(quantity) > ? then ? when cast( avg(discount) as bigint) * ? between ? AND ? then ? else 3 end in (?,?) " +
             "    or case when address is null then 'NULL ADDR' else lower(substr(address,1,3)) end " + 
             "          like case when address is null then 'NULL ADDR' when address like 'J 6%' then lower(?) else null end" +
             " order by avg(src), count(1) desc  " }}
          ,{//input parameters
             { new Integer(Types.INTEGER), new Integer(19) }
            ,{ new Integer(Types.INTEGER), new Integer(1) }
            ,{ new Integer(Types.INTEGER), new Integer(2) }
            ,{ new Integer(Types.INTEGER), new Integer(3) }
            ,{ new Integer(Types.INTEGER), new Integer(4) }
            
            ,{ new Integer(Types.VARCHAR), new String("J 9%") }
            
            ,{ new Integer(Types.INTEGER), new Integer(600) }
            ,{ new Integer(Types.INTEGER), new Integer(1) }
            
            ,{ new Integer(Types.INTEGER), new Integer(97) }
            ,{ new Integer(Types.INTEGER), new Integer(3000) }
            ,{ new Integer(Types.INTEGER), new Integer(5000) }
            ,{ new Integer(Types.INTEGER), new Integer(2) }
            
            ,{ new Integer(Types.INTEGER), new Integer(1) }
            ,{ new Integer(Types.INTEGER), new Integer(2) }
            
            ,{ new Integer(Types.VARCHAR), new String("J 9%") }
           }
          ,{//output (atleast 1 row for Nodes reconciliation)
             { new Integer(3), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
            ,{ new Integer(Ordered), new Integer(1), new Integer[]{140,171,57} } 
            ,{ new Integer(Ordered), new Integer(2), new Integer[]{5,4,2} } 
           }
          ,{//getRoutingParameter inputs
             { new Integer(noCheck) }
           }
        } //14
       ,{ {{ "select count(src2) from testtable having sum(distinct src) = ?" }}
         ,{//input parameters
              { new Integer(Types.INTEGER), new Integer(15) }
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(1), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(Contains), new Integer(1), new Integer[]{13} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //15
       ,{ {{ "Select count(src2), cast(sum(distinct src) as REAL)/count(src2) from testtable " +
             " group by substr(description, 1, length(description)-1 ) " }}
         ,{//input parameters
          }
         ,{//output (atleast 1 row for Nodes reconciliation)
            { new Integer(3), new Integer(3), new Integer(0) } //expectedRows, prunedNodes, noExecQueryNodes
           ,{ new Integer(Ordered), new Integer(1), new Integer[]{5,2,6} } 
           ,{ new Integer(Ordered), new Integer(2), new Float[]{3.0F, 3.0F, 2.5F} } 
          }
         ,{//getRoutingParameter inputs
            { new Integer(noCheck) }
          }
        } //16
    };
  
    try {
      
      VM[] vms = new VM[] {this.serverVMs.get(0), this.serverVMs.get(1), this.serverVMs.get(2)};
      QueryEvaluationHelper.evaluate4DQueryArray(queries, vms, this);
    
    } finally {
      GemFireXDQueryObserverHolder
      .setInstance(new GemFireXDQueryObserverAdapter());
      //invokeInEveryVM(QueryEvaluationHelper.class, "reset");         
    }
  } // end of Aggregates test


  public static void prepareTable(double price, boolean withForeignKeys,
      boolean withIndexes) throws SQLException {
    TestUtil.setupConnection();
    prepareTable(price, withForeignKeys, withIndexes, TestUtil.jdbcConn, false);
  }

  public static void prepareTable(double price, boolean withForeignKeys,
      boolean withIndexes, Connection conn, boolean isOffHeap) throws SQLException {

      TestUtil.jdbcConn = conn;

      createTable(withForeignKeys, isOffHeap);

      if (withIndexes) {
          // these indexes will evaluate 'select distinct' differently in certain conditions.
          // see testGroupBy_2
        TestUtil.jdbcConn.createStatement().execute(
              "create index testtable_country on TESTTABLE(COUNTRY) ");
          
        TestUtil.jdbcConn.createStatement().execute(
              "create index testtable_address on TESTTABLE(ADDRESS) ");
      }

      PreparedStatement ps = TestUtil.jdbcConn.prepareStatement(
          "insert into TESTTABLE values (?,?,?,?,?, ?,?,?,?,?, ?,? ) ");

      for (int i = 1; i <= 13; ++i) {
           ps.setInt(1, i);

           if(i%2 == 0)
             ps.setNull(2, Types.VARCHAR);
           else
             ps.setString(2, "First"+i);

           if(i%2 == 0 && i > 4 )
             ps.setNull(3, Types.VARCHAR);
           else
             ps.setString(3, (i%3==0?"J 604":"J 987"));

           ps.setString(4, (i%5==0?"INDIA": (i%3==0?"US":"ROWLD")));
           ps.setLong  (5, (i*100) );
           ps.setDouble(6, price);
           ps.setInt   (7, (i*10-i));
           ps.setFloat (8, (i*3.45F) );
           ps.setDouble(9, (i+0.98746D) );
           ps.setInt   (10, Integer.MAX_VALUE);
           
           ps.setInt   (11, (i%5)+1);
           ps.setInt   (12, (i%2)+1);
           
           ps.executeUpdate();
      }

      String createFunction="CREATE FUNCTION times " +
                            "(value INTEGER) " +
                            "RETURNS INTEGER "+
                            "LANGUAGE JAVA " +
                            "EXTERNAL NAME 'com.pivotal.gemfirexd.functions.TestFunctions.times' " +
                            "PARAMETER STYLE JAVA " +
                            "NO SQL " +
                            "RETURNS NULL ON NULL INPUT ";

      TestUtil.jdbcConn.createStatement().execute(createFunction);
  }

  private static void createTable(boolean withForeignKeys, boolean isOffHeap) throws SQLException {
    
    if(withForeignKeys) {
      
      if (testRunForReplicatedTablesOnly) {
        TestUtil.jdbcConn.createStatement().execute(
            "create table SOURCE (ID int primary key, DESCRIPTION varchar(20) ) replicate" + (isOffHeap? " offheap ":""));
      }
      else {
        TestUtil.jdbcConn.createStatement().execute(
            "create table SOURCE (ID int primary key, DESCRIPTION varchar(20) ) "+ (isOffHeap? " offheap ":""));
      }
      
      TestUtil.jdbcConn.createStatement().execute(
          "Insert into SOURCE values "
          + " (1, 'ONE')"
          + ",(2, 'TWO')"
          + ",(3, 'THREE')"
          + ",(4, 'FOUR')"
          + ",(5, 'FIVE')"
       );
      
      if (testRunForReplicatedTablesOnly) {
        TestUtil.jdbcConn.createStatement().execute(
            "create table TESTTABLE (ID int not null, "
            + " DESCRIPTION varchar(1024) , ADDRESS varchar(1024), COUNTRY varchar(1024), " 
            + " QUANTITY bigint, PRICE double, DISCOUNT int, AMOUNT real, MULTIPLY double, INT_OVERFLOW int, "
            + " SRC int, SRC2 int,"
            + " foreign key (src ) references SOURCE(id), "
            + " foreign key (src2) references SOURCE(id) )"
            + " replicate" + (isOffHeap? " offheap ":""));
      }
      else {
        TestUtil.jdbcConn.createStatement().execute(
            "create table TESTTABLE (ID int not null, "
            + " DESCRIPTION varchar(1024) , ADDRESS varchar(1024), COUNTRY varchar(1024), " 
            + " QUANTITY bigint, PRICE double, DISCOUNT int, AMOUNT real, MULTIPLY double, INT_OVERFLOW int, "
            + " SRC int, SRC2 int,"
            + " foreign key (src ) references SOURCE(id), "
            + " foreign key (src2) references SOURCE(id) )"
            + " PARTITION BY RANGE ( ID )"
            + "  ( VALUES BETWEEN 1 and 4, VALUES BETWEEN  4 and 8 )" 
            + " redundancy 3"+ (isOffHeap? " offheap ":""));
      }
      
      return;
    }
    
    if (testRunForReplicatedTablesOnly) {
      TestUtil.jdbcConn.createStatement().execute(
          "create table TESTTABLE (ID int not null, "
              + " DESCRIPTION varchar(1024) , ADDRESS varchar(1024), COUNTRY varchar(1024), " 
              + " QUANTITY bigint, PRICE double, DISCOUNT int, AMOUNT real, MULTIPLY double, INT_OVERFLOW int, "
              + " SRC int, SRC2 int,"
              + " primary key (ID) )"
              + " replicate"+ (isOffHeap? " offheap ":""));
    }
    else {
      TestUtil.jdbcConn.createStatement().execute(
          "create table TESTTABLE (ID int not null, "
              + " DESCRIPTION varchar(1024) , ADDRESS varchar(1024), COUNTRY varchar(1024), " 
              + " QUANTITY bigint, PRICE double, DISCOUNT int, AMOUNT real, MULTIPLY double, INT_OVERFLOW int, "
              + " SRC int, SRC2 int,"
              + " primary key (ID) )"
              + " PARTITION BY RANGE ( ID )"
              + "  ( VALUES BETWEEN 1 and 4, VALUES BETWEEN  4 and 8 )"+ (isOffHeap? " offheap ":""));
    }
  }

  public static void clearTables(boolean withForeignKeys, boolean withIndexes) 
     throws SQLException, StandardException {
    
      if(withIndexes) {
        TestUtil.jdbcConn.createStatement().execute("DROP INDEX testtable_country");
        TestUtil.jdbcConn.createStatement().execute("DROP INDEX testtable_address");
      }
      
      TestUtil.jdbcConn.createStatement().execute("DROP TABLE TESTTABLE");
      
      if(withForeignKeys) {
        TestUtil.jdbcConn.createStatement().execute("DROP TABLE SOURCE");
      }
  }

  @Override
  protected void vmTearDown() throws Exception {
    super.vmTearDown();
    QueryEvaluationHelper.reset();
  }
}
