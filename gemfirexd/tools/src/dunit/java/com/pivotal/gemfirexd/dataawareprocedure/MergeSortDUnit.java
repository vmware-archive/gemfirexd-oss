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
package com.pivotal.gemfirexd.dataawareprocedure;

import java.util.*;
import java.sql.*;

import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.procedure.IncomingResultSet;
import com.pivotal.gemfirexd.procedure.ProcedureExecutionContext;
import com.pivotal.gemfirexd.procedure.ProcedureProcessorContext;
import com.pivotal.gemfirexd.procedure.ProcedureResultProcessor;

@SuppressWarnings("serial")
public class MergeSortDUnit extends DistributedSQLTestBase {
  
  public MergeSortDUnit(String name) {
    super(name);
  }
  
  public void testMergeSortExample() throws Exception {
    setupVMs();
    setupMergeSortData();
    clientSQLExecute(1, "CREATE PROCEDURE MergeSort () "
        + "LANGUAGE JAVA PARAMETER STYLE JAVA "
        + "READS SQL DATA DYNAMIC RESULT SETS 1 " + "EXTERNAL NAME '"
        + MergeSortDUnit.class.getName() + ".mergeSort' ");
    
    // create custom processor
    clientSQLExecute(1, "CREATE ALIAS MergeSortProcessor FOR '"
        + MergeSortProcessor.class.getName() + "'");
    getLogWriter().info(
        "Processor class name: " + MergeSortProcessor.class.getName());

    CallableStatement cs = prepareCall("CALL MergeSort() "
        + "WITH RESULT PROCESSOR MergeSortProcessor "
        + "ON TABLE EMP.PARTITIONTESTTABLE1");

    cs.execute();
    ResultSet rs = cs.getResultSet();
    for(int i = 1; i <= 6; i++) {
      assertTrue(rs.next());
      getLogWriter().info(i+": "+rs.getInt(1)+" "+rs.getInt(2)+" "+rs.getString(3));
      switch (i) {
        case 1:
        case 2:
        case 3:
          assertEquals(2, rs.getInt(2));
          break;
          
        case 4:
          assertEquals(3, rs.getInt(2));
          break;
          
        case 5:
          assertEquals(4, rs.getInt(2));
          break;
          
        case 6:
          assertEquals(5, rs.getInt(2));
          break;
      }
    }
    assertFalse(rs.next());

    cs = prepareCall("CALL MergeSort() "
        + "WITH RESULT PROCESSOR " + MergeSortProcessor.class.getName() + ":: "
        + "ON TABLE EMP.PARTITIONTESTTABLE1");

    cs.execute();
    rs = cs.getResultSet();
    for(int i = 1; i <= 6; i++) {
      assertTrue(rs.next());
      getLogWriter().info(i+": "+rs.getInt(1)+" "+rs.getInt(2)+" "+rs.getString(3));
      switch (i) {
        case 1:
        case 2:
        case 3:
          assertEquals(2, rs.getInt(2));
          break;
          
        case 4:
          assertEquals(3, rs.getInt(2));
          break;
          
        case 5:
          assertEquals(4, rs.getInt(2));
          break;
          
        case 6:
          assertEquals(5, rs.getInt(2));
          break;
      }
    }
    assertFalse(rs.next());

    String processorClass = MergeSortProcessor2.class.getName();
    String callstring = "CALL MergeSort() "
      + "WITH RESULT PROCESSOR " + processorClass + " ON TABLE EMP.PARTITIONTESTTABLE1";
    getLogWriter().trace("Call string is: " + callstring);
    cs = prepareCall(callstring);

    cs.execute();
    rs = cs.getResultSet();
    for(int i = 1; i <= 6; i++) {
      assertTrue(rs.next());
      getLogWriter().info(i+": "+rs.getInt(1)+" "+rs.getInt(2)+" "+rs.getString(3));
      switch (i) {
        case 1:
        case 2:
        case 3:
          assertEquals(2, rs.getInt(2));
          break;
          
        case 4:
          assertEquals(3, rs.getInt(2));
          break;
          
        case 5:
          assertEquals(4, rs.getInt(2));
          break;
          
        case 6:
          assertEquals(5, rs.getInt(2));
          break;
      }
    }
    assertFalse(rs.next());

    cs = prepareCall("CALL MergeSort() "
        + "WITH RESULT PROCESSOR \"MergeSortProcessor2\" "
        + "ON TABLE EMP.PARTITIONTESTTABLE1");

    cs.execute();
    rs = cs.getResultSet();
    for(int i = 1; i <= 6; i++) {
      assertTrue(rs.next());
      getLogWriter().info(i+": "+rs.getInt(1)+" "+rs.getInt(2)+" "+rs.getString(3));
      switch (i) {
        case 1:
        case 2:
        case 3:
          assertEquals(2, rs.getInt(2));
          break;
          
        case 4:
          assertEquals(3, rs.getInt(2));
          break;
          
        case 5:
          assertEquals(4, rs.getInt(2));
          break;
          
        case 6:
          assertEquals(5, rs.getInt(2));
          break;
      }
    }
    assertFalse(rs.next());

    cs = prepareCall("CALL MergeSort() "
        + "WITH RESULT PROCESSOR MergeSortProcessor2:: "
        + "ON TABLE EMP.PARTITIONTESTTABLE1");

    cs.execute();
    rs = cs.getResultSet();
    for(int i = 1; i <= 6; i++) {
      assertTrue(rs.next());
      getLogWriter().info(i+": "+rs.getInt(1)+" "+rs.getInt(2)+" "+rs.getString(3));
      switch (i) {
        case 1:
        case 2:
        case 3:
          assertEquals(2, rs.getInt(2));
          break;
          
        case 4:
          assertEquals(3, rs.getInt(2));
          break;
          
        case 5:
          assertEquals(4, rs.getInt(2));
          break;
          
        case 6:
          assertEquals(5, rs.getInt(2));
          break;
      }
    }
    assertFalse(rs.next());
  }

  public static void mergeSort(ResultSet[] outResults,
      ProcedureExecutionContext context) throws SQLException {

    //Misc.getCacheLogWriter().info("KN: entering mergesort");
    String tableName = "EMP.PARTITIONTESTTABLE1";
    /*
    String tableName = context.getTableName();
     */
    assert tableName != null;
    String queryString = "<local> SELECT * FROM " + tableName
        + " ORDER BY SECONDID";

    /*
     Connection cxn = context.getConnection();
     */
    Connection cxn = DriverManager.getConnection("jdbc:default:connection");
    Statement stmt = cxn.createStatement();
    ResultSet rs = stmt.executeQuery(queryString);
    outResults[0] = rs;
    // Do not close the connection since this would also
    // close the result set.
  }

  public static void justanothermergeSort(ResultSet[] outResults,
      ProcedureExecutionContext context) throws SQLException {

  }
  
  public static class MergeSortProcessor implements ProcedureResultProcessor {
    
    private ProcedureProcessorContext context;
    
    /**
     * Initialize this processor.
     */
    public void init(ProcedureProcessorContext context) {
      this.context = context;
    }
    
    
    /**
     * Provide the out parameters for this procedure to the client as an
     * Object[].
     *
     * @throws InterruptedException if interrupted while waiting to receive
     *         data.
     */
    public Object[] getOutParameters() throws InterruptedException {
      throw new AssertionError("this procedure has no out parameters");    
    }
    
    
    /**
     * Provide the next row for result set number resultSetNumber.
     * The processor should do whatever processing is required on the
     * incoming data to provide the next row.
     *
     * Return the next row of the result set specified by resultSetNumber,
     * or null if there are no more rows in this result set.
     *
     * @param resultSetNumber the 1-based result set number for the row being
     *        requested
     * @throws InterruptedException if interrupted while waiting to receive
     *         data.
     *
     *
     * @throws InterruptedException if interrupted while waiting to receive
     *         data.
     */
    public List<Object> getNextResultRow(int resultSetNumber)
    throws InterruptedException {
      // this procedure deals with only first result set
      assert resultSetNumber == 0: "unexpected resultSetNumber="
          + resultSetNumber;
      IncomingResultSet[] inSets = this.context.getIncomingResultSets(0);
      //getLogWriter().info("KN: inSets length: "+inSets.length);
      List<Object> lesserRow = null;
      Comparator<List<Object>> cmp = getComparator();

      IncomingResultSet setWithLeastRow = null;
      for (IncomingResultSet inSet : inSets) {
        List<Object> nextRow = inSet.waitPeekRow(); // blocks until row is available
        if (nextRow == IncomingResultSet.END_OF_RESULTS) {
          //getLogWriter().info("KN: nextRow: END_OF_RESULTS");
          // no more rows in this incoming results
          continue;
        }
        //getLogWriter().info("KN: lesserRow: "+lesserRow+" nextRow: "+nextRow);
        // find the least row so far
        if (lesserRow == null || cmp.compare(nextRow, lesserRow) <= 0) {
          lesserRow = nextRow;
          setWithLeastRow = inSet;
        }
      }

      if (setWithLeastRow != null) {
        // consume the lesserRow by removing lesserRow from the incoming result
        // set
        List<Object> takeRow = setWithLeastRow.takeRow();
        assert takeRow == lesserRow;
      }

      // if lesserRow is null, then there are no more rows in any incoming
      // results
      return lesserRow;
    }

    /**
     * Called by GemFireXD when this statement is closed.
     */
    public void close() {
      this.context = null;
    }

    /** Return an appropriate Comparator for sorting the rows */
    private Comparator<List<Object>> getComparator() {
      return new Comparator<List<Object>>() {
        public int compare(List<Object> a1, List<Object> a2) {
          if ((Integer)a1.get(1) < (Integer)a2.get(1)) {
            return -1;
          }
          else if ((Integer)a1.get(1) > (Integer)a2.get(1)) {
            return 1;
          }
          return 0;
        }
      };
    }
  }

  private void setupVMs() throws Exception {
    startVMs(1, 2);
  }
  
  private void setupMergeSortData() throws Exception {
    clientSQLExecute(1,"create table EMP.PARTITIONTESTTABLE1 (ID int NOT NULL,"
                     + " SECONDID int not null, THIRDID varchar(10) not null,"
                     + " PRIMARY KEY (SECONDID, THIRDID)) PARTITION BY COLUMN (ID)");
    //   + " PARTITION BY COLUMN (ID)");
    clientSQLExecute(1,"INSERT INTO EMP.PARTITIONTESTTABLE1 VALUES (2, 2, '3') ");
    clientSQLExecute(1,"INSERT INTO EMP.PARTITIONTESTTABLE1 VALUES (3, 3, '3') ");
    clientSQLExecute(1,"INSERT INTO EMP.PARTITIONTESTTABLE1 VALUES (4, 4, '3') ");
    clientSQLExecute(1,"INSERT INTO EMP.PARTITIONTESTTABLE1 VALUES (5, 5, '3') ");
    clientSQLExecute(1,"INSERT INTO EMP.PARTITIONTESTTABLE1 VALUES (2, 2, '2') ");
    clientSQLExecute(1,"INSERT INTO EMP.PARTITIONTESTTABLE1 VALUES (2, 2, '4') ");
  }
  
  private CallableStatement prepareCall(String sql) throws SQLException {
    /** get a new connection with default properties */
    Connection conn = TestUtil.getConnection();
    CallableStatement cs = conn.prepareCall(sql);   
    return cs;
  }
  
}
