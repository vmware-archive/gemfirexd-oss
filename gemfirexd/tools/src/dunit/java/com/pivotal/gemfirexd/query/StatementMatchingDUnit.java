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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.sql.execute.AbstractGemFireResultSet;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.sanity.SanityManager;
import com.pivotal.gemfirexd.internal.iapi.sql.Activation;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.depend.DependencyManager;

import io.snappydata.test.dunit.AsyncInvocation;
import io.snappydata.test.dunit.SerializableRunnable;

@SuppressWarnings("serial")
public class StatementMatchingDUnit extends DistributedSQLTestBase {
  
  public StatementMatchingDUnit(String name) {
    super(name);
  }

  public void testStatement_PreparedStatementMixing() throws Exception {
	  if(isTransactional) {
		  return;
	  }
    startVMs(2, 3);

    Connection conn = TestUtil.getConnection();
    Statement st = conn.createStatement();
  
    conn.createStatement().execute(
        " create table course ( " + "course_id int, course_name varchar(20),"
            + "primary key(course_id, course_name)" + ") partition by column (course_id) ");
  
    conn.createStatement().execute(
        " create table student ( " + "st_id varchar(10), c_id int,"
            + "primary key(st_id, c_id)" + ") replicate ");
  
  
    st.execute("insert into student values('x', 1)");
    st.execute("insert into student values('x', 2)");
    st.execute("insert into student values('x', 3)");
    st.execute("insert into student values('a', 1)");
    st.execute("insert into student values('bb', 4)");
  
    st.execute("insert into course values(4, 'FOUR')");
    st.execute("insert into course values(5, 'FIVE')");
    st.execute("insert into course values(6, 'SIX')");
    
    conn.createStatement().execute(
        " create table course_1 ( " + "course_id int, course_name varchar(20),"
            + "primary key(course_id, course_name)" + ") partition by column (course_id) ");
  
    conn.createStatement().execute(
        " create table student_1 ( " + "st_id varchar(10), c_id int,"
            + "primary key(st_id, c_id) ) " + " partition by column (c_id)  colocate with (course_1) ");
    st.execute("insert into student_1 values('y', 1)");
    st.execute("insert into student_1 values('y', 2)");
    st.execute("insert into student_1 values('y', 3)");
    st.execute("insert into student_1 values('a', 1)");
    st.execute("insert into student_1 values('bb', 4)");
  
    st.execute("insert into course_1 values(1, 'ONE')");
    st.execute("insert into course_1 values(2, 'TWO')");
    st.execute("insert into course_1 values(3, 'THREE')");
    st.execute("insert into course_1 values(4, 'FOUR')");
    st.execute("insert into course_1 values(5, 'FIVE')");
    st.execute("insert into course_1 values(6, 'SIX')");

    st.close();
    conn.close();
  
    SerializableRunnable selects = new SerializableRunnable() {
      
      long[] res = new long[] { 6, 5, 4 };
      
      @Override
      public void run() {
        Connection conn;
        try {
          conn = TestUtil.getConnection();
          Statement st = conn.createStatement();

          ResultSet st_st = st
              .executeQuery("select c_id from student where st_id = 'x' order by c_id desc NULLS first ");

          PreparedStatement c_ps = conn
              .prepareStatement("select * from course where course_id = ? ");

          conn
              .createStatement()
              .executeQuery(
                  "select c_id from student_1 where st_id = 'y' and c_id not in (select distinct course_id from course_1) order by c_id desc NULLS first ");

          conn
              .prepareStatement("select * from course_1 where course_id = ? ");

          int r = -1;
          while (st_st.next()) {
            int param = st_st.getInt(1);
            c_ps.setInt(1, param + 3);
            ResultSet rc = c_ps.executeQuery();
            assert r < res.length;
            // assertTrue(st_st1.next());
            // c_ps1.setInt(1, st_st1.getInt(1));
            // ResultSet rc1 = c_ps.executeQuery();
            
            assertTrue(rc.next());
            long received = rc.getLong("course_id");
            getLogWriter().info("Received course row " + received + " passed in param " + param);
            assertEquals(res[++r], received);
            assertFalse(rc.next());

            // assertEquals(res[r], rc1.getLong("course_id"));
          }
          assertTrue(r != -1);
        } catch (SQLException e) {
          fail("Exception occured ", e);
        }
      }
    };
    
    ArrayList<AsyncInvocation> runlist1 = executeTaskAsync(new int[] {2} , new int[] {1,2,3} , selects);
    
    selects.run();

    joinAsyncInvocation(runlist1);
    
  }
  
  public void testBug42482_reprepare() throws Exception {
    startVMs(1, 3);
    
    SerializableRunnable installReprepare = new SerializableRunnable("Installing reprepare hook ") {

      @Override
      public void run() {
        SanityManager.DEBUG_SET("StatementMatching");
        GemFireXDUtils.initFlags();
        
        GemFireXDQueryObserver prepReprepare = new GemFireXDQueryObserverAdapter() {

          private int numTimesCompiled = 0;
          
          @Override
          public void beforeGemFireResultSetOpen(AbstractGemFireResultSet rs, LanguageConnectionContext lcc) throws StandardException {
            numTimesCompiled++;
            // first compilation raise 'recompile'
            if(numTimesCompiled == 1) {
              getLogWriter().info("Raising re-compile exception");
              final Activation act = rs.getActivation();
              act.getPreparedStatement().makeInvalid(
                  DependencyManager.PREPARED_STATEMENT_RELEASE,
                  act.getLanguageConnectionContext());
              throw StandardException.newException("XCL32.S");
            }
            
          }
          
          public void afterGemFireResultSetOpen(AbstractGemFireResultSet rs, LanguageConnectionContext lcc) {
            assertTrue(
                "numTimesCompiled is expected more than once due to recompile exception ",
                numTimesCompiled > 1);
          }
        };
        GemFireXDQueryObserverHolder.setInstance(prepReprepare);
        
      }
      
    };

    
    Connection conn = TestUtil.getConnection();
    Statement st = conn.createStatement();
    
    conn.createStatement().execute(
        " create table course_1 ( " + "course_id int, course_name varchar(30),"
            + "primary key(course_id)" + ") partition by column (course_id) ");
    
    st.execute("insert into course_1 values(11, 'ONE')");
    st.execute("insert into course_1 values(12, 'TWO')");
    st.execute("insert into course_1 values(13, 'THREE')");
    st.execute("insert into course_1 values(14, 'FOUR')");
    st.execute("insert into course_1 values(15, 'FIVE')");
    st.execute("insert into course_1 values(16, 'SIX')");
    
    st.execute("update course_1 set course_name = 'MURRAY' where course_id > 10 ");
    
    st.execute("update course_1 set course_name = 'MURRAY BLVD' where course_id > 5 ");

    ArrayList<AsyncInvocation> runlist1 = executeTaskAsync(null, new int[] {1,2,3}, installReprepare);
    
    installReprepare.run();
    
    joinAsyncInvocation(runlist1);
    

    st.execute("update course_1 set course_name = 'HURRY.FAIL.ME' where course_id > 8 ");
    
  }
  
  public void testBug42488() throws Exception {

    startVMs(1, 1);

    Connection conn = TestUtil.getConnection();
    Statement st = conn.createStatement();

    conn.createStatement().execute(
        " create table course ( " + "course_id int, course_name varchar(20),"
            + "primary key(course_id, course_name)"
            + ") partition by column (course_id) ");

    conn.createStatement().execute(
        " create table student ( " + "st_id varchar(10), c_id int,"
            + "primary key(st_id, c_id)" + ") replicate ");

    st.execute("insert into student values('x', 1)");
    st.execute("insert into student values('x', 2)");
    st.execute("insert into student values('x', 3)");
    st.execute("insert into student values('a', 1)");
    st.execute("insert into student values('bb', 4)");

    st.execute("insert into course values(4, 'FOUR')");
    st.execute("insert into course values(5, 'FIVE')");
    st.execute("insert into course values(6, 'SIX')");
    String query1 = "Select * from course where  course_id >= 4";
    ResultSet rs = st.executeQuery(query1);
    int num = 0;
    while (rs.next()) {
      rs.getInt(1);
      ++num;
    }
    assertEquals(3, num);
    
    startVMs(0, 1);
    num = 0;
    String query2 = "Select * from course where  course_id >= 8";

    st.execute("insert into course values(7, 'Seven')");
    st.execute("insert into course values(8, 'Eight')");
    st.execute("insert into course values(9, 'Nine')");
    rs = st.executeQuery(query2);
    while (rs.next()) {
      rs.getInt(1);
      ++num;
    }
    assertEquals(2, num);
    st.close();
    conn.close();

  }
}

