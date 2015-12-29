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
package com.pivotal.gemfirexd.internal.engine.distributed;

import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserver;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.distributed.ResultHolder;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.ColumnQueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.QueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.SelectQueryInfo;
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils;
import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.sql.conn.LanguageConnectionContext;
import com.pivotal.gemfirexd.internal.iapi.sql.dictionary.ColumnDescriptorList;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedPreparedStatement;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.GenericPreparedStatement;
import com.pivotal.gemfirexd.jdbc.JdbcTestBase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

/**
 * @author Asif
 * 
 */
public class ResultHolderTest extends JdbcTestBase {

  boolean callbackInvoked = false;

  private QueryInfo qInfo;

  public ResultHolderTest(String name) {
    super(name);
  }

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(ResultHolderTest.class));
  }

  public void testSerializability() throws SQLException, IOException,
      ClassNotFoundException, StandardException {
    Connection conn = getConnection();
    createTable(conn);
    //Insert a row

    PreparedStatement psInsert = conn
        .prepareStatement("insert into orders values (?, ?,?,?,?,?)");

    psInsert.setInt(1, 1);
    psInsert.setString(2, "asif");
    psInsert.setInt(3, 1);
    psInsert.setString(4, "asif");
    psInsert.setInt(5, 1);
    psInsert.setString(6, "asif");
    psInsert.executeUpdate();

    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder.setInstance(
          new GemFireXDQueryObserverAdapter() {

        @Override
        public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qinfo,
            GenericPreparedStatement gps, LanguageConnectionContext lcc) {
          ResultHolderTest.this.callbackInvoked = true;
          assertTrue(qinfo instanceof SelectQueryInfo);
          qInfo = qinfo;
        }
      });

      conn.setAutoCommit(false);

      EmbedPreparedStatement pqry = (EmbedPreparedStatement) conn
              .prepareStatement("Select num,cust_name from orders where id = ?");
      //.prepareStatement("Select * from orders where id = ?");
      pqry.setInt(1, 1);
      assert qInfo instanceof SelectQueryInfo : "queryinfo is not an instance of SelectQueryInfo";
      ColumnQueryInfo[] cqi = ((SelectQueryInfo) qInfo).getProjectionColumnQueryInfo();
      ColumnDescriptorList pqryRowTemplate = new ColumnDescriptorList();
      for (int i = 0; i < cqi.length; i++) {
        pqryRowTemplate.add(cqi[i].getColumnDescriptor());
      }

      PreparedStatement pqry1 = conn
              .prepareStatement("Select num,cust_name from orders where id = ?");
      pqry1.setInt(1, 1);
      cqi = ((SelectQueryInfo) qInfo).getProjectionColumnQueryInfo();
      ColumnDescriptorList pqry1RowTemplate = new ColumnDescriptorList();
      for (int i = 0; i < cqi.length; i++) {
        pqry1RowTemplate.add(cqi[i].getColumnDescriptor());
      }

      //
      // We select the rows and verify the results.
      //
      EmbedResultSet ers = (EmbedResultSet) pqry.executeQuery();
      EmbedResultSet ers1 = (EmbedResultSet) pqry1.executeQuery();
      ers.setupContextStack(true);
      LanguageConnectionContext lcc = ((EmbedConnection) conn).getLanguageConnection();
      lcc.pushStatementContext(true, true, pqry.getSQLText(),
              pqry.getParameterValueSet(),
              false, 1000000);

      final long origMaxChunkSize = GemFireXDUtils.DML_MAX_CHUNK_SIZE;
      GemFireXDUtils.DML_MAX_CHUNK_SIZE = 1000000;
      ResultHolder rhSender = new ResultHolder(ers,
              pqry, null, 0, null, false);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutput dop = new DataOutputStream(baos);
      rhSender.prepareSend(null);
      rhSender.toData(dop);
      ResultHolder rhReciever = new ResultHolder();
      ByteArrayInputStream bis = new ByteArrayInputStream(baos.toByteArray());
      DataInput di = new DataInputStream(bis);
      final RowFormatter rf = new RowFormatter(pqryRowTemplate, "APP",
              "ORDERS", 1, null);
      rhReciever.setRowFormatter(rf, false);
      //di.readByte();
      rhReciever.fromData(di);
      ers1.next();
      //Once indx bug is fixed change this to obtain execRow from QueryInfo
      ExecRow row = ers1.getCurrentRow();
      row = rhReciever.getNext(row, null);
      assertEquals(row.getRowArray().length, 2);
      DataValueDescriptor dvd[] = row.getRowArray();
      assertEquals(1, dvd[0].getInt());
      assertEquals("asif", dvd[1].getString());

      GemFireXDUtils.DML_MAX_CHUNK_SIZE = origMaxChunkSize;
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  public void testSerializabilityOfEmptyResultSetWithProjection()
      throws SQLException, IOException, ClassNotFoundException,
      StandardException {
    executeQueryForEmptyResult(true /* with projection*/);
  }

  public void testSerializabilityOfEmptyResultSetWithoutProjection()
      throws SQLException, IOException, ClassNotFoundException,
      StandardException {
    executeQueryForEmptyResult(false /* without projection*/);
  }

  private void executeQueryForEmptyResult(boolean doProjection)
      throws SQLException, IOException, ClassNotFoundException,
      StandardException {
    Connection conn = getConnection();
    createTable(conn);
    // Insert a row

    PreparedStatement psInsert = conn
        .prepareStatement("insert into orders values (?, ?,?,?,?,?)");

    psInsert.setInt(1, 1);
    psInsert.setString(2, "asif");
    psInsert.setInt(3, 1);
    psInsert.setString(4, "asif");
    psInsert.setInt(5, 1);
    psInsert.setString(6, "asif");
    psInsert.executeUpdate();

    GemFireXDQueryObserver old = null;
    try {
      old = GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {

            @Override
            public void queryInfoObjectFromOptmizedParsedTree(QueryInfo qinfo, GenericPreparedStatement gps, LanguageConnectionContext lcc)
            {
              ResultHolderTest.this.callbackInvoked = true;
              assertTrue(qinfo instanceof SelectQueryInfo);
              qInfo = qinfo;
            }
          });

      EmbedPreparedStatement pqry = null;
      if(doProjection) {
        pqry = (EmbedPreparedStatement)conn.prepareStatement("Select num,cust_name from orders where id = ?");
      }else {
        pqry = (EmbedPreparedStatement)conn.prepareStatement("Select * from orders where id = ?");
      }
      pqry.setInt(1, 5);
      assert qInfo instanceof SelectQueryInfo: "queryinfo is not an instance of SelectQueryInfo";
      ColumnQueryInfo[] cqi = ((SelectQueryInfo)qInfo)
          .getProjectionColumnQueryInfo();
      ColumnDescriptorList pqryRowTemplate = new ColumnDescriptorList();
      for (int i = 0; i < cqi.length; i++) {
        pqryRowTemplate.add(cqi[i].getColumnDescriptor());
      }
      cqi = ((SelectQueryInfo)qInfo).getProjectionColumnQueryInfo();
      ColumnDescriptorList pqry1RowTemplate = new ColumnDescriptorList();
      for (int i = 0; i < cqi.length; i++) {
        pqry1RowTemplate.add(cqi[i].getColumnDescriptor());
      } //
      // We select the rows and verify the results.
      //
      EmbedResultSet ers = (EmbedResultSet)pqry.executeQuery();
      ers.setupContextStack(true);
      LanguageConnectionContext lcc = ((EmbedConnection)conn)
          .getLanguageConnection();
      lcc.pushStatementContext(true, true, pqry.getSQLText(), pqry
          .getParameterValueSet(), false, 1000000);

      ResultHolder rhSender = new ResultHolder(ers,
          pqry, null, 0, null, false);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      DataOutput dop = new DataOutputStream(baos);
      rhSender.toData(dop);
      ResultHolder rhReciever = new ResultHolder();
      ByteArrayInputStream bis = new ByteArrayInputStream(baos.toByteArray());
      DataInput di = new DataInputStream(bis);
      final RowFormatter rf = new RowFormatter(pqryRowTemplate, "APP",
          "ORDERS", 1, null);
      rhReciever.setRowFormatter(rf, false);
      //di.readByte();
      rhReciever.fromData(di);
      assertFalse(rhReciever.hasNext(null));
    }
    finally {
      if (old != null) {
        GemFireXDQueryObserverHolder.setInstance(old);
      }
    }
  }

  public void createTableWithPrimaryKey(Connection conn) throws SQLException {
    Statement s = conn.createStatement();
    // We create a table...
    s.execute("create table orders"
        + "(id int PRIMARY KEY, cust_name varchar(200), vol int, "
        + "security_id varchar(10), num int, addr varchar(100))");
    s.close();
  }

  public void createTable(Connection conn) throws SQLException
  {
    Statement s = conn.createStatement();
    // We create a table...
    s.execute("create table orders"
        + "(id int not null , cust_name varchar(200), vol int, "
        + "security_id varchar(10), num int, addr varchar(100))");
    s.close();
  }

  public void createTableWithCompositeKey(Connection conn) throws SQLException
  {
    Statement s = conn.createStatement();
    // We create a table...
    s.execute("create table orders"
        + "(id int , cust_name varchar(200), vol int, "
        + "security_id varchar(10), num int, addr varchar(100),"
        + " Primary Key (id, cust_name))");
    s.close();

  }

  @Override
  public void tearDown() throws Exception {
    this.callbackInvoked = false;
    super.tearDown();
  }
}
