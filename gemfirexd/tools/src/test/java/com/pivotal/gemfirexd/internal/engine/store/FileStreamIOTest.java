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

package com.pivotal.gemfirexd.internal.engine.store;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Properties;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverAdapter;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.raw.store.FileStreamInputOutput;
import com.pivotal.gemfirexd.internal.engine.sql.conn.GfxdHeapThresholdListener;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.services.io.FormatableBitSet;
import com.pivotal.gemfirexd.internal.iapi.sql.execute.ExecRow;
import com.pivotal.gemfirexd.internal.iapi.store.access.ColumnOrdering;
import com.pivotal.gemfirexd.internal.iapi.store.access.RowSource;
import com.pivotal.gemfirexd.internal.iapi.store.access.conglomerate.TransactionManager;
import com.pivotal.gemfirexd.internal.iapi.store.raw.StreamContainerHandle;
import com.pivotal.gemfirexd.internal.iapi.store.raw.Transaction;
import com.pivotal.gemfirexd.internal.iapi.types.DataType;
import com.pivotal.gemfirexd.internal.iapi.types.SQLChar;
import com.pivotal.gemfirexd.internal.iapi.types.SQLInteger;
import com.pivotal.gemfirexd.internal.iapi.types.SQLTimestamp;
import com.pivotal.gemfirexd.internal.iapi.types.UserType;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedResultSet;
import com.pivotal.gemfirexd.internal.impl.sql.execute.CountAggregator;
import com.pivotal.gemfirexd.internal.impl.sql.execute.ValueRow;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

public class FileStreamIOTest extends TestCase {

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(FileStreamIOTest.class));
  }

  public FileStreamIOTest(String name) {
    super(name);
  }

  public void testRowOverflow() throws SQLException, StandardException {

    EmbedConnection conn = (EmbedConnection)TestUtil.getConnection();

    Statement st = conn.createStatement();
    try {
      st.execute("drop table test");
    } catch (Exception i) {
    }

    try {
      st
          .execute("create table test (id int primary key , description varchar(1024), "
              + "add_1 varchar(1024), add_2 varchar(1024) ) replicate ");

      PreparedStatement ps = conn
          .prepareStatement("insert into test values( ?, ? ,? , ?) ");

      ps.setInt(1, 1);
      ps.setString(2, "description");
      ps.setString(3, "address");
      ps.setString(4, "address");
      ps.execute();

      TransactionManager tm = (TransactionManager)conn.getLanguageConnection()
          .getTransactionExecute();
      Transaction t = tm.getRawStoreXact();

      EmbedResultSet ers = (EmbedResultSet)st
          .executeQuery("select * from test");

      ByteBuffer b = FileStreamInputOutput.allocateBuffer(50, tm);

      long containerId;
      StreamContainerHandle sh;
      final RowFormatter rf;
      ExecRow row;
      { // single row
        containerId = t.addAndLoadStreamContainer(
            StreamContainerHandle.TEMPORARY_SEGMENT, null, new TestRowSource(
                ers), b);
        ers.close();
        sh = t.openStreamContainer(StreamContainerHandle.TEMPORARY_SEGMENT,
            containerId, false);
        containerId = 0;

        final Region<?, ?> r = Misc.getRegion(Misc.getRegionPath(
            TestUtil.currentUserName, "TEST", conn.getLanguageConnection()),
            true, false);
        rf = ((GemFireContainer)r.getUserAttribute()).getCurrentRowFormatter();
        row = new CompactExecRow(rf);
        assertTrue(sh.fetchNext(row));
        assertEquals(1, row.getColumn(1).getInt());
        assertFalse(sh.fetchNext(row));
      }

      { // multiple CompactRow
        for (int i = 2; i < 10; i++) {
          ps.setInt(1, i);
          ps.setString(2, "desc_" + i);
          if (i % 2 == 0)
            ps.setString(3, "add_" + i);
          else
            ps.setString(3, "my address of sea facing juhu bunglow is " + i);

          if (i % 3 == 0)
            ps.setString(4, "address_" + i);
          else
            ps.setString(4, "অ আ ই ঈ উ ঊ ঋ এ ঐ ও ঔ ক খ গ ঘ ঙ চ ছ জ ঝ ঞ ট ঠ "
                + i);

          ps.execute();
        }

        ers = (EmbedResultSet)st.executeQuery("select * from test");

        containerId = t.addAndLoadStreamContainer(
            StreamContainerHandle.TEMPORARY_SEGMENT, null, new TestRowSource(
                ers), b);
        sh = t.openStreamContainer(StreamContainerHandle.TEMPORARY_SEGMENT,
            containerId, false);
        containerId = 0;

        int[] rmap = new int[9];
        for (int i = 1; i < 10; i++) {
          assertTrue(sh.fetchNext(row));
          int val = row.getColumn(1).getInt() - 1;
          assertTrue("val[" + i + "]=" + val, val >= 0 && val < rmap.length);
          rmap[val] += 1;
        }
        assertFalse(sh.fetchNext(row));

        for (int i = 1; i < 10; i++) {
          assertTrue(i + "th index repeated " + rmap[i - 1], rmap[i - 1] == 1);
        }
      }

      {// check ValueRow.
        row = new ValueRow(rf.getNumColumns());
        ers = (EmbedResultSet)st.executeQuery("select id, length(description) "
            + "from test order by id");
        containerId = t.addAndLoadStreamContainer(
            StreamContainerHandle.TEMPORARY_SEGMENT, null, new TestRowSource(
                ers), b);
        sh = t.openStreamContainer(StreamContainerHandle.TEMPORARY_SEGMENT,
            containerId, false);

        for (int i = 1; i < 10; i++) {
          assertTrue(sh.fetchNext(row));
          int val = row.getColumn(1).getInt();
          assertEquals(i, val);
        }
        assertFalse(sh.fetchNext(row));

        ers = (EmbedResultSet)st
            .executeQuery("select description from test order by id");
        containerId = t.addAndLoadStreamContainer(
            StreamContainerHandle.TEMPORARY_SEGMENT, null, new TestRowSource(
                ers), b);
        sh = t.openStreamContainer(StreamContainerHandle.TEMPORARY_SEGMENT,
            containerId, false);

        for (int i = 1; i < 10; i++) {
          assertTrue(sh.fetchNext(row));
          String val = row.getColumn(1).getString();
          assertEquals("desc", val.substring(0, 4));
        }
        assertFalse(sh.fetchNext(row));
      }

    } finally {
      TestUtil.shutDown();
    }

  }

  public void testUserTypeFileIO() throws SQLException, StandardException,
      IOException, ClassNotFoundException {

    final UserType ut = new UserType();
    CountAggregator ca = new CountAggregator();
    ca.setup("count(*)", new SQLInteger(122));
    ut.setValue(ca);

    Connection conn = TestUtil.getConnection();

    // write user type.
    FileStreamInputOutput fileIO = new FileStreamInputOutput(-1,
        FileStreamInputOutput.allocateBuffer(10, null));
    ut.toData(fileIO);
    fileIO.flushAndSync(false);
    fileIO.flipToRead();
    UserType res = null;

    // HeapDataOutputStream hout = new HeapDataOutputStream();
    // ut.toData(hout);
    // //now read the user type.
    // ByteBufferInputStream bi = new
    // ByteBufferInputStream(hout.toByteBuffer());
    // res = (UserType)DataType.readDVD(bi);
    // ca = (CountAggregator)ut.getObject();
    // assertEquals(122, ca.getResult().getLong());

    res = (UserType)DataType.readDVD(fileIO);
    ca = (CountAggregator)res.getObject();
    assertEquals(122, ca.getResult().getLong());
    conn.close();
    TestUtil.shutDown();
  }

  public void testMultipleMergeRuns() throws SQLException,
      StandardException, IOException, ClassNotFoundException {
    EmbedConnection conn = (EmbedConnection)TestUtil.getConnection();

    Statement st = conn.createStatement();
    try {
      st.execute("drop table test");
    } catch (Exception i) {
    }

    st
        .execute("create table test (id int primary key , description varchar(1024), "
            + "add_1 varchar(1024), add_2 varchar(1024), t_val bigint ) replicate ");

    PreparedStatement ps = conn
        .prepareStatement("insert into test values( ?,?,?,?,?) ");

    for (int i = 1; i < 10; i++) {
      ps.setInt(1, i);
      ps.setString(2, "desc_" + i % 10);
      ps.setString(3, "add_" + i % 10);
      ps.setString(4, "address2_" + i % 10);
      ps.setLong(5, i % 10);
      ps.execute();
    }

    GemFireXDQueryObserverHolder
        .setInstance(new GemFireXDQueryObserverAdapter() {
          private static final long serialVersionUID = 1L;

          public int overrideSortBufferSize(ColumnOrdering[] columnOrdering,
              int sortBufferMax) {
            return 3;
          }

          public boolean avoidMergeRuns() {
            return false;
          }

        });

    try {
      ResultSet rs = st
          .executeQuery("select description, count(1) from test group by t_val, description");
      while (rs.next()) {
        System.out.println(rs.getString(1));
      }
    } finally {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter());
      TestUtil.shutDown();
    }
  }

  public void testAllDataTypesDiskWrite() throws SQLException {
    EmbedConnection conn = (EmbedConnection)TestUtil.getConnection();

    Statement st = conn.createStatement();
    try {
      st.execute("drop table test");
    } catch (Exception i) {
    }

    try {
      st
          .execute("create table test (id int primary key, "
              + "lv bigint, si smallint, fl float, dbl double, "
              + "nmr numeric(10,2), dml decimal(30,20), "
              + "dt date, tm time, ts timestamp, cr char(254), vcr varchar(8192), id2 int, id3 int )"); // rl
      // real

      PreparedStatement ps = conn
          .prepareStatement("insert into test values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )");

      String vstr = "ঙ চ ছ জ ঝ ঞ ট This is varchar max upto 8k bytes ... so I suppose this should test long " +
      		" enought string for now .... with some more bytes added for randomization ";
      for (int row = 1; row < 1000; row++) {
        ps.setInt(1, row);
        ps.setLong(2, row);
        ps.setShort(3, (short)row);
        ps.setFloat(4, row * 1.3f);
        ps.setDouble(5, row + 1.2 / 0.9);
        ps.setBigDecimal(6, BigDecimal.valueOf(row / 1.34d));
        ps.setBigDecimal(7, BigDecimal.valueOf(row / 1.43d * row));
        final int d = ((row % 20) + 1);
        ps.setDate(8, java.sql.Date
            .valueOf("2011-04-" + (d < 10 ? "0" + d : d)));
        ps.setTime(9, java.sql.Time.valueOf("12:00:01"));
        ps.setTimestamp(10, java.sql.Timestamp
            .valueOf("2011-02-28 11:59:59.99999"));
        ps.setString(11, "This is char fixed width upto 254 ...  ঋ এ ঐ ও ");
        int begin = PartitionedRegion.rand.nextInt(vstr.length()-1);
        ps
            .setString(
                12, vstr.substring(begin, vstr.length()));
        ps.setInt(13, row+1);
        ps.setInt(14, row+2);
        
        ps.executeUpdate();
      }

      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter() {
            private static final long serialVersionUID = 1L;

            public int overrideSortBufferSize(ColumnOrdering[] columnOrdering,
                int sortBufferMax) {
              return 100;
            }

            public boolean avoidMergeRuns() {
              return false;
            }
          });

//      ResultSet rs = st
//          .executeQuery("select cr, tm, vcr, dbl, lv, fl, dml, ts, nmr, dt, id from test order by ts, vcr, dt");
//      while (rs.next()) {
//        System.out.println(rs.getInt("id"));
//      }

      ResultSet rs = st
          .executeQuery("select id, id2, id3, dml, ts from test order by id2 desc, dml");
      while (rs.next()) {
        System.out.println(rs.getInt("id"));
//        TestUtil.getLogger().fine(Integer.toString(rs.getInt("id"));
      }
      
    } finally {
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter());
      TestUtil.shutDown();
    }
  }

  public void test43041() throws Exception {
    
    final SQLChar[] chars = new SQLChar[] { new SQLChar("abcd ef gh ij"), new SQLChar("another huge string that will cross buffer boundary ") } ;
    final SQLTimestamp ts = new SQLTimestamp("2011-04-08 19:35:00.995", true, null);

    Connection conn = TestUtil.getConnection();

    // write chars & integer crossing over rwBuffer.
    FileStreamInputOutput fileIO = new FileStreamInputOutput(-1,
        FileStreamInputOutput.allocateBuffer(10, null));

    chars[0].toData(fileIO);
    chars[1].toData(fileIO);
    ts.toData(fileIO);
    
    fileIO.flushAndSync(false);
    fileIO.flipToRead();
    final SQLChar[] readchars = new SQLChar[2];
    final SQLTimestamp readts;

    readchars[0] = (SQLChar)DataType.readDVD(fileIO);
    readchars[1] = (SQLChar)DataType.readDVD(fileIO);
    readts = (SQLTimestamp)DataType.readDVD(fileIO);
    assertTrue(chars[0].equals(readchars[0]));
    assertTrue(chars[1].equals(readchars[1]));
    assertTrue(ts.equals(readts));
    
    fileIO = new FileStreamInputOutput(-2,
        FileStreamInputOutput.allocateBuffer(5, null));

    fileIO.writeByte(127);
    fileIO.writeByte(126);
    fileIO.writeByte(125);
    chars[0].toData(fileIO);
    chars[0].toData(fileIO);
    fileIO.flushAndSync(false);
    fileIO.flipToRead();
    assertEquals(127,fileIO.readByte());
    assertEquals(126,fileIO.readByte());
    assertEquals(125,fileIO.readByte());
    SQLChar c = (SQLChar)DataType.readDVD(fileIO);
    assertEquals(c, chars[0]);
    c = (SQLChar)DataType.readDVD(fileIO);
    assertEquals(c, chars[0]);

    conn.close();
    TestUtil.shutDown();
  }

  /**
   * Currently disabled due to problems with ResourceManager EVICTION/CRITICAL
   * events. Need to take into account the young/survivor gen memory too since
   * that will get moved to tenured heap in a big chunk (that causes CRITICAL to
   * be raised directly with no EVICTION_UP notification).
   */
  public void PERF_testSortPerformance() throws Exception {
    System.setProperty("gemfirexd.TraceTempFileIO", "true");
    Properties p = new Properties();
    p.setProperty("log-level", "config");
    p.setProperty("critical-heap-percentage", "95");
    p.setProperty("eviction-heap-percentage", "72");
    TestUtil.reduceLogLevel("config");
    Connection conn = TestUtil.getConnection(p);

    Statement st = conn.createStatement();
    try {
      st.execute("drop table if exists test");
    } catch (Exception i) {
    }

    GemFireXDQueryObserverHolder
        .setInstance(new GemFireXDQueryObserverAdapter() {
      private static final long serialVersionUID = 1L;

      @Override
      public int overrideSortBufferSize(ColumnOrdering[] columnOrdering,
          int sortBufferMax) {
        return sortBufferMax;
      }

      @Override
      public boolean avoidMergeRuns() {
        return false;
      }
    });

    try {
      st.execute("create table test (id varchar(40), id1 varchar(2000), "
          + "id2 int, id3 int) replicate persistent asynchronous "
          + "eviction by lruheappercent evictaction overflow");
      PreparedStatement ps = conn
          .prepareStatement("insert into test values(?, ?, ?, ?) ");

      for (int c = 1; c <= 3; c++) {
        final char[] pre = new char[1990];
        Arrays.fill(pre, 'a');
        final String prefix = new String(pre);
        final int numItems = 500000;
        final int maxId2 = 10;
        for (int i = 1; i <= numItems; i++) {
          int rnd = PartitionedRegion.rand.nextInt(numItems);
          ps.setString(1, "strkey-" + rnd);
          ps.setString(2, prefix + rnd);
          ps.setInt(3, i % maxId2);
          ps.setInt(4, i % 50);
          ps.execute();
        }
        System.gc();
        System.runFinalization();
        // wait if EVICTION_UP is still in play to let table get evicted
        GfxdHeapThresholdListener heapListener = Misc.getMemStore()
            .thresholdListener();
        while (heapListener.isEviction() || heapListener.isCritical()) {
          SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_TEMP_FILE_IO,
              "waiting for EVICTION_DOWN...");
          System.gc();
          System.runFinalization();
          Thread.sleep(1000);
        }

        long start, end;
        int n;
        ResultSet rs;

        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_TEMP_FILE_IO,
            "starting ORDER BY query");
        start = System.nanoTime();
        n = 0;
        rs = st.executeQuery("select * from test order by id");
        // now check if properly sorted
        String prev = "";
        while (rs.next()) {
          String str = rs.getString(1);
          if ((c % 2) == 1) {
            if (str.compareTo(prev) < 0) {
              fail("incorrect ordering for '" + str + "', prev='" + prev + "'");
            }
          }
          prev = str;
          n++;
        }
        rs.close();
        rs = null;
        if (n != numItems) {
          fail("inserted " + numItems + " but got " + n);
        }
        end = System.nanoTime();
        System.out.println("Time taken for sorting: "
            + ((end - start) / 1000000.0) + " millis");

        System.gc();
        System.runFinalization();

        // check for an aggregate+sort
        SanityManager.DEBUG_PRINT(GfxdConstants.TRACE_TEMP_FILE_IO,
            "starting SUM+GROUP BY+ORDER BY query");
        start = System.nanoTime();
        n = 0;
        rs = st.executeQuery("select id2, sum(id3) from test "
            + "group by id2 order by id2");
        int prevId2 = Integer.MIN_VALUE;
        while (rs.next()) {
          int id2 = rs.getInt(1);
          if ((c % 2) == 1) {
            if (id2 < prevId2) {
              fail("incorrect ordering for " + id2 + ", prev=" + prevId2);
            }
          }
          prevId2 = id2;
          n++;
        }
        rs.close();
        rs = null;
        if (n != maxId2) {
          fail("inserted " + maxId2 + " groups but got " + n);
        }
        end = System.nanoTime();
        System.out.println("Time taken for aggregate+sorting: "
            + ((end - start) / 1000000.0) + " millis");

        st.execute("truncate table test");
        System.gc();
        System.runFinalization();
      }
    } finally {
      System.clearProperty("gemfirexd.TraceTempFileIO");
      GemFireXDQueryObserverHolder
          .setInstance(new GemFireXDQueryObserverAdapter());
      TestUtil.shutDown();
    }
  }

  public void noneeded_test43041() throws SQLException {

    EmbedConnection conn = (EmbedConnection)TestUtil.getConnection();
    Statement st = conn.createStatement();

    st
        .execute("create table trade.customers "
            + " (cid int not null, cust_name varchar(100), since date, addr varchar(100), tid int, primary key (cid))   "
            + " partition by range (cid) "
            + " ( VALUES BETWEEN 0 AND 599, VALUES BETWEEN 599 AND 902, "
            + "   VALUES BETWEEN 902 AND 1255, VALUES BETWEEN 1255 AND 1678, "
            + "   VALUES BETWEEN 1678 AND 100000)");

    st
        .execute("create table trade.portfolio "
            + " (cid int not null, sid int not null, qty int not null, availQty int not null, subTotal decimal(30,20), tid int, "
            + "  constraint portf_pk primary key (cid, sid), constraint cust_fk foreign key (cid) references trade.customers (cid) on delete restrict, "
            + "  constraint sec_fk foreign key (sid) references trade.securities (sec_id), "
            + "  constraint qty_ck check (qty>=0), constraint avail_ch check (availQty>=0 and availQty<=qty))   "
            + "  partition by range (cid) ( VALUES BETWEEN 0 AND 599, VALUES BETWEEN 599 AND 902, "
            + "  VALUES BETWEEN 902 AND 1255, VALUES BETWEEN 1255 AND 1678, "
            + "  VALUES BETWEEN 1678 AND 100000) "
            + " colocate with (trade.customers)");

    st
        .execute("create table trade.sellorders "
            + " (oid int not null constraint orders_pk primary key, cid int, sid int, qty int, ask decimal (30, 20), order_time timestamp, status varchar(10) default 'open', tid int, "
            + " constraint portf_fk foreign key (cid, sid) references trade.portfolio (cid, sid) on delete restrict, "
            + " constraint status_ch check (status in ('cancelled', 'open', 'filled')))   "
            + " partition by range (cid) ( VALUES BETWEEN 0 AND 599, VALUES BETWEEN 599 AND 902, "
            + "  VALUES BETWEEN 902 AND 1255, VALUES BETWEEN 1255 AND 1678, VALUES BETWEEN 1678 AND 100000) "
            + " colocate with (trade.customers) ");

    PreparedStatement psC = conn
        .prepareStatement("insert into trade.customers values (?, ?, ?, ?, ?) ");
    PreparedStatement psP = conn
        .prepareStatement("insert into trade.portfolio values (?, ?, ?, ?, ?, ?) ");
    PreparedStatement psS = conn
        .prepareStatement("insert into trade.sellorders values (?, ?, ?, ?, ?, ?, ?) ");

    st
        .executeQuery("select f.cid, cust_name, f.sid, so.sid, so.qty, subTotal, oid, order_time, ask "
            + "from trade.customers c, trade.portfolio f, trade.sellorders so "
            + "where c.cid= f.cid and f.sid = so.sid and c.cid = so.cid and subTotal >10000 and f.cid>? order by order_time");

  }

  private class TestRowSource implements RowSource {

    private final EmbedResultSet ers;

    TestRowSource(EmbedResultSet _ers) {
      ers = _ers;
    }

    @Override
    public ExecRow getNextRowFromRowSource() throws StandardException {
      try {
        ers.next();
      } catch (SQLException e) {
        throw StandardException.newException(
            "SQLException occured in test row source", e);
      }
      return ers.getCurrentRow();
    }

    @Override
    public void closeRowSource() {
    }

    @Override
    public FormatableBitSet getValidColumns() {
      return null;
    }

    @Override
    public boolean needsToClone() {
      return false;
    }

  }

}
