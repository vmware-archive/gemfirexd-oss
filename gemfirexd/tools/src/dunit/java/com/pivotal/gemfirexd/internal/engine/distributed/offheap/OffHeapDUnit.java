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
package com.pivotal.gemfirexd.internal.engine.distributed.offheap;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.rowset.serial.SerialClob;

import com.pivotal.gemfirexd.DistributedSQLTestBase;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.distributed.metadata.SelectQueryInfo;
import com.pivotal.gemfirexd.internal.iapi.types.HarmonySerialBlob;
import com.pivotal.gemfirexd.jdbc.offheap.OffHeapTest.DataGenerator;
import udtexamples.UDTPrice;

public class OffHeapDUnit extends DistributedSQLTestBase {

  public OffHeapDUnit(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    this.configureDefaultOffHeap(true);
  }

  public void testConcurrentInserts() throws Exception {
    final AtomicInteger ai = new AtomicInteger(0);
    // Start one client and three servers
    startClientVMs(1, 0, null);
    startServerVMs(3, 0, null);
    final Properties props = new Properties();
    props.put(com.pivotal.gemfirexd.Attribute.GFXD_HOST_DATA, "false");
    Connection conn1 = TestUtil.getConnection();
    final CyclicBarrier cb = new CyclicBarrier(5);
    Statement st = conn1.createStatement();
    st.execute("create table t1 (ID int primary key , uid int ,"
        + "col1 int ,constraint uid_uq unique (uid) )"
        + " partition by range (ID) ( VALUES BETWEEN 0 AND 10000) offheap");
    final Random rnd = new Random();
    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        try {
          cb.await();
          Connection conn = TestUtil.getConnection();
          PreparedStatement ps = conn.prepareStatement("insert into "
              + "t1 values(?,?,?)");
          for (int i = 1; i < 10000; ++i) {
            ps.setInt(1, i);
            ps.setInt(2, rnd.nextInt(5000));
            ps.setInt(3, i);
            try {
              ps.executeUpdate();
              ai.incrementAndGet();
            } catch (SQLException sqle) {
              if (sqle.getSQLState().equals("23505")
                  || sqle.toString().indexOf("primary key") != -1) {
                // ok
              } else {
                throw sqle;
              }
            }

          }
        } catch (Exception e) {
          e.printStackTrace();
          fail(e.toString());
        }

      }
    };

    Thread t1, t2, t3, t4, t5;
    t1 = new Thread(runnable);
    t2 = new Thread(runnable);
    t3 = new Thread(runnable);
    t4 = new Thread(runnable);
    t5 = new Thread(runnable);

    t1.start();
    t2.start();
    t3.start();
    t4.start();
    t5.start();

    t1.join();
    t2.join();
    t3.join();
    t4.join();
    t5.join();

  }

  public void testConcurrentUpdates() throws Exception {
    this.reduceLogLevelForTest(getLogLevel());
    final AtomicInteger ai = new AtomicInteger(0);
    // Start one client and three servers
    startClientVMs(1, 0, null);
    startServerVMs(3, 0, null);
    final Properties props = new Properties();
    props.put(com.pivotal.gemfirexd.Attribute.GFXD_HOST_DATA, "false");

    Connection conn1 = TestUtil.getConnection();
    final CyclicBarrier cb = new CyclicBarrier(5);
    Statement st = conn1.createStatement();
    st.execute("create table t1 (ID int primary key , uid int ,"
        + "col1 int ,constraint uid_uq unique (uid) )"
        + " partition by range (ID) ( VALUES BETWEEN 0 AND 10000) offheap");
    final Random rnd = new Random();
    Runnable runnable = new Runnable() {
      @Override
      public void run() {
        try {

          Connection conn = TestUtil.getConnection();
          PreparedStatement ps = conn.prepareStatement("insert into "
              + "t1 values(?,?,?)");
          for (int i = 1; i < 5000; ++i) {
            ps.setInt(1, i);
            ps.setInt(2, rnd.nextInt(6000));
            ps.setInt(3, i);
            try {
              ps.executeUpdate();
              ai.incrementAndGet();
            } catch (SQLException sqle) {
              if (sqle.getSQLState().equals("23505")
                  || sqle.toString().indexOf("primary key") != -1) {
                // ok
              } else {
                throw sqle;
              }
            }

          }
        } catch (Exception e) {
          e.printStackTrace();
          fail(e.toString());
        }

      }
    };
    runnable.run();
    assertTrue(ai.get() > 0);
    final AtomicInteger ai2 = new AtomicInteger(0);
    Runnable update = new Runnable() {
      @Override
      public void run() {
        try {
          cb.await();
          Connection conn = TestUtil.getConnection(props);
          PreparedStatement ps = conn.prepareStatement("update t1 set uid = ?"
              + " where id = ?");
          for (int i = 1; i < 5000; ++i) {
            ps.setInt(1, rnd.nextInt(5300));
            ps.setInt(2, i);

            try {
              int n = ps.executeUpdate();
              if (n > 0)
                ai2.incrementAndGet();
            } catch (SQLException sqle) {
              if (sqle.getSQLState().equals("23505")
                  || sqle.toString().indexOf("primary key") != -1) {
                // ok
              } else {
                throw sqle;
              }
            }

          }
        } catch (Exception e) {
          e.printStackTrace();
          fail(e.toString());
        }

      }
    };
    Thread t1, t2, t3, t4, t5;
    t1 = new Thread(update);
    t2 = new Thread(update);
    t3 = new Thread(update);
    t4 = new Thread(update);
    t5 = new Thread(update);

    t1.start();
    t2.start();
    t3.start();
    t4.start();
    t5.start();

    t1.join();
    t2.join();
    t3.join();
    t4.join();
    t5.join();

    assertTrue(ai2.get() > 0);

  }

  public void testBug49427() throws Exception {

    startClientVMs(1, 0, null);
    startServerVMs(2, 0, null);
    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);
    PreparedStatement psInsert1, psInsert2, psInsert3, psInsert4 = null;
    Statement s = null;
    Connection conn = TestUtil.getConnection();
    s = conn.createStatement();
    try {

      s.execute("CREATE TYPE trade.UDTPrice EXTERNAL NAME 'udtexamples.UDTPrice' LANGUAGE JAVA");
      s.execute("CREATE TYPE trade.UUID EXTERNAL NAME 'java.util.UUID' LANGUAGE JAVA");
      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.
      String tab1 = " create table trade.companies (symbol varchar(10) not null, exchange varchar(10) "
          + " not null, companytype smallint, uid CHAR(16) FOR BIT DATA, uuid trade.UUID, "
          + "companyname char(100), companyinfo clob, note long varchar, histprice trade.udtprice, "
          + "asset bigint, logo varchar(100) for bit data, tid int, "
          + "constraint comp_pk primary key (symbol, exchange)) replicate offheap";

      // We create a table...
      s.execute(tab1);

      psInsert4 = conn.prepareStatement("insert into trade.companies"
          + " values (?, ?, ?,?,?,?,?,?,?,?,?,?)");

      DataGenerator
          .insertIntoCompanies(
              "s",
              "tse",
              (short) 7,
              new byte[] { 123, 119, -95, 80, 60, -94, 78, 12, -106, 37, 126,
                  44, -39, -111, 67, -44 },
              UUID.fromString("7b77a150-3ca2-4e0c-9625-7e2cd99143d4"),
              "i}\\h[?bW1xzZ :jBONH\\f@'A<6XNQh9N`)EkQ)7&!gAl=U$2mx'q3.-:xJ\"!R{@Zp}uG5)J7",
              null, "fcae89533a563a2e648d174f6a7748fb", new UDTPrice(
                  new BigDecimal(28.950000000000001f), new BigDecimal(
                      38.950000000000001f)), 2533297914191977240l, new byte[] {
                  125, -125, 71, 101, -76, -99, 33, -79, -99, 116, -82, -53,
                  -98, 83, 88, -75, -49, 84, 98, 68, 123, -46, -42, 50, -51,
                  -56, 6, -2, -67, 0, 124, 85, -98, -59, -93, 102, 58, -45, 75,
                  -14, 41, -93, 99, 82, -86, 30, -118, -66, -99, 116, -1, -70,
                  -44, 0, -79, -12, -44, 89, -122, -90, 17, 55, -36, -71, 25,
                  111, 13, 89, -65, -53, -25, -89, -72, -10, -57, -89, -119,
                  -92, -15, -67, -98, 111, -52, -106, 20, 19, 123, -43, -42,
                  -42, -27, 17, 92, -65, 22, 101, 29 }, 45, psInsert4);
      try {
        DataGenerator
            .insertIntoCompanies(
                "s",
                "tse",
                (short) 8,
                new byte[] { -81, 112, -67, 52, -86, -2, 78, -73, -91, -105,
                    110, -75, -104, 103, -126, 46 },

                UUID.fromString("af70bd34-aafe-4eb7-a597-6eb59867822e"),
                "2H1z_hP1LZ(Hi0!tPHf\\_Twa-;cJ\\=^3fvxbPTz5UY9xO{`>nSmI'uMFw79RbO wDW;Z>_iyp",
                null, "3c628efc7ab37a3da2f71c515edde23", new UDTPrice(
                    new BigDecimal(25.73), new BigDecimal(35.73)),
                -7292667050878051554l, new byte[] { -124, -90, 51, 9, -112, 9,
                    -83, 31, -86, 42, 29, -59, -116, -104, -126, 7, -54, 83,
                    -95, -90, -104, -96, 121, -116, -40, 71, -49, -49, 7, -62,
                    -6, 34, 84, 79, -115, -33, 46, 79, 37, -68, 104, -81, 7,
                    -108, 58, 71, 106, -86, -55, 91, -70, 47, 82, -103, -37,
                    78, 121, 29, 7, -102, 97, -103, -98, 106, -66, 22, 109,
                    -112, 26, 42, 58, -76, -44, 121, 3, 12, 41, -21, 62, -11 },
                14, psInsert4);
        fail("PK violation expected");
      } catch (SQLException sqle) {
        if (!"23505".equals(sqle.getSQLState())) {
          throw sqle;
        }
      }

      try {
        DataGenerator.insertIntoCompanies("s", "tse", (short) 1,
            new byte[] { 72, -28, 97, -127, 7, 26, 79, 11, -99, -28, 57, 80,
                84, 48, 30, -40 }, UUID
                .fromString("48e46181-071a-4f0b-9de4-395054301ed8"),
            "_2FOQU!_-\"tFam55(+5szXb*Ka/M7<=mLam>+S`)|ksVOTD", new SerialClob(
                "ceaf4ca3ebb26021d1c8f65a7ee4ac".toCharArray()),
            "535179aa524b229485b0572f992a959a", new UDTPrice(new BigDecimal(
                36.31f), new BigDecimal(46.31f)), -3134025377833192825l,
            new byte[] { 116, -91, 90, 14, 71, -122, 62, 39, -86, -20, 3, -18,
                -119, -113, 111, -21, 18, -36, 38, -68, -75, 11, 83, 6, 95, 11,
                -79, 72, 59, -29, 18, 102, 107, -28, 60, -35, -11, -32, -92, 4,
                -113, 96, 92, 82, -46, -97, -90, -42, 4, -34, 97, -100, -122,
                -117, -61, -118, 81, 28, -44, -21, -116, -17, -35, -109, 47,
                -104, 40, -65 }, 47, psInsert4);
        fail("PK violation expected");
      } catch (SQLException sqle) {
        if (!"23505".equals(sqle.getSQLState())) {
          throw sqle;
        }
      }

      try {
        DataGenerator
            .insertIntoCompanies(
                "s",
                "tse",
                (short) 6,
                new byte[] { 36, 121, 0, -67, -96, -103, 79, 104, -127, 101, 9,
                    -103, -5, 8, -91, -105 },

                UUID.fromString("247900bd-a099-4f68-8165-0999fb08a597"),
                "MgOiL'<,?w_L6m2w)DZT?V9[ah)MOa|9SVeL97c*eGcc&HziIS1z1aIp o{&4-w'j\"8Y&i#awy79C\"A)cX",
                new SerialClob("f01aa5c3c3d944d158dcc7c8cad05b".toCharArray()),
                "981825e8ac81c7a98b945aa2c63c40a", new UDTPrice(new BigDecimal(
                    36.06), new BigDecimal(46.06)), -9015588843748895269l,
                new byte[] { -64, -122, 60, 124, 53, -46, 111, 105, -47, 27,
                    46, 8, 113, -39, -89, 39, -1, 110, 57, 55, 30, -99, -76,
                    -98, 30, -100, 83, 45, -10, -89, 1, -125, -47, -98, -70,
                    -9, 100 }, 16, psInsert4);
        fail("PK violation expected");
      } catch (SQLException sqle) {
        if (!"23505".equals(sqle.getSQLState())) {
          throw sqle;
        }
      }

      try {
        DataGenerator.insertIntoCompanies("s", "tse", (short) 3, new byte[] {
            104, 38, -56, -11, 121, 58, 73, -63, -81, 54, 102, 46, -69, -15,
            60, 31 },

        UUID.fromString("6826c8f5-793a-49c1-af36-662ebbf13c1f"),
            "z(#(Adud!mcy'H/CvaHUbg:ygU|", new SerialClob(
                "2b168c8dc33a25cc22faa97f83d545".toCharArray()),
            "fd5b3e2731ffff2b3e442fb8d2844c", new UDTPrice(
                new BigDecimal(28.48), new BigDecimal(38.48)),
            -1353682257425529307l, new byte[] { -32, -23, -12, 116, -98, -106,
                -17, 51, -105, 124, 1, 56, -5, 77, -74, -10, 9, 45, -76, 80,
                -41, -40, 61, 65, -85, -114, -54, 37, 91, 121, -123, -1, -65,
                112, -43, 35, 103, 126, -123, -109, 80, -45, 78 }, 13,
            psInsert4);
        fail("PK violation expected");
      } catch (SQLException sqle) {
        if (!"23505".equals(sqle.getSQLState())) {
          throw sqle;
        }
      }

      try {
        DataGenerator.insertIntoCompanies("s", "tse", (short) 2, new byte[] {
            96, 62, 12, -118, 103, 28, 64, -104, -78, 31, -73, -114, -66, -104,
            53, 18 }, UUID.fromString("603e0c8a-671c-4098-b21f-b78ebe983512"),
            "cW{h%>dG*fQ4Q$F<,LX-{0yx8%Q;ai3mRQi`Cr0gmfy4xEI%$uuJGPp `%B",
            null, "29f8a66124314958358ce1bddad5d6", new UDTPrice(
                new BigDecimal(25.75f), new BigDecimal(35.75f)),
            -5335896508433741988l, new byte[] { 72, -2, 112, -52, -111, -29,
                43, -97, 113, -107, 72, -91, -36, 42, 59, 63, -119, 71, -62,
                -1, -52, -119, -12, -119, 15, -103, 95, -100, 84, -99, 104,
                124, -23, -88, 63, 71, 58, -116, -111, 81, 27, -76, 92, -84,
                -99, -100, 101, 26, 91, 23, -30, -62 }, 53, psInsert4);
        fail("PK violation expected");
      } catch (SQLException sqle) {
        if (!"23505".equals(sqle.getSQLState())) {
          throw sqle;
        }
      }

      s.executeUpdate("delete from trade.companies");

      if (s != null) {
        s.execute("drop table if exists trade.comapnies");
      }
    } finally {
      try {
        DriverManager.getConnection("jdbc:derby:;shutdown=true");
      } catch (SQLException sqle) {
        if (sqle.getMessage().indexOf("shutdown") == -1) {
          sqle.printStackTrace();
          throw sqle;
        }
      }
      GemFireXDQueryObserverHolder.clearInstance();
    }

  }

  public void testBug50432() throws Exception {
    startClientVMs(1, 0, null);
    startServerVMs(1, 0, null);

    Connection conn = TestUtil.getConnection();

    Statement s = conn.createStatement();
    try {
      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.
      byte[] blobBytes = new byte[] { -98, -126, 101, 99, -38, 4, -115 };
      String tab = " create table portfoliov1 (cid int not null, sid int not null, data blob)  OFFHEAP";
      s.execute(tab);
      Blob data = new HarmonySerialBlob(blobBytes);
      PreparedStatement insert = conn
          .prepareStatement("insert into portfoliov1 values (?, ?,?)");
      insert.setInt(1, 659);
      insert.setInt(2, 753);
      insert.setBlob(3, data);
      insert.executeUpdate();

      ResultSet rs = s.executeQuery("select * from portfoliov1");
      assertTrue(rs.next());
      assertEquals(659, rs.getInt(1));
      assertEquals(753, rs.getInt(2));
      Blob out = rs.getBlob(3);
      InputStream is = out.getBinaryStream();
      int size = is.available();
      byte[] resultBytes = new byte[size];
      int read = 0;
      while (read != size) {
        int partRead = is.read(resultBytes, read, size - read);
        read += partRead;
      }
      assertTrue(Arrays.equals(blobBytes, resultBytes));

    } finally {

      try {
        if (s != null) {
          s.execute("drop table if exists portfoliov1");
        }

        DriverManager.getConnection("jdbc:derby:;shutdown=true");
      } catch (SQLException sqle) {
        if (sqle.getMessage().indexOf("shutdown") == -1) {
          sqle.printStackTrace();
          throw sqle;
        }
      }
    }
  }

  public void testBug49014() throws Exception {

    startClientVMs(1, 0, null);
    startServerVMs(2, 0, null);

    SelectQueryInfo.setTestFlagIgnoreSingleVMCriteria(true);

    PreparedStatement psInsert1, psInsert2, psInsert3, psInsert4 = null;
    Statement s = null;

    Connection conn = TestUtil.getConnection();

    s = conn.createStatement();
    try {
      // Creating a statement object that we can use for running various
      // SQL statements commands against the database.

      String tab3 = " create table trade.buyorders(oid int not null constraint buyorders_pk primary key,"
          + " cid int, sid int, qty int, bid decimal (30, 20), ordertime timestamp, status varchar(10),"
          + " tid int, constraint bo_qty_ck check (qty>=0))  offheap";

      String exchange[] = new String[] { "nasdaq", "nye", "amex", "lse", "fse",
          "hkse", "tse" };
      // We create a table...

      s.execute(tab3);
      s.execute("create index i1 on trade.buyorders(status)");
      s.execute("create index i2 on trade.buyorders(cid, sid)");
      s.execute("create index i3 on trade.buyorders(qty)");

      conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      psInsert2 = conn
          .prepareStatement("insert into trade.buyorders values (?, ?,?,?,?,?,?,?)");

      for (int i = 1; i < 3; ++i) {
        psInsert2.setInt(1, i);
        psInsert2.setInt(2, i);
        psInsert2.setInt(3, -1 * i);
        psInsert2.setInt(4, 100 * i);
        psInsert2.setFloat(5, 30.40f);
        psInsert2.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
        psInsert2.setString(7, "cancelled");
        psInsert2.setInt(8, 5);
        assertEquals(1, psInsert2.executeUpdate());
      }
      conn.commit();
      Statement st = conn.createStatement();
      st.executeUpdate("update trade.buyorders set status = 'closed'");
      st.executeUpdate("update trade.buyorders set status = 'cancelled'");

      conn.commit();

      psInsert2.setInt(1, 4);
      psInsert2.setInt(2, 4);
      psInsert2.setInt(3, -1 * 4);
      psInsert2.setInt(4, 100 * 4);
      psInsert2.setFloat(5, 30.40f);
      psInsert2.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
      psInsert2.setString(7, "cancelled");
      psInsert2.setInt(8, 5);
      assertEquals(1, psInsert2.executeUpdate());
      conn.commit();
      s.executeUpdate("delete from trade.buyorders");
      conn.commit();
      if (s != null) {
        s.execute("drop table if exists trade.buyorders");

      }

    } finally {
      try {
        DriverManager.getConnection("jdbc:derby:;shutdown=true");
      } catch (SQLException sqle) {
        if (sqle.getMessage().indexOf("shutdown") == -1) {
          sqle.printStackTrace();
          throw sqle;
        }
      }
      GemFireXDQueryObserverHolder.clearInstance();
    }
  }

  public void testBug51451_clob() throws Exception {
    /*
    Properties props = new Properties();
    props.setProperty("log-level", "fine");
    props.setProperty("gemfirexd.debug.true",
        "QueryDistribution,TraceTran,TraceTranVerbose");
    */
    startVMs(1, 1, 0, null, null);
    Connection cxn = TestUtil.getConnection(null);

    String createTable1 = "create table trade.customers (cid int not null, "
        + "cust_name varchar(100), since date, addr varchar(100), tid int, "
        + "primary key (cid) , clob_details clob , networth_clob clob , "
        + "buyorder_clob clob )  replicate offheap";
    // "buyorder_clob clob )  replicate ";

    String createTable2 = "create table trade.networth (cid int not null, "
        + "cash decimal (30, 20), securities decimal (30, 20), "
        + "loanlimit int, availloan decimal (30, 20),  tid int, "
        + "constraint netw_pk primary key (cid), constraint cust_newt_fk "
        + "foreign key (cid) references trade.customers (cid) on delete restrict, "
        + "constraint cash_ch check (cash>=0), constraint sec_ch check (securities >=0), "
        + "constraint availloan_ck check (loanlimit>=availloan and availloan >=0) , "
        + "clob_details clob ) offheap";
    // "clob_details clob ) ";
    try {
      cxn.createStatement().execute(createTable1);
      cxn.createStatement().execute(createTable2);

      cxn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      cxn.setAutoCommit(false);

      java.sql.PreparedStatement ps1 = cxn
          .prepareStatement("insert into trade.customers values "
              + "(?, ?, ?, ?, ? , ?, ?, ?)");

      for (int i = 1; i <= 5; i++) {
        ps1.setInt(1, i);
        ps1.setString(2, "name" + i);
        ps1.setDate(3, new Date(1980, 1, 1));
        ps1.setString(4, "address" + i);
        ps1.setInt(5, i);
        ps1.setObject(6, "{f" + i + ":" + i + "}");
        ps1.setObject(7, "{f" + i + ":" + i + "}");
        ps1.setObject(8, "{f" + i + ":" + i + "}");
        ps1.addBatch();
      }
      ps1.executeBatch();
      cxn.commit();

      java.sql.PreparedStatement ps2 = cxn
          .prepareStatement("insert into trade.networth values "
              + "(?, ?, ?, ?, ? , ?, ?)");
      for (int i = 1; i <= 5; i++) {
        ps2.setInt(1, i);
        ps2.setInt(2, i);
        ps2.setInt(3, i);
        ps2.setInt(4, i * 2);
        ps2.setInt(5, i);
        ps2.setInt(6, i);
        ps2.setObject(7, "{f1 : " + i + "}");
        ps2.addBatch();
      }
      ps2.executeBatch();
      cxn.commit();

      String updateStatement = "update trade.networth set securities=? ,  "
          + "clob_details = ? where tid  = ?";

      // update a row
      java.sql.PreparedStatement ps3 = cxn.prepareStatement(updateStatement);
      ps3.setInt(1, 1000);
      ps3.setObject(2, "{f1 : " + 1000 + "}");
      ps3.setInt(3, 1); // update the row with tid=1
      ps3.execute();

      // cxn.commit();

      // update the same row again in the same tx
      ps3.setInt(1, 2000);
      ps3.setObject(2, "{f1 : " + 2000 + "}");
      ps3.setInt(3, 1);
      ps3.execute();

      cxn.commit();

      ResultSet rs = cxn.createStatement().executeQuery(
          "select * from trade.networth order by tid");
      int i = 1;
      while (rs.next()) {
        assertEquals(i, rs.getInt(1));
        assertEquals(i, rs.getInt(2));
        if (i == 1) {
          assertEquals(i * 2000, rs.getInt(3));
        } else {
          assertEquals(i, rs.getInt(3));
        }
        assertEquals(i * 2, rs.getInt(4));
        assertEquals(i, rs.getInt(5));
        assertEquals(i, rs.getInt(6));
        rs.getString(7);
        i++;
      }
      Statement s = cxn.createStatement();
      if (s != null) {
        s.execute("drop table if exists trade.networth");
        s.execute("drop table if exists trade.customers");

      }
    } finally {
      try {
        DriverManager.getConnection("jdbc:derby:;shutdown=true");
      } catch (SQLException sqle) {
        if (sqle.getMessage().indexOf("shutdown") == -1) {
          sqle.printStackTrace();
          throw sqle;
        }
      }
      GemFireXDQueryObserverHolder.clearInstance();
    }
  }

}
