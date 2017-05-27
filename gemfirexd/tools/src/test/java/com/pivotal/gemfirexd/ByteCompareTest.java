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
package com.pivotal.gemfirexd;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;

import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.pivotal.gemfirexd.internal.engine.store.RowFormatter;
import com.pivotal.gemfirexd.internal.iapi.types.SQLChar;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;
import com.pivotal.gemfirexd.jdbc.JdbcTestBase;
import com.pivotal.gemfirexd.query.SingleTablePredicatesCheckDUnit;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

public class ByteCompareTest extends JdbcTestBase {

  public ByteCompareTest(String name) {
    super(name);
  }

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(ByteCompareTest.class));
  }

  /**
   * Test for string comparisons from SQL layer for the optimized byte[]
   * comparisons
   */
  public void testTableCharComparisons() throws Exception {
    setupConnection();
    final Statement stmt = jdbcConn.createStatement();
    stmt.execute("create table trade.customers (cust_id char(10), cid int, "
        + "cust_name varchar(100) default 'unknown', since date, "
        + "addr varchar(100), tid int, primary key (cid)) partition by range "
        + "(cid) (VALUES BETWEEN 0 AND 999, VALUES BETWEEN 1000 AND 1102, "
        + "VALUES BETWEEN 1103 AND 1250, VALUES BETWEEN 1251 AND 1677, "
        + "VALUES BETWEEN 1678 AND 10000)" + getOffHeapSuffix());

    // perform some inserts of null, non-null and default values in different
    // columns
    stmt.execute("insert into trade.customers values ('id1', 1, "
        + "'cust1', '2010-01-01', 'addr1', 1)");
    stmt.execute("insert into trade.customers values (NULL, 10, "
        + "'cust10', '2010-01-01', 'addr10', 10)");
    stmt.execute("insert into trade.customers values ('id100', 100, "
        + "NULL, '2010-01-01', 'addr100', 100)");
    stmt.execute("insert into trade.customers values ('id1000', 1000, "
        + "'cust1000', '2010-01-01', NULL, 1000)");
    stmt.execute("insert into trade.customers values (NULL, 10000, "
        + "'cust10000', '2010-01-01', NULL, 10000)");
    stmt.execute("insert into trade.customers (cust_id, cid, since, addr, tid)"
        + " values (NULL, 100000, '2010-01-01', NULL, 100000)");

    PreparedStatement ps;
    ResultSet rs;

    // now selects with incoming value as null or non-null, check for default
    // value and for space sorting

    // for cust_id
    rs = stmt.executeQuery("select * from trade.customers "
        + "where cust_id='id100'");
    assertTrue(rs.next());
    assertEquals(100, rs.getInt("CID"));
    assertFalse(rs.next());

    rs = stmt.executeQuery("select * from trade.customers "
        + "where cust_id is null order by cid");
    assertTrue(rs.next());
    assertEquals(10, rs.getInt(2));
    assertTrue(rs.next());
    assertEquals(10000, rs.getInt(2));
    assertTrue(rs.next());
    assertEquals(100000, rs.getInt(2));
    assertFalse(rs.next());

    rs = stmt.executeQuery("select * from trade.customers "
        + "where cust_id > 'id' order by cid");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(2));
    assertTrue(rs.next());
    assertEquals(100, rs.getInt(2));
    assertTrue(rs.next());
    assertEquals(1000, rs.getInt(2));
    assertFalse(rs.next());

    ps = jdbcConn.prepareStatement("select * from trade.customers "
        + "where cust_id > ? order by cid");
    ps.setString(1, "id100\n");
    rs = ps.executeQuery();
    assertTrue(rs.next());
    assertEquals(100, rs.getInt(2));
    assertTrue(rs.next());
    assertEquals(1000, rs.getInt(2));
    assertFalse(rs.next());

    // for cust_name
    rs = stmt.executeQuery("select * from trade.customers "
        + "where cust_name='cust1000'");
    assertTrue(rs.next());
    assertEquals(1000, rs.getInt(2));
    assertFalse(rs.next());

    rs = stmt.executeQuery("select * from trade.customers "
        + "where cust_name is null order by cid");
    assertTrue(rs.next());
    assertEquals(100, rs.getInt("CID"));
    assertFalse(rs.next());

    rs = stmt.executeQuery("select * from trade.customers "
        + "where cust_name > 'cust1' order by cid");
    assertTrue(rs.next());
    assertEquals(10, rs.getInt(2));
    assertTrue(rs.next());
    assertEquals(1000, rs.getInt(2));
    assertTrue(rs.next());
    assertEquals(10000, rs.getInt("CID"));
    assertTrue(rs.next());
    assertEquals(100000, rs.getInt(2));
    assertFalse(rs.next());

    ps = jdbcConn.prepareStatement("select * from trade.customers "
        + "where cust_name > ? order by cid");
    ps.setString(1, "cust1\n");
    rs = ps.executeQuery();
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(2));
    assertTrue(rs.next());
    assertEquals(10, rs.getInt(2));
    assertTrue(rs.next());
    assertEquals(1000, rs.getInt(2));
    assertTrue(rs.next());
    assertEquals(10000, rs.getInt("CID"));
    assertTrue(rs.next());
    assertEquals(100000, rs.getInt(2));
    assertFalse(rs.next());

    // for addr
    rs = stmt.executeQuery("select * from trade.customers "
        + "where addr='addr1'");
    assertTrue(rs.next());
    assertEquals(1, rs.getInt(2));
    assertFalse(rs.next());

    rs = stmt.executeQuery("select * from trade.customers "
        + "where addr is null order by cid");
    assertTrue(rs.next());
    assertEquals(1000, rs.getInt("CID"));
    assertTrue(rs.next());
    assertEquals(10000, rs.getInt(2));
    assertTrue(rs.next());
    assertEquals(100000, rs.getInt(2));
    assertFalse(rs.next());

    rs = stmt.executeQuery("select * from trade.customers "
        + "where addr > 'addr10' order by cid");
    assertTrue(rs.next());
    assertEquals(100, rs.getInt(2));
    assertFalse(rs.next());

    ps = jdbcConn.prepareStatement("select * from trade.customers "
        + "where addr > ? order by cid");
    ps.setString(1, "addr10\n");
    rs = ps.executeQuery();
    assertTrue(rs.next());
    assertEquals(10, rs.getInt(2));
    assertTrue(rs.next());
    assertEquals(100, rs.getInt("CID"));
    assertFalse(rs.next());
    
    rs = stmt.executeQuery("select * from trade.customers "
        + " order by addr  NULLS FIRST, cid ");
    assertTrue(rs.next());
    assertEquals(1000, rs.getInt("CID"));
    assertTrue(rs.next());
    assertEquals(10000, rs.getInt("CID"));
    assertTrue(rs.next());
    assertEquals(100000, rs.getInt("CID"));
    assertTrue(rs.next());
    assertEquals(1, rs.getInt("CID"));
    assertTrue(rs.next());
    assertEquals(10, rs.getInt("CID"));
    assertTrue(rs.next());
    assertEquals(100, rs.getInt("CID"));
    assertFalse(rs.next());

    stmt.close();
  }
  
  public void testSingleTablePredicates() throws Exception {
    final Connection conn = TestUtil.getConnection();
    final Statement stmt = jdbcConn.createStatement();
    boolean isOffHeap = getOffHeapSuffix().toLowerCase().indexOf("offheap") != -1;
    Double dblPrice = PartitionedRegion.rand.nextDouble();
    SingleTablePredicatesCheckDUnit.prepareTable(dblPrice, false, false, conn, isOffHeap);

    ResultSet rs = stmt.executeQuery("select ID, DESCRIPTION from TESTTABLE "
        + "where ID = 5 and DESCRIPTION like 'First%' "
        + "Order By DESCRIPTION ASC NULLS FIRST");

    assertTrue(rs.next());
    assertEquals("First5", rs.getString(2));
    assertFalse(rs.next());

    rs = stmt
        .executeQuery(
            "select ID, DESCRIPTION from TESTTABLE where ID  >= 5 And ID <= 7 Order By DESCRIPTION ASC NULLS FIRST");

    assertTrue(rs.next());
    assertNull(rs.getString(2));
    assertTrue(rs.next());
    assertEquals("First5", rs.getString(2));
    assertTrue(rs.next());
    assertEquals("First7", rs.getString(2));
    assertFalse(rs.next());

    rs = stmt
        .executeQuery(
            "select ID, DESCRIPTION from TESTTABLE where ID  >= 5 And ID <= 7 Order By DESCRIPTION ASC NULLS LAST");

    assertTrue(rs.next());
    assertEquals("First5", rs.getString(2));
    assertTrue(rs.next());
    assertEquals("First7", rs.getString(2));
    assertTrue(rs.next());
    assertNull(rs.getString(2));
    assertFalse(rs.next());

    
    rs = stmt
        .executeQuery("Select count(src2), cast(sum(distinct src) as REAL)/count(src2) from testtable" +
        		" group by substr(description, 1, length(description)-1 )");
    assertTrue(rs.next());
    assertEquals(3.0F, rs.getFloat(2));
    assertTrue(rs.next());
    assertEquals(3.0F, rs.getFloat(2));
    assertTrue(rs.next());
    assertEquals(2.5F, rs.getFloat(2));
    assertFalse(rs.next());
    
    rs = stmt
        .executeQuery("select distinct * from TESTTABLE where SRC > 1 order by address desc nulls first, id desc");    
    
    assertTrue(rs.next());
    assertNull(rs.getString(2));
    assertTrue(rs.next());
    assertNull(rs.getString(2));
    assertTrue(rs.next());
    assertNull(rs.getString(2));
    assertTrue(rs.next());
    assertEquals("First13", rs.getString(2));
    assertTrue(rs.next());
    assertEquals("First11", rs.getString(2));
    assertTrue(rs.next());
    assertEquals("First7", rs.getString(2));
    assertTrue(rs.next());
    assertNull(rs.getString(2));
    assertTrue(rs.next());
    assertNull(rs.getString(2));
    assertTrue(rs.next());
    assertEquals("First1", rs.getString(2));
    assertTrue(rs.next());
    assertEquals("First9", rs.getString(2));
    assertTrue(rs.next());
    assertEquals("First3", rs.getString(2));
    assertFalse(rs.next());
  }

  public void testCharComparisons() throws Exception {

    String s1 = "test1";
    String s2 = "test10";
    
    int res = compareStringBytes(getBytes(s1), getBytes(s2), s1, s2);
    res = res > 0 ? 1 : res < 0 ? -1 : 0;

    int chres = SQLChar.stringCompare(s1, s2);
    
    System.out.println("Result " + res + (res != chres ? " != " : " == ") + chres);
    
    String rstrs[] = new String[10000];
    byte[][] rstrsbytes = new byte[rstrs.length][];
    final int spaceRatio = rstrs.length / 10;
    for (int i = 0; i < rstrs.length - spaceRatio; i++) {
      rstrs[i] = getRandomAlphaNumericString( i % 991 % 4);
      rstrsbytes[i] = getBytes(rstrs[i]);
    }
    
    for (int i = rstrs.length - spaceRatio; i < rstrs.length; i++) {
      int randPick = PartitionedRegion.rand.nextInt(rstrs.length - spaceRatio);
      StringBuilder sb = new StringBuilder(rstrs[randPick]);
      for(int spaces = 0; spaces < randPick + 10; spaces++)
        sb.append(" ");
      rstrs[i] = sb.toString();
      rstrsbytes[i] = getBytes(rstrs[i]);
    }

    long totalbytetime = 0, totalstrtime = 0;
    long currenttotalbytetime = 0, currenttotalstrtime = 0;
    for (int runno = 0; runno < rstrs.length; runno++) {
       int[] bytecomp = new int[rstrs.length];
       int[] strcomp  = new int[rstrs.length];
       
       long timeByteBegin = System.currentTimeMillis();
       for(int src = 0; src < rstrs.length; src++) {
          bytecomp[src] = compareStringBytes(rstrsbytes[runno], rstrsbytes[src], rstrs[runno], rstrs[src]) ;
       }
       currenttotalbytetime += (System.currentTimeMillis() - timeByteBegin);
       
       long timeStrBegin = System.currentTimeMillis();
       for(int src = 0; src < rstrs.length; src++) {
          strcomp[src] = SQLChar.stringCompare(rstrs[runno], rstrs[src]);
       }
       currenttotalstrtime += (System.currentTimeMillis() - timeStrBegin);
       
       for(int src = 0; src < rstrs.length; src++) {
          bytecomp[src] = (bytecomp[src] > 0 ? 1 : bytecomp[src] < 0 ? -1 : 0);
          if( bytecomp[src] !=  strcomp[src]) {
            System.out.println(rstrs[src]);
            String msg = "Failed " + rstrs[runno] + " == " + rstrs[src] + "\t((" + bytecomp[src] + " != " + strcomp[src] + "))";
            msg.replaceAll("\n", "\r\n");
            System.out.println("Failed ");
            System.out.println("str1=" + rstrs[runno] );
            System.out.println("str2=" + rstrs[src] );
            SanityManager.THROWASSERT(msg);
          }
       }
       
       if(runno %1000 == 0) {
          String msg = (rstrs.length * 100 ) + " byte comparisons took "
              + (currenttotalbytetime ) + " ms and " + rstrs.length
              + " str comparisons took " + (currenttotalstrtime ) + " ms ";
          System.out.println(msg);
        
         totalbytetime += currenttotalbytetime;
         totalstrtime += currenttotalstrtime;
         currenttotalbytetime = 0; currenttotalstrtime = 0;
         
         if(runno %1000 == 0) {
           System.out.println(runno + " done");
         }
       }
    }
    
    System.out.println("Byte T " + (totalbytetime ) );
    System.out.println("Str  T " + (totalstrtime ) );
  }
  
  private static byte[] getBytes(String st) {
    int strlen = st.length();
    HeapDataOutputStream out = new HeapDataOutputStream(strlen+32);
    boolean isLongUTF = false;
    // for length than 64K, see format description above
    int utflen = strlen;
    for (int i = 0 ; (i < strlen) && (utflen <= 65535); i++)
    {
        int c = st.charAt(i);
        if ((c >= 0x0001) && (c <= 0x007F))
        {
            // 1 byte for character
        }
        else if (c > 0x07FF)
        {
            utflen += 2; // 3 bytes for character
        }
        else
        {
            utflen += 1; // 2 bytes for character
        }
    }
    
    if (utflen > 65535)
    {
        isLongUTF = true;
        utflen = 0;
    }
    
    for (int i = 0 ; i < strlen ; i++)
    {
        int c = st.charAt(i);
        if ((c >= 0x0001) && (c <= 0x007F))
        {
            out.writeByte((byte)(c & 0xFF));
        }
        else if (c > 0x07FF)
        {
            out.writeByte((byte)(0xE0 | ((c >> 12) & 0x0F)));
            out.writeByte((byte)(0x80 | ((c >>  6) & 0x3F)));
            out.writeByte((byte)(0x80 | ((c >>  0) & 0x3F)));
        }
        else
        {
            out.writeByte((byte)(0xC0 | ((c >>  6) & 0x1F)));
            out.writeByte((byte)(0x80 | ((c >>  0) & 0x3F)));
        }
    }

    if (isLongUTF)
    {
      // write the following 3 bytes to terminate the string:
      // (11100000, 00000000, 00000000)
        out.writeByte((byte)(0xE0 & 0xFF));
        out.writeByte(0);
        out.writeByte(0);
    }

    //handle zero length string.
    if(! isLongUTF && strlen == 0) {
      out.writeByte(-1);
    }

    return out.toByteArray();
  }

  public void __testIntComparisons() throws Exception {
    
  }
  
  public void __testDateComparisons() {
     int dt1 = (2007 << 16) | (02 << 8) | 01;
     int dt2 = (2007 << 16) | (02 << 8) | 02;
     
     byte[] dtb1 = new byte[4];
     byte[] dtb2 = new byte[4];
     RowFormatter.writeInt(dtb1, dt1, 0);
     RowFormatter.writeInt(dtb2, dt2, 0);
     
     int res = compareDateBytes(dtb1, dtb2);
     System.out.println("date comp res " + res + " expected " + (dt1 > dt2 ? 1 : dt1 < dt2 ? -1 : 0) );
     
     int rdts[] = new int[10000]; 
     byte[][] rdtsbytes = new byte[rdts.length][4]; 
     for (int i = 0; i < rdts.length; i++) {
       rdts[i] = (PartitionedRegion.rand.nextInt(9999) << 16) |
           (PartitionedRegion.rand.nextInt(12) << 8) |
           PartitionedRegion.rand.nextInt(31);
       RowFormatter.writeInt(rdtsbytes[i], rdts[i], 0);
     }
     
     long totalbytetime = 0, totaldttime = 0;
     long currenttotalbytetime = 0, currenttotaldttime = 0;
     for (int runno = 0; runno < rdts.length; runno++) {
        int[] bytecomp = new int[rdts.length];
        int[] dbcomp  = new int[rdts.length];
        
        long timeByteBegin = System.currentTimeMillis();
        for(int src = 0; src < rdts.length; src++) {
           bytecomp[src] = compareDateBytes(rdtsbytes[runno], rdtsbytes[src]) ;
        }
        currenttotalbytetime += (System.currentTimeMillis() - timeByteBegin);
        
        long timeStrBegin = System.currentTimeMillis();
        for(int src = 0; src < rdts.length; src++) {
           dbcomp[src] = (rdts[runno] > rdts[src] ? 1 : rdts[runno] < rdts[src] ? -1 : 0);
        }
        currenttotaldttime += (System.currentTimeMillis() - timeStrBegin);
        
        for(int src = 0; src < rdts.length; src++) {
          if (!((bytecomp[src] > 0 && dbcomp[src] > 0)
              || (bytecomp[src] < 0 && dbcomp[src] < 0) 
              || (bytecomp[src] == 0 && dbcomp[src] == 0))
           ) {
             System.out.println(rdts[src]);
             String msg = "Failed " + rdts[runno] + " == " + rdts[src] + "\t((" + bytecomp[src] + " != " + dbcomp[src] + "))";
             msg.replaceAll("\n", "\r\n");
             System.err.println(msg);
             SanityManager.THROWASSERT(msg);
           }
//           else {
//             System.out.println(rdbs[src]);
//             String msg = "SUCCESS " + rdbs[runno] + " == " + rdbs[src] + "\t((" + bytecomp[src] + " != " + dbcomp[src] + "))";
//             msg.replaceAll("\n", "\r\n");
//             System.out.println(msg);
//           }
        }
        
        if(runno %1000 == 0) {
           String msg = (rdts.length * 100 ) + " byte comparisons took "
               + (currenttotalbytetime ) + " ms and " + rdts.length
               + " dt comparisons took " + (currenttotaldttime ) + " ms ";
           System.out.println(msg);
         
          totalbytetime += currenttotalbytetime;
          totaldttime += currenttotaldttime;
          currenttotalbytetime = 0; currenttotaldttime = 0;
          
          if(runno %1000 == 0) {
            System.out.println(runno + " done");
          }
        }
     }
     
     System.out.println("Byte    T " + (totalbytetime ) );
     System.out.println("Int Dt  T " + (totaldttime ) );
  }
  
  public void __testDoubleComparisons() throws Exception {
    
    byte[] db1 = getBytes(0.6322981066856577d);
    byte[] db2 = getBytes(0.39386921313482437d);
    int r = compareDoubleBytes(db1, db2, 0.6322981066856577d, 0.39386921313482437d);
    if( compareDoubles(0.6322981066856577d, 0.39386921313482437d) != r) {
      throw new Exception("Not equal");
    }
    
    double rdbs[] = new double[10000];
    byte[][] rdbsbytes = new byte[rdbs.length][];
    for (int i = 0; i < rdbs.length; i++) {
      rdbs[i] = PartitionedRegion.rand.nextDouble();
      rdbsbytes[i] = getBytes(rdbs[i]);
    }
    
    long totalbytetime = 0, totaldbtime = 0;
    long currenttotalbytetime = 0, currenttotalstrtime = 0;
    for (int runno = 0; runno < rdbs.length; runno++) {
       int[] bytecomp = new int[rdbs.length];
       int[] dbcomp  = new int[rdbs.length];
       
       long timeByteBegin = System.currentTimeMillis();
       for(int src = 0; src < rdbs.length; src++) {
          bytecomp[src] = compareDoubleBytes(rdbsbytes[runno], rdbsbytes[src], rdbs[runno], rdbs[src]) ;
       }
       currenttotalbytetime += (System.currentTimeMillis() - timeByteBegin);
       
       long timeStrBegin = System.currentTimeMillis();
       for(int src = 0; src < rdbs.length; src++) {
          dbcomp[src] = compareDoubles(rdbs[runno], rdbs[src]);
       }
       currenttotalstrtime += (System.currentTimeMillis() - timeStrBegin);
       
       for(int src = 0; src < rdbs.length; src++) {
        if (!((bytecomp[src] > 0 && dbcomp[src] > 0)
               || (bytecomp[src] < 0 && dbcomp[src] < 0) 
               || (bytecomp[src] == 0 && dbcomp[src] == 0))
            ) {
            System.out.println(rdbs[src]);
            String msg = "Failed " + rdbs[runno] + " == " + rdbs[src] + "\t((" + bytecomp[src] + " != " + dbcomp[src] + "))";
            msg.replaceAll("\n", "\r\n");
            System.err.println(msg);
            SanityManager.THROWASSERT(msg);
          }
//          else {
//            System.out.println(rdbs[src]);
//            String msg = "SUCCESS " + rdbs[runno] + " == " + rdbs[src] + "\t((" + bytecomp[src] + " != " + dbcomp[src] + "))";
//            msg.replaceAll("\n", "\r\n");
//            System.out.println(msg);
//          }
       }
       
       if(runno %1000 == 0) {
          String msg = (rdbs.length * 100 ) + " byte comparisons took "
              + (currenttotalbytetime ) + " ms and " + rdbs.length
              + " double comparisons took " + (currenttotalstrtime ) + " ms ";
          System.out.println(msg);
        
         totalbytetime += currenttotalbytetime;
         totaldbtime += currenttotalstrtime;
         currenttotalbytetime = 0; currenttotalstrtime = 0;
         
         if(runno %1000 == 0) {
           System.out.println(runno + " done");
         }
       }
    }
    
    System.out.println("Byte    T " + (totalbytetime ) );
    System.out.println("Double  T " + (totaldbtime ) );
    
  }
  
  private static byte[] getBytes(double d) {
    
    long bits = Double.doubleToLongBits(d);

    byte[] lb = new byte[8];
    RowFormatter.writeLong(lb, bits, 0);
    
    return lb;
  }
  
  public int compareDateBytes(byte[] dtb1, byte[] dtb2) {

    short yr1 = (short)((dtb1[3] << 8) | dtb1[2] & 0xff);
    short yr2 = (short)((dtb2[3] << 8) | dtb2[2] & 0xff);
    
    if (yr1 == yr2) {
      int res = dtb1[1] - dtb2[1];
      return res != 0 ? res : dtb1[0] - dtb2[0];
    }
    else {
      return yr1 - yr2;
    }
    
  }
  
  public int compareDoubleBytes(byte[] db1, byte[] db2, double d1, double d2) throws Exception {
    long l1 = RowFormatter.readLong(db1, 0);
    long l2 = RowFormatter.readLong(db2, 0);
    
    if( l1 > l2)
      return 1;
    else if (l1 < l2)
      return -1;
    else
      return 0;
    
//    double dd1 = Double.longBitsToDouble(l1);
//    double dd2 = Double.longBitsToDouble(l2);
//    
//    if(dd1 > dd2) {
//      return 1;
//    }
//    if(dd1 < dd2) {
//      return -1;
//    }
//    else {
//      return 0;
//    }
    
//    
//    System.out.println("Comparing " + dd1 + "("+d1+") == " + dd2 + "("+d2+") , expected result " + (d1 > d2 ? 1 : d1 < d2 ? -1 : 0));
    
//    byte b1 = db1[0];
//    byte b2 = db2[0];
//
//    /* checking sign bit */
//    if( (b1 >>> 7) > (b2 >>> 7) ) {
//      return 1;
//    }
//    else if( (b1 >>> 7) < (b2 >>> 7) ) {
//      return -1;
//    }
//
//    /* checking exponent */
//    short e1 = (short)( ((b1 << 5) & 0xff) | (db1[1] >> 4) & 0xff) ;
//    short e2 = (short)( ((b2 << 5) & 0xff) | (db2[1] >> 4) & 0xff) ;
//    
//    if( e1 > e2 ) {
//      return 1;
//    }
//    else if( e1 < e2) {
//      return -1;
//    }
//
//    int m1 = (db1[1] << 4) & 0xff;
//    m1 = (m1 << 4) | db1[2] & 0xff;
//    m1 = (m1 << 8) | db1[3] & 0xff;
//    m1 = (m1 << 8) | db1[4] & 0xff;
//    m1 = (m1 << 8) | db1[5] & 0xff;
//    m1 = (m1 << 8) | db1[6] & 0xff;
//    m1 = (m1 << 8) | db1[7] & 0xff;
//    
//    int m2 = (db2[1] << 4) & 0xff;
//    m2 = (m2 << 4) | db2[2] & 0xff;
//    m2 = (m2 << 8) | db2[3] & 0xff;
//    m2 = (m2 << 8) | db2[4] & 0xff;
//    m2 = (m2 << 8) | db2[5] & 0xff;
//    m2 = (m2 << 8) | db2[6] & 0xff;
//    m2 = (m2 << 8) | db2[7] & 0xff;
//    
//    
//    if( m1 > m2) {
//      return 1;
//    }
//    else if( m1 < m2) {
//      return -1;
//    }
//    else {
//      return 0;
//    }
    
  }
  
  public int comparebyte(byte b1, byte b2) {
    return b1 == b2 ? 0 : b1 > b2 ? 1 : -1;
  }
  
  public int compareDoubles(double d1, double d2) {

    if (d1 == d2)
      return 0;
    else if (d1 > d2)
      return 1;
    else
      return -1;
  }
  
  
  public int compareStringBytes(byte[] sb1, byte[] sb2, String s1, String s2) {
    final int leftlen = sb1.length;
    final int rightlen = sb2.length;
    
    int         posn;
    int         retvalIfLTSpace;
    byte[]      remainingString;
    int         remainingLen;

    /*
    ** By convention, nulls sort High, and null == null
    */
    if (sb1 == null || sb2 == null)
    {
        if (sb1 != null)    // s2 == null
            return -1;
        if (sb2 != null)    // s1 == null
            return 1;
        return 0;           // both null
    }
    
    /*
    ** Compare characters until we find one that isn't equal, or until
    ** one String or the other runs out of characters.
    */
    int shorterLen = leftlen < rightlen ? leftlen : rightlen;
    int ltposn = 0, rtposn = 0;
    
    for (; ltposn < shorterLen && rtposn < shorterLen; )
    {
          final byte lb = sb1[ltposn];
          final byte rb = sb2[rtposn];
          if ((lb & 0x80) == 0x00) {
              // 1 byte
              if ((rb & 0x80) == 0x00) {
                int res = sb1[ltposn] - sb2[rtposn];
                if (res != 0) {
                  return res;
                }
                else {
                  ltposn++;
                  rtposn++;
                  continue;
                }
              }
    
              return -1;
          }
          // left two byte character
          else if ((lb & 0x60) == 0x40) // we know the top bit is set here
          {
              // right 1 byte
              if ((rb & 0x80) == 0x00) {
                return 1;
              }
    
              // right three bytes
              if (((rb & 0x70) == 0x60)) {
                return -1;
              }
    
              ltposn += 1; rtposn += 1;
              
              final byte l2b = sb1[ltposn];
              final byte r2b = sb2[rtposn];
      
              int res = l2b - r2b;
              if(res != 0) {
                return res;
              }
      
              res = lb - rb;
              if (res != 0) {
                return res;
              }
              else {
                ltposn++;
                rtposn++;
                continue;
              }
    
            // actualChar = (char)(((c & 0x1F) << 6) | (char2 & 0x3F));
          }
          // left 3 bytes
          else if ((lb & 0x70) == 0x60) // we know the top bit is set here
          {
    
              // right 1 byte
              if ((rb & 0x80) == 0x00) {
                return 1;
              }
    
              // right two bytes
              if ((rb & 0x60) == 0x40) {
                return 1;
              }
    
              ltposn += 2; rtposn += 2;
              final byte l3b = sb1[ltposn];
              final byte r3b = sb2[rtposn];
      
              int res = l3b - r3b;
              if (res != 0) {
                return res;
              }
              
              final byte l2b = sb1[ltposn-1];
              final byte r2b = sb2[rtposn-1];
              res = l2b - r2b;
              if (res != 0) {
                return res;
              }
              
              if (lb != rb) {
                return lb - rb;
              }
              else {
                ltposn++;
                rtposn++;
                continue;
              }
          }
    } //end of for

    
    /*
    ** All the characters are equal up to the length of the shorter
    ** string.  If the two strings are of equal length, the values are
    ** equal.
    */
    if (leftlen == rightlen) {
        return 0;
    }

      
    /*
    ** One string is shorter than the other.  Compare the remaining
    ** characters in the longer string to spaces (the SQL standard says
    ** that in this case the comparison is as if the shorter string is
    ** padded with blanks to the length of the longer string.
    */
    if( rtposn > rightlen)
    {
        /*
        ** Remaining characters are on the left.
        */

        /* If a remaining character is less than a space, 
         * return -1 (op1 < op2) */
        retvalIfLTSpace = -1;
        remainingString = sb1;
        posn = ltposn;
        remainingLen = leftlen;
    }
    else 
    {
        /*
        ** Remaining characters are on the right.
        */

        /* If a remaining character is less than a space, 
         * return 1 (op1 > op2) */
        retvalIfLTSpace = 1;
        remainingString = sb2;
        posn = rtposn;
        remainingLen = rightlen;
    }

    /* Look at the remaining characters in the longer string */
    for ( ; posn < remainingLen; posn++)
    {
        byte    remainingChar;

        /*
        ** Compare the characters to spaces, and return the appropriate
        ** value, depending on which is the longer string.
        */

        remainingChar = remainingString[posn];

        if (remainingChar < ' ')
            return retvalIfLTSpace;
        else if (remainingChar > ' ')
            return -retvalIfLTSpace;
    }

    /* The remaining characters in the longer string were all spaces,
    ** so the strings are equal.
    */
    return 0;
  }
  
  private static final char[] alphabets = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ~!@#$%^&*()_+|"
      .toCharArray();
  
  private static final char[] numerics = "0123456789"
    .toCharArray();
  
  private static final char[] specials = " \t ".toCharArray();
  
  private static final char[] unicode1 = "অআইঈউঊঋএঐওঔকখগঘঙচছজঝঞটঠ".toCharArray();
  
  private static final char[] unicode2 = "¡¢£¤¥¦§¨©ª«¬®¯°±²³´µ¶·¸¹º»¼½¾".toCharArray();

  public static String getRandomAlphaNumericString(int pureAscii) {

    final Random rand = PartitionedRegion.rand;

      final int strlen = rand.nextInt(65) + 20;
      
      final int mixby = rand.nextInt(8) + 5;
      
      final char[] userChars = new char[strlen];
      
      int byteoffset = 0;
      
      for(int mix = 1; mix < mixby; mix++) {
        
        final int nextpart = rand.nextInt(strlen/mixby) + 3;
      
        for (int index = byteoffset; index < byteoffset + nextpart && index < strlen; ++index) {
          if(mix % 2 != 0) {
             userChars[index] = alphabets[rand.nextInt(alphabets.length)];
          }
          else {
            userChars[index] = numerics[rand.nextInt(numerics.length)];
          }
        }
        
        byteoffset += nextpart;
      }
      
      if(pureAscii == 0) {
        int ch = strlen - 1 - rand.nextInt(strlen/2);
        userChars[ ch < 0 ? 0 : ch] = specials[rand.nextInt(specials.length)];
      }
      else if(pureAscii == 1) {
        for(int pos = rand.nextInt(strlen), r = rand.nextInt(pos+4); pos < r && pos < strlen; pos++) {
          userChars[pos] = unicode1[rand.nextInt(unicode1.length)];
        }
      }
      else if(pureAscii == 2) {
        for(int pos = rand.nextInt(strlen), r = rand.nextInt(pos+4); pos < r && pos < strlen; pos++) {
          userChars[pos] = unicode2[rand.nextInt(unicode2.length)];
        }
      }
      else if(pureAscii == 3) {
        for(int pos = rand.nextInt(strlen), r = rand.nextInt(pos+4); pos < r && pos < strlen; pos++) {
          userChars[pos] = unicode1[rand.nextInt(unicode1.length)];
        }
        for(int pos = rand.nextInt(strlen), r = rand.nextInt(pos+4); pos < r && pos < strlen; pos++) {
          userChars[pos] = unicode2[rand.nextInt(unicode2.length)];
        }
      }
      
      return String.valueOf(userChars);

  }
  protected String getOffHeapSuffix() {
    return "  ";
  }
  
}
