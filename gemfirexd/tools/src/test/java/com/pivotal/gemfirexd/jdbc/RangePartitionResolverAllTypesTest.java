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
package com.pivotal.gemfirexd.jdbc;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.util.Date;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.PartitionResolver;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdRangePartitionResolver;
import com.pivotal.gemfirexd.internal.iapi.error.StandardException;
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor;
import com.pivotal.gemfirexd.internal.iapi.types.SQLDate;
import com.pivotal.gemfirexd.internal.iapi.types.SQLDouble;
import com.pivotal.gemfirexd.internal.iapi.types.SQLInteger;
import com.pivotal.gemfirexd.internal.iapi.types.SQLTime;
import com.pivotal.gemfirexd.internal.iapi.types.SQLTimestamp;
import com.pivotal.gemfirexd.internal.iapi.types.SQLVarchar;

import junit.framework.TestSuite;
import junit.textui.TestRunner;

// This test is aimed to test the functioning of RangePartitionResolver for all kinds of possible data types.
// Right now it is testing:
//    1. Integer
//    2. String
//    3. DECIMAL
//    4. Date
//    5. Time
//    6. Date Time or TimeStamp
//
// For all the data types the two tests below namely testRanges and testRangesInfinity will run.

public class RangePartitionResolverAllTypesTest extends JdbcTestBase {

  private static String[] Region_Names = { "/EMP/PARTITIONTESTTABLE_INT",
      "/EMP/PARTITIONTESTTABLE_STRING", "/EMP/PARTITIONTESTTABLE_DECIMAL",
      "/EMP/PARTITIONTESTTABLE_DATE", "/EMP/PARTITIONTESTTABLE_TIME",
      "/EMP/PARTITIONTESTTABLE_TIMESTAMP" };

  private static String[] Set1_CreateTableStmntDiffDataType = {
      // Integer
      "create table EMP.PARTITIONTESTTABLE_INT (ID int not null, "
          + " DESCRIPTION varchar(1024) not null, primary key (ID))"
          + "PARTITION BY RANGE ( ID )"
          + " ( VALUES BETWEEN 20 and 40, VALUES BETWEEN 40 and 60, "
          + "VALUES BETWEEN 60 and 80, VALUES BETWEEN 90 and 100 )"

      // String
      ,
      "create table EMP.PARTITIONTESTTABLE_STRING (ID varchar(1024) not null, "
          + " DESCRIPTION varchar(1024) not null, primary key (ID))"
          + "PARTITION BY RANGE ( ID )"
          + " ( VALUES BETWEEN 'abc' and 'abg', VALUES BETWEEN 'abg' and 'abk', "
          + "VALUES BETWEEN 'abk' and 'abp', VALUES BETWEEN 'abt' and 'abz' )"

      // decimal
      ,
      "create table EMP.PARTITIONTESTTABLE_DECIMAL (ID decimal not null, "
          + " DESCRIPTION varchar(1024) not null, primary key (ID))"
          + "PARTITION BY RANGE ( ID )"
          + " ( VALUES BETWEEN 1.1 and 5.1, VALUES BETWEEN 5.1 and 10.1, "
          + "VALUES BETWEEN 10.1 and 15.1, VALUES BETWEEN 20.1 and 25.1 )"

      // Date
      ,
      "create table EMP.PARTITIONTESTTABLE_DATE (ID date not null, "
          + " DESCRIPTION varchar(1024) not null, primary key (ID))"
          + "PARTITION BY RANGE ( ID )"
          + " ( VALUES BETWEEN {d '2000-01-01'} and {d '2000-06-30'}, VALUES BETWEEN {d '2000-06-30'} and {d '2000-12-31'}, "
          + "VALUES BETWEEN {d '2000-12-31'} and {d '2008-08-30'}, VALUES BETWEEN {d '2008-12-20'} and {d '2009-05-01'} )"

      // Time
      ,
      "create table EMP.PARTITIONTESTTABLE_TIME (ID Time not null, "
          + " DESCRIPTION varchar(1024) not null, primary key (ID))"
          + "PARTITION BY RANGE ( ID )"
          + " ( VALUES BETWEEN {t '00:20:00'} and {t '00:40:00'}, VALUES BETWEEN {t '00:40:00'} and {t '01:20:00'}, "
          + "VALUES BETWEEN {t '01:20:00'} and {t '02:30:30'}, VALUES BETWEEN {t '03:00:00'} and {t '04:00:00'} )"

      // Timestamp
      ,
      "create table EMP.PARTITIONTESTTABLE_TIMESTAMP (ID Timestamp not null, "
          + " DESCRIPTION varchar(1024) not null, primary key (ID))"
          + "PARTITION BY RANGE ( ID )"
          + " ( VALUES BETWEEN {ts '2000-01-01 00:01:00'} and {ts '2000-06-30 00:01:00'}, VALUES BETWEEN {ts '2000-06-30 00:01:00'} and {ts '2000-12-31 00:01:00'}, "
          + "VALUES BETWEEN {ts '2000-12-31 00:01:00'} and {ts '2008-08-30 00:01:00'}, VALUES BETWEEN {ts '2008-12-20 00:01:00'} and {ts '2009-05-01 00:01:00'} )" };

  private static SQLDate getDate(String s) {
    try {
      return new SQLDate(s, true, null);
    } catch (StandardException ex) {
      ex.printStackTrace();
      return null;
    }
  }

  private static SQLTime getTime(String s) {
    try {
      return new SQLTime(s, true, null);
    } catch (StandardException ex) {
      ex.printStackTrace();
      return null;
    }
  }

  private static SQLTimestamp getTimeStamp(String s) {
    try {
      return new SQLTimestamp(s, true, null);
    } catch (StandardException ex) {
      ex.printStackTrace();
      return null;
    }
  }

  private static SQLDouble getDouble(double d) {
    try {
      return new SQLDouble(d);
    } catch (StandardException ex) {
      ex.printStackTrace();
      return null;
    }
  }
  
  private static Object[] Set1_params = {
  // Integer
      new Object[] {
          new Object[] { new SQLInteger(25), Boolean.TRUE, new SQLInteger(75),
              Boolean.TRUE,
              new Integer[] { new Integer(1), new Integer(2), new Integer(3) } },

          new Object[] { new SQLInteger(25), Boolean.TRUE, new SQLInteger(75),
              Boolean.FALSE,
              new Integer[] { new Integer(1), new Integer(2), new Integer(3) } },

          new Object[] { new SQLInteger(25), Boolean.TRUE, new SQLInteger(60),
              Boolean.FALSE, new Integer[] { new Integer(1), new Integer(2) } },

          new Object[] { new SQLInteger(25), Boolean.FALSE, new SQLInteger(75),
              Boolean.TRUE,
              new Integer[] { new Integer(1), new Integer(2), new Integer(3) } },

          new Object[] { new SQLInteger(40), Boolean.TRUE, new SQLInteger(75),
              Boolean.TRUE, new Integer[] { new Integer(2), new Integer(3) } },

          new Object[] { new SQLInteger(15), Boolean.TRUE, new SQLInteger(75),
              Boolean.TRUE, null },

          new Object[] { new SQLInteger(40), Boolean.FALSE, new SQLInteger(60),
              Boolean.FALSE, new Integer[] { new Integer(2) } },

          new Object[] { new SQLInteger(4), Boolean.TRUE, new SQLInteger(95),
              Boolean.TRUE, null },

          new Object[] { new SQLInteger(20), Boolean.TRUE, new SQLInteger(30),
              Boolean.TRUE, new Integer[] { new Integer(1) } },

          new Object[] { new SQLInteger(60), Boolean.TRUE, new SQLInteger(60),
              Boolean.TRUE, new Integer[] { new Integer(3) } } },

      // String
      new Object[] {
          new Object[] { new SQLVarchar("abd"), Boolean.TRUE, new SQLVarchar("abm"),
              Boolean.TRUE,
              new Integer[] { new Integer(1), new Integer(2), new Integer(3) } },

          new Object[] { new SQLVarchar("abd"), Boolean.TRUE, new SQLVarchar("abm"),
              Boolean.FALSE,
              new Integer[] { new Integer(1), new Integer(2), new Integer(3) } },

          new Object[] { new SQLVarchar("abd"), Boolean.TRUE, new SQLVarchar("abk"),
              Boolean.FALSE, new Integer[] { new Integer(1), new Integer(2) } },

          new Object[] { new SQLVarchar("abd"), Boolean.FALSE, new SQLVarchar("abm"),
              Boolean.TRUE,
              new Integer[] { new Integer(1), new Integer(2), new Integer(3) } },

          new Object[] { new SQLVarchar("abg"), Boolean.TRUE, new SQLVarchar("abm"),
              Boolean.TRUE, new Integer[] { new Integer(2), new Integer(3) } },

          new Object[] { new SQLVarchar("aba"), Boolean.TRUE, new SQLVarchar("abm"),
              Boolean.TRUE, null },

          new Object[] { new SQLVarchar("abg"), Boolean.FALSE, new SQLVarchar("abk"),
              Boolean.FALSE, new Integer[] { new Integer(2) } },

          new Object[] { new SQLVarchar("abg"), Boolean.TRUE, new SQLVarchar("abv"),
              Boolean.TRUE, null },

          new Object[] { new SQLVarchar("abc"), Boolean.TRUE, new SQLVarchar("abe"),
              Boolean.TRUE, new Integer[] { new Integer(1) } },

          new Object[] { new SQLVarchar("abk"), Boolean.TRUE, new SQLVarchar("abk"),
              Boolean.TRUE, new Integer[] { new Integer(3) } } },

      // Float
      new Object[] {
          new Object[] { getDouble(2.1), Boolean.TRUE,
              getDouble(13.1), Boolean.TRUE,
              new Integer[] { new Integer(1), new Integer(2), new Integer(3) } },

          new Object[] { getDouble(2.1), Boolean.TRUE,
              getDouble(13.1), Boolean.FALSE,
              new Integer[] { new Integer(1), new Integer(2), new Integer(3) } },

          new Object[] { getDouble(2.1), Boolean.TRUE,
              getDouble(10.1), Boolean.FALSE,
              new Integer[] { new Integer(1), new Integer(2) } },

          new Object[] { getDouble(2.1), Boolean.FALSE,
              getDouble(13.1), Boolean.TRUE,
              new Integer[] { new Integer(1), new Integer(2), new Integer(3) } },

          new Object[] { getDouble(5.1), Boolean.TRUE,
              getDouble(13.1), Boolean.TRUE,
              new Integer[] { new Integer(2), new Integer(3) } },

          new Object[] { getDouble(0.1), Boolean.TRUE,
              getDouble(13.1), Boolean.TRUE, null },

          new Object[] { getDouble(5.1), Boolean.FALSE,
              getDouble(10.1), Boolean.FALSE,
              new Integer[] { new Integer(2) } },

          new Object[] { getDouble(5.1), Boolean.TRUE,
              getDouble(27.1), Boolean.TRUE, null },

          new Object[] { getDouble(1.1), Boolean.TRUE,
              getDouble(4.1), Boolean.TRUE,
              new Integer[] { new Integer(1) } },

          new Object[] { getDouble(10.1), Boolean.TRUE,
              getDouble(10.1), Boolean.TRUE,
              new Integer[] { new Integer(3) } } },

      // Date
      new Object[] {
          new Object[] { getDate("2000-03-03"), Boolean.TRUE,
              getDate("2004-12-31"), Boolean.TRUE,
              new Integer[] { new Integer(1), new Integer(2), new Integer(3) } },

          new Object[] { getDate("2000-03-03"), Boolean.TRUE,
              getDate("2004-12-31"), Boolean.FALSE,
              new Integer[] { new Integer(1), new Integer(2), new Integer(3) } },

          new Object[] { getDate("2000-03-03"), Boolean.TRUE,
              getDate("2000-12-31"), Boolean.FALSE,
              new Integer[] { new Integer(1), new Integer(2) } },

          new Object[] { getDate("2000-03-03"), Boolean.FALSE,
              getDate("2004-12-31"), Boolean.TRUE,
              new Integer[] { new Integer(1), new Integer(2), new Integer(3) } },

          new Object[] { getDate("2000-06-30"), Boolean.TRUE,
              getDate("2004-12-31"), Boolean.TRUE,
              new Integer[] { new Integer(2), new Integer(3) } },

          new Object[] { getDate("1999-01-03"), Boolean.TRUE,
              getDate("2004-12-31"), Boolean.TRUE, null },

          new Object[] { getDate("2000-06-30"), Boolean.FALSE,
              getDate("2000-12-31"), Boolean.FALSE,
              new Integer[] { new Integer(2) } },

          new Object[] { getDate("2000-06-30"), Boolean.TRUE,
              getDate("2009-03-03"), Boolean.TRUE, null },

          new Object[] { getDate("2000-01-01"), Boolean.TRUE,
              getDate("2000-03-03"), Boolean.TRUE,
              new Integer[] { new Integer(1) } },

          new Object[] { getDate("2000-12-31"), Boolean.TRUE,
              getDate("2000-12-31"), Boolean.TRUE,
              new Integer[] { new Integer(3) } } },

      // Time
      new Object[] {
          new Object[] { getTime("00:30:00"), Boolean.TRUE,
              getTime("02:00:00"), Boolean.TRUE,
              new Integer[] { new Integer(1), new Integer(2), new Integer(3) } },

          new Object[] { getTime("00:30:00"), Boolean.TRUE,
              getTime("02:00:00"), Boolean.FALSE,
              new Integer[] { new Integer(1), new Integer(2), new Integer(3) } },

          new Object[] { getTime("00:30:00"), Boolean.TRUE,
              getTime("01:20:00"), Boolean.FALSE,
              new Integer[] { new Integer(1), new Integer(2) } },

          new Object[] { getTime("00:30:00"), Boolean.FALSE,
              getTime("02:00:00"), Boolean.TRUE,
              new Integer[] { new Integer(1), new Integer(2), new Integer(3) } },

          new Object[] { getTime("00:40:00"), Boolean.TRUE,
              getTime("02:00:00"), Boolean.TRUE,
              new Integer[] { new Integer(2), new Integer(3) } },
          // 6
          new Object[] { getTime("00:10:00"), Boolean.TRUE,
              getTime("02:00:00"), Boolean.TRUE, null },

          new Object[] { getTime("00:40:00"), Boolean.FALSE,
              getTime("01:20:00"), Boolean.FALSE,
              new Integer[] { new Integer(2) } },

          new Object[] { getTime("00:40:00"), Boolean.TRUE,
              getTime("03:30:00"), Boolean.TRUE, null },

          new Object[] { getTime("00:20:00"), Boolean.TRUE,
              getTime("00:30:00"), Boolean.TRUE,
              new Integer[] { new Integer(1) } },

          new Object[] { getTime("01:20:00"), Boolean.TRUE,
              getTime("01:20:00"), Boolean.TRUE,
              new Integer[] { new Integer(3) } } },
      //
      // Timestamp
      new Object[] {
          new Object[] { getTimeStamp("2000-03-01 00:01:00"), Boolean.TRUE,
              getTimeStamp("2004-01-01 00:01:00"), Boolean.TRUE,
              new Integer[] { new Integer(1), new Integer(2), new Integer(3) } },

          new Object[] { getTimeStamp("2000-03-01 00:01:00"), Boolean.TRUE,
              getTimeStamp("2004-01-01 00:01:00"), Boolean.FALSE,
              new Integer[] { new Integer(1), new Integer(2), new Integer(3) } },

          new Object[] { getTimeStamp("2000-03-01 00:01:00"), Boolean.TRUE,
              getTimeStamp("2000-12-31 00:01:00"), Boolean.FALSE,
              new Integer[] { new Integer(1), new Integer(2) } },

          new Object[] { getTimeStamp("2000-03-01 00:01:00"), Boolean.FALSE,
              getTimeStamp("2004-01-01 00:01:00"), Boolean.TRUE,
              new Integer[] { new Integer(1), new Integer(2), new Integer(3) } },
          // 5
          new Object[] { getTimeStamp("2000-06-30 00:01:00"), Boolean.TRUE,
              getTimeStamp("2004-01-01 00:01:00"), Boolean.TRUE,
              new Integer[] { new Integer(2), new Integer(3) } },

          new Object[] { getTimeStamp("1990-01-01 00:01:00"), Boolean.TRUE,
              getTimeStamp("2004-01-01 00:01:00"), Boolean.TRUE, null },

          new Object[] { getTimeStamp("2000-06-30 00:01:00"), Boolean.FALSE,
              getTimeStamp("2000-12-31 00:01:00"), Boolean.FALSE,
              new Integer[] { new Integer(2) } },
          // 8
          new Object[] { getTimeStamp("2000-06-30 00:01:00"), Boolean.TRUE,
              getTimeStamp("2009-01-01 00:01:00"), Boolean.TRUE, null },

          new Object[] { getTimeStamp("2000-01-01 00:01:00"), Boolean.TRUE,
              getTimeStamp("2000-03-01 00:01:00"), Boolean.TRUE,
              new Integer[] { new Integer(1) } },

          new Object[] { getTimeStamp("2000-12-31 00:01:00"), Boolean.TRUE,
              getTimeStamp("2000-12-31 00:01:00"), Boolean.TRUE,
              new Integer[] { new Integer(3) } } } };

  private static String[] Set2_CreateTableStmntDiffDataType = {
      // Integer
      "create table EMP.PARTITIONTESTTABLE_INT (ID int not null, "
          + " DESCRIPTION varchar(1024) not null, primary key (ID))"
          + " PARTITION BY RANGE ( ID )"
          + " ( VALUES BETWEEN -INFINITY and 40, VALUES BETWEEN 50 and +INFINITY )"

      // String
      ,
      "create table EMP.PARTITIONTESTTABLE_STRING (ID varchar(1024) not null, "
          + " DESCRIPTION varchar(1024) not null, primary key (ID))"
          + " PARTITION BY RANGE ( ID )"
          + " ( VALUES BETWEEN -INFINITY and 'abc', VALUES BETWEEN 'abt' and +INFINITY )"

      // decimal
      ,
      "create table EMP.PARTITIONTESTTABLE_DECIMAL (ID decimal not null, "
          + " DESCRIPTION varchar(1024) not null, primary key (ID))"
          + " PARTITION BY RANGE ( ID )"
          + " ( VALUES BETWEEN -INFINITY and 1.1, VALUES BETWEEN 2.1 and +INFINITY )"

      // Date
      ,
      "create table EMP.PARTITIONTESTTABLE_DATE (ID date not null, "
          + " DESCRIPTION varchar(1024) not null, primary key (ID))"
          + " PARTITION BY RANGE ( ID )"
          + " ( VALUES BETWEEN -INFINITY and {d '2004-01-01'}, VALUES BETWEEN {d '2008-01-01'} and +INFINITY )"

      // Time
      ,
      "create table EMP.PARTITIONTESTTABLE_TIME (ID Time not null, "
          + " DESCRIPTION varchar(1024) not null, primary key (ID))"
          + " PARTITION BY RANGE ( ID )"
          + " ( VALUES BETWEEN -INFINITY and {t '10:20:30'}, VALUES BETWEEN {t '20:30:40'} and +INFINITY )"

      // Timestamp
      ,
      "create table EMP.PARTITIONTESTTABLE_TIMESTAMP (ID Timestamp not null, "
          + " DESCRIPTION varchar(1024) not null, primary key (ID))"
          + " PARTITION BY RANGE ( ID )"
          + " ( VALUES BETWEEN -INFINITY and {ts '2004-01-01 10:10:10'}, VALUES BETWEEN {ts '2008-01-01 20:20:20'} and +INFINITY )" };

  private static Object[] Set2_params = {
  // Integer
      new Object[] {
          new Object[] { new SQLInteger(-10), Boolean.TRUE, new SQLInteger(30),
              Boolean.TRUE, new Integer[] { new Integer(1) } },

          new Object[] { new SQLInteger(10), Boolean.TRUE, new SQLInteger(45),
              Boolean.TRUE, null },

          new Object[] { new SQLInteger(40), Boolean.TRUE, new SQLInteger(60),
              Boolean.TRUE, null },

          new Object[] { new SQLInteger(50), Boolean.TRUE, new SQLInteger(70),
              Boolean.TRUE, new Integer[] { new Integer(2) } },

          new Object[] { null, Boolean.TRUE, new SQLInteger(30), Boolean.TRUE,
              new Integer[] { new Integer(1) } },

          new Object[] { null, Boolean.TRUE, new SQLInteger(45), Boolean.TRUE,
              null },

          new Object[] { new SQLInteger(50), Boolean.TRUE, null, Boolean.TRUE,
              new Integer[] { new Integer(2) } } },

      // String
      new Object[] {
          new Object[] { new SQLVarchar("aba"), Boolean.TRUE, new SQLVarchar("abb"),
              Boolean.TRUE, new Integer[] { new Integer(1) } },

          new Object[] { new SQLVarchar("abb"), Boolean.TRUE, new SQLVarchar("abd"),
              Boolean.TRUE, null },

          new Object[] { new SQLVarchar("abc"), Boolean.TRUE, new SQLVarchar("abv"),
              Boolean.TRUE, null },

          new Object[] { new SQLVarchar("abt"), Boolean.TRUE, new SQLVarchar("abv"),
              Boolean.TRUE, new Integer[] { new Integer(2) } },

          new Object[] { null, Boolean.TRUE, new SQLVarchar("abb"), Boolean.TRUE,
              new Integer[] { new Integer(1) } },

          new Object[] { null, Boolean.TRUE, new SQLVarchar("abd"), Boolean.TRUE,
              null },

          new Object[] { new SQLVarchar("abt"), Boolean.TRUE, null, Boolean.TRUE,
              new Integer[] { new Integer(2) } } },

      // Float
      new Object[] {
          new Object[] { getDouble(-1.1), Boolean.TRUE,
              getDouble(0.1), Boolean.TRUE,
              new Integer[] { new Integer(1) } },

          new Object[] { getDouble(0.1), Boolean.TRUE,
              getDouble(1.4), Boolean.TRUE, null },

          new Object[] { getDouble(1.1), Boolean.TRUE,
              getDouble(3.1), Boolean.TRUE, null },

          new Object[] { getDouble(2.1), Boolean.TRUE,
              getDouble(5.1), Boolean.TRUE,
              new Integer[] { new Integer(2) } },

          new Object[] { null, Boolean.TRUE, getDouble(0.1), Boolean.TRUE,
              new Integer[] { new Integer(1) } },

          new Object[] { null, Boolean.TRUE, getDouble(1.5), Boolean.TRUE,
              null },

          new Object[] { getDouble(2.1), Boolean.TRUE, null, Boolean.TRUE,
              new Integer[] { new Integer(2) } } },

      // Date
      new Object[] {
          new Object[] { getDate("1999-01-01"), Boolean.TRUE,
              getDate("2003-01-01"), Boolean.TRUE,
              new Integer[] { new Integer(1) } },

          new Object[] { getDate("2003-08-08"), Boolean.TRUE,
              getDate("2005-04-04"), Boolean.TRUE, null },

          new Object[] { getDate("2004-01-01"), Boolean.TRUE,
              getDate("2009-01-01"), Boolean.TRUE, null },

          new Object[] { getDate("2008-01-01"), Boolean.TRUE,
              getDate("2009-01-01"), Boolean.TRUE,
              new Integer[] { new Integer(2) } },

          new Object[] { null, Boolean.TRUE, getDate("2003-08-08"), Boolean.TRUE,
              new Integer[] { new Integer(1) } },
          
          new Object[] { null, Boolean.TRUE, getDate("2005-01-01"), Boolean.TRUE,
              null },
          
          new Object[] { getDate("2008-01-01"), Boolean.TRUE, null, Boolean.TRUE,
              new Integer[] { new Integer(2) } }
      },

      // Time
      new Object[] {
          new Object[] { getTime("5:20:30"), Boolean.TRUE, getTime("08:20:30"),
              Boolean.TRUE, new Integer[] { new Integer(1) } },

          new Object[] { getTime("5:20:30"), Boolean.TRUE, getTime("11:20:30"),
              Boolean.TRUE, null },

          new Object[] { getTime("10:20:30"), Boolean.TRUE,
              getTime("21:30:40"), Boolean.TRUE, null },

          new Object[] { getTime("20:30:40"), Boolean.TRUE,
              getTime("22:30:40"), Boolean.TRUE,
              new Integer[] { new Integer(2) } },

         new Object[] { null, Boolean.TRUE, getTime("08:20:30"), Boolean.TRUE,
              new Integer[] { new Integer(1) } },
      
         new Object[] { null, Boolean.TRUE, getTime("11:20:30"), Boolean.TRUE,
              null },
      
         new Object[] { getTime("20:30:40"), Boolean.TRUE, null, Boolean.TRUE,
              new Integer[] { new Integer(2) } }
      },
      //
      // Timestamp
      new Object[] {
          new Object[] { getTimeStamp("1996-01-01 10:10:10"), Boolean.TRUE,
              getTimeStamp("2003-01-01 10:10:10"), Boolean.TRUE,
              new Integer[] { new Integer(1) } },

          new Object[] { getTimeStamp("2003-01-01 10:10:10"), Boolean.TRUE,
              getTimeStamp("2005-01-01 10:10:10"), Boolean.TRUE, null },

          new Object[] { getTimeStamp("2004-01-01 10:10:10"), Boolean.TRUE,
              getTimeStamp("2009-01-01 10:10:10"), Boolean.TRUE, null },

          new Object[] { getTimeStamp("2008-01-01 20:20:20"), Boolean.TRUE,
              getTimeStamp("2009-01-01 10:10:10"), Boolean.TRUE,
              new Integer[] { new Integer(2) } },

          new Object[] { null, Boolean.TRUE, getTimeStamp("2003-01-01 10:10:10"),
              Boolean.TRUE,
              new Integer[] { new Integer(1) } },
      
          new Object[] { null, Boolean.TRUE, getTimeStamp("2006-01-01 10:10:10"),
              Boolean.TRUE,
              null },
      
          new Object[] { getTimeStamp("2008-01-01 20:20:20"), Boolean.TRUE, null,
              Boolean.TRUE,
              new Integer[] { new Integer(2) } } 
      } };

  public static void main(String[] args) {
    TestRunner.run(new TestSuite(RangePartitionResolverAllTypesTest.class));
  }

  public RangePartitionResolverAllTypesTest(String name) {
    super(name);
  }

  public void testRanges() throws SQLException, StandardException, IOException {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create schema EMP");

    Cache cache = CacheFactory.getAnyInstance();
    for (int i = 0; i < Set1_CreateTableStmntDiffDataType.length; i++) {
      System.out.println("Region Name = " + Region_Names[i]);
      s.execute(Set1_CreateTableStmntDiffDataType[i]);
      Region regtwo = cache.getRegion(Region_Names[i]);
      RegionAttributes rattr = regtwo.getAttributes();
      PartitionResolver pr = rattr.getPartitionAttributes()
          .getPartitionResolver();
      GfxdRangePartitionResolver rpr = (GfxdRangePartitionResolver)pr;

      Object[] eachTypeArray = (Object[])Set1_params[i];
      for (int eachCase = 0; eachCase < eachTypeArray.length; eachCase++) {
        Object[] params = (Object[])eachTypeArray[eachCase];
        Object[] routingObjects = rpr.getRoutingObjectsForRange((DataValueDescriptor)params[0],
            ((Boolean)params[1]).booleanValue(), (DataValueDescriptor)params[2],
            ((Boolean)params[3]).booleanValue());
        Integer[] verifyRoutingObjects = (Integer[])params[4];
        if (verifyRoutingObjects == null) {
          assertNull(routingObjects);
        }
        else {
          assertEquals(verifyRoutingObjects.length, routingObjects.length);
          for (int v = 0; v < verifyRoutingObjects.length; v++) {
            assertEquals(verifyRoutingObjects[v], routingObjects[v]);
          }
        }
      }

    }
    s.close();
    conn.close();
  }

  public void testRangesInfinity() throws SQLException, StandardException {
    Connection conn = getConnection();
    Statement s = conn.createStatement();
    s.execute("create schema EMP");

    Cache cache = CacheFactory.getAnyInstance();
    for (int i = 0; i < Set2_CreateTableStmntDiffDataType.length; i++) {
      s.execute(Set2_CreateTableStmntDiffDataType[i]);
      Region regtwo = cache.getRegion(Region_Names[i]);
      System.out.println("Region Name = " + Region_Names[i]);
      RegionAttributes rattr = regtwo.getAttributes();
      PartitionResolver pr = rattr.getPartitionAttributes()
          .getPartitionResolver();
      GfxdRangePartitionResolver rpr = (GfxdRangePartitionResolver)pr;

      Object[] eachTypeArray = (Object[])Set2_params[i];
      // System.out.println("eachTypeArray index = " + eachDataType);
      for (int eachCase = 0; eachCase < eachTypeArray.length; eachCase++) {
        Object[] params = (Object[])eachTypeArray[eachCase];
        Object[] routingObjects = rpr.getRoutingObjectsForRange((DataValueDescriptor)params[0],
            ((Boolean)params[1]).booleanValue(), (DataValueDescriptor)params[2],
            ((Boolean)params[3]).booleanValue());
        Integer[] verifyRoutingObjects = (Integer[])params[4];
        if (verifyRoutingObjects == null) {
          assertNull(routingObjects);
        }
        else {
          assertEquals(verifyRoutingObjects.length, routingObjects.length);
          for (int v = 0; v < verifyRoutingObjects.length; v++) {
            assertEquals(verifyRoutingObjects[v], routingObjects[v]);
          }
        }
      }

    }
    s.close();
    conn.close();
  }
}
