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
package cacheLoader.hc;

import com.gemstone.gemfire.cache.*;

import java.sql.*;

import hydra.*;
import util.*;

import oracle.jdbc.OracleResultSet;
import oracle.sql.BLOB;

public class Util { 

    //--------------------------------------------------------------------------
    // DataBase Utilities

    
    /**
     *  Register the JDBC driver
     */
    public static void registerDriver() {
	String driverName = TestConfig.tab().stringAt(DBParms.jdbcDriver);
	Log.getLogWriter().info("Registering " + driverName);
        try{
            DriverManager.registerDriver
		((Driver) Class.forName(driverName).newInstance());
        } catch (Exception e) {
	    throw new TestException(TestHelper.getStackTrace(e));
        }
    }
    
    /**
     *  (Re)Creates table for data.
     *  Loads Blob data in RDB, as per the following
     *  config parms:
     *    numBytes (BLOB size)
     *    numPreload (per slave)
     */
    public static synchronized void reloadTableTask() {
	registerDriver();
	Util utilWorker = new Util();
	utilWorker.reCreateTable();
	utilWorker.preLoadTable();
        utilWorker.dumpTable();
    }

    /**
     *  (Re)Creates table in RDB.
     */
    public void reCreateTable() {
	String tableName = DBParms.getTableName();
        String dropTable1 = "DROP TABLE " + tableName;
        String createTable1 = "CREATE TABLE " + tableName + 
	    " (key " + DBParms.getKeyColumnType() +
	    ", value " + DBParms.getValueColumnType() + ")";
	String dropSeq = "DROP SEQUENCE KEY_SEQ";
	String createSeq = "CREATE SEQUENCE KEY_SEQ START WITH 1";
	try {
	    Connection conn = getUnpooledConnection();
	    Statement stmt = conn.createStatement();
	    Log.getLogWriter().info("Executing " + dropTable1);
	    try {
		stmt.execute(dropTable1);
	    } catch (SQLException e) {}
	    Log.getLogWriter().info("Executing " + dropSeq);
	    try {
		stmt.execute(dropSeq);
	    } catch (SQLException e) {}
	    conn.commit();
	    Log.getLogWriter().info("Executing " + createTable1);
	    stmt.execute(createTable1);
	    Log.getLogWriter().info("Executing " + createSeq);
	    stmt.execute(createSeq);
	    conn.commit();
	    stmt.close();
	    conn.close();
	} catch (SQLException e) {
	    throw new TestException(TestHelper.getStackTrace(e));
        }
    }


    /**
     *  (Re)Creates Blob data in RDB, as per the following
     *  config parms:
     *    numBytes (BLOB size)
     *    numPreload (per slave)
     */
    public void preLoadTable() {
	String tableName = DBParms.getTableName();
	byte[] text;
//	long key;
//	ResultSet rs;
	boolean validate = BridgeParms.getValidate();

	try {
	    Connection conn = getUnpooledConnection();
	    conn.setAutoCommit(false);
	    PreparedStatement pstmt =
		conn.prepareStatement("INSERT INTO " + tableName + 
				      " VALUES (KEY_SEQ.NEXTVAL, ?)"); 

	    int numPreload = TestConfig.tab().intAt(DBParms.numPreload);
	    Log.getLogWriter().info("Inserting " + numPreload + 
				    " rows into " + tableName);
	    for (int i = 0; i < numPreload; i++) {

		// use setBinaryStream (or Oracle-specific version)?
		// see generateAndInsertBlob alternative
		if (validate)
		    pstmt.setBytes(1, getKeyed_arrayOfBytes(i+1));
		else 
		    pstmt.setBytes(1, getRandom_arrayOfBytes());
		pstmt.executeUpdate();
		conn.commit();

	    }
	    pstmt.close();
	    conn.close();

	} catch (SQLException e) {
	    throw new TestException(TestHelper.getStackTrace(e));
	}
    }


    /**
     *  Gets random Blob data in RDB, logs size first few bytes.
     *  Designed for internal testing use.
     */
    public static void getRandomBlobTask() {
	int numPreload = TestConfig.tab().intAt(DBParms.numPreload);
	int key = TestConfig.tab().getRandGen().nextInt(numPreload - 1) + 1;
	byte[] data = getBlob(key);
	byte[] initialBytes = new byte[5];
	if (data.length == 0) {
	    Log.getLogWriter().info("No blob found for " + key);
	} else {
	    for (int i = 0; i < 5; i++) {
		initialBytes[i] = data[i];
	    }
	    if (DBParms.getDriverLogging())
		Log.getLogWriter().info("Retrieved blob for " + key + 
					" containing " + data.length + " bytes" +
					" beginning with " + initialBytes);
	}
    }

    /**
     *    dump entries from table
     */
    public static synchronized void HydraTask_dumpTable() {
	registerDriver();
	Util utilWorker = new Util();
	utilWorker.dumpTable();
    }

    protected void dumpTable() {
      int numPreload = TestConfig.tab().intAt(DBParms.numPreload);
      for (int i = 1; i <= numPreload; i++) {
         getBlob(i);
      }
    }

    /**
     *  Gets Blob data in RDB corresponding to given key.
     *  Returns blob as array of bytes, with 0 elements if
     *  key could not be found.
     */
    public static byte[] getBlob(int key) {
	byte[] data;
	try {
	    Connection conn = getUnpooledConnection();
	    data = getBlob(conn, key);
	    conn.close();
	} catch (SQLException e) {
	    throw new TestException(TestHelper.getStackTrace(e));
        }
	return data;
    }

    // @todo: consider using PreparedStatement
    /**
     *  Gets Blob data in RDB corresponding to given key.
     *  Returns blob as array of bytes, with 0 elements if
     *  key could not be found.
     */
    public static byte[] getBlob(Connection conn, int key) {
	String tableName = DBParms.getTableName();
	String selectBlob = "SELECT value FROM " + tableName + 
	    " WHERE key ='" + key + "'";
	byte[] data;
	try {
	    Statement stmt = conn.createStatement();
	    try {
 		if (DBParms.getDriverLogging()) {
 		   Log.getLogWriter().info ("getBlob executing " + selectBlob);
                }
		ResultSet rs = stmt.executeQuery(selectBlob);
		rs.next();
		BLOB blob = (BLOB)((OracleResultSet) rs).getBlob(1);
// 		Blob blob = (Blob) rs.getBlob(1);
		int len = (int) blob.length();
		data = blob.getBytes(1,len);
		if (DBParms.getDriverLogging()) {
	            if (data.length == 0) {
	              Log.getLogWriter().info("No value found for " + key);
	            } else {
                      StringBuffer aStr = new StringBuffer();
	              for (int j = 0; j < 4; j++) {
                        aStr.append("   initialBytes[" + j + "] = " + data[j] + "\n");
	              }
                      Log.getLogWriter().info("Retrieved value for " + key +
                                              " containing " + len + " bytes" +
                                              " beginning with \n" + aStr.toString());
                    }
                }
	    } catch (SQLException e) {
		data = new byte[0];
		Log.getLogWriter().info(TestHelper.getStackTrace(e));
	    }
	} catch (SQLException e) {
	    throw new TestException(TestHelper.getStackTrace(e));
        }
	return data;
    }

    public byte[] getRandom_arrayOfBytes() {
	int arraySize = TestConfig.tab().intAt(DBParms.numBytes);
	byte[] byteArr = new byte[arraySize];
	TestConfig.tab().getRandGen().nextBytes(byteArr);
	return byteArr;
    }

    public byte[] getKeyed_arrayOfBytes(int key) {
	int arraySize = TestConfig.tab().intAt(DBParms.numBytes);
	if (arraySize < 4)
	    throw new TestException
		("Cannot insert keyed byte value into " + arraySize + 
		 " length byte array");
	byte[] byteArr = new byte[arraySize];
	TestConfig.tab().getRandGen().nextBytes(byteArr);
	byte[] keyBytes = intTObytes(key);
        StringBuffer aStr = new StringBuffer();
        aStr.append("getKeyed_arrayOfBytes returns \n");
	for (int i=0; i<4; i++) {
	    byteArr[i] = keyBytes[i];
            aStr.append("   keyBytes[" + i + "] = " + keyBytes[i] + "\n");
	}
	if (DBParms.getDriverLogging()) {
           Log.getLogWriter().info(aStr.toString());
        }
	return byteArr;
    }

    public static Connection getUnpooledConnection() {
	try {
	    String url = TestConfig.tab().stringAt(DBParms.jdbcUrl);
	    String user = TestConfig.tab().stringAt(DBParms.rdbUser);
	    String password = TestConfig.tab().stringAt(DBParms.rdbPassword);
	    return DriverManager.getConnection(url, user, password);
        } catch (SQLException e) {
	    throw new TestException(TestHelper.getStackTrace(e));
        }
    }


    //--------------------------------------------------------------------------
    // Constants

    private static final String lb = "[";
    private static final String rb = "]";


    //--------------------------------------------------------------------------
    // internal/test utility methods

    protected static String log(Region region, Object key) {
	return (region.getFullPath() + lb + key + rb);
    }

    protected static String log(Region region, Object key, Object value) {
	return (log(region, key) + " = " + value);
    }

    protected static byte[] intTObytes(int i) {
	byte[] byteArray = new byte[4];
	byteArray[0]=(byte)((i & 0xff000000)>>>24);
	byteArray[1]=(byte)((i & 0x00ff0000)>>>16);
	byteArray[2]=(byte)((i & 0x0000ff00)>>>8);
	byteArray[3]=(byte)((i & 0x000000ff));
	return byteArray;
    }

    protected static int bytesTOint(byte[] byteArray) {
	int i = ((byteArray[0] & 0xFF) << 24)
	      | ((byteArray[1] & 0xFF) << 16)
	      | ((byteArray[2] & 0xFF) << 8)
 	      | (byteArray[3] & 0xFF);
	return i;
    }

} 
