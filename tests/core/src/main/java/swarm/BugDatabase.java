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
package swarm;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class BugDatabase {

	
	private static final String URL = "jdbc:postgresql://hydrogen.gemstone.com/trac_gemfire";
	static {
		try {
			Class.forName("org.postgresql.Driver");
		} catch(Exception e) {
			e.printStackTrace();
			throw new ExceptionInInitializerError(e.getMessage());
		}
	}
	
	static ThreadLocal<Connection> conns = new ThreadLocal<Connection>();
	
	public static Connection getConnection() throws SQLException {
	  Connection c = conns.get();
	  if(c==null) {
	    c = DriverManager.getConnection(URL,"gregp","Q6to2ZFd1j9y");
	    conns.set(c);
	  }
	  return c;
	}
	
	public static ResultSet executeQuery(String sql,boolean debug) throws SQLException {
		Statement st = getConnection().createStatement();
		if(debug) {
			System.out.println("[SWARMSQL]:"+sql);
		}
		return st.executeQuery(sql);
	}
	
	 public static PreparedStatement prepareStatement(String sql) throws SQLException {
	   return prepareStatement(sql,true);
	 }
	 
	public static PreparedStatement prepareStatement(String sql,boolean debug) throws SQLException {
	  if(debug) {
      System.out.println("[SWARMPREPARGFXD]:"+sql);
    }
		PreparedStatement st = getConnection().prepareStatement(sql);
		return st;
	}
	
	
	public static ResultSet executeQuery(String sql) throws SQLException {
		return executeQuery(sql,true);
	}
	
	
	public static int executeUpdate(String sql,boolean debug) throws SQLException {
		Statement st = getConnection().createStatement();
		if(debug) {
			System.out.println("[SWARMSQL]:"+sql);
		}
		return st.executeUpdate(sql);
	}
	
	public static int executeUpdate(String sql) throws SQLException {
		return executeUpdate(sql,true);
	}
	
	
}
