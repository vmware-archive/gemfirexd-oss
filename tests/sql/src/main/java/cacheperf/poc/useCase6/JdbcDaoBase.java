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
package cacheperf.poc.useCase6;

import hydra.Log;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.sql.Date;
import java.util.List;

public class JdbcDaoBase {

  public Connection con = null;

  /** SQL */
  private SqlBuilder sql = null;

  /** PreparedStatement */
  protected PreparedStatement ps = null;

  /** ResultSet */
  private ResultSet rs = null;

  protected boolean fineEnabled = false;

  java.util.Map<String, PreparedStatement> psCache = new java.util.HashMap();


  protected JdbcDaoBase() {
    con = null;
    fineEnabled = Log.getLogWriter().fineEnabled();
  }

  protected JdbcDaoBase(Connection connection) {
    con = connection;
  }

  protected Connection getConnection() {
    if (con == null) {
      // fConnection = xxxxx;
    }
    return con;
  }

  protected SqlBuilder createSqlBuilder(int size) {
    sql = new SqlBuilder(size);
    return sql;
  }

  protected SqlBuilder getSqlBuilder() {
    return sql;
  }

  protected void setSqlBuilderql(SqlBuilder sql) {
    this.sql = sql;
  }

  protected void sqlTransrator(SQLException e1) {
    if (ps != null) {
      if (rs != null) {
        try {
          rs.close();
        } catch (SQLException e2) {
          throw new RuntimeException(e2);
        }
      }
      try {
        ps.close();
      } catch (SQLException e2) {
        throw new RuntimeException(e2);
      }
    }
    throw new RuntimeException(e1);
  }

  protected PreparedStatement createPrepareStatement(String sql)
      throws SQLException {
    ps = con.prepareStatement(sql);
    return ps;
  }

  protected PreparedStatement createPrepareStatement(SqlBuilder sql)
      throws SQLException {
    ps = con.prepareStatement(sql.getBuf().toString());
    return ps;
  }

  protected void close() throws SQLException {
    /****
    if (ps != null) {
      if (rs != null) {
        rs.close();
        rs = null;
      }
      ps.close();
      ps = null;
    }
    ****/
  }

  protected void sqlTransrator(SQLException e, String msg) {
    if (fineEnabled) {
      Log.getLogWriter().fine(e);
    }
    try {
      close();
    } catch (SQLException e2) {
      if (fineEnabled) {
        Log.getLogWriter().fine(e);
      }
    }
    throw new RuntimeException(msg, e);
  }

  protected ResultSet executeQuery() {
    return prepareStatemet(this.sql);
  }

  protected ResultSet prepareStatemet(SqlBuilder sql) {
    StringBuilder buf = sql.getBuf();
    String sqlStr = buf.toString();
    if (fineEnabled) {
      Log.getLogWriter().fine(sqlStr);
    }

    List<Object> params = sql.getParams();
    try {
      if ((ps = psCache.get(sqlStr)) == null ) {
        ps = con.prepareStatement(sqlStr);
        psCache.put(sqlStr, ps);
        Log.getLogWriter().info(" NOT in PS Cache ...");
      }
      if (params != null) {
        for (int i = 0; i < params.size(); i++) {
          Object obj = params.get(i);
          if (obj instanceof String) {
            ps.setString(i + 1, (String) obj);
            if (fineEnabled) {
              Log.getLogWriter().fine("params("+i+")[String] " + (String)obj);
            }
          } else if (obj instanceof Date) {
            ps.setDate(i + 1, (Date) obj);
            if (fineEnabled) {
              Log.getLogWriter().fine("params("+i+")[Date] " + ((Date)obj).toString());
            }
          } else if (obj instanceof Double) {
            ps.setDouble(i + 1, (Double) obj);
            if (fineEnabled) {
              Log.getLogWriter().fine("params("+i+")[Double] " + ((Double)obj).doubleValue());
            }
          }
        }
      }
    } catch (SQLException e) {
      sqlTransrator(e);
    }

    return rs;
  }

  protected class SqlBuilder {

    private StringBuilder buf;

    private List<Object> params;

    private SqlBuilder() {
    }

    private SqlBuilder(int size) {
      buf = new StringBuilder(size);
    }

    public void append(SqlBuilder sql) {
      if (sql == null) {
        throw new IllegalArgumentException();
      }
      if (params == null) {
        params = new ArrayList<Object>();
      }
      buf.append(sql.getBuf().toString());
      for (Object obj : sql.getParams()) {
        params.add(obj);
      }
    }

    public void append(String str) {
      if (str == null) {
        throw new IllegalArgumentException();
      }
      buf.append(str);
      buf.append(" ");
    }

    public void append(String str, String val) {
      if (val == null) {
        throw new IllegalArgumentException();
      }
      if (params == null) {
        params = new ArrayList<Object>();
      }
      params.add(val);
      buf.append(str);
      buf.append(" ");
    }

    public void append(String str, String val1, String val2) {
      if (val1 == null || val2 == null) {
        throw new IllegalArgumentException();
      }
      if (params == null) {
        params = new ArrayList<Object>();
      }
      params.add(val1);
      params.add(val2);
      buf.append(str);
      buf.append(" ");
    }

    public void appendIfNotNull(String str, String val) {
      if (val == null) {
        return;
      }
      if (params == null) {
        params = new ArrayList<Object>();
      }
      params.add(val);
      buf.append(str);
      buf.append(" ");
    }

    public void appendIfNotNull(String str, String val1, String val2) {
      if (val1 == null && val2 == null) {
        return;
      } else if (val1 == null || val2 == null) {
        throw new IllegalArgumentException();
      }
      if (params == null) {
        params = new ArrayList<Object>();
      }
      params.add(val1);
      params.add(val2);
      buf.append(str);
      buf.append(" ");
    }

    public void append(String str, java.util.Date val) {
      if (val == null) {
        throw new IllegalArgumentException();
      }
      append(str, new Date(val.getTime()));
    }

    public void append(String str, Date val) {
      if (val == null) {
        throw new IllegalArgumentException();
      }
      if (params == null) {
        params = new ArrayList<Object>();
      }
      params.add(val);
      buf.append(str);
      buf.append(" ");
    }

    public void append(String str, double val) {
      if (params == null) {
        params = new ArrayList<Object>();
      }
      params.add(val);
      buf.append(str);
      buf.append(" ");
    }

    public void appendIfNotNull(String str, java.util.Date val) {
      if (val == null) {
        return;
      }
      appendIfNotNull(str, new Date(val.getTime()));
    }

    public void appendIfNotNull(String str, Date val) {
      if (val == null) {
        return;
      }
      if (params == null) {
        params = new ArrayList<Object>();
      }
      params.add(val);
      buf.append(str);
      buf.append(" ");
    }

    public StringBuilder getBuf() {
      return buf;
    }

    public void setBuf(StringBuilder buf) {
      this.buf = buf;
    }

    public List<Object> getParams() {
      return params;
    }

    public void setParams(List<Object> params) {
      this.params = params;
    }
  }
}
