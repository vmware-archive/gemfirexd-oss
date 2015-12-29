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
package com.pivotal.gemfirexd.internal.engine.hadoop.mapreduce;

import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pivotal.gemfirexd.hadoop.mapred.RowOutputFormat;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedResultSet;

public class OutputFormatUtil {
  public static final String OUTPUT_TABLE = "gfxd.output.tablename";
  public static final String OUTPUT_SCHEMA = "gfxd.output.schemaname";
  public static final String OUTPUT_URL = "gfxd.output.dburl";
  
  private static final String METHOD_PREFIX = "set";
  private final Logger logger;

  // count returned from batch execution. This variable is intended to be used
  // FOR TESTING ONLY
  public static int resultCountTest;

  public OutputFormatUtil() {
    this.logger = LoggerFactory.getLogger(OutputFormatUtil.class);
  }
  
  /**
   * Uses reflection to identify all methods of type
   * {@code setX(int, PreparedStatement)} in the {@code VALUE} class. Each such
   * method will be called iteratively to prepare SQL statement
   * 
   * @return
   */
  public <VALUE> List<Method> spotTableColumnSetters(VALUE value) {
    List<Method> columnSetters = new ArrayList<Method>();
    Method[] methods = value.getClass().getDeclaredMethods();
    for (Method method : methods) {
      Class<?>[] parameters = method.getParameterTypes();
      if (parameters == null || parameters.length != 2) {
        // parameter count does not match the expected count
        continue;
      }

      String name = method.getName();
      if (!name.startsWith(METHOD_PREFIX)
          || !(name.length() > METHOD_PREFIX.length())) {
        // not a valid name
        continue;
      }

      if (Modifier.isPrivate(method.getModifiers())) {
        // only public methods
        continue;
      }

      if (!int.class.isAssignableFrom(parameters[0])
          || !PreparedStatement.class.isAssignableFrom(parameters[1])) {
        // parameter type does not match the expected type
        continue;
      }

      columnSetters.add(method);
    }

    return columnSetters;
  }

  /**
   * Constructs field string from column setter names
   * 
   * @param tableName
   * @param columnSetters
   * @return query usable for inserting data in gemfirexd
   */
  public String createQuery(String tableName, List<Method> columnSetters) {
    StringBuffer query = new StringBuffer("PUT INTO ");

    query.append(tableName).append("(");
    for (Iterator<Method> iter = columnSetters.iterator(); iter.hasNext();) {
      Method method = iter.next();

      String name = method.getName();
      int prefixLen = METHOD_PREFIX.length();
      query.append(Character.toLowerCase(name.charAt(prefixLen)));
      if (name.length() > prefixLen + 1) {
        query.append(name.substring(prefixLen + 1));
      }

      if (iter.hasNext()) {
        query.append(", ");
      }
    }
    query.append(") VALUES (");

    for (Iterator<Method> iter = columnSetters.iterator(); iter.hasNext();) {
      iter.next();
      query.append("?");
      if (iter.hasNext()) {
        query.append(", ");
      }
    }

    query.append(");");
    return query.toString();
  }
  
  public class RowCommandBatchExecutor {
    // statement will be executed if the number of commands is more than the
    // batch size
    final int BATCH_SIZE;
    PreparedStatement st;
    private final Connection connection;
    
    // count of unexecuted commands in the batch
    int count;
    
    public RowCommandBatchExecutor(String driver, String url, int batch) throws IOException,
        ClassNotFoundException {
      this(getConnection(driver, url, logger), batch);
    }
    
    public RowCommandBatchExecutor(Connection conn, int batchSize) {
      this.BATCH_SIZE = batchSize;
      this.connection = conn;
    }

    public void initStatement(String query) throws SQLException {
      st = connection.prepareStatement(query);
    }

    public boolean isNotInitialized() {
      return st == null;
    }

    /**
     * Invokes column setters to set values for prepared statement and then
     * executes the statement to insert data in the batch
     * 
     * @return number of put commands executed
     */
    public <V> int executeWriteStatement(V val, List<Method> methods)
        throws IOException, SQLException {
      int i = 0;
      for (Method method : methods) {
        try {
          method.invoke(val, ++i, st);
        } catch (Exception e) {
          logger.error("Error executing setter method " + method.getName(), e);
          e.printStackTrace();
          throw new IOException(e);
        }
      }

      st.addBatch();
      count++;

      resultCountTest = 0;
      if (BATCH_SIZE > 0 && count >= BATCH_SIZE) {
        resultCountTest = st.executeBatch().length;
        count = 0;
      }

      return resultCountTest;
    }

    /**
     * Closes prepared statement and connection. Before closing it tries to
     * execute batch
     * @return number of put commands executed
     * @throws IOException 
     */
    public int close() throws IOException {
      resultCountTest = 0;
      try {
        if (st != null) {
          resultCountTest = st.executeBatch().length;
        }
      } catch (SQLException e) {
        // connection.rollback();
        e.printStackTrace();
        throw new IOException("Failed to commit data", e);
      } finally {
        try {
          if (st != null) {
            st.close();
            st = null;
          }
        } catch (SQLException e) {
          e.printStackTrace();
        } finally {
          try {
            if (connection != null && ! connection.isClosed()) {
              connection.close();
            }
          } catch (SQLException e) {
            e.printStackTrace();
          }
        }
      }
      
      return resultCountTest;
    }
  }
  
  private static Connection getConnection(String driver, String url,
      Logger logger) throws IOException, ClassNotFoundException {
    Class.forName(driver);

    try {
      Connection conn = DriverManager.getConnection(url);
      return conn;
    } catch (SQLException e) {
      logger.error("Error in connecting to gemfirexd instance", e);
      throw new IOException(e);
    }
  }
  
  /**
   * This method fixes class resolution while building jar.
   */
  <V> RowOutputFormat<V> getMapreduceInputFormat(V val) {
    return new RowOutputFormat<V>();
  }

  public static ResultSet getNonIterableResultSet(EmbedResultSet rs) {
    return new NoIterNoUpdateResultSetProxy(rs);
  }
}
