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
package com.gemstone.gemfire.internal.datasource;

/**
 * @author tnegi
 */
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import javax.sql.*;
import java.io.*;
import com.gemstone.gemfire.internal.jta.TransactionUtils;
import java.sql.*;
import java.util.logging.Logger;

/**
 * AbstractDataSource implements the Datasource interface. This is base class
 * for the datasouce types. The class also implements the Serializable and
 * Referenceable behavior.
 * 
 * @author tnegi
 * @author Asif : This class now contains only those paramaters which are needed
 *         by the Gemfire DataSource configuration. This maps to those
 *         paramaters which are specified as attributes of <jndi-binding>tag.
 *         Those parameters which are specified as attributes of <property>tag
 *         are not stored.
 */
public abstract class AbstractDataSource implements Serializable, DataSource {

  protected transient PrintWriter dataSourcePW;
  protected int loginTimeOut;
  protected String user;
  protected String password;
  protected String serverName;
  protected String url;
  protected String jdbcDriver;
  protected ConfiguredDataSourceProperties configProps;
  protected boolean isActive = true;

  /**
   * Constructor for the AbstractDataSource.
   * 
   * @param configs ConfiguredDataSourceProperties object containing all the
   *          data required to construct the datasource object.
   * @throws SQLException
   *  
   */
  public AbstractDataSource(ConfiguredDataSourceProperties configs)
      throws SQLException {
    loginTimeOut = configs.getLoginTimeOut();
    user = configs.getUser();
    password = configs.getPassword();
    url = configs.getURL();
    jdbcDriver = configs.getJDBCDriver();
    dataSourcePW = configs.getPrintWriter();
    isActive = true;
  }

  /**
   * Behavior from the dataSource interface. This is an abstract function which
   * will implemented in the Datasource classes.
   * 
   * @throws SQLException
   * @return Connection Connection object for the Database connection.
   */
  public abstract Connection getConnection() throws SQLException;

  /**
   * Behavior from the dataSource interface. This is an abstract function which
   * will be implemented in the Datasource classes.
   * 
   * @param username Username used for making database connection.
   * @param password Passowrd used for making database connection.
   * @throws SQLException
   * @return ???
   */
  public abstract Connection getConnection(String username, String password)
      throws SQLException;

  //DataSource Interface functions
  /**
   * Returns the writer for logging
   * 
   * @throws SQLException
   * @return PrintWriter for logs.
   */
  public PrintWriter getLogWriter() throws SQLException {
    return dataSourcePW;
  }

  /**
   * Returns the amount of time that login will wait for the connection to
   * happen before it gets time out.
   * 
   * @throws SQLException
   * @return int login time out.
   */
  public int getLoginTimeout() throws SQLException {
    return loginTimeOut;
  }

  /**
   * sets the log writer for the datasource.
   * 
   * @param out PrintWriter for writing the logs.
   * @throws SQLException
   */
  public void setLogWriter(java.io.PrintWriter out) throws SQLException {
    dataSourcePW = out;
  }

  /**
   * Sets the login time out for the database connection
   * 
   * @param seconds login time out in seconds.
   * @throws SQLException
   */
  public void setLoginTimeout(int seconds) throws SQLException {
    loginTimeOut = seconds;
  }

  /**
   * @param conn
   * @return boolean
   */
  protected boolean validateConnection(Connection conn) {
    try {
      return (!conn.isClosed());
    }
    catch (SQLException e) {
      LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
      if (writer.fineEnabled())
          writer
              .fine(
                  "AbstractDataSource::validateConnection:exception in validating connection",
                  e);
      return false;
    }
  }

  //Get Methods for DataSource Properties
  /**
   * Returns the default user for the database.
   * 
   * @return String
   */
  public String getUser() {
    return user;
  }

  /**
   * Returns the default password for the database.
   * 
   * @return String
   */
  public String getPassword() {
    return password;
  }

  /**
   * The name of the JDBC driver for the database.
   * 
   * @return String
   */
  public String getJDBCDriver() {
    return jdbcDriver;
  }

  // Set Method for Datasource Properties
  /**
   * Sets the default username for the database
   * 
   * @param usr
   */
  public void setUser(String usr) {
    user = usr;
  }

  /**
   * Sets the default passord for the database user.
   * 
   * @param passwd Password for the user
   */
  public void setPassword(String passwd) {
    password = passwd;
  }

  /**
   * Sets the JDBC driver name of the datasource
   * 
   * @param confDriver
   */
  public void setJDBCDriver(String confDriver) {
    jdbcDriver = confDriver;
  }

  /**
   * Authenticates the username and password for the database connection.
   * 
   * @param clUser The username for the database connection
   * @param clPass The password for the database connection
   * @throws SQLException
   */
  public void checkCredentials(String clUser, String clPass)
      throws SQLException {
    if (clUser == null || !clUser.equals(user) || clPass == null
        || !clPass.equals(password)) {
      String error = LocalizedStrings.AbstractDataSource_CANNOT_CREATE_A_CONNECTION_WITH_THE_USER_0_AS_IT_DOESNT_MATCH_THE_EXISTING_USER_NAMED_1_OR_THE_PASSWORD_WAS_INCORRECT.toLocalizedString(new Object[] { clUser, clPass });
      throw new SQLException(error);
    }
  }

  /**
   *  
   */
  public void clearUp() {
    isActive = false;
  }

  public Logger getParentLogger() {
    throw new AssertionError("should be overridden in JDBC 4.1");
  }

}
