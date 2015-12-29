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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.jta.TransactionUtils;
import com.gemstone.org.jgroups.util.StringId;

/**
 * GemFireBasicDataSource extends AbstractDataSource. This is a datasource class
 * which provides connections. fromthe databse without any pooling.
 * 
 * @author tnegi
 * @author Asif
 */
public class GemFireBasicDataSource extends AbstractDataSource  {
  private static final long serialVersionUID = -4010116024816908360L;

  /** Creates a new instance of BaseDataSource */
  protected transient Driver driverObject = null;

  /**
   * Place holder for abstract method 
   * isWrapperFor(java.lang.Class) in java.sql.Wrapper
   * required by jdk 1.6
   *
   * @param iface - a Class defining an interface. 
   * @throws SQLException 
   * @return boolean
   */
   public boolean isWrapperFor(Class iface) throws SQLException {
     return true;
   }

  /**
   * Place holder for abstract method 
   * java.lang Object unwrap(java.lang.Class) in java.sql.Wrapper
   * required by jdk 1.6
   *
   * @param iface - a Class defining an interface.
   * @throws SQLException
   * @return java.lang.Object
   */
   public Object unwrap(Class iface)  throws SQLException { 
     return iface;
   }

  /**
   * Creates a new instance of GemFireBasicDataSource
   * 
   * @param configs The ConfiguredDataSourceProperties containing the datasource
   *          properties.
   * @throws SQLException
   */
  public GemFireBasicDataSource(ConfiguredDataSourceProperties configs)
      throws SQLException {
    super(configs);
    loadDriver();
  }

  /**
   * Implementation of datasource interface function. This method is used to get
   * the connection from the database. Default user name and password will be
   * used.
   * 
   * @throws SQLException
   * @return ???
   */
  @Override
  public Connection getConnection() throws SQLException {
    //Asif : In case the user is requesting the
    //connection without username & password
    //we should just return the desired connection
    Connection connection = null;
    if (driverObject == null) {
      synchronized (this) {
        if (driverObject == null) loadDriver();
      }
    }
    if (url != null) {
      Properties props = new Properties();
      props.put("user", user);
      props.put("password", password);
      connection = driverObject.connect(url, props);
    }
    else {
      StringId exception = LocalizedStrings.GemFireBasicDataSource_GEMFIREBASICDATASOURCE_GETCONNECTION_URL_FOR_THE_DATASOURCE_NOT_AVAILABLE;
      LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
      if (writer.infoEnabled()) writer.info(exception);
      throw new SQLException(exception.toLocalizedString());
    }
    return connection;
  }

  /**
   * Implementation of datasource function. This method is used to get the
   * connection. The specified user name and passowrd will be used.
   * 
   * @param clUsername The username for the database connection.
   * @param clPassword The password for the database connection.
   * @throws SQLException
   * @return ???
   */
  @Override
  public Connection getConnection(String clUsername, String clPassword)
      throws SQLException {
    //First Autheticate the user
    checkCredentials(clUsername, clPassword);
    return getConnection();
  }

  private void loadDriver() throws SQLException {
    try {
      Class driverClass = ClassPathLoader.getLatest().forName(jdbcDriver);
      driverObject = (Driver) driverClass.newInstance();
    }
    catch (Exception ex) {
      LogWriterI18n writer = TransactionUtils.getLogWriterI18n();
      StringId msg = LocalizedStrings.GemFireBasicDataSource_AN_EXCEPTION_WAS_CAUGHT_WHILE_TRYING_TO_LOAD_THE_DRIVER;
      String msgArg = ex.getLocalizedMessage();
      if (writer.errorEnabled()) writer.error(msg, msgArg, ex);
      throw new SQLException(msg.toLocalizedString(msgArg));
    }
  }
}
