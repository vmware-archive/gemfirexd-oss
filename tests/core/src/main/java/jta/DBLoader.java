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
package jta;

import java.util.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.naming.Context;
import javax.sql.DataSource;
import javax.transaction.UserTransaction;
import javax.transaction.RollbackException;

import com.gemstone.gemfire.cache.*;

import hydra.*;
import util.*;

/** CacheLoader to load entries from the database
 * 
 */
public class DBLoader implements CacheLoader {
	
String tableName = JtaCacheCallbackHydraTest.TABLENAME;
boolean txLoad = false;
	
  /** implement CacheLoader interface */
  public final Object load(LoaderHelper helper) throws CacheLoaderException {
    return loadFromDatabase(helper.getKey());
  }
	
  /** load the given key from the database */
  private Object loadFromDatabase(Object ob) {
    Object obj = null;
    try {
      Connection conn = null;
      if (this.txLoad) {
        conn = DBUtil.getXADSConnection();
      } else {
        conn = DBUtil.getSimpleDSConnection();
      }
      Statement stm = conn.createStatement();
      ResultSet rs = stm.executeQuery("select name from " + tableName + " where id = ('" + ob.toString() + "')");
      rs.next();
      obj = rs.getString(1);
      if (rs != null) rs.close();
      if (stm != null) stm.close();
      if (conn != null) conn.close();
      return obj;
    } catch (Exception e) {
       Log.getLogWriter().info("Exception in DBLoader::loadFromDatabase, The error is "+ e);
    }
    return obj;
  }

  /** Set txLoad to indicate that XADSConnections are to be used.
   *  Initially, this is false so that the test can load the region from the 
   *  database without invoking remote loaders for remotely located PR entries).
   */
  public void setTxLoad(boolean b) {
    this.txLoad = b;
  }

  public void close() {
  }
}

