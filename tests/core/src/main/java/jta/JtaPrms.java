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

import com.gemstone.gemfire.cache.*;


import hydra.*;

public class JtaPrms extends BasePrms {

public static Long numberOfEvents;  
public static Long entryOperations;  
public static Long numberOfRandomRegions;

  /**
   * (String) path for cache xml file with jndi bindings 
   */
  public static long cacheXmlFile;
  protected static String getCacheXmlFile() {
    Long key = cacheXmlFile;
    String val = TestConfig.tasktab().stringAt( key, tab().stringAt( key, null ) );
    return (val);
  }

  /** 
   * (boolean) Whether or not to use the GemFire CacheTransactionManager
   * vs. JTA.
   */
  public static Long useGemFireTransactionManager;
  protected static boolean useGemFireTransactionManager() {
     Long key = useGemFireTransactionManager;
     boolean val = tab().booleanAt(key, false);
     return (val);
  }

  /** 
   * (boolean) Whether or not to use the Derby network server (for 
   * multiVM JTA tests).
   */
  public static Long useDerbyNetworkServer;
  protected static boolean useDerbyNetworkServer() {
     Long key = useDerbyNetworkServer;
     boolean val = tab().booleanAt(key, false);
     return (val);
  }

  /** 
   * (boolean) Execute database operations inline to tx (vs. in the 
   * Cache and Tx Writers and Listeners).  We must do this as we can 
   * only have 1 connection to the derby db.  For PRs, the writers, etc.
   * will be invoked on the primary (which could be remote).
   */
  public static Long executeDBOpsInline;
  protected static boolean executeDBOpsInline() {
     Long key = executeDBOpsInline;
     boolean val = tab().booleanAt(key, false);
     return (val);
  }

  //---------------------------------------------------------------------
  // GemFire Oracle DataSource settings for Gemfire Transactions
  //---------------------------------------------------------------------
  /** (String) The JDBC driver class name. */
  public static Long jdbcDriver;

  /** (String) The JDBC URL. */
  public static Long jdbcUrl;

  /** (String) The RDB user. */
  public static Long rdbUser;

  /** (String) The RDB password. */
  public static Long rdbPassword;

  /** (int) The pool minimum limit. */
  public static Long poolMinLimit;

  //---------------------------------------------------------------------------
  // TransactionListener
  //---------------------------------------------------------------------------
  /**
   *  Class name of transaction listener to use.  Defaults to null.
   */
  public static Long txListener;
  public static TransactionListener getTxListener() {
    Long key = txListener;
    String val = tasktab().stringAt( key, tab().stringAt( key, null ) );
    try {
      return (TransactionListener)instantiate( key, val );
    } catch( ClassCastException e ) {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val + " does not implement TransactionListener", e );
    }
  }

  //---------------------------------------------------------------------------
  // TransactionWriter
  //---------------------------------------------------------------------------
  /**
   *  Class name of transaction writer to use.  Defaults to null.
   */
  public static Long txWriter;
  public static TransactionWriter getTxWriter() {
    Long key = txWriter;
    String val = tasktab().stringAt( key, tab().stringAt( key, null ) );
    try {
      return (TransactionWriter)instantiate( key, val );
    } catch( ClassCastException e ) {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val + " does not implement TransactionWriter", e );
    }
  }

  private static Object instantiate( Long key, String classname ) {
    if ( classname == null ) {
      return null;
    }
    try {
      Class cls = Class.forName( classname );
      return cls.newInstance();
    } catch( Exception e ) {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": cannot instantiate " + classname, e );
    }
  }

// ================================================================================
static {
   BasePrms.setValues(JtaPrms.class);
}

}
