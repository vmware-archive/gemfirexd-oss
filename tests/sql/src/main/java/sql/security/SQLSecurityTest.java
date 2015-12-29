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
package sql.security;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.pivotal.gemfirexd.FabricServer;
import com.pivotal.gemfirexd.FabricService;
import com.pivotal.gemfirexd.FabricServiceManager;
import com.pivotal.gemfirexd.internal.iapi.reference.Attribute;
import com.pivotal.gemfirexd.internal.iapi.reference.Property;

import hydra.HydraThreadLocal;
import hydra.Log;
import hydra.MasterController;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import hydra.gemfirexd.FabricServerHelper;
import hydra.gemfirexd.GfxdConfigPrms;
import sql.ddlStatements.AuthorizationDDLStmt;
import sql.ddlStatements.DDLStmtIF;
import sql.ddlStatements.FunctionDDLStmt;
import sql.security.AuthenticationTest;
import sql.sqlutil.DDLStmtsFactory;
import sql.ClientDiscDBManager;
import sql.GFEDBManager;
import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import util.PRObserver;
import util.TestException;
import util.TestHelper;

public class SQLSecurityTest extends SQLTest {
  protected static SQLSecurityTest ssTest;
  public static String userPasswdMap = "userPasswdMap";
  public static String systemUserPasswdMap = "systemUserPasswdMap";
  public static String userPrefix = "thr_";
  protected static boolean[] serverStarted = new boolean[1];
  public static boolean sqlAuthorization = TestConfig.tab().booleanAt(
      hydra.gemfirexd.FabricSecurityPrms.sqlAuthorization, false);
  static boolean useBUILTIN = TestConfig.tab().booleanAt(SQLPrms.useBUILTINSchema, true);
  public static String gfxdUserCreated = "userCreated";
  public static boolean bootedAsSuperUser = true;
  public static boolean useLDAP = TestConfig.tab().booleanAt(SQLPrms.useLDAPSchema, false);
  protected static String systemUser = null;
  public static boolean hasMultiSystemUsers = TestConfig.tab().
      booleanAt(SQLPrms.hasMultiSystemUsers, false);
  protected static boolean hasRoutine = TestConfig.tab().
      booleanAt(SQLPrms.hasRoutineInSecurityTest, false);
  public static HydraThreadLocal prepareStmtException = new HydraThreadLocal();
  
  public static synchronized void HydraTask_initialize() {
    if (ssTest == null) {
      ssTest = new SQLSecurityTest();
      //if (sqlTest == null) sqlTest = new SQLTest();
      
      PRObserver.installObserverHook();
      PRObserver.initialize(RemoteTestModule.getMyVmid());

      ssTest.initialize();
    }
  }

  
  public static void HydraTask_createGfxdLocatorTask() {
    FabricServerHelper.createLocator();
  }
  
  public static void HydraTask_startGfxdLocatorTask() {
    String networkServerConfig = GfxdConfigPrms.getNetworkServerConfig();
    if (!hasMultiSystemUsers)
      FabricServerHelper.startLocator(networkServerConfig);
    else {
      Map<String, String> m = getMultiSystemUserLocatorMap();
      FabricServerHelper.startLocator(networkServerConfig, m);
    }
  }
  
  @SuppressWarnings("unchecked")
  private static Map<String, String> getMultiSystemUserLocatorMap() {
    return (Map<String, String>) SQLBB.getBB()
      .getSharedMap().get(SQLSecurityTest.systemUserPasswdMap);
  }
  
  protected void initialize() {
    super.initialize();
  }

  public static void HydraTask_createAuthDiscDB() {
    ssTest.createAuthDiscDB();
  }
  
  protected void createAuthDiscDB() {
    if (hasDerbyServer && discConn == null) {
      discConn = getDerbySuperUserConnection();
    }
  }
  
  public static void HydraTask_setupDerbyUserAuthentication() {
    ssTest.setupDerbyUserAuthentication();
  }
  
  public static void HydraTask_setupGfxdUserAuthentication() {
    if (!useLDAP) ssTest.setupGfxdUserAuthentication();
  } 
  
  /**
   * All working threads should set up its user name and password pair
   * This will be the base for authentication and authorization
   */
  public static void HydraTask_setupUsers() {
    ssTest.setupUsers();
  }
  
  //use synchronized for now until #42499 is fixed
  public static synchronized void HydraTask_startAuthFabricServer() {
    if (serverStarted[0]) {
      Log.getLogWriter().info("fabric server is started");
      return;  
    }
    //comment out above once synchronized was removed after #42499
    ssTest.startAuthFabricServer();
  } 
  
  protected void startAuthFabricServer() {
    if (useLDAP) {
      startAuthFabricServerAsSuperUser(); 
      return;
    }
    Properties p = FabricServerHelper.getBootProperties();
    Log.getLogWriter().info("user password is " + 
      p.getProperty(Property.USER_PROPERTY_PREFIX + "superUser"));
    p.setProperty(Property.USER_PROPERTY_PREFIX + "thr_" + 
      getMyTid(), "thr_" + getMyTid());
    p.remove(Property.USER_PROPERTY_PREFIX + "superUser");
    Log.getLogWriter().info("boot with this authentication setting -- user: " + 
      Property.USER_PROPERTY_PREFIX + "thr_" + 
      getMyTid() + "password: thr_" + getMyTid());
    p.setProperty(random.nextBoolean() ? com.pivotal.gemfirexd.Attribute.USERNAME_ATTR : com.pivotal.gemfirexd.Attribute.USERNAME_ALT_ATTR, "thr_" + getMyTid());
    p.setProperty(com.pivotal.gemfirexd.Attribute.PASSWORD_ATTR, "thr_" + getMyTid());
    // default to partitioned tables for tests
    p.setProperty("table-default-partitioned", "true");
    if (p != null
        && "false".equalsIgnoreCase(p
            .getProperty(com.pivotal.gemfirexd.Attribute.GFXD_HOST_DATA))
        && "true".equalsIgnoreCase(p
            .getProperty(com.pivotal.gemfirexd.Attribute.GFXD_PERSIST_DD))) {
      p.setProperty(com.pivotal.gemfirexd.Attribute.GFXD_PERSIST_DD, "false");
    }
    FabricServer fs = FabricServiceManager.getFabricServerInstance();
    boolean userCreated = SQLBB.getBB().getSharedMap().get(gfxdUserCreated) != null;
    try {
      fs.start(p);
    } catch (SQLException se) {
     
      if (se.getSQLState().equalsIgnoreCase("XJ040") && 
          (!userCreated || !systemUser.equals(userPrefix+getMyTid()))) {
        Log.getLogWriter().info("got expected userid or password " +
            "invalid exception, continuing test");
        return;
      } else if (se.getSQLState().equalsIgnoreCase("XJ040") && userCreated
          && systemUser.equals(userPrefix+getMyTid())) {
        SQLHelper.printSQLException(se);
        throw new TestException("the system user has been created and received but got " +
        "userid  or password invalid exception" + TestHelper.getStackTrace(se));
      }else SQLHelper.handleSQLException(se);
    }
    synchronized (serverStarted) {
      serverStarted[0] = true;
      bootedAsSuperUser = false; //booted as "thr_+ tid"
      Log.getLogWriter().info("booted as systemUser: thr_" + getMyTid());
    }
    if (!userCreated) {
      throw new TestException ("did not get expected invalid userid " +
            "or password exception due to #42496");
      //it is OK to start gemfirexd using system user
    }
  }
  
  public static void HydraTask_startAuthFabricServerAsSuperUser() {
    ssTest.startAuthFabricServerAsSuperUser();
  }
  
  protected void startAuthFabricServerAsSuperUser(){
    synchronized (serverStarted) {
      if (serverStarted[0]) {
        Log.getLogWriter().info("to workaround #42499, no more fabric servers " +
            "will be started");
        return;
      } else _startAuthFabricServerAsSuperUser();
      //else reStartAuthFabricServerWithAuthorizationAsSuperUser();
    }
    logGfxdSystemProperties();
  }
  protected void _startAuthFabricServerAsSuperUser() {
    //FabricServerHelper.startFabricServer();
    Properties bootProperties = FabricServerHelper.getBootProperties();
    FabricServer fs = FabricServiceManager.getFabricServerInstance();
    // default to partitioned tables for tests
    if (bootProperties != null) {
      bootProperties.setProperty("table-default-partitioned", "true");
    }
    else {
      System.setProperty("gemfirexd.table-default-partitioned", "true");
    }
    if (bootProperties != null
        && "false".equalsIgnoreCase(bootProperties
            .getProperty(com.pivotal.gemfirexd.Attribute.GFXD_HOST_DATA))
        && "true".equalsIgnoreCase(bootProperties
            .getProperty(com.pivotal.gemfirexd.Attribute.GFXD_PERSIST_DD))) {
      bootProperties.setProperty(com.pivotal.gemfirexd.Attribute.GFXD_PERSIST_DD, "false");
    }
    try {
      fs.start(bootProperties);
      serverStarted[0] = true;
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    } catch (IllegalStateException ise) {
      if (serverStarted[0]) 
        Log.getLogWriter().info("server has been started already, " +
            "continuing tests");
      else
        throw new TestException("got unexpected IllegalStateException" +
           " during fabric server start");
    }
  }
  /**
   * create map to store user password pairs.
   */
  protected void setupUsers() {
    Map<String, String> userPasswd = new HashMap<String, String>();
    SQLBB.getBB().getSharedMap().put(userPasswdMap, userPasswd);
    if (hasMultiSystemUsers) {
      Map<String, String> systemUserPasswd = new HashMap<String, String>();
      SQLBB.getBB().getSharedMap().put(systemUserPasswdMap, systemUserPasswd);
      initUsers(); //make sure the ddl thread will also be a system user
    }
  }
  
  public static void HydraTask_initUsers() {
    ssTest.initUsers();
  }
  
  public static void HydraTask_addLoader() {
    ssTest.addLoader();
  }
  
  protected void addLoader() {
    if (!populateThruLoader && !testLoaderCreateRandomRow) {
      Log.getLogWriter().info("no loader is attached");
      return;
    }
    Connection conn = getAuthGfxdConnection();
    
    try {
      addLoader(conn);
    } catch (SQLException se) {
      if (se.getSQLState().equals("42504")) {
        Log.getLogWriter().info("gets expected authorization exception when " +
        		"user by default does not have the right to call SYS.ATTACH_LOADER procedure, " +
        		"continue testing");
      } else 
      SQLHelper.handleSQLException(se);
    }
    
    conn = getGfxdSuperUserConnection();
    String grantProcedure = "GRANT EXECUTE ON PROCEDURE SYS.ATTACH_LOADER TO " +
    		"thr_" + getMyTid();
    try {
      conn.createStatement().execute(grantProcedure);
      Log.getLogWriter().info("executed " + grantProcedure);
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }   
    
    conn = getAuthGfxdConnection();
    try {
      addLoader(conn);
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
  }
  
  protected void addLoader(Connection conn) throws SQLException {
    String schemaName = "trade";
    String tableName = "buyorders";
    String loader = "sql.loader.BuyOrdersLoader";
    String initStr = testLoaderCreateRandomRow? "createRandomRow": null;

    addLoader(conn, schemaName, tableName, loader, initStr);
    conn.commit(); //no effect as ddl is autocommit
  }
  
  /**
   * create user password pair for each thread 
   * for multiple system user case, set up the system user as well
   */
  @SuppressWarnings("unchecked")
  protected synchronized void initUsers() {
    int tid = getMyTid();
    String user;
    String password;
    user = userPrefix + tid;
    password = user;
    getLock();
    Map<String, String> userPasswd = (Map<String, String>) SQLBB.getBB().
      getSharedMap().get(userPasswdMap);
    userPasswd.put(user, password);
    SQLBB.getBB().getSharedMap().put(userPasswdMap, userPasswd);
    if (systemUser == null && hasMultiSystemUsers) {
      systemUser = userPrefix + getMyTid();
      Map<String, String> systemUserPasswd = (Map<String, String>) SQLBB.getBB().
      getSharedMap().get(systemUserPasswdMap);
      systemUserPasswd.put(systemUser, password);
      Log.getLogWriter().info("system user for this member is " + systemUser);
      SQLBB.getBB().getSharedMap().put(systemUserPasswdMap, systemUserPasswd);
    }
    releaseLock();
  }
  
  protected void setupDerbyUserAuthentication() {
    Log.getLogWriter().info("in derby setupUserAuth");
    Connection dConn = getDerbySuperUserConnection();
    AuthenticationTest.getDerbyAuthorizationInfo(dConn);
    try {
      AuthenticationTest.createUserAuthentication(dConn);
    } catch (SQLException se) {
       SQLHelper.handleSQLException(se);
    }
  }
  
  protected void setupGfxdUserAuthentication() {
    Log.getLogWriter().info("in gfxd setupUserAuth");
    Connection conn = getGfxdSuperUserConnection();
    logGfxdSystemProperties();
    try {
      //if (random.nextBoolean()) AuthenticationTest.createUserAuthentication(conn);
      //else createGfxdUserAuthentication(conn);
      createGfxdUserAuthentication(conn);
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
    closeGFEConnection(conn);
  }
  
  protected void logGfxdSystemProperties() {
    Properties sprops = System.getProperties();
    Log.getLogWriter().info("Value of gemfirexd system properties sqlAuthorization is "
        + sprops.getProperty("gemfirexd.sql-authorization"));
    Log.getLogWriter().info("Value of gemfirexd system properties defaultConnectionMode is "
        + sprops.getProperty("gemfirexd.authz-default-connection-mode"));
  }
  
  @SuppressWarnings("unchecked")
  public static void createGfxdUserAuthentication(Connection conn)
      throws SQLException {
    Map<String, String> userPasswd = (Map<String, String>) SQLBB.getBB()
        .getSharedMap().get(SQLSecurityTest.userPasswdMap);
    if (useBUILTIN) {
      try {
        for (Map.Entry<String, String> e : userPasswd.entrySet()) {
          changeUsersPassword(conn, e.getKey(), "", e.getValue());
          throw new TestException("change_password call should get exception " +
          		"when the user has not been created");
        }
      }catch (SQLException se){
        if (se.getSQLState().equalsIgnoreCase("28502")) {
          Log.getLogWriter().info("got expected user not valid exception, continuing tests");
        } else {
          SQLHelper.handleSQLException(se); 
        } 
      }
    }
    
    try {
      for (Map.Entry<String, String> e : userPasswd.entrySet()) {
        createGfxdUsers(conn, e.getKey(), e.getValue());
      }
    }catch (SQLException se){
      /*
      if (se.getSQLState().equalsIgnoreCase("28502")) {
        Log.getLogWriter().info("got expected user definition is not prefixed with " +
            "'gemfirexd.user.' for BUILTIN scheme, continuing tests");
      } else {
      */
        SQLHelper.handleSQLException(se); //gemfirexd.user. prefix is optional now, so the above check is no longer valid
      //}
    }
    
    if (useBUILTIN) {
      for (Map.Entry<String, String> e : userPasswd.entrySet()) {
        //test when using incorrect password
        String emptyPasswd = "";
        String oldPasswd = e.getValue();
        try {
          changeUsersPassword(conn, Property.USER_PROPERTY_PREFIX + e.getKey(), emptyPasswd, oldPasswd);
        } catch (SQLException se){
          SQLHelper.handleSQLException(se); //system user is allowed to override the password
        }
        
        //change_password system call
        if (random.nextBoolean()) {
          String newPasswd = oldPasswd + 'a';
          changeUsersPassword(conn, Property.USER_PROPERTY_PREFIX + e.getKey(), oldPasswd, newPasswd);
          oldPasswd = newPasswd;
        }
        changeUsersPassword(conn, Property.USER_PROPERTY_PREFIX + e.getKey(), oldPasswd, e.getValue());
      }
    }
    boolean[] userCreated = new boolean[1];
    userCreated[0] = true;
    SQLBB.getBB().getSharedMap().put(gfxdUserCreated, userCreated);
  }
  
  //each user needs existing one to change the password
  public static void HydraTask_changePassword() {
    if (!useLDAP) ssTest.changePassword();
  } 
  
  protected void changePassword() {
    Log.getLogWriter().info("in gfxd to change my password");
    Connection conn = getAuthGfxdConnection();
    try {
      changePassword(conn);
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
    closeGFEConnection(conn);
  }
  
  public void changePassword(Connection conn)
  throws SQLException {   
    if (useBUILTIN) {
      //test when using incorrect password
      String prefix = "thr_";
      String oldPasswd = prefix + getMyTid();
      String newPasswd = oldPasswd;
      String user = oldPasswd;
      String otheruser = prefix + (getMyTid()-1);
      String wrongPasswd = oldPasswd + 'b';
      boolean reproduce47917 = false; //temporarily setting to avoid #47917 in regression

      //add the test case when another user tries to change password
      if (getMyTid() != 0 && reproduce47917 == true) {
        try {
          changeUsersPassword(conn, Property.USER_PROPERTY_PREFIX + otheruser, wrongPasswd, oldPasswd);
          throw new TestException("change_password call should get exception when " +
          		"the old password is an invalid one");
        } catch (SQLException se){
          if (se.getSQLState().equals("08004")) {
            log().info("Got expected invalid password exception, continue testing");
            //assume 08004 is the exception to be thrown for invalid existing password
          } else SQLHelper.handleSQLException(se);
        }
      }


      //change_password system call
      if (random.nextBoolean()) {
        String changedPasswd = oldPasswd + 'a';
        changeUsersPassword(conn, Property.USER_PROPERTY_PREFIX + user, oldPasswd, changedPasswd);
        Connection newconn = getAuthGfxdConnection(user, changedPasswd);
        closeGFEConnection(newconn);
        try {
          newconn = getAuthGfxdConnection(user, oldPasswd);
        } catch (SQLException se) {
          if (se.getSQLState().equals("08004")) {
            log().info("Got expected invalid password exception, continue testing");
          } else SQLHelper.handleSQLException(se);
        }
        closeGFEConnection(newconn);
        oldPasswd = changedPasswd;
      }
      changeUsersPassword(conn, Property.USER_PROPERTY_PREFIX + user, oldPasswd, newPasswd);
    }
    boolean[] userCreated = new boolean[1];
    userCreated[0] = true;
    SQLBB.getBB().getSharedMap().put(gfxdUserCreated, userCreated);
  }
  
  private static void createGfxdUsers(Connection conn, 
      String user, String passwd)throws SQLException {
    CallableStatement cs;
    String sql = "CALL sys.create_user('" + user+ 
       "', '" + passwd + "')";
    Log.getLogWriter().info(sql);
    cs = conn.prepareCall(sql);
    cs.execute();
    conn.commit();
  } 
  
  private static void changeUsersPassword(Connection conn, 
      String user, String oldPasswd, String newPasswd) throws SQLException {
    CallableStatement cs;
    String sql = "CALL sys.change_password('" + user+ 
       "', '" + oldPasswd + "', '" + newPasswd + "')";
    Log.getLogWriter().info(sql);
    cs = conn.prepareCall(sql);
    cs.execute();
    conn.commit();
  }
  
  //for the first create when security is enabled
  protected Connection getDerbySuperUserConnection() {
    Connection conn = null;
    try {
      conn = ClientDiscDBManager.getSuperUserConnection();
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException ("Not able to get connection as a super user: " +
           TestHelper.getStackTrace(e));
    }
    return conn;
  }
  
  protected Connection getGfxdSuperUserConnection() {
    Connection conn = null;
    try {
      conn = GFEDBManager.getSuperUserConnection();
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException ("Not able to get connection as system user " 
          + TestHelper.getStackTrace(e));
    }
    return conn;
  }
  
  protected Connection getGfxdSystemUserConnection() {
    Connection conn = null;
    try {
      conn = GFEDBManager.getAuthConnection(systemUser);
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException ("Not able to get connection as system user " 
          + TestHelper.getStackTrace(e));
    }
    return conn;
  }
  
  public static void HydraTask_turnOnAuthorization() {
    ssTest.turnOnAuthorization();
   }
    
  protected void turnOnAuthorization() {
    Connection dConn =null;
    if (hasDerbyServer) {
      dConn = getDerbySuperUserConnection();
    }  //when not test uniqueKeys and not in serial execution, only connection to gfe is provided.
    Connection gConn = getGfxdSuperUserConnection();
    AuthenticationTest.turnOnAuthorization(dConn, gConn);
  }
    
  protected Connection getAuthGfxdConnection() {
    Connection conn = null;
    try {
      conn = GFEDBManager.getAuthConnection(getMyTid());
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException ("Not able to get connection " + TestHelper.getStackTrace(e));
    }
    return conn;
  }
  
  protected Connection getAuthGfxdConnection(String user) {
    Connection conn = null;
    try {
      conn = GFEDBManager.getAuthConnection(user);
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException ("Not able to get connection " + TestHelper.getStackTrace(e));
    }
    return conn;
  }
  
  protected Connection getAuthGfxdConnection(String user, String password) throws SQLException{
    Connection conn = null;
    try {
      conn = GFEDBManager.getAuthConnection(user, password);
    } catch (SQLException se) {
      SQLHelper.printSQLException(se);
      throw se;
    }
    return conn;
  }
  
  protected Connection getInvalidAuthGfxdConnection() {
    Connection conn = null;
    try {
      conn = GFEDBManager.getInvalidAuthConnection(getMyTid());
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException ("Not able to get connection " + TestHelper.getStackTrace(e));
    }
    return conn;
  }
  
  protected Connection getNoneAuthGfxdConnection() {
    Connection conn = null;
    try {
      conn = GFEDBManager.getNoneAuthConnection(getMyTid());
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException ("Not able to get connection " + TestHelper.getStackTrace(e));
    }
    return conn;
  }
  
  protected Connection getAuthDerbyConnection() {
    Connection conn = null;
    try {
      conn = ClientDiscDBManager.getAuthConnection(getMyTid());  //user, password requried
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException ("Not able to create database " + TestHelper.getStackTrace(e));
    }
    return conn;
  }
  
  public static synchronized void HydraTask_createDiscDB() {
    ssTest.createDiscDB();
  }
  
  protected void createDiscDB() {
    if (hasDerbyServer && discConn == null) {
      while (true) {
        try {
          discConn = getFirstDiscConnection();
          break;
        } catch (SQLException se) {
          Log.getLogWriter().info("Not able to connect to Derby server yet, Derby server may not be ready.");
          SQLHelper.printSQLException(se);
          int sleepMS = 10000;
          MasterController.sleepForMs(sleepMS); //sleep 10 sec to wait for Derby server to be ready.
        }
      }
    }
  }
  
  public static synchronized void HydraTask_createDiscSchemas() {
    ssTest.createDiscSchemas();
  }

  public static synchronized void HydraTask_createDiscTables(){
    ssTest.createDiscTables();
  }
  
  protected void createDiscSchemas() {
    if (!hasDerbyServer) {
      return;
    }
    Connection conn = null;
    if (sqlAuthorization)
      conn= getDerbySuperUserConnection();
    else
      conn = getAuthDerbyConnection();
    Log.getLogWriter().info("creating schemas on disc.");
    createSchemas(conn);
    Log.getLogWriter().info("done creating schemas on disc.");
    closeDiscConnection(conn);
  }

  protected void createDiscTables() {
    if (!hasDerbyServer) {
      return;
    }
    Connection conn = getAuthDerbyConnection();
    Log.getLogWriter().info("creating tables on disc.");
    createTables(conn);
    Log.getLogWriter().info("done creating tables on disc.");
    closeDiscConnection(conn);
  }
  
  public static void HydraTask_createGFETables(){
    ssTest.createFuncMonth();  //partition by expression
    ssTest.createGFETables();
  }
  
  protected void createFuncMonth(){
    Connection conn = getAuthGfxdConnection();
    createFuncMonth(conn);
    closeGFEConnection(conn);
  }

  protected void createGFETables() {
    Connection conn = getAuthGfxdConnection();
    //Connection conn = getAuthGfxdConnection("trade");
    Log.getLogWriter().info("dropping tables in gfe.");
    dropTables(conn); //drop table before creating it -- impact for ddl replay
    Log.getLogWriter().info("done dropping tables in gfe");
    Log.getLogWriter().info("creating tables in gfe.");
    createTables(conn);
    Log.getLogWriter().info("done creating tables in gfe.");
    closeGFEConnection(conn);
  }
  
  public static void HydraTask_createGFESchemas() {
    ssTest.createGFESchemas();
  }
  
  //use gfe conn to create schemas
  protected void createGFESchemas() {
    Connection conn = null;
    if (!sqlAuthorization || !bootedAsSuperUser)
     conn = getAuthGfxdConnection();
    else {
      conn = getGfxdSuperUserConnection();
    }
    Log.getLogWriter().info("creating schemas in gfxd.");
    if (!testServerGroupsInheritence) createSchemas(conn);
    else {
      String[] schemas = SQLPrms.getGFESchemas(); //with server group
      createSchemas(conn, schemas);
    }
    Log.getLogWriter().info("done creating schemas in gfxd.");
    closeGFEConnection(conn);
  }
  
  public static void HydraTask_createFuncForSubquery() {
    ssTest.createFuncForSubquery();
  }
  
  protected void createFuncForSubquery() {
    Log.getLogWriter().info("performing create function maxCid Op, myTid is " + getMyTid());
    Connection dConn =null;
    if (hasDerbyServer) {
      dConn = getAuthDerbyConnection();
    }
    Connection gConn = getAuthGfxdConnection();

    try {
      FunctionDDLStmt.createFuncMaxCid(dConn);
      FunctionDDLStmt.createFuncMaxCid(gConn);
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }

    if (dConn!= null) {
      commit(dConn);
      closeDiscConnection(dConn);
    }

    commit(gConn);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_createFunctionToPopulate() {
    ssTest.createFunctionToPopulate();
  }
  
  protected void createFunctionToPopulate() {
    Connection gConn = getAuthGfxdConnection();
    try {
      FunctionDDLStmt.createFuncPortf(gConn);
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_setTableCols() {
    ssTest.setTableCols();
  }
  
  protected void setTableCols() {
    Connection gConn = getAuthGfxdConnection();
    setTableCols(gConn);
    commit(gConn);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_fineGrainAuthorization() {
    ssTest.fineGrainAuthorization();
  }
  
  protected void fineGrainAuthorization() {
  }
  
  public static void HydraTask_provideAllPrivToAll() {
    ssTest.provideAllPrivToAll();
  }
  
  protected void provideAllPrivToAll() {
    Log.getLogWriter().info("performing ddlOp, myTid is " + getMyTid());
    Connection dConn =null;
    if (hasDerbyServer) {
      dConn = getAuthDerbyConnection();
    }  //when not test uniqueKeys and not in serial execution, only connection to gfe is provided.
    Connection gConn = getAuthGfxdConnection();

    AuthorizationDDLStmt authDDL = new AuthorizationDDLStmt();

    authDDL.provideAllPrivToAll(dConn, gConn);

    if (dConn !=null)    closeDiscConnection(dConn);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_connectWithInvalidPassword() {
    ssTest.connectWithInvalidPassword();
  }
  
  public static void HydraTask_connectWithNoCredentials() {
    ssTest.connectWithNoCredentials();
  }
  
  protected void connectWithInvalidPassword() {
    if (hasDerbyServer) {
      List<SQLException> exList = new ArrayList<SQLException>();
      try {
        ClientDiscDBManager.getInvalidAuthConnection(getMyTid());
      } catch (SQLException se) {
        SQLHelper.handleDerbySQLException(se, exList);
      } 
      try {
        GFEDBManager.getInvalidAuthConnection(getMyTid());
      } catch (SQLException se) {
        SQLHelper.handleGFGFXDException(se, exList);
      }
      SQLHelper.handleMissedSQLException(exList);
    } else {
      try {
        GFEDBManager.getInvalidAuthConnection(getMyTid());
      } catch (SQLException se) {
        if (se.getSQLState().equalsIgnoreCase("08004")) {
          Log.getLogWriter().info("Got expected invalid credentials, continuing tests");
          return;
        } else SQLHelper.handleSQLException(se);
      }
      throw new TestException("does not get invalid credentials exception while using " +
          "invalid credentials");
    }
    
  }
  
  protected void connectWithNoCredentials() {
    List<SQLException> exList = new ArrayList<SQLException>();
    if (hasDerbyServer) {
      try {
        ClientDiscDBManager.getNoneAuthConnection(getMyTid());
      } catch (SQLException se) {
        SQLHelper.handleDerbySQLException(se, exList);
      }
      try {
        GFEDBManager.getNoneAuthConnection(getMyTid());
      } catch (SQLException se) {
        SQLHelper.handleGFGFXDException(se, exList);
      }
      SQLHelper.handleMissedSQLException(exList);
    } else {
      try {
        GFEDBManager.getInvalidAuthConnection(getMyTid());
      } catch (SQLException se) {
        if (se.getSQLState().equalsIgnoreCase("08004")) {
          Log.getLogWriter().info("Got expected invalid credentials, continuing tests");
          return;
        } else SQLHelper.handleSQLException(se);
      }
      throw new TestException("does not get invalid credentials exception while using " +
          "no credentials");
    }
    
  }
  
  public static void HydraTask_populateTables() {
    ssTest.populateTables();
  }
  
  protected void populateTables() {
    Connection dConn =getCorrectDerbyAuthConnection();
    Connection gConn = getAuthGfxdConnection();
    populateTables(dConn, gConn);
    if (dConn !=null)    closeDiscConnection(dConn);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_getUserAccessPriviledge() {
    ssTest.getUserAccessPriviledge(); 
  }
  
  protected void getUserAccessPriviledge() {
    Connection dConn = null;
    Connection gConn = null;
    if (hasDerbyServer) {
      dConn = getDerbySuperUserConnection();
      AuthenticationTest.logFullAccessUsers(dConn);
      AuthenticationTest.logReadyOnlyAccessUsers(dConn);
      //AuthenticationTest.getDerbyAuthorizationInfo(dConn);
    } 
    gConn = getGfxdSuperUserConnection();
    AuthenticationTest.logFullAccessUsers(gConn);
    AuthenticationTest.logReadyOnlyAccessUsers(gConn);
    AuthenticationTest.getGfxdAuthorizationInfo(gConn);
    if (dConn !=null)    closeDiscConnection(dConn);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_setUserAccessPriviledge() {
    ssTest.setUserAccessPriviledge(); 
  }
  
  protected void setUserAccessPriviledge() {
    Connection dConn = null;
    Connection gConn = getGfxdSuperUserConnection();
    setUserAccessPriviledge(dConn, gConn);
    if (dConn !=null)    closeDiscConnection(dConn);
    closeGFEConnection(gConn);
  }
  
  @SuppressWarnings("unchecked")
  protected void setUserAccessPriviledge(Connection dConn, Connection gConn) {
    Map<String, String> userPasswd = (Map<String, String>) SQLBB.getBB()
    .getSharedMap().get(SQLSecurityTest.userPasswdMap);
    StringBuilder fullAccessUsers = new StringBuilder();
    StringBuilder readOnlyAccessUsers = new StringBuilder();
    for (String e : userPasswd.keySet()) {
      if (e.equalsIgnoreCase("thr_" + getMyTid())) {
        fullAccessUsers.append(e +",");
      } else {
        if(random.nextBoolean())
          fullAccessUsers.append(e +",");
        else
        readOnlyAccessUsers.append(e +",");
      }
    }
    fullAccessUsers.delete(fullAccessUsers.lastIndexOf(","),
        fullAccessUsers.length());
    readOnlyAccessUsers.delete(readOnlyAccessUsers.lastIndexOf(","), 
        readOnlyAccessUsers.length());
    if (hasDerbyServer) {
      dConn = getDerbySuperUserConnection();
      AuthenticationTest.setFullAccessMode(dConn, gConn, 
        fullAccessUsers.toString());
      AuthenticationTest.setReadOnlyAccessMode(dConn, gConn,
        readOnlyAccessUsers.toString());
    }
    else {
      AuthenticationTest.setUserFullAccessMode(gConn, 
        fullAccessUsers.toString());
      AuthenticationTest.setUserReadOnlyAccessMode(gConn, 
        readOnlyAccessUsers.toString());
    }
    AuthenticationTest.logFullAccessUsers(dConn, gConn);
    AuthenticationTest.logReadyOnlyAccessUsers(dConn, gConn);
  }
  
  public static synchronized void HydraTask_shutDownDerby(){
    ssTest.shutDownDerby();
  }
  
  protected void shutDownDerby() {
    //if (hasDerbyServer && !hasPersistentTables) {
    if (hasDerbyServer) {
      ClientDiscDBManager.shutDownDB();
      discConn = null;
    }
  }
  
  public static synchronized void HydraTask_shutDownGemFireXD() {
    ssTest.shutDownGemFireXD();
  }
  
  public static synchronized void HydraTask_shutDownGemFireXDAsSuperUser() {
    ssTest.shutDownGemFireXDAsSuperUser();
  }
  
  protected void shutDownGemFireXDAsSuperUser(){
    synchronized (serverStarted) {
      if (!serverStarted[0]) return;
      else {
        GFEDBManager.shutDownDB();
        serverStarted[0] = false;
      }
    }
  }
  
  protected void shutDownGemFireXD(){
    if (!serverStarted[0]) return;
    else {
      GFEDBManager.shutDownDB("thr_" + getMyTid(), "thr_" + getMyTid());
      serverStarted[0] = false;
    }
  }
  
  public static void HydraTask_createProcedures() {
    ssTest.createProcedures();
  }
  
  protected void createProcedures() {
    Log.getLogWriter().info("performing create procedure Op, myTid is " + getMyTid());
    Connection dConn =getCorrectDerbyAuthConnection();
    Connection gConn = getAuthGfxdConnection();
    createProcedures(dConn, gConn);
    
    if (dConn!= null) {
      closeDiscConnection(dConn);
    }
    closeGFEConnection(gConn);
  }
  
  //could return null when no derby server in the test
  protected Connection getCorrectDerbyAuthConnection() {
    Connection dConn =null;
    if (hasDerbyServer) {
      if (!bootedAsSuperUser && systemUser.equalsIgnoreCase(userPrefix + getMyTid())){
        dConn = getDerbySuperUserConnection();
        //tread gfxd system user as derby super user
      }
      else
        dConn = getAuthDerbyConnection(); 
    }//when not testing uniqueKeys and not in serial executi
    return dConn;
  }
  
  public static void HydraTask_doDMLOp() {
    ssTest.doDMLOp();
  }
  
  public static void HydraTask_doOps() {
    ssTest.doOps();
  }

  protected void doOps() {
    if (random.nextInt(100) == 1) {
      connectWithInvalidPassword();
    }
    Connection dConn =getCorrectDerbyAuthConnection();
    Connection gConn = getAuthGfxdConnection();
    doOps(dConn, gConn);
    
    if (hasDerbyServer) {
      closeDiscConnection(dConn);
    }
    closeGFEConnection(gConn);
    if (hasDerbyServer) {
      waitForBarrier();
    }
  }
  
  protected void doOps(Connection dConn, Connection gConn) {
    doAuthOp(dConn, gConn);
    
    if (hasDerbyServer) {
      waitForBarrier();
    //synchronize the authorization operation before the new connection takes effects
    }
    
    if (hasRoutine) { 
      int num = 10;
      boolean dmlOp = (random.nextInt(num)==0)? false: true; //1 out of num chance to do ddl operation
      if (dmlOp) doDMLOp(dConn, gConn);
      else if (random.nextBoolean()) doDDLOp(dConn,gConn);
      else callProcedures(dConn, gConn);  //call procedure
    }
    else
      doDMLOp(dConn, gConn);
  }
  
  /*
   * in security test as of now, the "with grant option" is not supported in gemfirexd/derby
   * therefore only the object owner could grant/revoke the privileges for the object.
   * test needs to be changed once with grant option is supported.
   * @see sql.SQLTest#doDMLOp()
   */
  protected void doDMLOp() {
    //only object owner (ddl thread) could do grant/revoke operations 
    //as system user could not do these operations (get SQLException-42509
    //Specified grant or revoke operation is not allowed), we do not need 
    //synch derby and gemfirexd for the grant/revoke operation yet
    doAuthOp();
    
    if (hasDerbyServer) {
      waitForBarrier();
    //synchronize the authorization operation before the new connection takes effects
    }
    
    Connection dConn =getCorrectDerbyAuthConnection();
    Connection gConn = getAuthGfxdConnection();

    
    doDMLOp(dConn, gConn);
    if (hasDerbyServer) {
      closeDiscConnection(dConn);
    }
    closeGFEConnection(gConn);
    if (hasDerbyServer) {
      waitForBarrier();
    }
    //synchronized, so that authorization in derby and gemfirexd could take effect at 
    //the same time.
  }
  
  public static void HydraTask_doAuthOp() {
    ssTest.doAuthOp();
  }
  
  /*
   * in security test as of now, the "with grant option" is not supported in gemfirexd/derby
   * therefore only the object owner could grant/revoke the privileges for the object.
   * test needs to be changed once with grant option is supported to synchronize the operations
   * in both derby and gemfirexd -- before acquiring the connection, both should have the same
   * privileges
   */
  protected void doAuthOp() {
    Connection dConn =getCorrectDerbyAuthConnection();
    Connection gConn = getAuthGfxdConnection();
    doAuthOp(dConn, gConn);
    if (hasDerbyServer) {
      closeDiscConnection(dConn);
    }
    closeGFEConnection(gConn);
  }
  
  protected void doAuthOp(Connection dConn, Connection gConn) {
    //perform the opeartions
    DDLStmtIF authStmt= ddlFactory.createDDLStmt(DDLStmtsFactory.AUTHORIZATION); //dmlStmt of a table

    authStmt.doDDLOp(dConn, gConn);

    commit(dConn); 
    commit(gConn);


    Log.getLogWriter().info("done ddlOp");
  }
  
  public static void HydraTask_verifyResultSets() {
    ssTest.verifyResultSets();
  }
  
  protected void verifyResultSets() {
    if (!hasDerbyServer) {
      Log.getLogWriter().info("skipping verification of query results "
          + "due to manageDerbyServer as false, myTid=" + getMyTid());
      return;
    }
    Connection dConn = getAuthDerbyConnection();
    Connection gConn = getAuthGfxdConnection();

    verifyResultSets(dConn, gConn);
    closeDiscConnection(dConn);
    closeGFEConnection(gConn);
    
  }
  
  public static synchronized void HydraTask_reStartAuthFabricServerWithAuthorizationAsSuperUser() {
    ssTest.reStartAuthFabricServerWithAuthorizationAsSuperUser();
  }
  
  protected void reStartAuthFabricServerWithAuthorizationAsSuperUser() {
    if (serverStarted[0]) {
      Log.getLogWriter().info("to workaround #42499, no more fabric servers " +
      "will be started");
      return;
    } 
    Properties p = FabricServerHelper.getBootProperties();
    Log.getLogWriter().info("gfxd sqlAuthorization is " + 
      p.getProperty(com.pivotal.gemfirexd.Property.SQL_AUTHORIZATION));
    //if (random.nextBoolean())  //TODO to comment out this
    p.setProperty(com.pivotal.gemfirexd.Property.SQL_AUTHORIZATION, "true");
    Log.getLogWriter().info("boot with this sqlAuthorization setting -- " +
        com.pivotal.gemfirexd.Property.SQL_AUTHORIZATION + "=" + 
        p.getProperty(com.pivotal.gemfirexd.Property.SQL_AUTHORIZATION));
    FabricServer fs = FabricServiceManager.getFabricServerInstance();
    // default to partitioned tables for tests
    p.setProperty("table-default-partitioned", "true");
    if (p != null
        && "false".equalsIgnoreCase(p
            .getProperty(com.pivotal.gemfirexd.Attribute.GFXD_HOST_DATA))
        && "true".equalsIgnoreCase(p
            .getProperty(com.pivotal.gemfirexd.Attribute.GFXD_PERSIST_DD))) {
      p.setProperty(com.pivotal.gemfirexd.Attribute.GFXD_PERSIST_DD, "false");
    }
    try {
      fs.start(p);
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    }
    serverStarted[0] = true;  //to avoid #42499, needs to be in the synchronized method
    sqlAuthorization = true;
    

  }
  
  public static HydraThreadLocal derbySuperUserConn = new HydraThreadLocal();
  public static HydraThreadLocal gfxdSuperUserConn = new HydraThreadLocal();
  
  //for different system users
  public static HydraThreadLocal gfxdSystemUserConn= new HydraThreadLocal(); 

  
  public static void HydraTask_setSuperUserConnections() {
    ssTest.initThreadLocalConnection();
  }
  /** 
   * Sets the per-thread Connection instance.
   */
  protected void initThreadLocalConnection() {
    if (hasDerbyServer) {
      Connection derbySuperUser = getDerbySuperUserConnection();
      derbySuperUserConn.set(derbySuperUser);
    }
    if (bootedAsSuperUser) {
      Connection gfxdSuperUser = getGfxdSuperUserConnection();
      gfxdSuperUserConn.set(gfxdSuperUser);
    } else {
      Log.getLogWriter().info("booted as systemUser, getting connection as systemUser");
      Connection gfxdSystemUser = getGfxdSystemUserConnection();
      gfxdSystemUserConn.set(gfxdSystemUser);
    } //used for querying without need to check authorization
  }
  
  public static synchronized void HydraTask_shutdownFabricServers() {
    if (ssTest == null) ssTest = new SQLSecurityTest();
    ssTest.shutdownFabricServers();
  }
  
  public static void HydraTask_shutdownFabricLocators() {
    if (ssTest == null) ssTest = new SQLSecurityTest();
    ssTest.shutdownFabricLocators();
  }
  
  protected void shutdownFabricServers() {
    //FabricServerHelper.stopFabricServer();
    FabricService fs = FabricServiceManager.currentFabricServiceInstance();
    if (fs == null) {
      Log.getLogWriter().info("fabric server already shut down");
      return;
    }
    Properties p = new Properties();
    String user = bootedAsSuperUser ? "superUser" : "thr_" + getMyTid(); 
    p.setProperty(Property.USER_PROPERTY_PREFIX + user, user);
    p.setProperty(random.nextBoolean() ? com.pivotal.gemfirexd.Attribute.USERNAME_ALT_ATTR : com.pivotal.gemfirexd.Attribute.USERNAME_ATTR, user);
    p.setProperty(com.pivotal.gemfirexd.Attribute.PASSWORD_ATTR, user);
    try {
      fs.stop(p);
      Log.getLogWriter().info("fabric server has been shut down");
    } catch (SQLException se) {
      if (se.getSQLState().equalsIgnoreCase("08004")) {
        Log.getLogWriter().info("Got expected invalid credentials, continuing tests");
      } else SQLHelper.handleSQLException(se);
    }
  }
  
  protected void shutdownFabricLocators() {
    shutdownFabricServers();
  }
  
  public static void HydraTask_delegatePrivilege() {
    ssTest.delegatePrivilege();
  }

  protected void delegatePrivilege() {
    Log.getLogWriter().info("performing ddlOp, myTid is " + getMyTid());
    Connection dConn =null;
    if (hasDerbyServer) {
      dConn = getAuthDerbyConnection();
    }  //when not test uniqueKeys and not in serial execution, only connection to gfe is provided.
    Connection gConn = getAuthGfxdConnection();

    AuthorizationDDLStmt authDDL = new AuthorizationDDLStmt();

    authDDL.delegateGrantOption(dConn, gConn);
    
    commit(dConn);
    commit(gConn);
    
    if (dConn !=null) closeDiscConnection(dConn);
    closeGFEConnection(gConn);
  }

  public static void HydraTask_grantDelegatedPrivilege() {
    ssTest.grantDelegatedPrivilege();
  }
  
  public static void HydraTask_revokeDelegatedPrivilege() {
    ssTest.revokeDelegatedPrivilege();
  }

  protected void grantDelegatedPrivilege() {
  Log.getLogWriter().info("performing ddlOp, myTid is " + getMyTid());
    Connection dConn =null;
    if (hasDerbyServer) {
      dConn = getAuthDerbyConnection();
    }  //when not test uniqueKeys and not in serial execution, only connection to gfe is provided.
    Connection gConn = getAuthGfxdConnection();

    AuthorizationDDLStmt authDDL = new AuthorizationDDLStmt();
    authDDL.useDelegatedPrivilege(dConn, gConn);

    if (dConn !=null) closeDiscConnection(dConn);
    closeGFEConnection(gConn);
  }
  
  protected void revokeDelegatedPrivilege() {
    Log.getLogWriter().info("performing ddlOp, myTid is " + getMyTid());
      Connection dConn =null;
      if (hasDerbyServer) {
        dConn = getAuthDerbyConnection();
      }  //when not test uniqueKeys and not in serial execution, only connection to gfe is provided.
    Connection gConn = getAuthGfxdConnection();

    AuthorizationDDLStmt authDDL = new AuthorizationDDLStmt();
    authDDL.revokeDelegatedPrivilege(dConn, gConn);

    if (dConn !=null) closeDiscConnection(dConn);
    closeGFEConnection(gConn);
  }
  
  public static void HydraTask_populateThruLoader() {
    int number = (int)SQLBB.getBB().getSharedCounters().incrementAndRead(SQLBB.populateThruLoader);
    if (number ==1) {
      ssTest.assignSelectToMe();  //only has select privilege
      ssTest.populateThruLoader();
    }
  }

  //populate if Loader is set and no inserts to gfxd
  protected void populateThruLoader() {
    try {
      Connection gConn = GFEDBManager.getAuthConnection(getMyTid());
      populateThruLoader(gConn);
      closeGFEConnection(gConn);
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
    } 
  }
  
  protected void assignSelectToMe(){
    Connection dConn = getDerbySuperUserConnection();
    Connection gConn = getGfxdSuperUserConnection();
    new AuthorizationDDLStmt().assignSelectToMe(dConn, gConn, getMyTid());
    if (dConn !=null)    closeDiscConnection(dConn);
    closeGFEConnection(gConn);
  }
}
