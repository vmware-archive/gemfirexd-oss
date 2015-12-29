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
package sql.mbeans;

import static management.util.HydraUtil.logInfo;
import static sql.mbeans.MBeanHelper.isLocator;
import static sql.mbeans.MBeanHelper.makeCompliantName;
import static sql.mbeans.MBeanHelper.quoteIfNeeded;
import static sql.mbeans.MBeanHelper.saveError;
import static sql.mbeans.MBeanTestConstants.AGGR_MEMBER_ATTRS;
import static sql.mbeans.MBeanTestConstants.MEMBER_ATTRS;
import static sql.mbeans.MBeanTestConstants.MEMBER_METHODS;
import static sql.mbeans.MBeanTestConstants.STATEMENT_ATTRS;
import static sql.mbeans.MBeanTestConstants.TABLE_ATTRS;
import static sql.mbeans.MBeanTestConstants.TABLE_METHODS;
import hydra.ClientVmInfo;
import hydra.DistributedSystemHelper;
import hydra.HydraConfigException;
import hydra.HydraRuntimeException;
import hydra.HydraThreadLocal;
import hydra.Log;
import hydra.MasterController;
import hydra.PortHelper;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import hydra.blackboard.Blackboard;
import hydra.blackboard.SharedCounters;
import hydra.blackboard.SharedLock;
import hydra.gemfirexd.FabricServerDescription;
import hydra.gemfirexd.FabricServerHelper;
import hydra.gemfirexd.FabricServerHelper.Endpoint;
import hydra.gemfirexd.GfxdConfigPrms;
import hydra.gemfirexd.LocatorBlackboard;
import hydra.gemfirexd.NetworkServerHelper;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetAddress;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransactionRollbackException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.RuntimeMBeanException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import management.util.HydraUtil;
import management.util.ManagementUtil;
import sql.SQLBB;
import sql.SQLHelper;
import sql.SQLPrms;
import sql.SQLTest;
import sql.mbeans.listener.CallBackListener;
import sql.sqlutil.GFXDStructImpl;
import sql.sqlutil.ResultSetHelper;
import util.StopStartPrms;
import util.StopStartVMs;
import util.TestException;
import util.TestHelper;

import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.CachePerfStats;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.execute.FunctionStats;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.pivotal.gemfirexd.FabricLocator;
import com.pivotal.gemfirexd.FabricService;
import com.pivotal.gemfirexd.FabricServiceManager;
import com.pivotal.gemfirexd.NetworkInterface;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.management.NetworkServerConnectionStats;
import com.pivotal.gemfirexd.internal.engine.management.NetworkServerNestedConnectionStats;
import com.pivotal.gemfirexd.internal.engine.management.impl.ManagementUtils;
import com.pivotal.gemfirexd.internal.engine.procedure.DistributedProcedureCallFunction;
import com.pivotal.gemfirexd.internal.engine.stats.ConnectionStats;
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore;
import com.pivotal.gemfirexd.internal.impl.sql.StatementStats;



/**
 * This test does not follow SQLTest's conventions : using DML and DDL Factory statements
 * Instead it implements its own workload.
 * 
 * This test does not validating stat collection. This is just validating mbean showing them correctly.
 * @author tushark
 * @author vbalaut
 */

/**
 * 1. MBean is creating different statement is statement is same but written in different case
 *  -- need to write test for the same
 * 2. No MBean for Put Statement - instead in is creating mbean for insert/update
 *  -- need to write test
 * 3. Test for Join Statement
 * 4. Need to write test for client
 * 5. Need to write test for Create Disk Store, Create HDFC, Async Event Queue
 * 6. Need to write test where we have different schema
 * @author vbalaut
 *
 */
public class MBeanTest extends SQLTest {

  private static final String TABLE_SERVER_GROUP = "TABLE_SERVER_GROUP";
  protected static MBeanTest mbeanTest;
  private HydraThreadLocal connectorTL = new HydraThreadLocal();
  private AtomicInteger connectorRetries = new AtomicInteger(0);
  public static final String JMX_URL_FORMAT = "service:jmx:rmi://{0}/jndi/rmi://{0}:{1}/jmxrmi";
  private static final String SELLORDER_INSERT = "insert into {0} values (?,?,?,?,?,?,?,?)";
  private static final String SELLORDER_PUT = "put into {0} values (?,?,?,?,?,?,?,?)";
  private static final String SELLORDER_UPDATE = "update {0} set qty=? where oid=?";
  private static final String SELLORDER_DELETE = "DELETE FROM {0} WHERE qty=? AND oid=?";

  private static final int MAX_RETRIES = MBeanPrms.maxRetries();
  private static String COUNTER_SELLORDER_INSERTS;
  private static String COUNTER_SELLORDER_UPDATES;
  private static String COUNTER_SELLORDER_DELETES;
  private static String COUNTER_SELLORDER_SELECTS;
  private static String COUNTER_SELLORDER_KEYSIZE;
  private static String COUNTER_SELLORDER_ENTRYSIZE;
  private static String COUNTER_SELLORDER_NO_OF_ROWS;
  private static String COUNTER_SELLORDER_COMMITS;
  private static String COUNTER_SELLORDER_ROLLBACKS;
  private static String COUNTER_MBEAN_PROC_CALLS;
  private static String COUNTER_MBEAN_PROC_CALLS_EDGE = "MBEAN_PROC_CALLS_EDGE";
  private static String COUNTER_MBEAN_STATEMENT;
  private static String COUNTER_NS_CLIENT ;
  private static String COUNTER_NS_PEER ;
  private static String COUNTER_NS_NESTED ;
  private static String COUNTER_NS_INTERNAL ;
  private static String COUNTER_MBEAN_PROC_CALLS_IN_PROGRESS;
  private static String COUNTER_MBEAN_PROC_CALLS_IN_PROGRESS_EDGE = "MBEAN_PROC_CALLS_IN_PROGRESS_EDGE";
  private static String COUNTER_MBEAN_PROC_CALLS_PREFIX = "MBEAN_PROC_CALLS_";
  private static String COUNTER_MBEAN_STATEMENT_PREFIX = "MBEAN_STATEMENT_";
  private static String COUNTER_MBEAN_PROC_PROGRESS_PREFIX = "MBEAN_PROC_PROGRESS_";
  private static String MEMBERS_MAPPING = "MEMBERS_MAPPING";
  private static String THIS_MEMBERID;
  private static String MANAGER_RUNNING = "MANAGER_RUNNING";
  private static String MANAGER_READERS = "MANAGER_READERS";
  private static String MANAGER_WRITERS = "MANAGER_WRITERS";  
  public static String distributedRegionONTemplate = "GemFire:service=Region,name=$1,type=Distributed";
  public static String regionONTemplate = ManagementConstants.OBJECTNAME__REGION_MXBEAN;
  public static String memberONTemplate = ManagementConstants.OBJECTNAME__GFXDMEMBER_MXBEAN;
  public static String tableONTemplate = ManagementConstants.OBJECTNAME__TABLE_MXBEAN;
  public static String statementONTemplate = ManagementConstants.OBJECTNAME__STATEMENT_MXBEAN;
  public static String aggrMemberONTemplate = ManagementConstants.OBJECTNAME__AGGREGATEMEMBER_MXBEAN;
  public static String aggrTableONTemplate = ManagementConstants.OBJECTNAME__AGGREGATETABLE_MXBEAN;
  public static String aggrStatementONTemplate = ManagementConstants.OBJECTNAME__AGGREGATESTATEMENT_MXBEAN;
  public static final String DEFAULT_SERVER_GROUP = ManagementUtils.DEFAULT_SERVER_GROUP;
  public static final String TABLE_COUNTER = "TABLE_COUNTER";
  public static final String CREATE_TABLE_TEMPLATE = "create table emp.MYTABLE_{0} (eid int not null constraint MYTABLE_{0}_pk primary key, emp_name varchar(100), since date, addr varchar(100), ssn varchar(9))";
  public static final String CREATE_TABLE_TEMPLATE_2 = "create table emp.MYTABLE_{0} (eid int not null constraint MYTABLE_{0}_pk primary key, emp_name varchar(100), since date, addr varchar(100), ssn varchar(9), COUNTRY_ISO_CODE CHAR(2) CONSTRAINT COUNTRIES_FK REFERENCES COUNTRIES (COUNTRY_ISO_CODE) )";
  
  public static final String DROP_TABLE_TEMPLATE = "drop table emp.MYTABLE_{0}";
  public static final String TABLE_NAME_TEMPLATE = "EMP.MYTABLE_{0}";
  
  public static final String PULSE_CREATE_TABLE_TEMAPLATE = "create table {0} (oid int not null constraint orders_pk_{1} primary key, cid int, sid int, qty int, ask decimal (30, 20), order_time timestamp, status varchar(10) default 'open', tid int, constraint portf_fk_{1} foreign key (cid, sid) references trade.portfolio (cid, sid) on delete restrict, constraint status_ch_{1} check (status in ('cancelled', 'open', 'filled')))";
  public static final String CONN_COMMIT_TAG = "CONN_COMMIT";
  public static final String SELECT_QUERY_TAG = "SELECT_QUERY";
  public static final String RENAME_INDEX = "RENAME_INDEX";
  public static final String CREATE_INDEX = "CREATE_INDEX";
  public static final String DROP_INDEX = "DROP_INDEX";
  protected static final String INDEX_MAP = "INDEX_MAP";
  private MBeanHelper mbeanHelper = new MBeanHelper();

  String[] regions = {
      "/TRADE/PORTFOLIO",
      "/TRADE/SELLORDERS"
  };
  private Properties TheFabricServerProperties;
  private boolean useLocks = false;


  public static String[] actions = {
    "execStmt",
    "createTable",
    "createDiskStore",
    "dropTable",
    "dropDiskStore",
    "insert",
    "delete",
    "pulseCounter"
  };

  public static String[] regionList = {
    "/TRADE/SELLORDERS",
    "/TRADE/PORTFOLIO",
    "/TRADE/TXHISTORY",
    "/TRADE/BUYORDERS",
    "/TRADE/SECURITIES",
    "/TRADE/NETWORTH",
    "/TRADE/TRADES",
    "/TRADE/CUSTOMERS"
  };

  public static String[] tableList = {
    "TRADE.SELLORDERS",
    "TRADE.PORTFOLIO",
    "TRADE.TXHISTORY",
    "TRADE.BUYORDERS",
    "TRADE.SECURITIES",
    "TRADE.NETWORTH",
    "TRADE.TRADES",
    "TRADE.CUSTOMERS"
  };

  public static void mBeanTestInitialize(){
    mbeanTest = new MBeanTest();
    //reset all counters
    String clientName = RemoteTestModule.getMyClientName();

    Log.getLogWriter().info("just to check Client Name is :" + clientName);
    COUNTER_SELLORDER_INSERTS = "SELLORDER_INSERTS_" + clientName;
    COUNTER_SELLORDER_UPDATES = "SELLORDER_UPDATES_" + clientName;
    COUNTER_SELLORDER_DELETES = "SELLORDER_DELETES_" + clientName;
    COUNTER_SELLORDER_SELECTS = "SELLORDER_SELECTS_" + clientName;
    COUNTER_SELLORDER_KEYSIZE = "SELLORDER_KEYSIZE_" + clientName;
    COUNTER_SELLORDER_ENTRYSIZE = "SELLORDER_ENTRYSIZE_" + clientName;
    COUNTER_SELLORDER_NO_OF_ROWS = "SELLORDER_NO_OF_ROW_" + clientName;
    COUNTER_NS_CLIENT = "COUNTER_NS_CLIENT_" + clientName;
    COUNTER_NS_PEER = "COUNTER_NS_PEER_" + clientName;
    COUNTER_NS_NESTED = "COUNTER_NS_NESTED_" + clientName;
    COUNTER_NS_INTERNAL = "COUNTER_NS_INTERNAL_" + clientName;
    
    
    COUNTER_SELLORDER_COMMITS = "COMMITS_"+ clientName +"_TX(S";
    COUNTER_SELLORDER_ROLLBACKS = "ROLLBACKS_"+ clientName +"_TXCOMMITS";
    COUNTER_MBEAN_PROC_CALLS = COUNTER_MBEAN_PROC_CALLS_PREFIX + clientName;
    COUNTER_MBEAN_STATEMENT = COUNTER_MBEAN_STATEMENT_PREFIX + clientName;
    COUNTER_MBEAN_PROC_CALLS_IN_PROGRESS = COUNTER_MBEAN_PROC_PROGRESS_PREFIX + clientName;
    THIS_MEMBERID = "MEMBERID_" + clientName;
    

    mbeanTest.saveCounter(COUNTER_SELLORDER_INSERTS, 0);
    mbeanTest.saveCounter(COUNTER_SELLORDER_UPDATES, 0);
    mbeanTest.saveCounter(COUNTER_SELLORDER_DELETES, 0);
    mbeanTest.saveCounter(COUNTER_SELLORDER_SELECTS, new HashMap<String, Integer>());
    mbeanTest.saveCounter(INDEX_MAP, new HashMap<String, String>());
    mbeanTest.saveCounter(COUNTER_MBEAN_STATEMENT, new HashMap<String, Map<String, Object>>());
    mbeanTest.saveCounter(COUNTER_SELLORDER_KEYSIZE, 0.0f);
    mbeanTest.saveCounter(COUNTER_SELLORDER_ENTRYSIZE, 0.0f);
    mbeanTest.saveCounter(COUNTER_SELLORDER_NO_OF_ROWS, 0l);
    mbeanTest.saveCounter(COUNTER_SELLORDER_COMMITS, 0);
    mbeanTest.saveCounter(COUNTER_SELLORDER_ROLLBACKS, 0);
    mbeanTest.saveCounter(COUNTER_MBEAN_PROC_CALLS, 0);
    mbeanTest.saveCounter(COUNTER_MBEAN_PROC_CALLS_EDGE, 0);
    mbeanTest.saveCounter(COUNTER_MBEAN_PROC_CALLS_IN_PROGRESS, 0);
    mbeanTest.saveCounter(COUNTER_MBEAN_PROC_CALLS_IN_PROGRESS_EDGE, 0);
    
    mbeanTest.saveCounter(TABLE_SERVER_GROUP, new HashMap<String, List<String>>());
    
    mbeanTest.saveCounter(COUNTER_NS_CLIENT, new NetworkServerConnectionStats("Client", 0,0,0,0,0,0,0));
    mbeanTest.saveCounter(COUNTER_NS_PEER, new NetworkServerConnectionStats("Peer", 0,0,0,0,0,0,0));
    mbeanTest.saveCounter(COUNTER_NS_NESTED, new NetworkServerNestedConnectionStats("Nested", 0,0,0));
    mbeanTest.saveCounter(COUNTER_NS_INTERNAL, new NetworkServerNestedConnectionStats("Internal", 0,0,0));
    

    Log.getLogWriter().info("Reset all counters for client " + clientName);
    
    /*
    mbeanTest.addListener(new CallBackListener() {
      
      @Override
      public String getName() {
        return "NETWORK_STATE";
      }
      
      @Override
      public void execute(Object... params) {
        if (params[0].equals("CREATE")) {
          mbeanTest.updateBBOnOpenConnction();
          mbeanTest.printOpenConnection("OPEN LISTENER");
          
        }
        if (params[0].equals("CLOSE")) {
          mbeanTest.updateBBOnCloseConnection();
          mbeanTest.printCloseConnection("CLOSE LISTENER");
          
        }
      }
    });
  mbeanTest.addListener(new CallBackListener() {
      
      @Override
      public String getName() {
        return "PRINT_CONN";
      }
      
      @Override
      public void execute(Object... params) {
          mbeanTest.printCloseConnection("PRINT_CONN");
      }
    });
  */
  
  mbeanTest.addListener(new CallBackListener() {
    
    @Override
    public String getName() {
      return RENAME_INDEX;
    }
    
    @Override
    public void execute(Object... params) {
        String oldIndexName = (String)params[0];
        String indexName = (String)params[0];
        SharedLock lock = SQLBB.getBB().getSharedLock();
        try {
          lock.lock();
          @SuppressWarnings("unchecked")
          Map<String, String> indexMap = (Map<String, String>) SQLBB.getBB().getSharedMap().get(INDEX_MAP);
          indexMap.put(indexName, indexMap.get(oldIndexName));
          indexMap.remove(oldIndexName);
        } finally{
          lock.unlock();
        }
      }
  });
  
  mbeanTest.addListener(new CallBackListener() {
    
    @Override
    public String getName() {
      return DROP_INDEX;
    }
    
    @Override
    public void execute(Object... params) {
        String indexName = (String)params[0];
        try {
          lock.lock();
          @SuppressWarnings("unchecked")
          Map<String, String> indexMap = (Map<String, String>) SQLBB.getBB().getSharedMap().get(INDEX_MAP);
          indexMap.remove(indexName);
          Log.getLogWriter().info("#dropIndex Index removed " + indexName);
        } finally {
          lock.unlock();
        }
      }
  });
  
  mbeanTest.addListener(new CallBackListener() {
    
    @Override
    public String getName() {
      return CREATE_INDEX;
    }
    
    @Override
    public void execute(Object... params) {
      
          String indexName = (String)params[0];
          String tableName = (String)params[1];
          try {
            lock.lock();
            @SuppressWarnings("unchecked")
            Map<String, String> indexMap = (Map<String, String>) SQLBB.getBB().getSharedMap().get(INDEX_MAP);
            Log.getLogWriter().info("#createIndex Index created  " + indexName + " on table " + tableName);
            indexMap.put(indexName, tableName);
          } finally {
            lock.unlock();
          }
    }
  });
  }

  @SuppressWarnings("unused")
  private void updateBBOnOpenConnction() {
    boolean isManagerRunning = getCounter(MANAGER_RUNNING)>0;
    if(isManagerRunning) {
      SharedLock lock = SQLBB.getBB().getSharedLock();
      try {
        lock.lock();
        NetworkServerConnectionStats connStat = (NetworkServerConnectionStats) SQLBB
            .getBB().getSharedMap().get(COUNTER_NS_PEER);
        connStat.updateNetworkServerConnectionStats(
            connStat.getConnectionsOpened() + 1, connStat.getConnectionsClosed(),
            connStat.getConnectionsAttempted() + 1,
            connStat.getConnectionsFailed(), connStat.getConnectionLifeTime(),
            connStat.getConnectionsOpen(),
            connStat.getConnectionsIdle() + 1);
        SQLBB.getBB().getSharedMap().put(COUNTER_NS_PEER, connStat);
        Log.getLogWriter().info(
            "Shared Map Updated after connection create for " + COUNTER_NS_PEER
                + ", value : " + connStat.toString());
      } finally {
        lock.unlock();
      }
    }
  }
  
  
  //Overriden for adding server groups
  public static void HydraTask_createGFEDB() {
    if(mbeanTest==null){
      mBeanTestInitialize();
    }
    mbeanTest.createGFEDB2();
  }

  public static void HydraTask_doHA() {
    if(mbeanTest==null){
      mBeanTestInitialize();
    }
    mbeanTest.doHA();
  }

  public static void HydraTask_execTest() {

    if (mbeanTest == null) {
      mBeanTestInitialize();
    }

    String test = MBeanPrms.getTests();
    if ("execStmt".equals(test)) {
      mbeanTest.execStmt();
    } else if ("pulseCounter".equals(test)) {
      mbeanTest.pulseCounter();
    }
  }

  public static void HydraTask_execTestUnderLock() {

    if (mbeanTest == null) {
      mBeanTestInitialize();
    }
    mbeanTest.pulseCounterUnderLock();
  }
  
  public static void HydraTask_callSQLProcs() {
    if(mbeanTest==null){
      mBeanTestInitialize();
    }
    mbeanTest.callSQLProcs();

  }

  public static void HydraTask_createDropTable() {

    if(mbeanTest==null){
      mBeanTestInitialize();
    }

    mbeanTest.createDropTable();

  }

  public static void HydraTask_startManager(){
    if(mbeanTest==null){
      mBeanTestInitialize();
    }
    mbeanTest.startManager();

  }

  public static void HydraTask_printCounters(){
    if(mbeanTest==null){
      mBeanTestInitialize();
    }
    mbeanTest.printCounters();
  }

  public static void HydraTask_prepareTest(){
    if(mbeanTest==null){
      mBeanTestInitialize();
    }
    mbeanTest.prepareTest();
  }

  public static void HydraTask_verifyAggregateValues() {
    if (mbeanTest == null) {
      mBeanTestInitialize();
    }
    mbeanTest.verifyAggregateValues();
  }

  public static void HydraTask_verifyTableMBeanValues() {
    if (mbeanTest == null) {
      mBeanTestInitialize();
    }
    mbeanTest.verifyTableMBeanValues();
  }
  
  public static void HydraTask_validateMemberView(){
    if(mbeanTest==null){
      mBeanTestInitialize();
    }
    mbeanTest.validateMemberView();
  }

  public static void HydraTask_populateDataToBlackBoard(){
    if(mbeanTest==null){
      mBeanTestInitialize();
    }
    mbeanTest.populateDataToBB();
  }
  
  public static void HydraTask_restartManager(){
    if(mbeanTest==null){
      mBeanTestInitialize();
    }
    mbeanTest.restartManager();
  }

  public static void HydraTask_restartManagerVM(){
    if(mbeanTest==null){
      mBeanTestInitialize();
    }
    mbeanTest.restartManagerVM();
  }
  
  public static void HydraTask_startLocatorWithManagerRole(){
    if(mbeanTest==null){
      mBeanTestInitialize();
    }
    mbeanTest.startLocatorWithManagerRole(true);
  }
  
  public static void HydraTask_createMultipleTables(){
    if(mbeanTest==null){
      mBeanTestInitialize();
    }
    mbeanTest.createMultipleTables();
  }
  
  public static void HydraTask_pulseStabilityTest(){
    if(mbeanTest==null){
      mBeanTestInitialize();
    }
    mbeanTest.pulseStabilityTest();
  }

  
  public static void HydraTask_createDropIndex() {
    if (mbeanTest == null) {
      mBeanTestInitialize();
    }
    mbeanTest.createDropIndexTest();
  }

  public static void HydraTask_alterTableTest() {
    if (mbeanTest == null) {
      mBeanTestInitialize();
    }
    mbeanTest.alterTableTest();
  }
  
  public static void HydraTask_waitForMBeanUpdater() {
    if (mbeanTest == null) {
      mBeanTestInitialize();
    }
    mbeanTest.waitForMBeanUpdater();
  }
  
  public static void HydraTask_validateAggregatedMemberMBean(){
    if (mbeanTest == null) {
      mBeanTestInitialize();
    }
    mbeanTest.validateAggregatedMemberMBean();
  }
  
  public static synchronized void HydraTask_dropRandomTable() {
    if (mbeanTest == null) {
      mBeanTestInitialize();
    }
    mbeanTest.dropRandomTable();
  }

  private void dropRandomTable() {
    // TODO Auto-generated method stub
    
  }

  public static synchronized void HydraTask_alterRandomTable() {
    if (mbeanTest == null) {
      mBeanTestInitialize();
    }
    mbeanTest.alterRandomTable();
  }
  
  private void alterRandomTable() {
    // TODO Auto-generated method stub
    
  }

  public static synchronized void HydraTask_ddlRandomTable() {
    if (mbeanTest == null) {
      mBeanTestInitialize();
    }
    mbeanTest.ddlRandomTable();
  }

  

  
  public static void HydraTask_checkForError() {
    checkForErrors();
  }
  
  private void ddlRandomTable() {
  }

  private void createMultipleTables() {
    Connection conn = getGFEConnection();
 //   printOpenConnection("createMultipleTables");
    int tableNum=0;
    try {
      for (int i = 0; i < 100; i++) {
        tableNum = incrementCounter(TABLE_COUNTER);
        String createTable = PULSE_CREATE_TABLE_TEMAPLATE.replace("{0}", ("trade.sellorders_"+tableNum));
        createTable = createTable.replace("{1}", ""+tableNum);
        Log.getLogWriter().info("Creating table <" + createTable + ">");
        conn.createStatement().executeUpdate(createTable);        
      }
      closeGFEConnection(conn);
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
      throw new TestException("createDropTable Failed", se);
    }    
  }

  @SuppressWarnings("unused")
  private void updateBBOnCloseConnection() {
    boolean isManagerRunning = getCounter(MANAGER_RUNNING) > 0;
    if(isManagerRunning) {
      SharedLock lock = SQLBB.getBB().getSharedLock();
      try {
        lock.lock();
        NetworkServerConnectionStats connStat = (NetworkServerConnectionStats)SQLBB.getBB().getSharedMap().get(COUNTER_NS_PEER);
        connStat.updateNetworkServerConnectionStats(
            connStat.getConnectionsOpened(),
            connStat.getConnectionsClosed() + 1,
            connStat.getConnectionsAttempted(),
            connStat.getConnectionsFailed(),
            connStat.getConnectionLifeTime(),
            connStat.getConnectionsOpen(),
            connStat.getConnectionsIdle() - 1);
        SQLBB.getBB().getSharedMap().put(COUNTER_NS_PEER, connStat);
        Log.getLogWriter().info("Shared Map Updated after connection close for " + COUNTER_NS_PEER + ", value : " + connStat.toString());
      } finally {
        lock.unlock();
      }
    }
  }
  
  private List<ClientVmInfo> getVmsExcludeShuttingDown(){
    List<ClientVmInfo> vmList = StopStartVMs.getAllVMs();
    String vm_shutting_down = (String) SQLBB.getBB().getSharedMap().getMap().get("VM_SHUTTING_DOWN");
    ClientVmInfo toRemoveVM = null;
    if (vm_shutting_down != null || "NULL".equals(vm_shutting_down)) {
      for (ClientVmInfo vm : vmList) {
        if (vm.getClientName().equals(vm_shutting_down)) {
          toRemoveVM = vm;
        }
      }
    }

    if (toRemoveVM != null) {
      boolean removed = vmList.remove(toRemoveVM);
      Log.getLogWriter().info(
          "Removing vminfoRecord for vm " + toRemoveVM.getClientName() + "VID=" + toRemoveVM.getVmid()
              + " as it is shutting down : " + removed);
    }
    return vmList;
  }

	private void createDropTable() {
		Connection conn = getGFEConnection();
		//printOpenConnection("    
		Log.getLogWriter().info("Incrementing counter for " + TABLE_COUNTER);
		int tableNum = incrementCounter(TABLE_COUNTER);
		Log.getLogWriter().info("Next Table Number is " + tableNum);
		boolean managerReadLocked = false;
		try {
      /*- tushark : TODO For Viren : template 2 is unstable create table fails most of the time
		  String table = HydraUtil.getRandomElement(new String[] { CREATE_TABLE_TEMPLATE, CREATE_TABLE_TEMPLATE_2 }); */
		  String table = CREATE_TABLE_TEMPLATE; 
		  
      /*-
      checkAndCreateDiskStore();
      if(CREATE_TABLE_TEMPLATE_2.equals(table)) {
        checkAndCreateRefTable();
      }*/
		  
			String createTable = table.replace("{0}", "" + tableNum);
			String serverGroupString = "";
			List<String> serverGroupList = new ArrayList<String>();
			boolean onServerGroups = HydraUtil.getRandomBoolean();
			String tableName = TABLE_NAME_TEMPLATE.replace("{0}", "" + tableNum);
			Log.getLogWriter().info("Table Name to create : " + tableName);
			
			//Viren: TODO: Need to check how can provide server group for thin client test
			if(!isEdge) {
  			if (onServerGroups) {
  			  
  			  //TODO: Viren add logic to add more server group and machine combintation
  				String sg = HydraUtil.getRandomElement(getServerGroupsArray());
  				Log.getLogWriter().info("Server group for table : " + sg);
  				// make sure sg!=default
  			
  				while (DEFAULT_SERVER_GROUP.equals(sg))
  					sg = HydraUtil.getRandomElement(getServerGroupsArray());
  				
  				serverGroupList.add(sg);
  				//TODO: Viren :- test if we can add multiple server groups
  				serverGroupString = " SERVER GROUPS (" + sg + ")";
  
  				createTable += serverGroupString;
  				Log.getLogWriter().info("sql for create table so far : " + createTable);	
  				saveTableToServerGroupMapping(tableName, Collections.singletonList(sg));
  			} else {
  				serverGroupList.add(DEFAULT_SERVER_GROUP);
  			}
			}
			/*defination
			 *  
			 */
			//((SystemManagementService)ManagementService.getExistingManagementService(null)).addProxyListener(listener);
			createTable += appendPersistentToTableDefinition();
			Log.getLogWriter().info("sql for create table so far : " + createTable);
			createTable += appendPartitionToTableDefinition();
			//TODO:Currently with eviction, it is setting eviction percent, we need ot tackle to change the evition percent BB after run
			// need to understand complete behaviour
		  //Log.getLogWriter().info("sql for create table so far : " + createTable);
			//createTable += appendEvicitionToTableDefinition();
			/*- TODO : For Viren : appendColocated method is wrong. fails everytime
			Log.getLogWriter().info("sql for create table so far : " + createTable);
			createTable += appendColocateToTableDefinition(table); */
			Log.getLogWriter().info("sql for create table so far : " + createTable);
      //TODO: Please add all type of table creation combination..
			
			boolean tableCreated = false;
      try {
        Log.getLogWriter().info("Creating table <" + createTable + ">");
        conn.createStatement().executeUpdate(createTable);
        tableCreated = true;
        Log.getLogWriter().info("Created table <" + createTable + ">");
      } catch (SQLException e) {
        tableCreated = false;
      }

      if (tableCreated) {
        sleepForDataUpdaterJMX();        
        lockReadManager();
        List<ClientVmInfo> vmList = getVmsExcludeShuttingDown();        
        MBeanServerConnection mbeanServer = connectJMXConnector();        
        managerReadLocked = true;
        checkTableMBean(mbeanServer, true, vmList, tableName, serverGroupList, true, true);
        String dropTable = DROP_TABLE_TEMPLATE.replace("{0}", "" + tableNum);
        Log.getLogWriter().info("Dropping table <" + dropTable + ">");
        conn.createStatement().executeUpdate(dropTable);
        Log.getLogWriter().info("Dropped table <" + dropTable + ">");
        sleepForDataUpdaterJMX();
        checkTableMBean(mbeanServer, false, vmList, tableName, serverGroupList, true, true);
      } else {
        Log.getLogWriter().info("SQLException while creating table skipping this iteration #CrDpTableFailed " + tableName);
      }
		} catch (SQLException se) {
			SQLHelper.handleSQLException(se);
			throw new TestException("createDropTable\n" + TestHelper.getStackTrace(se));
		} catch (MalformedObjectNameException e) {
			throw new TestException("createDropTable\n" + TestHelper.getStackTrace(e));
		} catch (NullPointerException e) {
			throw new TestException("createDropTable\n" + TestHelper.getStackTrace(e));
		} catch (IOException e) {
			throw new TestException("createDropTable\n" + TestHelper.getStackTrace(e));
		} catch (AttributeNotFoundException e) {
      throw new TestException("createDropTable\n" + TestHelper.getStackTrace(e));
    } catch (TestException e) {
      throw new TestException("createDropTable\n" + TestHelper.getStackTrace(e));
    } finally{
      if(managerReadLocked)
        unLockReadManager();
    }
		Log.getLogWriter().info("createDropTable : OK ");
	}

  private void sleepForDataUpdaterJMX() {
    HydraUtil.sleepForDataUpdaterJMX(15);    
  }

  private void checkAndCreateRefTable() {
    Log.getLogWriter().info("Trying to create ref country table...");
    String sql = "CREATE TABLE COUNTRIES"
        + "("
        + "COUNTRY VARCHAR(26) NOT NULL CONSTRAINT COUNTRIES_UNQ_NM Unique, "
        + "COUNTRY_ISO_CODE CHAR(2) NOT NULL CONSTRAINT COUNTRIES_PK PRIMARY KEY,"
        + "REGION VARCHAR(26), "
        + "CONSTRAINT COUNTRIES_UC CHECK (country_ISO_code = upper(country_ISO_code) )"
        + ") PARTITION BY PRIMARY KEY";
    Connection conn = getGFEConnection();
    try {
      conn.createStatement().executeUpdate(sql);
    } catch (SQLException e) {
      if(!e.getMessage().contains("already exists")) {
        Log.getLogWriter().error("Error occurred while creating COUNTRIES table for ref, it may be already exist, Error : "+  e.getMessage());
      }
    } finally {
      closeGFEConnection(conn);
    }
  }

  private void checkAndCreateDiskStore() {
    Log.getLogWriter().info("Trying to create disk store...");
    String sql = "CREATE DISKSTORE OVERFLOWDISKSTORE";
    Connection conn = getGFEConnection();
    try {
      conn.createStatement().executeUpdate(sql);
    } catch (SQLException e) {
      if(!e.getMessage().contains("already exists")) {
        Log.getLogWriter().error("Error occurred while creating diskstore for ref, it may be already exist, Error : "+  e.getMessage());
      }
    } finally {
      closeGFEConnection(conn);
    }
  }
  
  private String appendColocateToTableDefinition(String table) {
    String colocated  = "";
    if(table.equals(CREATE_TABLE_TEMPLATE_2)) {
      colocated = HydraUtil.getRandomElement(new String[] { " COLOCATE WITH (COUNTRIES)", " " });
    }
    return colocated;
  }

  private String appendEvicitionToTableDefinition() {
    String eviction = HydraUtil
        .getRandomElement(new String[] {
            " EVICTION BY CRITERIA (EID < 300000) EVICTION FREQUENCY 180 SECONDS ",
            " EVICTION BY LRUMEMSIZE 1000 EVICTACTION DESTROY",
            " EVICTION BY LRUCOUNT 2 EVICTACTION OVERFLOW 'OverflowDiskStore'",
            " " });
    return eviction;

  }

  private String appendPartitionToTableDefinition() {
    String partition = HydraUtil.getRandomElement(new String[] {
        "  PARTITION BY PRIMARY KEY REDUNDANCY 1 ",
        "  PARTITION BY COLUMN (SINCE) ",
        "  PARTITION BY RANGE (SINCE) "
            + "( VALUES BETWEEN '2010-01-01' AND '2010-04-01', "
            + "  VALUES BETWEEN '2010-04-01' AND '2010-07-01', "
            + "VALUES BETWEEN '2010-07-01' AND '2010-10-01',"
            + "VALUES BETWEEN '2010-10-01' AND '2011-01-01'" + ")",
        " PARTITION BY LIST (SSN)" + " ( VALUES ('PDX', 'LAX'),"
            + " VALUES ('AMS', 'DUB')," + " VALUES ('DTW', 'ORL'))",
        "  PARTITION BY (MONTH(SINCE))" });
    return partition;
  }

  private String appendPersistentToTableDefinition() {
    String persistent = HydraUtil
        .getRandomElement(new String[] { "  PERSISTENT ASYNCHRONOUS",
            "  PERSISTENT SYNCHRONOUS", "  PERSISTENT" });
    return persistent;

  }

  private void saveTableToServerGroupMapping(String tableName, List<String> sg) {
    Log.getLogWriter().info("Saving Counter for Server Group : tableName : " + tableName + " and ServerGroup : " + sg);
    SharedLock lock = SQLBB.getBB().getSharedLock();
    try {
      lock.lock();
      @SuppressWarnings("unchecked")
      Map<String, List<String>> map = (Map<String, List<String>>) SQLBB.getBB().getSharedMap().get(TABLE_SERVER_GROUP);
      map.put(tableName, sg);
      Log.getLogWriter().info("Map to save for Table Server group : " + map);      
      SQLBB.getBB().getSharedMap().put(TABLE_SERVER_GROUP, map);
    } finally {
      lock.unlock();
    }
    
  }

  private void saveTableAttribute(String tableName, Map<String, Object> tableAttrMap) {
    SharedLock lock = SQLBB.getBB().getSharedLock();
    try {
      lock.lock();
      SQLBB.getBB().getSharedMap().put(tableName, tableAttrMap);
    } finally {
      lock.unlock();
    }
  }
  
  @SuppressWarnings("unchecked")
  private Map<String, Object> getTableAttribute(String tableName) {
    SharedLock lock = SQLBB.getBB().getSharedLock();
    try {
      lock.lock();
      return (Map<String, Object>)SQLBB.getBB().getSharedMap().get(tableName);
    } finally {
      lock.unlock();
    }
  }
  
  @SuppressWarnings({ "unused", "unchecked" })
  private Map<String, String> getAllIndex() {
    return (Map<String, String>)SQLBB.getBB().getSharedMap().get(INDEX_MAP);
  }

  private List<String> getAllIndex(String tableName) {
    List<String> indexForTable = new ArrayList<String>();
    @SuppressWarnings("unchecked")
    Map<String, String> indexMap = (Map<String, String>) SQLBB.getBB().getSharedMap().get(INDEX_MAP);
    if (indexMap != null) {
      for (Entry<String, String> entry : indexMap.entrySet()) {
        if (tableName.equalsIgnoreCase(entry.getValue())) {
          indexForTable.add(entry.getKey());
        }
      }
    }
    return indexForTable;
  }

  protected void createGFEDB2() {
    Properties info = getGemFireProperties();
    info.setProperty("host-data", "true");
    Log.getLogWriter().info("Connecting with properties: " + info);
    String groups = getServerGroups();
    if(groups != null) {
      info.setProperty("server-groups", groups);
    }
    startGFXDDB(info);
  }

  private void validateMemberView() {
    ClientVmInfo vmInfo = new ClientVmInfo(RemoteTestModule.getMyVmid(), RemoteTestModule.getMyClientName(),
        RemoteTestModule.getMyLogicalHost());
    List<ClientVmInfo> vmInfoList = new ArrayList<ClientVmInfo>();
    vmInfoList.add(vmInfo);
    checkMemberViewFromManager(true, vmInfoList);
    checkAggrTableViewFromManager(true, vmInfoList);
    Log.getLogWriter().info("validateMemberView : OK");
  }

  private void populateDataToBB() {
    HydraUtil.sleepForReplicationJMX();    
    List<ClientVmInfo> vmInfoList = new ArrayList<ClientVmInfo>();
    ClientVmInfo vmInfo = new ClientVmInfo(RemoteTestModule.getMyVmid(),
        RemoteTestModule.getMyClientName(), RemoteTestModule.getMyLogicalHost());
    vmInfoList.add(vmInfo);
    List<String> defaultGrp = new ArrayList<String>();
    defaultGrp.add(DEFAULT_SERVER_GROUP);
    
    MBeanServerConnection mbeanServer = null;
    
    try {
      lockReadManager();
      mbeanServer = connectJMXConnector();

      for (String table : tableList) {
        try {
          populateTableMBean(mbeanServer, true, vmInfoList, table, defaultGrp);
        } catch (MalformedObjectNameException e) {
          throw new TestException("Error while populating tableMBeans into BB\n" + TestHelper.getStackTrace(e));
        } catch (NullPointerException e) {
          throw new TestException("Error while populating tableMBeans into BB\n" + TestHelper.getStackTrace(e));
        } catch (IOException e) {
          throw new TestException("Error while populating tableMBeans into BB\n" + TestHelper.getStackTrace(e));
        } catch (AttributeNotFoundException e) {
          throw new TestException("Error while populating tableMBeans into BB\n" + TestHelper.getStackTrace(e));
        } catch (TestException e) {
          throw new TestException("Error while populating tableMBeans into BB\n" + TestHelper.getStackTrace(e));
        }
      }
    } finally {
      closeJMXConnector();
      unLockReadManager();      
    }
  }

  private void verifyTableMBeanValues() {
    SharedLock lock = MBeanCloseTaskBB.getBB().getSharedLock();
    try {
      HydraUtil.sleepForReplicationJMX();
      useLocks = false;
      lockReadManager();
      
      MBeanServerConnection mbeanServer = connectJMXConnector();
      lock.lock();
      checkTableMBeans(mbeanServer, true);
      checkMemberMBeans(mbeanServer, true);
    } finally {
      lock.unlock();
      closeJMXConnector();
      unLockReadManager();
    }

  }

  private void verifyAggregateValues() {
    SharedLock lock = MBeanCloseTaskBB.getBB().getSharedLock();
    try {
      HydraUtil.sleepForReplicationJMX();
      useLocks = false;
      lockReadManager();
      MBeanServerConnection mbeanServer = connectJMXConnector();
      checkProcCalls(mbeanServer);
      checkDSCounters(mbeanServer);
      lock.lock();
      checkTableMBeans(mbeanServer, true, true);
      checkMemberMBeans(mbeanServer, true, true);
    } finally {
      lock.unlock();
      closeJMXConnector();
      unLockReadManager();
    }

  }
  
  private void restartManager() {
    Log.getLogWriter().info("In restartManager");
    @SuppressWarnings("unchecked")
    List<ClientVmInfo> vmList = StopStartVMs.getAllVMs();
    Cache cache = Misc.getGemFireCache();
    ManagementService service = ManagementService.getExistingManagementService(cache);
    saveCounter(MANAGER_RUNNING,0);
    lockWriteManager();
    Log.getLogWriter().info("Waiting for all validation operation to finish ....");
    HydraUtil.sleepForReplicationJMX();
    HydraUtil.sleepForReplicationJMX();
    Log.getLogWriter().info("Stopping manager ....");
    service.stopManager();
    Log.getLogWriter().info("Starting manager ....");
    service.startManager();
    Log.getLogWriter().info("Stared manager waiting for all mbeans to register....");
    unLockWriteManager();
    incrementCounter(MANAGER_RUNNING);
    HydraUtil.sleepForReplicationJMX();
    HydraUtil.sleepForReplicationJMX();
    Log.getLogWriter().info("Stared manager completed....");
    checkMemberViewFromManager(true,vmList);
    checkAggrTableViewFromManager(true, vmList);
    Log.getLogWriter().info("restartManager completed successfully waiting for pending validations to complete....");  
    HydraUtil.sleepForReplicationJMX();
    HydraUtil.sleepForReplicationJMX();
    Log.getLogWriter().info("restartManager completed successfully : OK....");
  }

  private void restartManagerVM() {
    Log.getLogWriter().info("In restartManagerVM");
    @SuppressWarnings("unchecked")
    List<ClientVmInfo> vmList = StopStartVMs.getAllVMs();
    List<ClientVmInfo> selectedVmList = new ArrayList<ClientVmInfo>();
    List<String> stopModeList = new ArrayList<String>();
    String clientId = RemoteTestModule.getMyClientName();

    for (ClientVmInfo cInfo : vmList) {
      Log.getLogWriter().info("Considering Vm " + cInfo.getClientName());
      if(cInfo.getClientName().contains("manager") && !cInfo.getClientName().contains(clientId)
          && selectedVmList.size()<1){
        selectedVmList.add(cInfo);
        SQLBB.getBB().getSharedMap().put("VM_SHUTTING_DOWN", cInfo.getClientName());
        stopModeList.add(TestConfig.tab().stringAt(StopStartPrms.stopModes));
      }
    }
    
    lockWriteManager();
    saveCounter(MANAGER_RUNNING,0);
    incrementCounter("VM_SHUTDOWN");
    logInfo("Shutting down client : " + selectedVmList);
    StopStartVMs.stopVMs(selectedVmList, stopModeList);    
    unLockWriteManager();

    logInfo(selectedVmList + " clients were Shutdown");
    HydraUtil.sleepForReplicationJMX();

    StopStartVMs.startVMs(selectedVmList);
    SQLBB.getBB().getSharedMap().put("VM_SHUTTING_DOWN", "NULL");
    logInfo("Restarted VMS : " + HydraUtil.ObjectToString(selectedVmList));

    //set connector used to connect to manager null to force new connector
    connectorTL.set(null);
    HydraUtil.sleepForReplicationJMX();
    checkMemberViewFromManager(true, vmList);
    checkAggrTableViewFromManager(true, vmList);
    saveCounter("VM_SHUTDOWN",0);
    Log.getLogWriter().info("In doHA : restartManagerVM");
  }

  private void prepareTest() {
    
    this.useLocks = MBeanPrms.useManagerLocks();
    Log.getLogWriter().info("Managerlock use : " + useLocks);
    
    if (MBeanPrms.isWANTest()) {
      // TODO : Dirty-way of getting to know WAN site, find some good
      // altertnative way
      String clientName = RemoteTestModule.getMyClientName();
      String[] array = clientName.split("_");
      String dsName = "ds_" + array[1];
      Log.getLogWriter().info("Client " + clientName + ", WanSite " + array[1] + ", dsName : " + dsName);
      SQLBB.getBB().getSharedMap().put("CLIENT_DS_" + RemoteTestModule.getMyClientName(), dsName);
    }
    
    Log.getLogWriter().info("Putting member details on bb for " + THIS_MEMBERID + " is " + ManagementUtil.getMemberID());
    SQLBB.getBB().getSharedMap().put(THIS_MEMBERID, ManagementUtil.getMemberID());
    saveMemberMapping();
    
    Log.getLogWriter().info("Member isStore : "  + GemFireStore.getBootedInstance().getMyVMKind().isStore());
    Log.getLogWriter().info("Member isAccessor : "  + GemFireStore.getBootedInstance().getMyVMKind().isAccessor());
    Log.getLogWriter().info("Member isAccesorOrStore : "  + GemFireStore.getBootedInstance().getMyVMKind().isAccessorOrStore());
    Log.getLogWriter().info("Member isLocator : "  + GemFireStore.getBootedInstance().getMyVMKind().isLocator());

    Log.getLogWriter().info("Member isAdmin : "  + GemFireStore.getBootedInstance().getMyVMKind().isAdmin());
    Log.getLogWriter().info("Member isAgent : "  + GemFireStore.getBootedInstance().getMyVMKind().isAgent());
    
    Log.getLogWriter().info("isEdge : " + isEdge);
    if(isEdge && GemFireStore.getBootedInstance().getMyVMKind().isLocator()) {
      return;
    }
    Connection conn = getGFEConnection();
  //  printOpenConnection("prepareTest");
    ResultSet rs = null;
    int max = 0;
    try {
      rs = conn.createStatement().executeQuery("select max(oid) from trade.sellorders");
      while (rs.next()) {
        max = rs.getInt(1);
      }
      rs.close();
      Log.getLogWriter().info("New Order Id is " + max);
      saveCounter("SELLORDER_OID", max);
    } catch (SQLException se) {
      SQLHelper.handleSQLException(se);
      throw new TestException("Failed", se);
    }
    
        //Find out total inserts done on sellorder table so far
//        int rowCount = 0;
//        rs = conn.createStatement().executeQuery("select count(*) from trade.sellorders");
//        while (rs.next()) {
//          rowCount = rs.getInt(1);
//        }
//        rs.close();
//        //saveCounter(COUNTER_SELLORDER_INSERTS, rowCount);
//        saveCounter(COUNTER_SELLORDER_INSERTS, getInsertsFromTableMBean());
        //TODO:Viren, getExecutionCount not working correctly, so disabling it.
        //Other way to do it where you first populate all data from mbean itself.
        //saveCounter(COUNTER_SELLORDER_INSERTS, getExecutionCount(SELLORDER_INSERT));
        


//        if(!MBeanPrms.isWANTest())
//          doProcedureCall();

  }

  private void saveMemberMapping() {
    SharedLock lock = SQLBB.getBB().getSharedLock();
    try {
      lock.lock();
      @SuppressWarnings("unchecked")
      Map<String, String> membersMap = (Map<String, String>) SQLBB.getBB().getSharedMap().get(MEMBERS_MAPPING);
      if(membersMap == null) {
        membersMap = new HashMap<String, String>();
      }
      membersMap.put(ManagementUtil.getMemberID(), RemoteTestModule.getMyClientName());
      SQLBB.getBB().getSharedMap().put(MEMBERS_MAPPING, membersMap);
    } finally {
      lock.unlock();
    }
  }


  private void startManager() {
    Log.getLogWriter().info("Starting the fabric server jmx manager");
    Properties info = getGemFireProperties();
    //info.setProperty("host-data", "true");
    info.setProperty("gemfire.jmx-manager", "true");
    info.setProperty("gemfire.jmx-manager-start", "true");
    int port = PortHelper.getRandomPort();
    info.setProperty("gemfire.jmx-manager-port", "" + port);
    String groups = getServerGroups();
    if(groups != null) {
      info.setProperty("server-groups", groups);
    }
    Log.getLogWriter().info("Connecting with properties: " + info);
    startGFXDDB(info);
    SQLBB.getBB().getSharedMap().put("JMX_PORT", port);
    SQLBB.getBB().getSharedMap().put("QUERY_NODE", ManagementUtil.getMemberID());
    Log.getLogWriter().info("Started the fabric server jmx manager");
    incrementCounter(MANAGER_RUNNING);
  }

  private void execStmt() {
    ResultSet rs = null;
    PreparedStatement statement = null;
    try {
      Connection conn = getGFEConnection();
    //  printOpenConnection("execStmt");
      String stmt = MBeanPrms.getStatements();
      statement = (PreparedStatement) conn.prepareStatement(stmt);
      Log.getLogWriter().info("Executing statment : " + stmt);
      int tid = RemoteTestModule.getCurrentThread().getThreadId();
      statement.setInt(1, tid);
      statement.execute();
      rs = statement.getResultSet();
     
      incrementCounter("STMT_" + stmt);
      Log.getLogWriter().info("Executed count  : " + getCounter("STMT_" + stmt));
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException("Not able to execute statement\n" + TestHelper.getStackTrace(e));
    } finally {
      closeRS(rs);
      closeStatement(statement);
    }
  }
/*
  private void printConnection(String methodName, String counterName) {
    if(isEdge) {
      return;
    }
    boolean isManagerRunning = getCounter(MANAGER_RUNNING) > 0;
      if(isManagerRunning) {
      lockReadManager();
      try {
        //HydraUtil.sleepForReplicationJMX();
        MBeanServerConnection mbeanServer = connectJMXConnector();
        ObjectName name = new ObjectName(MessageFormat.format(ManagementConstants.OBJECTNAME__GFXDMEMBER_MXBEAN,
            new Object[] { "CG", ManagementUtil.getMemberID() }));
        Log.getLogWriter().info("*** [" + counterName + "] Printing Connection Stat  in " + methodName + "***");
        NetworkServerConnectionStats connStat = (NetworkServerConnectionStats) SQLBB.getBB().getSharedMap()
            .get(COUNTER_NS_PEER);
        Log.getLogWriter().info(
            "[" + counterName + "] Conn Stat in Blackboard in " + methodName + "" + connStat.toString());
        CompositeDataSupport peerConnStats = (CompositeDataSupport) mbeanServer.getAttribute(name,
            "NetworkServerPeerConnectionStats");
        Log.getLogWriter().info(
            "[" + counterName + "] Conn Stat in JMX in " + methodName + "" + peerConnStats.toString());

      } catch(Exception e) {
        Log.getLogWriter().error(e);
        Log.getLogWriter().info(
            "*** [" + counterName + "] Error while printing connection stat  in " + methodName + "***");
      } finally {
        unLockReadManager();
      }
    }
  }
  private void printOpenConnection(String methodName) {
   printConnection(methodName, "CONN_STAT_OPEN");
  }
  
  private void printCloseConnection(String methodName) {
    printConnection(methodName, "CONN_STAT_CLOSE");
   }

*/

  private void closeStatement(PreparedStatement statement) {
    if(statement == null) {
      return;
    }
    try {
      statement.close();
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  private void closeRS(ResultSet rs) {
    if(rs == null) {
      return;
    }

    try {
      rs.close();
    } catch (SQLException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }



  private void pulseCounter() {
    try {
      transaction("trade.sellorders");
    } catch (SQLTransactionRollbackException e) {
      if (MBeanPrms.isHATest() && getCounter("VM_SHUTDOWN")>0) {
        Log.getLogWriter().info("SQLTransactionRollbackException possibly due to concurrent vm shutdown");
        return;
      } else {
        SQLHelper.printSQLException(e);
        throw new TestException("Not able to execute transaction\n" + TestHelper.getStackTrace(e));
      }
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException("Not able to execute transaction\n" + TestHelper.getStackTrace(e));
    }


    try {
      // Sleep for statistics sampling and federation of metrics
      HydraUtil.sleepForReplicationJMX();
      lockReadManager();
      MBeanServerConnection mbeanServer = connectJMXConnector();

      if (isEdge) {
        Log.getLogWriter().info("member is edge, so no need to verify mbean here");
        return;
      }
      if (mbeanServer == null && isThisManagerVM()) {
        Log.getLogWriter().info("Skipping verification as manager is restarting ...");
        return;
      }
      if (mbeanServer == null) {
        throw new TestException("mbeanServer is null");
      }

      checkMemberCounters(mbeanServer);
      checkRegionCounter(mbeanServer);
      checkGfxdMBeans(mbeanServer, false);
    } finally {
      closeJMXConnector();
      unLockReadManager();      
    }
    
  }
  
  private void pulseStabilityTest(){
    try {
      int tablesCreated = getCounter(TABLE_COUNTER);
      for(int i=0;i<5;i++){
        int randomIndex = HydraUtil.getnextNonZeroRandomInt(tablesCreated);
        String tableName = "trade.sellorders_" + randomIndex;
        transaction(tableName);
      }      
    } catch (SQLTransactionRollbackException e) {
      if (MBeanPrms.isHATest() && getCounter("VM_SHUTDOWN")>0) {
        Log.getLogWriter().info("SQLTransactionRollbackException possibly due to concurrent vm shutdown");
        return;
      } else {
        SQLHelper.printSQLException(e);
        throw new TestException("Not able to execute transaction\n" + TestHelper.getStackTrace(e));
      }
    } catch (SQLException e) {
      SQLHelper.printSQLException(e);
      throw new TestException("Not able to execute transaction\n" + TestHelper.getStackTrace(e));
    }
  }

  @SuppressWarnings("unchecked")
  private void createDropIndexTest() {
    Connection conn = getGFEConnection();
 //   printOpenConnection("createDropIndexTest");
    try {
      conn.setAutoCommit(false);
      conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
    } catch (SQLException e) {
      throw new TestException("Error while getting connection \n" + TestHelper.getStackTrace(e));
    }
    setTableCols();
    sql.ddlStatements.IndexDDLStmt indexStmt = new sql.ddlStatements.IndexDDLStmt();
    indexStmt.addListener(new CallBackListener() {
      @Override
      public String getName() {
        return CONN_COMMIT_TAG;
      }

      @Override
      public void execute(Object... params) {
        incrementCounter(COUNTER_SELLORDER_COMMITS);
      }
    });
    //TODO Viren :- Index lock
    SharedLock sharedLock = MBeanIndexBB.getBB().getSharedLock();
    try {
      sharedLock.lock();
    indexStmt.doDDLOp(null, conn);
    } finally {
      // TODO Viren :- Index unlock
      sharedLock.unlock();
    }
    Log.getLogWriter().info("Index Map : " + (Map<String, String>) SQLBB.getBB().getSharedMap().get(INDEX_MAP));
  }

  private void alterTableTest() {
    Connection conn = getGFEConnection();
  //  printOpenConnection("alterTableTest");
    String op = HydraUtil.getRandomElement(new String[] { "add", "delete" });
    try {
      conn.setAutoCommit(false);
      conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);

      if ("delete".equals(op)) {
        ResultSet rs = conn
            .createStatement()
            .executeQuery(
                "select tablename "
                    + "from sys.systables where tabletype = 'T' and tableschemaname='TRADE'");
        while (rs.next()) {
          String tableName = rs.getString(1);
          if (!tableName.equalsIgnoreCase("txhistory")
              && !tableName.equalsIgnoreCase("TRADES")) {
            dropColumn(conn, tableName);
          }
        }
      }
      if ("add".equals(op)) {
        ResultSet rs = conn
            .createStatement()
            .executeQuery(
                "select tablename "
                    + "from sys.systables where tabletype = 'T' and tableschemaname='TRADE'");
        while (rs.next()) {
          String tableName = rs.getString(1);
          List<String> columnDef = getDropColumnsFromSharedMap(tableName);
          if (columnDef != null && columnDef.size() > 0) {
            String addColumn = "alter table trade." + tableName + " add column " + columnDef.get(0);
            columnDef.remove(0);
            try {
              conn.createStatement().execute(addColumn);
            } catch (SQLException se) {
              SQLHelper.handleSQLException(se);
            }
            Log.getLogWriter().info("added the column " + columnDef + " to the table " + tableName);
          } 
        }
      }
    } catch (SQLException e) {
      throw new TestException("Error while getting connection \n" + TestHelper.getStackTrace(e));
    }
  }
  
  private void waitForMBeanUpdater() {
    Log.getLogWriter().info("Sleeping for 1.5 minutes so that table mbean updates all memAnalytics data");
    try {
      Thread.sleep(1*90*1000);
    } catch (InterruptedException e) {      
    }
    Log.getLogWriter().info("Sleeping done");    
  }
  
  private void dropColumn(Connection conn, String tableName) {
    try {
      DatabaseMetaData meta = conn.getMetaData();
      ResultSet columns = meta.getColumns(null, "TRADE", tableName, null);
      List<Struct>columnList = ResultSetHelper.asList(columns, false);
      log().info("columns are " + ResultSetHelper.listToString(columnList));
      String columnType = null;
      String columnName = null;
      while (true) {
        Struct aColumn = columnList.get(random.nextInt(columnList.size()));
        columnType = (String) aColumn.get("TYPE_NAME");
        columnName = (String) aColumn.get("COLUMN_NAME");
        if (!columnName.equals("TID")) break;
      }
      String dropColumn = "alter table trade." + tableName + " drop"
          + (random.nextBoolean() ? " column " : " ") + columnName
          + " RESTRICT ";
      Log.getLogWriter().info(
          "in gfxd dropping the column " + columnName + " in table " + tableName);
      conn.createStatement().execute(dropColumn);
      List<String> droppedColumn = getDropColumnsFromSharedMap(tableName);
      if(droppedColumn == null) {
          droppedColumn = new ArrayList<String>();
          SQLBB.getBB().getSharedMap().put("dropColumn"+tableName, droppedColumn);
      }
      droppedColumn.add(columnName+ " " + columnType);
    } catch (SQLException e) {
      Log.getLogWriter().warning("Error while droppin column.. ignoring it...");
    } 
  }

  @SuppressWarnings("unchecked")
  private List<String> getDropColumnsFromSharedMap(String tableName) {
    return (List<String>) SQLBB.getBB().getSharedMap().get("dropColumn" + tableName);
  }


  private void transaction(String tableName) throws SQLException {
    Connection conn = getGFEConnection();
 //   printOpenConnection("transaction");
    conn.setAutoCommit(false);
    conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
    String op = HydraUtil.getRandomElement(new String[] { "insert", "delete", "update", "put", "select" });
    //String op = "insert";
    Log.getLogWriter().info("Trying to execute " + op + " on " + tableName);
    try {
      if ("insert".equals(op)) {
        doInsert(conn, tableName);
      } else if ("update".equals(op)) {
        doUpdate(conn, tableName);
      } else if ("delete".equals(op)) {
        doDelete(conn, tableName);
      } else if ("select".equals(op)) {
        doSelect(conn, tableName);
      } else if ("put".equals(op)) {
        doPut(conn, tableName);
      }

      if (HydraUtil.getRandomBoolean()) {
        conn.commit();
        incrementCounter(COUNTER_SELLORDER_COMMITS);
        Log.getLogWriter().info("Executed COMMITS  : " + getCounter(COUNTER_SELLORDER_COMMITS));
      } else {
        conn.rollback();
        incrementCounter(COUNTER_SELLORDER_ROLLBACKS);
        Log.getLogWriter().info("Executed ROLLBACKS  : " + getCounter(COUNTER_SELLORDER_ROLLBACKS));
      }
    } catch (SQLException e) {
     Log.getLogWriter().error("Error occurred while executing Op : " + op + ", This will no impact Test. Error : " + e.getMessage());
    }
  }


  private void callSQLProcs() {
    try {
      doProcedureCall();
    } catch (SQLException e) {
      if (MBeanPrms.isHATest() && getCounter("VM_SHUTDOWN") > 0) {
        Log.getLogWriter().info("SQLExceptoin possibly due to concurrent shutdown");
        return;
      } else {
        Log.getLogWriter().info("SQLExceptoin possibly due to concurrent shutdown VM Count : " + getCounter("VM_SHUTDOWN")
            + " isHATest : " + MBeanPrms.isHATest());
        SQLHelper.printSQLException(e);
        throw new TestException("Not able to execute procedure\n" + TestHelper.getStackTrace(e));
      }
    }
    try {
      HydraUtil.sleepForReplicationJMX();
      lockReadManager();
      MBeanServerConnection mbeanServer = connectJMXConnector();
      if (isEdge) {
        Log.getLogWriter().info("member is edge, so no need to verify mbean here");
        return;
      }
      if (mbeanServer == null && isThisManagerVM()) {
        Log.getLogWriter().info("Skipping verification as manager is restarting ...");
        return;
      }
      if (mbeanServer == null) {
        throw new TestException("mbeanServer is null");
      }
      checkProcCalls(mbeanServer);
    } finally {
      closeJMXConnector();
      unLockReadManager();      
    }
  }

  private void doHA() {

    Log.getLogWriter().info("In doHA");
    @SuppressWarnings("unchecked")
    List<ClientVmInfo> vmList = StopStartVMs.getAllVMs();
    List<ClientVmInfo> selectedVmList = new ArrayList<ClientVmInfo>();
    List<String> stopModeList = new ArrayList<String>();
    String clientId = RemoteTestModule.getMyClientName();

    for (ClientVmInfo cInfo : vmList) {
      Log.getLogWriter().info("Considering Vm " + cInfo.getClientName());
      if(!cInfo.getClientName().contains("manager") && !cInfo.getClientName().contains(clientId)
          && selectedVmList.size()<1){
        selectedVmList.add(cInfo);
        SQLBB.getBB().getSharedMap().put("VM_SHUTTING_DOWN", cInfo.getClientName());
        stopModeList.add(TestConfig.tab().stringAt(StopStartPrms.stopModes));
      }
    }    
   
    //use locks to prevent concurrent table drop affecting gemfirexd booting
   //boolean managerLocked=false;
    try {
      //lockWriteManager();
      //managerLocked = true;
      logInfo("Shutting down client : " + selectedVmList);
      StopStartVMs.stopVMs(selectedVmList, stopModeList);
      incrementCounter("VM_SHUTDOWN");
      logInfo(selectedVmList + " clients were Shutdown");
    } finally {
      //if(managerLocked)
      //  unLockWriteManager();
    }
    
    HydraUtil.sleepForReplicationJMX();
    //managerLocked=false;
    checkMemberViewFromManager(false, selectedVmList);
    try{      
      //lockWriteManager();
      //managerLocked=true;
      StopStartVMs.startVMs(selectedVmList);
      SQLBB.getBB().getSharedMap().put("VM_SHUTTING_DOWN", "NULL");
      logInfo("Restarted VMS : " + HydraUtil.ObjectToString(selectedVmList));
    }finally {
      //if(managerLocked)
      //  unLockWriteManager();
    }

    HydraUtil.sleepForReplicationJMX();
    
    checkMemberViewFromManager(true, selectedVmList);
    saveCounter("VM_SHUTDOWN",0);
    Log.getLogWriter().info("In doHA : OK");

  }
  
  private void checkAggrTableViewFromManager(boolean expectedBeans, List<ClientVmInfo> selectedVmList){
    lockReadManager();
    MBeanServerConnection mbeanServer = connectJMXConnector();  

    try {
      for (int i = 0; i < regionList.length; i++) {
        String aggrTableONStr = aggrTableONTemplate.replace("{0}", tableList[i]);
        ObjectName aggrTableON = new ObjectName(aggrTableONStr);
        Log.getLogWriter().info("Querying for aggrTableON instance " + aggrTableON);

        try {
          ObjectInstance instance = mbeanServer.getObjectInstance(aggrTableON);
          Log.getLogWriter().info("Found instance " + aggrTableON + " Expected " + expectedBeans);
          if (!expectedBeans) {
            if (instance != null)
              throw new TestException("Found aggreTable instance for " + tableList[i] + " when not expected ");
          }
        } catch (InstanceNotFoundException e) {
          if (expectedBeans) {
            logMBeans(
                MessageFormat.format(ManagementConstants.OBJECTNAME__AGGREGATETABLE_MXBEAN, new Object[] { "*" }),
                mbeanServer);
            throw new TestException("checkAggrTableViewFromManager\n" + TestHelper.getStackTrace(e));
          }
        }
      }
    } catch (IOException e) {
      throw new TestException("checkAggrTableViewFromManager\n" + TestHelper.getStackTrace(e));
    } catch (MalformedObjectNameException e) {
      throw new TestException("checkAggrTableViewFromManager\n" + TestHelper.getStackTrace(e));
    } catch (NullPointerException e) {
      throw new TestException("checkAggrTableViewFromManager\n" + TestHelper.getStackTrace(e));
    }finally{
      unLockReadManager();
    }
  }

  private void checkMemberViewFromManager(boolean expectedBeans, List<ClientVmInfo> selectedVmList) {
    try {
      lockReadManager();
      MBeanServerConnection mbeanServer = connectJMXConnector();
      for (ClientVmInfo info : selectedVmList) {
        try {

          // String memberId = (String)
          // SQLBB.getBB().getSharedMap().get(THIS_MEMBERID);
          String memberId = (String) SQLBB.getBB().getSharedMap().get("MEMBERID_" + info.getClientName());

          List<String> serverGroups = getServerGroupsArray(info.getClientName());

          for (String serverGroup : serverGroups) {
            checkMemberView(expectedBeans, mbeanServer, memberId, serverGroup);
          }

          // check regionMBean
          int i = 0;
          for (String region : regionList) {
            String regionONStr = regionONTemplate.replace("{0}", region);
            regionONStr = regionONStr.replace("{1}", memberId);
            ObjectName regionON = new ObjectName(regionONStr);
            Log.getLogWriter().info("Querying for region instance " + regionON);

            try {
              ObjectInstance instance = mbeanServer.getObjectInstance(regionON);
              Log.getLogWriter().info("Found instance " + regionON + " Expected " + expectedBeans);
              if (!expectedBeans && instance != null)
                throw new TestException("Found region table instance " + regionON + " when not expected ");
            } catch (InstanceNotFoundException e) {
              //unLockReadManager();
              if (expectedBeans) {
                logMBeans(
                    MessageFormat.format(ManagementConstants.OBJECTNAME__TABLE_MXBEAN, new Object[] { "*", "*", "*" }),
                    mbeanServer);
                throw new TestException("checkMemberViewFromManager\n" + TestHelper.getStackTrace(e));
              }
            }

            // Tables initially are created only on default server groups
            String serverGroup = DEFAULT_SERVER_GROUP;
            // for (String serverGroup : serverGroups) {
            Log.getLogWriter().info("Checking table mbean for server-group " + serverGroup);
            // check tableMBean
            String tableONStr = tableONTemplate.replace("{0}", serverGroup);
            tableONStr = tableONStr.replace("{1}", memberId);
            tableONStr = tableONStr.replace("{2}", tableList[i]);
            ObjectName tableON = new ObjectName(tableONStr);
            Log.getLogWriter().info("Querying for instance " + tableON);

            try {
              ObjectInstance instance = mbeanServer.getObjectInstance(tableON);
              Log.getLogWriter().info("Found instance " + tableON + " Expected " + expectedBeans);
              if (!expectedBeans && instance != null)
                throw new TestException("Found table instance " + tableON + " when not expected ");
            } catch (InstanceNotFoundException e) {
              //unLockReadManager();
              if (expectedBeans) {
                logMBeans(
                    MessageFormat.format(ManagementConstants.OBJECTNAME__TABLE_MXBEAN, new Object[] { "*", "*", "*" }),
                    mbeanServer);
                throw new TestException("checkMemberViewFromManager\n" + TestHelper.getStackTrace(e));
              }
            }
            i++;
          }

          // check statment mbean
          validateStatementStatistics(expectedBeans, mbeanServer, memberId);

          // check cluster mbean for members attribute
          String[] members = getGfxdClusterMembers(mbeanServer);
          Set<DistributedMember> apiMemberSet = getMembersSet();
          if (members.length != apiMemberSet.size()) {
            saveError("Member set on cluster mbean is wrong jmx " + HydraUtil.ObjectToString(members) + " API : "
                + HydraUtil.ObjectToString(apiMemberSet));
            throw new TestException("Member set on cluster mbean is wrong jmx " + HydraUtil.ObjectToString(members) + " API : "
              + HydraUtil.ObjectToString(apiMemberSet));
          }

        } catch (Exception e) {
          //unLockReadManager();
          throw new TestException("checkMemberViewFromManager\n" + TestHelper.getStackTrace(e));
        }
      }
    } finally {
      unLockReadManager();
    }
  }

  private void checkMemberView(boolean expectedBeans,
      MBeanServerConnection mbeanServer, String memberId, String serverGroup)
      throws MalformedObjectNameException, IOException, TestException {
    Log.getLogWriter().info("Checking GFXDMemberMBean for server group " + serverGroup + " and memberId : "  + memberId);
    String memberONstr = memberONTemplate.replace("{0}", serverGroup);
    memberONstr = memberONstr.replace("{1}", memberId);
    ObjectName memberON = new ObjectName(memberONstr);

    // check memberMBean
    Log.getLogWriter().info("Querying for member instance " + memberON);

    try {
      ObjectInstance instance = mbeanServer.getObjectInstance(memberON);
      Log.getLogWriter().info("Found instance " + memberON + " Expected " + expectedBeans);
      if (!expectedBeans && instance != null)
        throw new TestException("Found member instance " + memberON + " when not expected ");
    } catch (InstanceNotFoundException e) {
      if (expectedBeans) {
        logMBeans(MessageFormat.format(ManagementConstants.OBJECTNAME__GFXDMEMBER_MXBEAN, new Object[] {"*", "*"}), mbeanServer);
        throw new TestException("checkMemberViewFromManager\n" + TestHelper.getStackTrace(e));
      }
    }
  }

  private void validateStatementStatistics(boolean expectedBeans, MBeanServerConnection mbeanServer, String memberId)
      throws MalformedObjectNameException, IOException, TestException {
    String statementONStr = statementONTemplate.replace("{0}", memberId);
    statementONStr = statementONStr.replace("{1}", "*");
    ObjectName statementON = new ObjectName(statementONStr);
    Set<ObjectName> set = mbeanServer.queryNames(statementON, null);
    Log.getLogWriter().info("Statement MBeans found " + HydraUtil.ObjectToString(set));
    if (expectedBeans) {
      if (set.size() == 0)
        throw new TestException("Found 0 Statement Mbeans");
    } else {
      if (set.size() > 0)
        throw new TestException("Found " + set.size() + " Statement Mbeans when expecting zero mbeans");
    }
  }

  static void logMBeans(String pattern, MBeanServerConnection mbeanServer) {
    try {
      Log.getLogWriter().info("EXISTING MBeans: " + mbeanServer.queryNames(MBeanJMXAdapter.getObjectName(pattern), null));
    } catch (IOException e) {
      Log.getLogWriter().info("Exception while querying EXISTING MBeans.", e);
    }
  }

  private void populateTableMBean(MBeanServerConnection mbeanServer,
      boolean expectedBeans, List<ClientVmInfo> selectedVmList,
      String tableName, List<String> serverGroups)
      throws MalformedObjectNameException, NullPointerException, IOException, AttributeNotFoundException, TestException {
    checkTableMBean(mbeanServer, expectedBeans, selectedVmList, tableName, serverGroups, true, false, false);
  }
  
  private void checkTableMBean(MBeanServerConnection mbeanServer,
      boolean expectedBeans, List<ClientVmInfo> selectedVmList,
      String tableName, List<String> serverGroups,  boolean verifyMBean, boolean verifyAggregateMBean)
      throws MalformedObjectNameException, NullPointerException, IOException, AttributeNotFoundException, TestException {
    checkTableMBean(mbeanServer, expectedBeans, selectedVmList, tableName, serverGroups, false, verifyMBean, verifyAggregateMBean);
  }

  private void checkTableMBean(MBeanServerConnection mbeanServer, 
      boolean expectedBeans, List<ClientVmInfo> selectedVmList,
      String tableName, List<String> serverGroups, boolean populateBB, boolean verifyMBean, boolean verifyAggregateMBean)
      throws MalformedObjectNameException, NullPointerException, IOException, AttributeNotFoundException, TestException {
    
    
    for (ClientVmInfo info : selectedVmList) {
      
      String memberId = (String) SQLBB.getBB().getSharedMap().get("MEMBERID_" + info.getClientName());
      List<String> newServerGroups = new ArrayList<String>(serverGroups);
      newServerGroups.add(DEFAULT_SERVER_GROUP);
      for (String serverGroup : newServerGroups) {
        checkTableMBeanForGroup(mbeanServer, expectedBeans, tableName, populateBB, info, memberId, serverGroup, verifyMBean);        
      }
    }

    if(populateBB) {
      return;
    }
    
    if(verifyAggregateMBean) {
      verifyAggregatedValueForTableMBean(mbeanServer, expectedBeans, tableName);
    }

  }

  private void verifyAggregatedValueForTableMBean(
      MBeanServerConnection mbeanServer, boolean expectedBeans, String tableName)
      throws MalformedObjectNameException, IOException, TestException {
    HydraUtil.sleepForReplicationJMX();
    // check aggreTable
    String tableONStr = aggrTableONTemplate.replace("{0}", tableName);
    ObjectName tableON = new ObjectName(tableONStr);
    Log.getLogWriter().info("Querying for instance " + tableON);
    try {
      ObjectInstance instance = mbeanServer.getObjectInstance(tableON);
      Log.getLogWriter().info("Found instance " + tableON + " Expected " + expectedBeans);
      if (!expectedBeans && instance != null) {
          //select TABLESCHEMANAME, TABLENAME from SYS.SYSTABLES order by TABLESCHEMANAME;
          saveError("Found aggreTable instance " + tableON + " when not expected ");
      } 
      
      if (expectedBeans) {
        
        if(instance==null) {
          saveError("Did not Found aggreTable instance " + tableON + " when expected");
          return;
        }
          
        String attrs[] = { "EntrySize", "NumberOfRows" };
        AttributeList list = mbeanServer.getAttributes(tableON, attrs);
        Log.getLogWriter().info("GFXD Aggregate Table Attributes " + HydraUtil.ObjectToString(list));
        for (Object object : list) {
          Attribute attribute = (Attribute) object;
          Log.getLogWriter().info("Validating Aggregated attribute : " + attribute);

          if ("NumberOfRows".equalsIgnoreCase(attribute.getName())) {
            int count = executeAndGetNumber("select count(*) from " + tableName);
            mbeanHelper.printActualAndExpectedForAttribute("Validating Aggregated Table Attributes for : ", attribute, count);
            if (count != ((Long) attribute.getValue()).longValue()) {
              mbeanHelper.printAndSaveError("Aggregated Attribute", tableName, attribute, count);
            }
          }
          if ("EntrySize".equalsIgnoreCase(attribute.getName())) {
            if ("trade.sellorders".equalsIgnoreCase(tableName)) {
              Number expected = getAggregatedValuesForCounter("SELLORDER_ENTRYSIZE_", mbeanServer);
              mbeanHelper.printActualAndExpectedForAttribute("Validating Aggregated Table Attributes for : ", attribute, expected);
              if (Math.abs(expected.doubleValue() - ((Number) attribute.getValue()).doubleValue()) > .001) {
                mbeanHelper.printAndSaveError("Aggregated Attribute", tableName, attribute, expected);
              }
            }
          }
          Log.getLogWriter().info(
              "GFXD Aggregate Table Attribute " + attribute.getName() + " validated for " + tableName);
        }
      }
    } catch (InstanceNotFoundException e) {
      if (expectedBeans) {
        logMBeans(MessageFormat.format(ManagementConstants.OBJECTNAME__AGGREGATETABLE_MXBEAN, new Object[] {"*"}), mbeanServer);
        throw new TestException("checkAggregateTableMBean\n" + TestHelper.getStackTrace(e));
      }
    } catch (ReflectionException e) {
    	throw new TestException("checkAggregateTableMBean\n" + TestHelper.getStackTrace(e));	
    	}
  }


  private Number getAggregatedValuesForCounter(String counter, MBeanServerConnection mbeanServer) {
    String[] members = null;
    try {
      members = getGfxdClusterMembers(mbeanServer);
    } catch (Exception e) {
    }
    Number aggregatedValue = 0;
    for (String member : members) {
      String memberName = member.substring(member.indexOf('(') + 1, member.indexOf(":"));
      @SuppressWarnings("unchecked")
      String clientName = ((Map<String, String>)SQLBB.getBB().getSharedMap().get(MEMBERS_MAPPING)).get(memberName);
      String counterName = counter + clientName;
      double doubleValue = getCounterNumber(counterName).doubleValue();
      aggregatedValue  = aggregatedValue.doubleValue() + doubleValue;
      Log.getLogWriter().info(counterName + " value for member : " + member + " is " + doubleValue);
    }
    return aggregatedValue;
  }

  private void checkTableMBeanForGroup(MBeanServerConnection mbeanServer,
      boolean expectedBeans, String tableName, boolean populateBB,
      ClientVmInfo info, String memberId, String serverGroup, boolean verifyMBean)
      throws MalformedObjectNameException, IOException, TestException, AttributeNotFoundException {
    // query only those members which part of server group specified in
    // CREATE TABLE
    Log.getLogWriter().info("validating table mbean for table : " + tableName + " for serverGroup : " + serverGroup + " for member : " + memberId);
    if(DEFAULT_SERVER_GROUP.equals(serverGroup)) {
      if(isTableAndMemberOfSameServerGroup(tableName, memberId) && !isTablePartOfServerGroup(tableName, DEFAULT_SERVER_GROUP))  {
        Log.getLogWriter().info("member and table both are part of same server group and table is not part of default server group");
        verifyTableMBeanShouldNotPresentInGroup(mbeanServer, tableName, memberId, DEFAULT_SERVER_GROUP);
      } else {
        Log.getLogWriter().info("member and table both are not part of same server group");
        verifyTableMBeanForGroup(mbeanServer, expectedBeans, tableName, populateBB, memberId, serverGroup, verifyMBean);
      }        
    } else {
      if (isMemberPartOfGroup(info.getClientName(), serverGroup) && isTablePartOfServerGroup(tableName, serverGroup)) {
        Log.getLogWriter().info("member and table both are part of same server group : " + serverGroup);
        verifyTableMBeanForGroup(mbeanServer, expectedBeans, tableName, populateBB, memberId, serverGroup, verifyMBean);
        verifyTableMBeanShouldNotPresentInGroup(mbeanServer, tableName, memberId, DEFAULT_SERVER_GROUP);
      }
      if (!isMemberPartOfGroup(info.getClientName(), serverGroup) && isTablePartOfServerGroup(tableName, serverGroup)) {
        Log.getLogWriter().info("member is not part of server group and table is part of server group : " + serverGroup);
        verifyTableMBeanForGroup(mbeanServer, expectedBeans, tableName, populateBB, memberId, DEFAULT_SERVER_GROUP, verifyMBean);
        verifyTableMBeanShouldNotPresentInGroup(mbeanServer, tableName, memberId, serverGroup);
      }
      
      if ( !isTablePartOfServerGroup(tableName, serverGroup) ) {
        Log.getLogWriter().info("table is not part of server group : " + serverGroup);
        verifyTableMBeanShouldNotPresentInGroup(mbeanServer, tableName, memberId, serverGroup);
        verifyTableMBeanForGroup(mbeanServer, expectedBeans, tableName, populateBB, memberId, DEFAULT_SERVER_GROUP, verifyMBean);
      }
    }
  }

  private boolean isTableAndMemberOfSameServerGroup(String tableName, String memberId) {
    Log.getLogWriter().info("Trying to find if table and member from same server group, table : " + tableName + " and memberId : " + memberId);
    List<String> groups = getServerGroupsArray(memberId);
    Log.getLogWriter().info("Server Groups for member : " + memberId + " is " + groups);
    @SuppressWarnings("unchecked")
    Map<String, List<String>> tableToServerGroupMapping = (Map<String, List<String>>) SQLBB.getBB().getSharedMap().get(TABLE_SERVER_GROUP);
    Log.getLogWriter().info("Getting the server groups for tableName : " + tableName + " Map : " + tableToServerGroupMapping);
    List<String> list = tableToServerGroupMapping.get(tableName);
    Log.getLogWriter().info("Server Groups for table : " + tableName + " is " + list);
    if(list == null) {
      if(groups.size() == 1 && groups.get(0).equals(DEFAULT_SERVER_GROUP)) {
        return true;
      }
      return false;
    }
    for(String group : groups ) {
      if (list.contains(group)) {
        return true;
      }
    }
    
    return false;
  }

  private void verifyTableMBeanShouldNotPresentInGroup(
      MBeanServerConnection mbeanServer, String tableName, String memberId, String serverGroup) throws MalformedObjectNameException, IOException {
    ObjectName tableON = getTableMBean(tableName, memberId, serverGroup);
      try {
        mbeanServer.getObjectInstance(tableON);
        saveError("Table MBean should not come for this server group, Server Group : " + serverGroup + ", and Table :" + tableName);
      } catch (InstanceNotFoundException e) {
      }
  }
  
  private boolean isPrProxyTableMBean(String tableName, String serverGroup,String tableServerGroup[], String memberName){
    if (serverGroup.equals(DEFAULT_SERVER_GROUP)) {
      String memberServerGroup[] = getServerGroupsArray(memberName).toArray(new String[0]);
      if (tableServerGroup.length > 0) {
        boolean isTableGroupAndServerGroupMatch = false;
        for (String tableG : tableServerGroup) {
          for (String mg : memberServerGroup) {
            if (!mg.equals(DEFAULT_SERVER_GROUP) && tableG.equals(mg)) {
              isTableGroupAndServerGroupMatch = true;
              break;
            }
          }
        }
        Log.getLogWriter()
            .info("#isPrProxyTableMBean : tableServerGroup " + HydraUtil.ObjectToString(tableServerGroup));
        Log.getLogWriter().info(
            "#isPrProxyTableMBean : memberServerGroup " + HydraUtil.ObjectToString(memberServerGroup));
        Log.getLogWriter().info(
            "#isPrProxyTableMBean : for table " + tableName + " servergrp " + serverGroup + " is " + (!isTableGroupAndServerGroupMatch));
        return !isTableGroupAndServerGroupMatch;
      } else
        return false;
    } else
      return false;
  }

  private void verifyTableMBeanForGroup(MBeanServerConnection mbeanServer,
      boolean expectedBeans, String tableName, boolean populateBB,
      String memberId, String serverGroup, boolean verifyMBean)
      throws MalformedObjectNameException, IOException, TestException,
      AttributeNotFoundException {
    Log.getLogWriter().info("Checking table mbean for table : " + tableName + " and server-group " + serverGroup + " for member " + memberId);
    ObjectName tableON = getTableMBean(tableName, memberId, serverGroup);

    try {
      ObjectInstance instance = mbeanServer.getObjectInstance(tableON);
      Log.getLogWriter().info("Found instance " + tableON + " Expected " + expectedBeans);
      if (!expectedBeans && instance != null)
        throw new TestException("Found table instance " + tableON + " when not expected ");

      AttributeList list = mbeanServer.getAttributes(tableON, TABLE_ATTRS);
      Log.getLogWriter().info("GFXD Table Attributes " + HydraUtil.ObjectToString(list));
      Map<String, Object> attrMap = new HashMap<String, Object>();
      if(!populateBB) {
        attrMap = getTableAttribute(tableName);
      }
      
      Statistics[] stat = getTableStats(tableName.substring(tableName.indexOf(".") + 1, tableName.length()),  true);
          //!"NA".equals(((String) getTableMBeanAttribute(mbeanServer, tableON, "PersistenceScheme"))));
      
      for (Object object : list) {
        Attribute attribute = (Attribute)object;
        Log.getLogWriter().info("Validating Attribute : " + attribute);
        if (populateBB) {
            Log.getLogWriter().info("Populating BB, so no validation will happen...");
            attrMap.put(attribute.getName(), attribute.getValue());
        } else if(attrMap != null && !attrMap.isEmpty()) {
          if (verifyMBean && attribute.getName().equalsIgnoreCase("Inserts")) {
            if (stat.length > 0) {
              validateTableIntAttr(tableName, mbeanServer, tableON, attribute, stat[0].get("creates").intValue());
            }
          } else if (verifyMBean && attribute.getName().equalsIgnoreCase("Updates")) {
            if (stat.length > 0) {
              validateTableIntAttr(tableName, mbeanServer, tableON, attribute, stat[0].get("puts").intValue());
            }
          } else if (verifyMBean && attribute.getName().equalsIgnoreCase("Deletes")) {
            if (stat.length > 0) {
              validateTableIntAttr(tableName, mbeanServer, tableON, attribute, stat[0].get("destroys").intValue());
            }
          }
          else {
            Log.getLogWriter().info("Validating Table Attributes for : " + attribute);
            List<String> doNotTestHere = verifyMBean ? Arrays.asList("NumberOfRows", "EntrySize", "KeySize") : Arrays.asList("NumberOfRows", "EntrySize", "KeySize", "Inserts", "Updates", "Deletes");
            if (attribute.getValue() instanceof String) {
              if (!attrMap.get(attribute.getName()).equals(attribute.getValue())) {
                saveError(attribute.getName() + " did not match for " + tableName);
              }
            } else if(attribute.getValue() == null && attrMap.get(attribute.getName()) != null) {
              saveError(attribute.getName() + " did not match for " + tableName);
            } else if(attribute.getValue() instanceof Object[]) {
              Object[] expectedArray = (Object[])attrMap.get(attribute.getName());
              Object[] actualArray = (Object[])attribute.getValue();
              if(((expectedArray == null && actualArray != null) || (expectedArray != null && actualArray == null)) 
                  && expectedArray.length != actualArray.length) {
                saveError(attribute.getName() + " did not match for " + tableName);
              }
              List<Object> actualList = Arrays.asList(actualArray);
              for(int i = 0; i < expectedArray.length; i++) {
                if(!actualList.contains(expectedArray[i])) {
                  saveError(attribute.getName() + " did not match for " + tableName);
                }
              }
            } else if (attribute.getValue() != null && !doNotTestHere.contains(attribute.getName())) {
                Log.getLogWriter().info("Not Validating Table Attributes for : " + attribute.getName());
                Log.getLogWriter().info("Table Attributes type is : " + attribute.getValue().getClass());
                saveError("Did not verified : " + attribute.getName() + " for " + tableName);
            }
          } 
        }
        verfiyAttributeForTableMBean(mbeanServer, tableName, memberId, verifyMBean, tableON, attribute);
      }
     
      
      
      //We only want to populate attribute, so no need to go further...
      if(populateBB) {
        Log.getLogWriter().info("Populating BB for tableName :" + tableName + "with attrMap : " + attrMap);
        saveTableAttribute(tableName, attrMap);
        return;
      }
  
      for (String m : TABLE_METHODS) {
        if(m.startsWith("listIndexInfo")) {
          SharedLock sharedLock = MBeanIndexBB.getBB().getSharedLock();
          try {
            sharedLock.lock();
            verifyListIndexInfo(mbeanServer, tableName, memberId, serverGroup, verifyMBean, tableON, m);
          } finally {
            sharedLock.unlock();
          }
          
        } else {
          Object result = mbeanServer.invoke(tableON, m, null, null);
         
          Log.getLogWriter().info("GFXD Table Op " + m + " : " + HydraUtil.ObjectToString(result));
          
          if(result != null) {
            Log.getLogWriter().info("GFXD Table Op "+ m + " : " + result.getClass());
          }
          if (MBeanPrms.isWANTest() && m.equals("fetchMetadata")) {
            CompositeData data = (CompositeData) result;
            String gatewayEnabled = (String) data.get("gatewayEnabled");
            String gatewaySenders = (String) data.get("gatewaySenders");
            if (gatewayEnabled.equals("false"))
              throw new TestException("Expected gatewayEnabled=true");
            if (gatewaySenders == null || "".equals(gatewaySenders))
              throw new TestException("Expected gatewaySenders non-empty");
            if (!gatewaySenders.equals(getSenderId())) {
              throw new TestException("Expected gatewaySenders=" + getSenderId() + " found <" + gatewaySenders + ">");
            }
          }
        }
      }
    } catch (InstanceNotFoundException e) {
      Log.getLogWriter().info("Trying to find mbean for : " + tableON + "where we are " + (expectedBeans ? "" : "not") + "expecting it");
      if (expectedBeans) {
        logMBeans(MessageFormat.format(ManagementConstants.OBJECTNAME__TABLE_MXBEAN, new Object[] {"*", "*", "*"}), mbeanServer);
        throw new TestException("checkTableMBean\n" + TestHelper.getStackTrace(e));
      }
    } catch (ReflectionException e) {
      throw new TestException("checkTableMBean\n" + TestHelper.getStackTrace(e));
    } catch (MBeanException e) {
      throw new TestException("checkTableMBean\n" + TestHelper.getStackTrace(e));
    }
  }
  
  private void verfiyAttributeForTableMBean(MBeanServerConnection mbeanServer,
      String tableName, String memberId, boolean verifyMBean,
      ObjectName tableON, Attribute attribute) throws TestException,
      AttributeNotFoundException, InstanceNotFoundException, MBeanException,
      ReflectionException, IOException {
  Log.getLogWriter().info("Verify MBean Value : " + verifyMBean);
  if (verifyMBean) {
    if (attribute.getName().equalsIgnoreCase("EntrySize")) {

      // select entry_size from sys.memoryanalytics
      Number value = validateTableAttr(
          tableName,
          mbeanServer,
          tableON,
          attribute,
          "select entry_size from sys.memoryanalytics WHERE INDEX_NAME IS NULL AND TABLE_name = '"
              + tableName + "'  and ID like '%" + memberId + "%'",
          OutputType.FLOAT);

      if ("trade.sellorders".equalsIgnoreCase(tableName)) {
        saveCounter(COUNTER_SELLORDER_ENTRYSIZE, value);
      }
    } else if (attribute.getName().equalsIgnoreCase("KeySize")) {
      Number value = validateTableAttr(
          tableName,
          mbeanServer,
          tableON,
          attribute,
          "select key_size from sys.memoryanalytics WHERE INDEX_NAME IS NULL AND TABLE_name = '"
              + tableName + "'  and ID like '%" + memberId + "%'",
          OutputType.FLOAT);
      if ("trade.sellorders".equalsIgnoreCase(tableName)) {
        saveCounter(COUNTER_SELLORDER_KEYSIZE, value);
      }
    } else if (attribute.getName().equalsIgnoreCase("NumberOfRows")) {
      Number value = validateTableAttr(
          tableName,
          mbeanServer,
          tableON,
          attribute,
          "select count(*) from " + tableName + " WHERE DSID() like '%" + memberId + "%'",
          OutputType.INT);
      if ("trade.sellorders".equalsIgnoreCase(tableName)) {
        saveCounter(COUNTER_SELLORDER_NO_OF_ROWS, value);
      }
    }
  }
}
  
  private void verifyListIndexInfo(MBeanServerConnection mbeanServer,
      String tableName, String memberId, String serverGroup,
      boolean verifyMBean, ObjectName tableON, String m)
      throws InstanceNotFoundException, MBeanException, ReflectionException,
      IOException, AttributeNotFoundException {
    Log.getLogWriter().info("Expected Index List : " + getAllIndex(tableName));
    Object result = mbeanServer.invoke(tableON, m, null, null);
    Log.getLogWriter().info(" Result list index Info" + result + "");
    Log.getLogWriter().info("GFXD Table Op "+ m + " : " + HydraUtil.ObjectToString(result));
    Log.getLogWriter().info("GFXD Table Op "+ m + " : " + result.getClass());
    CompositeData[] data = (CompositeData[]) result;
    Log.getLogWriter().info(" composite data length: " + data.length);
    for(int i = 0; i < data.length; i++) {
      Log.getLogWriter().info(" composite data : " + HydraUtil.ObjectToString(data[i]) + "");
    }
    String[] array = tableName.split("\\.");
    Log.getLogWriter().info("#listIndexInfo schemaName " + array[0] + " tableName " + array[1]);
    mbeanHelper.runQueryAndPrintValue("select * from SYS.INDEXES where SCHEMANAME='"+ array[0] +"' and TABLENAME='"+ array[1] + "'");
    Number indexCount = (Number)mbeanHelper.runQueryAndGetValue("select COUNT(*) from SYS.INDEXES where SCHEMANAME='"+ array[0] +"' and TABLENAME='"+ array[1] + "'", OutputType.INT);
    
    String[] tableServerGroups = (String[]) mbeanServer.getAttribute(tableON, "ServerGroups");
    String memberName = tableON.getKeyProperty("member");
    boolean isPartitionProxy = isPrProxyTableMBean(tableName, serverGroup, tableServerGroups, memberName);
    if (!isPartitionProxy) {
      Log.getLogWriter().info(
      "Table is NOT partition proxy table hence matching indexinfo #listIndexInfo #isPartitionProxy");
      match("Index count does not match  for " + tableName, indexCount.intValue(), data.length);
    } else {
      Log.getLogWriter().info(
          "Table is partition proxy table hence skipping matching indexinfo #listIndexInfo #isPartitionProxy");
    }
    
    if(data.length > 0 && verifyMBean){
      result = mbeanServer.invoke(tableON, "listIndexStats", null, null);
      Log.getLogWriter().info(" Result list index Info" + result + "");
      Log.getLogWriter().info("GFXD Table Op listIndexStats : " + HydraUtil.ObjectToString(result));
      Log.getLogWriter().info("GFXD Table Op listIndexStats : " + result.getClass());
      CompositeData[] stats = (CompositeData[]) result;
      Log.getLogWriter().info(" index length: " + stats.length);
      for(int i = 0; i < stats.length; i++) {
        Log.getLogWriter().info(" index stats : " + HydraUtil.ObjectToString(stats[i]) + "");              
        Number entrySize = (Number)stats[i].get("entrySize");
        String indexName = (String) stats[i].get("indexName");              
        Number keySize = (Number) stats[i].get("keySize");
        Number rowCount = (Number) stats[i].get("rowCount");
        
        Number actualEntrySize = (Number)mbeanHelper.runQueryAndGetValue("select  ENTRY_SIZE from sys.memoryanalytics WHERE  TABLE_name = '"
            + tableName + "'  and ID like '%" + memberId + "%' AND INDEX_NAME='" + indexName + "'", OutputType.FLOAT);
        match("Validation failed for matching Index " + indexName + " on table " + tableName + " attribute ENTRY_SIZE" , entrySize, actualEntrySize);
        
        Number actualKeySize = (Number)mbeanHelper.runQueryAndGetValue("select  KEY_SIZE from sys.memoryanalytics WHERE  TABLE_name = '"
            + tableName + "'  and ID like '%" + memberId + "%' AND INDEX_NAME='" + indexName + "'", OutputType.FLOAT);
        match("Validation failed for matching Index " + indexName + " on table " + tableName + " attribute KEY_SIZE" , keySize, actualKeySize);
        
        Number actualRowCount = (Number)mbeanHelper.runQueryAndGetValue("select  NUM_ROWS from sys.memoryanalytics WHERE TABLE_name = '"
            + tableName + "'  and ID like '%" + memberId + "%' AND INDEX_NAME='" + indexName + "'", OutputType.LONG);
        match("Validation failed for matching Index " + indexName + " on table " + tableName + "  attribute ROW_COUNT" , rowCount, actualRowCount);
        
        Log.getLogWriter().info("#listIndexStats IndexName <" + indexName +"> ES <" + entrySize +"> COUNT <" + rowCount
            + " KS <" + keySize+">");
        
        Log.getLogWriter().info("#listIndexStatsFromMemAnalytics IndexName <" + indexName +"> ES <" + actualEntrySize +"> COUNT <" + actualRowCount
            + " KS <" + actualKeySize+">");
      }
    }
  }
//
//  private void checkTableShouldPresentOnlyInThereRespectiveGroup(
//      MBeanServerConnection mbeanServer, String tableName, String memberId,
//      String serverGroup) throws MalformedObjectNameException, IOException {
//    if(isTablePartOfServerGroup(tableName, serverGroup)) {
//      ObjectName tableON = getTableMBean(tableName, memberId, serverGroup);
//      try {
//        mbeanServer.getObjectInstance(tableON);
//      } catch (InstanceNotFoundException e) {
//        saveError("Table MBean should come for this server group, Server Group : " + serverGroup + ", and Table :" + tableName);
//      }
//    }
//    // if server group is default and this member is not part of server group for table then we should see mbean
//    // but if server group is default and this member is part of server group for table then we should not see mbean
//    if (DEFAULT_SERVER_GROUP.equals(serverGroup)) {
//      List<String> serverGroups = getServerGroupsForTable(tableName);
//      ObjectName tableON = getTableMBean(tableName, memberId, serverGroup);
//      if (serverGroups == null) {
//        try {
//          mbeanServer.getObjectInstance(tableON);
//        } catch (InstanceNotFoundException e) {
//          saveError("Table MBean should come for this server group, Server Group : " + serverGroup + ", and Table :" + tableName);
//        }
//      } else {
//        for (String sg : serverGroups) {
//          if (isMemberPartOfGroup(memberId, sg)) {
//            try {
//              mbeanServer.getObjectInstance(tableON);
//              saveError("Table MBean should not come for this server group, Server Group : " + serverGroup + ", and Table :" + tableName);
//            } catch (InstanceNotFoundException e) {
//            }
//          } else {
//            try {
//              mbeanServer.getObjectInstance(tableON);
//            } catch (InstanceNotFoundException e) {
//              saveError("Table MBean should come for this server group, Server Group : " + serverGroup + ", and Table :" + tableName);
//            }
//          }
//        }
//      }
//    } else if (!isTablePartOfServerGroup(tableName, serverGroup) && isMemberPartOfGroup(memberId, serverGroup)) {
//      ObjectName tableON = getTableMBean(tableName, memberId, serverGroup);
//      try {
//        mbeanServer.getObjectInstance(tableON);
//        saveError("Table MBean should not come for this server group, Server Group : " + serverGroup + ", and Table :" + tableName);
//      } catch (InstanceNotFoundException e) {
//      }
//    }
//  }

  private ObjectName getTableMBean(String tableName, String memberId, String serverGroup) throws MalformedObjectNameException {
    Log.getLogWriter().info("Checking table mbean for table " + tableName + " for server-group " + serverGroup + " for member " + memberId);
    String tableONStr = tableONTemplate.replace("{0}", serverGroup);
    tableONStr = tableONStr.replace("{1}", memberId);
    tableONStr = tableONStr.replace("{2}", tableName);
    ObjectName tableON = new ObjectName(tableONStr);
    Log.getLogWriter().info("Querying for instance " + tableON);
    return tableON;
  }

//  private List<String> getServerGrou    psForTable(String tableName) {
//    Map<String, List<String>> tableToServerGroupMapping = (Map<String, List<String>>) SQLBB.getBB().getSharedMap().get(TABLE_SERVER_GROUP);
//    Log.getLogWriter().info("Getting the server groups for tableName : " + tableName);
//    List<String> list = tableToServerGroupMapping.get(tableName);
//    Log.getLogWriter().info("Server groups for table : " + tableName + " is " + list);
//    return list;
//  }

  private boolean isTablePartOfServerGroup(String tableName, String serverGroup) {

    Log.getLogWriter().info("Trying to find is table : " + tableName + " part of server group : " + serverGroup );
    
    @SuppressWarnings("unchecked")
    Map<String, List<String>> tableToServerGroupMapping = (Map<String, List<String>>) SQLBB.getBB().getSharedMap().get(TABLE_SERVER_GROUP);    
    Log.getLogWriter().info("Getting the server groups for tableName : " + tableName + " map : " + tableToServerGroupMapping);
    List<String> list = tableToServerGroupMapping.get(tableName);
    Log.getLogWriter().info("Server groups for table : " + tableName + " is " + list);
    if(list != null) {
      return list.contains(serverGroup);
    }
    if(list == null && DEFAULT_SERVER_GROUP.equals(serverGroup)) {
      return true;
    }
    return false;
  }

  private Object getTableMBeanAttribute(MBeanServerConnection mbeanServer,ObjectName tableON, String attributeName) {
    try {
      return mbeanServer.getAttribute(tableON, attributeName);
    } catch (AttributeNotFoundException e) {
      throw new TestException("checkTableMBean\n" + TestHelper.getStackTrace(e));
    } catch (InstanceNotFoundException e) {
    } catch (MBeanException e) {
      throw new TestException("checkTableMBean\n" + TestHelper.getStackTrace(e));
    } catch (ReflectionException e) {
      throw new TestException("checkTableMBean\n" + TestHelper.getStackTrace(e));
    } catch (IOException e) {
      throw new TestException("checkTableMBean\n" + TestHelper.getStackTrace(e));
    }
    return "NA";
  }

  private int executeAndGetNumber(String sql) {
    Connection conn = null;
    try {
      conn = getGFEConnection();
    //  printOpenConnection("executeAndGetNumber");
      ResultSet rs = conn.createStatement().executeQuery(sql);
      if (rs.next()) {
        return rs.getInt(1);
      }
    } catch (SQLException e) {
       throw new TestException("checkTableMBean\n" + TestHelper.getStackTrace(e));
    } finally {
      if (conn != null) {
        closeGFEConnection(conn);
      }
    }
    return 0;

  }

  private Number validateTableAttr(String tableName, MBeanServerConnection mBeanServer, ObjectName name, Attribute attribute, String sql, OutputType type) throws TestException, AttributeNotFoundException, InstanceNotFoundException, MBeanException, ReflectionException, IOException {
    mbeanHelper.runQueryAndPrintValue(sql);
    Number expected = (Number)mbeanHelper.runQueryAndGetValue(sql, type);
    Number actual = ((Number)mBeanServer.getAttribute(name, attribute.getName()));
  
    if(!actual.toString().equals(expected.toString())) {
      saveError(attribute.getName() + " attribute did not match for " + tableName + " where expected = " + expected + " and actual : " + actual);
    } else {
      Log.getLogWriter().info(attribute.getName() + " attribute match for " + tableName + " where expected = " + expected + " and actual : " + actual);
    }
    return actual;
  }
  

  private void validateTableIntAttr(String tableName, MBeanServerConnection mBeanServer, ObjectName name, Attribute attribute, Number value) throws AttributeNotFoundException, InstanceNotFoundException, MBeanException, ReflectionException, IOException {
    int actual = ((Integer)mBeanServer.getAttribute(name, attribute.getName())).intValue();
   
    if(actual - value.intValue() < 0) {
      saveError(attribute.getName() + " attribute did not match for " + tableName + " where expected = " + value + " and actuals : " + actual);
    } else {
      Log.getLogWriter().info(attribute.getName() + " attribute match for " + tableName + " where expected = " + value + " and actuals : " + actual);
    }
  }

  private Object getSenderId() {
    String clientName = RemoteTestModule.getMyClientName();
    String[] array = clientName.split("_");
    String dsName = "SENDER_" + array[1];    
    if("1".equals(array[1]))
      dsName += "_2";
    else
      dsName += "_1";
    
    Log.getLogWriter().info("Sender ID for this VM is " + dsName);
    return dsName;
  }

  private boolean isMemberPartOfGroup(String clientName, String serverGroup) {
    if(DEFAULT_SERVER_GROUP.equals(serverGroup))
      return true;
    List<String> groups = getServerGroupsArray(clientName);
    for(String s : groups)
      if(s.equals(serverGroup) && !s.equals(DEFAULT_SERVER_GROUP))
        return true;
    Log.getLogWriter().info("VM " + clientName + " is not part of group " + serverGroup);
    return false;
  }

  // task is serial and procedure is called on members
  // Counter is synchronized  with actual invokations only if test is serial
  private void doProcedureCall() throws SQLException {
    int times = HydraUtil.getnextNonZeroRandomInt(10);
    Connection conn = getGFEConnection();
  //  printOpenConnection("doProcedureCall");
    CallableStatement cs;
    
    String procName = HydraUtil.getRandomElement(new String[] { "trade.show_customers(?)", "trade.longRunningProcedure(?)" });
    String sql = "CALL  " + procName + " ON ALL";
    cs = conn.prepareCall(sql);
    Log.getLogWriter().info("Executing proc sql " + sql);
    for (int i = 0; i < times; i++) {
      try {
        Log.getLogWriter().info("Running Procedure : " + procName);
        int counter = isEdge ? incrementCounter(COUNTER_MBEAN_PROC_CALLS_IN_PROGRESS_EDGE) : incrementCounter(COUNTER_MBEAN_PROC_CALLS_IN_PROGRESS);
        cs.setInt(1, RemoteTestModule.getCurrentThread().getThreadId());
        cs.execute();
        counter = isEdge ? incrementCounter(COUNTER_MBEAN_PROC_CALLS_EDGE) : incrementCounter(COUNTER_MBEAN_PROC_CALLS);
      } catch(SQLException e) {
        if (e.getSQLState().equals("XCL54")) {
          Log.getLogWriter().warning("Error occurred while executing query : " + sql + ", Low Memory Exception");
        } else {
          throw e;
        }
      } finally {
        if(isEdge) {
          decrementCounter(COUNTER_MBEAN_PROC_CALLS_IN_PROGRESS_EDGE);
        } else {
          decrementCounter(COUNTER_MBEAN_PROC_CALLS_IN_PROGRESS);
        }
        
        Log.getLogWriter().info("Procedure Completed : " + procName);
      }
    }
    
    cs.close();
  }


  private void doUpdate(Connection conn, String tableName) throws SQLException {
    String updateStmt = SELLORDER_UPDATE.replace("{0}", tableName);
    PreparedStatement stmt = conn.prepareStatement(updateStmt);
    int qty = HydraUtil.getnextNonZeroRandomInt(100);
    int oid = HydraUtil.getnextNonZeroRandomInt(getCounter("SELLORDER_OID"));
    stmt.setInt(1, qty);
    stmt.setInt(2, oid);
    Log.getLogWriter().info("Updating qty for orderid=" + oid + " new qty " + qty);
    stmt.execute();
    stmt.close();
    incrementCounter(COUNTER_SELLORDER_UPDATES);
  }

	private void doDelete(Connection conn, String tableName) throws SQLException {
		String deleteStmt = SELLORDER_DELETE.replace("{0}", tableName);
		PreparedStatement stmt = conn.prepareStatement(deleteStmt);
		int qty = HydraUtil.getnextNonZeroRandomInt(100);
		int oid = HydraUtil.getnextNonZeroRandomInt(getCounter("SELLORDER_OID"));
		stmt.setInt(1, qty);
		stmt.setInt(2, oid);
		Log.getLogWriter().info("Deleting qty for orderid=" + oid + " new qty " + qty);
		stmt.execute();
		stmt.close();
		incrementCounter(COUNTER_SELLORDER_DELETES);
	}

	private void doSelect(Connection conn, String tableName) throws SQLException {
	  //DMLStmtIF dmlStmt = dmlFactory.createDMLStmt(tableName); //dmlStmt of a table
    MBeanTradeSellOrdersDMLStmt dmlStmt = new MBeanTradeSellOrdersDMLStmt();
    dmlStmt.addListener(new CallBackListener() {
      @Override
      public String getName() {
        return SELECT_QUERY_TAG;
      }

      @Override
      public void execute(Object... params) {
        String sql = (String) params[0];
        getAndIncrementCounter(COUNTER_SELLORDER_SELECTS, sql);

      }
    });
    dmlStmt.query(conn);
	}
	
	
  private void doInsert(Connection conn, String tableName) throws SQLException {
    String status = HydraUtil.getRandomElement(new String[]{"cancelled", "open", "filled"});
    int tid = RemoteTestModule.getCurrentThread().getThreadId();
    int size=1;
    int cids[] = new int[size];
    int sids[] = new int[size];
    getCustomerAndPortFolio(conn, tid, size, cids, sids);
    String insetStmt = SELLORDER_INSERT.replace("{0}", tableName);
    Log.getLogWriter().info("Trying to execute : " + insetStmt);
    PreparedStatement stmt = conn.prepareStatement(insetStmt);
    for (int i = 0; i < size; i++) {
      int cid = cids[i];
      int sid = sids[i];
      int oid = getOrderId(conn, cid, sid);
      int qty = HydraUtil.getnextNonZeroRandomInt(15);
      
      BigDecimal ask = new BigDecimal(TestConfig.tab().getRandGen().nextDouble());
      Timestamp order_time = new Timestamp(System.currentTimeMillis());
      stmt.setInt(1, oid);
      stmt.setInt(2, cid);
      stmt.setInt(3, sid);
      stmt.setInt(4, qty);
      stmt.setBigDecimal(5, ask);
      stmt.setTimestamp(6, order_time);
      stmt.setString(7, status);
      stmt.setInt(8, tid);
      int rowCount = stmt.executeUpdate();
      Log.getLogWriter().info("Params used for insert query : cid :" + cid + ", sid : " + sid + ", oid : " + oid + " and qty : " + qty);
      Log.getLogWriter().info("Rows Inserted for " + insetStmt + " is " + rowCount);
      SQLWarning warning = stmt.getWarnings(); // test to see there is a warning
      if (warning != null) {
        SQLHelper.printSQLWarning(warning);
      }
      stmt.close();
      incrementCounter(COUNTER_SELLORDER_INSERTS);
    }
  }

  
  private void doPut(Connection conn, String tableName) throws SQLException {
    String status = HydraUtil.getRandomElement(new String[] { "cancelled", "open", "filled" });
    int tid = RemoteTestModule.getCurrentThread().getThreadId();
    int size=1;
    int cids[] = new int[size];
    int sids[] = new int[size];
    getCustomerAndPortFolio(conn, tid, size, cids, sids);
    String insetStmt = SELLORDER_PUT.replace("{0}", tableName);
    Log.getLogWriter().info("Trying to execute : " + insetStmt);
    PreparedStatement stmt = conn.prepareStatement(insetStmt);
    for (int i = 0; i < size; i++) {
      int cid = cids[i];
      int sid = sids[i];
      int oid = getOrderId(conn, cid, sid);
      int qty = HydraUtil.getnextNonZeroRandomInt(15);
      
      BigDecimal ask = new BigDecimal(TestConfig.tab().getRandGen().nextDouble());
      Timestamp order_time = new Timestamp(System.currentTimeMillis());
      stmt.setInt(1, oid);
      stmt.setInt(2, cid);
      stmt.setInt(3, sid);
      stmt.setInt(4, qty);
      stmt.setBigDecimal(5, ask);
      stmt.setTimestamp(6, order_time);
      stmt.setString(7, status);
      stmt.setInt(8, tid);
      int rowCount = stmt.executeUpdate();
      Log.getLogWriter().info("Params used for put query : cid :" + cid + ", sid : " + sid + ", oid : " + oid + " and qty : " + qty);
      Log.getLogWriter().info("Rows Inserted for " + insetStmt + " is " + rowCount);
      SQLWarning warning = stmt.getWarnings(); // test to see there is a warning
      if (warning != null) {
        SQLHelper.printSQLWarning(warning);
      }
      stmt.close();
      incrementCounter(COUNTER_SELLORDER_INSERTS);
    }
  }
  
  // should have used generated id instead :-)
  private int getOrderId(Connection conn, int cid, int sid) {
    int max = incrementCounter("SELLORDER_OID");
    Log.getLogWriter().info("New Order Id for cid,sid : (" + cid + "," + sid + ") is " + max);
    return max;
  }

  private void getCustomerAndPortFolio(Connection conn, int tid, int size, int[] cids, int[] sids) throws SQLException {
    ResultSet rs = null;
    Random rand = TestConfig.tab().getRandGen();

    String sql = "select * from trade.portfolio where tid = " + tid;
    try {
      rs = conn.createStatement().executeQuery(sql);

      
      // use gfxd connection, should not get lock timeout exception
      List<Struct> rsList = ResultSetHelper.asList(rs, ResultSetHelper.getStructType(rs), SQLHelper.isDerbyConn(conn));

      int rsSize = rsList.size();
      if (rsSize >= size) {
        int offset = rand.nextInt(rsSize - size); // start from a randomly
                                                  // chosen
                                                  // position
        for (int i = 0; i < size; i++) {
          cids[i] = ((Integer) ((GFXDStructImpl) rsList.get(i + offset)).get("CID")).intValue();
          Log.getLogWriter().info("cid is " + cids[i]);
          sids[i] = ((Integer) ((GFXDStructImpl) rsList.get(i + offset)).get("SID")).intValue();
          Log.getLogWriter().info("sid is " + sids[i]);
        }
      } else {
        for (int i = 0; i < rsSize; i++) {
          cids[i] = ((Integer) ((GFXDStructImpl) rsList.get(i)).get("CID")).intValue();
          sids[i] = ((Integer) ((GFXDStructImpl) rsList.get(i)).get("SID")).intValue();
        }
        // remaining cid, sid using default of 0, and should failed due to fk
        // constraint
        
      }
    } catch (SQLException e) {
      if (e.getSQLState().equals("XCL54")) {
        Log.getLogWriter().warning("Error occurred while executing query : " + sql + ", Low Memory Exception");
      } else {
        throw e;
      }
    }
  }

  private void checkProcCalls(MBeanServerConnection mbeanServer) {
    ObjectName name;
    try {
      name = new ObjectName(MessageFormat.format(ManagementConstants.OBJECTNAME__GFXDMEMBER_MXBEAN, new Object[] {
          (isEdge ? DEFAULT_SERVER_GROUP : "CG"), ManagementUtil.getMemberID() }));
      Log.getLogWriter().info("Object Name : " + name);
      
      int timesProcCalled = isEdge ? getCounter(COUNTER_MBEAN_PROC_CALLS_EDGE) : getCounter(COUNTER_MBEAN_PROC_CALLS);
      int jmxcalls = (Integer) mbeanServer.getAttribute(name, "ProcedureCallsCompleted");
      int inProgress = (Integer) mbeanServer.getAttribute(name, "ProcedureCallsInProgress");
      InternalDistributedSystem ds = InternalDistributedSystem.getConnectedInstance();
      FunctionStats functionStats = FunctionStats.getFunctionStats(DistributedProcedureCallFunction.FUNCTIONID, ds);
      int statsCalls = functionStats.getFunctionExecutionsCompleted();
      int statCalls2 = functionStats.getFunctionExecutionsCompletedDN();
      int executionRunning = functionStats.getFunctionExecutionsRunning();
      Log.getLogWriter().info("ProcedureCallsInProgress : Expected : " + inProgress + ", Actual : " + executionRunning );
      if(isEdge) {
        
      } else {
        if (timesProcCalled != jmxcalls && !MBeanPrms.isHATest()) {
          saveError("ProcedureCallsCompleted does not match actual value " + jmxcalls + " expected "
              + timesProcCalled + " statValue : " + statsCalls + " dnstatValue : " + statCalls2);
        }
      }
    } catch (MalformedObjectNameException e) {
      throw new TestException("Not able to execute procedure\n" + TestHelper.getStackTrace(e));
    } catch (NullPointerException e) {
      throw new TestException("Not able to execute procedure\n" + TestHelper.getStackTrace(e));
    } catch (AttributeNotFoundException e) {
      throw new TestException("Not able to execute procedure\n" + TestHelper.getStackTrace(e));
    } catch (InstanceNotFoundException e) {
      throw new TestException("Not able to execute procedure\n" + TestHelper.getStackTrace(e));
    } catch (MBeanException e) {
      throw new TestException("Not able to execute procedure\n" + TestHelper.getStackTrace(e));
    } catch (ReflectionException e) {
      throw new TestException("Not able to execute procedure\n" + TestHelper.getStackTrace(e));
    } catch (IOException e) {
      throw new TestException("Not able to execute procedure\n" + TestHelper.getStackTrace(e));
    }
  }

  private void checkGfxdMBeans(MBeanServerConnection mbeanServer, boolean verifyMBeans, boolean verifyAggregatedMBeans) {
    checkStatementMBeans(mbeanServer);
    checkMemberMBeans(mbeanServer, verifyMBeans, verifyAggregatedMBeans);
    checkTableMBeans(mbeanServer, verifyMBeans, verifyAggregatedMBeans);
  }
  
  private void checkGfxdMBeans(MBeanServerConnection mbeanServer, boolean verifyAtEnd) {
    checkStatementMBeans(mbeanServer);
    checkMemberMBeans(mbeanServer, verifyAtEnd);
    checkTableMBeans(mbeanServer, verifyAtEnd);
  }

  private void checkMemberMBeans(MBeanServerConnection mbeanServer, boolean verifyAtEnd) {
    checkMemberMBeans(mbeanServer, verifyAtEnd, false);
  }
  private void checkMemberMBeans(MBeanServerConnection mbeanServer, boolean verifyMBeans, boolean verifyAggregatedMBean) {
    
  try {
    String[] members = getGfxdClusterMembers(mbeanServer);
    Log.getLogWriter().info("Member set JMX : " + HydraUtil.ObjectToString(members));
    Log.getLogWriter().info("Member set API : " + getMembersSet());
    if (members.length != getMembersSet().size() && !MBeanPrms.isHATest()) {
      saveError("Member set on cluster mbean is wrong jmx " + HydraUtil.ObjectToString(members) + " API : " + HydraUtil.ObjectToString(getMembersSet()));
      throw new TestException("Member set on cluster mbean is wrong jmx " + HydraUtil.ObjectToString(members) + " API : " + HydraUtil.ObjectToString(getMembersSet()));
    }

    ObjectName name = null;
    if (MBeanPrms.isWANTest() || isHDFSTest() || isEdge) {
      // No server groups in WAN test
      name = new ObjectName(MessageFormat.format(ManagementConstants.OBJECTNAME__GFXDMEMBER_MXBEAN, new Object[] {
              DEFAULT_SERVER_GROUP, ManagementUtil.getMemberID() }));
      boolean isLocator = (Boolean) mbeanServer.getAttribute(name, "Locator");
      if (isLocator) {
        throw new TestException("isLocator expected false");
      }
    } else {
      name = new ObjectName(MessageFormat.format(
          ManagementConstants.OBJECTNAME__GFXDMEMBER_MXBEAN, new Object[] {
              "CG", ManagementUtil.getMemberID() }));
      boolean isLocator = (Boolean) mbeanServer.getAttribute(name, "Locator");
      if (isLocator) {
        throw new TestException("isLocator expected false");
      }
    }

    AttributeList list = mbeanServer.getAttributes(name, MEMBER_ATTRS);
    Log.getLogWriter().info("GFXDMEMBER Attributes " + HydraUtil.ObjectToString(list));
    int timesProcCalled = getCounter(COUNTER_MBEAN_PROC_CALLS);
    Map<String, Object> expectedMap = new HashMap<String, Object>();
    expectedMap.put("Name", ManagementUtil.getMember().getName());
    expectedMap.put("Id", ManagementUtil.getMember().getId());
  //  expectedMap.put("Groups", value);
    expectedMap.put("DataStore", GemFireStore.getBootedInstance().getMyVMKind().isStore());
    if(verifyMBeans) {
      if(!isEdge) {
        expectedMap.put("ProcedureCallsInProgress", getCounter(COUNTER_MBEAN_PROC_CALLS_IN_PROGRESS));
        expectedMap.put("ProcedureCallsCompleted", getCounter(COUNTER_MBEAN_PROC_CALLS));
      }
    }
    mbeanHelper.matchValues(list, expectedMap);



    
    if(verifyMBeans)
    {
      sleepForDataUpdaterJMX();
      List<Object> connStat = getConnStatForInternalAndNested();
      if(connStat.size() > 0) {
        Log.getLogWriter().info("conn state fetch from stats is : " + connStat + "");
        saveCounter(COUNTER_NS_PEER, connStat.get(0));
        saveCounter(COUNTER_NS_CLIENT, connStat.get(1));
        saveCounter(COUNTER_NS_NESTED, connStat.get(2));
        saveCounter(COUNTER_NS_INTERNAL, connStat.get(3));
      } 
      Log.getLogWriter().info("Verifying network connection stat...");
      CompositeDataSupport clientConnStats = (CompositeDataSupport)mbeanServer.getAttribute(name, "NetworkServerClientConnectionStats");
      CompositeDataSupport peerConnStats = (CompositeDataSupport)mbeanServer.getAttribute(name, "NetworkServerPeerConnectionStats");
      CompositeDataSupport nestedConnStats = (CompositeDataSupport)mbeanServer.getAttribute(name, "NetworkServerNestedConnectionStats");
      CompositeDataSupport internalConnStats = (CompositeDataSupport)mbeanServer.getAttribute(name, "NetworkServerInternalConnectionStats");
      if(connStat.size() > 0) {
        compare((NetworkServerConnectionStats)connStat.get(0), peerConnStats, true);
        compare((NetworkServerConnectionStats)connStat.get(1), clientConnStats, true);
        compare((NetworkServerNestedConnectionStats)connStat.get(2), nestedConnStats, true);
        compare((NetworkServerNestedConnectionStats)connStat.get(3), internalConnStats, true);
      }
    }
    
    getMemberStats();
    //compare(getMemberStats(), internalConnStats);
    //compare(, nestedConnStats);
    //Viren : not needed, we are already doing it
//    if (timesProcCalled > 0)
//      mbeanHelper.ensureNonZero(list, new String[] { "ProcedureCallsCompleted" });
      if (list.size() != MEMBER_ATTRS.length) {
        throw new TestException("Wrong list size");
      }

      SharedLock lock = MBeanTaskBB.getBB().getSharedLock();
      for (String method : MEMBER_METHODS) {
        Object result = null;
        if (method.startsWith("update")) {
          
          lock.lock();
          callUpdateFunctionForMember(mbeanServer, name, method);
          lock.unlock();
        } else {
          if (method.startsWith("fetch") && !"fetchMetadata".equals(method)) {
            lock.lock();
            result = mbeanServer.invoke(name, method, null, null);  
            callFetchFunctionForMember(method, result);
            lock.unlock();
          }
          if (!"detectDeadlocks".equals(method)) {
            Log.getLogWriter().info("GFXDMEMBER Op " + method + " : " + HydraUtil.ObjectToString(result));
          }
          
        }
      }

      /*
      if (verifyAggregatedMBean) {
        validateAggregatedMemberMBean(mbeanServer);
      }*/
      
    } catch (Exception e) {
      throw new TestException("Error while queryMBeans\n" + TestHelper.getStackTrace(e));
    }
  }

  private List<Object> getConnStatForInternalAndNested() {
    List<Object> toReturn = new ArrayList<Object>(4);
    GemFireCacheImpl cache = Misc.getGemFireCacheNoThrow();
    if (cache != null) {
      InternalDistributedSystem system = cache.getDistributedSystem();
      StatisticsType connectionStatsType = system.findType(ConnectionStats.name);
      if (connectionStatsType != null) {
        Statistics[] foundStatistics = system.findStatisticsByType(connectionStatsType);
       // printConnectionStats(foundStatistics[0]);
        for (Statistics statistics : foundStatistics) {
          Log.getLogWriter().info("Found the stats for Network Server connection... : " + statistics);
//          printConnectionStats(statistics);

          //TODO:Viren, it can also be done by checking connection instance of
          // [soubhik] these ideally shouldn't be hardcoded like this.
          toReturn.add(new NetworkServerConnectionStats("Peer", 
              statistics.getLong("peerConnectionsOpened"),
              statistics.getLong("peerConnectionsClosed"),
              statistics.getLong("peerConnectionsAttempted"),
              statistics.getLong("peerConnectionsFailed"),
              statistics.getLong("peerConnectionsLifeTime"),
              statistics.getLong("peerConnectionsOpen"),
              0 ));
          toReturn.add(new NetworkServerConnectionStats("Client",
              statistics.getLong("clientConnectionsOpened"),
              statistics.getLong("clientConnectionsClosed"),
              statistics.getLong("clientConnectionsAttempted"),
              statistics.getLong("clientConnectionsFailed"),
              statistics.getLong("clientConnectionsLifeTime"),
              statistics.getLong("clientConnectionsOpen"),
              statistics.getLong("clientConnectionsIdle") ));
          
          
          toReturn.add(new NetworkServerNestedConnectionStats("Nested",
              statistics.getLong("nestedConnectionsOpened"),
              statistics.getLong("nestedConnectionsClosed"),
              statistics.getLong("nestedConnectionsOpen")));
          toReturn.add(new NetworkServerNestedConnectionStats("Internal",
              statistics.getLong("internalConnectionsOpened"),
              statistics.getLong("internalConnectionsClosed"),
              statistics.getLong("internalConnectionsOpen")));
        }
      }
    }
    if(toReturn.isEmpty()) {
      Log.getLogWriter().error("Did not able to get any conn stat for nested and internal. validation for these stat won't match.");
    }
    
    return toReturn;
  }


  private void callFetchFunctionForMember(String method, Object result) {
    Number expected = getCounterNumber(ManagementUtil.getMemberID() + "_" + method.substring(5));
    Log.getLogWriter().info("GFXDMEMBER Op " + method + " : Expected  : " + expected.floatValue() + ". and actual : "
            + HydraUtil.ObjectToString(result));
    if ((result == null && expected.floatValue() > 0) || expected.floatValue() != ((Float) result).floatValue()) {
      // throw new TestException(method + " value did not match.");
      saveError("GFXDMEMBER Op " + method + " : Expected  : " + expected.floatValue() + ". and actual : " + HydraUtil.ObjectToString(result));
    }
  }

  private void callUpdateFunctionForMember(MBeanServerConnection mbeanServer, ObjectName name, String method) throws Exception {

    float percent = TestConfig.tab().getRandGen().nextFloat() * HydraUtil.getRandomElement(new Integer[] { -1, 10, 50, 100, 1000});
    //TODO:Viren, remove it 
    if(percent < 50) {
      Log.getLogWriter().info("Trying to set " +  percent + " which is less than 50, returning from here");  
      return;
    }
    Log.getLogWriter().info("Trying to set " + method + " with value : " + percent);
    try {
      if(!handleIfCriticalIsLessThanEviction(mbeanServer, name, method, percent)) {
        Log.getLogWriter().info("Evicition is greater than critical, cannot do anything , returning");
        return;
      }
      Log.getLogWriter().info("Now we can set the eviction or critical percentage to " + percent);
      setEvicitionOrCriticalPercentage(mbeanServer, name, method, percent);
    } catch (RuntimeMBeanException e) {
      if (percent > 0.0 && percent <= 100.0) {
        printAndGetEvictionPercent(mbeanServer, name);
        printAndGetCriticalPercent(mbeanServer, name);
        throw new TestException(method + " trying to set value : " + percent + " and it should not throw any exception", e);
      }
      return;
    }
    if (percent < 60.0f) {
      Log.getLogWriter().info(percent + " is less than 60 so setting it to 100 for " + method);
      percent = 100.0f; 
      if("updateEvictionPercent".equals(method)) {
        Log.getLogWriter().info("Resetting value of critical percentage because we are resetting eviction percent to 100");
        setCriticalPercent(mbeanServer, name, percent);
      }
      percent  = percent - 1;
      setEvicitionOrCriticalPercentage(mbeanServer, name, method, percent);
    }
    if(percent <= 100.0f) {
      saveCounter(ManagementUtil.getMemberID() + "_" + method.substring(6), percent);
      Log.getLogWriter().info("GFXDMEMBER Op " + method + " updated value  : " + percent);
    }
  }

  private void setEvicitionOrCriticalPercentage(
      MBeanServerConnection mbeanServer, ObjectName name, String method,
      float percent) throws InstanceNotFoundException, MBeanException,
      ReflectionException, IOException {
    try {
      Log.getLogWriter().info("Trying to set " + method + " with value : " + percent);
      mbeanServer.invoke(name, method, new Object[] { percent }, new String[] { float.class.getName() });
    } catch(RuntimeMBeanException e) {
      Log.getLogWriter().info(e.getMessage());
      throw e;
    } finally {
      printAndGetEvictionPercent(mbeanServer, name);
      printAndGetCriticalPercent(mbeanServer, name);
    }
  }

  private boolean handleIfCriticalIsLessThanEviction(MBeanServerConnection mbeanServer, ObjectName name, String method, float percent) throws InstanceNotFoundException, MBeanException, ReflectionException, IOException {
    
    if("updateCriticalPercent".equals(method)) {
      
      Number evictionPercent = printAndGetEvictionPercent(mbeanServer, name);//getCounterNumber(ManagementUtil.getMemberID() + "_" + "EvictionPercent");
      Log.getLogWriter().info("Current Eviction Percent Is : " + evictionPercent + " and Percent to set in Critical is : " + percent);
      if(percent < evictionPercent.floatValue()) {
        return setEvictionPercent(mbeanServer, name, percent - 1.0f);
      }
    }
    if("updateEvictionPercent".equals(method)) {
      Number criticalPercent = printAndGetCriticalPercent(mbeanServer, name);//(ManagementUtil.getMemberID() + "_" + "CriticalPercent");
      Log.getLogWriter().info("Current Critical Percent is : " + criticalPercent + " and Percent to set in Eviction is : " + percent);
      if(criticalPercent.floatValue() == 0.0) {
        Log.getLogWriter().info("Current Critical Percent is : " + criticalPercent + " returning form here");  
        return false;
      }
      if(percent > criticalPercent.floatValue()) {
        return false;
      }
    }
    return true;
  }

  private boolean setEvictionPercent(MBeanServerConnection mbeanServer,
      ObjectName name, float percentToSet) throws InstanceNotFoundException,
      MBeanException, ReflectionException, IOException {
    Log.getLogWriter().info("Trying to set new percent to eviction percent : " + (percentToSet));
    try {
      mbeanServer.invoke(name, "updateEvictionPercent", new Object[] { (percentToSet) }, new String[] { float.class.getName() });
      saveCounter(ManagementUtil.getMemberID() + "_" + "EvictionPercent", (percentToSet));
      return true;
    } catch (RuntimeException e) {
      Log.getLogWriter().info("We cannot set "+ (percentToSet) + " to the eviction percent, Reason : " + e.getMessage());
      return false;
    } catch(Exception e) {
      Log.getLogWriter().info("(Exception)We cannot set "+ (percentToSet) + " to the eviction percent, Reason : " + e.getMessage());
      return false;
    } finally {
      printAndGetEvictionPercent(mbeanServer, name);
    }
  }

  private float printAndGetEvictionPercent(MBeanServerConnection mbeanServer,
      ObjectName name) throws InstanceNotFoundException, MBeanException,
      ReflectionException, IOException {
    return printAndGetXXXPercent(mbeanServer, name, "fetchEvictionPercent");
  }

  

  private float printAndGetCriticalPercent(MBeanServerConnection mbeanServer,
      ObjectName name) throws InstanceNotFoundException, MBeanException,
      ReflectionException, IOException {
    return printAndGetXXXPercent(mbeanServer, name, "fetchCriticalPercent");
  }
  



  private boolean setCriticalPercent(MBeanServerConnection mbeanServer,
      ObjectName name, float percentToSet) throws InstanceNotFoundException,
      MBeanException, ReflectionException, IOException {
    Log.getLogWriter().info("Trying to set new percent to critical percent : " + (percentToSet));
    try {
      mbeanServer.invoke(name, "updateCriticalPercent", new Object[] { (percentToSet) }, new String[] { float.class.getName() });
      saveCounter(ManagementUtil.getMemberID() + "_" + "CriticalPercent", (percentToSet));
      return true;
    } catch (RuntimeException e) {
      Log.getLogWriter().info("We cannot set "+ (percentToSet) + " to the critical percent, Reason : " + e.getMessage());
      return false;
    } finally {
      printAndGetCriticalPercent(mbeanServer, name);
    }
  }

  private float printAndGetXXXPercent(MBeanServerConnection mbeanServer,
      ObjectName name, String methodName) throws InstanceNotFoundException, MBeanException,
      ReflectionException, IOException {
    Object value = mbeanServer.invoke(name, methodName, null, null);
    Log.getLogWriter().info("Current " + methodName + " Percent is : " + value + "");
    return ((Number)value).floatValue();
  }
  
  private void validateAggregatedMemberMBean() {
    
    try {
      
      sleepForDataUpdaterJMX();
      lockReadManager();
      MBeanServerConnection mbeanServer = connectJMXConnector();

      AttributeList list;

      ObjectName aggregateName = new ObjectName(ManagementConstants.OBJECTNAME__AGGREGATEMEMBER_MXBEAN);
      list = mbeanServer.getAttributes(aggregateName, AGGR_MEMBER_ATTRS);
      Log.getLogWriter().info("GFXDMEMBER Aggregated Attributes " + HydraUtil.ObjectToString(list));

      String[] members = getGfxdClusterMembers(mbeanServer);
      Set<DistributedMember> apiMemberSet = getMembersSet();
      if (members.length != apiMemberSet.size()) {
        throw new TestException("Member set on cluster mbean is wrong jmx " + HydraUtil.ObjectToString(members) + " API : " + HydraUtil.ObjectToString(apiMemberSet));
      }
      int aggregatedTimesProcCalled = 0;
      int aggregatedTimesProcInProgress = 0;
      NetworkServerConnectionStats aggregatedClientConnStat = new NetworkServerConnectionStats("Client", 0, 0, 0, 0, 0, 0, 0);
      NetworkServerConnectionStats aggregatedPeerConnStat = new NetworkServerConnectionStats("Peer", 0, 0, 0, 0, 0, 0, 0);
      NetworkServerNestedConnectionStats aggregatedNestedConnStat = new NetworkServerNestedConnectionStats("Nested", 0, 0, 0);
      NetworkServerNestedConnectionStats aggregatedInternalConnStat = new NetworkServerNestedConnectionStats("Internal", 0, 0, 0);
      for (String member : members) {
        Log.getLogWriter().info("Consolidating aggregated values for member : " + member);
        String memberName = member.substring(member.indexOf('(') + 1, member.indexOf(":"));
        @SuppressWarnings("unchecked")
        String clientName = ((Map<String, String>)SQLBB.getBB().getSharedMap().get(MEMBERS_MAPPING)).get(memberName);
        if(isEdge) {
          aggregatedTimesProcCalled = getCounter(COUNTER_MBEAN_PROC_CALLS_EDGE);
        } else {
          aggregatedTimesProcCalled += getCounter(COUNTER_MBEAN_PROC_CALLS_PREFIX + clientName);
        }
        Log.getLogWriter().info("aggregatedTimesProcCalled : " + aggregatedTimesProcCalled + " for clientName : " + clientName);
        aggregatedTimesProcInProgress += getCounter(COUNTER_MBEAN_PROC_PROGRESS_PREFIX + clientName);
        Log.getLogWriter().info("aggregatedTimesProcInProgress : " + aggregatedTimesProcInProgress+ " for clientName : " + clientName);
        NetworkServerConnectionStats memberNCC = (NetworkServerConnectionStats) SQLBB.getBB().getSharedMap().get("COUNTER_NS_CLIENT_" + clientName);
        Log.getLogWriter().info("Conn Stat for Member : " + member + ", stat : " + memberNCC + " for Client");
        aggregatedClientConnStat.updateNetworkServerConnectionStats(
            aggregatedClientConnStat.getConnectionsOpened() + memberNCC.getConnectionsOpened(),
            aggregatedClientConnStat.getConnectionsClosed() + memberNCC.getConnectionsClosed(),
            aggregatedClientConnStat.getConnectionsAttempted() + memberNCC.getConnectionsAttempted(),
            aggregatedClientConnStat.getConnectionsFailed() + memberNCC.getConnectionsFailed(),
            aggregatedClientConnStat.getConnectionLifeTime() + memberNCC.getConnectionLifeTime(),
            aggregatedClientConnStat.getConnectionsOpen() + memberNCC.getConnectionsOpen(),
            aggregatedClientConnStat.getConnectionsIdle() + memberNCC.getConnectionsIdle());
        
        NetworkServerConnectionStats peerNCC = (NetworkServerConnectionStats) SQLBB.getBB().getSharedMap().get("COUNTER_NS_PEER_" + clientName);
        Log.getLogWriter().info("Conn Stat for Member : " + member + ", stat : " + peerNCC + " for Peer");
        aggregatedPeerConnStat.updateNetworkServerConnectionStats(
            aggregatedPeerConnStat.getConnectionsOpened() + peerNCC.getConnectionsOpened(),
            aggregatedPeerConnStat.getConnectionsClosed() + peerNCC.getConnectionsClosed(),
            aggregatedPeerConnStat.getConnectionsAttempted() + peerNCC.getConnectionsAttempted(),
            aggregatedPeerConnStat.getConnectionsFailed() + peerNCC.getConnectionsFailed(),
            aggregatedPeerConnStat.getConnectionLifeTime()+ peerNCC.getConnectionLifeTime(),
            aggregatedPeerConnStat.getConnectionsOpen() + peerNCC.getConnectionsOpen(),
            aggregatedPeerConnStat.getConnectionsIdle() + peerNCC.getConnectionsIdle());
        
        NetworkServerNestedConnectionStats nestedNCC = (NetworkServerNestedConnectionStats) SQLBB.getBB().getSharedMap().get("COUNTER_NS_NESTED_" + clientName);
        Log.getLogWriter().info("Conn Stat for Member : " + member + ", stat : " + nestedNCC + " for Nested");
        aggregatedNestedConnStat.updateNetworkServerConnectionStats(
            aggregatedNestedConnStat.getConnectionsOpened() + nestedNCC.getConnectionsOpened(),
            aggregatedNestedConnStat.getConnectionsClosed() + nestedNCC.getConnectionsClosed(),
            aggregatedNestedConnStat.getConnectionsActive() + nestedNCC.getConnectionsActive());

        NetworkServerNestedConnectionStats internalNCC = (NetworkServerNestedConnectionStats) SQLBB.getBB().getSharedMap().get("COUNTER_NS_INTERNAL_" + clientName);
        Log.getLogWriter().info("Conn Stat for Member : " + member + ", stat : " + internalNCC + "for Internal");
        aggregatedInternalConnStat.updateNetworkServerConnectionStats(
            aggregatedInternalConnStat.getConnectionsOpened() + internalNCC.getConnectionsOpened(),
            aggregatedInternalConnStat.getConnectionsClosed() + internalNCC.getConnectionsClosed(),
            aggregatedInternalConnStat.getConnectionsActive() + internalNCC.getConnectionsActive());

      }
    
      CompositeDataSupport clientConnStat = (CompositeDataSupport) mbeanServer.getAttribute(aggregateName, "NetworkServerClientConnectionStats");
      Log.getLogWriter().info("Aggregated NetworkServerClientConnectionStats : Expected : "+ aggregatedClientConnStat + ", Actual : "+ clientConnStat);
      compare(aggregatedClientConnStat, clientConnStat, true, true);
      CompositeDataSupport peerConnStat = (CompositeDataSupport) mbeanServer.getAttribute(aggregateName, "NetworkServerPeerConnectionStats");
      Log.getLogWriter().info("Aggregated NetworkServerPeerConnectionStats : Expected : " + aggregatedPeerConnStat + ", Actual : " + peerConnStat);
      compare(aggregatedPeerConnStat, peerConnStat, true, true);
      
      CompositeDataSupport nestedConnStat = (CompositeDataSupport) mbeanServer.getAttribute(aggregateName, "NetworkServerNestedConnectionStats");
      Log.getLogWriter().info("Aggregated NetworkServerNestedConnectionStats : Expected : " + aggregatedNestedConnStat + ", Actual : " + nestedConnStat);
      compare(aggregatedNestedConnStat, nestedConnStat, true, true);

      CompositeDataSupport internalConnStat = (CompositeDataSupport) mbeanServer.getAttribute(aggregateName, "NetworkServerInternalConnectionStats");
      Log.getLogWriter().info("Aggregated NetworkServerInternalConnectionStats : Expected : " + aggregatedInternalConnStat + ", Actual : " + internalConnStat);
      //TODO: Viren, Its hard to test it because internally it running queries and that may change aggregate values.
      //We will see how we can do that.
     // compare(aggregatedInternalConnStat, internalConnStat, true, true);

      int procCallCompletedAttr = (Integer) mbeanServer.getAttribute(aggregateName, "ProcedureCallsCompleted");
      Log.getLogWriter().info("Aggregated ProcedureCallsCompleted : Expected : " + aggregatedTimesProcCalled + ", Actual : " + procCallCompletedAttr);
      compare(aggregatedTimesProcCalled, procCallCompletedAttr,  "ProcedureCallsCompleted", aggregateName);
      int procedureCallsInProgressAttr = (Integer) mbeanServer.getAttribute(aggregateName, "ProcedureCallsInProgress");
      Log.getLogWriter().info("Aggregated ProcedureCallsInProgress : Expected : " + aggregatedTimesProcInProgress + ", Actual : " + procedureCallsInProgressAttr);
      compare(aggregatedTimesProcInProgress, procedureCallsInProgressAttr,  "ProcedureCallsInProgress", aggregateName);
      closeJMXConnector();
    } catch (AttributeNotFoundException e) {
      throw new TestException("Error while validateAggregatedMemberMBean\n" + TestHelper.getStackTrace(e));
    } catch (MBeanException e) {
      throw new TestException("Error while validateAggregatedMemberMBean\n" + TestHelper.getStackTrace(e));
    } catch (MalformedObjectNameException e) {
      throw new TestException("Error while validateAggregatedMemberMBean\n" + TestHelper.getStackTrace(e));
    } catch (NullPointerException e) {
      throw new TestException("Error while validateAggregatedMemberMBean\n" + TestHelper.getStackTrace(e));
    } catch (InstanceNotFoundException e) {
      throw new TestException("Error while validateAggregatedMemberMBean\n" + TestHelper.getStackTrace(e));
    } catch (ReflectionException e) {
      throw new TestException("Error while validateAggregatedMemberMBean\n" + TestHelper.getStackTrace(e));
    } catch (IOException e) {
      throw new TestException("Error while validateAggregatedMemberMBean\n" + TestHelper.getStackTrace(e));
    } catch (Exception e) {
      throw new TestException("Error while validateAggregatedMemberMBean\n" + TestHelper.getStackTrace(e));
    }
    finally{
      unLockReadManager();
    }

  }

  private void compare(int expected, int actual, String method, ObjectName aggregateName) {
    if(expected != actual) {
      saveError(method  + " value did not match for " + aggregateName + ", Expected : " + expected + " and actual : " + actual);
    }
  }

  private void compare(NetworkServerConnectionStats expected, CompositeDataSupport actual, boolean actualCanBeGreater) {
    compare(expected, actual, actualCanBeGreater, false);
  }

  private void compare(NetworkServerConnectionStats expected, CompositeDataSupport actual, boolean actualCanBeGreater, boolean isAggregated) {
    if(expected == null || actual == null) {
      saveError(getAggregatePrefix(isAggregated) + "NetworkServerConnectionStats not match, one or both is null, Actual : " + actual + " and Expected : " + expected);      
    }

    String connectionStateType = (String)actual.get("connectionStatsType");
    
    if(!expected.getConnectionStatsType().equals(connectionStateType)) {
      saveError(getAggregatePrefix(isAggregated) + "NetworkServerConnectionStats ConnectionStatsType not match, Actual : "
          + connectionStateType
          + " and Expected : " + expected.getConnectionStatsType());
    }
    
    Log.getLogWriter().info("Verifing "  + getAggregatePrefix(isAggregated) + "Network Connection Stat of " + connectionStateType);
    Log.getLogWriter().info("Expected "  + getAggregatePrefix(isAggregated) + "Network conn stat : " + expected);
    Log.getLogWriter().info("Actual "  + getAggregatePrefix(isAggregated) + "Network conn stat : " + actual);
    long connectionsOpened = (Long)actual.get("connectionsOpened");
    if(actualCanBeGreater ? (expected.getConnectionsOpened() > connectionsOpened) : (expected.getConnectionsOpened() != connectionsOpened)) {
      saveError(getAggregatePrefix(isAggregated) + "NetworkServerConnectionStats connectionsOpened not match, Actual : " + connectionsOpened + " and Expected : " + expected.getConnectionsOpened());
      
    }
    long connectionsClosed = (Long)actual.get("connectionsClosed");
    if(actualCanBeGreater ? (expected.getConnectionsClosed() > connectionsClosed) : (expected.getConnectionsClosed() != connectionsClosed)) {
      saveError(getAggregatePrefix(isAggregated) +"NetworkServerConnectionStats connectionsClosed not match, Actual : " + connectionsClosed + " and Expected : " + expected.getConnectionsClosed());
      
    }
    long connectionsAttempted = (Long)actual.get("connectionsAttempted");
    if(actualCanBeGreater ? (expected.getConnectionsAttempted() > connectionsAttempted) : (expected.getConnectionsAttempted() != connectionsAttempted)) {
      saveError(getAggregatePrefix(isAggregated) +"NetworkServerConnectionStats connectionsAttempted not match, Actual : " + connectionsAttempted + " and Expected : " + expected.getConnectionsAttempted());
    }
    long connectionsFailed = (Long)actual.get("connectionsFailed");
    if(actualCanBeGreater ? (expected.getConnectionsFailed() > connectionsFailed) : (expected.getConnectionsFailed() != connectionsFailed)) {
      saveError(getAggregatePrefix(isAggregated) +"NetworkServerConnectionStats connectionsFailed not match, Actual : " + connectionsFailed + " and Expected : " + expected.getConnectionsFailed());
    }
    long connectionsOpen = (Long)actual.get("connectionsOpen");
    if(!actualCanBeGreater) {
      if(expected.getConnectionsOpen() != connectionsOpen) {
        saveError(getAggregatePrefix(isAggregated) +"NetworkServerConnectionStats connectionsOpen not match, Actual : "
            + connectionsOpen
            + " and Expected : " + expected.getConnectionsOpen());
      }
    }
    long connectionsIdle = (Long)actual.get("connectionsIdle");
    if(!actualCanBeGreater) {
      if(expected.getConnectionsIdle() != connectionsIdle) {
        saveError(getAggregatePrefix(isAggregated) +"NetworkServerConnectionStats connectionsIdle not match, Actual : "
            + connectionsIdle
            + " and Expected : " + expected.getConnectionsIdle());
      }
    }
  }

  private void compare(NetworkServerNestedConnectionStats expected, CompositeDataSupport actual, boolean actualCanBeGreater) {
    compare(expected, actual, actualCanBeGreater, false);
  }
  
  private void compare(NetworkServerNestedConnectionStats expected, CompositeDataSupport actual, boolean actualCanBeGreater, boolean isAggregated) {
    if(expected == null || actual == null) {
      saveError("NetworkServerNestedConnectionStats ConnectionStatsType not match, Actual : " + actual + " and Expected : " + expected);      
    }
    String connectionStateType = (String) actual.get("connectionStatsType");
    if (!expected.getConnectionStatsType().equals(connectionStateType)) {
      saveError(getAggregatePrefix(isAggregated) +  "NetworkServerNestedConnectionStats ConnectionStatsType not match, Actual : "
          + connectionStateType + " and Expected : "
          + expected.getConnectionStatsType());
    }
    
    Log.getLogWriter().info("Verifing " + getAggregatePrefix(isAggregated) + "Network Connection Stat of " + connectionStateType);
    Log.getLogWriter().info("Expected " + getAggregatePrefix(isAggregated) + "Network conn stat : " + expected);
    Log.getLogWriter().info("Actual " + getAggregatePrefix(isAggregated) + "Network conn stat : " + actual);
    
    long connectionsOpened = (Long) actual.get("connectionsOpened");
    if (actualCanBeGreater ? expected.getConnectionsOpened() > connectionsOpened : expected.getConnectionsOpened() != connectionsOpened) {
      saveError(
          getAggregatePrefix(isAggregated) + "NetworkServerNestedConnectionStats connectionsOpened not match, Actual : "
              + connectionsOpened + " and Expected : "
              + expected.getConnectionsOpened());

    }
    long connectionsClosed = (Long)actual.get("connectionsClosed");
    if(actualCanBeGreater ? expected.getConnectionsClosed() > connectionsClosed :  expected.getConnectionsClosed() != connectionsClosed) {
      saveError(getAggregatePrefix(isAggregated) + "NetworkServerNestedConnectionStats connectionsClosed not match, Actual : "
          + connectionsClosed
          + " and Expected : " + expected.getConnectionsClosed());
      
    }
    long connectionsActive = (Long)actual.get("connectionsActive");
    if(actualCanBeGreater ? expected.getConnectionsActive() > connectionsActive : expected.getConnectionsActive() != connectionsActive) {
      saveError(getAggregatePrefix(isAggregated) +"NetworkServerNestedConnectionStats connectionsActive not match, Actual : " + connectionsActive + " and Expected : " + expected.getConnectionsActive());
    }
  }

  private String getAggregatePrefix(boolean isAggregated) {
    return isAggregated ? "Aggregated " : "";
  }

  private boolean isHDFSTest() {
    try {
      boolean b = TestConfig.tab().booleanAt(SQLPrms.hasHDFS);
      return b;
    } catch (HydraConfigException e) {
      return false;
    }
  }

  private void checkTableMBeans(MBeanServerConnection mbeanServer, boolean verifyMBean, boolean verifyAggregatedMBean) {
    List<ClientVmInfo> vmInfoList = new ArrayList<ClientVmInfo>();
    ClientVmInfo vmInfo = new ClientVmInfo(RemoteTestModule.getMyVmid(), RemoteTestModule.getMyClientName(), RemoteTestModule.getMyLogicalHost());
    vmInfoList.add(vmInfo);
    List<String> defaultGrp = new ArrayList<String>();
    defaultGrp.add(DEFAULT_SERVER_GROUP);
    for (String table : tableList) {
      try {
        checkTableMBean(mbeanServer, true, vmInfoList, table, defaultGrp, verifyMBean, verifyAggregatedMBean);
      } catch (MalformedObjectNameException e) {
        throw new TestException("Error while verifiying Check Table Mbeans", e);
      } catch (AttributeNotFoundException e) {
        throw new TestException("Error while verifiying Check Table Mbeans", e);
      } catch (NullPointerException e) {
        throw new TestException("Error while verifiying Check Table Mbeans", e);
      } catch (IOException e) {
        throw new TestException("Error while verifiying Check Table Mbeans", e);
      } catch (TestException e) {
        throw new TestException("Error while verifiying Check Table Mbeans", e);
      }
    }
  }
  private void checkTableMBeans(MBeanServerConnection mbeanServer, boolean verifyMBean) {
    checkTableMBeans(mbeanServer, verifyMBean, false);
  }

  private String[] getGfxdClusterMembers(MBeanServerConnection mbeanServer) throws Exception {
    ObjectName name = new ObjectName(ManagementConstants.OBJECTNAME__CLUSTER_MXBEAN);
    String[] members = (String[]) mbeanServer.getAttribute(name, "Members");
    return members;
  }

  private void checkStatementMBeans(MBeanServerConnection mbeanServer)  {
    Statistics[] stats = getStatementMBeans(ManagementUtil.getMemberID());
    for(Statistics stat : stats) {
      if(stat.getTextId().toLowerCase().contains("sys.")) {
        Log.getLogWriter().info(stat.getTextId()  + " is of type sys. No need to validate this statement. ");
        return;
      }
      if(isLocator()) {
        validateForNoMBeanOnLocator(mbeanServer, stat, ManagementUtil.getMemberID());
        Log.getLogWriter().info("Member " + ManagementUtil.getMemberID() + " is Locator. So no need to verify over here.");
        return;
      }
      Log.getLogWriter().info("Verfiy Statement MBean for " + stat.getTextId());
      verfiyStat(mbeanServer, stat);
      Log.getLogWriter().info("Verfiy Aggregated Statement MBean for " + stat.getTextId());
      verifyAggregatedStatementMBean(mbeanServer, stat);
    }
  }

  private void validateForNoMBeanOnLocator(MBeanServerConnection mbeanServer, Statistics stat, String memberId) {
    ObjectName name = getStatementMBeanName(ManagementUtil.getMemberID(), stat.getTextId());
    try {
      AttributeList valueFromMBean = mbeanServer.getAttributes(name, STATEMENT_ATTRS);
      Log.getLogWriter().error(
          "No Attribute for statement should found on member : " + memberId
              + "because it is locator but found :" + valueFromMBean);
    } catch (InstanceNotFoundException e) {
    } catch (Exception e) {
      throw new TestException("verifyStatementMBean\n" + TestHelper.getStackTrace(e));      
    } 
  }

  private void verfiyStat(MBeanServerConnection mbeanServer, Statistics stat) {

    Log.getLogWriter().info("Trying to validate Statement Stat for : " + stat.getTextId());
    ObjectName name = getStatementMBeanName(ManagementUtil.getMemberID(), stat.getTextId());
    for(String attr : STATEMENT_ATTRS) {
      try {
        Number valueFromStats = 0;
        try {
         valueFromStats = stat.get(attr);
        } catch(IllegalArgumentException e) {
          Log.getLogWriter().error(attr  + "  did not found for Statement : " + stat.getTextId());  
          continue;
        }
        Object valueFromMBean = null;
        valueFromMBean = mbeanServer.getAttribute(name, attr);
        saveStatementMBean(stat.getTextId(), attr, valueFromMBean);
        Log.getLogWriter().info("Validating Statement MBean Attr : " + attr  + " for " + stat.getTextId() + ", Expected : " + valueFromStats + " And Actual :" + valueFromMBean);
        match("Error occurred while validating Statement MBean for Attr : " + attr + " for Statement " + stat.getTextId(), stat.get(attr),
            (Number) valueFromMBean);
      } catch (InstanceNotFoundException e) {
        saveError("Expecting statement to be present for " + stat.getTextId() + " but it is not present");
        return;
      } catch (MBeanException e) {
        throw new TestException("verifyStatementMBean\n" + TestHelper.getStackTrace(e));
      } catch (ReflectionException e) {
        throw new TestException("verifyStatementMBean\n" + TestHelper.getStackTrace(e));
      } catch (IOException e) {
        throw new TestException("verifyStatementMBean\n" + TestHelper.getStackTrace(e));
      } catch (AttributeNotFoundException e) {
        throw new TestException("verifyStatementMBean\n" + TestHelper.getStackTrace(e));      }
    }
    
  }

  private void saveStatementMBean(String textId, String attr, Object valueFromMBean) {
    @SuppressWarnings("unchecked")
    Map<String, Map<String, Object>> statementMap = (Map<String, Map<String, Object>>) SQLBB.getBB().getSharedMap().get(COUNTER_MBEAN_STATEMENT);
    Map<String, Object> attrMap = statementMap.get(textId);
    if(attrMap == null) {
      attrMap = new HashMap<String, Object>();
    }
    attrMap.put(attr, valueFromMBean);
    
    SharedLock lock = SQLBB.getBB().getSharedLock();
    try {
      lock.lock();
      SQLBB.getBB().getSharedMap().put(COUNTER_MBEAN_STATEMENT, statementMap);
    } finally {
      lock.unlock();
    }
  }


  
  private void match(String errorStringSuffix, Object expcted, Object actual) {
    if(!expcted.toString().equals(actual.toString())) {
      saveError(errorStringSuffix + " Expected : " + expcted + " And Actual : " + actual);
    }
  }

  private void verifyAggregatedStatementMBean(MBeanServerConnection mbeanServer, Statistics stat) {
    String[] attrs = {
      "NumTimesCompiled",
      "NumExecution",
      "NumExecutionsInProgress",
      "NumTimesGlobalIndexLookup",
      "NumRowsModified",
      "ParseTime",
      "BindTime",
      "OptimizeTime",
      "RoutingInfoTime",
      "GenerateTime",
      "TotalCompilationTime",
      "ExecutionTime",
      "ProjectionTime",
      "TotalExecutionTime",
      "RowsModificationTime",
      "QNNumRowsSeen",
      "QNMsgSendTime",
      "QNMsgSerTime",
      "QNRespDeSerTime"};
    Log.getLogWriter().info("Trying to validate Aggregate Statement Stat for : " + stat.getTextId());
    ObjectName name = getAggregatedStatementMBeanName(stat.getTextId());
    if(name != null) {
      for(String attr : attrs) {
       // Number valueFromStats = stat.get(attr);
        Object valueFromMBean = null;
        try {
          valueFromMBean = mbeanServer.getAttribute(name, attr);
          Log.getLogWriter().info("Validating Aggregated Statement MBean for " + stat.getTextId() + ", Expected : <NA> And Actual :" + valueFromMBean);
        } catch (AttributeNotFoundException e) {
          throw new TestException("verifyAggregatedStatementMBean\n" + TestHelper.getStackTrace(e));
        } catch (InstanceNotFoundException e) {
          throw new TestException("verifyAggregatedStatementMBean\n" + TestHelper.getStackTrace(e));
        } catch (MBeanException e) {
          throw new TestException("verifyAggregatedStatementMBean\n" + TestHelper.getStackTrace(e));
        } catch (ReflectionException e) {
          throw new TestException("verifyAggregatedStatementMBean\n" + TestHelper.getStackTrace(e));
        } catch (IOException e) {
          throw new TestException("verifyAggregatedStatementMBean\n" + TestHelper.getStackTrace(e));
        }
      }
    }

    if(name != null) {
      try {
        AttributeList list = mbeanServer.getAttributes(name, attrs);
        Log.getLogWriter().info("GFXD Aggregated Statement Attributes " + HydraUtil.ObjectToString(list));
        // NumTimesCompiled should not be zero
        // NumExecution should not be zero
        // TotalCompilationTime should not be zero
        // TotalExecutionTime should not be zero
      } catch (InstanceNotFoundException e) {
        throw new TestException("verifyAggregatedStatementMBean\n" + TestHelper.getStackTrace(e));
      } catch (ReflectionException e) {
        throw new TestException("verifyAggregatedStatementMBean\n" + TestHelper.getStackTrace(e));      
      } catch (IOException e) {
        throw new TestException("verifyAggregatedStatementMBean\n" + TestHelper.getStackTrace(e));
      }
    }
  }

  private void checkRegionCounter(MBeanServerConnection mbeanServer) {    
    checkDSRegionCounters(mbeanServer);
    Log.getLogWriter().info("checkRegionCounter  : OK");
  }

  private void checkDSRegionCounters(MBeanServerConnection mbeanServer) {
    Log.getLogWriter().info("checkDSRegionCounters  : ");
    ObjectName memberON = ManagementUtil.getLocalMemberMBeanON();
    try {
      String[] regionNames = (String[]) mbeanServer.invoke(memberON, "listRegions", null, null);
      for (String region : regionNames) {
        if (region.startsWith("/TRADE/") && !region.contains("_")) { //only app regions
          String objectName = distributedRegionONTemplate.replace("$1", region);
          ObjectName regionON = new ObjectName(objectName);
          String attributes[] = { "RegionType", "Members", "DiskReadsRate", "DiskWritesRate" };
          AttributeList list = mbeanServer.getAttributes(regionON, attributes);
          Log.getLogWriter().info(region + " Attributes List " + HydraUtil.ObjectToString(list));
          CompositeDataSupport rData = (CompositeDataSupport) mbeanServer.invoke(regionON, "listRegionAttributes",
              null, null);
          Log.getLogWriter().fine(region + " AttributesData " + HydraUtil.ObjectToString(rData));
          
          if (!MBeanPrms.isWANTest() && !isHDFSTest()) {
            boolean offHeapEnable = (Boolean) rData.get("enableOffHeapMemory");
            //TODO:Viren Disabling it
           // if (!offHeapEnable)
             // throw new TestException("Expected offHeapEnable=true");
          }          

        }
      }

    } catch (InstanceNotFoundException e) {
      throw new TestException("Error while getAttributes : checkDSRegionCounters\n" + TestHelper.getStackTrace(e));
    } catch (MBeanException e) {
      throw new TestException("Error while getAttributes : checkDSRegionCounters\n" + TestHelper.getStackTrace(e));
    } catch (ReflectionException e) {
      throw new TestException("Error while getAttributes : checkDSRegionCounters\n" + TestHelper.getStackTrace(e));
    } catch (IOException e) {
      throw new TestException("Error while getAttributes : checkDSRegionCounters\n" + TestHelper.getStackTrace(e));
    } catch (MalformedObjectNameException e) {
      throw new TestException("Error while getAttributes : checkDSRegionCounters\n" + TestHelper.getStackTrace(e));
    } catch (NullPointerException e) {
      throw new TestException("Error while getAttributes : checkDSRegionCounters\n" + TestHelper.getStackTrace(e));
    }

  }

  private void checkMemberCounters(MBeanServerConnection mbeanServer) {
    checkTxCounters(mbeanServer);
    checkOtherCounters(mbeanServer);
    Log.getLogWriter().info("checkMemberCounters  : OK");
  }

  private int getTotalRegionEntryCount(MBeanServerConnection mbeanServer) {
    String template = ManagementConstants.OBJECTNAME__DISTRIBUTEDSYSTEM_MXBEAN;
    try {
      ObjectName dsON = new ObjectName(template);
      String[] attributes = { "TotalRegionEntryCount"};
      AttributeList list = mbeanServer.getAttributes(dsON, attributes);
      Log.getLogWriter().info("checkDSCounters Attributes List " + HydraUtil.ObjectToString(list));
      return 0;
    } catch (InstanceNotFoundException e) {
      throw new TestException("Error while getAttributes : checkDSRegionCounters\n" + TestHelper.getStackTrace(e));
    } catch (ReflectionException e) {
      throw new TestException("Error while getAttributes : checkDSRegionCounters\n" + TestHelper.getStackTrace(e));
    } catch (IOException e) {
      throw new TestException("Error while getAttributes : checkDSRegionCounters\n" + TestHelper.getStackTrace(e));
    } catch (MalformedObjectNameException e) {
      throw new TestException("Error while getAttributes : checkDSRegionCounters\n" + TestHelper.getStackTrace(e));
    } catch (NullPointerException e) {
      throw new TestException("Error while getAttributes : checkDSRegionCounters\n" + TestHelper.getStackTrace(e));
    }
  }

  
  private void checkDSCounters(MBeanServerConnection mbeanServer) {
    Log.getLogWriter().info("checkDSCounters  : ");
    //"GemFire:service=System,name=$1,type=Distributed";
    String template = ManagementConstants.OBJECTNAME__DISTRIBUTEDSYSTEM_MXBEAN;
    String[] attributes = { 
        "TransactionCommitted",
        "TransactionRolledBack",
        "TotalHeapSize",
        "UsedHeapSize",
        "DiskReadsRate",
        "DiskWritesRate",
        "TotalRegionCount",
        "AverageReads",
        "AverageWrites"
    };

    String[] nonZeroAttrs = {
        "TotalHeapSize",
        "UsedHeapSize",
        "TotalRegionCount"
    };

    /*- These are diff counters so again the same issue as similar to RATE issue */
    Map<String, Object> expectedMap = new HashMap<String, Object>();
    expectedMap.put("TotalRegionCount", mbeanHelper.getTotalRegions());
    expectedMap.put("TransactionCommitted", mbeanHelper.getTotalTxCommits());
    expectedMap.put("TransactionRolledBack", mbeanHelper.getTotalTxRollbacks());
    

    try {
      ObjectName dsON = new ObjectName(template);
      AttributeList list = mbeanServer.getAttributes(dsON, attributes);
      Log.getLogWriter().info("checkDSCounters Attributes List " + HydraUtil.ObjectToString(list));
      //matchValues(list, expectedMap);
      mbeanHelper.ensureNonZero(list, nonZeroAttrs);
    } catch (InstanceNotFoundException e) {
      throw new TestException("Error while getAttributes : checkDSRegionCounters\n" + TestHelper.getStackTrace(e));
    } catch (ReflectionException e) {
      throw new TestException("Error while getAttributes : checkDSRegionCounters\n" + TestHelper.getStackTrace(e));
    } catch (IOException e) {
      throw new TestException("Error while getAttributes : checkDSRegionCounters\n" + TestHelper.getStackTrace(e));
    } catch (MalformedObjectNameException e) {
      throw new TestException("Error while getAttributes : checkDSRegionCounters\n" + TestHelper.getStackTrace(e));
    } catch (NullPointerException e) {
      throw new TestException("Error while getAttributes : checkDSRegionCounters\n" + TestHelper.getStackTrace(e));
    }
    Log.getLogWriter().info("checkDSCounters  : OK");
  }


  private void checkOtherCounters(MBeanServerConnection mbeanServer) {
    Log.getLogWriter().info("checkMemberCounters  : checkOtherCounters");
    ObjectName on = ManagementUtil.getLocalMemberMBeanON();
    String[] attributes = { "OffHeapFreeMemory", "OffHeapUsedMemory", "CpuUsage", "HostCpuUsage", "NumThreads",
        "LoadAverage", "TotalFileDescriptorOpen", "CurrentHeapSize", "DiskReadsRate", "DiskWritesRate", "AverageReads",
        "AverageWrites", "TotalRegionCount" };

    Map<String, Object> expectedMap = new HashMap<String, Object>();
    expectedMap.put("TotalRegionCount", mbeanHelper.getTotalRegions());

    try {
      AttributeList list = mbeanServer.getAttributes(on, attributes);
      Log.getLogWriter().info("Attributes List " + HydraUtil.ObjectToString(list));
      if (!(MBeanPrms.isWANTest() || isHDFSTest())) {
        if(MBeanPrms.offHeap()) {
          mbeanHelper.ensureNonZero(list, new String[] { "OffHeapFreeMemory", "OffHeapUsedMemory"});
        }
      }
      mbeanHelper.ensureNonZero(list, new String[] { "NumThreads", "TotalFileDescriptorOpen", "CurrentHeapSize" });

      // RATE : check diff and rate counters difficult to get in deterministic manner : DiskReadsRate,
      // DiskWritesRate, AverageReads, AverageWrites

    } catch (InstanceNotFoundException e) {
      throw new TestException("Error while getAttributes : checkOtherCounters\n" + TestHelper.getStackTrace(e));
    } catch (ReflectionException e) {
      throw new TestException("Error while getAttributes : checkOtherCounters\n" + TestHelper.getStackTrace(e));
    } catch (IOException e) {
      throw new TestException("Error while getAttributes : checkOtherCounters\n" + TestHelper.getStackTrace(e));
    }
  }

  private void checkTxCounters(MBeanServerConnection mbeanServer) {
    Log.getLogWriter().info("checkMemberCounters  : txAttributes");
    ObjectName on = ManagementUtil.getLocalMemberMBeanON();
    String[] txAttributes = { "TransactionCommittedTotalCount","TransactionRolledBackTotalCount"};
    int txCommits = getCounter(COUNTER_SELLORDER_COMMITS);
    int txRollBacks = getCounter(COUNTER_SELLORDER_ROLLBACKS);
    Map<String, Number> expectedMap = new HashMap<String, Number>();
    expectedMap.put("TransactionCommittedTotalCount", txCommits);

    expectedMap.put("TransactionRolledBackTotalCount", txRollBacks);
    try {
      AttributeList list = mbeanServer.getAttributes(on,txAttributes );

      CachePerfStats stats = GemFireCacheImpl.getInstance().getCachePerfStats();
      Log.getLogWriter().info("From stats rollbacks : " + stats.getTxRollbacks() + " commits : " + stats.getTxCommits());
      // TODO:Viren some issue is coming when comparing the above value from counter, bypassing that
      expectedMap.put("TransactionRolledBackTotalCount", stats.getTxRollbacks());
      expectedMap.put("TransactionCommittedTotalCount", stats.getTxCommits());
      mbeanHelper.compareValues(list, expectedMap, true);
    } catch (InstanceNotFoundException e) {
      throw new TestException("Error while getAttributes : txAttributes\n" + TestHelper.getStackTrace(e));
    } catch (ReflectionException e) {
      throw new TestException("Error while getAttributes : txAttributes\n" + TestHelper.getStackTrace(e));
    } catch (IOException e) {
      throw new TestException("Error while getAttributes : txAttributes\n" + TestHelper.getStackTrace(e));
    }
  }


  private void lockWriteManager(){
    if(!useLocks) 
      throw new TestException("MRWL : Test is attmpting locking manager without configuring use of locks");
    Log.getLogWriter().info("MRWL : WriteLocking manager from thread " + RemoteTestModule.getCurrentThread().getThreadId());    
    int readCount,writeCount = 0;
    SharedLock lock = SQLBB.getBB().getSharedLock();
    lock.lock();
    Integer readers = (Integer) SQLBB.getBB().getSharedMap().get(MANAGER_READERS);
    Integer writers = (Integer) SQLBB.getBB().getSharedMap().get(MANAGER_WRITERS);
   
    if (readers == null)
      readCount = 0;
    else
      readCount = readers.intValue();
    
    if (writers == null)
      writeCount = 0;
    else
      writeCount = writers.intValue();
    
    Log.getLogWriter().info("MRWL : Read Count is "  +readCount +  " Writecount is " + writeCount);
    
    if(readCount<=0){ //TODO : Find out why sometimes count is negative : tushar
      Log.getLogWriter().info("Read Count is <= Zero. Increasing write count...");
      writeCount++;
      SQLBB.getBB().getSharedMap().put(MANAGER_WRITERS, writeCount);
      lock.unlock();
    } else{      
      Log.getLogWriter().info("MRWL : Manager read operation undergoing, waiting ....");
      boolean readopCompleted = false;
      while (!readopCompleted) {
        Log.getLogWriter().info("MRWL : Sleeping for manager read operation to complete. readCount " + readers.intValue());
        lock.unlock();
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {          
          e.printStackTrace();
        }
        lock.lock();
        readers = (Integer) SQLBB.getBB().getSharedMap().get(MANAGER_READERS);
        readopCompleted = readers.intValue() == 0;        
      }
      Log.getLogWriter().info("MRWL : Manager read operation completed");
      writeCount++;
      SQLBB.getBB().getSharedMap().put(MANAGER_WRITERS, writeCount);
      Log.getLogWriter().info("Read Count is Zero. Increasing write count...");
      lock.unlock();
    }
    Log.getLogWriter().info("MRWL : Manager WriteLock acquired from thread " + RemoteTestModule.getCurrentThread().getThreadId());    
  }
  
  private void unLockWriteManager(){
    if(!useLocks) 
      throw new TestException("MRWL : Test is attmpting locking manager without configuring use of locks");
    Log.getLogWriter().info("MRWL : UnLockingWrite manager from thread " + RemoteTestModule.getCurrentThread().getThreadId());
    int writeCount = 0;
    SharedLock lock = SQLBB.getBB().getSharedLock();
    lock.lock();
    Integer writers = (Integer) SQLBB.getBB().getSharedMap().get(MANAGER_WRITERS);
    writeCount = writers.intValue();
    writeCount--;
    SQLBB.getBB().getSharedMap().put(MANAGER_WRITERS, writeCount);
    lock.unlock();
    Log.getLogWriter().info("MRWL : Manager WriteLock unlocked from thread " + RemoteTestModule.getCurrentThread().getThreadId());    
  }  
  
  private void lockReadManager() {
    if (useLocks) {
      Log.getLogWriter().info("MRWL : ReadLocking manager from thread " + RemoteTestModule.getCurrentThread().getThreadId());
      int readCount, writeCount = 0;
      SharedLock lock = SQLBB.getBB().getSharedLock();
      lock.lock();
      Integer readers = (Integer) SQLBB.getBB().getSharedMap().get(MANAGER_READERS);
      Integer writers = (Integer) SQLBB.getBB().getSharedMap().get(MANAGER_WRITERS);

      if (readers == null)
        readCount = 0;
      else
        readCount = readers.intValue();

      if (writers == null)
        writeCount = 0;
              else
        writeCount = writers.intValue();

      Log.getLogWriter().info("MRWL : Read Count is " + readCount + " Writecount is " + writeCount);

      if (writeCount <= 0) {
        Log.getLogWriter().info("Read Count is " + readCount + " increasing readCount");
        readCount++;
        SQLBB.getBB().getSharedMap().put(MANAGER_READERS, readCount);
        lock.unlock();
      } else {

        Log.getLogWriter().info("MRWL : Manager write operation undergoing, waiting ....");
        boolean writeOpComplete = false;
        while (!writeOpComplete) {
          lock.unlock();
          Log.getLogWriter().info(
              "MRWL : Sleeping for manager write operation to complete writers " + writers.intValue());
          try {
            Thread.sleep(500);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          lock.lock();
          writers = (Integer) SQLBB.getBB().getSharedMap().get(MANAGER_WRITERS);
          writeOpComplete = writers.intValue() == 0;
        }
        Log.getLogWriter().info("MRWL : Manager write operation completed");
        readCount++;
        SQLBB.getBB().getSharedMap().put(MANAGER_READERS, readCount);
        Log.getLogWriter().info("MRWL : Write Count is Zero. Increasing read count...");
        lock.unlock();
      }
      Log.getLogWriter().info(
          "MRWL : Manager ReadLock acquired from thread " + RemoteTestModule.getCurrentThread().getThreadId());
        }
      }
  
  private void unLockReadManager() {
    if (useLocks) {
      Log.getLogWriter().info("MRWL : UnLockingRead manager from thread " + RemoteTestModule.getCurrentThread().getThreadId());
      int readCount = 0;
      SharedLock lock = SQLBB.getBB().getSharedLock();
      lock.lock();
      Integer readers = (Integer) SQLBB.getBB().getSharedMap().get(MANAGER_READERS);
      readCount = readers.intValue();
      readCount--;
      SQLBB.getBB().getSharedMap().put(MANAGER_READERS, readCount);
      lock.unlock();
      Log.getLogWriter().info(
          "MRWL : Manager ReadLock unlocked from thread " + RemoteTestModule.getCurrentThread().getThreadId());
    }
  }

  private synchronized MBeanServerConnection connectJMXConnector() {
    Log.getLogWriter().info("connectJMXConnector : Start");
    boolean isManagerRunning = getCounter(MANAGER_RUNNING) > 0;
    Log.getLogWriter().info("isManagerRunning : " + isManagerRunning + " retries " + connectorRetries);
    if (!isManagerRunning) {

      if (isThisManagerVM()) {
        return null; // force all task running on the vm to skip verification
      }

      if (connectorRetries.get() >= MAX_RETRIES){
        throw new TestException("MAX_RETRIES exceeded while waiting for manager");
      }
      connectorTL.set(null);
      Log.getLogWriter().info("Sleeping for some time till manager comes online");
      MBeanPrms.waitForManager();
      connectorRetries.incrementAndGet();
      return connectJMXConnector();
    }

    try {
      JMXConnector connector = (JMXConnector) connectorTL.get();
      MBeanServerConnection conn = null;
      if (connector == null) {
        String host = InetAddress.getLocalHost().getHostName();
        int port = getJMXPort();
        Log.getLogWriter().info("Creating new JMX-RMI Connection on port " + port);
        JMXServiceURL serviceUrl = new JMXServiceURL(MessageFormat.format(JMX_URL_FORMAT, host, String.valueOf(port)));
        connector = JMXConnectorFactory.connect(serviceUrl);
        connectorTL.set(connector);
        connectorRetries.set(0);
        conn = connector.getMBeanServerConnection();
      } else {
        connectorRetries.set(0);
        conn = connector.getMBeanServerConnection();
        //to cause network call so that stale connections are reset
        conn.getDomains();
      }
      return conn;
    } catch (IOException e) {
      connectorTL.set(null);
      if (!isManagerRunning) {
        if (connectorRetries.get() >= MAX_RETRIES)
          throw new TestException("Not able to connect to jmx-manager\n" + TestHelper.getStackTrace(e));
      } else {
        Log.getLogWriter().info("Sleeping for some time till manager comes online");
        MBeanPrms.waitForManager();
        connectorRetries.incrementAndGet();
        return connectJMXConnector();
      }
      throw new TestException("Not able to connect to jmx-manager\n" + TestHelper.getStackTrace(e));
    }
  }

  private void closeJMXConnector() {
    try {
      if(connectorTL.get() == null) {
        return;
      }
      JMXConnector connector = (JMXConnector) connectorTL.get();
      connector.close();
      connectorTL.set(null);
    } catch (IOException e) {
      throw new TestException("Error while closing jmx-rmi-connectors\n" + TestHelper.getStackTrace(e));
    }
  }

  private String getServerGroups() {
    return getServerGroups(RemoteTestModule.getMyClientName());
  }

  private String getServerGroups(String clientName) {
    if (MBeanPrms.hasMultipleServerGroups()) {
      String groupString = "CG,";
      if (clientName.contains("peer"))
        groupString = groupString + "peer";

      if (clientName.contains("manager"))
        groupString = groupString + "manager";

      Log.getLogWriter().info("Starting with groups " + groupString);

      return groupString;
    }
    return null;
  }

  private List<String> getServerGroupsArray() {    
    List<String> groupList = new ArrayList<String>();
    if (!MBeanPrms.isWANTest())  {//only for non-wan tests
      String groups = getServerGroups();
      if(groups != null) {
        for (String s : groups.split(","))
          groupList.add(s.toUpperCase());
      }
    }
    groupList.add(DEFAULT_SERVER_GROUP);
    Log.getLogWriter().info("Group List : " + groupList);
    return groupList;
  }

  private List<String> getServerGroupsArray(String clientName) {
    List<String> groupList = new ArrayList<String>();
    if (!MBeanPrms.isWANTest() && !isHDFSTest()) {
      Log.getLogWriter().info("Trying to getServer Groups for client :" + clientName);
      String groups = getServerGroups(clientName);
      if(groups != null) {
      for (String s : groups.split(","))
        groupList.add(s.toUpperCase());
      }
    }
    groupList.add(DEFAULT_SERVER_GROUP);
    Log.getLogWriter().info("Server groups for client : " + clientName + " is " + groupList);
    return groupList;
  }

  private int getJMXPort() {
    Integer portInt = null;
    if (!MBeanPrms.isWANTest()) {
       portInt = (Integer) SQLBB.getBB().getSharedMap().get("JMX_PORT");
    } else {
      String dsName = (String) SQLBB.getBB().getSharedMap().get("CLIENT_DS_" + RemoteTestModule.getMyClientName());
      Log.getLogWriter().info("Trying to find JMX_PORT for ds : " + dsName + ", counter name is : " + "JMX_PORT_" + dsName);
      portInt = (Integer) SQLBB.getBB().getSharedMap().get("JMX_PORT_" + dsName);
    }
    if (portInt == null)
      throw new TestException("No JMX_PORT Registered. JMX Manager is not present");
    return portInt.intValue();
  }

  private Set<DistributedMember> getMembersSet() {
    Set<DistributedMember> set = InternalDistributedSystem.getConnectedInstance().getAllOtherMembers();
    Set<DistributedMember> memberSet = new HashSet<DistributedMember>();
    memberSet.add(InternalDistributedSystem.getConnectedInstance().getDistributedMember());
    for (DistributedMember member : set) {
      if (MBeanPrms.isWANTest() || isHDFSTest()) {
        //wan test and hdfs test and thin clinet use locator topology        
        memberSet.add(member);
      } else {
        if (!member.getId().contains("locator")) {
            memberSet.add(member);
        } else {
          if(isEdge) {
            memberSet.add(member);
          } else {
            Log.getLogWriter().info("Not considering member " + member.getId());
          }
        }
      }
    }
    return memberSet;
  }
  
  private int getAndIncrementCounter(String counterName, String value) {
    SharedLock lock = SQLBB.getBB().getSharedLock();
    try {
      lock.lock();
      int counter=0;
      Map<String, Integer> cMap = getMapForCounterFromSharedMap(counterName);
      Log.getLogWriter().info("Current value for " + counterName + " is " + cMap);
      if(cMap == null) {
        SQLBB.getBB().getSharedMap().put(counterName, new HashMap<String, Integer>());
        cMap = getMapForCounterFromSharedMap(counterName);
      }
      Integer valueCounter = cMap.get(value);
      if(valueCounter != null) {
        counter = valueCounter.intValue();
      }
      counter++;
      cMap.put(value, counter);
      return counter;
    } finally {
      lock.unlock();
    }
  }

  @SuppressWarnings("unchecked")
  private Map<String, Integer> getMapForCounterFromSharedMap(String counterName) {
    return (Map<String, Integer>) SQLBB.getBB().getSharedMap().get(counterName);
  }

  
  private int incrementCounter(String string) {
    SharedLock lock = SQLBB.getBB().getSharedLock();
    try {
      lock.lock();
      int counter = 0;
      Integer cInt = (Integer) SQLBB.getBB().getSharedMap().get(string);
      Log.getLogWriter().info("Current value for " + string + " is " + cInt);
      if (cInt != null) {
        counter = cInt.intValue();
      }
      counter++;
      SQLBB.getBB().getSharedMap().put(string, counter);
      cInt = (Integer) SQLBB.getBB().getSharedMap().get(string);
      Log.getLogWriter().info("Now value for " + string + " is " + cInt);
      return counter;
    } finally {
      lock.unlock();
    }
  }

  private int decrementCounter(String string) {
    SharedLock lock = SQLBB.getBB().getSharedLock();
    try {
      lock.lock();
      int counter=0;
      Integer cInt = (Integer) SQLBB.getBB().getSharedMap().get(string);
      Log.getLogWriter().info("Current value for " + string + " is " + cInt);
      if(cInt != null) {
        counter = cInt.intValue();
      }
      counter--;
      SQLBB.getBB().getSharedMap().put(string, counter);
      cInt = (Integer) SQLBB.getBB().getSharedMap().get(string);
      Log.getLogWriter().info("Now value for " + string + " is " + cInt);
      return counter;
    } finally {
      lock.unlock();
    }
  }
  
  private void saveCounter(String string, Object counter) {
    SharedLock lock = SQLBB.getBB().getSharedLock();
    try {
      lock.lock();
      SQLBB.getBB().getSharedMap().put(string, counter);
    } finally {
      lock.unlock();
    }

  }

  @SuppressWarnings("unused")
  private int getCounter(String string, String value) {
    SharedLock lock = SQLBB.getBB().getSharedLock();
    try {
      lock.lock();
      Map<String, Integer> cMap = getMapForCounterFromSharedMap(string);
      if(cMap==null || cMap.get(value) == null)
        return 0;
      else return cMap.get(value).intValue();
    } finally {
      lock.unlock();
    }
  }
  
  private int getCounter(String string) {
    SharedLock lock = SQLBB.getBB().getSharedLock();
    try {
      lock.lock();
      Integer cInt = (Integer) SQLBB.getBB().getSharedMap().get(string);
      if(cInt==null)
        return 0;
      else return cInt.intValue();
    } finally {
      lock.unlock();
    }
  }

  private Number getCounterNumber(String string) {
    SharedLock lock = SQLBB.getBB().getSharedLock();
    try {
      lock.lock();
      Number cInt = (Number) SQLBB.getBB().getSharedMap().get(string);
      if(cInt==null)
        return 0;
      else return cInt;
    } finally {
      lock.unlock();
    }
  }
  
  @SuppressWarnings("rawtypes")
  private void printCounters() {
    Set set = SQLBB.getBB().getSharedMap().getMap().entrySet();
    for(Object o : set){
      Entry entry  = (Entry) o;
      Log.getLogWriter().info("Key : " + entry.getKey() + " Value " + entry.getValue());
    }
  }

  @SuppressWarnings("unused")
  private Object getTotalCounter(String prefix) {
    SharedLock lock = SQLBB.getBB().getSharedLock();
    int count=0;
    try {
      lock.lock();
      for(Object e : SQLBB.getBB().getSharedMap().getMap().entrySet()){
        @SuppressWarnings("rawtypes")
        Map.Entry entry = (Map.Entry)e;
        String key = (String)entry.getKey();
        if(key.startsWith(prefix)){
          Integer cInt = (Integer) SQLBB.getBB().getSharedMap().get(key);
          if(cInt!=null)
            count +=cInt.intValue();
        }
      }
      return count;
    } finally {
      lock.unlock();
    }
  }

  @SuppressWarnings("unused")
  private Object incrementCounterWithPrefix(String prefix) {
    SharedLock lock = SQLBB.getBB().getSharedLock();
    int count=0;
    try {
      lock.lock();
      String vm_shutting_down = (String) SQLBB.getBB().getSharedMap().getMap().get("VM_SHUTTING_DOWN");
      Log.getLogWriter().info("Not considering vm <" + vm_shutting_down + ">");
      for (Object e : SQLBB.getBB().getSharedMap().getMap().entrySet()) {
        @SuppressWarnings("rawtypes")
        Map.Entry entry = (Map.Entry) e;
        String key = (String) entry.getKey();
        boolean isThisVMShuttingDown = false;
        if (vm_shutting_down != null) {
          if (!vm_shutting_down.equals("NULL") && key.contains(vm_shutting_down))
            isThisVMShuttingDown = true;
          else
            isThisVMShuttingDown = false;
        } else
          isThisVMShuttingDown = false;
        if (key.startsWith(prefix) && !isThisVMShuttingDown) {
          Integer cInt = (Integer) SQLBB.getBB().getSharedMap().get(key);
          if (cInt != null)
            count = cInt.intValue();
          else
            count = 0;
          count++;
          SQLBB.getBB().getSharedMap().put(key, count);
          Log.getLogWriter().info("**** Incremented counter for key " + key + " value " + count);
        }
      }
      return count;
    } finally {
      lock.unlock();
    }
  }



  private Statistics getStatementStats(String sting, String like) {
    final InternalDistributedSystem dsys = InternalDistributedSystem.getConnectedInstance();
    final StatisticsType st = dsys.findType(StatementStats.name);
    Statistics[] stats = dsys.findStatisticsByType(st);
    for (Statistics s : stats) {
      String id = s.getTextId();
      if (id.contains("TRADE.SELLORDERS")) {
        if (sting.contains(like) && id.contains(like)) {
          return s;
        }
      }
    }
    return null;
  }

  private Statistics[] getTableStats(String tableName, boolean isPR) {
    String statsTxtId = CachePerfStats.REGIONPERFSTATS_TEXTID_PREFIX + (isPR ? CachePerfStats.REGIONPERFSTATS_TEXTID_PR_SPECIFIER : "") + tableName;
    Log.getLogWriter().info("Trying to find table stat for " + statsTxtId);
    final InternalDistributedSystem dsys = InternalDistributedSystem.getConnectedInstance();
    Statistics[] stats = dsys.findStatisticsByTextId(statsTxtId);
    
    for (Statistics s : stats) {
      String id = s.getTextId();
      Log.getLogWriter().info("*********** Stat + " + id);
      int inserts = s.get("creates").intValue();
      Log.getLogWriter().info("Insert for " + id + " : " + inserts);
      int updates = s.get("puts").intValue();
      Log.getLogWriter().info("Put for " + id + " : " + updates);
      int deletes = s.get("destroys").intValue();
      Log.getLogWriter().info("Destroy for " + id + " : " + deletes);
    }
    return stats;
  }
  
  
  private Statistics getMemberStats() {
    final InternalDistributedSystem dsys = InternalDistributedSystem.getConnectedInstance();
    final StatisticsType st = dsys.findType(ConnectionStats.name);
    Statistics[] stats = dsys.findStatisticsByType(st);
    for (Statistics s : stats) {
      String id = s.getTextId();
      Log.getLogWriter().info("Member Stats just to check : " + id + ", Stat : " + s);
    }
    return null;
  }

 
  public static Statistics[] getStatementMBeans(String memberNameOrId) {
    final InternalDistributedSystem dsys = InternalDistributedSystem.getConnectedInstance();
    final StatisticsType st = dsys.findType(StatementStats.name);
    Statistics[] stats = dsys.findStatisticsByType(st);
    return stats;
  }
  

  public static ObjectName getStatementMBeanName(String memberNameOrId, String textId) {
    String name = quoteIfNeeded(makeCompliantName(textId));
    return MBeanJMXAdapter.getObjectName((MessageFormat.format(ManagementConstants.OBJECTNAME__STATEMENT_MXBEAN,
        new Object[] { makeCompliantName(quoteIfNeeded(memberNameOrId)), name })));
  }
  
  public static ObjectName getAggregatedStatementMBeanName(String executeStatementNameAndSchema) {
    final InternalDistributedSystem dsys = InternalDistributedSystem.getConnectedInstance();
    final StatisticsType st = dsys.findType(StatementStats.name);
    Statistics[] stats = dsys.findStatisticsByType(st);
    String textId=null;

    for (Statistics s : stats) {
      String id = s.getTextId();
      Log.getLogWriter().info("Processing " + id + " for AggregateStatement");
      if (executeStatementNameAndSchema.contains("select") && id.contains("select")) {
        Log.getLogWriter().info("SELECT STMT AGGR STAT : textId " + id + " uniqueId " + s.getNumericId());
        textId = id;
        break;
      }
      else if (executeStatementNameAndSchema.contains("insert") && id.contains("insert")) {
          Log.getLogWriter().info("INSERT STMT AGGR STAT : textId " + id + " uniqueId " + s.getNumericId());
          textId = id;
          break;
      } else if (executeStatementNameAndSchema.contains("update") && id.contains("update")) {
          Log.getLogWriter().info("UPDATE STMT AGGR STAT : textId " + id + " uniqueId " + s.getNumericId());
          textId = id;
      } else if (executeStatementNameAndSchema.contains("delete") && id.contains("delete")) {
        Log.getLogWriter().info("DELETE STMT AGGR STAT : textId " + id + " uniqueId " + s.getNumericId());
        textId = id;
      }
    }
    if(textId==null)
      return null;
    String name = quoteIfNeeded(makeCompliantName(textId));
    return MBeanJMXAdapter.getObjectName((MessageFormat.format(ManagementConstants.OBJECTNAME__AGGREGATESTATEMENT_MXBEAN,
        new Object[] { name })));
  }

  private boolean isThisManagerVM() {
    return RemoteTestModule.getMyClientName().contains("manager");
  }
  
  /* This method is taken from WanTest, its needed to change the jmx-port
   * Copied from FabricServerHelper and added jmx-manager-start=true;
   */
  public void startLocatorWithManagerRole(boolean mgmtEnabled) {

    String networkServerConfig = GfxdConfigPrms.getNetworkServerConfig();
    //if (networkServerConfig == null) {
      Log.getLogWriter().info("Starting peer locator only");

      Endpoint endpoint = findEndpoint();
      if (endpoint == null) {
        String s = "Locator has not been created yet";
        throw new HydraRuntimeException(s);
      }
      
      Log.getLogWriter().info("This Locator EndPoint " + endpoint);

      FabricServerDescription fsd = FabricServerHelper.getFabricServerDescription();

      Properties bootProps = FabricServerHelper.getBootProperties();
      Locator locator = DistributedSystemHelper.getLocator();

      bootProps.put(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, fsd.getDistributedSystemId().toString());

      bootProps.put(DistributionConfig.REMOTE_LOCATORS_NAME, getRemoteLocators(fsd));
      
      //Change Added for starting jmx-manager on given port
      if (mgmtEnabled) {
        int port = PortHelper.getRandomPort();
        bootProps.put(DistributionConfig.JMX_MANAGER_PORT_NAME, "" + port);
        bootProps.put(DistributionConfig.JMX_MANAGER_START_NAME, "true");
        if (MBeanPrms.isWANTest()) {
          String dsName = fsd.getDistributedSystem();
          SQLBB.getBB().getSharedMap().put("JMX_PORT_" + dsName, port);
          Log.getLogWriter().info("For ds=" + dsName + " manager is at port " + port);
        } else {
          SQLBB.getBB().getSharedMap().put("JMX_PORT", port);
          Log.getLogWriter().info("Manager is at port " + port);
        }
      }
      if (locator == null) {
        DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
        if (ds == null) {
          SharedCounters counters = LocatorBlackboard.getInstance(fsd.getDistributedSystem()).getSharedCounters();
          
          while (counters.incrementAndRead(LocatorBlackboard.locatorLock) != 1) {
            MasterController.sleepForMs(500);
          }
          
          Log.getLogWriter().info("Starting gemfirexd locator");
          
          try {
            Log.getLogWriter()
                .info("Starting gemfirexd locator \"" + endpoint + "\" using boot properties: " + bootProps);
            FabricServiceManager.getFabricLocatorInstance().start(endpoint.getAddress(), endpoint.getPort(), bootProps);

            ////Change***
            incrementCounter(MANAGER_RUNNING);            
            
            if (networkServerConfig != null){
              MBeanNWServerHelper.startNetworkLocators2(networkServerConfig);
            }            
            
          } catch (SQLException e) {
            String s = "Problem starting gemfirexd locator";
            throw new HydraRuntimeException(s, e);
          }
          
          Log.getLogWriter().info("Started locator: " + locator);
          TheFabricServerProperties = bootProps; // cache them
          counters.zero(LocatorBlackboard.locatorLock); // let others proceed

        } else {
          String s = "This VM is already connected to a distributed system. " + "Too late to start a locator";
          throw new HydraRuntimeException(s);
        }
      } else if (TheFabricServerProperties == null) {
        // block attempt to start locator in multiple ways
        String s = "Locator was already started without FabricServerHelper"
            + " using an unknown, and possibly different, configuration";
        throw new HydraRuntimeException(s);

      } else if (!TheFabricServerProperties.equals(bootProps)) {
        // block attempt to connect to system with clashing configuration
        String s = "Already booted using properties " + TheFabricServerProperties + ", cannot also use " + bootProps;
        throw new HydraRuntimeException(s);

      } else {
        // make sure this it a running gemfirexd locator
        FabricLocator loc = FabricServiceManager.getFabricLocatorInstance();
        if (loc.status() != FabricService.State.RUNNING) {
          String s = "This VM already contains a non-GemFireXD locator";
          throw new HydraRuntimeException(s);
        }
      } // else it was already started with this configuration, which is fine
    //} else {
      //Log.getLogWriter().info("Starting network locator");
      //startLocatorWithNetworkServer(networkServerConfig);
    //}

  }
  
  public static class MBeanNWServerHelper extends NetworkServerHelper {
    
    protected static List<NetworkInterface> startNetworkLocators2(
        String networkServerConfig) {
      Log.getLogWriter().info("SQL : ISHA " + sqlTest.isHATest);
      Log.getLogWriter().info("MBEAN : ISHA " + mbeanTest.isHATest);
      return startNetworkLocators(networkServerConfig);
    }
    
  }  

  protected String getRemoteLocators(FabricServerDescription fsd) {    
    String locs = "";
    List<String> dsnames = fsd.getRemoteDistributedSystems();
    Log.getLogWriter().info("Remote dsnames " + dsnames);
    if (dsnames != null) {
      List<FabricServerHelper.Endpoint> endpoints = FabricServerHelper.getEndpoints(dsnames);
      Log.getLogWriter().info("Remote endpoints " + endpoints);
      for (FabricServerHelper.Endpoint endpoint : endpoints) {
        if (locs.length() > 0)
          locs += ",";
        locs += endpoint.getId();
        Log.getLogWriter().info("Added endpoint to remoted Locators " + endpoint);
      }
    }
    Log.getLogWriter().info("Remote Locators " + locs);
    return locs;
  }
  
  private static Endpoint TheLocatorEndpoint;
  
  private static synchronized Endpoint findEndpoint() {    
      Integer vmid = RemoteTestModule.getMyVmid();
      TheLocatorEndpoint = (Endpoint)LocatorBlackboard.getInstance()
                                    .getSharedMap().get(vmid);
    return TheLocatorEndpoint;
  }



  private static void checkForErrors() {
    ArrayList<String> errorList = (ArrayList<String>) SQLBB.getBB().getSharedMap().get("DATA_ERROR");
    if (errorList != null && errorList.size() > 0) {
      errorList.add(0, "Errors (" + errorList.size() + ") were found:\n");
      throw new TestException(errorList.toString());
    } else {
      Log.getLogWriter().info("*** No Errors Were Found!  Congrats, now go celebrate! ***");
    }
  }

  enum OutputType {
    LONG(1), INT (2), FLOAT(3), STRING(4);
    
    private int type;

    OutputType(int type) {
      this.type = type;
    }

    public boolean isInt() {
      return type == 2;
    }
    
    public boolean isLong() {
      return type == 1;
    }
    
    public boolean isFloat() {
      return type == 3;
    }
    
    public boolean isString() {
      return type == 4;
    }

  }

  static String longRunningProcedure = "create procedure trade.longRunningProcedure(DP1 Integer) " +
  "PARAMETER STYLE JAVA " +
  "LANGUAGE JAVA " +
  "READS SQL DATA " +
  "DYNAMIC RESULT SETS 1 " +
  "EXTERNAL NAME 'sql.ProcedureTest.longRunningProcedure'";
  

  //to create procedures on both derby and gemfirexd/gfe
  public static void HydraTask_createProcedures() {
    if(mbeanTest == null) {
      mBeanTestInitialize();      
    }
    mbeanTest.createProcedures();
  }
  
  protected void createProcedures() {
    super.createProcedures();
    Connection conn = getGFEConnection();
    Statement stmt;
    try {
      stmt = conn.createStatement();
      stmt.executeUpdate(longRunningProcedure);
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      closeConnection(conn);
    }

  }
  
  public static synchronized void HydraTask_createDiskStores() {
    if(mbeanTest == null) {
      mBeanTestInitialize();      
    }
    mbeanTest.createDiskStores();
  }

  public static synchronized void HydraTask_initServers() {
    isEdge = true;
  }
  
  public static synchronized void HydraTask_verifyMBeans() {
    if (mbeanTest == null) {
      mBeanTestInitialize();
    }
    mbeanTest.verifyMBeans();
  }

  private void verifyMBeans() {
    MBeanServerConnection mbeanServer = connectJMXConnector();
    if (mbeanServer == null && isThisManagerVM()) {
      Log.getLogWriter().info("Skipping verification as manager is restarting ...");
      return;
    }
    if (mbeanServer == null) {
      throw new TestException("mbeanServer is null");
    }

    checkMemberCounters(mbeanServer);
    checkRegionCounter(mbeanServer);
    checkGfxdMBeans(mbeanServer, true);
    closeJMXConnector();
  }
  
  public static synchronized void HydraTask_createRandomTable() {
    if (mbeanTest == null) {
      mBeanTestInitialize();
    }
    mbeanTest.createRandomTable();
  }

  private void createRandomTable() {
    Connection conn = getGFEConnection();
    //printOpenConnection("    
    Log.getLogWriter().info("Incrementing counter for " + TABLE_COUNTER);
    int tableNum = incrementCounter(TABLE_COUNTER);
    Log.getLogWriter().info("Next Table Number is " + tableNum);
   
      String table = HydraUtil.getRandomElement(new String[] { CREATE_TABLE_TEMPLATE, CREATE_TABLE_TEMPLATE_2 });
      checkAndCreateDiskStore();
      if(CREATE_TABLE_TEMPLATE_2.equals(table)) {
        checkAndCreateRefTable();
      }
      String createTable = table.replace("{0}", "" + tableNum);
      String serverGroupString = "";
      List<String> serverGroupList = new ArrayList<String>();
      boolean onServerGroups = HydraUtil.getRandomBoolean();
      String tableName = TABLE_NAME_TEMPLATE.replace("{0}", "" + tableNum);
      Log.getLogWriter().info("Table Name to create : " + tableName);
      
      //Viren: TODO: Need to check how can provide server group for thin client test
      if(!isEdge) {
        if (onServerGroups) {
          String sg = HydraUtil.getRandomElement(getServerGroupsArray());
          Log.getLogWriter().info("Server group for table : " + sg);
          // make sure sg!=default
        
          while (DEFAULT_SERVER_GROUP.equals(sg))
            sg = HydraUtil.getRandomElement(getServerGroupsArray());
          
          serverGroupList.add(sg);
          //TODO: Viren :- test if we can add multiple server groups
          serverGroupString = " SERVER GROUPS (" + sg + ")";
  
          createTable += serverGroupString;
          Log.getLogWriter().info("sql for create table so far : " + createTable);  
          saveTableToServerGroupMapping(tableName, Collections.singletonList(sg));
        } else {
          serverGroupList.add(DEFAULT_SERVER_GROUP);
        }
      }
      /*defination
       *  
       */
      //((SystemManagementService)ManagementService.getExistingManagementService(null)).addProxyListener(listener);
      createTable += appendPersistentToTableDefinition();
      Log.getLogWriter().info("sql for create table so far : " + createTable);
      createTable += appendPartitionToTableDefinition();
      Log.getLogWriter().info("sql for create table so far : " + createTable);
      createTable += appendEvicitionToTableDefinition();
      Log.getLogWriter().info("sql for create table so far : " + createTable);
      createTable += appendColocateToTableDefinition(table);
      Log.getLogWriter().info("sql for create table so far : " + createTable);
      //TODO: Please add all type of table creation combination..
      
      Log.getLogWriter().info("Creating table <" + createTable + ">");
      try {
        int rows = conn.createStatement().executeUpdate(createTable);
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      //add into table Map
      HydraUtil.sleepForReplicationJMX();
      MBeanServerConnection mbeanServer = connectJMXConnector();
    
  }


  private void pulseCounterUnderLock() {
    SharedLock lock = MBeanTransactionBB.getBB().getSharedLock();
    try {
      lock.lock();
      Log.getLogWriter().info("LOCK - Holding Lock");
      try {
        doOperations();
      } catch (SQLTransactionRollbackException e) {
        if (MBeanPrms.isHATest() && getCounter("VM_SHUTDOWN") > 0) {
          Log.getLogWriter().info("SQLTransactionRollbackException possibly due to concurrent vm shutdown");
          return;
        } else {
          SQLHelper.printSQLException(e);
          throw new TestException("Not able to execute transaction\n" + TestHelper.getStackTrace(e));
        }
      } catch (SQLException e) {
        SQLHelper.printSQLException(e);
        throw new TestException("Not able to execute transaction\n" + TestHelper.getStackTrace(e));
      }

      // Sleep for statistics sampling and federation of metrics
      HydraUtil.sleepForReplicationJMX();

      MBeanServerConnection mbeanServer = connectJMXConnector();

      if (isEdge) {
        Log.getLogWriter().info(
            "member is edge, so no need to verify mbean here");
        return;
      }
      if (mbeanServer == null && isThisManagerVM()) {
        Log.getLogWriter().info(
            "Skipping verification as manager is restarting ...");
        return;
      }
      if (mbeanServer == null) {
        throw new TestException("mbeanServer is null");
      }
      checkGfxdMBeans(mbeanServer, true, false);
      closeJMXConnector();
    } finally {
      Log.getLogWriter().info("LOCK - releasing Lock");
      lock.unlock();
      
    }
    
  }
  
  private void doOperations() throws SQLException {

    String op = HydraUtil.getRandomElement(new String[] { "transaction"/*, "createDropIndex", "createDropTable", "alterTable" */});
    // String op = "insert";
    Log.getLogWriter().info("Trying to execute " + op);

    if ("transaction".equals(op)) {
      transaction("trade.sellorders");
    } else if ("createDropIndex".equals(op)) {
      createDropIndexTest();
    } else if ("createDropTable".equals(op)) {
      createDropTable();
    } else if ("alterTable".equals(op)) {
      alterTableTest();
    }
  }
  
  
  public static class MBeanIndexBB extends Blackboard {
    // Blackboard creation variables
    static String MBean_BB_NAME = "MBeanIndex_Blackboard";
    static String MBean_BB_TYPE = "RMI";

    public static MBeanIndexBB bbInstance = null;

    public static synchronized MBeanIndexBB getBB() {
      if (bbInstance == null) {
        bbInstance = new MBeanIndexBB(MBean_BB_NAME, MBean_BB_TYPE);
      }
      return bbInstance;
    }

    public MBeanIndexBB() {

    }

    public MBeanIndexBB(String name, String type) {
      super(name, type, MBeanIndexBB.class);
    }
  }
  
  public static class MBeanCloseTaskBB extends Blackboard {
    // Blackboard creation variables
    static String MBean_BB_NAME = "MBeanCloseTask_Blackboard";
    static String MBean_BB_TYPE = "RMI";

    public static MBeanCloseTaskBB bbInstance = null;

    public static synchronized MBeanCloseTaskBB getBB() {
      if (bbInstance == null) {
        bbInstance = new MBeanCloseTaskBB(MBean_BB_NAME, MBean_BB_TYPE);
      }
      return bbInstance;
    }

    public MBeanCloseTaskBB() {

    }

    public MBeanCloseTaskBB(String name, String type) {
      super(name, type, MBeanCloseTaskBB.class);
    }
  }
  
  public static class MBeanTaskBB extends Blackboard {
    // Blackboard creation variables
    static String MBean_BB_NAME = "MBeanTask_Blackboard";
    static String MBean_BB_TYPE = "RMI";

    public static MBeanTaskBB bbInstance = null;

    public static synchronized MBeanTaskBB getBB() {
      if (bbInstance == null) {
        bbInstance = new MBeanTaskBB(MBean_BB_NAME, MBean_BB_TYPE);
      }
      return bbInstance;
    }

    public MBeanTaskBB() {

    }

    public MBeanTaskBB(String name, String type) {
      super(name, type, MBeanTaskBB.class);
    }
  }
}
