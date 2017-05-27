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
package com.pivotal.gemfirexd.internal.engine.management;

import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.ManagementConstants;
import com.pivotal.gemfirexd.TestUtil;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GemFireXDQueryObserverHolder;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.management.impl.InternalManagementService;
import com.pivotal.gemfirexd.internal.engine.management.impl.ManagementUtils;
import com.pivotal.gemfirexd.internal.engine.management.impl.GfxdManagementTestBase;
import com.pivotal.gemfirexd.internal.engine.management.impl.StatementMBean;
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedStatement;
import com.pivotal.gemfirexd.internal.impl.sql.StatementStats;

import io.snappydata.test.dunit.VM;

public class StatementMBeanDUnit extends GfxdManagementTestBase {
  static final  String QNNumTimesCompiled = "QNNumTimesCompiled";
  static final  String QNNumExecutions = "QNNumExecutions";
  static final  String QNNumExecutionsInProgress = "QNNumExecutionsInProgress";
  static final  String QNNumTimesGlobalIndexLookup = "QNNumTimesGlobalIndexLookup";  
  static final  String QNNumRowsModified = "QNNumRowsModified";
  static final  String QNParseTime = "QNParseTime";
  static final  String QNBindTime = "QNBindTime";
  static final  String QNOptimizeTime = "QNOptimizeTime";  
  static final  String QNRoutingInfoTime = "QNRoutingInfoTime";
  static final  String QNGenerateTime = "QNGenerateTime";
  static final  String QNTotalCompilationTime = "QNTotalCompilationTime";
  static final  String QNExecuteTime = "QNExecuteTime";  
  static final  String QNProjectionTime = "QNProjectionTime";
  static final  String QNTotalExecutionTime = "QNTotalExecutionTime";
  static final  String QNRowsModificationTime = "QNRowsModificationTime";
  
  static final  String DNNumProjectedRows = "DNNumProjectedRows";  
  static final  String NumNLJoinRowsReturned = "NumNLJoinRowsReturned";
  static final  String NumHASHJoinRowsReturned = "NumHASHJoinRowsReturned";
  static final  String DNNumTableRowsScanned = "DNNumTableRowsScanned";
  
  static final  String DNSubQueryNumRowsSeen = "DNSubQueryNumRowsSeen";
  static final  String DNSubQueryExecutionTime = "DNSubQueryExecutionTime";  
  static final  String DNNumTimesCompiled = "DNNumTimesCompiled";
  static final  String DNNumExecution = "DNNumExecution";
  
  static final  String DNNumExecutionsInProgress = "DNNumExecutionsInProgress";  
  static final  String DNNumTimesGlobalIndexLookup = "DNNumTimesGlobalIndexLookup";
  static final  String DNNumRowsModified = "DNNumRowsModified";
  static final  String DNParseTime = "DNParseTime";
  
  static final  String DNBindTime = "DNBindTime"; 
  static final  String DNOptimizeTime = "DNOptimizeTime";
  static final  String DNRoutingInfoTime = "DNRoutingInfoTime";
  static final  String DNGenerateTime = "DNGenerateTime";
  
  static final  String DNTotalCompilationTime = "DNTotalCompilationTime";  
  static final  String DNExecutionTime = "DNExecutionTime";
  static final  String DNProjectionTime = "DNProjectionTime";
  static final  String DNTotalExecutionTime = "DNTotalExecutionTime";
  
  static final  String DNRowsModificationTime = "DNRowsModificationTime";  
  static final  String QNNumRowsSeen = "QNNumRowsSeen";
  static final  String QNMsgSendTime = "QNMsgSendTime";
  static final  String QNMsgSerTime = "QNMsgSerTime";
  static final  String QNRespDeSerTime = "QNRespDeSerTime"; 
  

  private static final long serialVersionUID = 1L;

  static final String grp1 = "GRP1";
  static final String grp2 = "GRP2";
  static final String grp3 = "GRP3";
  static final int    numberOfInserts = 500;
  static final int    numberOfDeletes = 100;
  static final String SCHEMA = "TRADE";
  static final String TABLE = "CUSTOMERS";  
  static final String POLICY = "REPLICATE"; 
  static final String column1 = "CID";
  static final String column2 = "CUST_NAME";
  static final String column3 = "SINCE";
  static final String column4 = "ADDR";
  static final String column5 = "TID";
  static final String PERSISTENCE_SCHEME = "SYNCHRONOUS";
  


  private static List<String> grps = new ArrayList<String>();
  static final String statemetTextForReplicatedTable = "create table "
      + StatementMBeanDUnit.SCHEMA + "." + StatementMBeanDUnit.TABLE
      + " ("+ column1 +" int not null, "
      + column2 + " varchar(100), "+ column3 +" date, "+ column4 +" varchar(100), "+ column5 + " int ) "
      + StatementMBeanDUnit.POLICY + 
      " PERSISTENT " + StatementMBeanDUnit.PERSISTENCE_SCHEME ;
  

  
  
  public static MBeanServer mbeanServer = ManagementFactory
      .getPlatformMBeanServer();

  public StatementMBeanDUnit(String name) {
    super(name);
    grps.add(GfxdMemberMBeanDUnit.grp1);
    grps.add(GfxdMemberMBeanDUnit.grp2);
    grps.add("DEFAULT");    
  }

  public void testStatementstats() throws Exception {
    try {
      Properties serverInfo = new Properties();
      serverInfo.setProperty("gemfire.enable-time-statistics", "true");      
      serverInfo.setProperty("statistic-sample-rate", "100");
      serverInfo.setProperty("statistic-sampling-enabled", "true");

      startServerVMs(1, 0, GfxdMemberMBeanDUnit.grp1, serverInfo);

      serverInfo.setProperty("jmx-manager", "true");
      serverInfo.setProperty("jmx-manager-start", "true");
      serverInfo.setProperty("jmx-manager-port", "0");// No need to start
                                                      // an
                                                      // Agent for this
                                                      // test      

      startServerVMs(1, 0, GfxdMemberMBeanDUnit.grp2, serverInfo);

      Properties info = new Properties();
      info.setProperty("host-data", "false");
      info.setProperty("gemfire.enable-time-statistics", "true");
      

      // start a client, register the driver.
      startClientVMs(1, 0, StatementMBeanDUnit.grp3, info);

      // enable StatementStats for all connections in this VM
      System.setProperty(GfxdConstants.GFXD_ENABLE_STATS, "true");

      // check that stats are enabled with System property set
      Connection conn = TestUtil.getConnection(info);
      createTableForVerification(conn, true, 1);
      conn.close();

      stopVMNums(1, -1);
    } finally {
      GemFireXDQueryObserverHolder.clearInstance();

    }
  }

  private void createTableForVerification(Connection conn,
      final boolean enableStats, final int numTimesSampled) throws Exception {

    //System.out.println("serverVMs= " + this.serverVMs.size());
    final VM serverVM = this.serverVMs.get(1); // Server Started as a
                                               // manager

    final Statement stmt = conn.createStatement();

    final String createSchemaOrder = "create schema "+ SCHEMA;
    stmt.execute(createSchemaOrder);

    //System.out.println("statemetTextForReplicatedTable= " + StatementMBeanDUnit.statemetTextForReplicatedTable);  

    stmt.execute(StatementMBeanDUnit.statemetTextForReplicatedTable);
    

    PreparedStatement psInsertCust = conn.prepareStatement("insert into "
        +  SCHEMA+ "."+ TABLE +" values (?,?,?,?,?)");
    
    for (int i = 0; i < StatementMBeanDUnit.numberOfInserts; i++) {

      psInsertCust.setInt(1, i);
      psInsertCust.setString(2, "XXXX" + i);
      java.sql.Date since = new java.sql.Date(System.currentTimeMillis());
      psInsertCust.setDate(3, since);
      psInsertCust.setString(4, "XXXX" + i);
      psInsertCust.setInt(5, i);
      psInsertCust.executeUpdate();
    }

    PreparedStatement psInsertCust2 = conn.prepareStatement("delete from "
        + SCHEMA+ "."+ TABLE + " where "+ column1 +" = ?");

    for (int i = 0; i < StatementMBeanDUnit.numberOfDeletes; i++) {
      psInsertCust2.setInt(1, i);
      psInsertCust2.executeUpdate();
    }

  
    
    //verify replicated table attributes
    String stmtId = ((EmbedStatement) stmt).getStatementStats()
        .getStatsId();   
    
    serverVM.invoke(this.getClass(), "verifyStatementMbean", new Object[] { stmtId });   

    
    psInsertCust.close();
    psInsertCust2.close();
    stmt.close();

    logInfo("done verifyTableMbeans");

  }  
  

  public static void verifyStatementMbean(String stmtId) {
    
    
    final InternalManagementService service = InternalManagementService
        .getInstance(Misc.getMemStore());
    GemFireCacheImpl cache = Misc.getGemFireCacheNoThrow();
    DistributedMember member = cache.getDistributedSystem()
        .getDistributedMember();
    String memberName = MBeanJMXAdapter.getMemberNameOrId(member);

    final InternalDistributedSystem dsys = InternalDistributedSystem
        .getConnectedInstance();
    final StatisticsType st = dsys.findType(StatementStats.name);
    Statistics[] statsFromDS = dsys.findStatisticsByType(st);
    
    //logInfo("stmtId==" + stmtId);

    final ObjectName statementObjectName = ManagementUtils
        .getStatementMBeanName(memberName, stmtId);
    
    waitForCriterion(new WaitCriterion() {
      public String description() {
        return "Waiting for the statement Mbean to get reflected at managing node";
      }

      public boolean done() {
        StatementMXBean bean = service.getMBeanInstance(statementObjectName,
            StatementMXBean.class);
        boolean done = (bean != null);
        return done;
      }

    }, ManagementConstants.REFRESH_TIME * 4, 500, true);

    StatementMBean statementMBean = (StatementMBean) service.getMBeanInstance(
        statementObjectName, StatementMXBean.class);   
    
    for (Statistics stat : statsFromDS) {
      //logInfo("statmentIDs from Ds  " + stat.getTextId());
      if (stat.getTextId().equals(stmtId)) {
        Number valueFromStats = 0;

        valueFromStats = stat.get(StatementMBeanDUnit.DNBindTime);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.DNBindTime + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getDNBindTime());
        assertEquals(valueFromStats.longValue(), statementMBean.getDNBindTime());

        valueFromStats = stat.get(StatementMBeanDUnit.DNGenerateTime);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.DNGenerateTime + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getDNGenerateTime());         
        assertEquals(valueFromStats.longValue(), statementMBean.getDNGenerateTime());

        valueFromStats = stat.get(StatementMBeanDUnit.DNNumExecutionsInProgress);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.DNNumExecutionsInProgress + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getDNNumExecutionsInProgress());
        assertEquals(valueFromStats.longValue(), statementMBean.getDNNumExecutionsInProgress());

        valueFromStats = stat.get(StatementMBeanDUnit.DNNumRowsModified);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.DNNumRowsModified + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getDNNumRowsModified());
        assertEquals(valueFromStats.longValue(), statementMBean.getDNNumRowsModified());
        
        valueFromStats = stat.get(StatementMBeanDUnit.DNNumTimesCompiled);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.DNNumTimesCompiled + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getDNNumTimesCompiled());
        assertEquals(valueFromStats.longValue(), statementMBean.getDNNumTimesCompiled());
        
        valueFromStats = stat.get(StatementMBeanDUnit.DNNumTimesGlobalIndexLookup);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.DNNumTimesGlobalIndexLookup + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getDNNumTimesGlobalIndexLookup());
        assertEquals(valueFromStats.longValue(), statementMBean.getDNNumTimesGlobalIndexLookup());
        
        valueFromStats = stat.get(StatementMBeanDUnit.DNOptimizeTime);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.DNOptimizeTime + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getDNOptimizeTime());
        assertEquals(valueFromStats.longValue(), statementMBean.getDNOptimizeTime());
        
        valueFromStats = stat.get(StatementMBeanDUnit.DNParseTime);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.DNParseTime + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getDNParseTime());
        assertEquals(valueFromStats.longValue(), statementMBean.getDNParseTime());
        
        valueFromStats = stat.get(StatementMBeanDUnit.DNProjectionTime);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.DNProjectionTime + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getDNProjectionTime());
        assertEquals(valueFromStats.longValue(), statementMBean.getDNProjectionTime());
        
        valueFromStats = stat.get(StatementMBeanDUnit.DNRoutingInfoTime);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.DNRoutingInfoTime + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getDNRoutingInfoTime());
        assertEquals(valueFromStats.longValue(), statementMBean.getDNRoutingInfoTime());
        
        valueFromStats = stat.get(StatementMBeanDUnit.DNRowsModificationTime);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.DNRowsModificationTime + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getDNRowsModificationTime());
        assertEquals(valueFromStats.longValue(), statementMBean.getDNRowsModificationTime());
        
        valueFromStats = stat.get(StatementMBeanDUnit.DNTotalCompilationTime);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.DNTotalCompilationTime + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getDNTotalCompilationTime());
        assertEquals(valueFromStats.longValue(), statementMBean.getDNTotalCompilationTime());
        
        valueFromStats = stat.get(StatementMBeanDUnit.DNTotalExecutionTime);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.DNTotalExecutionTime + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getDNTotalExecutionTime());
        assertEquals(valueFromStats.longValue(), statementMBean.getDNTotalExecutionTime());
        
        
        valueFromStats = stat.get(StatementMBeanDUnit.QNBindTime);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.QNBindTime + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getQNBindTime());
        assertEquals(valueFromStats.longValue(), statementMBean.getQNBindTime());        
        
        valueFromStats = stat.get(StatementMBeanDUnit.QNExecuteTime);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.QNExecuteTime + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getQNExecuteTime());
        assertEquals(valueFromStats.longValue(), statementMBean.getQNExecuteTime());        
        
        valueFromStats = stat.get(StatementMBeanDUnit.QNGenerateTime);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.QNGenerateTime + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getQNGenerateTime());
        assertEquals(valueFromStats.longValue(), statementMBean.getQNGenerateTime());
        
        valueFromStats = stat.get(StatementMBeanDUnit.QNMsgSendTime);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.QNMsgSendTime + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getQNMsgSendTime());
        assertEquals(valueFromStats.longValue(), statementMBean.getQNMsgSendTime());
        
        valueFromStats = stat.get(StatementMBeanDUnit.QNMsgSerTime);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.QNMsgSerTime + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getQNMsgSerTime());
        assertEquals(valueFromStats.longValue(), statementMBean.getQNMsgSerTime());
        
        valueFromStats = stat.get(StatementMBeanDUnit.QNNumExecutions);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.QNNumExecutions + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getQNNumExecutions());
        assertEquals(valueFromStats.longValue(), statementMBean.getQNNumExecutions());
        
        valueFromStats = stat.get(StatementMBeanDUnit.QNNumExecutionsInProgress);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.QNNumExecutionsInProgress + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getQNNumExecutionsInProgress());
        assertEquals(valueFromStats.longValue(), statementMBean.getQNNumExecutionsInProgress());
        
        valueFromStats = stat.get(StatementMBeanDUnit.QNNumRowsModified);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.QNNumRowsModified + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getQNNumRowsModified());
        assertEquals(valueFromStats.longValue(), statementMBean.getQNNumRowsModified());
        
        valueFromStats = stat.get(StatementMBeanDUnit.QNNumRowsSeen);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.QNNumRowsSeen + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getQNNumRowsSeen());
        assertEquals(valueFromStats.longValue(), statementMBean.getQNNumRowsSeen());
        
        valueFromStats = stat.get(StatementMBeanDUnit.QNNumTimesCompiled);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.QNNumTimesCompiled + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getQNNumTimesCompiled());
        assertEquals(valueFromStats.longValue(), statementMBean.getQNNumTimesCompiled());
        
        valueFromStats = stat.get(StatementMBeanDUnit.QNNumTimesGlobalIndexLookup);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.QNNumTimesGlobalIndexLookup + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getQNNumTimesGlobalIndexLookup());
        assertEquals(valueFromStats.longValue(), statementMBean.getQNNumTimesGlobalIndexLookup());
        
        valueFromStats = stat.get(StatementMBeanDUnit.QNOptimizeTime);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.QNOptimizeTime + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getQNOptimizeTime());
        assertEquals(valueFromStats.longValue(), statementMBean.getQNOptimizeTime());
        
        valueFromStats = stat.get(StatementMBeanDUnit.QNParseTime);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.QNParseTime + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getQNParseTime());
        assertEquals(valueFromStats.longValue(), statementMBean.getQNParseTime());
        
        valueFromStats = stat.get(StatementMBeanDUnit.QNProjectionTime);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.QNProjectionTime + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getQNProjectionTime());
        assertEquals(valueFromStats.longValue(), statementMBean.getQNProjectionTime());
        
        valueFromStats = stat.get(StatementMBeanDUnit.QNRespDeSerTime);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.QNRespDeSerTime + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getQNRespDeSerTime());
        assertEquals(valueFromStats.longValue(), statementMBean.getQNRespDeSerTime());
 
        valueFromStats = stat.get(StatementMBeanDUnit.DNNumProjectedRows);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.DNNumProjectedRows + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getNumProjectedRows());
        assertEquals(valueFromStats.longValue(), statementMBean.getNumProjectedRows()); 
 
        valueFromStats = stat.get(StatementMBeanDUnit.DNNumTableRowsScanned);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.DNNumTableRowsScanned + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getNumTableRowsScanned());
        assertEquals(valueFromStats.longValue(), statementMBean.getNumTableRowsScanned());
       
        valueFromStats = stat.get(StatementMBeanDUnit.DNSubQueryNumRowsSeen);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.DNSubQueryNumRowsSeen + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getSubQueryNumRowsSeen());
        assertEquals(valueFromStats.longValue(), statementMBean.getSubQueryNumRowsSeen());

        valueFromStats = stat.get(StatementMBeanDUnit.DNSubQueryExecutionTime);
        logInfo("verifyStatementMbean attr=" + StatementMBeanDUnit.DNSubQueryExecutionTime + " stats value==" + valueFromStats + " from mbean==" + statementMBean.getSubQueryExecutionTime());
        assertEquals(valueFromStats.longValue(), statementMBean.getSubQueryExecutionTime());
        
        
        //break the loop
        break;
      }
    }  

  }

  @Override
  public void tearDown2() throws Exception {
    super.tearDown2();
  }
}