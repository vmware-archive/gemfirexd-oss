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
package com.pivotal.gemfirexd.wan;

public class GfxdParallelWanDUnit extends GfxdWanCommonTestBase {

  private static final long serialVersionUID = 1L;

  public GfxdParallelWanDUnit(String name) {
    super(name);
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
  }
  @Override
  protected void vmTearDown() throws Exception {
    super.vmTearDown();
  }

  @Override
  protected String getCreateGatewayDML() {
    return "CREATE gatewaysender MySender (remotedsid 2 isparallel true ) server groups (sgSender)";
  }
 
  /*public void _testParallelWan() throws Exception {
    // starts 4 sites
    // connected to each other and each having 2 data-store nodes, 1 accessor  and 1 locator
    // 1 locator
    // 1 datastore + sender
    // 1 datastore + sender + receiver
    // 1 accessor + sender + receiver
    startSites();
    
    // create a gateway receiver with default start and end port on the server groups
    // it creates a gateway receiver on accessor node in each site
    final String createGWR = "create gatewayreceiver myrcvr() server groups(sgSender)" ;
    executeSql(SITE_A, createGWR);
    executeSql(SITE_B, createGWR);
    executeSql(SITE_C, createGWR);
    executeSql(SITE_D, createGWR);
    
    // create 2 gatewaysender on site A. one connecting to B and other to C
    // create 1 gatewaysender on site B. connecting to A
    // create 1 gatewaysender on site C. connecting to D
    
    executeSql(SITE_A, "CREATE gatewaysender mySender (remotedsid 2 isparallel true ) server groups (sg1)");
    executeSql(SITE_A, "CREATE gatewaysender mySenderB (remotedsid 3 isparallel true) server groups (sg1)");
    executeSql(SITE_B, "CREATE gatewaysender mySender (remotedsid 1 isparallel true ) server groups (sg1)");
    executeSql(SITE_C, "CREATE gatewaysender mySender (remotedsid 4 isparallel true ) server groups (sg1)");
    
    final String createPTable = "create table EMP.PARTITIONED_TABLE (ID int , "
        + "DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) not null, "
        + "primary key (ID)) partition by column(ADDRESS)  redundancy 1 server groups (sg1)";
    
    // create the partitioned table on all site.
    // attach the gatewaysender on each site to the table
    // it seems case of the char does't matter in syntax
    executeSql(SITE_A, createPTable + " GatewaySender (MySender,mySenderB)");
    executeSql(SITE_B, createPTable + " GatewaySender (MySender)");
    executeSql(SITE_C, createPTable + " GatewaySender (MySender)");
    executeSql(SITE_D, createPTable);
    
    // create a colocated table. colocated with emp.partitioned_table
    String createCTable = "create table EMP.COLOCATED_TABLE (ID int not null, " +
                "DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) not null, primary key (ID), " +
                "foreign key (ID) references EMP.PARTITIONED_TABLE (id)) " +
                "partition by column(ADDRESS) COLOCATE WITH (EMP.PARTITIONED_TABLE ) redundancy 1 server groups (sg1)";

    // attach the senders on each site to the table
    executeSql(SITE_A, createCTable + " GatewaySender (MySender,mySenderB)");
    executeSql(SITE_B, createCTable + " GatewaySender (MySender)");
    executeSql(SITE_C, createCTable + " GatewaySender (MySender)");
    executeSql(SITE_D, createCTable);
    
    // now insert into the tables on site A
    executeSql(SITE_A,
        "insert into EMP.PARTITIONED_TABLE values (1, 'First', 'A714')");
    executeSql(SITE_A,
        "insert into EMP.COLOCATED_TABLE values (1, 'First', 'A714')");

    // wait for the queue to become EMPTY
    executeSql(SITE_A,
        "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('MYSENDER', 0, 0)");
    
    // get the golden file!
    String goldenFile = TestUtil.getResourcesDir()
    + "/lib/GemFireXDGatewayDUnit.xml";
    
    // in the golden file we get the result set from the resultSetId provided 
    // here
    // It should match the result set that we get after executing the stmt
    
    // check that all the events have reached to B,C,D
    sqlExecuteVerify(SITE_A, "select ID from EMP.COLOCATED_TABLE", goldenFile,
        "id1", true, false);
    
    sqlExecuteVerify(SITE_B, "select ID from EMP.COLOCATED_TABLE", goldenFile,
        "id1", true, false);
    executeSql(SITE_A,
        "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('MYSENDERB', 0, 0)");
    
    sqlExecuteVerify(SITE_C, "select ID from EMP.COLOCATED_TABLE", goldenFile,
        "id1", true, false);
    
    executeSql(SITE_C,
        "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('MYSENDER', 0, 0)");
    
    sqlExecuteVerify(SITE_D, "select ID from EMP.COLOCATED_TABLE", goldenFile,
        "id1", true, false);

    // Check B to A
    executeSql(SITE_B,
        "insert into EMP.PARTITIONED_TABLE values(2, 'Second', 'Address2')");

    executeSql(SITE_B,
        "insert into EMP.COLOCATED_TABLE values(2, 'Second', 'Address2')");
    executeSql(SITE_B, "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('MYSENDER', 0, 0)");
    sqlExecuteVerify(SITE_A, "select DESCRIPTION from EMP.COLOCATED_TABLE WHERE ID = 2", goldenFile,
        "id2", true, false);
    
    executeSql(SITE_B,
        "Update EMP.PARTITIONED_TABLE set DESCRIPTION = 'Second' where ADDRESS = 'A714'");
    
    executeSql(SITE_B,
        "Update EMP.COLOCATED_TABLE set DESCRIPTION = 'Second' where ADDRESS = 'A714'");
    executeSql(SITE_B,
        "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('MYSENDER', 0, 0)");
    sqlExecuteVerify(SITE_A, "select DESCRIPTION from EMP.COLOCATED_TABLE WHERE ADDRESS = 'A714'", goldenFile,
        "id2", true, false);
    
    executeSql(SITE_A,
        "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('MYSENDERB', 0, 0)");
    sqlExecuteVerify(SITE_C, "select DESCRIPTION from EMP.COLOCATED_TABLE WHERE ADDRESS = 'A714'", goldenFile,
        "id2", true, false);    
    
    executeSql(SITE_C,
        "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('MYSENDER', 0, 0)");

    sqlExecuteVerify(SITE_D, "select DESCRIPTION from EMP.COLOCATED_TABLE WHERE ADDRESS = 'A714'", goldenFile,
        "id2", true, false);
    
    getLogWriter().info("SJ: END OF TEST");
    
  }
  
  public void _testUpdatePropagation() throws Exception {
    startSites();
    String goldenFile = TestUtil.getResourcesDir()
    + "/lib/GemFireXDGatewayDUnit.xml";
    
    final String createGWR = "create gatewayreceiver myrcvr() server groups(sgSender)" ;
    executeSql(SITE_A, createGWR);
    executeSql(SITE_B, createGWR);
    executeSql(SITE_C, createGWR);
    executeSql(SITE_D, createGWR);
    
    executeSql(SITE_A, "CREATE gatewaysender mySender (remotedsid 2 isparallel true ) server groups (sg1)");
    executeSql(SITE_A, "CREATE gatewaysender mySenderB (remotedsid 3 isparallel true) server groups (sg1)");
    executeSql(SITE_B, "CREATE gatewaysender mySender (remotedsid 1 isparallel true ) server groups (sg1)");
    executeSql(SITE_C, "CREATE gatewaysender mySender (remotedsid 4 isparallel true ) server groups (sg1)");
    
    final String createPTable = "create table EMP.PARTITIONED_TABLE (ID int , "
        + "DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) not null, "
        + "primary key (ID)) partition by column(ADDRESS)  redundancy 1 server groups (sg1)";
    
    executeSql(SITE_A, createPTable + " GatewaySender (MySender,mySenderB)");
    executeSql(SITE_B, createPTable + " GatewaySender (MySender)");
    executeSql(SITE_C, createPTable + " GatewaySender (MySender)");
    executeSql(SITE_D, createPTable);
    
    String createCTable = "create table EMP.COLOCATED_TABLE (ID int not null, " +
                "DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) not null, primary key (ID), " +
                "foreign key (ID) references EMP.PARTITIONED_TABLE (id)) " +
                "partition by column(ADDRESS) COLOCATE WITH (EMP.PARTITIONED_TABLE ) redundancy 1 server groups (sg1)";

    executeSql(SITE_A, createCTable + " GatewaySender (MySender,mySenderB)");
    executeSql(SITE_B, createCTable + " GatewaySender (MySender)");
    executeSql(SITE_C, createCTable + " GatewaySender (MySender)");
    executeSql(SITE_D, createCTable);
    
    // insert id1 in site A and verify in other sites
    executeSql(SITE_A,
        "insert into EMP.PARTITIONED_TABLE values (1, 'First', 'A714')");
    sqlExecuteVerify(SITE_A, "select ID from EMP.PARTITIONED_TABLE where ADDRESS = 'A714'", goldenFile,
        "id1", true, false);
    executeSql(SITE_A, "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('MYSENDER', 0, 0)");
    sqlExecuteVerify(SITE_B, "select ID from EMP.PARTITIONED_TABLE where ADDRESS = 'A714'", goldenFile,
        "id1", true, false);
    
    executeSql(SITE_A, "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('MYSENDERB', 0, 0)");
    sqlExecuteVerify(SITE_C, "select ID from EMP.PARTITIONED_TABLE where ADDRESS = 'A714'", goldenFile,
        "id1", true, false);
        
    executeSql(SITE_C,
        "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('MYSENDER', 0, 0)");
    sqlExecuteVerify(SITE_D, "select ID from EMP.PARTITIONED_TABLE where ADDRESS = 'A714'", goldenFile,
        "id1", true, false);

    // update the same id1 to id2 in A and verify in other sites
    executeSql(SITE_A,
        "Update EMP.PARTITIONED_TABLE set DESCRIPTION = 'Second' where ADDRESS = 'A714'");

    sqlExecuteVerify(SITE_A, "select DESCRIPTION from EMP.PARTITIONED_TABLE", goldenFile,
        "id2", true, false);
    executeSql(SITE_A, "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('MYSENDER', 0, 0)");
    sqlExecuteVerify(SITE_B, "select DESCRIPTION from EMP.PARTITIONED_TABLE", goldenFile,
        "id2", true, false);
    
    executeSql(SITE_A,
        "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('MYSENDERB', 0, 0)");
    sqlExecuteVerify(SITE_C, "select ID from EMP.PARTITIONED_TABLE where ADDRESS = 'A714'", goldenFile,
        "id1", true, false);
    
    executeSql(SITE_C,
        "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('MYSENDER', 0, 0)");
    sqlExecuteVerify(SITE_D, "select ID from EMP.PARTITIONED_TABLE where ADDRESS = 'A714'", goldenFile,
        "id1", true, false);
    
    // Check B to A
    // insert id2 in site B and verify in other sites
    executeSql(SITE_B,
        "insert into EMP.PARTITIONED_TABLE values(2, 'Second', 'Address2')");
    sqlExecuteVerify(SITE_B, "select DESCRIPTION from EMP.PARTITIONED_TABLE WHERE ID = 2", goldenFile,
        "id2", true, false);
    executeSql(SITE_B, "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('MYSENDER', 0, 0)");
    
    sqlExecuteVerify(SITE_A, "select DESCRIPTION from EMP.PARTITIONED_TABLE WHERE ID = 2", goldenFile,
        "id2", true, false);
    executeSql(SITE_A, "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('MYSENDERB', 0, 0)");
    sqlExecuteVerify(SITE_C, "select DESCRIPTION from EMP.PARTITIONED_TABLE WHERE ID = 2", goldenFile,
        "id2", true, false);
    executeSql(SITE_C, "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('MYSENDER', 0, 0)");
    sqlExecuteVerify(SITE_D, "select DESCRIPTION from EMP.PARTITIONED_TABLE where ADDRESS = 'Address2'", goldenFile,
        "id2", true, false);
    
    
    // Update one row in site B and verify in other sites.
    executeSql(SITE_B,
        "Update EMP.PARTITIONED_TABLE set DESCRIPTION = 'Second' where ADDRESS = 'A714'");
    
    executeSql(SITE_B, "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('MYSENDER', 0, 0)");
    executeSql(SITE_A, "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('MYSENDERB', 0, 0)");
    executeSql(SITE_C, "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('MYSENDER', 0, 0)");
    
    sqlExecuteVerify(SITE_A, "select DESCRIPTION from EMP.PARTITIONED_TABLE WHERE ADDRESS = 'A714'", goldenFile,
        "id2", true, false);
    sqlExecuteVerify(SITE_C, "select DESCRIPTION from EMP.PARTITIONED_TABLE WHERE ADDRESS = 'A714'", goldenFile,
        "id2", true, false);
    sqlExecuteVerify(SITE_D, "select DESCRIPTION from EMP.PARTITIONED_TABLE WHERE ADDRESS = 'A714'", goldenFile,
        "id2", true, false);
    
    // Do the same for colocated table
    executeSql(SITE_B,
        "insert into EMP.COLOCATED_TABLE values(2, 'Second', 'Address2')");
    executeSql(SITE_B,
        "Update EMP.COLOCATED_TABLE set DESCRIPTION = 'Second' where ADDRESS = 'Address2'");

    executeSql(SITE_B, "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('MYSENDER', 0, 0)");
    executeSql(SITE_A, "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('MYSENDERB', 0, 0)");
    executeSql(SITE_C, "call SYS.WAIT_FOR_SENDER_QUEUE_FLUSH('MYSENDER', 0, 0)");
    
    sqlExecuteVerify(SITE_A, "select DESCRIPTION from EMP.COLOCATED_TABLE WHERE ADDRESS = 'Address2'", goldenFile,
        "id2", true, false);
    sqlExecuteVerify(SITE_C, "select DESCRIPTION from EMP.COLOCATED_TABLE WHERE ADDRESS = 'Address2'", goldenFile,
        "id2", true, false);    
    sqlExecuteVerify(SITE_D, "select DESCRIPTION from EMP.COLOCATED_TABLE WHERE ADDRESS = 'Address2'", goldenFile,
        "id2", true, false);
  }
  
  *//** Single node in two sites *//*
  public void _testParallelWan_singleNode() throws Exception {
    String goldenFile = TestUtil.getResourcesDir()
        + "/lib/GemFireXDGatewayDUnit.xml";

    // DS0 Locator
    Properties props = new Properties();
    int lnPort = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    props.put(com.pivotal.gemfirexd.Attribute.SYS_PERSISTENT_DIR, getUniqueName()
        + "1");
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, "1");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + lnPort
        + "]");
    startLocatorVM("localhost", lnPort, null, props);
    assertTrue(this.serverVMs.size() == 1);

    // DS1 Locator
    int nyPort = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    props = new Properties();
    props.put(com.pivotal.gemfirexd.Attribute.SYS_PERSISTENT_DIR, getUniqueName()
        + "2");
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, "2");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + nyPort
        + "]");
    props.setProperty(DistributionConfig.REMOTE_LOCATORS_NAME, "localhost["
        + lnPort + "]");
    startLocatorVM("localhost", nyPort, null, props);
    assertTrue(this.serverVMs.size() == 2);

    // DS1 Receiver
    props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + nyPort
        + "]");
    AsyncVM async3 = invokeStartServerVM(3, 0, "sg1", props);
    joinVMs(true, async3);
    assertTrue(this.serverVMs.size() == 3);

    Runnable createRecevier = createGatewayReceiver(0, 0);
    serverExecute(3, createRecevier);

    // DS0 Sender
    props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + lnPort
        + "]");
    AsyncVM async4 = invokeStartServerVM(4, 0, "sg1", props);
    joinVMs(true, async4);
    assertTrue(this.serverVMs.size() == 4);

    serverExecute(4, createGatewaySender("mySender", "2", "sg1"));

    VM receiver = serverVMs.get(2);
    VM sender = serverVMs.get(3);

    this.executeSql(receiver, "create schema EMP");
    this.executeSql(sender, "create schema EMP");

    // String createTable =
    String createTable = "create table EMP.TESTTABLE (ID int , DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024), primary key (ID)) gatewaysender (MYSENDER) server groups (sg1) ";
    this.executeSql(receiver, createTable);
    this.executeSql(sender, createTable);

    // Test insert propagation
    // insert into EMP.TESTTABLE values (1, 'First', 'J 604')
    // batchInsert
    serverExecute(4, batchInsert());
    serverExecute(4, getExecutorToWaitForQueuesToDrain("MYSENDER", 0));
    // This should have propagated to wan site 2
    this.sqlExecuteVerify(receiver, "select ID from EMP.TESTTABLE", goldenFile,
        "id1", true, false);

    // Test update propagation (bulk)
    // This should have propagated to wan site 2
    this.executeSql(sender,
        "Update EMP.TESTTABLE set DESCRIPTION = 'Second' where ID = 1");
    this.executeSql(sender,
        "Update EMP.TESTTABLE set DESCRIPTION = 'First' where ID = 2");
    
    this.sqlExecuteVerify(sender, "select DESCRIPTION from EMP.TESTTABLE where ID = 1",
        goldenFile, "id2", true, false);
    this.sqlExecuteVerify(sender, "select DESCRIPTION from EMP.TESTTABLE where ID = 2",
        goldenFile, "id1", true, false);
    serverExecute(4, getExecutorToWaitForQueuesToDrain("MYSENDER", 0));
    this.sqlExecuteVerify(sender, "select DESCRIPTION from EMP.TESTTABLE where ID = 1",
        goldenFile, "id2", true, false);
    this.sqlExecuteVerify(sender, "select DESCRIPTION from EMP.TESTTABLE where ID = 2",
        goldenFile, "id1", true, false);

    // Test delete propagation
    this.executeSql(sender, "Delete from  EMP.TESTTABLE where DESCRIPTION = 'Second'"); //DESCRIPTION = 'Second'
    this.executeSql(sender, "Delete from  EMP.TESTTABLE where ID = 2"); //DESCRIPTION = 'First'
    
    this.sqlExecuteVerify(sender, "select DESCRIPTION from EMP.TESTTABLE where ID = 1",
        goldenFile, "empty", true, false);
    this.sqlExecuteVerify(sender, "select DESCRIPTION from EMP.TESTTABLE where ID = 2",
        goldenFile, "empty", true, false);
    serverExecute(4, getExecutorToWaitForQueuesToDrain("MYSENDER", 0));
    this.sqlExecuteVerify(sender, "select DESCRIPTION from EMP.TESTTABLE where ID = 1",
        goldenFile, "empty", true, false);
    this.sqlExecuteVerify(sender, "select DESCRIPTION from EMP.TESTTABLE where ID = 2",
        goldenFile, "empty", true, false);
    serverExecute(4, stopSender());

    this.executeSql(sender, "drop table emp.testtable");
    this.executeSql(receiver, "drop table emp.testtable");

    this.executeSql(sender, "drop gatewaysender mysender");
    this.executeSql(receiver, "drop gatewayreceiver myrcvr");

    this.executeSql(sender, "drop schema emp restrict");
    this.executeSql(receiver, "drop schema emp restrict");
  }
  
  public void _testParallelWan_TwoNode() throws Exception {
    String goldenFile = TestUtil.getResourcesDir()
        + "/lib/GemFireXDGatewayDUnit.xml";

    // DS0 Locator
    Properties props = new Properties();
    int lnPort = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    props.put(com.pivotal.gemfirexd.Attribute.SYS_PERSISTENT_DIR, getUniqueName()
        + "1");
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, "1");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + lnPort
        + "]");
    startLocatorVM("localhost", lnPort, null, props);
    assertTrue(this.serverVMs.size() == 1);

    // DS1 Locator
    int nyPort = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
    props = new Properties();
    props.put(com.pivotal.gemfirexd.Attribute.SYS_PERSISTENT_DIR, getUniqueName()
        + "2");
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, "2");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + nyPort
        + "]");
    props.setProperty(DistributionConfig.REMOTE_LOCATORS_NAME, "localhost["
        + lnPort + "]");
    startLocatorVM("localhost", nyPort, null, props);
    assertTrue(this.serverVMs.size() == 2);

    // DS1 Receiver
    props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + nyPort
        + "]");
    AsyncVM async3 = invokeStartServerVM(1, 0, "sg1", props);
    joinVMs(true, async3);
    assertTrue(this.serverVMs.size() == 3);

    Runnable createRecevier = createGatewayReceiver(0, 0);
    serverExecute(3, createRecevier);

    // DS0 Sender1
    props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + lnPort
        + "]");
    AsyncVM async4 = invokeStartServerVM(1, 0, "sg1", props);
    joinVMs(true, async4);
    assertTrue(this.serverVMs.size() == 4);

    serverExecute(4, createGatewaySender("mySender", "2", "sg1"));
    
    // DS0 Sender2
    props = new Properties();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    props.setProperty(DistributionConfig.LOCATORS_NAME, "localhost[" + lnPort
        + "]");
    AsyncVM async5 = invokeStartServerVM(1, 0, "sg1", props);
    joinVMs(true, async5);
    assertTrue(this.serverVMs.size() == 5);

    //serverExecute(5, createGatewaySender("mySender", "2", "sg1"));
    

    VM reciever = serverVMs.get(2);
    VM sender = serverVMs.get(3);

    this.executeSql(reciever, "create schema EMP");
    this.executeSql(sender, "create schema EMP");

    // String createTable =
    // "create table EMP.TESTTABLE (ID int , DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024) , primary key (ID)) replicate gatewaysender (MYSENDER) server groups (sg1) ";
    String createTable = "create table EMP.TESTTABLE (ID int , DESCRIPTION varchar(1024) not null, ADDRESS varchar(1024), primary key (ID)) gatewaysender (MYSENDER) server groups (sg1) ";
    this.executeSql(reciever, createTable);
    this.executeSql(sender, createTable);

    // Test insert propagation
    // this.executeSql(sender,
    // "insert into EMP.TESTTABLE values (1, 'First', 'J 604')");

    serverExecute(4, batchInsert());
    serverExecute(4, getExecutorToWaitForQueuesToDrain("MYSENDER", 0));
    // This should have propagated to wan site 2
    this.sqlExecuteVerify(reciever, "select ID from EMP.TESTTABLE", goldenFile,
        "id1", true, false);

    // Test update propagation (bulk)
    // This should have propagated to wan site 2
    this.executeSql(sender,
        "Update EMP.TESTTABLE set DESCRIPTION = 'Second' where ID = 1");
    this.sqlExecuteVerify(sender, "select DESCRIPTION from EMP.TESTTABLE",
        goldenFile, "id2", true, false);
    serverExecute(4, getExecutorToWaitForQueuesToDrain("MYSENDER", 0));
    this.sqlExecuteVerify(reciever, "select DESCRIPTION from EMP.TESTTABLE",
        goldenFile, "id2", true, false);

    // Test delete propagation
//    this.executeSql(sender,
//        "Delete from  EMP.TESTTABLE where DESCRIPTION = 'Second'");
//    this.sqlExecuteVerify(sender, "select DESCRIPTION from EMP.TESTTABLE",
//        goldenFile, "empty", true, false);
//    serverExecute(4, getExecutorToWaitForQueuesToDrain("MYSENDER", 0));
//    this.sqlExecuteVerify(reciever, "select DESCRIPTION from EMP.TESTTABLE",
//        goldenFile, "empty", true, false);

//    serverExecute(4, stopSender());
//    this.executeSql(sender, "drop table emp.testtable");
//    this.executeSql(reciever, "drop table emp.testtable");
//    
//    this.executeSql(sender, "drop gatewaysender mysender");
//    this.executeSql(reciever, "drop gatewayreceiver myrcvr");
    
    //this.executeSql(sender, "drop schema emp");
    //this.executeSql(reciever, "drop schema emp");
  }
  
  
  public static Runnable stopSender() {
    CacheSerializableRunnable receiverConf = new CacheSerializableRunnable(
        "Configurator") {
      @Override
      public void run2() throws CacheException {
        try {
          Connection conn = TestUtil.jdbcConn;
          Statement st = conn.createStatement();
          StringBuilder str = new StringBuilder();
          str.append("CALL SYS.STOP_GATEWAYSENDER ('MYSENDER')");
          st.execute(str.toString());
        }
        catch (SQLException sqle) {
          throw GemFireXDRuntimeException.newRuntimeException(null, sqle);
        }
      }
    };
    return receiverConf;
  }
  public static Runnable createGatewayReceiver(final int startPort,
      final int endPort) {
    CacheSerializableRunnable receiverConf = new CacheSerializableRunnable(
        "Receiver Configurator") {
      @Override
      public void run2() throws CacheException {
        try {
          Connection conn = TestUtil.jdbcConn;
          Statement st = conn.createStatement();
          StringBuilder str = new StringBuilder();
          str.append("create gatewayreceiver myrcvr");
          if (startPort != 0) {
            str.append("( startport " + startPort);
          }
          else {
            str.append("( startport " + GatewayReceiver.DEFAULT_START_PORT);
          }
          if (endPort != 0) {
            str.append(" endport " + endPort + ")");
          }
          else {
            str.append(" endport " + GatewayReceiver.DEFAULT_END_PORT + ")");
          }
          str.append(" server groups (sg1)");
          st.execute(str.toString());
        }
        catch (SQLException sqle) {
          throw GemFireXDRuntimeException.newRuntimeException(null, sqle);
        }
      }
    };
    return receiverConf;
  }

  public static Runnable createGatewaySender(final String id,
      final String remoteDs, final String serverGroups) {
    CacheSerializableRunnable senderConf = new CacheSerializableRunnable(
        "Sender Configurator") {
      @Override
      public void run2() throws CacheException {
        try {
          Connection conn = TestUtil.jdbcConn;
          Statement st = conn.createStatement();
          StringBuilder str = new StringBuilder();
          str.append("CREATE gatewaysender " + id + " (remotedsid " + remoteDs
              + " manualstart false isparallel true) server groups (" + serverGroups + ") ");
          st.execute(str.toString());
        }
        catch (SQLException sqle) {
          throw GemFireXDRuntimeException.newRuntimeException(null, sqle);
        }
      }
    };
    return senderConf;
  }*/
}
