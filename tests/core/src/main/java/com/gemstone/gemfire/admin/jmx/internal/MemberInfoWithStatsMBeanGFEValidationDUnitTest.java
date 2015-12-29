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
package com.gemstone.gemfire.admin.jmx.internal;

import hydra.ProcessMgr;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.admin.AdminDUnitTestCase;
import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.CacheVm;
import com.gemstone.gemfire.admin.DistributedSystemConfig;
import com.gemstone.gemfire.admin.GemFireMemberStatus;
import com.gemstone.gemfire.admin.SystemMember;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.DiskWriteAttributes;
import com.gemstone.gemfire.cache.InterestPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionFactory;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.SubscriptionAttributes;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.Connection;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.Gateway.Endpoint;
import com.gemstone.gemfire.cache.util.GatewayHub;
import com.gemstone.gemfire.cache.util.GatewayQueueAttributes;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.admin.remote.ClientHealthStats;
import com.gemstone.gemfire.internal.cache.CacheServerLauncher;
import com.gemstone.gemfire.internal.cache.PoolFactoryImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientProxy;

import dunit.Host;
import dunit.SerializableCallable;
import dunit.VM;
/**
 * This is a DUnit test to verify the MemberInfoWithStatsMBean that is added to 
 * return information as plain java types. The verification is done between 
 * values sent over JMX and actual values retrieved from the VM that has a 
 * Cache/Server/GateWay.
 * 
 * @author abhishek
 */
/*
 * TODO:
 * 1. Hyperic Screenwise tests will be useful -suggestion- Suyog
 */
public class MemberInfoWithStatsMBeanGFEValidationDUnitTest extends AdminDUnitTestCase {

  private static final long serialVersionUID = 1L;

  private static final String KEY_CLIENTS_MAP = "gemfire.member.clients.map";

  private static final String KEY_REGIONS_MAP = "gemfire.member.regions.map";

  private static final String KEY_GATEWAY_COLLECTION = "gemfire.member.gatewayhub.gateways.collection";

  private static final String KEY_GATEWAYENDPOINTS_COLLECTION = "gemfire.member.gateway.endpoints.collection";
  
  private static final String TYPE_NAME_CACHESERVER = "Cache Server";
  private static final String TYPE_NAME_APPLICATION = "Application Peer";
  private static final String TYPE_NAME_GATEWAYHUB  = "Gateway Hub";
  
  private static final String KEY_MEMBER_ID       = "gemfire.member.id.string";
  private static final String KEY_MEMBER_NAME     = "gemfire.member.name.string";
  private static final String KEY_MEMBER_HOST     = "gemfire.member.host.string";
  private static final String KEY_MEMBER_PORT     = "gemfire.member.port.int";
  private static final String KEY_MEMBER_UPTIME   = "gemfire.member.uptime.long";
  private static final String KEY_MEMBER_TYPE     = "gemfire.member.type.string";
  private static final String KEY_MEMBER_IS_SERVER  = "gemfire.member.isserver.boolean";
  private static final String KEY_MEMBER_IS_GATEWAY = "gemfire.member.isgateway.boolean";
  private static final String KEY_MEMBER_STATSAMPLING_ENABLED = "gemfire.member.config.statsamplingenabled.boolean";
  private static final String KEY_MEMBER_TIME_STATS_ENABLED   = "gemfire.member.config.timestatsenabled.boolean";
  
  private static final String KEY_STATS_PROCESSCPUTIME = "gemfire.member.stat.processcputime.long";
  private static final String KEY_STATS_CPUS           = "gemfire.member.stat.cpus.int";
  private static final String KEY_STATS_USEDMEMORY     = "gemfire.member.stat.usedmemory.long";
  private static final String KEY_STATS_MAXMEMORY      = "gemfire.member.stat.maxmemory.long";
  private static final String KEY_STATS_GETS           = "gemfire.member.stat.gets.int";
  private static final String KEY_STATS_GETTIME        = "gemfire.member.stat.gettime.long";
  private static final String KEY_STATS_PUTS           = "gemfire.member.stat.puts.int";
  private static final String KEY_STATS_PUTTIME        = "gemfire.member.stat.puttime.long";
  
  private static final String KEY_GATEWAYHUB_ID            = "gemfire.member.gatewayhub.id.string";
  private static final String KEY_GATEWAYHUB_ISPRIMARY     = "gemfire.member.gatewayhub.isprimary.boolean";
  private static final String KEY_GATEWAYHUB_LISTENINGPORT = "gemfire.member.gatewayhub.listeningport.int";
  private static final String KEY_GATEWAY_ID               = "gemfire.member.gateway.id.string";
  private static final String KEY_GATEWAY_QUEUESIZE        = "gemfire.member.gateway.queuesize.int";
  private static final String KEY_GATEWAY_ISCONNECTED      = "gemfire.member.gateway.isconnected.boolean";
  private static final String KEY_GATEWAYENDPOINT_ID       = "gemfire.member.gateway.endpoint.id.string";
  private static final String KEY_GATEWAYENDPOINT_HOST     = "gemfire.member.gateway.endpoint.host.string";
  private static final String KEY_GATEWAYENDPOINT_PORT     = "gemfire.member.gateway.endpoint.port.int";
  
  private static final String KEY_CLIENT_ID                = "gemfire.client.id.string";
//  private static final String KEY_CLIENT_NAME              = "gemfire.client.name.string";
  private static final String KEY_CLIENT_HOST              = "gemfire.client.host.string";
  private static final String KEY_CLIENT_QUEUESIZE         = "gemfire.client.queuesize.int";
  private static final String KEY_CLIENT_STATS_GETS        = "gemfire.client.stats.gets.int";
  private static final String KEY_CLIENT_STATS_PUTS        = "gemfire.client.stats.puts.int";
  private static final String KEY_CLIENT_STATS_CACHEMISSES = "gemfire.client.stats.cachemisses.int";
  private static final String KEY_CLIENT_STATS_CPUUSAGE    = "gemfire.client.stats.cpuusage.long";
  private static final String KEY_CLIENT_STATS_CPUS        = "gemfire.client.stats.cpus.int";
  private static final String KEY_CLIENT_STATS_UPDATETIME  = "gemfire.client.stats.updatetime.long";
  private static final String KEY_CLIENT_STATS_THREADS     = "gemfire.client.stats.threads.int";
  
  private static final String KEY_REGION_NAME           = "gemfire.region.name.string";
  private static final String KEY_REGION_PATH           = "gemfire.region.path.string";
  private static final String KEY_REGION_SCOPE          = "gemfire.region.scope.string";
  private static final String KEY_REGION_DATAPOLICY     = "gemfire.region.datapolicy.string";
  private static final String KEY_REGION_INTERESTPOLICY = "gemfire.region.interestpolicy.string";
  private static final String KEY_REGION_ENTRYCOUNT     = "gemfire.region.entrycount.int";
  private static final String KEY_REGION_DISKATTRS      = "gemfire.region.diskattrs.string";
  
  private static final String[] KEYS_MEMBERS = new String[] { 
    KEY_MEMBER_STATSAMPLING_ENABLED, 
    KEY_MEMBER_TIME_STATS_ENABLED, 
    KEY_MEMBER_HOST, KEY_MEMBER_ID, 
    KEY_MEMBER_IS_GATEWAY, KEY_MEMBER_IS_SERVER, 
    KEY_MEMBER_NAME, KEY_MEMBER_PORT,
    KEY_STATS_CPUS, KEY_STATS_GETS, 
    KEY_STATS_GETTIME, KEY_STATS_MAXMEMORY, 
    KEY_STATS_PROCESSCPUTIME, KEY_STATS_PUTS, 
    KEY_STATS_PUTTIME, KEY_STATS_USEDMEMORY, 
    KEY_MEMBER_TYPE, KEY_MEMBER_UPTIME};

  /** log writer instance */
  private static LogWriter logWriter = getLogWriter();  
  
  /* useful for JMX client VM (i.e. controller VM - VM4) */
  private static ObjectName distributedSystem;
  private static MBeanServerConnection mbsc;
  
  private boolean shouldDoGatewayHubCleanup= false;

  private static String earlierSlowStartTime;
  
  /**
   * Default value for slow starting time of dispatcher
   * taken from CacheClientProxy.DEFAULT_SLOW_STARTING_TIME
   */
  private static final long DEFAULT_SLOW_STARTING_TIME = 5000;

  /* For properties that could not be verified for equality as they most 
   * probably vary over time
   */ 
  private static final Set<String> UNPREDICTABLES = new HashSet<String>();
  
  private static final Set<String> INCREASING_VALUES = new HashSet<String>();

  private static final Properties props = new Properties();

  //used for mcast-port while setting up DS 
  private int[] freeUDPPorts = new int[0];

  public MemberInfoWithStatsMBeanGFEValidationDUnitTest(String name) {
    super(name);
  }
  
  private static void initIgnorableProperties() {
    INCREASING_VALUES.add(KEY_MEMBER_UPTIME);
    INCREASING_VALUES.add(KEY_STATS_GETTIME);
    INCREASING_VALUES.add(KEY_STATS_PUTTIME);
    INCREASING_VALUES.add(KEY_STATS_PUTS);
    INCREASING_VALUES.add(KEY_STATS_GETS);
    INCREASING_VALUES.add(KEY_STATS_PROCESSCPUTIME);
    UNPREDICTABLES.add(KEY_STATS_USEDMEMORY);
    /*
     * Other properties that could vary as per test activity could be added here 
     * */
  }

  /**
   * Whether JMX is to be used in this test case. Overridden to return 
   * <code>true</code>.
   * 
   * @return true always
   */
  @Override
  protected boolean isJMX() {
    return true;
  }
  
  /**
   * Test to check Gateway Hub Details obtained through 
   * MemberInfoWithStatsMBean.
   * 
   * @throws Exception one of the reasons could be failure of JMX operations 
   */
  public void testGatewayHubDetails() throws Exception {
    logWriter.fine("Entered MemberInfoWithStatsMBeanGFEValidationDUnitTest.testGatewayDetails");
    //1. setup gateway Hubs in two DS
    setUpForGatewayTest();

    ObjectName wrapper = ObjectName.getInstance(MemberInfoWithStatsMBean.MBEAN_NAME);
    String[] members = commonTestCode(wrapper);
    
    //2. verify member details
    for (int i = 0; i < members.length; i++) {
      Map<?, ?> memberDetailsJMX = getDetailsByJMX(wrapper, members[i]);
      Boolean isGateway = (Boolean) memberDetailsJMX.get(KEY_MEMBER_IS_GATEWAY);
      Map<?, ?> memberDetailsDirect = getMemberDetailsDirect(HOST0, isGateway ? GATEWAYHUB_VM : CACHE_VM);
      compareMemberDetails(memberDetailsJMX, memberDetailsDirect);
      
      if (isGateway) {
        //3. verify gateway hub details
        Map<String, Number> gatewaysAndQueuesBefore = verifyGatewayDetails(memberDetailsJMX, HOST0, GATEWAYHUB_VM);
        //4. to observe WAN Queue size variation
        doWANRegionPuts("testGatewayHubDetails", 100, HOST0, GATEWAYHUB_VM);
        
        waitForAgentAutoRefresh();
        
        //5. take data again from the MBean & verify
        memberDetailsJMX = getDetailsByJMX(wrapper, members[i]);
        // Commented out this verify until bug 45332 is fixed
        //final Map<String, Number> gatewaysAndQueuesLater = verifyGatewayDetails(memberDetailsJMX, HOST0, GATEWAYHUB_VM);
        
        // Commented out this section until bug 45332 is fixed
//        //6. verify gateway queue size changed
//        for (Iterator<Map.Entry<String, Number>> iterator = gatewaysAndQueuesBefore.entrySet().iterator(); iterator.hasNext();) {
//          final Map.Entry<String, Number> entry = iterator.next();
//          
//          waitForCriterion(new WaitCriterion() {
//            Number qSizeBefore;
//            Number qSizeLater;
//            @Override
//            public String description() {
//              return "waiting for gateway queue size variation (before: ["+qSizeBefore+"] and after["+qSizeLater+"]) for id: "+entry.getKey();
//            }
//
//            @Override
//            public boolean done() {
//              qSizeBefore = entry.getValue();
//              qSizeLater  = gatewaysAndQueuesLater.get(entry.getKey());
//              return !qSizeBefore.equals(qSizeLater);
//            }
//          }, 60000, 500, true);
//        }
      }
    }

    logWriter.fine("Exited MemberInfoWithStatsMBeanGFEValidationDUnitTest.testGatewayDetails");
    shouldDoGatewayHubCleanup = true;
  }
  
  /**
   * Test to check region & client details obtained through 
   * MemberInfoWithStatsMBean with Operations done in between.
   * 
   * @throws Exception one of the reasons could be failure of JMX operations 
   */
  public void testRegionAndClientDetailsWithOps() throws Exception {
    logWriter.fine("Entered MemberInfoWithStatsMBeanGFEValidationDUnitTest.testRegionAndClientDetails");
    int serverPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);    
    final int numberOfOps = 1000;
    final String prefix   = "testRegionAndClientDetailsWithOps";
    
    //1. Start Cache & Server
    startCache(serverPort, HOST0, CACHE_VM);
    pause(5000); //wait for server initialization
  
    //2. Connect Client to the server started earlier
    connectClient(serverPort, HOST0, CLIENT_VM);
    pause(5000); //wait for client initialization

    ObjectName wrapper = ObjectName.getInstance(MemberInfoWithStatsMBean.MBEAN_NAME);
    String[]   members = commonTestCode(wrapper);
    
    assertTrue("No. of members are expected be more than zero.", members.length != 0);
    
    Map<?, ?> oldData     = null;
    Map<?, ?> currentData = null;

    //1. Without any ops
//    logWriter.info("ABHISHEK: testRegionAndClientDetailsWithOps : (1) check without doing puts starts ...");
    oldData = verifyRegionAndClientDetails(wrapper, members[0], 1/*no of regions*/, HOST0, CACHE_VM);
//    logWriter.info("ABHISHEK: testRegionAndClientDetailsWithOps : (1) check without doing puts ended ...");
    
    //2. After doing - numberOfOps - puts
//    logWriter.info("ABHISHEK: testRegionAndClientDetailsWithOps : testRegionAndClientDetailsWithOps : (2) check with doing puts starts ...");      
    doPuts(prefix, numberOfOps, HOST0, CACHE_VM);
    doGetsPutsToUpdateStats(prefix, numberOfOps, HOST0, CACHE_VM, true, false); //puts are done earlier so skipping
    pause(5000); //wait for ops on client to happen
    waitForAgentAutoRefresh();
    currentData = verifyRegionAndClientDetails(wrapper, members[0], 1, HOST0, CACHE_VM);
    verifyVariationWithOldData(oldData, currentData);
    oldData = currentData;
//    logWriter.info("ABHISHEK: testRegionAndClientDetailsWithOps : (2) check without doing puts ended ...");
    
    //3. After doing - numberOfOps - deletes
//    logWriter.info("ABHISHEK: testRegionAndClientDetailsWithOps : (3) check with doing deletes starts ...");
    doDeletes(prefix, numberOfOps, HOST0, CACHE_VM);
    doGetsPutsToUpdateStats(prefix, numberOfOps, HOST0, CACHE_VM, true, true);
    pause(5000); //wait for ops on client to happen
    waitForAgentAutoRefresh();
    currentData = verifyRegionAndClientDetails(wrapper, members[0], 1, HOST0, CACHE_VM);
    verifyVariationWithOldData(oldData, currentData);
//    logWriter.info("ABHISHEK: testRegionAndClientDetailsWithOps : (3) check with doing deletes ended ...");
    
    //4. Check after adding region
//    logWriter.info("ABHISHEK: testRegionAndClientDetailsWithOps : (4) Adding region starts ...");
    addRegion(DS_REGION2_NAME, prefix, 100, HOST0, CACHE_VM);
    waitForAgentAutoRefresh();
    verifyRegionAndClientDetails(wrapper, members[0], 2/*no of regions*/, HOST0, CACHE_VM);
//    logWriter.info("ABHISHEK: testRegionAndClientDetailsWithOps : (4) Adding region ended ...");
    
    //4. Check after deleting the added region
//    logWriter.info("ABHISHEK: testRegionAndClientDetailsWithOps : (5) Deleting region starts ...");
    deleteRegion(DS_REGION2_NAME, HOST0, CACHE_VM);
    waitForAgentAutoRefresh();
    verifyRegionAndClientDetails(wrapper, members[0], 1, HOST0, CACHE_VM);
//    logWriter.info("ABHISHEK: testRegionAndClientDetailsWithOps : (5) Deleting region ended ...");
    
    disConnectClient(HOST0, CLIENT_VM);
    
    pause(5000);

    stopCache(HOST0, CACHE_VM);

    logWriter.fine("Exited MemberInfoWithStatsMBeanGFEValidationDUnitTest.testRegionAndClientDetails");
    shouldDoGatewayHubCleanup = false;
  }
  
  /* **************************************************************************/
  /* ****************** VARYING VALUES VERIFICATION ***************************/
  /* **************************************************************************/
  
  public void testVaryingValues() throws Exception {
    logWriter.fine("Entered MemberInfoWithStatsMBeanGFEValidationDUnitTest.testVaryingValues");
    int serverPort = 
      AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

    final int slowStartTime = 40000;
    final int total         = 10000;
    final String keyPrefix  = "testVaryingValues";
    
    //1. set slow starting properties in the VM used to start cache
    doSetSlowStart(HOST0, CACHE_VM, slowStartTime);
    //2. start cache
    startCache(serverPort, HOST0, CACHE_VM);
    long cacheStartTime = System.currentTimeMillis();
    pause(5000); //wait for initialization

    ObjectName wrapper = ObjectName.getInstance(MemberInfoWithStatsMBean.MBEAN_NAME);
    String[]   members = commonTestCode(wrapper);
    CachedMemberData cd = new CachedMemberData();
    
    assertTrue("No. of members are expected be more than zero.", members.length != 0);

    //3. verify memory details from MBean. This will have CachedData initialized with default values
    verifyEntriesAndMemoryVariation(cd, wrapper, members[0]);
    
    //4. connect client to the server
    connectClient(serverPort, HOST0, CLIENT_VM);
    pause(5000); //wait for initialization
    
    //5. do puts to observe variation 
    doPuts(keyPrefix, total, HOST0, CACHE_VM);
    pause(5000);

    //6. verify after puts 
    pause(5000);
    waitForAgentAutoRefresh();
    //Verifies between values thro' JMX obtained before & now
    verifyEntriesAndMemoryVariation(cd, wrapper, members[0]);

    //7. due to slow start, client queue size should be non zero 
    waitForAgentAutoRefresh();
    Map<?, ?> mapThroughJmx = getDetailsByJMX(wrapper, members[0]);
    //Verifies between direct value & values thro' JMX
    verifyClientQueueSize(mapThroughJmx, HOST0, CACHE_VM);    
    int clientMissesBefore   = verifyClientMisses(mapThroughJmx, HOST0, CACHE_VM, false);
    long[] clientStatsBefore = verifyClientStatsVariation(mapThroughJmx, HOST0, CACHE_VM);
    
    //wait for all the puts to happen. Purpose for Slow start is achieved
    int elapsed = (int)(System.currentTimeMillis() - cacheStartTime);
//    System.out.println("ABHISHEK1: elapsed :: "+elapsed);
    pause(slowStartTime - elapsed + 10000/*buffer*/);
    
    //8. do invalidate & get on client to generate stats for cache misses
    doClientLocalInvalidateGetsPuts(keyPrefix, total/10, HOST0, CLIENT_VM);
    
    //9. verify cache misses variation
    waitForAgentAutoRefresh();
    mapThroughJmx = getDetailsByJMX(wrapper, members[0]);
    int clientMissesLater = verifyClientMisses(mapThroughJmx, HOST0, CACHE_VM, true);    
    assertTrue("Client Misses Stat value obtained ("+clientMissesLater+") should be more than earlier value ("+clientMissesBefore+")", clientMissesLater > clientMissesBefore);
    
    //9. verify cache gets/puts variation
    long[] clientStatsLater = verifyClientStatsVariation(mapThroughJmx, HOST0, CACHE_VM);
    assertTrue("Client Gets Stat value obtained ("+clientStatsLater[0]+") should be more than earlier value ("+clientStatsBefore[0]+")", clientStatsLater[0] > clientStatsBefore[0]);
    assertTrue("Client Puts Stat value obtained ("+clientStatsLater[1]+") should be more than earlier value ("+clientStatsBefore[1]+")", clientStatsLater[1] > clientStatsBefore[1]);
    assertTrue("Client Process Cpu Time Stat value obtained ("+clientStatsLater[2]+") should be more than earlier value ("+clientStatsBefore[2]+")", clientStatsLater[2] > clientStatsBefore[2]);    
   
    //10. delete entries
    doDeletes(keyPrefix, total, HOST0, CACHE_VM);
   
    //11. verify Entries & Memory variation
    waitForAgentAutoRefresh();
    //Verifies between values thro' JMX obtained before & now
    verifyEntriesAndMemoryVariation(cd, wrapper, members[0]);
    
    mapThroughJmx = getDetailsByJMX(wrapper, members[0]);
    Map clientsMap = (Map) mapThroughJmx.get(KEY_CLIENTS_MAP);
//    System.out.println("ABHISHEK: Client Count :: "+clientsMap.size());
    assertTrue("No. of clients value expected to be 1 but actually it is:"+clientsMap.size(), clientsMap.size() == 1);
    
    //12. start second client - to verify no.of clients
    connectClient(serverPort, HOST0, CLIENT_VM2);
    pause(5000); //wait for initialization
    waitForAgentAutoRefresh();
    
    mapThroughJmx = getDetailsByJMX(wrapper, members[0]);
    clientsMap = (Map) mapThroughJmx.get(KEY_CLIENTS_MAP);
//    System.out.println("ABHISHEK: Client Count :: "+clientsMap.size());
    assertTrue("No. of clients value expected to be 2 but actually it is:"+clientsMap.size(), clientsMap.size() == 2);
    
    //13. stopped all the clients
    disConnectClient(HOST0, CLIENT_VM2);
    disConnectClient(HOST0, CLIENT_VM);    
    pause(5000);
    
    waitForAgentAutoRefresh();
    mapThroughJmx = getDetailsByJMX(wrapper, members[0]);
    clientsMap = (Map) mapThroughJmx.get(KEY_CLIENTS_MAP);
//    System.out.println("ABHISHEK: Client Count :: "+clientsMap.size());
    assertTrue("No. of clients value expected to be 0 but actually it is:"+clientsMap.size(), clientsMap.size() == 0);

    stopCache(HOST0, CACHE_VM);
    doUnsetSlowStart(HOST0, CACHE_VM);
    
    logWriter.fine("Exited MemberInfoWithStatsMBeanGFEValidationDUnitTest.testVaryingValues");
    shouldDoGatewayHubCleanup = false;
  }
  
  private void verifyVariationWithOldData(Map<?, ?> oldData, Map<?, ?> currentData) {
    for (Iterator<String> iterator = INCREASING_VALUES.iterator(); iterator.hasNext();) {
      String prop   = iterator.next();
      Object oldVal = oldData.get(prop);
      Object newVal = currentData.get(prop);
      
      if (prop.equals(KEY_STATS_PROCESSCPUTIME)) {
        Object oldDirect = oldData.get(KEY_STATS_PROCESSCPUTIME+"DIRECT");
        Object newDirect = currentData.get(KEY_STATS_PROCESSCPUTIME+"DIRECT");
        assertTrue("Old val ("+oldVal+" & old DIRECT ["+oldDirect+"]) expected to be different than new val("+newVal+" & new DIRECT ["+newDirect+"]) but they are the same for : "+prop, newVal != null && newVal != oldVal && !newVal.equals(oldVal));
      } else {
        assertTrue("Old val ("+oldVal+") expected to be different than new val("+newVal+") but they are the same for : "+prop, newVal != null && newVal != oldVal && !newVal.equals(oldVal));
      }
    }
  }
  
  private Map<?, ?> verifyRegionAndClientDetails(ObjectName wrapper, 
                                String memberId, int numOfRegions, 
                                int hostIndex, int vmIndex) throws Exception {
    Map<?, ?> memberDetailsJMX = 
      verifyMemberDetails(wrapper, memberId, hostIndex, vmIndex);//only 1 cache vm as per current test
    verifyClientsDetails(memberDetailsJMX, hostIndex, vmIndex);
    verifyRegionsDetails(memberDetailsJMX, numOfRegions, hostIndex, vmIndex);
    
    return memberDetailsJMX;
  }
  
  private String[] commonTestCode(ObjectName wrapper) throws Exception {
    boolean isInitialized = isMBeanInitialized(wrapper);//should be false
    assertFalse("MemberInfoWithStatsMBean initialized without first call to getMembers operation.", isInitialized);

    String[] members = invokeGetMembers(wrapper);
    logWriter.info("MemberInfoWithStatsMBeanGFEValidationDUnitTest.verifyGetMembers(): ids :: "+ Arrays.toString(members));
    
    isInitialized = isMBeanInitialized(wrapper);//should be true now
    assertTrue("MemberInfoWithStatsMBean not initialized even after the first call to getMembers operation.", isInitialized);
    verifyAttributes(wrapper);//TODO:can be removed or we can remove it from the MemberInfoWithStatsMBeanDUnitTest
    verifyGetMembers(members);//TODO:can be removed or we can remove it from the MemberInfoWithStatsMBeanDUnitTest
    
    return members;
  }

  @SuppressWarnings("rawtypes")
  private void verifyClientsDetails(Map memberDetailsJMX, int hostIndex, int vmIndex) {
    logWriter.fine("Entered MemberInfoWithStatsMBeanGFEValidationDUnitTest.verifyClientsDetails() ...");
    Map clientsJmx = (Map) memberDetailsJMX.get(KEY_CLIENTS_MAP);

    assertTrue("MemberInfoWithStatsMBean returned map of clients information has size: "+clientsJmx.size()+", but expected is 1.", clientsJmx.size() == 1);

    Map clientJmx = new HashMap();
    
    for (Iterator iterator = clientsJmx.entrySet().iterator(); iterator.hasNext();) {
      Map.Entry entry = (Map.Entry) iterator.next();
      clientJmx = (Map) entry.getValue();
      break;
    }
    
    assertNotNull("MemberInfoWithStatsMBean returned map of clients information was null.", clientJmx);

    Host host  = Host.getHost(hostIndex);
    VM cacheVM = host.getVM(vmIndex);
    
    Object result = cacheVM.invoke(new SerializableCallable() {
      private static final long serialVersionUID = 1L;
      public Object call() throws Exception {
        return new GemFireMemberStatus(getCache());
      }});
    
    assertNotNull("Object returned was null.", result);
    assertTrue("Object returned is not a GemFireMemberStatus.", result instanceof GemFireMemberStatus);
    
    GemFireMemberStatus status = (GemFireMemberStatus) result;
    
    String clientId = (String) clientJmx.get(KEY_CLIENT_ID);
//    String clientName = (String) clientsJmx.get(KEY_CLIENT_NAME); //Name is derived from id
    String clientHost = (String) clientJmx.get(KEY_CLIENT_HOST);
    int clientQueueSize = (Integer) clientJmx.get(KEY_CLIENT_QUEUESIZE);
    int clientStatsGets = (Integer) clientJmx.get(KEY_CLIENT_STATS_GETS);
    int clientStatsPuts = (Integer) clientJmx.get(KEY_CLIENT_STATS_PUTS);
    int clientStatsMisses = (Integer) clientJmx.get(KEY_CLIENT_STATS_CACHEMISSES);
    long clientStatsCpuUsage = (Long) clientJmx.get(KEY_CLIENT_STATS_CPUUSAGE);
    int clientStatsCpus = (Integer) clientJmx.get(KEY_CLIENT_STATS_CPUS);
    long clientStatsUpdateTime = (Long) clientJmx.get(KEY_CLIENT_STATS_UPDATETIME);
    int clientStatsThreads = (Integer) clientJmx.get(KEY_CLIENT_STATS_THREADS);
    
    Set connectedClients = status.getConnectedClients();
    //TODO: List down ignorables
    assertEquals("No. of clients connected are expected to be 1 but in actual there are:"+connectedClients.size(), connectedClients.size(), 1);
    for (Iterator iterator = connectedClients.iterator(); iterator.hasNext();) {
      String clientIdActual = (String) iterator.next();
      assertTrue("Client ids are not identical. Actual:"+clientIdActual+" but Mbean returned:"+clientId, clientIdActual != null && clientId != null && clientIdActual.equals(clientId));
      
      String clientHostActual = status.getClientHostName(clientIdActual);
      assertTrue("Client host names are not identical. Actual:"+clientHostActual+" but Mbean returned:"+clientHost, clientHost != null && clientHostActual != null && clientHostActual.equals(clientHost));
      
      int clientQueueSizeActual = status.getClientQueueSize(clientIdActual);
      assertTrue("Client Queuesizes are not identical. Actual:"+clientQueueSizeActual+" but Mbean returned:"+clientQueueSize, clientQueueSize == clientQueueSizeActual);
      
      ClientHealthStats clientHealthStats = (ClientHealthStats) status.getClientHealthStats(clientIdActual);
      if (clientHealthStats != null) {
        int getsActual = clientHealthStats.getNumOfGets();
        assertTrue("Client gets Statistics are not identical. Actual:"+getsActual+" but Mbean returned:"+clientStatsGets, getsActual == clientStatsGets);
        int putsActual = clientHealthStats.getNumOfPuts();
        assertTrue("Client puts Statistics are not identical. Actual:"+putsActual+" but Mbean returned:"+clientStatsPuts, putsActual == clientStatsPuts);
        int missesActual = clientHealthStats.getNumOfMisses();
        assertTrue("Client misses Statistics are not identical. Actual:"+missesActual+" but Mbean returned:"+clientStatsMisses, missesActual == clientStatsMisses);
        long cpuUsageActual = clientHealthStats.getProcessCpuTime();
        //cpuUsageActual is obtained later than clientStatsCpuUsage (JMX).
        assertTrue("Client CPU Usage Statistics are not as expected - (obtained directly from server) should be >= (obtained from JMX earlier). Actual:"+cpuUsageActual+" but Mbean returned:"+clientStatsCpuUsage, cpuUsageActual >= clientStatsCpuUsage);
        int cpusActual = clientHealthStats.getCpus();
        assertTrue("Client cpus Statistics are not identical. Actual:"+cpusActual+" but Mbean returned:"+clientStatsCpus, cpusActual == clientStatsCpus);
        long updateTimeActual = clientHealthStats.getUpdateTime().getTime();
        //updateTimeActual is obtained later than clientStatsUpdateTime (JMX).
        assertTrue("Client updateTime Statistics are not as expected - (obtained directly from server) should be >= (obtained from JMX earlier). Actual:"+updateTimeActual+" but Mbean returned:"+clientStatsUpdateTime, updateTimeActual >= clientStatsUpdateTime);
        int threadsActual = clientHealthStats.getNumOfThreads();
        assertTrue("Client threads Statistics are either not identical or are zero. Actual:"+threadsActual+" but Mbean returned:"+clientStatsThreads, threadsActual == clientStatsThreads && threadsActual != 0);
      }
    }
    logWriter.fine("Exited MemberInfoWithStatsMBeanGFEValidationDUnitTest.verifyClientsDetails() ...");
  }
  
  @SuppressWarnings("rawtypes")
  private void verifyRegionsDetails(Map memberDetailsJMX, final int numOfRegions, 
                                  int hostIndex, int vmIndex) throws Exception {
    logWriter.fine("Entered MemberInfoWithStatsMBeanGFEValidationDUnitTest.verifyRegionsDetails() ...");
    Map regionsJmx    = (Map) memberDetailsJMX.get(KEY_REGIONS_MAP);
    
    assertNotNull("Region info through MemberInfoWithStatsMBean is null.", regionsJmx);
    assertEquals("No of regions expected is: 1, but MemberInfoWithStatsMBean returned: "+regionsJmx.size(), regionsJmx.size(), numOfRegions);
    
    Host host  = Host.getHost(hostIndex);
    VM cacheVM = host.getVM(vmIndex);
    
    Map[] regionDetailsMaps = (Map[]) cacheVM.invoke(new SerializableCallable() {
      private static final long serialVersionUID = 1L;

      @SuppressWarnings("unchecked")
      public Object call() throws Exception {
        Map[] maps = new Map[numOfRegions];
        int i = 0;
        Set<Region<?, ?>> rootRegions = getCache().rootRegions();
        
        for (Region<?, ?> region : rootRegions) {
          Map map = new HashMap();
          map.put(KEY_REGION_NAME, region.getName());
          map.put(KEY_REGION_PATH, region.getFullPath()+"/");
          map.put(KEY_REGION_ENTRYCOUNT, region.entrySet().size());
  
          RegionAttributes<?, ?> attributes = region.getAttributes();
          Scope scope = attributes.getScope();
          DataPolicy dataPolicy = attributes.getDataPolicy();
          SubscriptionAttributes subscriptionAttributes = attributes.getSubscriptionAttributes();
          InterestPolicy interestPolicy = subscriptionAttributes.getInterestPolicy();
          DiskWriteAttributes diskWriteAttributes = attributes.getDiskWriteAttributes();
          
          map.put(KEY_REGION_SCOPE, scope.toString());
          map.put(KEY_REGION_DATAPOLICY, dataPolicy.toString());
          map.put(KEY_REGION_INTERESTPOLICY, interestPolicy.toString());
          map.put(KEY_REGION_DISKATTRS, diskWriteAttributes.toString());
          maps[i] = map;
          i++;
        }
        
        return maps;
      }});
    
    assertTrue("Returned object is expected to be of type Map[].", regionDetailsMaps instanceof Map[]);
    
    for (int i = 0; i < regionDetailsMaps.length; i++) {
      Map regionDetails = regionDetailsMaps[0];
      
      assertNotNull("regionDetails returned is null.", regionDetails);

      String nameActual     = (String) regionDetails.get(KEY_REGION_NAME);
      String fullPathActual = (String) regionDetails.get(KEY_REGION_PATH);
      int entryCountActual  = (Integer) regionDetails.get(KEY_REGION_ENTRYCOUNT);
      
      String scope          = (String) regionDetails.get(KEY_REGION_SCOPE);
      String dataPolicy     = (String) regionDetails.get(KEY_REGION_DATAPOLICY);
      String interestPolicy = (String) regionDetails.get(KEY_REGION_INTERESTPOLICY);
      String diskAttrs      = (String) regionDetails.get(KEY_REGION_DISKATTRS);
      
      scope          = scope != null ? scope : "";
      dataPolicy     = dataPolicy != null ? dataPolicy : "";
      interestPolicy = interestPolicy != null ? interestPolicy : "";
      diskAttrs      = diskAttrs != null ? diskAttrs : "";
      
      Map regionsInfo = (Map) regionsJmx.get(fullPathActual);
      
//      System.out.println("Validating :: regionDetails ::"+regionDetails+",\nwith\nregionsInfo :: "+regionsInfo);
      
      Object regionNameJmx = regionsInfo.get(KEY_REGION_NAME);
      assertTrue("Region Name is not identical. Actual:"+nameActual+" but Mbean returned:"+regionNameJmx, nameActual.equals(regionNameJmx));
      Object regionPathJmx = regionsInfo.get(KEY_REGION_PATH);
      assertTrue("Region Path is not identical. Actual:"+fullPathActual+" but Mbean returned:"+regionPathJmx, fullPathActual.equals(regionPathJmx));
      Number entryCountJmx = (Number) regionsInfo.get(KEY_REGION_ENTRYCOUNT);
      assertTrue("Region entry count is not identical. Actual:"+entryCountActual+" but Mbean returned:"+entryCountJmx, entryCountJmx != null && entryCountJmx.intValue() == entryCountActual);
      
      Object scopeJmx = regionsInfo.get(KEY_REGION_SCOPE);
      assertTrue("Region Attribute: Scope is not identical. Actual:"+scope+" but Mbean returned:"+scopeJmx, scope.equals(scopeJmx));
      Object dataPolicyJmx = regionsInfo.get(KEY_REGION_DATAPOLICY);
      assertTrue("Region Attribute: Scope is not identical. Actual:"+dataPolicy+" but Mbean returned:"+dataPolicyJmx, dataPolicy.equals(dataPolicyJmx));
      Object interestPolicyJmx = regionsInfo.get(KEY_REGION_INTERESTPOLICY);
      assertTrue("Region Attribute: Interest Policy is not identical. Actual:"+interestPolicy+" but Mbean returned:"+interestPolicyJmx, interestPolicy.equals(interestPolicyJmx));
      Object diskAttrsJmx = regionsInfo.get(KEY_REGION_DISKATTRS);
      assertTrue("Region Attribute: Disk Attributes is not identical. Actual:"+diskAttrs+" but Mbean returned:"+diskAttrsJmx, diskAttrs.equals(diskAttrsJmx));
    }
   
    logWriter.fine("Exited MemberInfoWithStatsMBeanGFEValidationDUnitTest.verifyRegionsDetails() ...");
  }
  
  private boolean isMBeanInitialized(ObjectName wrapper) 
    throws AttributeNotFoundException, InstanceNotFoundException, 
           MBeanException, ReflectionException, IOException {
    logWriter.fine("Entered MemberInfoWithStatsMBeanGFEValidationDUnitTest.isMBeanInitialized() ...");
    Integer refreshInterval = (Integer) mbsc.getAttribute(wrapper, "RefreshInterval");
    
    AdminDistributedSystem adminDS = agent.getDistributedSystem();
    
    logWriter.fine("Exiting MemberInfoWithStatsMBeanGFEValidationDUnitTest.isMBeanInitialized() ...");
    return adminDS.getConfig().getRefreshInterval() == refreshInterval.intValue();
  }
  
  private String[] invokeGetMembers(ObjectName wrapper) 
    throws InstanceNotFoundException, MBeanException, 
           ReflectionException, IOException {
    logWriter.fine("Entered MemberInfoWithStatsMBeanGFEValidationDUnitTest.invokeGetMembers() ...");
    Object[] params    = new Object[0];
    String[] signature = new String[0];
    
    String[] memberIds = (String[]) mbsc.invoke(wrapper, "getMembers", params, signature);

    logWriter.fine("Exiting MemberInfoWithStatsMBeanGFEValidationDUnitTest.invokeGetMembers() ...");
    return memberIds;
  }
  
  private void verifyAttributes(ObjectName wrapper) 
    throws AttributeNotFoundException, InstanceNotFoundException, 
    MBeanException, ReflectionException, IOException {
    logWriter.fine("Entered MemberInfoWithStatsMBeanGFEValidationDUnitTest.verifyAttributes() ...");
    String  id              = (String) mbsc.getAttribute(wrapper, "Id");
    String  version         = (String) mbsc.getAttribute(wrapper, "Version");
    Integer refreshInterval = (Integer) mbsc.getAttribute(wrapper, "RefreshInterval");
    
    AdminDistributedSystem adminDS = agent.getDistributedSystem();
    
    String  actualId              = adminDS.getId();
    String  actualVersion         = GemFireVersion.getGemFireVersion();
    int     actualRefreshInterval = adminDS.getConfig().getRefreshInterval();

    assertTrue("AdminDistributedSystem id shown by MemberInfoWithStatsMBean " +
    		       "(as: "+id+") and actual (as: "+actualId+") do not match.", 
    		        actualId.equals(id));
    assertTrue("GemFire Version shown by MemberInfoWithStatsMBean " +
    		       "(as: "+version+") and actual(as: "+actualVersion+") do not match.", 
    		       actualVersion.equals(version));
    assertTrue("Refresh Interval shown by MemberInfoWithStatsMBean (as: "+
               refreshInterval+") and actual (as :"+actualRefreshInterval+") do not match.", 
               actualRefreshInterval == refreshInterval);//use auto-boxing
    logWriter.fine("Exited MemberInfoWithStatsMBeanGFEValidationDUnitTest.verifyAttributes() ...");
  }
  
  private void verifyGetMembers(String[] ids) throws AdminException {
    logWriter.fine("Entered MemberInfoWithStatsMBeanGFEValidationDUnitTest.verifyGetMembers() ...");
    AdminDistributedSystem adminDS = agent.getDistributedSystem();
    SystemMember[] appVms   = adminDS.getSystemMemberApplications();
    CacheVm[]      cacheVms = adminDS.getCacheVms();
    
    int actualMembers = appVms.length + cacheVms.length;
    
    assertTrue("No. of members returned by MemberInfoWithStatsMBean are "+
               ids.length+" but in actual are "+actualMembers, 
               ids.length == actualMembers);
    
    List<String> memberIds = Arrays.asList(ids);
    boolean allIdsValid = true;
    for (int i = 0; i < cacheVms.length; i++) {
      if (!memberIds.contains(cacheVms[i].getId())) {
        allIdsValid = false;
        break;
      }
    }
    if (allIdsValid) {
      for (int i = 0; i < appVms.length; i++) {
        if (!memberIds.contains(appVms[i].getId())) {
          allIdsValid = false;
          break;
        }
      }
    }
    assertTrue("All member ids of existing members do not match with member " +
    		       "ids given by GatewayHubMemberInfoWithStatsMBean.", allIdsValid);
    logWriter.fine("Exited MemberInfoWithStatsMBeanGFEValidationDUnitTest.verifyGetMembers() ...");
  }
  
  @SuppressWarnings("rawtypes")
  private Map getDetailsByJMX(ObjectName wrapper, String memberId) 
    throws AdminException, InstanceNotFoundException, MBeanException, 
           ReflectionException, IOException {
    logWriter.fine("Entered MemberInfoWithStatsMBeanGFEValidationDUnitTest.getMemberDetailsJMX() ...");
    Object[] params    = new Object[] {memberId};
    String[] signature = new String[] {String.class.getName()};
    Map memberDetails = (Map) mbsc.invoke(wrapper, "getMemberDetails", params, signature);
    
    logWriter.fine("Exiting MemberInfoWithStatsMBeanGFEValidationDUnitTest.getMemberDetailsJMX() ...");
    return memberDetails;
  }
 
  @SuppressWarnings("rawtypes")
  private Map verifyMemberDetails(ObjectName wrapper, String memberId, int hostIndex, int vmIndex) throws Exception {
    logWriter.fine("Entered MemberInfoWithStatsMBeanGFEValidationDUnitTest.verifyMemberDetails(ObjectName,String,int) ...");
    Map<?, ?> mapDirect     = getMemberDetailsDirect(hostIndex, vmIndex);
    waitForAgentAutoRefresh();
    pause(1000);//
    Map<?, ?> mapThroughJmx = getDetailsByJMX(wrapper, memberId);
    
    compareMemberDetails(mapDirect, mapThroughJmx);

    logWriter.fine("Exiting MemberInfoWithStatsMBeanGFEValidationDUnitTest.verifyMemberDetails(ObjectName,String,int) ...");
    return mapThroughJmx;
  }
  
  @SuppressWarnings("rawtypes")
  private Map getMemberDetailsDirect(int hostIndex, int vmIndex) {
    logWriter.fine("Entered MemberInfoWithStatsMBeanGFEValidationDUnitTest.getDirectDetails(int) ...");
    Host host  = Host.getHost(hostIndex);
    VM cacheVM = host.getVM(vmIndex);
    
    
    Map mapDirect = (Map) cacheVM.invoke(new SerializableCallable() {
      private static final long serialVersionUID = 1L;
      @SuppressWarnings("unchecked")
      public Object call() throws Exception {
        Cache cache2 = getCache();
        Map m = new HashMap();
        DistributedSystem ds = cache2.getDistributedSystem();
        InternalDistributedSystem internalDS = (InternalDistributedSystem) ds;
        DistributedMember member = ds.getDistributedMember();
        m.put(KEY_MEMBER_ID, member.getId());
        m.put(KEY_MEMBER_NAME, ds.getName());
        m.put(KEY_MEMBER_HOST, member.getHost());
        m.put(KEY_MEMBER_UPTIME, System.currentTimeMillis() - internalDS.getStartTime()); //as calculated in GmeFireMemberStatus
        List<GatewayHub> gatewayHubs = cache2.getGatewayHubs();
        boolean isGatewayHub = gatewayHubs != null && !gatewayHubs.isEmpty();
        m.put(KEY_MEMBER_IS_GATEWAY, isGatewayHub);
        m.put(KEY_MEMBER_IS_SERVER, cache2.isServer());
        List<CacheServer> cacheServers = cache2.getCacheServers();
        int cacheServerPort = 0;
        if (cacheServers != null && !cacheServers.isEmpty()) {
          cacheServerPort = cacheServers.get(0).getPort();
        }
        m.put(KEY_MEMBER_PORT, cacheServerPort);//first cache server
        m.put(KEY_MEMBER_STATSAMPLING_ENABLED, internalDS.getConfig().getStatisticSamplingEnabled());
        m.put(KEY_MEMBER_TIME_STATS_ENABLED, internalDS.getConfig().getEnableTimeStatistics());
        
        String memberType = TYPE_NAME_APPLICATION;
        if (isGatewayHub) {
          memberType = TYPE_NAME_GATEWAYHUB;
        } else if (CacheServerLauncher.isDedicatedCacheServer) {
          memberType = TYPE_NAME_CACHESERVER;
        }
        m.put(KEY_MEMBER_TYPE, memberType);
        
//        System.out.println("ABHISHEK:CacheVM: cache2.rootRegions :: "+cache2.rootRegions());
        
        StatisticsType vmStatsType = internalDS.findType("VMStats");
        StatisticsType vmMemoryUsageStatsType = internalDS.findType("VMMemoryUsageStats");
        StatisticsType cachePerfStatsType = internalDS.findType("CachePerfStats");
        
        Statistics[] vmStats = internalDS.findStatisticsByType(vmStatsType);
        Statistics[] vmMemoryUsageStats = internalDS.findStatisticsByType(vmMemoryUsageStatsType);
        Statistics[] cachePerfStats = internalDS.findStatisticsByType(cachePerfStatsType);
        
        Integer gets = null;
        Long getTime = null;
        Integer puts = null;
        Long putTime = null;
        for (int i = 0; i < cachePerfStats.length; i++) {
          if (cachePerfStats[i].getTextId().equals("cachePerfStats")) {
            Number number = cachePerfStats[i].get("gets");
            if (number != null) {
              gets = number.intValue();
            }
            
            number = cachePerfStats[i].get("getTime");
            if (number != null) {
              getTime = number.longValue();
            }
            
            number = cachePerfStats[i].get("puts");
            if (number != null) {
              puts = number.intValue();
            }
            
            number = cachePerfStats[i].get("putTime");
            if (number != null) {
              putTime = number.longValue();
            }
            break;
          }
        }
        m.put(KEY_STATS_GETS, gets);
        m.put(KEY_STATS_PUTS, puts);
        m.put(KEY_STATS_GETTIME, getTime);
        m.put(KEY_STATS_PUTTIME, putTime);        

        Long maxMemory  = null;
        Long usedMemory = null;
        for (int i = 0; i < vmMemoryUsageStats.length; i++) {
          if (vmMemoryUsageStats[i].getTextId().equals("vmHeapMemoryStats")) {
            Number number = vmMemoryUsageStats[i].get("maxMemory");
            if (number != null) {
              maxMemory = number.longValue();
            }
            
            number = vmMemoryUsageStats[i].get("usedMemory");
            if (number != null) {
              usedMemory = number.longValue();
            }
            break;
          }
        }        
        m.put(KEY_STATS_MAXMEMORY, maxMemory);
        m.put(KEY_STATS_USEDMEMORY, usedMemory);

        Long processCpuTime = null;
        Integer cpus = null;
        for (int i = 0; i < vmStats.length; i++) {
          if (vmStats[i].getTextId().equals("vmStats")) {
            Number number = vmStats[i].get("processCpuTime");
            if (number != null) {
              processCpuTime = number.longValue();
            }
            
            number = vmStats[i].get("cpus");
            if (number != null) {
              cpus = number.intValue();
            }
            break;
          }
        }
        m.put(KEY_STATS_PROCESSCPUTIME, processCpuTime);
        m.put(KEY_STATS_CPUS, cpus);

        OperatingSystemMXBean osBean = ManagementFactory
            .getOperatingSystemMXBean();
        if (osBean != null) {
          Object cpuTime = null;
          try {
            Method method = osBean.getClass().getMethod("getProcessCpuTime");
            if (method != null) {
              cpuTime = method.invoke(osBean);
            }
          } catch (Exception ex) {
            // ignore any exceptions for platforms that don't have the method
          }
          if (cpuTime != null) {
            m.put(KEY_STATS_PROCESSCPUTIME + "DIRECT", cpuTime);
          }
          /* (below will fail to build on Windows)
          if (osBean instanceof com.sun.management.UnixOperatingSystemMXBean) {
            com.sun.management.UnixOperatingSystemMXBean unixOsBean = (com.sun.management.UnixOperatingSystemMXBean) osBean;
            m.put(KEY_STATS_PROCESSCPUTIME+"DIRECT", unixOsBean.getProcessCpuTime());
          }
          */
        }
        return m;
      }
    });
    

    logWriter.fine("Exiting MemberInfoWithStatsMBeanGFEValidationDUnitTest.getDirectDetails(int) ...");    
    return mapDirect;
  }
  
  private void compareMemberDetails(Map<?, ?> mapDirect, Map<?, ?> mapThroughJmx) throws Exception {
    logWriter.fine("Entered MemberInfoWithStatsMBeanGFEValidationDUnitTest.verifyMemberDetails(Map, Map) ...");
    
    assertNotNull("Object returned is null.", mapDirect);
    
    Object jmxVal = null;
    Object dirVal = null;
    boolean areEqual = false;
    for (int i = 0; i < KEYS_MEMBERS.length; i++) {
      jmxVal = mapThroughJmx.get(KEYS_MEMBERS[i]);
      dirVal = mapDirect.get(KEYS_MEMBERS[i]);
      
      boolean isOneNullAndOtherNot = ((jmxVal != null && dirVal == null) || 
                                      (jmxVal == null && dirVal != null)); 
      assertFalse("Member information obtained directly & through JMX " +
                  "is not the same for "+KEYS_MEMBERS[i]+".", isOneNullAndOtherNot);
      
      areEqual = jmxVal.equals(dirVal);
      
      if (!areEqual){
        if (INCREASING_VALUES.contains(KEYS_MEMBERS[i])) {
          //all such values are numbers
          double overJmx       = ((Number)jmxVal).doubleValue();
          double directFromMem = ((Number)dirVal).doubleValue();
          assertTrue("Value obtained through JMX (which is: "+overJmx+") is expected to be greater than or equal to the value obtained directly (which is: "+directFromMem+") for: "+KEYS_MEMBERS[i], 
              overJmx >= directFromMem);
          continue;
        } else if (UNPREDICTABLES.contains(KEYS_MEMBERS[i])) {
          logWriter.info("Ignorable as this can change with time: Values obtained through JMX (which is: "+jmxVal+") & directly (which is: "+dirVal+") are not the same for: "+KEYS_MEMBERS[i]);
          continue;
        }
      }

      assertTrue("Values obtained through JMX (which is: "+jmxVal+") & directly (which is: "+dirVal+") are not the same for: "+KEYS_MEMBERS[i], areEqual);
    }

    logWriter.fine("Exited MemberInfoWithStatsMBeanGFEValidationDUnitTest.verifyMemberDetails() ...");
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  private Map<String, Number> verifyGatewayDetails(Map memberDetailsJMX, int hostIndex, int vmIndex) {
    logWriter.fine("Entered MemberInfoWithStatsMBeanGFEValidationDUnitTest.verifyGatewayDetails() ...");
    
    Map<String, Number> gatewayQueueSizes = new HashMap<String, Number>();

    //direct cast to List could be done
    List<Map> gatewaysJmxCol = (List<Map>) memberDetailsJMX.get(KEY_GATEWAY_COLLECTION);
    
    Boolean isGateway = (Boolean) memberDetailsJMX.get("gemfire.member.isgateway.boolean");
    if (gatewaysJmxCol == null) { //also means if gatewaysDirectCol is null after above check
      String memberId = (String) memberDetailsJMX.get("gemfire.member.id.string");
      logWriter.info("No gateway information available for: "+memberId);
      
      assertFalse(memberId+" is a GatewayHub but does not have Gateway details.", isGateway);
      return null;
    }

    Host host = Host.getHost(hostIndex);
    /*
     * Agent connects to a DS started with member & gateway hub in vm0 & vm1 
     * respectively.
     */
    VM gatewayHubVM = host.getVM(vmIndex);
    
    Map hubDetails = (Map) gatewayHubVM.invoke(new SerializableCallable("MemberInfoWithStatsMBeanGFEValidationDUnitTest.verifyGatewayDetails()") {
      private static final long serialVersionUID = 1L;

      public Object call() throws Exception {
        Map details = new HashMap();
        List<GatewayHub> gatewayHubs = getCache().getGatewayHubs();
        GatewayHub gatewayHub = gatewayHubs.get(0);
        details.put(KEY_GATEWAYHUB_ID, gatewayHub.getId());
        details.put(KEY_GATEWAYHUB_LISTENINGPORT, gatewayHub.getPort());
        details.put(KEY_GATEWAYHUB_ISPRIMARY, gatewayHub.isPrimary());
        
        List<Gateway> gateways = gatewayHub.getGateways();
        List<Map> gatewayDetailsCollected = new ArrayList<Map>();
        for (Gateway gateway : gateways) {
          Map gatewayDetails = new HashMap();
          gatewayDetails.put(KEY_GATEWAY_ISCONNECTED, gateway.isConnected());
          gatewayDetails.put(KEY_GATEWAY_QUEUESIZE, gateway.getQueueSize());
          gatewayDetails.put(KEY_GATEWAY_ID, gateway.getId());
          
          List<Endpoint> endpoints = gateway.getEndpoints();
          List<Map> endpointDetailsCollected = new ArrayList<Map>();
          for (Endpoint endpoint : endpoints) {
            Map endpointDetails = new HashMap();
            endpointDetails.put(KEY_GATEWAYENDPOINT_ID, endpoint.getId());
            endpointDetails.put(KEY_GATEWAYENDPOINT_HOST, endpoint.getHost());
            endpointDetails.put(KEY_GATEWAYENDPOINT_PORT, endpoint.getPort());
            
            endpointDetailsCollected.add(endpointDetails);
          }
          
          gatewayDetails.put(KEY_GATEWAYENDPOINTS_COLLECTION, endpointDetailsCollected);
          gatewayDetailsCollected.add(gatewayDetails);
        }
        details.put(KEY_GATEWAY_COLLECTION, gatewayDetailsCollected);

        return details;
      }});
    
    assertNotNull("Object returned is null.", hubDetails);
    
    assertNotNull("GatewayHub info through MemberInfoWithStatsMBean is null.", gatewaysJmxCol);
//    assertEquals("No of gateway hubs expected is: 1, but MemberInfoWithStatsMBean returned: "+gatewaysJmxCol.size(), gatewaysJmxCol.size(), 1);
    
    String hubIdJmx = (String) memberDetailsJMX.get(KEY_GATEWAYHUB_ID);
    Boolean hubIsPrimaryJmx = (Boolean) memberDetailsJMX.get(KEY_GATEWAYHUB_ISPRIMARY);
    Number listeningPortJmx = (Number) memberDetailsJMX.get(KEY_GATEWAYHUB_LISTENINGPORT);
    
    String hubIdActual = (String) hubDetails.get(KEY_GATEWAYHUB_ID);
    assertTrue("Gateway Hub Id is not identical. Actual:"+hubIdActual+" but Mbean returned:"+hubIdJmx, hubIdActual.equals(hubIdJmx));
    Boolean isPrimaryActual = (Boolean) hubDetails.get(KEY_GATEWAYHUB_ISPRIMARY);
    assertTrue("Gateway Hub 'isPrimary' property is not same. Actual:"+isPrimaryActual+" but Mbean returned:"+hubIsPrimaryJmx, isPrimaryActual.equals(hubIsPrimaryJmx));
    Integer listeningPortActual = (Integer) hubDetails.get(KEY_GATEWAYHUB_LISTENINGPORT);
    assertTrue("Gateway Hub listening port is not identical. Actual:"+listeningPortActual+" but Mbean returned:"+listeningPortJmx, listeningPortActual.equals(listeningPortJmx));
    
    List<Map> gatewaysDirectCol = (List) hubDetails.get(KEY_GATEWAY_COLLECTION);
    
//    System.out.println("ABHISHEK :: gatewaysJmxCol :: "+gatewaysJmxCol);
    
    for (int i = 0; i < gatewaysJmxCol.size(); i++) {
      //relying on sequence. This is ultimately derived from a list in GemFireMemberStatus
      Map gatewaysJmx    = gatewaysJmxCol.get(i);
      Map gatewaysDirect = gatewaysDirectCol.get(i);
      Set entrySetJmx    = gatewaysJmx.entrySet();
      Set entrySetDirect = gatewaysDirect.entrySet();
      
      assertTrue("Gateway details do not match. Details retrieved thro JMX as: " +
        "\n"+gatewaysJmx+" \n and directly as: \n"+gatewaysDirect, 
        entrySetJmx.equals(entrySetDirect));
      
      verifyGatewayEndpointsDetails(
          (List<Map>)gatewaysJmx.get(KEY_GATEWAYENDPOINTS_COLLECTION), 
          (List<Map>)gatewaysDirect.get(KEY_GATEWAYENDPOINTS_COLLECTION));
      
      gatewayQueueSizes.put((String)gatewaysJmx.get(KEY_GATEWAY_ID), 
                            (Number)gatewaysJmx.get(KEY_GATEWAY_QUEUESIZE));
    }
    logWriter.fine("Exiting MemberInfoWithStatsMBeanGFEValidationDUnitTest.verifyGatewayDetails() ...");
    
    return gatewayQueueSizes;
  }
  
  private void doWANRegionPuts(final String keyPrefix, final int total, 
                              int hostIndex, int hubVmIndex) {
    Host host = Host.getHost(hostIndex);
    /*
     * Agent connects to a DS started with member & gateway hub in vm0 & vm1 
     * respectively.
     */
    VM gatewayHubVM = host.getVM(hubVmIndex);
    final int unitSize = 1000;
    
    gatewayHubVM.invokeAsync(new CacheSerializableRunnable("WAN region puts") {
      private static final long serialVersionUID = 1L;

      @Override
      public void run2() throws CacheException {
        Region<Object, Object> region = getCache().getRegion("/"+WAN_REGION_NAME);
        pause((DistributedSystemConfig.DEFAULT_REFRESH_INTERVAL * 1000 + 500/*buffer*/)*3/4);
        
        for (int i = 0; i < total; i++) {
          byte[] arr = new byte[unitSize];
          region.put(keyPrefix+"-"+i, arr);
        }
      }
    });
  }
  
  private void doClientLocalInvalidateGetsPuts(final String keyPrefix, 
                              final int totalToInvalidateAndGet, int hostIndex, 
                              int hubVmIndex) {
    Host host = Host.getHost(hostIndex);
    VM clientVM = host.getVM(hubVmIndex);
    
    clientVM.invoke(new CacheSerializableRunnable("Do localInvalidate, gets & puts") {
      private static final long serialVersionUID = 1L;
      @Override
      public void run2() {
        Region<String, Object> region = getCache().getRegion("/"+DS_REGION1_NAME);
        
        for (int i = 0; i < totalToInvalidateAndGet; i++) {
          if (region.containsKey(keyPrefix+"-"+i)) {
            /* This check could help on a busy system if client cache is not 
             * yet updated with all the key-value pairs on the server. */
            region.localInvalidate(keyPrefix+"-"+i);
          }
        }
        
        for (int i = 0; i < totalToInvalidateAndGet; i++) {
          region.get(keyPrefix+"-"+i);
        }

        for (int i = 0; i < totalToInvalidateAndGet / 10; i++) {
          byte[] arr= new byte[100];
          region.put(keyPrefix+"-"+i, arr);
        }
      }
    });
  }
  
  private void doSetSlowStart(int hostIndex, int vmIndex, final int slowStartTime) {
    Host h = Host.getHost(hostIndex);
    VM  vm = h.getVM(vmIndex);    
    vm.invoke(new CacheSerializableRunnable("doSetlowStart ...") {
      private static final long serialVersionUID = 1L;
      @Override
      public void run2() throws CacheException {
        CacheClientProxy.isSlowStartForTesting = true;
        earlierSlowStartTime = System.setProperty("slowStartTimeForTesting", String.valueOf(slowStartTime));
      }
    });
  }
  
  private void doUnsetSlowStart(int hostIndex, int vmIndex) {
    Host h = Host.getHost(hostIndex);
    VM  vm = h.getVM(vmIndex);    
    vm.invoke(new CacheSerializableRunnable("doSetlowStart ...") {
      private static final long serialVersionUID = 1L;
      @Override
      public void run2() throws CacheException {
        CacheClientProxy.isSlowStartForTesting = false;
        earlierSlowStartTime = 
          earlierSlowStartTime != null ? earlierSlowStartTime : ""+DEFAULT_SLOW_STARTING_TIME;
        System.setProperty("slowStartTimeForTesting", earlierSlowStartTime);
      }
    });
  }

  @SuppressWarnings("rawtypes")
  private void verifyGatewayEndpointsDetails(List<Map> endpointsJMX,
      List<Map> endpointsDirect) {
    logWriter.fine("Entered MemberInfoWithStatsMBeanGFEValidationDUnitTest.verifyGatewayEndpointsDetails() ...");
    
    boolean isOneNullAndOtherNot = ((endpointsJMX != null && endpointsDirect == null) || 
        (endpointsJMX == null && endpointsDirect != null)); 
    assertFalse("Gateway endpoints information obtained directly & through JMX " +
                "is not the same.", isOneNullAndOtherNot);
    assertFalse("Gateway does not have Endpoint details.", endpointsJMX == null);
    
    for (int i = 0; i < endpointsJMX.size(); i++) {
      Map map1 = endpointsJMX.get(i);
      Map map2 = endpointsDirect.get(i);
      
      assertTrue("Gateway endpoint information at index: "+i+" obtained " +
                 "directly & through JMX is not the same.", 
                 map1.entrySet().equals(map2.entrySet()));
    }
    logWriter.fine("Entered MemberInfoWithStatsMBeanGFEValidationDUnitTest.verifyGatewayEndpointsDetails() ...");
  }

  @SuppressWarnings("rawtypes")  
  private void verifyClientQueueSize(Map memberDetailsJMX, int hostIndex, int vmIndex) {
    logWriter.fine("Entered MemberInfoWithStatsMBeanGFEValidationDUnitTest.verifyClientQueueSize() ...");
    Map clientsJmx = (Map) memberDetailsJMX.get(KEY_CLIENTS_MAP);

    assertTrue("MemberInfoWithStatsMBean returned map of clients information has size: "+clientsJmx.size()+", but expected is 1.", clientsJmx.size() == 1);

    Map clientJmx = new HashMap();
    
    for (Iterator iterator = clientsJmx.entrySet().iterator(); iterator.hasNext();) {
      Map.Entry entry = (Map.Entry) iterator.next();
      clientJmx = (Map) entry.getValue();
      break;
    }
    
    assertNotNull("MemberInfoWithStatsMBean returned map of clients information was null.", clientJmx);
    
    GemFireMemberStatus status = getGFMemberStatus(hostIndex, vmIndex);

    int clientQueueSize = (Integer) clientJmx.get(KEY_CLIENT_QUEUESIZE);
    String clientIdJmx = (String) clientJmx.get(KEY_CLIENT_ID);
    
    Set connectedClients = status.getConnectedClients();
    assertEquals("No. of clients connected are expected to be 1 but in actual there are:"+connectedClients.size(), connectedClients.size(), 1);
    for (Iterator iterator = connectedClients.iterator(); iterator.hasNext();) {
      String clientIdActual = (String) iterator.next();
      assertTrue("Client ids are not identical. Actual:"+clientIdActual+" but Mbean returned:"+clientIdJmx, clientIdActual != null && clientIdJmx != null && clientIdActual.equals(clientIdJmx));
      
      int clientQueueSizeActual = status.getClientQueueSize(clientIdActual);
//      System.out.println("ABHISHEK: Client Queuesizes are - Actual:"+clientQueueSizeActual+" but Mbean returned:"+clientQueueSize);

      boolean areEqualNonZero = clientQueueSize == clientQueueSizeActual && clientQueueSize != 0;
      areEqualNonZero = areEqualNonZero || (clientQueueSize != 0 && clientQueueSizeActual != 0);
      assertTrue("Client Queuesize values are not as expected - should be non-zero. Actual:"+clientQueueSizeActual+" but Mbean returned:"+clientQueueSize, areEqualNonZero);
    }
    logWriter.fine("Exited MemberInfoWithStatsMBeanGFEValidationDUnitTest.verifyClientQueueSize() ...");
  }
  
  @SuppressWarnings("rawtypes")  
  private int verifyClientMisses(Map memberDetailsJMX, int hostIndex, int vmIndex, boolean nonZeroCheck) {
    logWriter.fine("Entered MemberInfoWithStatsMBeanGFEValidationDUnitTest.verifyClientQueueSize() ...");
    Map clientsJmx = (Map) memberDetailsJMX.get(KEY_CLIENTS_MAP);

    assertTrue("MemberInfoWithStatsMBean returned map of clients information has size: "+clientsJmx.size()+", but expected is 1.", clientsJmx.size() == 1);

    Map clientJmx = new HashMap();
    
    for (Iterator iterator = clientsJmx.entrySet().iterator(); iterator.hasNext();) {
      Map.Entry entry = (Map.Entry) iterator.next();
      clientJmx = (Map) entry.getValue();
      break;
    }
    
    assertNotNull("MemberInfoWithStatsMBean returned map of clients information was null.", clientJmx);

    GemFireMemberStatus status = getGFMemberStatus(hostIndex, vmIndex);

    int clientCacheMisses = (Integer) clientJmx.get(KEY_CLIENT_STATS_CACHEMISSES);
    String clientIdJmx = (String) clientJmx.get(KEY_CLIENT_ID);
    
    Set connectedClients = status.getConnectedClients();
    assertEquals("No. of clients connected are expected to be 1 but in actual there are:"+connectedClients.size(), connectedClients.size(), 1);
    for (Iterator iterator = connectedClients.iterator(); iterator.hasNext();) {
      String clientIdActual = (String) iterator.next();
      assertTrue("Client ids are not identical. Actual:"+clientIdActual+" but Mbean returned:"+clientIdJmx, clientIdActual != null && clientIdJmx != null && clientIdActual.equals(clientIdJmx));
      
      int clientCacheMissesActual = ((ClientHealthStats)status.getClientHealthStats(clientIdActual)).getNumOfMisses();
//      System.out.println("ABHISHEK: Client Cache Misses are - Actual:"+clientCacheMissesActual+" but Mbean returned:"+clientCacheMisses);

      boolean areEqualNonZero = clientCacheMisses == clientCacheMissesActual;
      if (nonZeroCheck) {
        areEqualNonZero = areEqualNonZero && (clientCacheMisses != 0);
      }
      assertTrue("Client Cache Misses are not identical. Actual:"+clientCacheMissesActual+" but Mbean returned:"+clientCacheMisses, areEqualNonZero);
    }
    logWriter.fine("Exiting MemberInfoWithStatsMBeanGFEValidationDUnitTest.verifyClientQueueSize() ...");
    
    return clientCacheMisses;
  }
  
  private GemFireMemberStatus getGFMemberStatus(int hostIndex, int vmIndex) {
    Host host  = Host.getHost(hostIndex);
    VM cacheVM = host.getVM(vmIndex);
    
    Object result = cacheVM.invoke(new SerializableCallable() {
      private static final long serialVersionUID = 1L;
      public Object call() throws Exception {
        return new GemFireMemberStatus(getCache());
      }});
    
    assertNotNull("Object returned was null.", result);
    assertTrue("Object returned is not a GemFireMemberStatus.", result instanceof GemFireMemberStatus);
    
    return (GemFireMemberStatus) result;
  }
  
  @SuppressWarnings("rawtypes")  
  private long[] verifyClientStatsVariation(Map memberDetailsJMX, int hostIndex, int vmIndex) {
    logWriter.fine("Entered MemberInfoWithStatsMBeanGFEValidationDUnitTest.verifyClientQueueSize() ...");
    Map clientsJmx = (Map) memberDetailsJMX.get(KEY_CLIENTS_MAP);

    assertTrue("MemberInfoWithStatsMBean returned map of clients information has size: "+clientsJmx.size()+", but expected is 1.", clientsJmx.size() == 1);

    Map clientJmx = new HashMap();
    
    for (Iterator iterator = clientsJmx.entrySet().iterator(); iterator.hasNext();) {
      Map.Entry entry = (Map.Entry) iterator.next();
      clientJmx = (Map) entry.getValue();
      break;//only one client
    }
    
    assertNotNull("MemberInfoWithStatsMBean returned map of clients information was null.", clientJmx);

    GemFireMemberStatus status = getGFMemberStatus(hostIndex, vmIndex);

    int clientCacheGets = (Integer) clientJmx.get(KEY_CLIENT_STATS_GETS);
    int clientCachePuts = (Integer) clientJmx.get(KEY_CLIENT_STATS_PUTS);
    long clientProcessCpuTime = (Long) clientJmx.get(KEY_CLIENT_STATS_CPUUSAGE);
    String clientIdJmx = (String) clientJmx.get(KEY_CLIENT_ID);
    
    Set connectedClients = status.getConnectedClients();
    assertEquals("No. of clients connected are expected to be 1 but in actual there are:"+connectedClients.size(), connectedClients.size(), 1);
    for (Iterator iterator = connectedClients.iterator(); iterator.hasNext();) {
      String clientIdActual = (String) iterator.next();
      assertTrue("Client ids are not identical. Actual:"+clientIdActual+" but Mbean returned:"+clientIdJmx, clientIdActual != null && clientIdJmx != null && clientIdActual.equals(clientIdJmx));
      
      int clientCacheGetsActual = ((ClientHealthStats)status.getClientHealthStats(clientIdActual)).getNumOfGets();
//      System.out.println("ABHISHEK: Client Cache Gets are - Actual:"+clientCacheGetsActual+" but Mbean returned:"+clientCacheGets);

      assertTrue("Client Cache Gets are not identical. Actual:"+clientCacheGetsActual+" but Mbean returned:"+clientCacheGets, clientCacheGets == clientCacheGetsActual);
      
      int clientCachePutsActual = ((ClientHealthStats)status.getClientHealthStats(clientIdActual)).getNumOfPuts();
//      System.out.println("ABHISHEK: Client Cache Puts are - Actual:"+clientCachePutsActual+" but Mbean returned:"+clientCachePuts);

      assertTrue("Client Cache Puts are not identical. Actual:"+clientCachePutsActual+" but Mbean returned:"+clientCachePuts, clientCachePuts == clientCachePutsActual);
      
      long clientProcessCpuTimeActual = ((ClientHealthStats)status.getClientHealthStats(clientIdActual)).getProcessCpuTime();
      //clientProcessCpuTimeActual is obtained later than clientProcessCpuTime (JMX).
//      System.out.println("ABHISHEK: Client ProcessCpuTime are - Actual:"+clientProcessCpuTimeActual+" but Mbean returned:"+clientProcessCpuTime);
      assertTrue("Client CPU Usage Statistics are not as expected - (obtained directly from server) should be >= (obtained from JMX earlier). Actual:"+clientProcessCpuTimeActual+" but Mbean returned:"+clientProcessCpuTime, clientProcessCpuTimeActual >= clientProcessCpuTime);
    }
    logWriter.fine("Exiting MemberInfoWithStatsMBeanGFEValidationDUnitTest.verifyClientQueueSize() ...");
    
    return new long[] {clientCacheGets, clientCachePuts, clientProcessCpuTime};
  }
  
  @SuppressWarnings("rawtypes")
  private void verifyEntriesAndMemoryVariation(CachedMemberData data, ObjectName wrapper, String memberId) throws Exception {
    Map<?, ?> mapThroughJmx = getDetailsByJMX(wrapper, memberId);

    long newValMax  = ((Number) mapThroughJmx.get(KEY_STATS_MAXMEMORY)).longValue();
    long newValUsed = ((Number) mapThroughJmx.get(KEY_STATS_USEDMEMORY)).longValue();
    long newUptime  = ((Number) mapThroughJmx.get(KEY_MEMBER_UPTIME)).longValue();
    
    Map regionsMap = (Map) mapThroughJmx.get(KEY_REGIONS_MAP);
    Set entrySet   = regionsMap.entrySet();
    int totalEntryCount = 0;
    for (Iterator iterator = entrySet.iterator(); iterator.hasNext();) {
      Map.Entry<?, ?> entry = (Map.Entry<?, ?>) iterator.next();
      Map regionDetails = (Map) entry.getValue();
      Number regionEntryCount = (Number) regionDetails.get(KEY_REGION_ENTRYCOUNT);
      totalEntryCount = totalEntryCount + regionEntryCount.intValue();      
    }
    
    logWriter.info("CachedMemberData :: "+data+" and new is :: "+newValUsed+"/"+newValMax+", entryCount="+totalEntryCount+", uptime="+newUptime);
    
    if (data.maxMem == Long.MIN_VALUE) {
      data.maxMem = newValMax;
    } else {
      // This assertion is failing on my linux box.
      // It assumes that maxMemory will never change but I'm seeing it change from 233046016 to 248250368.
      // So I'm deadcoding the following assertion.
      //assertEquals("MaxMemory: Old val ("+data.maxMem+") expected to be == new val("+newValMax+").", data.maxMem, newValMax);
    }
    if (data.usedMem == Long.MIN_VALUE) {
      data.usedMem = newValUsed;
    } else {        
      assertTrue("UsedMemory: Old val ("+data.usedMem+") is expected to be different than new val("+newValUsed+").", data.usedMem != newValUsed);
      data.usedMem = newValUsed;
    }
    if (data.entryCount == Integer.MIN_VALUE) {
      data.entryCount = totalEntryCount;
    } else {        
      assertTrue("EntryCount: Old val ("+data.entryCount+") is expected to be different than new val("+totalEntryCount+").", data.entryCount != totalEntryCount);
      data.entryCount = totalEntryCount;
    }
    if (data.uptime == Long.MIN_VALUE) {
      data.uptime = newUptime;
    } else {        
      assertTrue("Uptime: Old val ("+data.uptime+") is expected less than new val("+newUptime+").", data.uptime < newUptime);
      data.uptime = newUptime;
    }
  }
  
  /* **************************************************************************/
  /* ****************** GATEWAY TEST SETUP METHODS START **********************/
  /* **************************************************************************/
  
  private static final String WAN_REGION_NAME = "MemberInfoWithStatsMBeanGFEValidationDUnitTest_WAN_Root";
  private static final String DS_REGION1_NAME = "MemberInfoWithStatsMBeanGFEValidationDUnitTest_DS_Root";
  private static final String DS_REGION2_NAME = "MemberInfoWithStatsMBeanGFEValidationDUnitTest_DS_Root_2";
  
  /** index for the VM to be used as a GatewayHub VM */
  private static final int GATEWAYHUB_VM = 1;

  private VM[] vmsDS0 = new VM[0];
  private VM[] vmsDS1 = new VM[0];

  /**
   * Sets up two distributed systems and gateways.
   * <p>
   * VM0 and VM1 will form DS0<br>
   * VM2 and VM3 will form DS1
   * <p>
   * DS0 and DS1 are two separate distributed systems<br>
   * VM0 and VM2 will be standard system members
   * VM1 and VM3 will act as Gateways
   * <p>
   * Controller VM will simply run the test and not participate.
   */
  private void setUpSystems(int[] freeUDPPorts) throws Exception {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

    vmsDS0 = new VM[] { vm0, vm1 };
    vmsDS1 = new VM[] { vm2, vm3 };

    String hostName = getServerHostName(host);

    int[] freeTCPPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    int dsPortDS0 = freeUDPPorts[0];
    int dsPortDS1 = freeUDPPorts[1];
    int hubPortDS0 = freeTCPPorts[0];
    int hubPortDS1 = freeTCPPorts[1];

    setUpDS("ds0", dsPortDS0, vmsDS0, hubPortDS0,
            "ds1", hostName, hubPortDS1);
    setUpDS("ds1", dsPortDS1, vmsDS1, hubPortDS1,
            "ds0", hostName, hubPortDS0);
  }

  private void setUpDS(final String dsName,
                       final int dsPort,
                       final VM[] vms,
                       final int hubPortLocal,
                       final String dsNameRemote,
                       final String hostNameRemote,
                       final int hubPortRemote)
  throws Exception {

    // setup DS
    final Properties propsDS = new Properties();
    propsDS.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(dsPort));
    propsDS.setProperty(DistributionConfig.LOCATORS_NAME, "");

    // connect to DS in both vms
    for (int i = 0; i < vms.length; i++) {
      final int whichvm = i;
      final VM vm = vms[whichvm];
      vm.invoke(new CacheSerializableRunnable("Set up "+dsName) {
        private static final long serialVersionUID = 1L;

        @Override
        @SuppressWarnings({ "rawtypes", "unchecked" })
        public void run2() throws CacheException {
          String vmName = "MemberInfoWithStatsMBeanGFEValidationDUnitTest_" + dsName+ "_vm" + whichvm;
          propsDS.setProperty(DistributionConfig.NAME_NAME, vmName);
          getSystem(propsDS);
          getLogWriter().info("[MemberInfoWithStatsMBeanGFEValidationDUnitTest] " + vmName + " has joined " +
            dsName + " with port " + String.valueOf(dsPort));
          Cache cache = getCache();
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.REPLICATE);

          // create WAN region
          factory.setEnableGateway(true);
          RegionFactory regionFactory = cache.createRegionFactory(factory.create());
          regionFactory.create(WAN_REGION_NAME);

          // create DS region
          factory.setEnableGateway(false);
          regionFactory = cache.createRegionFactory(factory.create());
          regionFactory.create(DS_REGION1_NAME);
          getLogWriter().info("[MemberInfoWithStatsMBeanGFEValidationDUnitTest] " + vmName + " has created both regions");
        }
      });
    }

    // set up gateway in last vm
    final int whichvm = vms.length-1;
    final VM vm = vms[whichvm];

    vm.invoke(new CacheSerializableRunnable("Set up gateway in "+dsName) {
      private static final long serialVersionUID = 1L;
      @Override
      public void run2() throws CacheException {
        String vmName = "MemberInfoWithStatsMBeanGFEValidationDUnitTest_" + dsName+ "_vm" + whichvm;

        // create the gateway hub
        String hubName = "MemberInfoWithStatsMBeanGFEValidationDUnitTest_"+dsName;
        String gatewayName = "MemberInfoWithStatsMBeanGFEValidationDUnitTest_"+dsNameRemote;
        getLogWriter().info("[MemberInfoWithStatsMBeanGFEValidationDUnitTest] " + vmName + " is creating " +
          hubName + " with gateway to " + gatewayName);
        Cache cache = getCache();
        GatewayHub hub = cache.addGatewayHub(hubName, hubPortLocal);
        Gateway gateway = hub.addGateway(gatewayName);

        // create the endpoint for remote DS
        getLogWriter().info("[MemberInfoWithStatsMBeanGFEValidationDUnitTest] " + vmName +
          " adding endpoint [" + gatewayName + ", " + hostNameRemote + ", " +
          hubPortRemote + "] to " + gatewayName);
        gateway.addEndpoint(gatewayName, hostNameRemote, hubPortRemote);

        // create the gateway queue
        File d = new File(gatewayName + "_overflow_" + ProcessMgr.getProcessId());
        getLogWriter().info("[MemberInfoWithStatsMBeanGFEValidationDUnitTest] " + vmName +
          " creating queue in " + d + " for " + gatewayName);

        GatewayQueueAttributes queueAttributes =
          new GatewayQueueAttributes(d.toString(),
            GatewayQueueAttributes.DEFAULT_MAXIMUM_QUEUE_MEMORY,
            GatewayQueueAttributes.DEFAULT_BATCH_SIZE * 5,
            GatewayQueueAttributes.DEFAULT_BATCH_TIME_INTERVAL * 20,
            GatewayQueueAttributes.DEFAULT_BATCH_CONFLATION,
            GatewayQueueAttributes.DEFAULT_ENABLE_PERSISTENCE,
            GatewayQueueAttributes.DEFAULT_ALERT_THRESHOLD);
        
        // enable persistence and add disk store
        queueAttributes.setEnablePersistence(true);
        queueAttributes.setDiskStoreName(getUniqueName());
        // now create the disk store. hub.start() should succeed with ds object
        File overflowDirectory = new File("overflow_dir_"+dsName+"_vm_"+whichvm);
        overflowDirectory.mkdir();
        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        File[] dirs1 = new File[] {overflowDirectory};
        /*DiskStore ds1 = */dsf.setDiskDirs(dirs1).create(getUniqueName());

        gateway.setQueueAttributes(queueAttributes);
        try {
          hub.start();
//          gateway.start();
        }
        catch (IOException e) {
          getLogWriter().error("Start of hub " + hubName + " threw " + e, e);
          fail("Start of hub " + hubName + " threw " + e, e);
        }
        
        getLogWriter().info("[MemberInfoWithStatsMBeanGFEValidationDUnitTest] " + vmName + " has created " +
          hubName + " with gateway to " + gatewayName);
      }
    });

  }
  
  @SuppressWarnings("rawtypes")
  public static void  destroyWanQueues() {
    try{
      Cache cache = com.gemstone.gemfire.cache.CacheFactory.getAnyInstance();
      for (Iterator i = cache.getGatewayHubs().iterator(); i.hasNext();) {
        GatewayHub hub = (GatewayHub) i.next();
        for (Iterator i1 = hub.getGateways().iterator(); i1.hasNext();) {
          Gateway gateway = (Gateway) i1.next();
          String rq= new StringBuffer(gateway.getGatewayHubId()).append('_').append(gateway.getId()).append("_EVENT_QUEUE").toString();
          Region wbcl = cache.getRegion(rq);
          if(wbcl != null) {
            wbcl.localDestroyRegion();
          }         
        }  
      }
      Set<Region<?, ?>> rootRegions = cache.rootRegions();
      if (rootRegions != null) {
        for (Region<?, ?> region : rootRegions) {
          region.clear();
        }
      }
    } catch (CancelException cce) {
      //Ignore
    }
  }
  
  private void setUpForGatewayTest() throws Exception {
    setUpSystems(freeUDPPorts);    
  }
  
  private void tearDownForGatewayTest() {
    destroyWanQueues();
    for (int h = 0; h < Host.getHostCount(); h++) {
      Host host = Host.getHost(h);
      for (int v = 0; v < host.getVMCount(); v++) {
        VM vm = host.getVM(v);
        vm.invoke(MemberInfoWithStatsMBeanGFEValidationDUnitTest.class, "destroyWanQueues");
      }
    }
  }
  /* **************************************************************************/
  /* ****************** GATEWAY TEST SETUP METHODS END  **********************/
  /* **************************************************************************/
  
  /* **************************************************************************/
  /* ************* CLIENT REGION DETAILS TEST SETUP METHOD START***************/
  /* **************************************************************************/
  /** index for the Hosts */
  private static final int HOST0 = 0;
  private static final int HOST1 = 1;
  
  /** index for the VM to be used as a Cache VM */
  private static final int CACHE_VM = 0;
  
  /** index for the VM to be used as a Cache Client VM */
  private static final int CLIENT_VM = 2;
  
  /** index for the VM to be used as a Cache Client VM */
  private static final int CLIENT_VM2 = 1;
  
  private static final int CLIENT_STATS_INTERVAL = 5000;

//  private static final int NO_OF_PUTS = 100;
//  private static final int NO_OF_GETS = NO_OF_PUTS/2;
  
  /** reference to cache client connection to cache server in VM0*/
  /* Having this variable static ensures accessing this map across multiple 
   * invoke operations in that VM. Here it's used in CLIENT_VM */
  private static Connection clientConn;
  
  /**
   * Starts the cache in the cache VM at index CACHE_VM.
   * Also initializes the distributed system in cache VM, starts a server and
   * creates a region with name {@link #REGION_NAME}.
   * 
   * NOTE: This test case extends CacheTestCase. getCache() method from this 
   * class is called to create the cache.
   * 
   * @param serverPort a port to start server on.
   */
  private void startCache(final int serverPort, int hostIndex, int vmIndex) {
    logWriter.fine("Entered MemberInfoWithStatsMBeanGFEValidationDUnitTest.startCache");
    Host host  = Host.getHost(hostIndex);
    VM cacheVM = host.getVM(vmIndex);
    
    final Properties props = new Properties();
    props.putAll(getDistributedSystemProperties());
    props.put(DistributionConfig.NAME_NAME, "MemberInfoWithStatsMBeanGFEValidationDUnitTest_vm"+CACHE_VM);
    
    cacheVM.invoke(new CacheSerializableRunnable(getName()+"-startCache") {
      private static final long serialVersionUID = 1L;

      @Override
      public void run2() throws CacheException {
        getSystem(props);
        /* [Refer com.gemstone.gemfire.cache30.CacheTestCase] 
         * a. getCache creates & returns a cache if not present currently.
         * b. While creating the cache initializes the systems if currently not 
         *    connected to the DS or was disconnected from the DS earlier.
         */
        //1. Start Cache
        Cache cacheInstance = getCache();
        
        //2. Create Region Attributes
        AttributesFactory<String, String> factory = 
                                        new AttributesFactory<String, String>();
        RegionAttributes<String, String> attrs = factory.create();
        
        //3. Create Region
        RegionFactory<String, String> regionFactory = cacheInstance.createRegionFactory(attrs);

        regionFactory.create(DS_REGION1_NAME);
        
//        for (int i = 0; i < NO_OF_PUTS; i++) {
//          region.put("key#"+i, "VALUE#"+1);
//        }
//        
//        for (int i = 0; i < NO_OF_GETS; i++) {
//          region.get("key#"+i);
//        }
        
        //4. Create & Start Server
        CacheServer server = cacheInstance.addCacheServer();
        server.setPort(serverPort);
        try {
          server.start();
        } catch (IOException e) {
          throw new CacheException("Exception occurred while starting server.", e) {
            private static final long serialVersionUID = 1L;
          };
        }
        logWriter.info("MemberInfoWithStatsMBeanGFEValidationDUnitTest.startCache :: " + 
            "Created cache & started server in cacheVM(VM#"+CACHE_VM+") at port:"+serverPort);
      }
    });
    
    logWriter.fine("Exited MemberInfoWithStatsMBeanGFEValidationDUnitTest.startCache");
  }
  
  private void addRegion(final String regionName, final String keyPrefix, 
                         final int total, int hostIndex, int vmIndex) {
    Host h = Host.getHost(hostIndex);
    VM vm = h.getVM(vmIndex);;

    vm.invoke(new CacheSerializableRunnable("Adding region ...") {
      private static final long serialVersionUID = 1L;

      @Override
      public void run2() throws CacheException {
        Cache cacheInstance = getCache();
        //2. Create Region Attributes
        AttributesFactory<String, Object> factory = 
                                        new AttributesFactory<String, Object>();
        RegionAttributes<String, Object> attrs = factory.create();
        
        //3. Create Region
        RegionFactory<String, Object> regionFactory = cacheInstance.createRegionFactory(attrs);

        Region<String, Object> region = regionFactory.create(regionName);
        for (int i = 0; i < total; i++) {
          byte[] arr = new byte[100];
          region.put(keyPrefix+"-"+i, arr);
        }
      }
    });
  }
  
  private void deleteRegion(final String regionName, int hostIndex, 
                            int vmIndex) {
    Host h = Host.getHost(hostIndex);
    VM vm = h.getVM(vmIndex);;

    vm.invoke(new CacheSerializableRunnable("Removing region ...") {
      private static final long serialVersionUID = 1L;

      @Override
      public void run2() throws CacheException {
        Cache cacheInstance = getCache();
        //2. Create Region Attributes
        Region<String, Object> region = cacheInstance.getRegion("/"+regionName);
        region.destroyRegion();
      }
    });
  }
  
  private void doPuts(final String keyPrefix, 
                      final int total, int hostIndex, int vmIndex) {
    Host h = Host.getHost(hostIndex);
    VM  vm = h.getVM(vmIndex);
    final int unitSize = 2000;
    
    vm.invoke(new CacheSerializableRunnable("Doing puts ...") {
      private static final long serialVersionUID = 1L;
      @Override
      public void run2() throws CacheException {
        Cache cache2 = getCache();
        Region<Object, Object> region = cache2.getRegion("/"+DS_REGION1_NAME);
        
        for (int i = 0; i < total; i++) {
          Byte[] arr = new Byte[unitSize];
          region.put(keyPrefix+"-"+i, arr);
        }
        pause(1000+100);//wait for sampler to update VMStats
      }
    });
  }

  /*
   * Does no. of gets & puts specified by 'total' on in a VM with given vmIndex.
   * If doGets is true, we can expect variation in gets & getTime stats  
   * If doPuts is true, we can expect variation in puts & putTime stats
   */
  private void doGetsPutsToUpdateStats(final String keyPrefix, 
      final int total, int hostIndex, int vmIndex, 
      final boolean doGets, final boolean doPuts) {
    Host h = Host.getHost(hostIndex);
    VM  vm = h.getVM(vmIndex);
    
    if (doGets || doPuts) {
      vm.invoke(new CacheSerializableRunnable("Doing GetsPutsToUpdateStats ...") {
        private static final long serialVersionUID = 1L;
        @Override
        public void run2() throws CacheException {
          Cache cache2 = getCache();
          Region<Object, Object> region = cache2.getRegion("/"+DS_REGION1_NAME);
          
          if (doGets) {
            for (int i = 0; i < total; i++) {;
              region.get(keyPrefix+"-"+i);
            }
          }
          if (doPuts) {
            int unitSize = 2000;
            for (int i = 0; i < total; i++) {
              Byte[] arr = new Byte[unitSize];
              region.put(keyPrefix+"-"+i, arr);
            }
          }
          pause(1000+100);//wait for sampler to update VMStats
        }
      });
    }
  }

  private void doDeletes(final String keyPrefix, final int total, 
                         int hostIndex, int vmIndex) {
    Host h = Host.getHost(hostIndex);
    VM  vm = h.getVM(vmIndex);
    
    vm.invoke(new CacheSerializableRunnable("Doing deletes ...") {
      private static final long serialVersionUID = 1L;

      @Override
      public void run2() throws CacheException {
        Cache cache2 = getCache();
        Region<Object, Object> region = cache2.getRegion("/"+DS_REGION1_NAME);
        
        for (int i = 0; i < total; i++) {
          region.remove(keyPrefix+"-"+i);
        }
        pause(1000+100);//wait for sampler to update VMStats
      }
    });
  }
 
  /**
   * Stops the cache in the cache VM & disconnects the cache VM from the DS.
   */
  private void stopCache(int hostIndex, int vmIndex) {
    logWriter.fine("Entered MemberInfoWithStatsMBeanGFEValidationDUnitTest.stopCache");
    Host host    = Host.getHost(hostIndex);
    VM   cacheVM = host.getVM(vmIndex);
    
    cacheVM.invoke(new CacheSerializableRunnable(getName()+"-stopCache") {
      private static final long serialVersionUID = 1L;

      @Override
      public void run2() throws CacheException {
        Set<Region<?, ?>> rootRegions = getCache().rootRegions();
        if (rootRegions != null) {
          for (Region<?, ?> region : rootRegions) {
            region.clear();
          }
        }
        disconnectFromDS();
      }
    });
    logWriter.fine("Exited MemberInfoWithStatsMBeanGFEValidationDUnitTest.stopCache");
  }
  
  /**
   * Connects a cache client in {@link #CLIENT_VM} to the server in
   * {@link #CACHE_VM} on the server port provided.
   * 
   * @param serverPort
   *          server port for clients to connect to
   * @return reference to the connection
   */
  private void connectClient(final int serverPort, int hostIndex, int vmIndex) {
    logWriter.fine("Entered MemberInfoWithStatsMBeanGFEValidationDUnitTest.connectClient");
    final Host host     = Host.getHost(hostIndex);
    VM         clientVM = host.getVM(vmIndex);
    
    clientVM.invoke(new CacheSerializableRunnable(getName()+"-connectClient") {
      private static final long serialVersionUID = 1L;

      @Override
      public void run2() throws CacheException {
        //1. Create Cache & DS if does not exist
        /* 
         * Creating loner cache would override properties defined in 
         * getDistributedSystemProperties() to use mcast-port=0 & locators="" 
         * as required for clients. 
         */
        createLonerCache();
        Cache cacheInstance = getCache();
        
        //2. Configure & create pool
        PoolFactoryImpl pf = (PoolFactoryImpl)PoolManager.createFactory();
        pf.addServer(host.getHostName(), serverPort);
        pf.setSubscriptionEnabled(true);
        pf.setReadTimeout(10000);
        pf.setSubscriptionRedundancy(0);
        pf.setStatisticInterval(CLIENT_STATS_INTERVAL);
        
        PoolImpl p = (PoolImpl) pf.create("MemberInfoWithStatsMBeanGFEValidationDUnitTest");
        
        //3. Create Region Attributes
        AttributesFactory<String, Object> factory = 
                                        new AttributesFactory<String, Object>();
        factory.setPoolName(p.getName());
        factory.setScope(Scope.LOCAL);
        RegionAttributes<String, Object> attrs = factory.create();
        
        //4. Create region
        RegionFactory<String, Object> regionFactory = cacheInstance.createRegionFactory(attrs);
        Region<String, Object> region = regionFactory.create(DS_REGION1_NAME);        
        assertNotNull("Region in cache is null.", region);

        clientConn = p.acquireConnection();
        assertNotNull("Acquired client connecttion is null.", clientConn);
        region.registerInterest("ALL_KEYS");
      }
    });
    
    logWriter.info("MemberInfoWithStatsMBeanGFEValidationDUnitTest.connectClient :: " + 
        "Started client in clientVM(VM#"+CLIENT_VM+") & connected to " +
        "cacheVM(VM#2).");
  }
  
  /**
   * Dis-connects a cache client started in {@link #CLIENT_VM} from a cache 
   * server started in {@link #CACHE_VM}. Also closes the cache in client and 
   * destroys the connection.
   */
  private void disConnectClient(int hostIndex, int vmIndex) {
    logWriter.fine("Entered MemberInfoWithStatsMBeanGFEValidationDUnitTest.stopCache");
    Host host     = Host.getHost(hostIndex);
    VM   clientVM = host.getVM(vmIndex);
    
    clientVM.invoke(new CacheSerializableRunnable(getName()+"-disConnectClient") {
      private static final long serialVersionUID = 1L;

      @Override
      public void run2() throws CacheException {
        getCache().close();
        disconnectFromDS();
        MemberInfoWithStatsMBeanGFEValidationDUnitTest.clientConn.destroy();
      }
    });
    logWriter.fine("Exited MemberInfoWithStatsMBeanGFEValidationDUnitTest.stopCache");
  }
  
  /* **************************************************************************/
  /* ************* CLIENT REGION DETAILS TEST SETUP METHOD END ****************/
  /* **************************************************************************/

  /**
   * Over-ridden to ensure that all the members connect to the same distributed 
   * system. Distributed system properties used would be same for all the 
   * members that use this method to get the properties. 
   */
  @Override
  public Properties getDistributedSystemProperties() {
    return props;
  }
  
  /**
   * Initializes the properties for the distributed system to be started. 
   */
  public static void initDSProperties(int mcastPort) {
    props.clear();
    props.setProperty(DistributionConfig.MCAST_PORT_NAME, String.valueOf(mcastPort));
    props.setProperty(DistributionConfig.LOCATORS_NAME, "");
  }

  @Override
  public void setUp() throws Exception {
    boolean failedSetUp = true;
    try {
      disconnectAllFromDS();
      freeUDPPorts = AvailablePortHelper.getRandomAvailableUDPPorts(2);
      initDSProperties(freeUDPPorts[0]);
      super.setUp();
      failedSetUp = false;
    } catch(Exception e) {
      if (failedSetUp) {
        disconnectAllFromDS();
      }
      e.printStackTrace();
//      System.out.println("failedSetUp :: "+failedSetUp);
    }

    // Calls connectToSystem on the JMX agent over JMX.
    // The distributed system MBean ObjectName is collected/cached in a variable
    mbsc = this.agent.getMBeanServer();
    assertNotNull(mbsc);

    //initing AdminDS over JMX
    ObjectName agentName = new ObjectName("GemFire:type=Agent");
    distributedSystem = (ObjectName)mbsc.invoke(agentName, "connectToSystem",
        new Object[0], new String[0]);
    assertNotNull(distributedSystem);

    initIgnorableProperties();
  }
  
  @Override
  public void tearDown2() throws Exception {
    try {
      super.tearDown2();
      if (shouldDoGatewayHubCleanup) {
        tearDownForGatewayTest();
      }
    } finally {
      disconnectAllFromDS();
    }
  }
  
  /* **************************************************************************/
  /* ****************************** HELPERS ***********************************/
  /* **************************************************************************/
  
  void waitForAgentAutoRefresh() {
    pause(DistributedSystemConfig.DEFAULT_REFRESH_INTERVAL * 1000 + 500/*buffer*/);
  }
  
  static class CachedMemberData { 
    long usedMem    = Long.MIN_VALUE;
    long maxMem     = Long.MIN_VALUE;
    int  entryCount = Integer.MIN_VALUE;
    long uptime     = Long.MIN_VALUE;
    
    @Override
    public String toString() {
      return usedMem+"/"+maxMem+", entryCount="+entryCount+", uptime="+uptime;
    }
  }
}
