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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
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
import javax.management.OperationsException;
import javax.management.ReflectionException;

import hydra.ProcessMgr;

import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.admin.AdminDUnitTestCase;
import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.CacheVm;
import com.gemstone.gemfire.admin.SystemMember;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.client.internal.Connection;
import com.gemstone.gemfire.cache.client.internal.PoolImpl;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.Gateway;
import com.gemstone.gemfire.cache.util.GatewayHub;
import com.gemstone.gemfire.cache.util.GatewayQueueAttributes;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.cache.PoolFactoryImpl;

import dunit.Host;
import dunit.VM;
/**
 * This is a DUnit test to verify the MemberInfoWithStatsMBean that is added to 
 * return information as plain java types. The verification is done between 
 * values sent over JMX and values retrieved directly through this MBean.
 * 
 * @author abhishek
 */
public class MemberInfoWithStatsMBeanDUnitTest extends AdminDUnitTestCase {

  private static final long serialVersionUID = 1L;

  private static final String KEY_CLIENTS_MAP = "gemfire.member.clients.map";

  private static final String KEY_REGIONS_MAP = "gemfire.member.regions.map";

  private static final String KEY_GATEWAY_COLLECTION = "gemfire.member.gatewayhub.gateways.collection";

  private static final String KEY_GATEWAYENDPOINTS_COLLECTION = "gemfire.member.gateway.endpoints.collection";;
  
  private static final String[] KEYS_MEMBERS = new String[] { 
      "gemfire.member.config.statsamplingenabled.boolean", 
      "gemfire.member.config.timestatsenabled.boolean", 
      "gemfire.member.host.string", "gemfire.member.id.string", 
      "gemfire.member.isgateway.boolean", "gemfire.member.isserver.boolean", 
      "gemfire.member.name.string", "gemfire.member.port.int",
      "gemfire.member.stat.cpus.int", "gemfire.member.stat.gets.int", 
      "gemfire.member.stat.gettime.long", "gemfire.member.stat.maxmemory.long", 
      "gemfire.member.stat.processcputime.long", "gemfire.member.stat.puts.int", 
      "gemfire.member.stat.puttime.long", "gemfire.member.stat.usedmemory.long", 
      "gemfire.member.type.string", "gemfire.member.uptime.long"};

  /** log writer instance */
  private static LogWriter logWriter = getLogWriter();  
  
  /** pause timeout */
  private static int PAUSE_TIME = 15;
  
  /* useful for JMX client VM (i.e. controller VM - VM4) */
  private static ObjectName distributedSystem;
  private static MBeanServerConnection mbsc;
  
  private boolean recentlyRanGatewayTest = false;

  //for properties that could not be verified for equality as they most probably vary over time
  private static final Set<String> IGNORABLES = new HashSet<String>();

  private static final Properties props = new Properties();
  //used for mcast-port while setting up DS 
  private int[] freeUDPPorts = new int[0];

  public MemberInfoWithStatsMBeanDUnitTest(String name) {
    super(name);
  }
  
  private static void initIgnorableProperties() {
    IGNORABLES.add("gemfire.member.uptime.long");
    IGNORABLES.add("gemfire.member.stat.gettime.long");
    IGNORABLES.add("gemfire.member.stat.puttime.long");
    IGNORABLES.add("gemfire.member.stat.processcputime.long");
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
   * Test to check clean up of managed stats resources.
   * 
   * @throws Exception one of the reasons could be failure of JMX operations 
   */
  public void testRegionAndClientDetails() throws Exception {
    logWriter.fine("Entered MemberInfoWithStatsMBeanDUnitTest.testRegionAndClientDetails");
    int serverPort = 
      AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    
    startCache(serverPort);
    pause(1000*PAUSE_TIME);
  
    connectClient(serverPort);
    pause(500*PAUSE_TIME);

    ObjectName wrapper = ObjectName.getInstance(MemberInfoWithStatsMBean.MBEAN_NAME);
    String[]   members = commonTestCode(wrapper);

    for (String member : members) {
//      verifyGetMemberClientRegionDetails(wrapper, members[i]);
      verifyGetMemberDetails(wrapper, member, false);
    }
    
    disConnectClient();
    
    pause(1000*PAUSE_TIME);

    stopCache();

    logWriter.fine("Exited MemberInfoWithStatsMBeanDUnitTest.testRegionAndClientDetails");
    recentlyRanGatewayTest = false;
  }  
  
  /**
   * Test to check clean up of managed stats resources.
   * 
   * @throws Exception one of the reasons could be failure of JMX operations 
   */
  public void testGatewayDetails() throws Exception {
    logWriter.fine("Entered MemberInfoWithStatsMBeanDUnitTest.testGatewayDetails");
    setUpForGatewayTest();

    ObjectName wrapper = ObjectName.getInstance(MemberInfoWithStatsMBean.MBEAN_NAME);
    String[] members = commonTestCode(wrapper);

    for (String member : members) {
//      verifyGetMemberGatewayDetails(wrapper, member);
      verifyGetMemberDetails(wrapper, member, true);
    }
    
    pause(1000*PAUSE_TIME);

    logWriter.fine("Exited MemberInfoWithStatsMBeanDUnitTest.testGatewayDetails");
    recentlyRanGatewayTest = true;
  }
  
  private String[] commonTestCode(ObjectName wrapper) throws Exception {
    boolean isInitialized = isMBeanInitialized(wrapper);//should be false
    assertFalse("MemberInfoWithStatsMBean initialized without first call to getMembers operation.", isInitialized);

    String[] members = invokeGetMembers(wrapper);
    logWriter.info("MemberInfoWithStatsMBeanDUnitTest.verifyGetMembers(): ids :: "+ Arrays.toString(members));
    
    isInitialized = isMBeanInitialized(wrapper);//should be true now
    assertTrue("MemberInfoWithStatsMBean not initialized even after the first call to getMembers operation.", isInitialized);
    verifyAttributes(wrapper);
    verifyGetMembers(members);
    
    return members;
  }
  
  @SuppressWarnings("rawtypes")
  private void verifyGetMemberDetails(ObjectName wrapper, String memberId, 
      boolean verifyGatewayDetails) throws AdminException, OperationsException, 
                                           MBeanException, ReflectionException, 
                                           IOException {
    logWriter.fine("Entered MemberInfoWithStatsMBeanDUnitTest.verifyGetMemberDetails() ...");
    Map memberDetailsJMX    = getMemberDetailsJMX(wrapper, memberId);
    Map memberDetailsDirect = getMemberDetailsDirect(memberId);

    verifyMemberDetails(memberDetailsJMX, memberDetailsDirect);
    if (verifyGatewayDetails) {
      verifyGatewayDetails(memberDetailsJMX, memberDetailsDirect);
    } else {
      verifyClientsDetails(memberDetailsJMX, memberDetailsDirect);
      verifyRegionsDetails(memberDetailsJMX, memberDetailsDirect);
    }
    logWriter.fine("Exited MemberInfoWithStatsMBeanDUnitTest.verifyGetMemberDetails() ...");
  }

  @SuppressWarnings("rawtypes")
  private void verifyClientsDetails(Map memberDetailsJMX, Map memberDetailsDirect) {
    logWriter.fine("Entered MemberInfoWithStatsMBeanDUnitTest.verifyClientsDetails() ...");
    Map clientsJmx    = (Map) memberDetailsJMX.get(KEY_CLIENTS_MAP);
    Map clientsDirect = (Map) memberDetailsDirect.get(KEY_CLIENTS_MAP);
    
    boolean isOneNullAndOtherNot = 
                          ((clientsJmx != null && clientsDirect == null) || 
                           (clientsJmx == null && clientsDirect != null));    
    assertFalse("Client information obtained directly & through JMX is not the same.", isOneNullAndOtherNot);
    
    boolean isEitherNull = (clientsDirect == null || clientsJmx == null);
    assertFalse("Non-null maps expected for client information, " +
                "but obtained directly is: "+clientsDirect + 
                " & through JMX is :"+clientsJmx, isEitherNull);
    
    //size of both maps
    assertTrue("No. of clients retrieved through JMX are: "+clientsJmx.size()+
               " but if retrived directly there are: "+clientsDirect.size(), 
               clientsJmx.size() == clientsDirect.size());
    
    Set entrySet1 = clientsJmx.entrySet();
    Set entrySet2 = clientsDirect.entrySet();
    
//    System.out.println("ABHISHEK::MemberInfoWithStatsMBeanDUnitTest.verifyClientsDetails() :: "+clientsJmx+"\n "+clientsDirect);
    
    assertTrue("Client details do not match. Details retrieved thro JMX as: " +
        "\n"+clientsJmx+" \n and directly as: \n"+clientsDirect, 
        entrySet1.equals(entrySet2));
    /* With entrySet equals check we are relying on default Map.Entry.equals() 
     * implementation(s) for equality that check for equality of values too */
    logWriter.fine("Exited MemberInfoWithStatsMBeanDUnitTest.verifyClientsDetails() ...");
  }
  
  @SuppressWarnings("rawtypes")
  private void verifyRegionsDetails(Map memberDetailsJMX,
      Map memberDetailsDirect) {
    logWriter.fine("Entered MemberInfoWithStatsMBeanDUnitTest.verifyRegionsDetails() ...");
    Map regionsJmx    = (Map) memberDetailsJMX.get(KEY_REGIONS_MAP);
    Map regionsDirect = (Map) memberDetailsDirect.get(KEY_REGIONS_MAP);
    
    boolean isOneNullAndOtherNot = 
                          ((regionsJmx != null && regionsDirect == null) || 
                           (regionsJmx == null && regionsDirect != null));    
    assertFalse("Regions information obtained directly & through JMX is not the same.", isOneNullAndOtherNot);
    
    boolean isEitherNull = (regionsDirect == null || regionsJmx == null);
    assertFalse("Non-null maps expected for regions information, " +
                "but obtained directly is: "+regionsDirect + 
                " & through JMX is :"+regionsJmx, isEitherNull);
    
    //size of both maps
    assertTrue("No. of regions retrieved through JMX are: "+regionsJmx.size()+
               " but if retrived directly there are: "+regionsDirect.size(), 
               regionsJmx.size() == regionsDirect.size());
    
    Set entrySet1 = regionsJmx.entrySet();
    Set entrySet2 = regionsDirect.entrySet();
    
//    System.out.println("ABHISHEK::verifyRegionsDetails() :: "+regionsJmx+"\n "+regionsDirect);
    
    assertTrue("Regions details do not match. Details retrieved thro JMX as: " +
        "\n"+regionsJmx+" \n and directly as: \n"+regionsDirect, 
        entrySet1.equals(entrySet2));
    /* With entrySet equals check we are relying on default Map.Entry.equals() 
     * implementation(s) for equality that check for equality of values too */
    logWriter.fine("Exited MemberInfoWithStatsMBeanDUnitTest.verifyRegionsDetails() ...");
  }
  
  private boolean isMBeanInitialized(ObjectName wrapper) 
    throws AttributeNotFoundException, InstanceNotFoundException, 
           MBeanException, ReflectionException, IOException {
    logWriter.fine("Entered MemberInfoWithStatsMBeanDUnitTest.isMBeanInitialized() ...");
    Integer refreshInterval = (Integer) mbsc.getAttribute(wrapper, "RefreshInterval");
    
    AdminDistributedSystem adminDS = agent.getDistributedSystem();
    
    logWriter.fine("Exiting MemberInfoWithStatsMBeanDUnitTest.isMBeanInitialized() ...");
    return adminDS.getConfig().getRefreshInterval() == refreshInterval.intValue();
  }
  
  private String[] invokeGetMembers(ObjectName wrapper) 
    throws InstanceNotFoundException, MBeanException, 
           ReflectionException, IOException {
    logWriter.fine("Entered MemberInfoWithStatsMBeanDUnitTest.invokeGetMembers() ...");
    Object[] params    = new Object[0];
    String[] signature = new String[0];
    
    String[] memberIds = (String[]) mbsc.invoke(wrapper, "getMembers", params, signature);

    logWriter.fine("Exiting MemberInfoWithStatsMBeanDUnitTest.invokeGetMembers() ...");
    return memberIds;
  }
  
  private void verifyAttributes(ObjectName wrapper) 
    throws AttributeNotFoundException, InstanceNotFoundException, 
    MBeanException, ReflectionException, IOException {
    logWriter.fine("Entered MemberInfoWithStatsMBeanDUnitTest.verifyAttributes() ...");
    String  id              = (String) mbsc.getAttribute(wrapper, "Id");
    String  version         = (String) mbsc.getAttribute(wrapper, "Version");
    Integer refreshInterval = (Integer) mbsc.getAttribute(wrapper, "RefreshInterval");
    
    AdminDistributedSystem adminDS = agent.getDistributedSystem();
    
    String  actualId              = adminDS.getId();
    String  actualVersion         = GemFireVersion.getGemFireVersion();
    int     actualRefreshInterval = adminDS.getConfig().getRefreshInterval();
    
//    System.out.println("ABHISHEK:: MemberInfoWithStatsMBeanDUnitTest.verifyAttributes() :: id : "+id+", version : "+version+", refreshInterval : "+refreshInterval);

    assertTrue("AdminDistributedSystem id shown by MemberInfoWithStatsMBean " +
    		       "(as: "+id+") and actual (as: "+actualId+") do not match.", 
    		        actualId.equals(id));
    assertTrue("GemFire Version shown by MemberInfoWithStatsMBean " +
    		       "(as: "+version+") and actual(as: "+actualVersion+") do not match.", 
    		       actualVersion.equals(version));
    assertTrue("Refresh Interval shown by MemberInfoWithStatsMBean (as: "+
               refreshInterval+") and actual (as :"+actualRefreshInterval+") do not match.", 
               actualRefreshInterval == refreshInterval);//use auto-boxing
    logWriter.fine("Exited MemberInfoWithStatsMBeanDUnitTest.verifyAttributes() ...");
  }
  
  private void verifyGetMembers(String[] ids) throws AdminException {
    logWriter.fine("Entered MemberInfoWithStatsMBeanDUnitTest.verifyGetMembers() ...");
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
    logWriter.fine("Exited MemberInfoWithStatsMBeanDUnitTest.verifyGetMembers() ...");
  }
  
  @SuppressWarnings("rawtypes")
  private Map getMemberDetailsJMX(ObjectName wrapper, String memberId) 
    throws AdminException, InstanceNotFoundException, MBeanException, 
           ReflectionException, IOException {
    logWriter.fine("Entered MemberInfoWithStatsMBeanDUnitTest.getMemberDetailsJMX() ...");
    Object[] params    = new Object[] {memberId};
    String[] signature = new String[] {String.class.getName()};
    Map memberDetails = (Map) mbsc.invoke(wrapper, "getMemberDetails", params, signature);
    
    logWriter.fine("Exiting MemberInfoWithStatsMBeanDUnitTest.getMemberDetailsJMX() ...");
    return memberDetails;
  }
  
  @SuppressWarnings("rawtypes")
  private Map getMemberDetailsDirect(String memberId) 
    throws OperationsException {
    logWriter.fine("Entered MemberInfoWithStatsMBeanDUnitTest.getMemberDetailsDirect() ...");
    MemberInfoWithStatsMBean memberInfoWithStatsMBean = ((AgentImpl)agent).memberInfoWithStatsMBean;
    Map<String, Object> memberDetails = memberInfoWithStatsMBean.getMemberDetails(memberId);

    logWriter.fine("Exiting MemberInfoWithStatsMBeanDUnitTest.getMemberDetailsDirect() ...");
    return memberDetails;
  }

  @SuppressWarnings({ "rawtypes" })
  private void verifyMemberDetails(Map mapThroughJmx, Map mapDirect) {
    logWriter.fine("Entered MemberInfoWithStatsMBeanDUnitTest.verifyMemberDetails() ...");
    Object jmxVal = null;
    Object dirVal = null;
    boolean areEqual = false;
    for (int i = 0; i < KEYS_MEMBERS.length; i++) {
      jmxVal = mapThroughJmx.get(KEYS_MEMBERS[i]);
      dirVal = mapDirect.get(KEYS_MEMBERS[i]);
      
      areEqual = (jmxVal != null && dirVal != null) && jmxVal.equals(dirVal);
      
      if (!areEqual && IGNORABLES.contains(KEYS_MEMBERS[i])) {
        //TODO: can a check of JMX values lower than direct values be good enough for such props?
        continue;
      }
      assertTrue("Values obtained through JMX & directly are not the same for: "+KEYS_MEMBERS[i], 
        (jmxVal != null && dirVal != null) && jmxVal.equals(dirVal));
    }

    logWriter.fine("Exited MemberInfoWithStatsMBeanDUnitTest.verifyMemberDetails() ...");
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  private void verifyGatewayDetails(Map memberDetailsJMX,
      Map memberDetailsDirect) {
    logWriter.fine("Entered MemberInfoWithStatsMBeanDUnitTest.verifyGatewayDetails() ...");
    //direct cast to List could be done
    List<Map> gatewaysJmxCol    = (List<Map>) memberDetailsJMX.get(KEY_GATEWAY_COLLECTION);
    List<Map> gatewaysDirectCol = (List<Map>) memberDetailsDirect.get(KEY_GATEWAY_COLLECTION);
    
    //both are not be the same
    boolean isOneNullAndOtherNot = 
                          ((gatewaysJmxCol != null && gatewaysDirectCol == null) || 
                           (gatewaysJmxCol == null && gatewaysDirectCol != null));    
    assertFalse("Gateway information obtained directly & through JMX is not the same.", isOneNullAndOtherNot);
    
    Boolean isGateway = (Boolean) memberDetailsJMX.get("gemfire.member.isgateway.boolean");
    if (gatewaysJmxCol == null) { //also means if gatewaysDirectCol is null after above check
      String memberId = (String) memberDetailsJMX.get("gemfire.member.id.string");
      logWriter.info("No gateway information available for: "+memberId);
      
      assertFalse(memberId+" is a GatewayHub but does not have Gateway details.", isGateway);
      return;
    }

    //Value for KEY_GATEWAY_COLLECTION could be null
    //size of both collections
    assertTrue("No. of gateways retrieved through JMX are: "+gatewaysJmxCol.size()+
               " but if retrived directly there are: "+gatewaysDirectCol.size(), 
               gatewaysJmxCol.size() == gatewaysDirectCol.size());
    
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
    }
    logWriter.fine("Exited MemberInfoWithStatsMBeanDUnitTest.verifyGatewayDetails() ...");
  }

  @SuppressWarnings("rawtypes")
  private void verifyGatewayEndpointsDetails(List<Map> endpointsJMX,
      List<Map> endpointsDirect) {
    logWriter.fine("Entered MemberInfoWithStatsMBeanDUnitTest.verifyGatewayEndpointsDetails() ...");
    
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
    logWriter.fine("Entered MemberInfoWithStatsMBeanDUnitTest.verifyGatewayEndpointsDetails() ...");
  }
  
  /* **************************************************************************/
  /* ****************** GATEWAY TEST SETUP METHODS START **********************/
  /* **************************************************************************/
  
  private static final String WAN_REGION_NAME = "ClientGatewayHubMemberInfoWithStatsMBeanDUnitTest_WAN_Root";
  private static final String DS_REGION_NAME = "ClientGatewayHubMemberInfoWithStatsMBeanDUnitTest_DS_Root";

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

        @SuppressWarnings({ "rawtypes", "unchecked" })
        public void run2() throws CacheException {
          String vmName = "ClientGatewayHubMemberInfoWithStatsMBeanDUnitTest_" + dsName+ "_vm" + whichvm;
          propsDS.setProperty(DistributionConfig.NAME_NAME, vmName);
          getSystem(propsDS);
          getLogWriter().info("[MemberInfoWithStatsMBeanDUnitTest] " + vmName + " has joined " +
            dsName + " with port " + String.valueOf(dsPort));
          Cache cache = getCache();
          AttributesFactory factory = new AttributesFactory();
          factory.setScope(Scope.DISTRIBUTED_ACK);
          factory.setDataPolicy(DataPolicy.REPLICATE);

          // create WAN region
//          factory.setEnableWAN(true);
          factory.setEnableGateway(true);
//          cache.getCacheTransactionManager().addListener(wanTxListener);
//          factory.setCacheListener(wanRegionListener);
          cache.createRegion(WAN_REGION_NAME,
                               factory.create());

          // create DS region
//          factory.setEnableWAN(false);
          factory.setEnableGateway(false);
//          factory.setCacheListener(dsRegionListener);
          cache.createRegion(DS_REGION_NAME,
                               factory.create());
          getLogWriter().info("[MemberInfoWithStatsMBeanDUnitTest] " + vmName + " has created both regions");
        }
      });
    }

    // set up gateway in last vm
    final int whichvm = vms.length-1;
    final VM vm = vms[whichvm];

    vm.invoke(new CacheSerializableRunnable("Set up gateway in "+dsName) {
      private static final long serialVersionUID = 1L;
      public void run2() throws CacheException {
        String vmName = "ClientGatewayHubMemberInfoWithStatsMBeanDUnitTest_" + dsName+ "_vm" + whichvm;

        // create the gateway hub
        String hubName = "ClientGatewayHubMemberInfoWithStatsMBeanDUnitTest_"+dsName;
        String gatewayName = "ClientGatewayHubMemberInfoWithStatsMBeanDUnitTest_"+dsNameRemote;
        getLogWriter().info("[MemberInfoWithStatsMBeanDUnitTest] " + vmName + " is creating " +
          hubName + " with gateway to " + gatewayName);
        Cache cache = getCache();
        GatewayHub hub = cache.addGatewayHub(hubName, hubPortLocal);
        Gateway gateway = hub.addGateway(gatewayName);

        // create the endpoint for remote DS
        getLogWriter().info("[MemberInfoWithStatsMBeanDUnitTest] " + vmName +
          " adding endpoint [" + gatewayName + ", " + hostNameRemote + ", " +
          hubPortRemote + "] to " + gatewayName);
        gateway.addEndpoint(gatewayName, hostNameRemote, hubPortRemote);

        // create the gateway queue
        File d = new File(gatewayName + "_overflow_" + ProcessMgr.getProcessId());
        getLogWriter().info("[MemberInfoWithStatsMBeanDUnitTest] " + vmName +
          " creating queue in " + d + " for " + gatewayName);

        GatewayQueueAttributes queueAttributes =
          new GatewayQueueAttributes(d.toString(),
            GatewayQueueAttributes.DEFAULT_MAXIMUM_QUEUE_MEMORY,
            GatewayQueueAttributes.DEFAULT_BATCH_SIZE,
            GatewayQueueAttributes.DEFAULT_BATCH_TIME_INTERVAL,
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
        }
        catch (IOException e) {
          getLogWriter().error("Start of hub " + hubName + " threw " + e, e);
          fail("Start of hub " + hubName + " threw " + e, e);
        }
        
        getLogWriter().info("[MemberInfoWithStatsMBeanDUnitTest] " + vmName + " has created " +
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
        vm.invoke(MemberInfoWithStatsMBeanDUnitTest.class, "destroyWanQueues");
      }
    }
  }
  /* **************************************************************************/
  /* ****************** GATEWAY TEST SETUP METHODS END  **********************/
  /* **************************************************************************/
  
  /* **************************************************************************/
  /* ************* CLIENT REGION DETAILS TEST SETUP METHOD START***************/
  /* **************************************************************************/
  /** index for the VM to be used as a Cache VM */
  private static final int CACHE_VM = 0;
  
  /** index for the VM to be used as a Cache Client VM */
  private static final int CLIENT_VM = 2;
  
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
  private void startCache(final int serverPort) {
    logWriter.fine("Entered MemberInfoWithStatsMBeanDUnitTest.startCache");
    Host host  = Host.getHost(0);
    VM cacheVM = host.getVM(CACHE_VM);
    
    final Properties props = new Properties();
    props.putAll(getDistributedSystemProperties());
    props.put(DistributionConfig.NAME_NAME, "ClientGatewayHubMemberInfoWithStatsMBeanDUnitTest_vm"+CACHE_VM);
    
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
        cacheInstance.createRegion(DS_REGION_NAME, attrs);
        
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
        logWriter.info("MemberInfoWithStatsMBeanDUnitTest.startCache :: " + 
            "Created cache & started server in cacheVM(VM#"+CACHE_VM+") at port:"+serverPort);
      }
    });
    
    logWriter.fine("Exited MemberInfoWithStatsMBeanDUnitTest.startCache");
  }
  
  /**
   * Stops the cache in the cache VM & disconnects the cache VM from the DS.
   */
  private void stopCache() {
    logWriter.fine("Entered MemberInfoWithStatsMBeanDUnitTest.stopCache");
    Host host    = Host.getHost(0);
    VM   cacheVM = host.getVM(CACHE_VM);
    
    cacheVM.invoke(new CacheSerializableRunnable(getName()+"-stopCache") {
      private static final long serialVersionUID = 1L;

      @Override
      public void run2() throws CacheException {
        disconnectFromDS();
      }
    });
    logWriter.fine("Exited MemberInfoWithStatsMBeanDUnitTest.stopCache");
  }
  
  /**
   * Connects a cache client in {@link #CLIENT_VM} to the server in
   * {@link #CACHE_VM} on the server port provided.
   * 
   * @param serverPort
   *          server port for clients to connect to
   * @return reference to the connection
   */
  private void connectClient(final int serverPort) {
    logWriter.fine("Entered MemberInfoWithStatsMBeanDUnitTest.connectClient");
    final Host host     = Host.getHost(0);
    VM         clientVM = host.getVM(CLIENT_VM);
    
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
        
        PoolImpl p = (PoolImpl) pf.create("MemberInfoWithStatsMBeanDUnitTest");
        
        //3. Create Region Attributes
        AttributesFactory<String, String> factory = 
                                        new AttributesFactory<String, String>();
        factory.setPoolName(p.getName());
        factory.setScope(Scope.LOCAL);
        RegionAttributes<String, String> attrs = factory.create();
        
        //4. Create region
        Region<String,String> region = cacheInstance.createRegion(DS_REGION_NAME, attrs);        
        assertNotNull("Region in cache is is null.", region);

        clientConn = p.acquireConnection();
        assertNotNull("Acquired client connecttion is null.", clientConn);
        
      }
    });
    
    logWriter.info("MemberInfoWithStatsMBeanDUnitTest.connectClient :: " + 
        "Started client in clientVM(VM#"+CLIENT_VM+") & connected to " +
        "cacheVM(VM#2).");
  }
  
  /**
   * Dis-connects a cache client started in {@link #CLIENT_VM} from a cache 
   * server started in {@link #CACHE_VM}. Also closes the cache in client and 
   * destroys the connection.
   */
  private void disConnectClient() {
    logWriter.fine("Entered MemberInfoWithStatsMBeanDUnitTest.stopCache");
    Host host     = Host.getHost(0);
    VM   clientVM = host.getVM(CLIENT_VM);
    
    clientVM.invoke(new CacheSerializableRunnable(getName()+"-disConnectClient") {
      private static final long serialVersionUID = 1L;

      @Override
      public void run2() throws CacheException {
        getCache().close();
        disconnectFromDS();
        MemberInfoWithStatsMBeanDUnitTest.clientConn.destroy();
      }
    });
    logWriter.fine("Exited MemberInfoWithStatsMBeanDUnitTest.stopCache");
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
      throw e;
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
  
  public void tearDown2() throws Exception {
    try {
      super.tearDown2();
      if (recentlyRanGatewayTest) {
        tearDownForGatewayTest();
      }
    } finally {
      disconnectAllFromDS();
    }
  }
}
