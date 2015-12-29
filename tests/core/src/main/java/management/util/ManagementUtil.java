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
package management.util;

import static management.util.HydraUtil.logError;
import static management.util.HydraUtil.logErrorAndRaiseException;
import static management.util.HydraUtil.logFine;
import static management.util.HydraUtil.logInfo;
import hydra.CacheHelper;
import hydra.ClientVmInfo;
import hydra.HydraThreadLocal;
import hydra.JMXManagerHelper;
import hydra.JMXManagerHelper.Endpoint;
import hydra.RemoteTestModule;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import management.jmx.GemfireMBeanServerConnection;
import management.jmx.JMXPrms;
import management.test.federation.FederationBlackboard;
import util.TestException;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.management.ManagementService;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.management.internal.cli.CommandManager;
import com.gemstone.gemfire.management.internal.cli.parser.CommandTarget;

@SuppressWarnings({"unchecked", "rawtypes"})
public class ManagementUtil {
  
  private static JMXConnectorServer cs = null;
  private static Registry registry = null;
  private static HydraThreadLocal jmxCThreadLocal = new HydraThreadLocal();  
  
  public static boolean hasConnectorStarted(){
    return cs==null;
  }
  
  public static synchronized void startRmiConnector(int port) {   
    if(cs==null){
      String vmId = null;      
      vmId= "jmxrmi";
      try {           
        if(registry==null)
          registry = LocateRegistry.createRegistry(port);
        JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://" + InetAddress.getLocalHost().getHostName()
            + ":" + port + "/" + vmId);
        cs = JMXConnectorServerFactory.newJMXConnectorServer(url, null, ManagementFactory.getPlatformMBeanServer());
        cs.start();
        logInfo("JMX RMI Connector started at url " + url.toString());
        FederationBlackboard.getBB().addManagingNode(vmId, url.toString());
      } catch (RemoteException e) {
        logErrorAndRaiseException("Could not start RMI Connector on " + vmId, e);
      } catch (MalformedURLException e) {
        logErrorAndRaiseException("Could not start RMI Connector on " + vmId, e);
      } catch (UnknownHostException e) {
        logErrorAndRaiseException("Could not start RMI Connector on " + vmId, e);
      } catch (IOException e) {
        logErrorAndRaiseException("Could not start RMI Connector on " + vmId, e);
      }
    }
  }

  public static synchronized void startRmiConnector() {
    int port = AvailablePortHelper.getRandomAvailableTCPPort();
    startRmiConnector(port);
  }

  public static synchronized void stopRmiConnector() {
    try {
      cs.stop();
    } catch (IOException e) {
      logErrorAndRaiseException("Could not stop RMI Connector on ", e);
    }
  }
  
  private static JMXConnector _connectToUrl(String url) throws MalformedURLException, IOException{    
    if(JMXPrms.useAuthentication()){
      Map env = new HashMap();
      String[] creds = {JMXPrms.jmxUser(), JMXPrms.jmxPassword()};
      env.put(JMXConnector.CREDENTIALS, creds);
      return JMXConnectorFactory.connect(new JMXServiceURL(url),env);
    }else
      return JMXConnectorFactory.connect(new JMXServiceURL(url));
  }
  
  /**
   * This method abstract connection to a url. When using gemfire proxies (enabled by parameter
   * JMXPrms-useGemfireProxies) it will return instance of GemfireMBeanServerConnection{@link GemfireMBeanServerConnection}
   * which is unification of platform mbean server and gemfire management service
   */
  
  public static MBeanServerConnection connectToUrlOrGemfireProxy(String url) throws MalformedURLException, IOException {
    if(JMXPrms.useGemfireProxies()){//GemfireAPI Testing
      return new GemfireMBeanServerConnection(ManagementService.getManagementService(CacheHelper.getCache()));
    }else{
      if(HydraUtil.runninghydra()){    
        synchronized (jmxCThreadLocal) {
          Map<String, MBeanServerConnection> map = (Map) jmxCThreadLocal.get();
          if (map == null) {
            map = new HashMap<String, MBeanServerConnection>();
            JMXConnector conn = _connectToUrl(url);
            MBeanServerConnection remoteMBS = conn.getMBeanServerConnection();
            logInfo("Created JMX connection to url " + url + " for thread " + Thread.currentThread().getName());
            map.put(url, remoteMBS);
            jmxCThreadLocal.set(map);
            return (remoteMBS);
          } else {
            MBeanServerConnection remoteMBS = map.get(url);
            if (remoteMBS == null) {
              JMXConnector conn = _connectToUrl(url);
              remoteMBS = conn.getMBeanServerConnection();
              logInfo("Created JMX connection to url " + url + " for thread " + Thread.currentThread().getName());
              map.put(url, remoteMBS);
              return (remoteMBS);
            } else
              return (remoteMBS);
          }
        }
      }else{
        JMXConnector conn = _connectToUrl(url);
        MBeanServerConnection remoteMBS = conn.getMBeanServerConnection();
        logInfo("Created JMX connection to url " + url + " for thread " + Thread.currentThread().getName());
        return remoteMBS;
      }
    }
  }
  
  /**
   * Removes connections to specified manager nodes (assuming they are being restarted)
   * @param selectedVmList
   */
  public static void stopRmiConnectors(List<ClientVmInfo> selectedVmList) {    
    for (ClientVmInfo info : selectedVmList) {
      if (info.getClientName().contains("managing")) {
        int vmId = info.getVmid();
        List<Endpoint> dsFilteredList = (JMXManagerHelper.getEndpoints());
        for (Endpoint pt : dsFilteredList) {
          if(pt.getVmid()==vmId){
            String url = FederationBlackboard.urlTemplate.replace("?1", pt.getHost());
            url = url.replace("?2", "" + pt.getPort());
            synchronized (jmxCThreadLocal) {
              Map<String, MBeanServerConnection> map = (Map) jmxCThreadLocal.get();
              map.remove(url);
            }
            ManagementUtil.checkUrl(url);
          }
        }
      }
    }
  }

  public static boolean checkUrl(String url) {
    logInfo("Checking url is rechable or not " + url);
    try {
      MBeanServerConnection connection = connectToUrl(url);
      connection.queryMBeans(null, null);
      return true;
    } catch (MalformedURLException e) {
      return false;
    } catch (IOException e) {
      synchronized (jmxCThreadLocal) {
        Map<String, MBeanServerConnection> map = (Map) jmxCThreadLocal.get();
        JMXConnector conn;
        try {
          conn = _connectToUrl(url);
          MBeanServerConnection remoteMBS = conn.getMBeanServerConnection();
          logInfo("Created JMX connection to url " + url + " for thread " + Thread.currentThread().getName());
          map.put(url, remoteMBS);
          return true;
        } catch (MalformedURLException e1) {
          return false;
        } catch (IOException e1) {
          throw new TestException("Event after retrying url " + url + " still not reachable");
        }
      }
    }
  }

  public static MBeanServerConnection connectToUrl(String url) throws MalformedURLException, IOException {
    
    if(HydraUtil.runninghydra()){    
      synchronized (jmxCThreadLocal) {
        Map<String, MBeanServerConnection> map = (Map) jmxCThreadLocal.get();
        if (map == null) {
          map = new HashMap<String, MBeanServerConnection>();
          JMXConnector conn = _connectToUrl(url);
          MBeanServerConnection remoteMBS = conn.getMBeanServerConnection();
          logInfo("Created JMX connection to url " + url + " for thread " + Thread.currentThread().getName());
          map.put(url, remoteMBS);
          jmxCThreadLocal.set(map);
          return remoteMBS;
        } else {
          MBeanServerConnection remoteMBS = map.get(url);
          if (remoteMBS == null) {
            JMXConnector conn = _connectToUrl(url);
            remoteMBS = conn.getMBeanServerConnection();
            logInfo("Created JMX connection to url " + url + " for thread " + Thread.currentThread().getName());
            map.put(url, remoteMBS);
            return remoteMBS;
          } else
            return remoteMBS;
        }
      }
    }else{
      JMXConnector conn = _connectToUrl(url);
      MBeanServerConnection remoteMBS = conn.getMBeanServerConnection();
      logInfo("Created JMX connection to url " + url + " for thread " + Thread.currentThread().getName());
      return remoteMBS;
    }
  } 

  public static void doStackDump() {
    Thread.dumpStack();
  }
  
  public static Set<ObjectName> getGemfireMBeans(String url){
    MBeanServerConnection remoteMBS = null;
    try {
      remoteMBS = ManagementUtil.connectToUrl(url);
    } catch (MalformedURLException e) {
      throw new TestException("Error trying to managing node at " + url, e);
    } catch (IOException e) {
      throw new TestException("Error trying to managing node at " + url, e);
    }
    try {      
      Set<ObjectName> objectNames = remoteMBS.queryNames(null, null);
      Set<ObjectName> fileteredON = new HashSet<ObjectName>();
      for(ObjectName n : objectNames){
        if(n.toString().contains("GemFire")){
          fileteredON.add(n);
          logFine("Added " + n + " in gemfire mbean set");
        }
      }
      logInfo("All mbeans " + objectNames);
      logInfo("Gemfire mbeans " + fileteredON);
      return fileteredON;
    } catch (IOException e) {      
      throw new TestException("Error trying to managing node at " + url, e);
    }
  }
  
  public static DistributedMember getMember(){
    return InternalDistributedSystem.getConnectedInstance().getDistributedMember();
  }
  
  public static String getMemberID(){    
    return MBeanJMXAdapter.getMemberNameOrId(getMember());    
  }
  
  public static ObjectName getLocalMemberMBeanON(){
    ObjectName name = MBeanJMXAdapter.getMemberMBeanName(getMember());
    return name;
  }
  public static String getLocalMemberMBean(){    
    return getLocalMemberMBeanON().toString();
  }
  
  public static ObjectName getLocalManagerMBean(){
    ObjectName name = MBeanJMXAdapter.getManagerName();
    return name;
  }
  
  public static ObjectName getLocalCacheServerMBean(int port) {
    ObjectName name = MBeanJMXAdapter.getClientServiceMBeanName(port,getMember());
    return name;
  }
  
  public static void saveMemberMbeanInBlackboard(){
    FederationBlackboard.getBB().addMemberON("vmId" + RemoteTestModule.getMyVmid(), getLocalMemberMBean());
  }
  
  public static void saveMemberManagerInBlackboard(){
    FederationBlackboard.getBB().addManagerON("vmId" + RemoteTestModule.getMyVmid(), getLocalMemberMBean());    
  }
  
  public static boolean checkLocalMemberMBean(){
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    try {      
      Set<ObjectName> objectNames = server.queryNames(null, null);
      logFine("Mbean Set registered at platform mbean server : " + objectNames);      
      boolean flag = false;
      for(ObjectName n : objectNames){
        if(n.toString().contains(getLocalMemberMBean())){
          flag = true;
          logFine("Found Member MBean " + n + " in gemfire mbean set");
          break;
        }
      }      
      return flag;
    } finally{      
    }    
  }
  
  
  public static boolean checkIfMBeanExists(MBeanServerConnection server, ObjectName mbean) throws IOException{    
    try {      
      Set<ObjectName> objectNames = server.queryNames(null, null);
      logFine("Mbean Set registered at mbean server after federation : " + objectNames);
      for(ObjectName n : objectNames){
        if(n.equals(mbean))
          return true;
      }
      return false;
    } finally{      
    }
  }
  
  public static String getWanSiteName(){
    String clientName = RemoteTestModule.getMyClientName();
    String array[] = clientName.split("_");
    logFine("clientName split Array DSName = " + HydraUtil.ObjectToString(array));
    logFine("clientName split Array length = " + array.length);
    String dsName = null;
    if(array.length>=4){
      dsName = array[array.length-1];
    }
    return dsName;
  }
  
  public static <T> List<T> filterForThisDS(List<T> list){    
    String clientName = RemoteTestModule.getMyClientName();
    String array[] = clientName.split("_");
    logFine("clientName split Array DSName = " + HydraUtil.ObjectToString(array));
    logFine("clientName split Array length = " + array.length);
    if(array.length>=4){
      String dsName = array[array.length-1];
      logInfo("Considering names of elements with DSName = " + dsName);
      List<T> filteredList = new ArrayList<T>();
      for(T t : list){
        String name = t.toString();
        if(name.contains(dsName))
          filteredList.add(t);
      }
      logFine("List fileted for " + dsName + " " + HydraUtil.ObjectToString(filteredList));
      return filteredList;
    }
    else return list;
  }
  
  public static <T> List<T> filter(Collection<T> list, String string){    
    List<T> filteredList = new ArrayList<T>();
    for(T t : list){
      String name = t.toString();
      if(!name.contains(string))
        filteredList.add(t);
    }
    logFine("List fileted for " + string + " " + HydraUtil.ObjectToString(filteredList));
    return filteredList;
  }
  
  
  public static boolean checkForManagedMemberMBean(MBeanServerConnection server) throws IOException{
    
    String clientName = RemoteTestModule.getMyClientName();
    String array[] = clientName.split("_");
    logFine("clientName split Array DSName = " + HydraUtil.ObjectToString(array));
    logFine("clientName split Array length = " + array.length);
    String dsName= null;
    if(array.length>=4){
      //wan test assume last part is DS Name
      dsName = array[array.length-1];
      logInfo("Considering names of clients with DSName = " + dsName);
    }
    
    Map<String,String> map = FederationBlackboard.getBB().getMemberONs();    
    int count = 0;
    List<String> listOFNS = new ArrayList<String>();
    for(String s: map.values()){
      if(dsName==null || (dsName!=null&& s.contains(dsName))){
        listOFNS.add(s);
        count++;
      }
    }
    
    logInfo("Checking " + listOFNS + " names on server "+ server);
    
    try {      
      Set<ObjectName> objectNames = server.queryNames(null, null);
      logFine("Mbean Set registered at platform mbean server after federation : " + objectNames);      
      for(ObjectName n : objectNames){
        for(String s : map.values())
          if(n.toString().contains(s)){
            count--;
            listOFNS.remove(s);
          }
      }
      logInfo("Final count of remaining mbeans not present in Manager view : " + count + " List : " + listOFNS);
      return count==0;
    } finally{      
    }
  }
  
  
  public static List<String> getBeanProxiesForBlackboardMap(MBeanServerConnection server, Map<String,String> map) throws IOException{
    String clientName = RemoteTestModule.getMyClientName();
    String array[] = clientName.split("_");
    logFine("clientName split Array DSName = " + HydraUtil.ObjectToString(array));
    logFine("clientName split Array length = " + array.length);
    String dsName= null;
    if(array.length>=4){
      //wan test assume last part if DS Name
      dsName = array[array.length-1];
      logInfo("Considering names of clients with DSName = " + dsName);
    }
    //int count = map.size();
    int count = 0;
    List<String> listOFNS = new ArrayList<String>();
    for(String s: map.values()){
      if(dsName==null || (dsName!=null&& s.contains(dsName))){
        listOFNS.add(s);
        count++;
      }
    }    
    logInfo("getBeanProxiesForBlackboardMap :  " + listOFNS);
    return listOFNS;
  }
  
  
  /**
   * This API gives list of managed member proxies which are not present in the manager view
   * 
   * @param MBeanServerConnection
   * @throws IOException
   */
  public static List<String> checkForManagedMbeanProxies(MBeanServerConnection server) throws IOException{
    Map<String,String> map = FederationBlackboard.getBB().getMemberONs();
    List<String> listOFNS = getBeanProxiesForBlackboardMap(server,map);    
    return checkForMBeanProxies(server, listOFNS, map);
  }
  
  
  /**
   * This API gives list of managing member proxies which are not present in the manager view
   * 
   * @param MBeanServerConnection
   * @throws IOException
   */
  public static List<String> checkForManagingMbeanProxies(MBeanServerConnection server) throws IOException{
    Map<String,String> map = FederationBlackboard.getBB().getManagerONs();
    List<String> listOFNS = getBeanProxiesForBlackboardMap(server,map);    
    return checkForMBeanProxies(server, listOFNS, map);
  }
  
  public static List<String> checkForMBeanProxies(MBeanServerConnection server,List<String> listOFNS, Map<String,String> map) throws IOException{
    logInfo("Checking " + listOFNS + " names on server "+ server);
    try {      
      Set<ObjectName> objectNames = server.queryNames(null, null);
      logFine("Mbean Set registered at platform mbean server after federation : " + objectNames);      
      for(ObjectName n : objectNames){
        for(String s : map.values())
          if(n.toString().contains(s)){
            listOFNS.remove(s);
          }
      }
      logInfo("Final count of remaining mbeans not present in Manager view : " + listOFNS.size() + " List : " + listOFNS);
      return listOFNS;
    } finally{      
    }
  }
  
  public static boolean checkForDistributedMBean(MBeanServerConnection server) throws IOException{
    try {      
      Set<ObjectName> objectNames = server.queryNames(null, null);      
      boolean flag = false;
      for(ObjectName n : objectNames){
        if(n.equals(MBeanJMXAdapter.getDistributedSystemName())){
          flag = true;
          logFine("Found Distributed System MBean " + n + " in gemfire mbean set");
          break;
        }
      }      
      return flag;
    } finally{      
    }    
  }
  
  public static boolean checkForManagerMBean(MBeanServerConnection server) throws IOException{
    try {      
      Set<ObjectName> objectNames = server.queryNames(null, null);      
      boolean flag = false;
      for(ObjectName n : objectNames){
        if(n.equals(MBeanJMXAdapter.getManagerName())){
          flag = true;
          logFine("Found ManagerMBean " + n + " in gemfire mbean set");
          break;
        }
      }      
      return flag;
    } finally{      
    }    
  }
  
  public static boolean subscribeDSNotifs(){
    return !(JMXPrms.useGemfireProxies() && RemoteTestModule.getMyClientName().contains("managed"));
  }
  
  
  public static void checkIfThisMemberIsCompliantManager(MBeanServerConnection server) throws IOException, TestException{
    
    List<String> list = ManagementUtil.checkForManagedMbeanProxies(server);
    if(list.size()>0)
      throw new TestException("Some managed member not present in Manager view. " + list);
    
    list = ManagementUtil.checkForManagingMbeanProxies(server);
    if(list.size()>0)
      throw new TestException("Some managing member not present in Manager view. " + list);    
    
    
    if(!ManagementUtil.checkForDistributedMBean(server))
      throw new TestException("DistributedSystemMBean is absent in Manager View Connection= " + server);
    if(!ManagementUtil.checkForManagerMBean(server))
      throw new TestException("Could not find manager mbean after startManager via ManagementService Connection= " + server); 
  }
  
  public static MBeanServerConnection getPlatformMBeanServer() {
    return ManagementFactory.getPlatformMBeanServer();
  }
  
  public static MBeanServerConnection getPlatformMBeanServerDW() {
    if(JMXPrms.useGemfireProxies())
      return new GemfireMBeanServerConnection(ManagementService.getManagementService(CacheHelper.getCache()));
    else  return ManagementFactory.getPlatformMBeanServer();
  }
  
  public static boolean checkIfCommandsAreLoadedOrNot(){	  
		CommandManager manager;
		try {
			manager = CommandManager.getInstance();
			Map<String,CommandTarget> commands = manager.getCommands();
			Set set = commands.keySet();
			logInfo("Started member with " + set.size() + " commands " + HydraUtil.ObjectToString(set));
			if(commands.size()<1){
				logError("CommandManager failed to load any commands " + commands);
				return false;
			}else return true;
		} catch (ClassNotFoundException e) {
			logError("CommandManager failed with " + e.getMessage(),e);
			return false;
		} catch (IOException e) {
			logError("CommandManager failed with " + e.getMessage(),e);
			return false;
		}		 
  }

  /**
   * Returns members for mentioned group. Scans groups list of memberMbeans
   * If group is null returns all members
   * @param connection
   * @param group
   * @return
   * @throws NullPointerException 
   * @throws MalformedObjectNameException 
   * @throws IOException 
   * @throws ReflectionException 
   * @throws MBeanException 
   * @throws InstanceNotFoundException 
   * @throws AttributeNotFoundException 
   */
  public static Set<String> getMembersForGroup(MBeanServerConnection connection, String group) throws MalformedObjectNameException, NullPointerException, InstanceNotFoundException, MBeanException, ReflectionException, IOException, AttributeNotFoundException {
    
    Set<String> memberSet = new HashSet<String>();
    ObjectName ds = new ObjectName("GemFire:service=System,type=Distributed");
    
    String[] memberList = (String[]) connection.invoke(ds, "listMembers", null, null);
    for(String member : memberList){
      ObjectName memberMBean = new ObjectName("GemFire:type=Member,member="+member);
      String groups[] = (String[]) connection.getAttribute(memberMBean, "Groups");
      for(String g : groups){
        if(g.equals(group) || group==null){
          memberSet.add(member);
        }
      }
    }    
    return memberSet;
  }

  public static Set<String> getMembersForGroup(String group) throws MalformedObjectNameException, InstanceNotFoundException, AttributeNotFoundException, NullPointerException, MBeanException, ReflectionException {
    String url = FederationBlackboard.getBB().getManagingNode();
    try {
      MBeanServerConnection connection = ManagementUtil.connectToUrl(url);
      return getMembersForGroup(connection, group);
    } catch (MalformedURLException e) {
      throw new TestException("Error connecting manger" ,e);
    } catch (IOException e) {
      throw new TestException("Error connecting manger" ,e);
    }    
  }
  
}
