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

package hydra;

import java.io.File;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.GemFireVersion;

/**
 *
 *  A TestConfig contains the results of parsing a hydra configuration file.
 *  The class maintains this instance as a singleton that can be accessed
 *  from any hydra VM via {@link #getInstance}.
 *
 */
public class TestConfig implements Serializable {

  private static transient TestConfig singleton = null; // hydra client version
  private static transient TestConfig hasingleton = null; // hostagent version

  private String TestName;
  private String TestUser;
  private String BuildVersion;
  private String BuildDate;
  private String SourceRepository;
  private String SourceRevision;
  private String SourceDate;
  private String BuildJDK;
  private String RuntimeJDK;
  private String JavaVMName;

  private Vector UnitTests;
  private Vector StartTasks;
  private Vector InitTasks;         // all inittasks
  private Vector StaticInitTasks;   // inittasks run always or once
  private Vector DynamicInitTasks;  // inittasks run always or dynamic
  private Vector Tasks;
  private Vector CloseTasks;        // all closetasks
  private Vector StaticCloseTasks;  // closetasks run always or once
  private Vector DynamicCloseTasks; // closetasks run always or dynamic
  private Vector EndTasks;
  private MasterDescription TheMasterDescription;
  private SortedMap HostDescriptions = new TreeMap();
  private SortedMap<String,HostDescription> HadoopHostDescriptions = new TreeMap();
  private SortedMap<String,HadoopDescription> HadoopDescriptions = new TreeMap();
  private SortedMap<String,GemFireDescription> GemFireDescriptions = new TreeMap();
  private SortedMap<String,JDKVersionDescription> JDKVersionDescriptions = new TreeMap();
  private SortedMap<String,VersionDescription> VersionDescriptions = new TreeMap();
  private SortedMap AdminDescriptions = new TreeMap();
  private SortedMap AgentDescriptions = new TreeMap();
  private SortedMap VmDescriptions = new TreeMap();
  private SortedMap<String,HostAgentDescription> HostAgentDescriptions = new TreeMap();
  private SortedMap PoolDescriptions = new TreeMap();
  private SortedMap DiskStoreDescriptions = new TreeMap();
  private SortedMap<String,HDFSStoreDescription> HDFSStoreDescriptions = new TreeMap();
  private SortedMap CacheDescriptions = new TreeMap();
  private SortedMap ResourceManagerDescriptions = new TreeMap();
  private SortedMap RegionDescriptions = new TreeMap();
  private SortedMap FixedPartitionDescriptions = new TreeMap();
  private SortedMap PartitionDescriptions = new TreeMap();
  private SortedMap SecurityDescriptions = new TreeMap();
  private SortedMap SSLDescriptions = new TreeMap();
  private SortedMap BridgeDescriptions = new TreeMap();
  private SortedMap GatewayReceiverDescriptions = new TreeMap();
  private SortedMap GatewaySenderDescriptions = new TreeMap();
  private SortedMap<String,AsyncEventQueueDescription> AsyncEventQueueDescriptions = new TreeMap();
  private SortedMap GatewayHubDescriptions = new TreeMap();
  private SortedMap GatewayDescriptions = new TreeMap();
  private SortedMap<String,ClientDescription> ClientDescriptions = new TreeMap();
  private SortedMap JProbeDescriptions = new TreeMap();
  private SortedMap HypericServerInstallDescriptions = new TreeMap();
  private SortedMap<String,ClientCacheDescription> ClientCacheDescriptions = new TreeMap();
  private SortedMap<String,ClientRegionDescription> ClientRegionDescriptions = new TreeMap();

  private SortedMap SystemProperties = new TreeMap();
  private Hashtable ThreadGroups = new Hashtable();
  private ConfigHashtable Parameters = new ConfigHashtable();
  private List<String> ClassNames = new ArrayList();

  //////////////////////////////////////////////////////////////////////////////
  ////  SINGLETON                                                           ////
  //////////////////////////////////////////////////////////////////////////////

  /** For internal use by hydra master controller only */
  public static TestConfig create() {
    if ( singleton == null ) {
      singleton = new TestConfig();
    } else {
      throw new HydraRuntimeException( "TestConfig has already been created" );
    }
    return singleton;
  }
  /** For internal use by config parsing tool only */
  protected static void destroy() {
    singleton = null;
  }

//------------------------------------------------------------------------------
// hostagent test configuration
//------------------------------------------------------------------------------

  /**
   *  Gets the singleton instance of the hostagent test configuration.
   *  Lazily deserializes the configuration from a file upon first access.
   *  Handles {@link Prms#noNFS} case by using master proxy if necessary.
   */
  public static synchronized TestConfig getHostAgentInstance() {
    if ( hasingleton == null ) {
      String config = getTestConfigFilename("hostagentConfig.obj");
      if (config == null) {
        Log.getLogWriter().info("Deserializing hostagent test configuration via master...");
        try {
          hasingleton = RmiRegistryHelper.lookupMaster().getHostAgentTestConfig();
        } catch (RemoteException e) {
          String s = "Unable to access master to get hostagent test configuration";
          throw new HydraRuntimeException(s, e);
        }
      } else {
        Log.getLogWriter().info("Deserializing hostagent test configuration...");
        hasingleton = (TestConfig) FileUtil.deserialize(config);
        //Log.getLogWriter().info( "HostAgentInstance: " + hasingleton.toString() );
      }
    }
    return hasingleton;
  }

  protected static TestConfig getHostAgentInstanceFromMaster() {
    String config = getTestConfigFilename("hostagentConfig.obj");
    Log.getLogWriter().info("Deserializing hostagent test configuration...");
    return (TestConfig)FileUtil.deserialize(config);
  }

//------------------------------------------------------------------------------
// hydra client test configuration
//------------------------------------------------------------------------------

  /**
   *  Gets the singleton instance of the test configuration.  Lazily
   *  deserializes the configuration from a file upon first access.
   *  Handles {@link Prms#noNFS} case by using master proxy if necessary.
   */
  public static synchronized TestConfig getInstance() {
    if ( singleton == null ) {
      String config = getTestConfigFilename("testConfig.obj");
      if (config == null) {
        Log.getLogWriter().info("Deserializing test configuration via master...");
        try {
          singleton = RmiRegistryHelper.lookupMaster().getTestConfig();
        } catch (RemoteException e) {
          String s = "Unable to access master to get test configuration";
          throw new HydraRuntimeException(s, e);
        }
      } else {
        Log.getLogWriter().info("Deserializing test configuration...");
        singleton = (TestConfig) FileUtil.deserialize(config);
      }
    }
    return singleton;
  }
  protected static TestConfig getInstanceFromMaster() {
    String config = getTestConfigFilename("testConfig.obj");
    Log.getLogWriter().info("Deserializing test configuration...");
    return (TestConfig)FileUtil.deserialize(config);
  }
  /**
   * Returns the path to the given config.  Looks first in "user.dir", and if not
   * found, tries "test.dir" as an alternate to support manual GFMon testing.
   */
  private static String getTestConfigFilename(String fn) {
    String config = System.getProperty("user.dir")
                  + File.separator + fn;
    if (FileUtil.exists(config)) {
      return config;
    } else {
      String testDir = System.getProperty("test.dir");
      if (testDir != null) {
        config = testDir + File.separator + fn;
        if (FileUtil.exists(config)) {
          return config;
        }
      }
    }
    return null;
  }

  public void configure() {
    // some of these are order-dependent, so change with care
    HostDescription.configure(this);
    HadoopDescription.configure(this);
    HostAgentDescription.configure(this);
    JProbeDescription.configure(this);
    SSLDescription.configure(this);
    SecurityDescription.configure(this);
    GemFireDescription.configure(this);
    AdminDescription.configure(this);
    AgentDescription.configure(this);
    VmDescription.configure(this);
    JDKVersionDescription.configure(this);
    VersionDescription.configure(this);
    ClientDescription.configure(this);
    DiskStoreDescription.configure(this);
    HDFSStoreDescription.configure(this);
    BridgeDescription.configure(this);
    PoolDescription.configure(this);
    FixedPartitionDescription.configure(this);
    PartitionDescription.configure(this);
    GatewayReceiverDescription.configure(this);
    GatewaySenderDescription.configure(this);
    AsyncEventQueueDescription.configure(this);
    RegionDescription.configure(this);
    ClientRegionDescription.configure(this);
    ResourceManagerDescription.configure(this);
    CacheDescription.configure(this);
    ClientCacheDescription.configure(this);
    GatewayDescription.configure(this);
    GatewayHubDescription.configure(this);
    HypericServerInstallDescription.configure(this);
  }

  /**
   * For hydra internal use only.
   */
  public void share() {
    String userDir = getMasterDescription().getVmDescription()
                    .getHostDescription().getUserDir();
    String config = userDir + "/testConfig.obj";
    FileUtil.serialize( singleton, config );
    String latest = userDir + "/latest.prop";
    String content = singleton.toString();
    content = content.replace( '\\', '/' );
    FileUtil.writeToFile( latest, content );
    log().info( singleton.toString() );

    hasingleton = new TestConfig();
    hasingleton.setClassNames(singleton.getClassNames());
    hasingleton.setParameters(singleton.getParameters());
    hasingleton.setMasterDescription(singleton.getMasterDescription());
    hasingleton.setHostAgentDescriptions(singleton.getHostAgentDescriptions());
    hasingleton.setHostDescriptions(singleton.getHostDescriptions());
    hasingleton.setHadoopHostDescriptions(singleton.getHadoopHostDescriptions());
    String haconfig = userDir + "/hostagentConfig.obj";
    FileUtil.serialize( hasingleton, haconfig );
    log().info( "HostAgentInstance: " + hasingleton.toString() );
  }

  public static ConfigHashtable tab() {
    return TestConfig.getInstance().getParameters();
  }

  public static ConfigHashtable tasktab() {
    try {
    return RemoteTestModule.getCurrentThread().getCurrentTask().getTaskAttributes();
    } catch (NullPointerException e) {
      return tab();
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  ////  ACCESSORS                                                           ////
  //////////////////////////////////////////////////////////////////////////////

  public String getTestName() {
    return TestName;
  }
  protected void setTestName( String testName ) {
    TestName = testName;
  }
  public String getTestUser() {
    return TestUser;
  }
  protected void setTestUser( String testUser ) {
    TestUser = testUser;
  }
  public MasterDescription getMasterDescription() {
    return TheMasterDescription;
  }
  protected void setMasterDescription( MasterDescription md ) {
    if ( TheMasterDescription == null )
      TheMasterDescription = md;
    //else
      //throw new HydraInternalException( "Master description already set" );
  }
  public Vector getUnitTests() {
    return UnitTests;
  }
  protected void addUnitTest( TestTask task ) {
    if ( UnitTests == null ) UnitTests = new Vector();
    UnitTests.add( task );
  }
  public Vector getStartTasks() {
    return StartTasks;
  }
  protected void addStartTask( TestTask task ) {
    if ( StartTasks == null ) StartTasks = new Vector();
    StartTasks.add( task );
  }
  public Vector getInitTasks() {
    return StaticInitTasks; // exclude dynamic-only tasks behind the scenes
  }
  protected void addInitTask( TestTask task ) {
    if ( InitTasks == null ) InitTasks = new Vector();
    InitTasks.add( task );
  }
  private void addStaticInitTask( TestTask task ) {
    if ( StaticInitTasks == null ) StaticInitTasks = new Vector();
    StaticInitTasks.add( task );
  }
  public Vector getDynamicInitTasksClone() {
    Vector clone = null; // return clone of dynamic-only tasks
    if ( DynamicInitTasks != null ) {
      for ( Iterator i = DynamicInitTasks.iterator(); i.hasNext(); ) {
        TestTask tt = (TestTask) i.next();
        if ( clone == null ) {
          clone = new Vector();
        }
        clone.add( tt.clone() );
      }
    }
    return clone;
  }
  private void addDynamicInitTask( TestTask task ) {
    if ( DynamicInitTasks == null ) DynamicInitTasks = new Vector();
    DynamicInitTasks.add( task );
  }
  public Vector getTasks() {
    return Tasks;
  }
  /**
  * Returns the task of the given type at the given index.
  */
  protected TestTask getTask( int type, int index ) {
    switch( type ) {
      case TestTask.STARTTASK:
             return (TestTask) StartTasks.elementAt(index);
      case TestTask.INITTASK:
             return (TestTask) StaticInitTasks.elementAt(index);
      case TestTask.DYNAMICINITTASK:
             return (TestTask) DynamicInitTasks.elementAt(index);
      case TestTask.TASK:
             return (TestTask) Tasks.elementAt(index);
      case TestTask.DYNAMICCLOSETASK:
             return (TestTask) DynamicCloseTasks.elementAt(index);
      case TestTask.CLOSETASK:
             return (TestTask) StaticCloseTasks.elementAt(index);
      case TestTask.ENDTASK:
             return (TestTask) EndTasks.elementAt(index);
      default: throw new HydraInternalException( "Invalid task type: " + type );
    }
  }
  protected void addTask( TestTask task ) {
    if ( Tasks == null ) Tasks = new Vector();
    Tasks.add( task );
  }
  public Vector getCloseTasks() {
    return StaticCloseTasks; // exclude dynamic-only tasks behind the scenes
  }
  protected void addCloseTask( TestTask task ) {
    if ( CloseTasks == null ) CloseTasks = new Vector();
    CloseTasks.add( task );
  }
  protected void addStaticCloseTask( TestTask task ) {
    if ( StaticCloseTasks == null ) StaticCloseTasks = new Vector();
    StaticCloseTasks.add( task );
  }
  public Vector getDynamicCloseTasksClone() {
    Vector clone = null; // return clone of dynamic-only tasks
    if ( DynamicCloseTasks != null ) {
      for ( Iterator i = DynamicCloseTasks.iterator(); i.hasNext(); ) {
        TestTask tt = (TestTask) i.next();
        if ( clone == null ) {
          clone = new Vector();
        }
        clone.add( tt.clone() );
      }
    }
    return clone;
  }
  protected void addDynamicCloseTask( TestTask task ) {
    if ( DynamicCloseTasks == null ) DynamicCloseTasks = new Vector();
    DynamicCloseTasks.add( task );
  }
  public Vector getEndTasks() {
    return EndTasks;
  }
  protected void addEndTask( TestTask task ) {
    if ( EndTasks == null ) EndTasks = new Vector();
    EndTasks.add( task );
  }
  public Hashtable getThreadGroups() {
    return ThreadGroups;
  }
  /**
  * Returns the thread group with the given name, and null if it doesn't exist.
  */
  public HydraThreadGroup getThreadGroup( String name ) {
    return (HydraThreadGroup) ThreadGroups.get( name );
  }
  /**
  * Adds the specified thread group to the thread group table.
  * @param group the thread group to add
  * @throws HydraConfigException if a thread group with the same name already exists
  */
  protected void addThreadGroup( HydraThreadGroup group ) {
    String name = group.getName();
    if ( ThreadGroups.get( name ) != null )
      throw new HydraConfigException( "Thread group " + name + " already exists" );
    ThreadGroups.put( name, group );
  }

  /**
   * Returns a mapping of the logical name of the host to
   * its {@link HostDescription}.
   */
  public SortedMap<String,HostDescription> getHostDescriptions() {
    return HostDescriptions;
  }
  private void setHostDescriptions(SortedMap<String,HostDescription> m) {
    HostDescriptions = m;
  }

  public HostDescription getHostDescription( String name ) {
    return (HostDescription) HostDescriptions.get( name );
  }

  /**
   * Returns the set of logical host names specified by {@link HostDescription}
   * objects via {@link HostPrms#names}.
   */
  public Vector getHostNames() {
    Vector names = new Vector();
    for ( Iterator i = HostDescriptions.values().iterator(); i.hasNext(); ) {
      HostDescription hd = (HostDescription) i.next();
      names.add( hd.getName() );
    }
    return ( names.size() == 0 ) ? null : names;
  }

  /**
   * Returns the set of physical host names specified by {@link HostDescription}
   * objects via {@link HostPrms#hostNames}.
   */
  public Vector getPhysicalHosts() {
    Vector hosts = new Vector();
    for ( Iterator i = HostDescriptions.values().iterator(); i.hasNext(); ) {
      HostDescription hd = (HostDescription) i.next();
      hosts.add( hd.getHostName() );
    }
    return ( hosts.size() == 0 ) ? null : hosts;
  }

  /**
   * Returns the set of physical host names specified by {@link HostDescription}
   * objects via {@link HostPrms#hostNames} as well as host specified by
   * {@link HadoopDescription} objects via {@link HadoopPrms#nameNodeHosts}
   * and {@link HadoopPrms#dataNodeHosts}.
   */
  public Vector getPhysicalHostsIncludingHadoop() {
    Vector hosts = getPhysicalHosts();
    if (hosts == null) {
      hosts = new Vector();
    }
    for (HadoopDescription hdd : HadoopDescriptions.values()) {
      for (String host : hdd.getNameNodeHosts()) {
        if (!hosts.contains(host)) {
          hosts.add(host);
        }
      }
      for (String host : hdd.getDataNodeHosts()) {
        if (!hosts.contains(host)) {
          hosts.add(host);
        }
      }
    }
    return ( hosts.size() == 0 ) ? null : hosts;
  }

  /**
   * Returns any HostDescription for the given physical host, whether
   * configured or generated.
   * <p>
   * Note that this method assumes that all descriptions for the host have
   * the same configuration, which is safe since hydra requires it.
   */
  protected HostDescription getAnyPhysicalHostDescription(String host) {
    for ( Iterator i = HadoopHostDescriptions.values().iterator(); i.hasNext(); ) {
      HostDescription hd = (HostDescription)i.next();
      if (HostHelper.compareHosts(hd.getHostName(), host)) {
        return hd;
      }
    }
    for ( Iterator i = HostDescriptions.values().iterator(); i.hasNext(); ) {
      HostDescription hd = (HostDescription)i.next();
      if (HostHelper.compareHosts(hd.getHostName(), host)) {
        return hd;
      }
    }
    String s = "No matching host description found for " + host
             + " in " + getPhysicalHosts();
    throw new HydraRuntimeException(s);
  }

  /**
  * Adds the specified host description to the test configuration.
  * @param hd the host description to add
  * @throws HydraConfigException if a host description with the same name already exists
  */
  protected void addHostDescription( HostDescription hd ) {
    String name = hd.getName();
    if ( HostDescriptions.containsKey( name ) )
      throw new HydraConfigException( "Host description " + name + " already exists" );
    HostDescriptions.put( name, hd );
  }

  /**
   * Returns a mapping of the logical name of the hadoop cluster host to
   * its {@link HostDescription}.
   */
  public SortedMap<String,HostDescription> getHadoopHostDescriptions() {
    return HadoopHostDescriptions;
  }

  private void setHadoopHostDescriptions(SortedMap<String,HostDescription> m) {
    HadoopHostDescriptions = m;
  }

  public HostDescription getHadoopHostDescription(String name) {
    return (HostDescription)HadoopHostDescriptions.get(name);
  }

  /**
  * Adds the specified hadoop host description to the test configuration.
  * @param hd the hadoop host description to add
  * @throws HydraConfigException if a hadoop host description with the same name already exists
  */
  protected void addHadoopHostDescription(HostDescription hd) {
    String name = hd.getName();
    if (HadoopHostDescriptions.containsKey(name))
      throw new HydraConfigException("Hadoop host description " + name + " already exists");
    HadoopHostDescriptions.put(name, hd);
  }

  /**
   * Returns the Hadoop {@link HostDescription} for the given physical host.
   */
  protected HostDescription getHadoopHostDescriptionForPhysicalHost(String host) {
    for (HostDescription hd : HadoopHostDescriptions.values()) {
      if (hd.getHostName().equals(host)) {
        return hd;
      }
    }
    String s = "No Hadoop HostDescription found for physical host: " + host;
    throw new HydraRuntimeException(s);
  }

  /**
   * Returns the set of physical host names specified in {@link
   * HadoopDescription} objects.
   */
  protected Vector getHadoopPhysicalHosts() {
    Vector hosts = new Vector();
    for (HostDescription hd : HadoopHostDescriptions.values()) {
      hosts.add(hd.getHostName());
    }
    return (hosts.size() == 0) ? null : hosts;
  }

  /**
   * Returns a mapping of the logical name of the hadoop cluster to
   * its {@link HadoopDescription}.
   */
  public SortedMap<String,HadoopDescription> getHadoopDescriptions() {
    return HadoopDescriptions;
  }

  private void setHadoopDescriptions(SortedMap<String,HadoopDescription> m) {
    HadoopDescriptions = m;
  }

  public HadoopDescription getHadoopDescription(String name) {
    return (HadoopDescription)HadoopDescriptions.get(name);
  }

  /**
  * Adds the specified hadoop description to the test configuration.
  * @param hd the hadoop description to add
  * @throws HydraConfigException if a hadoop description with the same name already exists
  */
  protected void addHadoopDescription(HadoopDescription hd) {
    String name = hd.getName();
    if (HostDescriptions.containsKey(name))
      throw new HydraConfigException("Hadoop description " + name + " already exists");
    HadoopDescriptions.put(name, hd);
  }

  /**
   * Returns a mapping of the logical name of the GemFire system to
   * its {@link GemFireDescription}.
   */
  public Map<String,GemFireDescription> getGemFireDescriptions() {
    return GemFireDescriptions;
  }
  public GemFireDescription getGemFireDescription( String name ) {
    return (GemFireDescription) GemFireDescriptions.get( name );
  }
  /**
  * Adds the specified GemFire description to the test configuration.
  * @param gfd the GemFire description to add
  * @throws HydraConfigException if a GemFire description with the same name already exists
  */
  protected void addGemFireDescription( GemFireDescription gfd ) {
    String name = gfd.getName();
    if ( GemFireDescriptions.containsKey( name ) )
      throw new HydraConfigException( "GemFire description " + name + " already exists" );
    GemFireDescriptions.put( name, gfd );
  }

//------------------------------------------------------------------------------
// JDKVersionDescription

  /**
   * Returns a mapping of the logical name of the JDK version description to
   * its {@link JDKVersionDescription}.
   */
  public Map<String,JDKVersionDescription> getJDKVersionDescriptions() {
    return JDKVersionDescriptions;
  }

  public JDKVersionDescription getJDKVersionDescription(String name) {
    return JDKVersionDescriptions.get(name);
  }

  /**
  * Adds the specified JDK version description to the test configuration.
  * @param jvd the JDK version description to add
  * @throws HydraConfigException if a JDK version description with the same name
  *         already exists
  */
  protected void addJDKVersionDescription(JDKVersionDescription jvd)
  {
    String name = jvd.getName();
    if (JDKVersionDescriptions.containsKey(name)) {
      String s = "JDK Version description " + name + " already exists";
      throw new HydraConfigException(s);
    }
    JDKVersionDescriptions.put(name, jvd);
  }

//------------------------------------------------------------------------------
// VersionDescription

  /**
   * Returns a mapping of the logical name of the version description to
   * its {@link VersionDescription}.
   */
  public Map<String,VersionDescription> getVersionDescriptions() {
    return VersionDescriptions;
  }

  public VersionDescription getVersionDescription(String name) {
    return (VersionDescription)VersionDescriptions.get(name);
  }

  /**
  * Adds the specified version description to the test configuration.
  * @param vd the version description to add
  * @throws HydraConfigException if a version description with the same name
  *         already exists
  */
  protected void addVersionDescription(VersionDescription vd) {
    String name = vd.getName();
    if (VersionDescriptions.containsKey(name)) {
      String s = "Version description " + name + " already exists";
      throw new HydraConfigException(s);
    }
    VersionDescriptions.put(name, vd);
  }

//------------------------------------------------------------------------------

  /**
   * Returns a mapping of the logical name of the admin system to
   * its {@link AdminDescription}.
   */
  public Map getAdminDescriptions() {
    return AdminDescriptions;
  }
  public AdminDescription getAdminDescription(String name) {
    return (AdminDescription)AdminDescriptions.get(name);
  }
  /**
  * Adds the specified admin description to the test configuration.
  * @param ad the admin description to add
  * @throws HydraConfigException if an admin description with the same name already exists
  */
  protected void addAdminDescription(AdminDescription ad) {
    String name = ad.getName();
    if (AdminDescriptions.containsKey(name))
      throw new HydraConfigException("Admin description " + name + " already exists");
    AdminDescriptions.put(name, ad);
  }
  /**
   * Returns a mapping of the logical name of the agent system to
   * its {@link AgentDescription}.
   */
  public Map getAgentDescriptions() {
    return AgentDescriptions;
  }
  public AgentDescription getAgentDescription(String name) {
    return (AgentDescription)AgentDescriptions.get(name);
  }
  /**
  * Adds the specified agent description to the test configuration.
  * @param ad the agent description to add
  * @throws HydraConfigException if an agent description with the same name already exists
  */
  protected void addAgentDescription(AgentDescription ad) {
    String name = ad.getName();
    if (AgentDescriptions.containsKey(name))
      throw new HydraConfigException("Agent description " + name + " already exists");
    AgentDescriptions.put(name, ad);
  }
  public Map getVmDescriptions() {
    return VmDescriptions;
  }
  public VmDescription getVmDescription( String name ) {
    return (VmDescription) VmDescriptions.get( name );
  }
  /**
  * Adds the specified vm description to the test configuration.
  * @param vmd the vm description to add
  * @throws HydraConfigException if a vm description with the same name already exists
  */
  protected void addVmDescription( VmDescription vmd ) {
    String name = vmd.getName();
    if ( VmDescriptions.containsKey( name ) )
      throw new HydraConfigException( "VM description " + name + " already exists" );
    VmDescriptions.put( name, vmd );
  }

  public SortedMap<String,HostAgentDescription> getHostAgentDescriptions() {
    return HostAgentDescriptions;
  }
  private void setHostAgentDescriptions(SortedMap m) {
    HostAgentDescriptions = m;
  }
  public HostAgentDescription getHostAgentDescription( String name ) {
    return (HostAgentDescription) HostAgentDescriptions.get( name );
  }
  /**
  * Adds the specified hostagent description to the test configuration.
  * @param had the hostagent description to add
  * @throws HydraConfigException if a hostagent description with the same name already exists
  */
  protected void addHostAgentDescription( HostAgentDescription had ) {
    String name = had.getName();
    if ( HostAgentDescriptions.containsKey( name ) )
      throw new HydraConfigException( "Hostagent description " + name + " already exists" );
    HostAgentDescriptions.put( name, had );
  }

  public Map getBridgeDescriptions() {
    return BridgeDescriptions;
  }
  public BridgeDescription getBridgeDescription( String name ) {
    return (BridgeDescription) BridgeDescriptions.get( name );
  }
  /**
  * Adds the specified bridge description to the test configuration.
  * @param bd the bridge description to add
  * @throws HydraConfigException if a bridge description with the same name already exists
  */
  protected void addBridgeDescription( BridgeDescription bd ) {
    String name = bd.getName();
    if ( BridgeDescriptions.containsKey( name ) ) {
      throw new HydraConfigException( "Bridge description " + name + " already exists" );
    }
    BridgeDescriptions.put( name, bd );
  }

  public Map getPoolDescriptions() {
    return PoolDescriptions;
  }
  public PoolDescription getPoolDescription(String name) {
    return (PoolDescription)PoolDescriptions.get(name);
  }
  /**
  * Adds the specified pool description to the test configuration.
  * @param pd the pool description to add
  * @throws HydraConfigException if a pool description with the same name already exists
  */
  protected void addPoolDescription(PoolDescription pd) {
    String name = pd.getName();
    if (PoolDescriptions.containsKey(name)) {
      throw new HydraConfigException("Pool description " + name + " already exists");
    }
    PoolDescriptions.put(name, pd);
  }

  public Map getDiskStoreDescriptions() {
    return DiskStoreDescriptions;
  }
  public DiskStoreDescription getDiskStoreDescription(String name) {
    return (DiskStoreDescription)DiskStoreDescriptions.get(name);
  }
  /**
  * Adds the specified disk store description to the test configuration.
  * @param dsd the disk store description to add
  * @throws HydraConfigException if a disk store description with the same name already exists
  */
  protected void addDiskStoreDescription(DiskStoreDescription dsd) {
    String name = dsd.getName();
    if (DiskStoreDescriptions.containsKey(name)) {
      throw new HydraConfigException("Disk store description " + name + " already exists");
    }
    DiskStoreDescriptions.put(name, dsd);
  }

  public Map getHDFSStoreDescriptions() {
    return HDFSStoreDescriptions;
  }

  public HDFSStoreDescription getHDFSStoreDescription(String name) {
    return (HDFSStoreDescription)HDFSStoreDescriptions.get(name);
  }

  /**
  * Adds the specified HDFS description to the test configuration.
  * @param hsd the HDFS store description to add
  * @throws HydraConfigException if a HDFS store description with the same name already exists
  */
  protected void addHDFSStoreDescription(HDFSStoreDescription hsd) {
    String name = hsd.getName();
    if (HDFSStoreDescriptions.containsKey(name)) {
      throw new HydraConfigException("HDFS store description " + name + " already exists");
    }
    HDFSStoreDescriptions.put(name, hsd);
  }

  public Map getCacheDescriptions() {
    return CacheDescriptions;
  }
  public CacheDescription getCacheDescription(String name) {
    return (CacheDescription)CacheDescriptions.get(name);
  }
  /**
  * Adds the specified cache description to the test configuration.
  * @param cd the cache description to add
  * @throws HydraConfigException if a cache description with the same name already exists
  */
  protected void addCacheDescription(CacheDescription cd) {
    String name = cd.getName();
    if (CacheDescriptions.containsKey(name)) {
      throw new HydraConfigException("Cache description " + name + " already exists");
    }
    CacheDescriptions.put(name, cd);
  }

  public Map<String,ClientCacheDescription> getClientCacheDescriptions() {
    return ClientCacheDescriptions;
  }

  public ClientCacheDescription getClientCacheDescription(String name) {
    return (ClientCacheDescription)ClientCacheDescriptions.get(name);
  }

  /**
   * Adds the specified client cache description to the test configuration.
   * @param ccd the client cache description to add
   * @throws HydraConfigException if a client cache description
   *                              with the same name already exists
   */
  protected void addClientCacheDescription(ClientCacheDescription ccd) {
    String name = ccd.getName();
    if (ClientCacheDescriptions.containsKey(name)) {
      String s = "Client cache description " + name + " already exists";
      throw new HydraConfigException(s);
    }
    ClientCacheDescriptions.put(name, ccd);
  }

  public Map getResourceManagerDescriptions() {
    return ResourceManagerDescriptions;
  }
  public ResourceManagerDescription getResourceManagerDescription(String name) {
    return (ResourceManagerDescription)ResourceManagerDescriptions.get(name);
  }
  /**
  * Adds the specified resource manager description to the test configuration.
  * @param rmd the resource manager description to add
  * @throws HydraConfigException if a resource manager description with the same name already exists
  */
  protected void addResourceManagerDescription(ResourceManagerDescription rmd) {
    String name = rmd.getName();
    if (ResourceManagerDescriptions.containsKey(name)) {
      throw new HydraConfigException("Resource manager description " + name + " already exists");
    }
    ResourceManagerDescriptions.put(name, rmd);
  }

  public Map getRegionDescriptions() {
    return RegionDescriptions;
  }
  public RegionDescription getRegionDescription(String name) {
    return (RegionDescription)RegionDescriptions.get(name);
  }
  /**
  * Adds the specified region description to the test configuration.
  * @param rd the region description to add
  * @throws HydraConfigException if a region description with the same name already exists
  */
  protected void addRegionDescription(RegionDescription rd) {
    String name = rd.getName();
    if (RegionDescriptions.containsKey(name)) {
      throw new HydraConfigException("Region description " + name + " already exists");
    }
    RegionDescriptions.put(name, rd);
  }

  public Map<String,ClientRegionDescription> getClientRegionDescriptions() {
    return ClientRegionDescriptions;
  }

  public ClientRegionDescription getClientRegionDescription(String name) {
    return (ClientRegionDescription)ClientRegionDescriptions.get(name);
  }

  /**
   * Adds the specified client region description to the test configuration.
   * @param crd the client region description to add
   * @throws HydraConfigException if a client region description
   *                              with the same name already exists
   */
  protected void addClientRegionDescription(ClientRegionDescription crd) {
    String name = crd.getName();
    if (ClientRegionDescriptions.containsKey(name)) {
      String s = "Client region description " + name + " already exists";
      throw new HydraConfigException(s);
    }
    ClientRegionDescriptions.put(name, crd);
  }

  public Map getFixedPartitionDescriptions() {
    return FixedPartitionDescriptions;
  }
  public FixedPartitionDescription getFixedPartitionDescription(String name) {
    return (FixedPartitionDescription)FixedPartitionDescriptions.get(name);
  }
  /**
  * Adds the specified fixed partition description to the test configuration.
  * @param fpd the fixed partition description to add
  * @throws HydraConfigException if a fixed partition description with the same name already exists
  */
  protected void addFixedPartitionDescription(FixedPartitionDescription fpd) {
    String name = fpd.getName();
    if (FixedPartitionDescriptions.containsKey(name)) {
      throw new HydraConfigException("Partition description " + name + " already exists");
    }
    FixedPartitionDescriptions.put(name, fpd);
  }

  public Map getPartitionDescriptions() {
    return PartitionDescriptions;
  }
  public PartitionDescription getPartitionDescription(String name) {
    return (PartitionDescription)PartitionDescriptions.get(name);
  }
  /**
  * Adds the specified partition description to the test configuration.
  * @param pd the partition description to add
  * @throws HydraConfigException if a partition description with the same name already exists
  */
  protected void addPartitionDescription(PartitionDescription pd) {
    String name = pd.getName();
    if (PartitionDescriptions.containsKey(name)) {
      throw new HydraConfigException("Partition description " + name + " already exists");
    }
    PartitionDescriptions.put(name, pd);
  }

  public Map getSecurityDescriptions() {
    return SecurityDescriptions;
  }
  public SecurityDescription getSecurityDescription(String name) {
    return (SecurityDescription)SecurityDescriptions.get(name);
  }
  /**
  * Adds the specified security description to the test configuration.
  * @param sd the security description to add
  * @throws HydraConfigException if a security description with the same name already exists
  */
  protected void addSecurityDescription(SecurityDescription sd) {
    String name = sd.getName();
    if (SecurityDescriptions.containsKey(name)) {
      throw new HydraConfigException("Security description " + name + " already exists");
    }
    SecurityDescriptions.put(name, sd);
  }

  public Map getSSLDescriptions() {
    return SSLDescriptions;
  }
  public SSLDescription getSSLDescription(String name) {
    return (SSLDescription)SSLDescriptions.get(name);
  }
  /**
  * Adds the specified SSL description to the test configuration.
  * @param sd the SSL description to add
  * @throws HydraConfigException if a SSL description with the same name already exists
  */
  protected void addSSLDescription(SSLDescription sd) {
    String name = sd.getName();
    if (SSLDescriptions.containsKey(name)) {
      throw new HydraConfigException("SSL description " + name + " already exists");
    }
    SSLDescriptions.put(name, sd);
  }

  public Map getGatewayReceiverDescriptions() {
    return GatewayReceiverDescriptions;
  }
  public GatewayReceiverDescription getGatewayReceiverDescription(String name) {
    return (GatewayReceiverDescription)GatewayReceiverDescriptions.get(name);
  }
  /**
  * Adds the specified gateway receiver description to the test configuration.
  * @param grd the gateway receiver description to add
  * @throws HydraConfigException if a gateway receiver description with the same name already exists
  */
  protected void addGatewayReceiverDescription(GatewayReceiverDescription grd) {
    String name = grd.getName();
    if (GatewayReceiverDescriptions.containsKey(name)) {
      throw new HydraConfigException("Gateway receiver description " + name + " already exists");
    }
    GatewayReceiverDescriptions.put(name, grd);
  }

  public Map getGatewaySenderDescriptions() {
    return GatewaySenderDescriptions;
  }
  public GatewaySenderDescription getGatewaySenderDescription(String name) {
    return (GatewaySenderDescription)GatewaySenderDescriptions.get(name);
  }
  /**
  * Adds the specified gateway sender description to the test configuration.
  * @param gsd the gateway sender description to add
  * @throws HydraConfigException if a gateway sender description with the same name already exists
  */
  protected void addGatewaySenderDescription(GatewaySenderDescription gsd) {
    String name = gsd.getName();
    if (GatewaySenderDescriptions.containsKey(name)) {
      throw new HydraConfigException("Gateway sender description " + name + " already exists");
    }
    GatewaySenderDescriptions.put(name, gsd);
  }

  public Map<String,AsyncEventQueueDescription> getAsyncEventQueueDescriptions() {
    return AsyncEventQueueDescriptions;
  }
  public AsyncEventQueueDescription getAsyncEventQueueDescription(String name) {
    return AsyncEventQueueDescriptions.get(name);
  }
  /**
  * Adds the specified async event queue description to the test configuration.
  * @param aeqd the async event queue description to add
  * @throws HydraConfigException if an async event queue description with the same name already exists
  */
  protected void addAsyncEventQueueDescription(AsyncEventQueueDescription aeqd) {
    String name = aeqd.getName();
    if (AsyncEventQueueDescriptions.containsKey(name)) {
      throw new HydraConfigException("Async event queue description " + name + " already exists");
    }
    AsyncEventQueueDescriptions.put(name, aeqd);
  }

  public Map getGatewayHubDescriptions() {
    return GatewayHubDescriptions;
  }
  public GatewayHubDescription getGatewayHubDescription(String name) {
    return (GatewayHubDescription)GatewayHubDescriptions.get(name);
  }
  /**
  * Adds the specified gateway hub description to the test configuration.
  * @param ghd the gateway hub description to add
  * @throws HydraConfigException if a gateway hub description with the same name already exists
  */
  protected void addGatewayHubDescription(GatewayHubDescription ghd) {
    String name = ghd.getName();
    if (GatewayHubDescriptions.containsKey(name)) {
      throw new HydraConfigException("Gateway hub description " + name + " already exists");
    }
    GatewayHubDescriptions.put(name, ghd);
  }

  public Map getGatewayDescriptions() {
    return GatewayDescriptions;
  }
  public GatewayDescription getGatewayDescription(String name) {
    return (GatewayDescription)GatewayDescriptions.get(name);
  }
  /**
  * Adds the specified gateway description to the test configuration.
  * @param gd the gateway description to add
  * @throws HydraConfigException if a gateway description with the same name already exists
  */
  protected void addGatewayDescription(GatewayDescription gd) {
    String name = gd.getName();
    if (GatewayDescriptions.containsKey(name)) {
      throw new HydraConfigException("Gateway description " + name + " already exists");
    }
    GatewayDescriptions.put(name, gd);
  }

  public Map<String,ClientDescription> getClientDescriptions() {
    return ClientDescriptions;
  }
  public ClientDescription getClientDescription( String name ) {
    return ClientDescriptions.get(name);
  }
  /**
   * Returns the set of logical client names specified by {@link ClientDescription}
   * objects via {@link ClientPrms#names}.
   */
  public Vector getClientNames() {
    Vector names = new Vector();
    for (ClientDescription cd : ClientDescriptions.values()) {
      names.add( cd.getName() );
    }
    return ( names.size() == 0 ) ? null : names;
  }
  /**
  * Adds the specified client description to the test configuration.
  * @param cd the client description to add
  * @throws HydraConfigException if a client description with the same name already exists
  */
  protected void addClientDescription( ClientDescription cd ) {
    String name = cd.getName();
    if ( ClientDescriptions.containsKey( name ) )
      throw new HydraConfigException( "Client description " + name + " already exists" );
    ClientDescriptions.put( name, cd );
  }
  /**
  * Returns the total number of vms the client descriptions generate.
  */
  public int getTotalVMs() {
    int totalVMs = 0;
    for (ClientDescription cd : ClientDescriptions.values()) {
      totalVMs += cd.getVmQuantity();
    }
    return totalVMs;
  }
  /**
  * Returns the total number of threads the client descriptions generate.
  */
  public int getTotalThreads() {
    int totalThreads = 0;
    for (ClientDescription cd : ClientDescriptions.values()) {
      totalThreads += cd.getVmQuantity() * cd.getVmThreads();
    }
    return totalThreads;
  }
  public Map getJProbeDescriptions() {
    return JProbeDescriptions;
  }
  public JProbeDescription getJProbeDescription( String name ) {
    return (JProbeDescription) JProbeDescriptions.get( name );
  }
  /**
  * Adds the specified jprobe description to the test configuration.
  * @param jpd the jprobe description to add
  * @throws HydraConfigException if a jprobe description with the same name already exists
  */
  protected void addJProbeDescription( JProbeDescription jpd ) {
    String name = jpd.getName();
    if ( JProbeDescriptions.containsKey( name ) )
      throw new HydraConfigException( "JProbe description " + name + " already exists" );
    JProbeDescriptions.put( name, jpd );
  }
  
  public Map getHypericServerInstallDescriptions() {
      return HypericServerInstallDescriptions;
    }
    public HypericServerInstallDescription getHypericServerInstallDescription( String name ) {
      return (HypericServerInstallDescription) HypericServerInstallDescriptions.get( name );
    }
    /**
    * Adds the specified HypericServerInstall description to the test configuration.
    * @param hqd the HypericServerInstall description to add
    * @throws HydraConfigException if a HypericServerInstall description with the same name already exists
    */
    protected void addHypericServerInstallDescription( HypericServerInstallDescription hqd ) {
      String name = hqd.getName();
      if ( HypericServerInstallDescriptions.containsKey( name ) )
        throw new HydraConfigException( "HypericServerInstall description " + name + " already exists" );
      HypericServerInstallDescriptions.put( name, hqd );
    }

  /**
   * Returns all of the Hydra system properties
   */
  public Map getSystemProperties() {
    return SystemProperties;
  }

  /**
   * Returns the Hydra system property with for the given
   * <code>key</code> (name)
   */
  public String getSystemProperty( String key ) {
    return (String) SystemProperties.get( key );
  }

  /**
  * Adds the specified system property to the test configuration.
  * @param key the property key
  * @param val the property value
  * @throws HydraConfigException if a property with the same key already exists with a different value
  */
  protected void addSystemProperty( String key, String val ) {
    String existingVal = (String) SystemProperties.get( key );
    if ( existingVal == null ) {
      SystemProperties.put( key, val );
    } else if ( ! existingVal.equals( val ) ) {
      throw new HydraConfigException( "System property " + key + " already exists with value " + existingVal + ", cannot reset it to " + val );
    }
  }
  public ConfigHashtable getParameters() {
    return Parameters;
  }
  private void setParameters(ConfigHashtable p) {
    Parameters = p;
  }
  /**
  * Puts the parameter value in the configuration table at the specified key.
  * If the key is already there, its value is overridden with the new value.
  * Croaks if the key is defined internallly.
  * @param key the key, as defined in class Prms
  * @param value the value
  */
  protected void addParameter( Long key, Object value ) {
    Parameters.put( key, value );
  }
  /**
  * Puts the parameter value in the configuration table at the specified key.
  * If the key is already there, its value is appended with the new value as
  * appropriate for the types of the old and new values (simple value vs.
  * list vs. list of lists).
  * @param key the key, as defined in class Prms
  * @param value the value
  */
  protected void addToParameter( Long key, Object value ) {
    // first handle special string cases
    if (key == Prms.testDescription) {
      String val = (String)Parameters.get(key, "");
      Parameters.put(key, val + " " + value);
      return;
    }
    if (key == Prms.testRequirement) {
      String val = (String)Parameters.get(key, "");
      Parameters.put(key, val + " " + value);
      return;
    }
    if (key == Prms.testTopology) {
      String val = (String)Parameters.get(key, "");
      Parameters.put(key, val + " " + value);
      return;
    }

    // treat general case as a vector
    HydraVector v = Parameters.vecAt( key, null );
    HydraVector w = value instanceof HydraVector ? (HydraVector) value : new HydraVector( value );
    if ( v == null ) {
      Parameters.put( key, value );
    } else {
      Parameters.put( key, combine( v, w ) );
    }
  }
  private HydraVector combine( HydraVector v, HydraVector w ) {
    Object v0 = v.elementAt(0); Object w0 = w.elementAt(0);
    if ( v0 instanceof HydraVector && w0 instanceof HydraVector ) {
      expandVecs( v, w );
      return combineVecs( v, w );
    } else if ( v0 instanceof HydraVector ) {
      HydraVector x = new HydraVector( w );
      expandVecs( v, x );
      return combineVecs( v, x );
    } else if ( w0 instanceof HydraVector ) {
      HydraVector x = new HydraVector( v );
      expandVecs( x, w );
      return combineVecs( x, w );
    } else {
      HydraVector x = new HydraVector();
      x.addAll( v ); x.addAll( w );
      return x;
    }
  }
  private void expandVecs( HydraVector v, HydraVector w ) {
    if ( v.size() < w.size() ) {
      expandVec( v, w.size() );
    } else if ( v.size() > w.size() ) {
      expandVec( w, v.size() );
    } // else nothing to do
  }
  private void expandVec( HydraVector v, int size ) {
    Object last = v.elementAt( v.size() - 1 );
    for ( int i = v.size(); i < size; i++ ) {
      v.addElement( last );
    }
  }
  private HydraVector combineVecs( HydraVector v, HydraVector w ) {
    HydraVector x = new HydraVector();
    for ( int i = 0; i < v.size(); i++ ) {
      HydraVector vv = (HydraVector) v.elementAt( i );
      HydraVector ww = (HydraVector) w.elementAt( i );
      HydraVector tt = new HydraVector();
      tt.addAll( vv ); tt.addAll( ww );
      x.add( tt );
    }
    return x;
  }
  public List<String> getClassNames() {
    return ClassNames;
  }
  public List<String> getClassNames(String prefix) {
    List<String> classnames = null;
    for (String classname : ClassNames) {
      if (classname.startsWith(prefix)) {
        if (classnames == null) {
          classnames = new ArrayList();
        }
        classnames.add(classname);
      }
    }
    return classnames;
  }
  private void setClassNames(List<String> v) {
    ClassNames = v;
  }

  /**
  * Stores the classnames for parameters in the parameter map for use by clients.
  * @param name the class name
  */
  protected void addClassName( String name ) {
    // eliminate duplicates
    if ( ! ClassNames.contains( name ) )
      ClassNames.add( name );
  }
  public String toString() {
    StringBuffer buf = new StringBuffer();
    SortedMap map = this.toSortedMap();
    for ( Iterator i = map.keySet().iterator(); i.hasNext(); ) {
      String key = (String) i.next();
      Object val = map.get( key );
      buf.append( key + "=" + val + "\n" );
    }
    return buf.toString();
  }
  public SortedMap toSortedMap() {
    SortedMap map = new TreeMap();
    map.put( "TestName", TestName );
    map.put( "TestUser", TestUser );
    map.put( "build.version", BuildVersion );
    map.put( "build.date", BuildDate );
    map.put( "source.repository", SourceRepository );
    map.put( "source.revision", SourceRevision );
    map.put( "source.date", SourceDate );
    map.put( "build.jdk", BuildJDK );
    map.put( "runtime.jdk", RuntimeJDK );
    map.put( "java.vm.name", JavaVMName );

    for ( Iterator i = SystemProperties.keySet().iterator(); i.hasNext(); ) {
      String key = (String) i.next();
      map.put( "hydra.SystemProperties-" + key, SystemProperties.get( key ) );
    }

    if ( TheMasterDescription != null )
      map.putAll( TheMasterDescription.toSortedMap() );

    if ( UnitTests != null )
      for ( Iterator i = UnitTests.iterator(); i.hasNext(); )
        map.putAll( ((TestTask)i.next()).toSortedMap() );
    if ( StartTasks != null )
      for ( Iterator i = StartTasks.iterator(); i.hasNext(); )
        map.putAll( ((TestTask)i.next()).toSortedMap() );
    if ( InitTasks != null )
      for ( Iterator i = InitTasks.iterator(); i.hasNext(); )
        map.putAll( ((TestTask)i.next()).toSortedMap() );
    if ( Tasks != null )
      for ( Iterator i = Tasks.iterator(); i.hasNext(); )
        map.putAll( ((TestTask)i.next()).toSortedMap() );
    if ( CloseTasks != null )
      for ( Iterator i = CloseTasks.iterator(); i.hasNext(); )
        map.putAll( ((TestTask)i.next()).toSortedMap() );
    if ( EndTasks != null )
      for ( Iterator i = EndTasks.iterator(); i.hasNext(); )
        map.putAll( ((TestTask)i.next()).toSortedMap() );

    if ( ThreadGroups.size() != 0 )
      for ( Iterator i = ThreadGroups.values().iterator(); i.hasNext(); )
        map.putAll( ((HydraThreadGroup)i.next()).toSortedMap() );
    if ( HostDescriptions.size() != 0 )
      for ( Iterator i = HostDescriptions.values().iterator(); i.hasNext(); )
        map.putAll( ((HostDescription)i.next()).toSortedMap() );
    if ( HadoopHostDescriptions.size() != 0 )
      for ( Iterator i = HadoopHostDescriptions.values().iterator(); i.hasNext(); )
        map.putAll( ((HostDescription)i.next()).toSortedMap() );
    if ( HadoopDescriptions.size() != 0 )
      for ( Iterator i = HadoopDescriptions.values().iterator(); i.hasNext(); )
        map.putAll( ((HadoopDescription)i.next()).toSortedMap() );
    if ( GemFireDescriptions.size() != 0 )
      for ( Iterator i = GemFireDescriptions.values().iterator(); i.hasNext(); )
        map.putAll( ((GemFireDescription)i.next()).toSortedMap() );
    if ( JDKVersionDescriptions.size() != 0 )
      for (JDKVersionDescription jvd : JDKVersionDescriptions.values())
        map.putAll(jvd.toSortedMap());
    if ( VersionDescriptions.size() != 0 )
      for (VersionDescription vd : VersionDescriptions.values())
        map.putAll(vd.toSortedMap());
    if ( AdminDescriptions.size() != 0 )
      for ( Iterator i = AdminDescriptions.values().iterator(); i.hasNext(); )
        map.putAll( ((AdminDescription)i.next()).toSortedMap() );
    if ( AgentDescriptions.size() != 0 )
      for ( Iterator i = AgentDescriptions.values().iterator(); i.hasNext(); )
        map.putAll( ((AgentDescription)i.next()).toSortedMap() );
    if ( JProbeDescriptions.size() != 0 )
      for ( Iterator i = JProbeDescriptions.values().iterator(); i.hasNext(); )
        map.putAll( ((JProbeDescription)i.next()).toSortedMap() );
    if ( VmDescriptions.size() != 0 )
      for ( Iterator i = VmDescriptions.values().iterator(); i.hasNext(); )
        map.putAll( ((VmDescription)i.next()).toSortedMap() );
    if ( HostAgentDescriptions.size() != 0 )
      for ( Iterator i = HostAgentDescriptions.values().iterator(); i.hasNext(); )
        map.putAll( ((HostAgentDescription)i.next()).toSortedMap() );
    if ( ClientDescriptions.size() != 0 )
      for (ClientDescription cd : ClientDescriptions.values())
        map.putAll(cd.toSortedMap());
    if ( BridgeDescriptions.size() != 0 )
      for ( Iterator i = BridgeDescriptions.values().iterator(); i.hasNext(); )
        map.putAll( ((BridgeDescription)i.next()).toSortedMap() );
    if (PoolDescriptions.size() != 0 )
      for ( Iterator i = PoolDescriptions.values().iterator(); i.hasNext(); )
        map.putAll( ((PoolDescription)i.next()).toSortedMap() );
    if (DiskStoreDescriptions.size() != 0 )
      for ( Iterator i = DiskStoreDescriptions.values().iterator(); i.hasNext(); )
        map.putAll( ((DiskStoreDescription)i.next()).toSortedMap() );
    if (HDFSStoreDescriptions.size() != 0 )
      for ( Iterator i = HDFSStoreDescriptions.values().iterator(); i.hasNext(); )
        map.putAll( ((HDFSStoreDescription)i.next()).toSortedMap() );
    if (CacheDescriptions.size() != 0 )
      for ( Iterator i = CacheDescriptions.values().iterator(); i.hasNext(); )
        map.putAll( ((CacheDescription)i.next()).toSortedMap() );
    if (ClientCacheDescriptions.size() != 0) {
      for (ClientCacheDescription ccd : ClientCacheDescriptions.values()) {
        map.putAll(ccd.toSortedMap());
      }
    }
    if (ResourceManagerDescriptions.size() != 0 )
      for ( Iterator i = ResourceManagerDescriptions.values().iterator(); i.hasNext(); )
        map.putAll( ((ResourceManagerDescription)i.next()).toSortedMap() );
    if (RegionDescriptions.size() != 0 )
      for ( Iterator i = RegionDescriptions.values().iterator(); i.hasNext(); )
        map.putAll( ((RegionDescription)i.next()).toSortedMap() );
    if (ClientRegionDescriptions.size() != 0) {
      for (ClientRegionDescription crd : ClientRegionDescriptions.values()) {
        map.putAll(crd.toSortedMap());
      }
    }
    if (FixedPartitionDescriptions.size() != 0 )
      for ( Iterator i = FixedPartitionDescriptions.values().iterator(); i.hasNext(); )
        map.putAll( ((FixedPartitionDescription)i.next()).toSortedMap() );
    if (PartitionDescriptions.size() != 0 )
      for ( Iterator i = PartitionDescriptions.values().iterator(); i.hasNext(); )
        map.putAll( ((PartitionDescription)i.next()).toSortedMap() );
    if (SecurityDescriptions.size() != 0 )
      for ( Iterator i = SecurityDescriptions.values().iterator(); i.hasNext(); )
        map.putAll( ((SecurityDescription)i.next()).toSortedMap() );
    if (SSLDescriptions.size() != 0 )
      for ( Iterator i = SSLDescriptions.values().iterator(); i.hasNext(); )
        map.putAll( ((SSLDescription)i.next()).toSortedMap() );
    if (GatewayReceiverDescriptions.size() != 0)
      for (Iterator i = GatewayReceiverDescriptions.values().iterator(); i.hasNext();)
        map.putAll(((GatewayReceiverDescription)i.next()).toSortedMap());
    if (GatewaySenderDescriptions.size() != 0)
      for (Iterator i = GatewaySenderDescriptions.values().iterator(); i.hasNext();)
        map.putAll(((GatewaySenderDescription)i.next()).toSortedMap());
    if (AsyncEventQueueDescriptions.size() != 0)
      for (AsyncEventQueueDescription aeqd:AsyncEventQueueDescriptions.values())
        map.putAll(aeqd.toSortedMap());
    if (GatewayHubDescriptions.size() != 0)
      for (Iterator i = GatewayHubDescriptions.values().iterator(); i.hasNext();)
        map.putAll(((GatewayHubDescription)i.next()).toSortedMap());
    if (GatewayDescriptions.size() != 0)
      for (Iterator i = GatewayDescriptions.values().iterator(); i.hasNext();)
        map.putAll(((GatewayDescription)i.next()).toSortedMap());
    if (HypericServerInstallDescriptions.size() != 0)
        for (Iterator i = HypericServerInstallDescriptions.values().iterator(); i.hasNext();)
          map.putAll(((HypericServerInstallDescription)i.next()).toSortedMap());
    
    if ( Parameters.size() != 0 )
      map.putAll( Parameters.toSortedMap() );

    return map;
  }

  //////////////////////////////////////////////////////////////////////////////
  ////  INTERNAL                                                            ////
  //////////////////////////////////////////////////////////////////////////////

  /**
  * For internal hydra use only.
  */
  protected void postprocess() {
    setBuildInfo();
    if ( UnitTests != null ) {
      setThreadGroupsForUnitTests( UnitTests );
      setThreadGroups( InitTasks );
      Vector tasks = new Vector();
      if (InitTasks != null) {
        tasks.addAll( InitTasks );
      }
      tasks.addAll( UnitTests );
      postprocessThreadGroups( tasks );
      postprocessUnitTests( UnitTests, InitTasks );
    } else {
      if ( ClientDescriptions.size() == 0 ) {
        throw new HydraConfigException( "No clients specified" );
      }
      Vector tasks = new Vector();
      if ( InitTasks != null )  tasks.addAll( InitTasks );
      if ( Tasks != null )      tasks.addAll( Tasks );
      if ( CloseTasks != null ) tasks.addAll( CloseTasks );
      setThreadGroups( tasks );
      // do threadgroups before dynamic init/close splits
      Map vms = postprocessThreadGroups( tasks );
      postprocessStartAndEndTasks( StartTasks );
      postprocessInitTasks( InitTasks );
      postprocessComplexTasks( Tasks, vms );
      postprocessCloseTasks( CloseTasks );
      postprocessStartAndEndTasks( EndTasks );
      // make sure inittasks, tasks, closetasks have threads to run them
      checkForZeroTotalThreads(tasks);
    }
    postprocessGemFireDescriptions();
    TheMasterDescription.postprocess();
  }

  /**
  * For internal hydra use only.
  *
  * Returns the random seed.
  */
  protected long getRandomSeed() {
    return Parameters.longAt( Prms.randomSeed, -1 );
  }

  /**
  * For internal hydra use only.
  *
  * Generates a random seed based on the system clock.
  */
  protected long generateRandomSeed() {
    return Long.valueOf(System.currentTimeMillis());
  }

  /**
  * For internal hydra use only.
  *
  * Tells the test configuration to set the seed for the random number
  * generator used by its configuration table to a seed based on the
  * system clock.
  *
  * Also logs the seed using the "severe" log level so that it can be
  * used to attempt to reproduce test results even when logging is minimal.
  */
  public void setRandomSeed( Long randomSeed ) {
    Parameters.put( Prms.randomSeed, randomSeed );
    log().severe( "Random seed is " + randomSeed );
  }

  //////////////////////////////////////////////////////////////////////////////
  ////  PRIVATE                                                             ////
  //////////////////////////////////////////////////////////////////////////////

  public static Properties getGemFireProductVersion() {
    String fn = "com/gemstone/gemfire/internal/GemFireVersion.properties";
    try {
      return FileUtil.getPropertiesFromResource(fn);
    } catch (IOException e) {
      String s = "Unable to find GemFire product version";
      throw new HydraRuntimeException(s, e);
    }
  }

  // master only
  private void setBuildInfo() {
    RuntimeJDK = System.getProperty("java.version");
    JavaVMName = System.getProperty("java.vm.name");
    Properties p = ProductVersionHelper.getInfo();
    if (p != null) {
      BuildVersion = p.getProperty(ProductVersionHelper.PRODUCT_VERSION) + " "
                   + p.getProperty(ProductVersionHelper.BUILD_ID) + " "
                   + p.getProperty(ProductVersionHelper.BUILD_DATE) + " javac "
                   + p.getProperty(ProductVersionHelper.BUILD_JAVA_VERSION);
      BuildDate = p.getProperty(ProductVersionHelper.BUILD_DATE);
      SourceRepository = p.getProperty(ProductVersionHelper.SOURCE_REPOSITORY);
      SourceRevision = p.getProperty(ProductVersionHelper.SOURCE_REVISION);
      SourceDate = p.getProperty(ProductVersionHelper.SOURCE_DATE);
      BuildJDK = p.getProperty(ProductVersionHelper.BUILD_JAVA_VERSION);
    }
  }
  private void postprocessUnitTests( Vector tests, Vector tasks ) {
    setTaskIndex( tests );
    checkReceiverSelector( tests );
    postprocessInitTasks( tasks );
  }
  private void postprocessStartAndEndTasks( Vector tasks ) {
    if ( tasks == null ) return;
    setTaskIndex( tasks );
    setClientNames( tasks );
    checkReceiverSelector( tasks );
  }
  private void postprocessInitTasks( Vector tasks ) {
    if ( tasks == null ) return;
    setRunMode( tasks );
    for ( Iterator i = InitTasks.iterator(); i.hasNext(); ) {
      TestTask tt = (TestTask) i.next();
      switch( tt.getRunMode() ) {
        case TestTask.ALWAYS:
          addStaticInitTask( (TestTask) tt.clone() );
          addDynamicInitTask( tt );
          tt.setTaskType( TestTask.DYNAMICINITTASK );
          break;
        case TestTask.ONCE:
          addStaticInitTask( tt );
          break;
        case TestTask.DYNAMIC:
          addDynamicInitTask( tt );
          tt.setTaskType( TestTask.DYNAMICINITTASK );
          break;
        default:
          throw new HydraInternalException( "run mode not set" );
      }
    }
    if ( StaticInitTasks != null ) setTaskIndex( StaticInitTasks );
    if ( DynamicInitTasks != null ) setTaskIndex( DynamicInitTasks );
    checkReceiverSelector( tasks );
  }
  private void postprocessCloseTasks( Vector tasks ) {
    if ( tasks == null ) return;
    setRunMode( tasks );
    for ( Iterator i = CloseTasks.iterator(); i.hasNext(); ) {
      TestTask tt = (TestTask) i.next();
      switch( tt.getRunMode() ) {
        case TestTask.ALWAYS:
          addStaticCloseTask( (TestTask) tt.clone() );
          addDynamicCloseTask( tt );
          tt.setTaskType( TestTask.DYNAMICCLOSETASK );
          break;
        case TestTask.ONCE:
          addStaticCloseTask( tt );
          break;
        case TestTask.DYNAMIC:
          addDynamicCloseTask( tt );
          tt.setTaskType( TestTask.DYNAMICCLOSETASK );
          break;
        default:
          throw new HydraInternalException( "run mode not set" );
      }
    }
    if ( StaticCloseTasks != null ) setTaskIndex( StaticCloseTasks );
    if ( DynamicCloseTasks != null ) setTaskIndex( DynamicCloseTasks );
    checkReceiverSelector( tasks );
  }
  private void postprocessComplexTasks( Vector tasks, Map vms ) {
    if ( tasks == null ) return;
    setTaskIndex( tasks );
    setMaxTimesToRun( tasks );
    setMaxThreads( tasks );
    setStartInterval( tasks );
    setEndInterval( tasks );
    setWeight( tasks ); // need thread group info for this one
    checkReceiverSelector( tasks );
    setNumClients( tasks, vms );
  }
  private void setNumClients( Vector tasks, Map vms ) {
    for ( Iterator i = tasks.iterator(); i.hasNext(); ) {
      TestTask task = (TestTask) i.next();
      setNumClients( task, vms );
    }
  }
  private void setNumClients( TestTask task, Map vms ) {
    if ( vms == null ) {
      return;
    }
    int numClients = 0;
    for ( Iterator i = vms.values().iterator(); i.hasNext(); ) {
      ClientVmRecord vm = (ClientVmRecord) i.next();
      for ( Iterator j = vm.getClients().values().iterator(); j.hasNext(); ) {
        ClientRecord client = (ClientRecord) j.next();
        if ( task.usesThreadGroup( client.getThreadGroupName() ) ) {
         ++numClients;
        }
      }
    }
    task.setNumClients( numClients );
  }
  private void checkReceiverSelector( Vector tasks ) {
    if ( Parameters.booleanAt( Prms.checkTaskMethodsExist ) )
      for ( Iterator it = tasks.iterator(); it.hasNext(); ) {
        TestTask task = (TestTask) it.next();
        checkReceiverSelector( task );
      }
  }
  private void checkReceiverSelector( TestTask task ) {
    String receiver = task.getReceiver();
    String selector = task.getSelector();

    checkReceiverSelector( receiver, selector, "Task " + task );
  }

  protected void checkReceiverSelector( String receiver, String selector, String id ) {
    Class cls;
    try { // check for class
      cls = Class.forName( receiver );
    } catch( NoClassDefFoundError e ) {
      throw new HydraConfigException( "Class " + receiver + " not found for " + id, e );
    } catch( ClassNotFoundException e ) {
      throw new HydraConfigException( "Class " + receiver + " not found for " + id, e );
    }
    Method[] methods = cls.getMethods();
    for ( int i = 0; i < methods.length; i++ )
      if ( methods[i].getName().equals( selector ) )
        if ( methods[i].getParameterTypes().length == 0 )
          if ( Modifier.isPublic( methods[i].getModifiers() ) )
            if ( Modifier.isStatic( methods[i].getModifiers() ) )
              return;
    throw new HydraConfigException( "No public static no-argument method " + selector + " found for " + id );
  }

  protected void checkForZeroTotalThreads(Vector tasks) {
    for (Iterator i = tasks.iterator(); i.hasNext();) {
      TestTask task = (TestTask)i.next();
      if (task.getTotalThreads() == 0) {
        String s = "Task has no threads in its threadgroups: " + task;
        throw new HydraConfigException(s);
      }
    }
  }

  private void setTaskIndex( Vector tasks ) {
    for ( int i = 0; i < tasks.size(); i++ ) {
      TestTask task = (TestTask) tasks.elementAt(i);
      task.setTaskIndex( i );
    }
  }
  private void setClientNames( Vector tasks ) {
    for ( int i = 0; i < tasks.size(); i++ ) {
      TestTask task = (TestTask) tasks.elementAt(i);
      Vector names = task.getClientNames();
      if ( names == null ) { // default to any client names
        task.setClientNames( getClientNames() );
      } else { // just make sure the names are valid
        for ( Iterator it = names.iterator(); it.hasNext(); ) {
          String name = (String) it.next();
          if ( getClientDescription( name ) == null ) {
            throw new HydraConfigException
              ( "Illegal client name (" + name + ") in task: " + task );
          }
        }
      }
    }
  }
  private void setRunMode( Vector tasks ) {
    for ( int i = 0; i < tasks.size(); i++ ) {
      TestTask task = (TestTask) tasks.elementAt(i);
      int n = task.getRunMode();
      if ( n == -1 ) {
        task.setRunMode( TestTask.DEFAULT_RUN_MODE );
      }
    }
  }
  private void setMaxTimesToRun( Vector tasks ) {
    for ( int i = 0; i < tasks.size(); i++ ) {
      TestTask task = (TestTask) tasks.elementAt(i);
      int n = task.getMaxTimesToRun();
      if ( n == -1 )
        task.setMaxTimesToRun( 9999999 );
      else if ( n <= 0 )
        throw new HydraConfigException
            ( "Illegal value for maxTimesToRun (" + n + ") in task: " + task );
    }
  }
  private void setMaxThreads( Vector tasks ) {
    for ( int i = 0; i < tasks.size(); i++ ) {
      TestTask task = (TestTask) tasks.elementAt(i);
      int n = task.getMaxThreads();
      if ( n == -1 )
        task.setMaxThreads( 9999999 );
      else if ( n <= 0 )
        throw new HydraConfigException
            ( "Illegal value for maxThreads (" + n + ") in task: " + task );
    }
  }
  private void setWeight( Vector tasks ) {
    // make sure that all tasks that share a thread group
    // either do or do not use weights
    for ( int i = 0; i < tasks.size(); i++ ) {
      TestTask task = (TestTask) tasks.elementAt(i);
      Map groups = task.getThreadGroups();
      int n = task.getWeight();
      if ( n == -1 ) { // not using custom weights
        for ( Iterator it = groups.values().iterator(); it.hasNext(); ) {
          HydraThreadGroup group = (HydraThreadGroup) it.next();
          if ( group.usesCustomWeights() == null )
            group.usesCustomWeights(Boolean.FALSE);
          else if ( group.usesCustomWeights().booleanValue() )
            throw new HydraConfigException( "Either all tasks or none using thread group " + group.getName() + " must use weights" );
        }
        task.setWeight( 1 ); // set default to use in scheduling
      } else if ( n > 0 ) { // using custom weights
        for ( Iterator it = groups.values().iterator(); it.hasNext(); ) {
          HydraThreadGroup group = (HydraThreadGroup) it.next();
          if ( group.usesCustomWeights() == null )
            group.usesCustomWeights(Boolean.TRUE);
          else if ( ! group.usesCustomWeights().booleanValue() )
            throw new HydraConfigException( "Either all tasks or none using thread group " + group.getName() + " must use weights" );
        }
      } else {
        throw new HydraConfigException
        (
          "Illegal value for weight (" + n + ") in task: " + task
        );
      }
    }
  }
  private void setStartInterval( Vector tasks ) {
    for ( int i = 0; i < tasks.size(); i++ ) {
      TestTask task = (TestTask) tasks.elementAt(i);
      int n = task.getStartInterval();
      if ( n == -1 )
        task.setStartInterval( 0 );
      else if ( n <= 0 )
        throw new HydraConfigException
            ( "Illegal value for startInterval (" + n + ") in task: " + task );
    }
  }
  private void setEndInterval( Vector tasks ) {
    for ( int i = 0; i < tasks.size(); i++ ) {
      TestTask task = (TestTask) tasks.elementAt(i);
      int n = task.getEndInterval();
      if ( n == -1 )
        task.setEndInterval( 0 );
      else if ( n <= 0 )
        throw new HydraConfigException
            ( "Illegal value for endInterval (" + n + ") in task: " + task );
    }
  }
  private void setThreadGroupsForUnitTests( Vector tasks ) {
    if ( tasks == null ) return;
    // assign them all to the default thread group
    for ( int i = 0; i < tasks.size(); i++ ) {
      TestTask task = (TestTask) tasks.elementAt(i);
      HydraThreadGroup tg = getDefaultThreadGroup();
      task.addThreadGroupName( tg.getName() );
      task.addThreadGroup( tg );
    }
  }
  private void setThreadGroups( Vector tasks ) {
    if ( tasks == null ) return;
    // first do all the tasks that specify groups
    for ( int i = 0; i < tasks.size(); i++ ) {
      TestTask task = (TestTask) tasks.elementAt(i);
      Vector names = task.getThreadGroupNames();
      if ( names != null ) {
        for ( Iterator it = names.iterator(); it.hasNext(); ) {
          String name = (String) it.next();
          HydraThreadGroup tg = getThreadGroup( name );
          if ( tg == null ) { // check for default since it's created lazily
            if ( name.equalsIgnoreCase( HydraThreadGroup.DEFAULT_NAME ) ) {
              tg = getDefaultThreadGroup();
            } else {
              throw new HydraConfigException( "Thread group (" + name + ") not found for task: " + task );
            }
          }
          task.addThreadGroup( tg );
        }
      }
    }
    // now do all the TASKs that default
    for ( int i = 0; i < tasks.size(); i++ ) {
      TestTask task = (TestTask) tasks.elementAt(i);
      if ( task.getThreadGroupNames() == null
           && task.getTaskType() == TestTask.TASK ) {
        HydraThreadGroup tg = getDefaultThreadGroup();
        task.addThreadGroupName( tg.getName() );
        task.addThreadGroup( tg );
      }
    }
    // now do all the INITTASKs and CLOSETASKs that default
    for ( int i = 0; i < tasks.size(); i++ ) {
      TestTask task = (TestTask) tasks.elementAt(i);
      Vector names = task.getThreadGroupNames();
      if ( task.getThreadGroupNames() == null
           && ( task.getTaskType() == TestTask.INITTASK
                || task.getTaskType() == TestTask.CLOSETASK ) ) {
        if ( getThreadGroups().size() == 0 ) getDefaultThreadGroup();
        for ( Iterator it = getThreadGroups().values().iterator(); it.hasNext(); ) {
          HydraThreadGroup tg = (HydraThreadGroup) it.next();
          task.addThreadGroupName( tg.getName() );
          task.addThreadGroup( tg );
        }
      }
    }
  }
  private HydraThreadGroup getDefaultThreadGroup() {
    String name = HydraThreadGroup.DEFAULT_NAME;
    HydraThreadGroup tg = getThreadGroup( name );
    if ( tg == null ) { // create default group if not there yet
      tg = new HydraThreadGroup( name );
      HydraThreadSubgroup tsg = new HydraThreadSubgroup( name );
      tsg.setTotalVMs( 0 );
      tsg.setTotalThreads( 0 );
      tsg.setClientNames( null );
      tg.addSubgroup( tsg );
      addThreadGroup( tg );
    }
    return tg;
  }
  private Map postprocessThreadGroups( Vector tasks ) {

    if ( ThreadGroups.size() == 0 ) return null;

    // first throw out thread groups not used by any test task

    Enumeration names = ThreadGroups.keys();
    while ( names.hasMoreElements() ) {
      String name = (String) names.nextElement();
      boolean used = false;
      for ( int i = 0; i < tasks.size(); i++ ) {
        TestTask task = (TestTask) tasks.elementAt(i);
        if ( task.getThreadGroupNames().contains( name ) ) {
          used = true;
          break;
        }
      }
      if ( ! used ) {
        ThreadGroups.remove( name );
      }
    }

    if ( ThreadGroups.size() == 0 ) return null;

    // now make sure any client names used by thread groups exist
    for ( Iterator i = ThreadGroups.values().iterator(); i.hasNext(); ) {
      HydraThreadGroup tg = (HydraThreadGroup) i.next();
      for ( Iterator j = tg.getSubgroups().iterator(); j.hasNext(); ) {
        HydraThreadSubgroup tsg = (HydraThreadSubgroup) j.next();
        Vector clientNames = tsg.getClientNames();
        if ( clientNames != null ) {
          for ( Iterator k = clientNames.iterator(); k.hasNext(); ) {
            String clientName = (String) k.next();
            if ( getClientDescription( clientName ) == null )
              throw new HydraConfigException( "Thread group specifies non-existent client name (" + clientName + "): " + tg );
          }
        }
      }
    }

    // now set the number of threads in the default group, if it's used
    HydraThreadGroup defaultGroup =
      (HydraThreadGroup) ThreadGroups.get( HydraThreadGroup.DEFAULT_NAME );
    if ( defaultGroup != null ) {
      int defaultThreads = getTotalThreads(); // start with all of them
      for ( Iterator i = ThreadGroups.values().iterator(); i.hasNext(); ) {
        HydraThreadGroup tg = (HydraThreadGroup) i.next();
        if ( tg.getName() != HydraThreadGroup.DEFAULT_NAME )
          defaultThreads -= tg.getTotalThreads();
      }
      HydraThreadSubgroup subgroup = defaultGroup.getSubgroup(0);
      subgroup.setTotalThreads( defaultThreads );
    }

    // now make sure there are no excess threads
    int usedThreads = 0;
    for (Iterator i = ThreadGroups.values().iterator(); i.hasNext();) {
      HydraThreadGroup tg = (HydraThreadGroup) i.next();
      usedThreads += tg.getTotalThreads();
    }
    int totalThreads = getTotalThreads();
    if (usedThreads < totalThreads) {
      String s = totalThreads + " total threads are configured using hydra.ClientPrms, but only " + usedThreads + " are required by the THREADGROUPs used by INITTASKs, TASKs, and CLOSETASKs: " + ThreadGroups.keySet() + ".  Excess threads are not allowed when the \"" + HydraThreadGroup.DEFAULT_NAME + "\" threadgroup is not used.";
      throw new HydraConfigException(s);
    } else if (usedThreads > totalThreads) {
      String s = totalThreads + " total threads are configured using hydra.ClientPrms, but " + usedThreads + " are required by the THREADGROUPs used by INITTASKs, TASKs, and CLOSETASKs: " + ThreadGroups.keySet() + ".";
      throw new HydraConfigException(s);
    }

    // now do a dummy assignment of clients to groups
    Map vms = new HashMap();
    int vmid = 1;
    int tid = 1;
    for (ClientDescription cd : ClientDescriptions.values()) {
      if ( log().finestEnabled() ) {
        log().finest( "Doing dummy client mapping with name=" + cd.getName()
             + " vms=" + cd.getVmQuantity() + " threads=" + cd.getVmThreads() );
      }
      for ( int j = 0; j < cd.getVmQuantity(); j++ ) {
        ClientVmRecord vm = new ClientVmRecord( vmid, tid, cd );
        for ( int threads = 0; threads < cd.getVmThreads(); threads++ ) {
          ClientRecord client = new ClientRecord( tid, vm );
          vm.addClient( client );
          ++tid;
        }
        vms.put( new Integer( vmid ), vm );
        ++vmid;
      }
    }
    ClientMapper mapper = new ClientMapper( ThreadGroups );
    mapper.mapClients( vms );
    if ( log().fineEnabled() ) {
      log().fine( mapper.getMappingAsString( vms ) );
    }
    return vms;
   }

  /**
   * Determines mcastPort and ensures all GemFireDescriptions have the same mcast port (within a distributed system).
   * Ensures all GemFireDescriptions have the same enableMcast setting (they may differ if ONEOF is used on conf file).
   *
   * @since 5.0
   * @author Jean Farris
   */
  private  void postprocessGemFireDescriptions() {
    Map gfds = getGemFireDescriptions();
    // dist. system name, ports
    SortedMap dsMcastPorts = new TreeMap();
    
    // determine mcast port for each distributed system in test
    if ( gfds.size() != 0 ) {
      for ( Iterator i = gfds.values().iterator(); i.hasNext(); ) {
        GemFireDescription gfd = (GemFireDescription) i.next();
	if ( gfd.getEnableMcast().booleanValue() ) {
          String dsName = gfd.getDistributedSystem();
	  if (! dsMcastPorts.containsKey( dsName )) {
            InetAddress addr = null;
            try {
              addr = InetAddress.getByName(gfd.getMcastAddress());
            } catch (UnknownHostException e) {
              String s = "Unable to get address for: " + gfd.getMcastAddress();
              throw new HydraRuntimeException(s, e);
            }
	    int port = gfd.getMcastPort().intValue();
	    if (port != DistributionConfig.MIN_MCAST_PORT) {
	      // port is specified in test config, log warning if port is already in use
	      if (!AvailablePort.isPortAvailable(port, AvailablePort.JGROUPS,
                                 addr)) {
		log().error("The port specified: " + port + " is already in use!  Test results will be invalid");
              }
            } else {
	      // port not specified in test config
	      port = AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS,
                                   addr);
            }
            log().info("assigning mcast port: " + port + " to distributed system: " + dsName);
	    dsMcastPorts.put( dsName, new Integer(port) );
            if (!gfd.getUseLocator().booleanValue()) { // using mcast discovery
              String data = "multicast:" + dsName + ":" + gfd.getMcastAddress()
                                                  + ":" + port;
              FileUtil.appendToFile(DistributedSystemHelper.DISCOVERY_FILE_NAME, data + "\n");
            }
          }
        }
      }
      // Update GemFireDescriptions mcast port and enableMcast setting
      // Each distributed system in dsMcastPorts had at least one gemfire description with
      // enableMcast set to true, so make sure all gemfire descriptions (in that dist. sys) are set correctly.
      for ( Iterator i = gfds.values().iterator(); i.hasNext(); ) {
        GemFireDescription gfd = (GemFireDescription) i.next();
        String dsName = gfd.getDistributedSystem();
        if (dsMcastPorts.containsKey(dsName)) {
          if (gfd.getEnableMcast().booleanValue()) {
            // use the port already selected for this distributed system
            gfd.setMcastPort((Integer)dsMcastPorts.get(dsName));
          } else {
            // all descriptions for this distributed system must be multicast
            String s = BasePrms.nameForKey(GemFirePrms.names) + "=" 
                     + gfd.getName()
                     + " is part of a multicast distributed system but it sets "
                     + BasePrms.nameForKey(GemFirePrms.enableMcast) + "="
                     + gfd.getEnableMcast() + ".";
            throw new HydraConfigException(s);
          }
        }
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  ////  UTILITY METHODS                                                     ////
  //////////////////////////////////////////////////////////////////////////////

  private LogWriter log() {
    return Log.getLogWriter();
  }
}
