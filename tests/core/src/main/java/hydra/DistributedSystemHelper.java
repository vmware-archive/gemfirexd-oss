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

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.AdminDistributedSystemFactory;
import com.gemstone.gemfire.admin.DistributedSystemConfig;
import com.gemstone.gemfire.admin.internal.AdminDistributedSystemImpl;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.DistributionManager;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.rmi.RemoteException;
import java.util.*;


/**
 * Helps hydra clients manage locators for and connections to a distributed
 * system.  Methods are thread-safe.
 */
public class DistributedSystemHelper {

  /** Name of the file containing locator or multicast endpoints */
  public static final String DISCOVERY_FILE_NAME = "discovery.dat";

  /** Properties used to connect the current distributed system */
  private static Properties TheDistributedSystemProperties;

  /** Name of the admin description used to create the admin ds */
  private static String TheAdminConfig;

  /** Perpetual endpoint for the locator. */
  private static Endpoint TheLocatorEndpoint;

  /** Locator configuration property. */
  private static String TheLocatorConfig;

//------------------------------------------------------------------------------
// DistributedSystem
//------------------------------------------------------------------------------

  /**
   * Connects to a distributed system using properties from the {@link
   * GemFireDescription} corresponding to the logical gemfire configuration
   * name from the system property {@link GemFirePrms#GEMFIRE_NAME_PROPERTY}.
   * Returns the existing distributed system if it is currently connected.
   *
   * @throws HydraRuntimeException if an attempt is made to reconfigure an
   *         existing distributed system.
   */
  public static DistributedSystem connect() {
    log("Connecting to distributed system");
    return _connect(null);
  }

  /**
   * Connects to a distributed system using properties from the {@link
   * GemFireDescription} corresponding to the logical gemfire configuration
   * name from the system property {@link GemFirePrms#GEMFIRE_NAME_PROPERTY}.
   * Returns the existing distributed system if it is currently connected.
   * <p>
   * Subsequent cache creation will use the configuration in the XML file.
   *
   * @throws HydraRuntimeException if an attempt is made to reconfigure an
   *         existing distributed system.
   */
  public static DistributedSystem connectWithXml(String cacheXmlFile) {
    if (cacheXmlFile == null) {
      throw new IllegalArgumentException("cacheXmlFile cannot be null");
    }
    log("Connecting to distributed system using XML: " + cacheXmlFile);
    return _connect(cacheXmlFile);
  }

  /**
   * Connects the distributed system using the optional cache XML file.
   */
  private static synchronized DistributedSystem _connect(String cacheXmlFile) {
    String fn = null;
    if (cacheXmlFile != null) {
      fn = FileUtil.absoluteFilenameFor(EnvHelper.expandEnvVars(cacheXmlFile));
    }

    DistributedSystem distributedSystem = getDistributedSystem();
    if (distributedSystem == null) {

      // connect to the distributed system
      Properties p = getDistributedSystemProperties(fn);
      log("Connecting to distributed system: " + prettyprint(p));
      distributedSystem = DistributedSystem.connect(p);
      log("Connected to distributed system");

      // save the distributed system config for future reference
      TheDistributedSystemProperties = p;

    } else {
      if (TheDistributedSystemProperties == null) {
        // block attempt to create distributed system in multiple ways
        String s = "Distributed system was already connected without"
                 + " DistributedSystemHelper using an unknown, and possibly"
                 + " different, configuration";
        throw new HydraRuntimeException(s);

      } else {
        Properties p = getDistributedSystemProperties(fn);
        if (!TheDistributedSystemProperties.equals(p)) {
          // block attempt to reconnect system with clashing properties
          String s = "Distributed system already exists using properties "
                   + TheDistributedSystemProperties + ", cannot also use " + p;
          throw new HydraRuntimeException(s);

        } // else it was already created with these properties, which is fine
      }
      log("Already connected to distributed system");
    }
    return distributedSystem;
  }

  /**
   * Returns the distributed system if it is connected, otherwise null.
   */
  public static synchronized DistributedSystem getDistributedSystem() {
    return InternalDistributedSystem.getAnyInstance();
  }

  /**
   * Returns the distributed system name.
   */
  public static synchronized String getDistributedSystemName() {
    return getGemFireDescription().getDistributedSystem();
  }

  /**
   * Returns the distributed system id.
   */
  public static synchronized Integer getDistributedSystemId() {
    return getGemFireDescription().getDistributedSystemId();
  }

  /**
   * Disconnects the distributed system if it is connected.
   */
  public static synchronized void disconnect() {
    DistributedSystem distributedSystem = getDistributedSystem();
    if (distributedSystem != null) {
      log("Disconnecting from the distributed system: "
         + distributedSystem.getName());
      CacheHelper.TheCacheConfig = null; // since cache will also close
      distributedSystem.disconnect();
      log("Disconnected from the distributed system");
      TheLocatorConfig = null;
      TheDistributedSystemProperties = null; // so the next connect can have a
                                             // different config
    }
  }

  /**
   * Cleans up disconnect-related variables in this helper class so that the JVM
   * can use it to reconnect.
   * <p>
   * NOTE: For use only after the product has automatically disconnected the JVM
   * from the distributed system. The test is responsible for verifying this.
   */
  public static synchronized void cleanupAfterAutoDisconnect() {
    log("Cleaning up after distributed system auto-disconnect");
    CacheHelper.TheCacheConfig = null;
    TheLocatorConfig = null;
    TheDistributedSystemProperties = null;
    log("Cleaned up after distributed system auto-disconnect");
  }

//------------------------------------------------------------------------------
// AdminDistributedSystem
//------------------------------------------------------------------------------

  /**
   * Configures, creates and connects an admin distributed system, using the
   * {@link AdminDescription} corresponding to the given admin configuration
   * from {@link AdminPrms#names}.
   *
   * @throws HydraRuntimeException if an attempt is made to reconfigure an
   *         existing distributed system.
   */
  public static synchronized AdminDistributedSystem connectAdmin(
                                                    String adminConfig) {
    DistributedSystemConfig dsConfig = configureAdmin(adminConfig);
    AdminDistributedSystem ads = createAdmin(dsConfig);
    connectAdmin(ads);
    return ads;
  }

  /**
   * Configures and creates an unconnected admin distributed system, using the
   * {@link AdminDescription} corresponding to the given admin configuration
   * from {@link AdminPrms#names}.
   * <p>
   * To connect the system, use {@link #connectAdmin(AdminDistributedSystem)}.
   *
   * @throws HydraRuntimeException if an attempt is made to reconfigure an
   *         existing distributed system.
   */
  public static synchronized AdminDistributedSystem createAdmin(
                                                    String adminConfig) {
    DistributedSystemConfig dsConfig = configureAdmin(adminConfig);
    return createAdmin(dsConfig);
  }

  /**
   * Returns the configuration for an admin distributed system, using the
   * {@link AdminDescription} corresponding to the given admin configuration
   * from {@link AdminPrms#names}.  This can be mutated as needed before
   * creating and connecting the system.
   * <p>
   * To create the system, use {@link #createAdmin(DistributedSystemConfig)}.
   * To connect the system, use {@link #connectAdmin(AdminDistributedSystem)}.
   *
   * @throws HydraRuntimeException if an attempt is made to reconfigure an
   *         existing admin distributed system.
   */
  public static synchronized DistributedSystemConfig configureAdmin(
                                                     String adminConfig) {
    DistributedSystemConfig dsConfig = null;
    if (TheAdminConfig == null) {
      log("Configuring admin distributed system from config: " + adminConfig);

      // look up the admin configuration
      AdminDescription ad = getAdminDescription(adminConfig);

      // configure the config
      AdminDistributedSystemFactory.setEnableAdministrationOnly(true);
      dsConfig = AdminDistributedSystemFactory.defineDistributedSystem();
      ad.configure(dsConfig);
      log("Configured admin distributed system: "
         + AdminDescription.adminToString(dsConfig));

      // save the admin config for future reference
      TheAdminConfig = adminConfig;
    }
    else if (!TheAdminConfig.equals(adminConfig)) {
      // block attempt to recreate admin ds with clashing configuration name
      String s = "Admin distributed system already configured using logical"
               + " admin configuration named " + TheAdminConfig
               + ", cannot also use " + adminConfig;
      throw new HydraRuntimeException(s);
    }
    // else it was already created with this configuration, which is fine
    return dsConfig;
  }

  /**
   * Creates an unconnected admin distributed system from the given
   * configuration.
   * <p>
   * To connect the system, use {@link #connectAdmin(AdminDistributedSystem)}.
   *
   * @throws HydraRuntimeException if an attempt is made to reconfigure an
   *         existing admin distributed system.
   */
  public static synchronized AdminDistributedSystem createAdmin(
                                             DistributedSystemConfig dsConfig) {
    AdminDistributedSystem ads = getAdminDistributedSystem();
    if (ads == null) {
      log("Creating admin distributed system from config: "
         + AdminDescription.adminToString(dsConfig));

      // create the system
      ads = AdminDistributedSystemFactory.getDistributedSystem(dsConfig);
      log("Created admin distributed system: " + adminToString(ads));
    }
    else if (TheAdminConfig == null && ads != null) {
      // block attempt to create admin ds in multiple ways
      String s = "Admin distributed system was already created without"
               + " DistributedSystemHelper using an unknown, and possibly"
               + " different, configuration";
      throw new HydraRuntimeException(s);
    }
    return ads;
  }

  /**
   * Connects the admin distributed system.  Returns the existing system if it
   * is currently connected.
   */
  public static synchronized void connectAdmin(AdminDistributedSystem ads) {
    AdminDistributedSystem connectedSystem = getAdminDistributedSystem();
    if (connectedSystem == null) {
      log("Connecting admin distributed system: "  + "|"+ ads + "|");
      ads.connect();
      log("Connected admin distributed system: " + ads + " "
                                                 + adminToString(ads));
    }
  }

  /**
   * Returns the admin distributed system if it is connected, otherwise null.
   */
  public static synchronized AdminDistributedSystem getAdminDistributedSystem() {
    // @todo don't rely on this, just keep the instance handy and do more
    //       thorough error-checking using adminToString in methods above
    return AdminDistributedSystemImpl.getConnectedInstance();
  }

  /**
   * Disconnects the admin distributed system if it is connected.
   */
  public static synchronized void disconnectAdmin() {
    AdminDistributedSystem ads = getAdminDistributedSystem();
    if (ads != null) {
      log("Disconnecting admin distributed system: " + ads + " "
                                                     + adminToString(ads));
      ads.disconnect();
      log("Disconnected admin distributed system: " + ads);
      // make sure next connect can have different, possibly non-admin, config
      AdminDistributedSystemFactory.setEnableAdministrationOnly(false);
      TheAdminConfig = null;
    }
  }

  /**
   * Returns the given admin distributed system as a string.
   */
  public static String adminToString(AdminDistributedSystem ads) {
    if (ads == null) {
      String s = "admin distributed system cannot be null";
      throw new IllegalArgumentException(s);
    }
    return AdminDescription.adminToString(ads.getConfig());
  }

//------------------------------------------------------------------------------
// Names and Ids
//------------------------------------------------------------------------------

  /**
   * Returns the distributed system id for the given distributed system name.
   */
  public static int getDistributedSystemId(String distributedSystemName) {
    for (GemFireDescription gfd :
                TestConfig.getInstance().getGemFireDescriptions().values()) {
      if (gfd.getDistributedSystem().equals(distributedSystemName)) {
        return gfd.getDistributedSystemId();
      }
    }
    String s = "Distributed system not found: " + distributedSystemName;
    throw new HydraRuntimeException(s);
  }

  /**
   * Returns the distributed system name for the given distributed system id.
   */
  public static String getDistributedSystemName(int distributedSystemId) {
    for (GemFireDescription gfd :
                TestConfig.getInstance().getGemFireDescriptions().values()) {
      if (gfd.getDistributedSystemId() == distributedSystemId) {
        return gfd.getDistributedSystem();
      }
    }
    String s = "Distributed system not found: " + distributedSystemId;
    throw new HydraRuntimeException(s);
  }

//------------------------------------------------------------------------------
// Members
//------------------------------------------------------------------------------

  /**
   * Returns a collection of the current distribution members. This excludes
   * admin members and locators.
   *
   * @see com.gemstone.gemfire.distributed.DistributedMember
   * @throws HydraRuntimeException if not currently connected.
   */
  public static Set getMembers() {
    return GemFireVersionHelper.getMembers(_getDM());
  }

  /**
   * Returns a collection of the current admin distribution members.
   *
   * @see com.gemstone.gemfire.distributed.DistributedMember
   * @throws HydraRuntimeException if not currently connected.
   */
  public static Set getAdminMembers() {
    DM dm = _getDM();
    if (dm instanceof DistributionManager) {
      return new HashSet(((DistributionManager)dm).getAdminMemberSet());
    }
    else {
      return new HashSet();
    }
  }

  /**
   * Returns the distribution manager for the current distributed system.
   * @throws HydraRuntimeException if not currently connected.
   */
  private static DM _getDM() {
    DistributedSystem distributedSystem = getDistributedSystem();
    if (distributedSystem == null) {
      String s = "This VM is not connected to a distributed system";
      throw new HydraRuntimeException(s);
    } else {
      return ((InternalDistributedSystem)distributedSystem)
                                        .getDistributionManager();
    }
  }

//------------------------------------------------------------------------------
// Properties
//------------------------------------------------------------------------------

  /**
   * Returns distributed system properties using the optional cache XML file.
   */
  public static Properties getDistributedSystemProperties(String cacheXmlFile) {
    GemFireDescription gfd = getGemFireDescription();
    return getDistributedSystemProperties(gfd, cacheXmlFile);
  }

  /**
   * Returns distributed system properties using the description and
   * optional cache XML file.
   */
  private static synchronized Properties getDistributedSystemProperties(
                            GemFireDescription gfd, String cacheXmlFile) {
    log("Looking up distributed system properties");
    Properties p = gfd.getDistributedSystemProperties();
    if (cacheXmlFile != null) {
      p.setProperty(DistributionConfig.CACHE_XML_FILE_NAME, cacheXmlFile);
    }
    if (TheLocatorConfig != null) {
      p.setProperty(DistributionConfig.START_LOCATOR_NAME, TheLocatorConfig);
      // always set the distributed system id on locators
      GemFireVersionHelper.setDistributedSystemId(p, gfd.getDistributedSystemId());
    }
    log("Looked up distributed system properties: " + prettyprint(p));
    return p;
  }

  /**
   * Returns a string containing indented properties, one per line.
   */
  private static String prettyprint(Properties p) {
    List l = Collections.list(p.propertyNames());
    SortedSet set = new TreeSet(l);
    StringBuffer buf = new StringBuffer();
    for (Iterator i = set.iterator(); i.hasNext();) {
      String key = (String)i.next();
      String val = p.getProperty(key);
      buf.append("\n  " + key + "=" + val);
    }
    return buf.toString();
  }

//------------------------------------------------------------------------------
// AdminDescription
//------------------------------------------------------------------------------

  /**
   * Returns the {@link AdminDescription} with the given configuration name
   * from {@link AdminPrms#names}.
   */
  public static AdminDescription getAdminDescription(String adminConfig) {
    if (adminConfig == null) {
      throw new IllegalArgumentException("adminConfig cannot be null");
    }
    log("Looking up admin config: " + adminConfig);
    AdminDescription ad = TestConfig.getInstance()
                                    .getAdminDescription(adminConfig);
    if (ad == null) {
      String s = adminConfig + " not found in "
               + BasePrms.nameForKey(AdminPrms.names);
      throw new HydraRuntimeException(s);
    }
    log("Looked up admin config:\n" + ad);
    return ad;
  }

//------------------------------------------------------------------------------
// GemFireDescription
//------------------------------------------------------------------------------

  /**
   * Returns the {@link GemFireDescription} for this VM, as specified by the
   * system property {@link GemFirePrms#GEMFIRE_NAME_PROPERTY}.  Also verifies
   * that the configured host is local.
   */
  public static GemFireDescription getGemFireDescription() {
    log("Looking for system property: " + GemFirePrms.GEMFIRE_NAME_PROPERTY);
    String gemfireConfig =
      System.getProperty(GemFirePrms.GEMFIRE_NAME_PROPERTY);
    if (gemfireConfig == null) {
      String s = "This VM is not configured for a distributed system: "
               + "missing property " + GemFirePrms.GEMFIRE_NAME_PROPERTY;
      throw new HydraRuntimeException(s);
    }
    log("Looking up gemfire config: " + gemfireConfig);
    GemFireDescription gfd = TestConfig.getInstance()
                                       .getGemFireDescription(gemfireConfig);
    // check that the target host is local
    String targetHost = gfd.getHostDescription().getHostName();
    if (!HostHelper.isLocalHost(targetHost)) {
      String s = gemfireConfig + " is on remote host " + targetHost;
      throw new HydraConfigException(s);
    }
    log("Looked up gemfire config:\n" + gfd);
    return gfd;
  }

//------------------------------------------------------------------------------
// Locator
//------------------------------------------------------------------------------

  /**
   * Creates a locator endpoint that this VM and other locator VMs in its
   * distributed system can use when configuring their locator lists.  The
   * endpoint need only be created once.
   * <p>
   * Upon initial creation, selects a random port and registers the locator
   * {@link Endpoint} in the {@link DistributedSystemBlackboard} map.  The
   * locator will always use this port during startup.
   * <p>
   * All locators associated with a given distributed system must be created
   * before locators are started, so that the locator list is complete.
   * At least one locator must be started before non-locators can connect.
   * <p>
   * To start the locator, use {@link #startLocatorAndDS} or {@link
   * #startLocatorAndAdminDS}.
   *
   * @throws HydraRuntimeException if the VM already has a locator that did
   *         not create its endpoint using this method or if master is managing
   *         locators.
   */
  public static synchronized void createLocator() {
    if (TestConfig.tab().booleanAt(Prms.manageLocatorAgents)) {
      String s = "Locators are being managed by the hydra master controller.  "
               + "Set " + BasePrms.nameForKey(Prms.manageLocatorAgents)
               + "=false to use hydra-client-managed locators.";
      throw new HydraConfigException(s);
    }
    Locator locator = getLocator();
    Endpoint endpoint = findEndpoint();
    if (locator == null) {
      if (endpoint == null) {
        generateEndpoint();
      } // else already created but not running
    } else if (endpoint == null) {
      String s = "Locator was already started without DistributedSystemHelper"
               + " using an unknown port";
      throw new HydraRuntimeException(s);
    } // else already created and running
  }

  /**
   * Starts the locator whose endpoint was created by {@link #createLocator}
   * and connects it to a non-admin distributed system (it can create
   * a cache).  The locator is configured using the {@link GemFireDescription}
   * corresponding to the logical gemfire configuration name from the system
   * property {@link GemFirePrms#GEMFIRE_NAME_PROPERTY}.  Returns the existing
   * locator if it is already started.
   * <p>
   * At least one locator must be started before non-locators can connect.
   * <p>
   * When starting multiple locators, use synchronization to ensure that one
   * locator starts up before the others, as a workaround for Bug 30341.
   *
   * @throws HydraRuntimeException if the VM is already connected or an attempt
   *         is made to reconfigure an existing locator.
   */
  public static synchronized Locator startLocatorAndDS() {
    return _startLocatorAndDS(false);
  }

  /**
   * Starts the locator whose endpoint was created by {@link #createLocator}
   * and connects it to an admin distributed system (it cannot create
   * a cache).  The locator is configured using the {@link GemFireDescription}
   * corresponding to the logical gemfire configuration name from the system
   * property {@link GemFirePrms#GEMFIRE_NAME_PROPERTY}.  Returns the existing
   * locator if it is already started.
   * <p>
   * At least one locator must be started before non-locators can connect.
   * <p>
   * When starting multiple locators, use synchronization to ensure that one
   * locator starts up before the others, as a workaround for Bug 30341.
   *
   * @throws HydraRuntimeException if the VM is already connected or an attempt
   *         is made to reconfigure an existing locator.
   */
  public static synchronized Locator startLocatorAndAdminDS() {
    return _startLocatorAndDS(true);
  }

  /**
   * Starts the locator and connects it to the specified type of distributed
   * system.
   */
  private static synchronized Locator _startLocatorAndDS(boolean adminOnly) {
    Endpoint endpoint = findEndpoint();
    if (endpoint == null) {
      String s = "Locator has not been created yet";
      throw new HydraRuntimeException(s);
    }
    Locator locator = getLocator();
    GemFireDescription gfd = getGemFireDescription();
    Properties p = getDistributedSystemProperties(gfd, null);
    // always set the distributed system id on locators
    GemFireVersionHelper.setDistributedSystemId(
                         p, gfd.getDistributedSystemId());
    int port = endpoint.getPort();
    String locatorConfig = endpoint.getLocatorProperty();
    if (locator == null) {
      DistributedSystem ds = getDistributedSystem();
      if (ds == null) {
        File log = new File(p.getProperty(DistributionConfig.LOG_FILE_NAME));
        log("Starting locator and admin distributed system");
        try {
          // for GemFireXD tests, first try to load via FabricService API
          boolean gfxdLocator = false;
          try {
            Class<?> fabAPI = Class
                .forName("com.pivotal.gemfirexd.FabricServiceManager");
            Object gfxdLoc = fabAPI.getMethod("getFabricLocatorInstance")
                .invoke(null);
            final String locDir = log.getParent();
            if (locDir != null && locDir.length() > 0) {
              p.setProperty("sys-disk-dir", locDir);
            }
            // reduce log-level for GFXD locator
            if ("fine".equals(p.getProperty("log-level"))) {
              p.remove("log-level");
            }
            Class.forName("com.pivotal.gemfirexd.FabricLocator")
                .getMethod("start", String.class, int.class, Properties.class)
                .invoke(gfxdLoc, endpoint.getAddress(), port, p);
            gfxdLocator = true;
          } catch (InvocationTargetException ite) {
            log("Failed to start GemFireXD locator due to " + ite, ite);
          } catch (Exception ignored) {
          }
          if (gfxdLocator) {
            log("Started GemFireXD locator \"" + endpoint.getId()
                + "\" with properties: " + prettyprint(p));
          }
          else if (adminOnly) {
            log("Starting locator \"" + locatorConfig + "\" and connecting "
              + "to admin-only distributed system: " + prettyprint(p));
            locator = Locator.startLocatorAndDS(port, log, null, p,
                                                endpoint.isPeerLocator(),
                                                endpoint.isServerLocator(),
                                                (String)null);
          } else {
            p.setProperty(DistributionConfig.START_LOCATOR_NAME, locatorConfig);
            log("Starting locator \"" + locatorConfig + "\" and connecting "
              + "to non-admin distributed system: " + prettyprint(p));
            DistributedSystem.connect(p);
            locator = getLocator();
          }
        } catch (IOException e) {
          try {
            ProcessMgr.logNetworkStatistics();
          } finally {
            String s = "Problem starting locator " + endpoint
                     + " using port " + port + " with properties " + p;
            throw new HydraRuntimeException(s, e);
          }
        }
        log("Started locator: " + locator);

        // save the distributed system properties for future reference
        TheDistributedSystemProperties = p;
        if (!adminOnly) {
          TheLocatorConfig = locatorConfig;
        }

      } else {
        String s = "This VM is already connected to a distributed system. "
                 + "Too late to start a locator";
        throw new HydraRuntimeException(s);
      }
    } else if (TheDistributedSystemProperties == null) {
      // block attempt to start locator in multiple ways
      String s = "Locator was already started without DistributedSystemHelper"
               + " using an unknown, and possibly different, configuration";
      throw new HydraRuntimeException(s);

    } else if (!TheDistributedSystemProperties.equals(p)) {
      // block attempt to connect to system with clashing configuration
      String s = "Already connected to distributed system using properties "
               + TheDistributedSystemProperties + ", cannot also use " + p;
      throw new HydraRuntimeException(s);

    } // else it was already started with this configuration, which is fine

    return locator;
  }

  /**
   * Returns the currently running locator, or null if no locator exists.
   * @throws HydraRuntimeException if there are multiple locators.
   */
  public static synchronized Locator getLocator() {
    Collection locators = Locator.getLocators();
    if (locators == null || locators.size() == 0) {
      return null;
    } else if (locators.size() == 1) {
      return (Locator)locators.iterator().next();
    } else {
      String s = "Too many locators in this VM: " + locators;
      throw new UnsupportedOperationException(s);
    }
  }

  /**
   * Stops the currently running locator, if it exists.
   */
  public static synchronized void stopLocator() {
    Locator locator = getLocator();
    if (locator != null) {
      log("Stopping locator: " + locatorToString(locator));
      locator.stop();
      TheDistributedSystemProperties = null;
      TheLocatorConfig = null;
      log("Stopped locator: " + locatorToString(locator));
    }
  }

  /**
   * Returns the given locator as a string.
   */
  public static String locatorToString(Locator locator) {
    return locator.toString();
  }

//------------------------------------------------------------------------------
// Locator Endpoints
//------------------------------------------------------------------------------

  /**
   * Returns all peer locator endpoints from the {@link
   * DistributedSystemBlackboard} map, a possibly empty list.  This includes
   * all peer locators that have ever started, regardless of their distributed
   * system or current active status.
   */
  public static synchronized List getEndpoints() {
    List peerLocatorEndpoints = new ArrayList();
    Collection endpoints = DistributedSystemBlackboard.getInstance()
                                      .getSharedMap().getMap().values();
    for (Iterator i = endpoints.iterator(); i.hasNext();) {
      Endpoint endpoint = (Endpoint)i.next();
      if (endpoint.isPeerLocator()) {
        peerLocatorEndpoints.add(endpoint);
      }
    }
    return peerLocatorEndpoints;
  }

  /**
   * Returns all peer locator endpoints for this VM's distributed system from
   * the {@link DistributedSystemBlackboard} map, a possibly empty list.  This
   * includes all peer locators that have ever started for this system,
   * regardless of their current active status.
   */
  public static synchronized List getSystemEndpoints() {
    return getEndpoints(getDistributedSystemName());
  }

  /**
   * Returns all peer locator endpoints for the specified distributed system
   * from the {@link DistributedSystemBlackboard} map, a possibly empty list.
   * This includes all peer locators that have ever started for the system,
   * regardless of their current active status.
   */
  public static synchronized List getEndpoints(String distributedSystemName) {
    List peerLocatorEndpoints = new ArrayList();
    for (Iterator i = getEndpoints().iterator(); i.hasNext();) {
      Endpoint endpoint = (Endpoint)i.next();
      if (endpoint.getDistributedSystemName().equals(distributedSystemName)) {
        peerLocatorEndpoints.add(endpoint);
      }
    }
    return peerLocatorEndpoints;
  }

  /**
   * Returns all contacts (server locator endpoints) from the {@link
   * DistributedSystemBlackboard} map, a possibly empty list.  This includes
   * all server locators that have ever started, regardless of their distributed
   * system or current active status.
   */
  public static synchronized List getContacts() {
    List serverLocatorEndpoints = new ArrayList();
    Collection endpoints = DistributedSystemBlackboard.getInstance()
                                      .getSharedMap().getMap().values();
    for (Iterator i = endpoints.iterator(); i.hasNext();) {
      Endpoint endpoint = (Endpoint)i.next();
      if (endpoint.isServerLocator()) {
        serverLocatorEndpoints.add(endpoint);
      }
    }
    return serverLocatorEndpoints;
  }

  /**
   * Returns all contacts (server locator endpoints) for this VM's distributed
   * system from the {@link DistributedSystemBlackboard} map, a possibly empty
   * list.  This includes all server locators that have ever started for this
   * system, regardless of their current active status.
   */
  public static synchronized List getSystemContacts() {
    return getContacts(getDistributedSystemName());
  }

  /**
   * Returns all contacts (server locator endpoints) for the specified
   * distributed system from the {@link DistributedSystemBlackboard} map, a
   * possibly empty list.  This includes all server locators that have ever
   * started for the system, regardless of their current active status.
   */
  public static synchronized List getContacts(String distributedSystemName) {
    List serverLocatorEndpoints = new ArrayList();
    for (Iterator i = getContacts().iterator(); i.hasNext();) {
      Endpoint endpoint = (Endpoint)i.next();
      if (endpoint.getDistributedSystemName().equals(distributedSystemName)) {
        serverLocatorEndpoints.add(endpoint);
      }
    }
    return serverLocatorEndpoints;
  }

  /**
   * Returns all contacts (server locator endpoints) for the specified
   * distributed systems from the {@link DistributedSystemBlackboard} map, a
   * possibly empty list.  This includes all server locators that have ever
   * started for the system, regardless of their current active status.
   */
  public static synchronized List<String>
                             getContacts(List<String> distributedSystemNames) {
    List<String> serverLocatorEndpoints = new ArrayList();
    for (String distributedSystemName : distributedSystemNames) {
      serverLocatorEndpoints.addAll(getContacts(distributedSystemName));
    }
    return serverLocatorEndpoints;
  }

  /**
   * Converts a list of locator endpoints to a locator string, or the empty
   * string if the list is null.
   */
  public static String endpointsToString(List endpoints) {
    String locatorStr = "";
    if (endpoints != null) {
      for (Iterator i = endpoints.iterator(); i.hasNext();) {
        Endpoint endpoint = (Endpoint)i.next();
        if (locatorStr.length() > 0) {
          locatorStr += ",";
        }
        locatorStr += endpointToString(endpoint);
      }
    }
    return locatorStr;
  }

  /**
   * Converts a locator endpoint to a locator string, or the empty string if
   * the endpoint is null.
   */
  public static String endpointToString(Endpoint endpoint) {
    return endpoint == null ? "" : endpoint.getId();
  }

  /**
   * Finds the locator endpoint for this VM from the shared {@link
   * DistributedSystemBlackboard} map, if it exists.  Caches the result.
   */
  private static synchronized Endpoint findEndpoint() {
    if (TheLocatorEndpoint == null) {
      Integer vmid = new Integer(RemoteTestModule.getMyVmid());
      TheLocatorEndpoint = (Endpoint)DistributedSystemBlackboard.getInstance()
                                    .getSharedMap().get(vmid);
    }
    return TheLocatorEndpoint;
  }

  /**
   * Generates a locator endpoint with a random port for this VM and stores it
   * in the shared {@link DistributedSystemBlackboard} map, if needed.  Caches
   * the result.
   */
  private static synchronized Endpoint generateEndpoint() {
    Endpoint endpoint = findEndpoint();
    if (endpoint == null) {
      log("Generating locator endpoint");
      Integer vmid = new Integer(RemoteTestModule.getMyVmid());
      String name = RemoteTestModule.getMyClientName();
      String host = HostHelper.getCanonicalHostName();
      String addr = HostHelper.getHostAddress();
      int port = PortHelper.getRandomPort();
      String id = addr + "[" + port + "]";

      GemFireDescription gfd = getGemFireDescription();
      String ds = gfd.getDistributedSystem();
      endpoint = new Endpoint(id, name, vmid.intValue(), host, addr, port, ds,
                              gfd.isPeerLocator().booleanValue(),
                              gfd.isServerLocator().booleanValue());
      log("Generated locator endpoint: " + endpoint);
      DistributedSystemBlackboard.getInstance().getSharedMap()
                                 .put(vmid, endpoint);
      try {
        RemoteTestModule.Master.recordLocator(endpoint);
      } catch (RemoteException e) {
        String s = "While recording locator endpoint";
        throw new HydraRuntimeException(s, e);
      }
    }
    TheLocatorEndpoint = endpoint; // cache it for later use
    return endpoint;
  }

  /**
   * Represents the endpoint for a locator.
   */
  public static class Endpoint implements Contact, Serializable {
    String id, name, host, addr, ds;
    int vmid, port;
    boolean isPeerLocator, isServerLocator;

    /**
     * Creates an endpoint for a master-managed locator agent record.
     *
     * @param lar the locator agent record.
     */
    protected Endpoint(GemFireLocatorAgentRecord lar) {
      if (lar == null) {
        throw new HydraInternalException("lar is null");
      }
      this.id   = lar.getHostAddress() + "[" + lar.getPort() + "]";
      this.name = "locatoragent";
      this.vmid = lar.getProcessId();
      this.host = lar.getHostName();
      this.addr = lar.getHostAddress();
      this.port = lar.getPort();
      this.ds   = lar.getDistributedSystem();
      this.isPeerLocator = true;
      this.isServerLocator = lar.isServerLocator();
    }

    /**
     * Creates an endpoint for a locator.
     *
     * @param id   the locator id.
     * @param name the logical hydra client VM name from {@link
     *             ClientPrms#names} found via the {@link
     *             ClientPrms#CLIENT_NAME_PROPERTY} system property.
     * @param vmid the logical hydra client VM ID found via {@link
     *             RemoteTestModule#getMyVmid}.
     * @param host the locator host.
     * @param addr the locator address.
     * @param port the locator port.
     * @param ds   the locator distributed system name.
     * @param isPeerLocator   whether the locator is a peer locator.
     * @param isServerLocator whether the locator is a server locator.
     */
    public Endpoint(String id, String name, int vmid,
                    String host, String addr, int port, String ds,
                    boolean isPeerLocator, boolean isServerLocator) {
      if (id == null) {
        throw new IllegalArgumentException("id cannot be null");
      }
      if (name == null) {
        throw new IllegalArgumentException("name cannot be null");
      }
      if (host == null) {
        throw new IllegalArgumentException("host cannot be null");
      }
      if (addr == null) {
        throw new IllegalArgumentException("addr cannot be null");
      }
      if (ds == null) {
        throw new IllegalArgumentException("ds cannot be null");
      }

      this.id   = id;
      this.name = name;
      this.vmid = vmid;
      this.host = host;
      this.addr = addr;
      this.port = port;
      this.ds   = ds;
      this.isPeerLocator = isPeerLocator;
      this.isServerLocator = isServerLocator;
    }

    /**
     * Returns the unique locator logical endpoint ID.
     */
    public String getId() {
      return this.id;
    }

    /**
     * Returns the locator logical VM name.
     */
    public String getName() {
      return this.name;
    }

    /**
     * Returns the locator logical VM ID.
     */
    public int getVmid() {
      return this.vmid;
    }

    /**
     * Returns the locator host.
     */
    public String getHost() {
      return this.host;
    }

    /**
     * Returns the locator address.
     */
    public String getAddress() {
      return this.addr;
    }

    /**
     * Returns the locator port.
     */
    public int getPort() {
      return this.port;
    }

    /**
     * Returns the locator distributed system name.
     */
    public String getDistributedSystemName() {
      return this.ds;
    }

    /**
     * Returns whether this is a peer locator.
     */
    public boolean isPeerLocator() {
      return this.isPeerLocator;
    }

    /**
     * Returns whether this is a server locator.
     */
    public boolean isServerLocator() {
      return this.isServerLocator;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof Endpoint) {
        Endpoint endpoint = (Endpoint)obj;
        return endpoint.getId().equals(this.getId())
            && endpoint.getName().equals(this.getName())
            && endpoint.getVmid() == this.getVmid()
            && endpoint.getHost().equals(this.getHost())
            && endpoint.getAddress().equals(this.getAddress())
            && endpoint.getPort() == this.getPort()
            && endpoint.getDistributedSystemName()
                       .equals(this.getDistributedSystemName())
            && (endpoint.isPeerLocator() == this.isPeerLocator())
            && (endpoint.isServerLocator() == this.isServerLocator());
      }
      return false;
    }

    @Override
    public int hashCode() {
      return this.port;
    }

    /**
     * Returns the string suitable for use as the value of the distributed
     * system property <code>DistributionConfig.START_LOCATOR_NAME</code>.
     */
    public String getLocatorProperty() {
      return this.addr + "[" + this.port + "],"
                       + "peer=" + this.isPeerLocator + ","
                       + "server=" + this.isServerLocator;
    }

    /**
     * Records locator information in a properties file for use when manually
     * running GUI tests.
     */
    public void record() {
      String data = this.id + "," + this.ds + "," + this.host + ","
                  + this.addr + "," + this.port;
      FileUtil.appendToFile(DISCOVERY_FILE_NAME, data + "\n");
    }

    /**
     * Returns the endpoint as a string.
     */
    @Override
    public String toString() {
      if (this.name.equals("locatoragent")) {
        return this.id + "(locator_" + this.ds + "_" + this.vmid
                       + ")";
      } else {
        return this.id + "(" + this.ds + ":vm_" + this.vmid + "_" + this.name
                       + "_" + this.host + ")";
      }
    }
  }

//------------------------------------------------------------------------------
// Log
//------------------------------------------------------------------------------

  private static LogWriter log;
  private static synchronized void log(String s) {
    log(s, null);
  }
  private static synchronized void log(String s, Throwable t) {
    if (log == null) {
      log = Log.getLogWriter();
    }
    if (log.infoEnabled()) {
      log.info(s, t);
    }
  }
}
