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

import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import java.util.*;

/**
*
* A class used to manage the distributed connection for a VM.
*
*/

public class DistributedConnectionMgr
{
  /** the current connection for this vm */
  private static DistributedSystem vmConnection = null;

  /**
   *  Gets the current connection to the distributed system, if any.  Returns
   *  null if this VM is not connected.
   */
  public static DistributedSystem getConnection() {
    return vmConnection;
  }
  /**
   *  Gets a connection to the distributed system named in the
   *  {@link hydra.GemFirePrms#GEMFIRE_NAME_PROPERTY} system property.
   *  This comes from {@link ClientPrms#gemfireNames}.
   *  <p>
   *  This method is thread-safe for clients in each VM.  It is not thread-safe
   *  when combined with calls to {@link #disconnect()}.
   *  @param xmlFileName The name of a declarative xml file to use at startup, or null.
   *  @throws HydraConfigException if the system property is not set.
   *  @throws HydraRuntimeException if the distributed system node is not local.
   */
  public static DistributedSystem connect(String cacheXmlFile) {
    if ( vmConnection == null ) {
      String gemfireName = System.getProperty( GemFirePrms.GEMFIRE_NAME_PROPERTY );
      if ( gemfireName == null ) {
        throw new HydraConfigException( "No gemfire name has been specified" );
      } else {
        GemFireDescription gfd = TestConfig.getInstance().getGemFireDescription( gemfireName );
        if ( HostHelper.isLocalHost( gfd.getHostDescription().getHostName() ) ) {
          _connect(gfd, cacheXmlFile);
        } else {
          throw new HydraConfigException( gemfireName + " is on remote system " + gfd.getHostDescription().getHostName() );
        }
      }
    } else {
      if (cacheXmlFile != null)
         throw new HydraRuntimeException("Cannot honor " + cacheXmlFile + " because there is already a connection to the distributed system");
    }
    return vmConnection;
  }
  /**
   *  Gets a connection to the distributed system named in the
   *  {@link hydra.GemFirePrms#GEMFIRE_NAME_PROPERTY} system property.
   *  This comes from {@link ClientPrms#gemfireNames}.
   *  <p>
   *  This method is thread-safe for clients in each VM.  It is not thread-safe
   *  when combined with calls to {@link #disconnect()}.
   *  @throws HydraConfigException if the system property is not set.
   *  @throws HydraRuntimeException if the distributed system node is not local.
   */
  public static DistributedSystem connect() {
    return connect(null);
  }

  /**
   *  This method was added as a workaround for testing locators running in client VM.
   *  It sets the VM connection to the distributed system named in the input
   *  name using the input system properties.
   *  The workaround was required to prevent hydra tests from connecting to the 
   *  hydra-configured gemfire system which allows one and only one locator.
   *  <p>
   *  This method is thread-safe for clients in each VM.  It is not thread-safe
   *  when combined with calls to {@link #disconnect()}.
   *  @throws HydraConfigException if the system property is not set.
   *  @throws HydraRuntimeException if the distributed system node is not local.
   */
  public static DistributedSystem connect(String name, Properties props) {
    if ( vmConnection == null ) {
      GemFireDescription gfd = TestConfig.getInstance().getGemFireDescription( name );
        if ( HostHelper.isLocalHost( gfd.getHostDescription().getHostName() ) ) {
          _connectWithProps(gfd, props );
        } else {
          throw new HydraConfigException( name + " is on remote system " + gfd.getHostDescription().getHostName() );
        }
    }
    return vmConnection;
  }


  /**
   *  Disconnects this VM from the distributed system by closing its connection,
   *  if one exists.
   *  <p>
   *  CAUTION:  If used from a VM that is running concurrent clients, it is up
   *  to the clients to handle the fallout.
   */
  public static synchronized void disconnect() {
    if ( vmConnection != null ) {
      vmConnection.disconnect();
      vmConnection = null;
    }
  }
  /**
   *  Answers whether this VM is connected.
   */
  public static boolean isConnected() {
    if ( vmConnection == null )
      return false;
    else
      return vmConnection.isConnected();
  }
  /**
   *  Safely connects this VM to GemFire. 
   *  @throws HydraRuntimeException if the system is not on this host.
   */
  private static synchronized void _connectWithProps(GemFireDescription gfd,
                                                     Properties props) {
    if ( HostHelper.isLocalHost( gfd.getHostDescription().getHostName() ) ) {
      if ( vmConnection == null ) {
        if (!gfd.getUseLocator().booleanValue()) {
          lock();   // workaround for 30341, remove when fixed
        }
        vmConnection = DistributedSystem.connect( props );
        if (!gfd.getUseLocator().booleanValue()) {
          unlock(); // workaround for 30341, remove when fixed
        }
      }
    } else {
      throw new HydraRuntimeException( "Cannot connect VM on host " + HostHelper.getLocalHost() + " to GemFire " + gfd.getName() + " on remote host " + gfd.getHostDescription().getHostName() );
    }
  }


  /**
   *  Safely connects this VM to GemFire using the given distributed system
   *  configuration and optional cache configuration.
   *  @throws HydraRuntimeException if the system is not on this host.
   */
  private static synchronized void _connect(GemFireDescription gfd,
                                            String cacheXmlFile) {
    if ( HostHelper.isLocalHost( gfd.getHostDescription().getHostName() ) ) {
      if ( vmConnection == null ) {
        Properties p = gfd.getDistributedSystemProperties();
        if (cacheXmlFile != null) { // add the cache configuration
          p.setProperty(DistributionConfig.CACHE_XML_FILE_NAME, cacheXmlFile);
        }
        if (!gfd.getUseLocator().booleanValue()) {
          lock();   // workaround for 30341, remove when fixed
        }
        vmConnection = DistributedSystem.connect( p );
        if (!gfd.getUseLocator().booleanValue()) {
          unlock(); // workaround for 30341, remove when fixed
        }
      }
    } else {
      throw new HydraRuntimeException( "Cannot connect VM on host " + HostHelper.getLocalHost() + " to GemFire " + gfd.getName() + " on remote host " + gfd.getHostDescription().getHostName() );
    }
  }
  // start workaround for 30341, remove when fixed
  private static void lock() {
    //try {
    //  master().lockToSynchronizeJavaGroupsForBug30341();
    //} catch( java.rmi.RemoteException e ) {
    //  throw new HydraRuntimeException( "Failed to lock JavaGroups synchronizer", e );
    //}
  }
  private static void unlock() {
    //try {
    //  master().unlockToSynchronizeJavaGroupsForBug30341();
    //} catch( java.rmi.RemoteException e ) {
    //  throw new HydraRuntimeException( "Failed to unlock JavaGroups synchronizer", e );
    //}
  }
  private static MasterProxyIF master() throws java.rmi.RemoteException {
    if ( Master == null ) {
      if ( RemoteTestModule.Master == null ) {
        Master = (MasterProxyIF) new MasterProxy();
      } else {
        Master = RemoteTestModule.Master;
      }
    }
    return Master;
  }
  private static MasterProxyIF Master;
  // end workaround for 30341, remove when fixed
}
