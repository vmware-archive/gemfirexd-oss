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

package com.pivotal.gemfirexd.internal.engine.fabricservice;

import java.net.InetAddress;
import java.sql.SQLException;
import java.util.Properties;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.pivotal.gemfirexd.Attribute;
import com.pivotal.gemfirexd.FabricLocator;
import com.pivotal.gemfirexd.internal.impl.jdbc.TransactionResourceImpl;

/**
 * Implementation of {@link FabricLocator} API. Future product versions may
 * extend this class to alter its behaviour.
 * 
 * @author swale
 */
public class FabricLocatorImpl extends FabricServiceImpl implements
    FabricLocator {

  /**
   * @see FabricLocator#start(String, int, Properties)
   */
  @Override
  public void start(String bindAddress, int port, Properties bootProperties)
      throws SQLException {
    start(bindAddress, port, bootProperties, false);
  }

  /**
   * @see FabricLocator#start(String, int, Properties, boolean)
   */
  @Override
  public synchronized void start(String bindAddress, int port,
      Properties bootProperties, boolean ignoreIfStarted) throws SQLException {

    if (bootProperties == null) {
      bootProperties = new Properties();
    }
    // set the locator specific properties
    bootProperties.setProperty(Attribute.STAND_ALONE_LOCATOR, "true");
    // don't host any data in this VM
    bootProperties.setProperty(Attribute.GFXD_HOST_DATA, "false");
    // set the mcast-port to zero if not set
    if (!bootProperties.containsKey(DistributionConfig.MCAST_PORT_NAME)) {
      bootProperties.setProperty(DistributionConfig.MCAST_PORT_NAME, "0");
    }
    // set the P2P bind-address to be the same as peer discovery one (#43679)
    if (bindAddress == null) {
      bindAddress = LOCATOR_DEFAULT_BIND_ADDRESS;
    }
    else if (!LOCATOR_DEFAULT_BIND_ADDRESS.equals(bindAddress)
        && bootProperties.get(DistributionConfig.BIND_ADDRESS_NAME) == null) {
      // don't pass wildcard addresses in bind-address (#46870)
      try {
        InetAddress addr = InetAddress.getByName(bindAddress);
        if (addr != null && !addr.isAnyLocalAddress()) {
          bootProperties.setProperty(DistributionConfig.BIND_ADDRESS_NAME,
              bindAddress);
        }
      } catch (Exception e) {
        // ignore
      }
    }
    // now set the GFE start-locator parameter
    if (port <= 0) {
      port = LOCATOR_DEFAULT_PORT;
    }
    else if (port <= 1025 || port >= 65535) {
      throw new IllegalArgumentException(
          "Allowed port range is between 1025 to 65535 (excluding limits)");
    }
    bootProperties.setProperty(DistributionConfig.START_LOCATOR_NAME,
        bindAddress + '[' + port + "],server=true");
    if (!bootProperties.containsKey(DistributionConfig.JMX_MANAGER_NAME)) {
      bootProperties.setProperty(DistributionConfig.JMX_MANAGER_NAME, "true");
    }
    
    try {
      startImpl(bootProperties, ignoreIfStarted, true);
    } catch (Throwable t) {
      Error err;
      if (t instanceof Error && SystemFailure.isJVMFailureError(
          err = (Error)t)) {
        FabricServiceUtils.clearSystemProperties(monitorlite, sysProps);
        SystemFailure.initiateFailure(err);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw err;
      }
      // Whenever you catch Error or Throwable, you must also
      // check for fatal JVM error (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      handleThrowable(t);
    }
  }

  // [bruce] the locator is started and owned by the DistributedSystem.  Shutting
  // it down here is not appropriate and causes a reconnected DS to not have
  // a locator.
//  @Override
//  protected void serviceShutdown() throws SQLException {
//    // stop any GemFire locator
//    final InternalLocator locator = InternalLocator.getLocator();
//    if (locator != null) {
//      try {
//        locator.stop();
//      } catch (Exception e) {
//        throw TransactionResourceImpl.wrapInSQLException(e);
//      }
//    }
//  }
}
