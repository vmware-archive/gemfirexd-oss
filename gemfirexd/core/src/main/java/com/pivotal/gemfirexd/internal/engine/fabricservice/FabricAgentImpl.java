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

import java.sql.SQLException;
import java.util.Properties;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.AdminException;
import com.gemstone.gemfire.admin.jmx.Agent;
import com.gemstone.gemfire.admin.jmx.AgentConfig;
import com.pivotal.gemfirexd.FabricAgent;
import com.pivotal.gemfirexd.internal.engine.Misc;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.jdbc.GemFireXDRuntimeException;
import com.pivotal.gemfirexd.internal.shared.common.sanity.SanityManager;

/**
 * GemFireXD JMX Agent servicing part of {@link FabricServiceManager} extending capabilities
 * of GemFire {@link Agent}.
 *  
 * @author soubhikc
 *
 */
public class FabricAgentImpl extends FabricServiceImpl implements FabricAgent {
  
  private Agent _impl;
  
  public FabricAgentImpl() {
    super();
  }
  
  @Override
  public void start(Properties bootProperties) throws SQLException {
     start(bootProperties, false);
  }

  @Override
  public synchronized void start(Properties bootProperties, boolean ignoreIfStarted)
      throws SQLException {
    if (bootProperties == null) {
      bootProperties = new Properties();
    }
    
    bootProperties.setProperty(com.pivotal.gemfirexd.Attribute.GFXD_HOST_DATA, "false");
    

    try {
      // set the agent indicator
      bootProperties.setProperty(GfxdConstants.PROPERTY_BOOT_INDICATOR,
          GfxdConstants.BT_INDIC.FABRICAGENT.toString());
      startImpl(bootProperties, ignoreIfStarted, true);
      _impl = Misc.getMemStore().getGemFireAgent();
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
    finally {
      bootProperties.remove(GfxdConstants.PROPERTY_BOOT_INDICATOR);
    }
  }
  
  @Override
  public ObjectName connectToSystem() throws AdminException,
      MalformedObjectNameException {
    checkNull(_impl);
    assert _impl.isConnected();
    return _impl.getObjectName();
  }

  @Override
  public void disconnectFromSystem() {
    try {
      checkNull(_impl);
      stop(null);
    } catch (SQLException sqle) {
      SanityManager.DEBUG_PRINT("error:" + fabapi, sqle.getSQLState()
          + " error occurred while starting server : " + sqle);
      throw GemFireXDRuntimeException.newRuntimeException(
          "GemFireXD:FabricAgent#disconnectToSystem exception occurred ", sqle);
    }
  }

  @Override
  public AgentConfig getConfig() {
    checkNull(_impl);
    return _impl.getConfig();
  }

  @Override
  public AdminDistributedSystem getDistributedSystem() {
    checkNull(_impl);
    return _impl.getDistributedSystem();
  }

  @Override
  public LogWriter getLogWriter() {
    checkNull(_impl);
    return _impl.getLogWriter();
  }

  @Override
  public MBeanServer getMBeanServer() {
    checkNull(_impl);
    return _impl.getMBeanServer();
  }

  @Override
  public ObjectName getObjectName() {
    checkNull(_impl);
    return _impl.getObjectName();
  }

  @Override
  public boolean isConnected() {
    checkNull(_impl);
    return _impl.isConnected();
  }

  @Override
  public ObjectName manageDistributedSystem()
      throws MalformedObjectNameException {
    checkNull(_impl);
    return _impl.manageDistributedSystem();
  }

  @Override
  public void saveProperties() {
    checkNull(_impl);
    _impl.saveProperties();
  }

  @Override
  public void start() {
    throw new GemFireXDRuntimeException("Operation UnSupported. Use FabricAgent#start(bootProperties) variants");
  }

  @Override
  public void stop() {
    throw new GemFireXDRuntimeException("Operation UnSupported. Use FabricAgent#stop(shutdownCredentials) variant");
  }
  
  private void checkNull(Agent impl) {
    if(impl == null) {
      throw new GemFireXDRuntimeException("Agent Not Connected");
    }
  }

}
