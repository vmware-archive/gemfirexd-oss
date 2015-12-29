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

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.admin.*;
import com.gemstone.gemfire.admin.internal.*;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.admin.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import javax.management.*;
import javax.management.modelmbean.*;
//import org.apache.commons.modeler.ManagedBean;

/**
 * The JMX "managed resource" that represents the health of GemFire.
 * Basically, it provides the behavior of
 * <code>GemFireHealthImpl</code>, but does some JMX stuff like
 * registering beans with the agent.
 *
 * @see AdminDistributedSystemJmxImpl#createGemFireHealth
 *
 * @author David Whitlock
 *
 * @since 3.5
 */
public class GemFireHealthJmxImpl extends GemFireHealthImpl 
  implements ManagedResource {

  /** The name of the MBean that will manage this resource */
  private final String mbeanName;

  /** The ModelMBean that is configured to manage this resource */
  private ModelMBean modelMBean;

  /** The object name of the MBean created for this managed resource */
  private final ObjectName objectName;

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Creates a new <code>GemFireHealthJmxImpl</code> that monitors the
   * health of the given distributed system and uses the given JMX
   * agent. 
   */
  GemFireHealthJmxImpl(GfManagerAgent agent,
                       AdminDistributedSystemJmxImpl system)
    throws AdminException {

    super(agent, system);
    this.mbeanName = new StringBuffer()
      .append(MBEAN_NAME_PREFIX)
      .append("GemFireHealth,id=")
      .append(MBeanUtil.makeCompliantMBeanNameProperty(system.getId()))
      .toString();
    this.objectName = MBeanUtil.createMBean(this);
  }

  //////////////////////  Instance Methods  //////////////////////

  public String getHealthStatus() {
    return getHealth().toString();
  }
  
  public ObjectName manageGemFireHealthConfig(String hostName)
  throws MalformedObjectNameException {
    LogWriterI18n logWriter = this.getDistributedSystem().getLogWriter().convertToLogWriterI18n();
    try {
      GemFireHealthConfig config = getGemFireHealthConfig(hostName);
      GemFireHealthConfigJmxImpl jmx = (GemFireHealthConfigJmxImpl) config;
      return new ObjectName(jmx.getMBeanName());
    } //catch (AdminException e) { logWriter.warning(e); throw e; }
    catch (RuntimeException e) { 
      logWriter.warning(e); 
      throw e; 
    } catch (Error e) {
      if (SystemFailure.isJVMFailureError(e)) {
        SystemFailure.initiateFailure(e);
        // If this ever returns, rethrow the error. We're poisoned
        // now, so don't let this thread continue.
        throw e;
      }
      // Whenever you catch Error or Throwable, you must also
      // check for fatal JVM error (see above).  However, there is
      // _still_ a possibility that you are dealing with a cascading
      // error condition, so you also need to check to see if the JVM
      // is still usable:
      SystemFailure.checkFailure();
      logWriter.error(e); throw e; 
    }
  }
    
  /**
   * Creates a new {@link DistributedSystemHealthConfigJmxImpl}
   */
  @Override
  protected DistributedSystemHealthConfig
    createDistributedSystemHealthConfig() {

    try {
      return new DistributedSystemHealthConfigJmxImpl(this);

    } catch (AdminException ex) {
      throw new RuntimeAdminException(LocalizedStrings.GemFireHealthJmxImpl_WHILE_GETTING_THE_DISTRIBUTEDSYSTEMHEALTHCONFIG.toLocalizedString(), ex);
    }
  }

  /**
   * Creates a new {@link GemFireHealthConfigJmxImpl}
   */
  @Override
  protected GemFireHealthConfig
    createGemFireHealthConfig(String hostName) {

    try {
      return new GemFireHealthConfigJmxImpl(this, hostName);

    } catch (AdminException ex) {
      throw new RuntimeAdminException(LocalizedStrings.GemFireHealthJmxImpl_WHILE_GETTING_THE_GEMFIREHEALTHCONFIG.toLocalizedString(), ex);
    }
  }

  /**
   * Ensures that the three primary Health MBeans are registered and returns
   * their ObjectNames. 
   */
  protected void ensureMBeansAreRegistered() {
    MBeanUtil.ensureMBeanIsRegistered(this);
    MBeanUtil.ensureMBeanIsRegistered((ManagedResource)this.defaultConfig);
    MBeanUtil.ensureMBeanIsRegistered((ManagedResource)this.dsHealthConfig);
  }
  
  public String getMBeanName() {
    return this.mbeanName;
  }
  
  public ModelMBean getModelMBean() {
    return this.modelMBean;
  }

  public void setModelMBean(ModelMBean modelMBean) {
    this.modelMBean = modelMBean;
  }

  public ManagedResourceType getManagedResourceType() {
    return ManagedResourceType.GEMFIRE_HEALTH;
  }

  public ObjectName getObjectName() {
    return this.objectName;
  }

  public void cleanupResource() {
    close();
  }
  
}
