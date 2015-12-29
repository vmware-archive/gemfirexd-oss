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
package com.gemstone.gemfire.management.internal;

import java.beans.IntrospectionException;
import java.util.Iterator;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.ObjectName;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.ClassLoadUtil;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.management.ManagementException;

/**
 * Instance of this class is responsible for proxy creation/deletion etc.
 * 
 * If a member is added/removed proxy factory is responsible for creating
 * removing the corresponding proxies for that member.
 * 
 * It also maintains a proxy repository {@link MBeanProxyInfoRepository} for
 * quick access to the proxy instances
 * 
 * @author rishim
 * 
 */
public class MBeanProxyFactory {
  
  private static final String THIS_COMPONENT = MBeanProxyFactory.class.getName();

  /**
	 * Proxy repository contains several indexes to search proxies in an efficient manner.
	 */
  private MBeanProxyInfoRepository proxyRepo;

  /**
	 * Logger
	 */
  private LogWriterI18n logger;

  /**
	 * For Future release
	 */
  private RemoteFilterChain remoteFilterChain;

  /**
   * Interface between GemFire federation layer and Java JMX layer
   */
  private MBeanJMXAdapter jmxAdapter;

  private SystemManagementService service;

  /**
   * 
   * @param remoteFilterChain remote chain to filter out remote members
   * @param jmxAdapter adapter to interface between JMX and GemFire
   * @param service management service
   */
  public MBeanProxyFactory(RemoteFilterChain remoteFilterChain,
      MBeanJMXAdapter jmxAdapter, SystemManagementService service) {

    this.remoteFilterChain = remoteFilterChain;
    this.jmxAdapter = jmxAdapter;
    this.proxyRepo = new MBeanProxyInfoRepository();
    this.logger = InternalDistributedSystem.getLoggerI18n();
    this.service = service;
  }

  /**
   * Creates a single proxy and adds a {@link ProxyInfo} to proxy repository
   * {@link MBeanProxyInfoRepository}
   * 
   * @param member
   *          {@link com.gemstone.gemfire.distributed.DistributedMember}
   * @param objectName
   *          {@link javax.management.ObjectName} of the Bean
   * @param monitoringRegion
   *          monitoring region containing the proxies
   * @throws ManagementException
   */
  public void createProxy(DistributedMember member, ObjectName objectName,
      Region<String, Object> monitoringRegion,Object newVal) {

    try {

      /*
       * Check the complete filter chain to evaluate the applicability of the
       * MBean
       */

      if (remoteFilterChain.isFiltered(objectName, member, "")) {
        if (logger.finerEnabled()) {
          logger.finer("returning from filter");
        }

        return;
      }

      Class interfaceClass = ClassLoadUtil
          .classFromName(((FederationComponent) monitoringRegion
              .get(objectName.toString())).getMBeanInterfaceClass());

      Object object = MBeanProxyInvocationHandler.newProxyInstance(member,
          monitoringRegion, objectName, interfaceClass);

      jmxAdapter.registerMBeanProxy(object, objectName);
      
      if (logger.fineEnabled()) {
        logger.fine("Registered ObjectName : " + objectName);
      }

      ProxyInfo proxyInfo = new ProxyInfo(interfaceClass, object, objectName);
      proxyRepo.addProxyToRepository(member, proxyInfo);
      
      service.afterCreateProxy(objectName, interfaceClass, object, (FederationComponent)newVal);      
      
      if (logger.fineEnabled()) {
        logger.fine("Proxy Created for : " + objectName);
      }     
      
    } catch (ClassNotFoundException e) {
      throw new ManagementException(e);
    } catch (IntrospectionException e) {
      throw new ManagementException(e);
    } catch (ManagementException e) {
      throw e;
    }

  }

  /**
   * This method will create all the proxies for a given DistributedMember. It
   * does not throw any exception to its caller. It handles the error and logs
   * error messages
   * 
   * It will be called from GII or when a member joins the system
   * 
   * @param member
   *          {@link com.gemstone.gemfire.distributed.DistributedMember}
   * @param monitoringRegion
   *          monitoring region containing the proxies
   */
  public void createAllProxies(DistributedMember member,
      Region<String, Object> monitoringRegion) {

    if (logger.infoEnabled()) {
      logger.info(LocalizedStrings.DEBUG,"Creating proxy for: " + member.getId());
    }
    /*
     * Check for member and server group filters if the member is filtered no
     * need to proceed further
     */

    if (remoteFilterChain.isServerGroupFiltered("")) {
      if (logger.finerEnabled()) {
        logger.finer("returning from server group filter");
      }
      return;

    }
    if (remoteFilterChain.isManagedNodeFiltered(member)) {
      if (logger.finerEnabled()) {
        logger.finer("returning from managed node filter");
      }
      return;
    }

    Set<String> mbeanNames = monitoringRegion.keySet();

    Iterator<String> it = mbeanNames.iterator();

    while (it.hasNext()) {

      ObjectName objectName = null;
      /*
       * This is for MBean filter check. If filtered MBean wont be registered
       */

      if (remoteFilterChain.isRemoteMBeanFiltered(objectName)) {
        if (logger.finerEnabled()) {
          logger.finer("continue  from remote MBEan node filter");
        }

        continue;
      }
      try {
        objectName = ObjectName.getInstance(it.next());
        if (logger.fineEnabled()) {
          logger.fine("Creating proxy for ObjectName: " + objectName.toString());
        }

        createProxy(member, objectName, monitoringRegion, monitoringRegion
            .get(objectName.toString()));
      } catch (ManagementException e) {
          logger.warning(LocalizedStrings.DEBUG,THIS_COMPONENT + ": Create Proxy failed for " + objectName + "With Exception " + e);
        continue;
      } catch (Exception e) {
        logger.warning(LocalizedStrings.DEBUG,THIS_COMPONENT + ": Create Proxy failed for " + objectName + "With Exception " + e);
        continue;
      }

    }
  }

  /**
   * Removes all proxies for a given member
   * 
   * @param member
   *          {@link com.gemstone.gemfire.distributed.DistributedMember}
   * @param monitoringRegion
   *          monitoring region containing the proxies
   */
  public void removeAllProxies(DistributedMember member,
      Region<String, Object> monitoringRegion) {

    Set<String> mbeanNames = monitoringRegion.keySet();
    
    if (logger.infoEnabled()) {
      logger.info(LocalizedStrings.DEBUG,"Removing " +  mbeanNames.size() + " proxies for member " +member.getId());
    }
    
    Iterator<String> it = mbeanNames.iterator();
    while (it.hasNext()) {
      ObjectName mbeanName = null;

      try {
        mbeanName = ObjectName.getInstance(it.next());
        removeProxy(member, mbeanName, monitoringRegion.get(mbeanName));
      } catch (Exception e) {
        if (!(e.getCause() instanceof InstanceNotFoundException) && logger.warningEnabled()) {
          logger.warning(LocalizedStrings.DEBUG, "Remove Proxy failed for " + mbeanName + "due to " + e);
        }
        continue;
      }

    }

  }

  /**
   * Removes the proxy
   * 
   * @param member
   *          {@link com.gemstone.gemfire.distributed.DistributedMember}
   * @param objectName
   *          {@link javax.management.ObjectName} of the Bean
   */
  public void removeProxy(DistributedMember member, ObjectName objectName,
      Object oldVal) {

    try {
      
      if (logger.fineEnabled()) {
        logger.fine("Removing proxy for ObjectName: "+ objectName.toString());
      }
      if (!remoteFilterChain.isRemoteMBeanFiltered(objectName)) {
        ProxyInfo proxyInfo = proxyRepo.findProxyInfo(objectName);
        proxyRepo.removeProxy(member, objectName);
        if (proxyInfo != null) {
          service.afterRemoveProxy(objectName, proxyInfo.getProxyInterface(), proxyInfo.getProxyInstance(),
              (FederationComponent)oldVal);
        }
        jmxAdapter.unregisterMBean(objectName);
        
        if (logger.fineEnabled())  {
          logger.fine("Removed proxy for ObjectName: "+ objectName.toString());
        }
      }

    } catch (Exception e) {
      if (!(e.getCause() instanceof InstanceNotFoundException) && logger.warningEnabled()) {
        logger.warning(LocalizedStrings.DEBUG,
            "Could not remove proxy for Member " + member + " due to " + e);
      }     
    }
  }

  public void updateProxy(ObjectName objectName, ProxyInfo proxyInfo, Object newObject, Object oldObject) {
    try {
      if (proxyInfo != null) {
        Class interfaceClass = proxyInfo.getProxyInterface();
        service.afterUpdateProxy(objectName, interfaceClass, proxyInfo.getProxyInstance(),(FederationComponent)newObject, (FederationComponent)oldObject);
      }
    } catch (Exception e) {
      throw new ManagementException(e);
    }
  }

  /**
   * Find a particular proxy instance for a {@link javax.management.ObjectName}
   * , {@link com.gemstone.gemfire.distributed.DistributedMember} and interface
   * class If the proxy interface does not implement the given interface class a
   * {@link java.lang.ClassCastException}. will be thrown
   * 
   * @param objectName
   *          {@link javax.management.ObjectName} of the MBean
   * @param interfaceClass
   *          interface class implemented by proxy
   * @return an instance of proxy exposing the given interface
   */
  public <T> T findProxy(ObjectName objectName,
      Class<T> interfaceClass) {


      return proxyRepo.findProxyByName(objectName, interfaceClass);
   

  }
  
  public ProxyInfo findProxyInfo(ObjectName objectName) {
    return proxyRepo.findProxyInfo(objectName);
  }

  /**
   * Find a set of proxies given a
   * {@link com.gemstone.gemfire.distributed.DistributedMember}
   * 
   * @param member
   *          {@link com.gemstone.gemfire.distributed.DistributedMember}
   * @return a set of {@link javax.management.ObjectName}
   */
  public Set<ObjectName> findAllProxies(DistributedMember member) {

    return proxyRepo.findProxySet(member);

  }

  /**
   * This will return the last updated time of the proxyMBean

   * @param objectName
   *          {@link javax.management.ObjectName} of the MBean
   * @return last updated time of the proxy
   * 
   * @throws InstanceNotFoundException
   */
  public long getLastUpdateTime(ObjectName objectName) {

    ProxyInterface proxyObj = findProxy(objectName,
        ProxyInterface.class);

    return proxyObj.getLastRefreshedTime();

  }

}