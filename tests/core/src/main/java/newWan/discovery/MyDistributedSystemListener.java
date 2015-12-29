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
package newWan.discovery;

import hydra.DistributedSystemHelper;
import hydra.Log;
import hydra.RemoteTestModule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.internal.cache.wan.DistributedSystemListener;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.InternalLocator;

/**
 * Implementation for DistributedSystemListener used to test Jayesh usecase.
 * It it set using system property -Dgemfire.DistributedSystemListner
 * 
 * @author rdiyewar
 * @since 6.8
 */
public class MyDistributedSystemListener implements DistributedSystemListener {

  public static List<String> siteAddedList = new ArrayList<String>();

  public static List<String> siteRemovedList = new ArrayList<String>();

  @Override
  public void removedDistributedSystem(int remoteDsId) {
    long siteRemovedcount = DynamicDiscoveryBB.getInstance()
        .getSharedCounters().incrementAndRead(
            DynamicDiscoveryBB.SITE_REMOVED_LISTENER_INVOCATION_COUNTER);
    String remoteDsName = DistributedSystemHelper
        .getDistributedSystemName(remoteDsId);
    Log.getLogWriter()
        .info(
            "In " + getMyUniqueName() + " removed distributed system "
                + remoteDsId);

    if (!siteRemovedList.contains(remoteDsName)) {
      siteRemovedList.add(remoteDsName);
    }
    else { // duplicate invocation
      DynamicDiscoveryBB
          .throwException("Duplicate invocation of MyDistributedSystemListener.removedDistributedSystem for "
              + remoteDsName);
    }

    List<Locator> locators = Locator.getLocators();
    Map gfLocMap = ((InternalLocator)locators.get(0)).getAllLocatorsInfo();
    if (gfLocMap.containsKey(remoteDsId)) {
      DynamicDiscoveryBB.throwException("Expected remote site " + remoteDsId
          + " to be removed , but it is found with locators "
          + gfLocMap.get(remoteDsId) + ". InternalLocator.getAllLocatorsInfo="
          + gfLocMap);
    }
    else {
      Log
          .getLogWriter()
          .info(
              "In MyDistributedSystemListener.removedDistributedSystem: InternalLocator.getAllLocatorsInfo="
                  + gfLocMap);
    }
  }

  @Override
  public void addedDistributedSystem(int remoteDsId) {
    long siteAddedcount = DynamicDiscoveryBB.getInstance().getSharedCounters()
        .incrementAndRead(
            DynamicDiscoveryBB.SITE_ADDED_LISTENER_INVOCATION_COUNTER);
    String remoteDsName = DistributedSystemHelper
        .getDistributedSystemName(remoteDsId);
    Log.getLogWriter().info(
        "In " + getMyUniqueName() + " added distributed system " + remoteDsId);

    if (!siteAddedList.contains(remoteDsName)) {
      siteAddedList.add(remoteDsName);
    }
    else { // duplicate invocation
      DynamicDiscoveryBB
          .throwException("Duplicate invocation of addedDistributedSystem for "
              + remoteDsName);
    }

    List<Locator> locators = Locator.getLocators();
    Map gfLocMap = ((InternalLocator)locators.get(0)).getAllLocatorsInfo();
    if (!gfLocMap.containsKey(remoteDsId)) {
      DynamicDiscoveryBB.throwException("Expected remote site " + remoteDsId
          + " in " + getMyUniqueName() + " , but it is not found."
          + " InternalLocator.getAllLocatorsInfo=" + gfLocMap);
    }
    else {
      Log
          .getLogWriter()
          .info(
              "In MyDistributedSystemListener.addedDistributedSystem: InternalLocator.getAllLocatorsInfo="
                  + gfLocMap);
    }
  }

  /**
   * Uses RemoteTestModule information to produce a name to uniquely identify a
   * client vm (vmid, clientName, host, pid) for the calling thread
   */
  private String getMyUniqueName() {
    StringBuffer buf = new StringBuffer(50);
    buf.append("vm_").append(RemoteTestModule.getMyVmid());
    buf.append("_").append(RemoteTestModule.getMyClientName());
    buf.append("_").append(RemoteTestModule.getMyHost());
    buf.append("_").append(RemoteTestModule.getMyPid());
    return buf.toString();
  }
}
