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

import hydra.DistributedSystemBlackboard;
import hydra.DistributedSystemHelper;
import hydra.GemFireDescription;
import hydra.MasterController;
import hydra.RemoteTestModule;
import hydra.DistributedSystemHelper.Endpoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;

import util.TestException;

import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;

import newWan.WANTest;

/**
 * Provides helper method for wan discovery tests. 
 * This was written to support the Jayesh usecase
 * 
 * @author rdiyewar
 * @since 6.8
 */
public class DynamicDiscoveryTest extends WANTest {

  /**
   * Add current ds to wan by starting the locator. This method is for Jayesh's
   * use case.
   */
  public synchronized static void addDistributedSystemToWANTask() {
    DynamicDiscoveryBB.getInstance().getSharedLock().lock();
    // start locator
    DistributedSystemHelper.startLocatorAndDS();

    // add entry in blackboard
    String dsname = DistributedSystemHelper.getDistributedSystemName();
    Integer vmid = new Integer(RemoteTestModule.getMyVmid());
    Map wanSites = (Map)DynamicDiscoveryBB.getInstance().getSharedMap().get(
        DynamicDiscoveryBB.WAN_SITES);
    if (wanSites == null) { // first site to come up
      wanSites = new HashMap();
      Set siteVms = new HashSet();
      siteVms.add(vmid);
      wanSites.put(dsname, siteVms);
      DynamicDiscoveryBB.getInstance().getSharedMap().put(
          DynamicDiscoveryBB.WAN_SITES, wanSites);

      // also create EXPECTED_ADD_PER_VM
      Map<Integer, List> addVmMap = (Map<Integer, List>)DynamicDiscoveryBB
          .getInstance().getSharedMap().get(
              DynamicDiscoveryBB.EXPECTED_ADD_PER_VM);
      if (addVmMap == null)
        addVmMap = new HashMap();
      DynamicDiscoveryBB.getInstance().getSharedMap().put(
          DynamicDiscoveryBB.EXPECTED_ADD_PER_VM, addVmMap);
    }
    else {
      // update WAN_SITES map
      Set siteVms = (Set)wanSites.get(dsname);
      if (siteVms == null)
        siteVms = new HashSet();
      siteVms.add(vmid);
      wanSites.put(dsname, siteVms);

      // update EXPECTED_ADD_PER_VM map
      Map<Integer, List> addVmMap = (Map<Integer, List>)DynamicDiscoveryBB
          .getInstance().getSharedMap().get(
              DynamicDiscoveryBB.EXPECTED_ADD_PER_VM);
      Set allds = new HashSet(wanSites.keySet());
      Set vms = (Set)wanSites.get(dsname);

      if (vms.size() == 1) {
        Set wanSiteKeys = wanSites.keySet();
        for (Iterator itr = wanSiteKeys.iterator(); itr.hasNext();) {
          Object ds = itr.next();
          if (ds.equals(dsname)) {
            List l = new ArrayList();
            allds.remove(dsname);
            l.addAll(allds);
            addVmMap.put(vmid, l);
          }
          else {
            Set vminds = (Set)wanSites.get(ds);
            for (Iterator i = vminds.iterator(); i.hasNext();) {
              Integer v = (Integer)i.next();
              List l = addVmMap.get(v);
              if (l == null)
                l = new ArrayList();
              l.add(dsname);
              addVmMap.put(v, l);
            }
          }
        }
      }
      else {
        List l = new ArrayList();
        allds.remove(dsname);
        l.addAll(allds);
        addVmMap.put(vmid, l);
      }

      // check for removed map
      Map siteRemoved = (Map)DynamicDiscoveryBB.getInstance().getSharedMap()
          .get(DynamicDiscoveryBB.WAN_SITES_REMOVED);
      if (siteRemoved != null && siteRemoved.get(dsname) != null) {
        Set removedvms = (Set)siteRemoved.remove(dsname);
        Set dsvms = (Set)wanSites.get(dsname);
        dsvms.addAll(removedvms);
        wanSites.put(dsname, dsvms);

        // update expected add per vms map
        List l = new ArrayList();
        allds.remove(dsname);
        l.addAll(allds);
        addVmMap.put(vmid, l);
        DynamicDiscoveryBB.getInstance().getSharedMap().put(
            DynamicDiscoveryBB.WAN_SITES_REMOVED, siteRemoved);
      }

      DynamicDiscoveryBB.getInstance().getSharedMap().put(
          DynamicDiscoveryBB.WAN_SITES, wanSites);
      DynamicDiscoveryBB.getInstance().getSharedMap().put(
          DynamicDiscoveryBB.EXPECTED_ADD_PER_VM, addVmMap);
    }
    DynamicDiscoveryBB.getInstance().getSharedLock().unlock();

  }

  public static void waitForLocatorDiscoveryTask() {
    new DynamicDiscoveryTest().waitForLocatorDiscovery();
  }

  /**
   * Remove the current distributed system from wan infrastructure. Here a
   * locator with negative distributed system id is started. 
   * This method is for Jayesh's use case.
   */
  public synchronized static void removeDistributedSystemFromWANTask() {
    Integer vmid = new Integer(RemoteTestModule.getMyVmid());
    Endpoint endpoint = (Endpoint)DistributedSystemBlackboard.getInstance()
        .getSharedMap().get(vmid);

    GemFireDescription gfd = DistributedSystemHelper.getGemFireDescription();
    Properties p = gfd.getDistributedSystemProperties();
    logger.info("Removing distributed system " + gfd.getDistributedSystem()
        + " from the wan.");

    Integer dsId = gfd.getDistributedSystemId();
    // use negative distributed system id
    p.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, "-"
        + dsId.toString());
    // get locators of only healthy site
    String remoteds = "ds_6";
    if (rand.nextInt(10) < 5)
      remoteds = "ds_7";
    List remoteEndpoints = DistributedSystemHelper.getContacts(remoteds);
    String val = DistributedSystemHelper.endpointsToString(remoteEndpoints);
    p.setProperty(DistributionConfig.REMOTE_LOCATORS_NAME, val);
    p.setProperty(DistributionConfig.LOCATORS_NAME, "");

    String remoteLocator = p
        .getProperty(DistributionConfig.REMOTE_LOCATORS_NAME);
    logger.info("rdrd: remote locator: " + remoteLocator);

    Locator locator = null;
    // start locator
    try {
      DynamicDiscoveryBB.getInstance().getSharedLock().lock();
      logger.info("Starting locator on port " + endpoint.getPort()
          + " with properties " + p);
      locator = Locator.startLocatorAndDS(endpoint.getPort(), null, p);
      logger.info("Started locator " + locator);

      // add entry in blackboard
      String dsname = DistributedSystemHelper.getDistributedSystemName();
      Map wanSites = (Map)DynamicDiscoveryBB.getInstance().getSharedMap().get(
          DynamicDiscoveryBB.WAN_SITES);
      if (wanSites == null) { // first site to go down
        wanSites = new HashMap();
        Set siteVms = new HashSet();
        siteVms.add(vmid);
        wanSites.put(dsname, siteVms);
        DynamicDiscoveryBB.getInstance().getSharedMap().put(
            DynamicDiscoveryBB.WAN_SITES, wanSites);
      }

      // update WAN_SITES map
      Map siteRemoved = (Map)DynamicDiscoveryBB.getInstance().getSharedMap()
          .get(DynamicDiscoveryBB.WAN_SITES_REMOVED);
      if (siteRemoved == null) {
        siteRemoved = new HashMap();
      }
      Set removedVms = (Set)wanSites.remove(dsname);
      siteRemoved.put(dsname, removedVms);

      // EXPECTED_REMOVE_PER_VM
      Map<Integer, List> removeVmMap = (Map<Integer, List>)DynamicDiscoveryBB
          .getInstance().getSharedMap().get(
              DynamicDiscoveryBB.EXPECTED_REMOVE_PER_VM);
      if (removeVmMap == null)
        removeVmMap = new HashMap();

      for (Iterator itr = wanSites.entrySet().iterator(); itr.hasNext();) {
        Entry<String, Set> e = (Entry<String, Set>)itr.next();
        String ds = e.getKey();
        Set vminds = e.getValue();
        for (Iterator i = vminds.iterator(); i.hasNext();) {
          Integer v = (Integer)i.next();
          List l = removeVmMap.get(v);
          if (l == null)
            l = new ArrayList();
          l.add(dsname);
          removeVmMap.put(v, l);
        }
      }

      logger.info("WAN_SITES=" + wanSites + "\n EXPECTED_REMOVE_PER_VM="
          + removeVmMap);
      DynamicDiscoveryBB.getInstance().getSharedMap().put(
          DynamicDiscoveryBB.WAN_SITES_REMOVED, siteRemoved);
      DynamicDiscoveryBB.getInstance().getSharedMap().put(
          DynamicDiscoveryBB.WAN_SITES, wanSites);
      DynamicDiscoveryBB.getInstance().getSharedMap().put(
          DynamicDiscoveryBB.EXPECTED_REMOVE_PER_VM, removeVmMap);
      DynamicDiscoveryBB.getInstance().getSharedLock().unlock();
    }
    catch (IOException e) {
      throw new TestException(
          "Exception is starting locator with negative ds id to remove the site",
          e);
    }

    // wait for some time
    MasterController.sleepForMs(10 * 1000);

    // stop locator
    System.out.println("Stoping locator " + locator);
    locator.stop();
    System.out.println("Locator stopped " + locator);
  }

  public static void verifyRemoteSiteAddedTask() {
    // get site addedSite list from listener and expected list from
    // configuration
    List<String> siteAddedList = MyDistributedSystemListener.siteAddedList;
    GemFireDescription gfd = DistributedSystemHelper.getGemFireDescription();
    Integer vmid = new Integer(RemoteTestModule.getMyVmid());
    Map<Integer, List> addVmMap = (Map<Integer, List>)DynamicDiscoveryBB
        .getInstance().getSharedMap().get(
            DynamicDiscoveryBB.EXPECTED_ADD_PER_VM);
    List<String> expectedRemoteDsList = addVmMap.get(vmid);
    logger.info("Remote sites added to this ds are " + siteAddedList
        + ", expected are " + expectedRemoteDsList);

    // check for missmatch
    List<String> extraSiteAddedList = new ArrayList<String>(siteAddedList);
    List<String> missingSiteAddedList = new ArrayList<String>();
    if (expectedRemoteDsList != null) {
      extraSiteAddedList.removeAll(expectedRemoteDsList);
      missingSiteAddedList = new ArrayList<String>(expectedRemoteDsList);
      missingSiteAddedList.removeAll(siteAddedList);
    }

    StringBuilder sb = new StringBuilder();
    if (extraSiteAddedList.size() > 0) {
      sb.append("Unexpected remote sites are added. Extra sites are "
          + extraSiteAddedList);
    }

    if (missingSiteAddedList.size() > 0) {
      sb.append("Some remote sites are missing. Missing sites are "
          + missingSiteAddedList);
    }

    if (sb.length() > 0) {
      throw new TestException(sb.toString());
    }

    logger.info("Verified remote site added.");
  }

  public static void verifyRemoteSiteRemovedTask() {
    // get site removed list from listener and expected list from configuration
    List<String> siteRemovedList = MyDistributedSystemListener.siteRemovedList;
    GemFireDescription gfd = DistributedSystemHelper.getGemFireDescription();
    String mydsname = gfd.getDistributedSystem();
    Integer vmid = new Integer(RemoteTestModule.getMyVmid());
    Map<Integer, List> removedVmMap = (Map<Integer, List>)DynamicDiscoveryBB
        .getInstance().getSharedMap().get(
            DynamicDiscoveryBB.EXPECTED_REMOVE_PER_VM);

    List<String> expectedRemoteDsList = removedVmMap.get(vmid);
    logger.info("Remote sites removed are " + siteRemovedList
        + ". Expected are " + expectedRemoteDsList);

    List<String> extraSiteRemovedList = new ArrayList<String>(siteRemovedList);
    List<String> siteNotRemovedList = new ArrayList<String>();
    ;
    if (expectedRemoteDsList != null) {
      extraSiteRemovedList.removeAll(expectedRemoteDsList);
      siteNotRemovedList = new ArrayList<String>(expectedRemoteDsList);
      siteNotRemovedList.removeAll(siteRemovedList);
    }

    StringBuilder sb = new StringBuilder();
    if (extraSiteRemovedList.size() > 0) {
      sb
          .append("Unexpected removal of remote sites observed. Extra sites removed are "
              + extraSiteRemovedList);
    }

    if (siteNotRemovedList.size() > 0) {
      sb
          .append("Expected sites to removed, but they are still exists. Unneccessary sites are "
              + siteNotRemovedList);
    }

    if (sb.length() > 0) {
      throw new TestException(sb.toString());
    }
    logger.info("Verified remote site removed.");
  }
}
