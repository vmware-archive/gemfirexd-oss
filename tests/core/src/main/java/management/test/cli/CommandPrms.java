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
package management.test.cli;

import java.util.List;

import util.TestException;

import hydra.BasePrms;
import hydra.TestConfig;

public class CommandPrms extends BasePrms {

  /** (boolean) If true, create empty or accessor regions, otherwise create
   *            regions that host data.
   */
  public static Long createProxyRegions;
  public static boolean getCreateProxyRegions() {
    Long key = createProxyRegions;
    boolean val = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return val;
  }
  
  /** (boolean) If true, create client regions for a client/server topology
   */
  public static Long createClientRegions;
  public static boolean getCreateClientRegions() {
    Long key = createClientRegions;
    boolean val = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return val;
  }

  /** (boolean) If true, create persistent regions 
   */
  public static Long createPersistentRegions;
  public static boolean getCreatePersistentRegions() {
    Long key = createPersistentRegions;
    boolean val = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return val;
  }

  /** (boolean) If true, create subregions 
   */
  public static Long createSubregions;
  public static boolean getCreateSubregions() {
    Long key = createSubregions;
    boolean val = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return val;
  }

  /** (String) The Desired number of managers to create
   */
  public static Long nbrOfManagers;
  public static String getNbrOfManagers() {
    Long key = nbrOfManagers;
    String val = tasktab().stringAt(key, tab().stringAt(key, "0"));
    return val;
  }
  
  /** (int) The Desired number of milliseconds to sleep for
   */
  public static Long sleepForMs;
  public static int getSleepForMs() {
    Long key = sleepForMs;
    return tasktab().intAt(key, tab().intAt(key, 0));
  }

  /**
   * (boolean) Whether functions have been registered
   */
  public static Long functionsRegistered;
  public static boolean getFunctionsRegistered() {
    Long key = functionsRegistered;
    boolean val = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return val;
  }

  /**
   * (boolean) Whether indexes have been added
   */
  public static Long indexAdded;
  public static boolean getIndexAdded() {
    Long key = indexAdded;
    boolean val = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return val;
  }

  /**
   * (boolean) Whether regions have been added
   */
  public static Long regionsAdded;
  public static boolean getRegionsAdded() {
    Long key = regionsAdded;
    boolean val = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return val;
  }

  /** (int) The number of members that should initialize by joining the
   *        distributed system only, but should not create a cache (and
   *        thus no regions).
   */
  public static Long numMembersJoinDSOnly;
  public static int getNumMembersJoinDSOnly() {
    Long key = numMembersJoinDSOnly;
    int val = tasktab().intAt(key, tab().intAt(key, 0));
    return val;
  }
  
  /** (int) The number of members that should initialize by joining the
   *        distributed system and creating a cache only, but should not 
   *        regions.
   */
  public static Long numMembersCreateCacheOnly;
  public static int getNumMembersCreateCacheOnly() {
    Long key = numMembersCreateCacheOnly;
    int val = tasktab().intAt(key, tab().intAt(key, 0));
    return val;
  }
  
  /** (int) The number of members to stop at one time.
   */
  public static Long numToStop;
  public static int getNumToStop() {
    Long key = numToStop;
    int val = tasktab().intAt(key, tab().intAt(key, 0));
    return val;
  }
  
  /** (int) The number of entries to load into each region.
   */
  public static Long numToLoadEachRegion;  
  public static int getNumToLoadEachRegion() {
    Long key = numToLoadEachRegion;
    int value = tasktab().intAt(key, tab().intAt(key, 100));
    return value;
  }
 
  /** (boolean) If true, we have Admin Hosts
   */
  public static Long haveAdminHosts;
  public static boolean haveAdminHosts() {
    Long key = haveAdminHosts;
    boolean val = tasktab().booleanAt(key, tab().booleanAt(key, false));
    return val;
  }

  /** (int) The number of member host that are created.
   */
  public static Long nbrMemberHosts;
  public static int getNbrMemberHosts() {
    Long key = nbrMemberHosts;
    int val = tasktab().intAt(key, tab().intAt(key, 0));
    return val;
  }
  
  /** (int) The number of members that should host a region. Defaults to -1 (all members).
   *        This is used so that not all members host all regions.
   */
  public static Long numMembersToHostRegion;
  public static int getNumMembersToHostRegion() {
    Long key = numMembersToHostRegion;
    int val = tasktab().intAt(key, tab().intAt(key, -1));
    return val;
  }

  /** (String) Can be one of:
   *              "locator" - to connect to a locator
   *              "jmxManager" to connect to a jmxManager
   *              "http" to use http with a jmxManager
   */
  public static Long howToConnect;
  public static String getHowToConnect() {
    Long key = howToConnect;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    return val;
  }
  
  // ***** still honored in tests, but preference is to use howToConnect instead
  /** (String) Can be one of "true", "false" or "random".
   *           If "true" connect the CLI to a locator, if "false" connect to a jmxManager.
   *           If "random" then randomly choose.
   */
  public static Long connectToLocator;
  public static boolean getConnectToLocator() {
    Long key = connectToLocator;
    String val = tasktab().stringAt(key, tab().stringAt(key, "true"));
    if (val.equalsIgnoreCase("true")) {
      return true;
    } else if (val.equalsIgnoreCase("false")) {
      return false;
    } else if (val.equalsIgnoreCase("random")) {
      return TestConfig.tab().getRandGen().nextBoolean();
    } else {
      throw new TestException(CommandPrms.class.getName() + "-connectToLocator must be set to true, false or random");
    }
  }
  
  /** (String) Used to limit managers started with the API to certain members.
   */
  public static Long limitManagersToMembers;
  public static String getLimitManagersToMembers() {
    Long key = limitManagersToMembers;
    String val = tasktab().stringAt(key, tab().stringAt(key, null));
    return val;
  }
  
  /** (boolean) If true, then verify that managers are only running in locators,
   *            otherwise allow non-locator members to be managers.
   */
  public static Long expectLocatorManagers;
  public static boolean getExpectLocatorManagers() {
    Long key = expectLocatorManagers;
    boolean val = tasktab().booleanAt(key, tab().booleanAt(key, true));
    return val;
  }

  /** (int) The number of seconds to wait for a gfsh command before it times out.
  */
  public static Long commandWaitSec;
  public static int getCommandWaitSec() {
    Long key = commandWaitSec;
    int val = tasktab().intAt(key, tab().intAt(key, 60));
    return val;
  }

  // ================================================================================
  static {
    BasePrms.setValues(CommandPrms.class);
  }

}
