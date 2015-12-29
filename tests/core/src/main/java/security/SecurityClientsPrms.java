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
package security;

import java.util.Vector;

import hydra.BasePrms;

/**
 * A class to store the Keys for the tests on Security clients
 * 
 * @author Rajesh Kumar
 * @since 5.5
 * 
 */

public class SecurityClientsPrms extends BasePrms {

  /**
   * Parameter used to specify number of regions in the tests.
   */
  public static Long numberOfRegions;
  
  /**
   * Parameter used to specify number of users per VM in the tests.
   */
  public static Long numberOfUsersPerVM;
  

  public static Long numKeyRangePerThread;

  /**
   * Parameter used to create region name in the tests.
   */
  public static Long regionName;

  /**
   * Parameter used to pick the region randomly to perform operations.
   */
  public static Long regionRange;

  /**
   * (Vector of Strings) A list of the operations on a region entry that this
   * test is allowed to do. Can be one or more of:<br>
   * put - creates a new key/val in a region or updates it if key already
   * exists.<br>
   * destroy - destroy an entry in a region.<br>
   * get - get an entry in a region.<br>
   */
  public static Long entryOperations;

  /**
   * Parameter to get the minimum numbers of servers required alive in failover
   * tests. This is required while deciding whether a server should be killed or
   * not.
   */
  public static Long minServersRequiredAlive;

  /** Wait period between kill and restart */
  public static Long restartWaitSec;

  /** Milliseconds between running kill task */
  public static Long killInterval;

  /**
   * Amount of time each {@link EntryOperation#startRandomOperations} should
   * last, in seconds.
   */
  public static Long operationTaskTimeSec;

  /**
   * (Vector of Strings) A list of the operations on a region entry that this
   * test allow to pass or fail.
   */
  public static Long successOrFailureEntryOperations;
  
  /**
   * (Vector of Strings) A list of all the operations on a region entry that this
   * test allow to pass or fail in case of multiUser mode.
   */
  public static Long successOrFailureEntryOperationsForMultiUserMode;
  
  /**
   * (Vector of Strings) A list of the username-password for all the users for dummyScheme.
   */
  public static Long userCredentialsForDummyScheme;
  
  /**
   * (Vector of Strings) A list of the username-password for all the users for ldapScheme.
   */
  public static Long userCredentialsForLdapScheme;
  
  /**
   * (Vector of Strings) A list of the keystore path for all the users for pkcsScheme.
   */
  public static Long kestorePathForPkcsScheme;
  
  /**
   * (Vector of Strings) A list of the keystore alias for all the users for pkcsScheme.
   */
  public static Long kestoreAliasForPkcsScheme;

  /**
   * boolean (default:true)
   * <p>
   * check whether the task should pass or fail.
   */

  public static Long isExpectedPass;

  /**
   * Number of expected members in DS
   */
  public static Long expectedMembers;
  
  /**
   * Int mapSize for putAll map
   */
  public static Long putAllMapSize;
  
  /**
   * boolean (default:false)
   * <p>
   * check whether the task should thorw expected exception or not.
   */
  public static Long isExpectedException;

  public static boolean isExpectedPass() {
    Long key = isExpectedPass;
    return tasktab().booleanAt(key, tab().booleanAt(key, true));
  }

  /**
   * boolean (default:false)
   * <p>
   * check whether the task should execute ops transactionally (begin/commit)
   */
  public static Long useTransactions;

  public static boolean useTransactions() {
    Long key = useTransactions;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  public static Vector getSuccessOrFailureEntryOperationsList() {
    Long key = successOrFailureEntryOperations;
    return tasktab().vecAt(key, tab().vecAt(key, null));
  }
  
  public static Vector getUserCredentialsListForDummyScheme() {
    Long key = userCredentialsForDummyScheme;
    return tasktab().vecAt(key, tab().vecAt(key, null));
  }
  
  public static Vector getUserCredentialsListForLdapScheme() {
    Long key = userCredentialsForLdapScheme;
    return tasktab().vecAt(key, tab().vecAt(key, null));
  }
  
  public static Vector getKeyStorePathListForPkcsScheme() {
    Long key = kestorePathForPkcsScheme;
    return tasktab().vecAt(key, tab().vecAt(key, null));
  }
  
  public static Vector getKeyStoreAliasListForPkcsScheme() {
    Long key = kestoreAliasForPkcsScheme;
    return tasktab().vecAt(key, tab().vecAt(key, null));
  }

  public static int getExpectedMembers() {
    Long key = expectedMembers;
    int val = tasktab().intAt(key, tab().intAt(key, 0));
    return val;
  }
  
  public static boolean isExpectedException() {
    Long key = isExpectedException;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

  static {
    setValues(SecurityClientsPrms.class);
  }

  public static void main(String args[]) {
    dumpKeys();
  }

}
