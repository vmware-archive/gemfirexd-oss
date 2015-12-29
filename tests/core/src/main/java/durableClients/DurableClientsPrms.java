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
package durableClients;

import hydra.BasePrms;
import util.TestException;
import com.gemstone.gemfire.cache.InterestResultPolicy;

/**
 * A class to store the Keys for the tests on Durable clients
 * 
 * @author Aneesh Karayil
 * @since 5.2
 * 
 */

public class DurableClientsPrms extends BasePrms {

  /**
   * Parameter used to specify number of regions in the tests.
   */
  public static Long numberOfRegions;

  public static Long numPutThreads;

  public static Long numKeyRangePerThread;

  public static Long regionName;

  public static Long regionRange;

  public static Long entryOperations;

  public static Long precreateLastKeyAtClient;

  public static Long minServersRequiredAlive;

  public static Long restartWaitSec;

  public static Long killInterval;
  
  public static Long putLastKey;
  
  public static Long isExpirationTest;
  
  public static Long remoteMachine1;

  public static Long remoteMachine2;
  
  public static Long registerInterestKeys;

  /** (String) InterestResultPolicy to use while registering interest
   *  Defaults to KEYS_VALUES.
   *
   *  Allowed:  keys_values (or keysValues), keys, none
   */
  public static Long interestResultPolicy;
  public static InterestResultPolicy getInterestResultPolicy() {
    InterestResultPolicy policy;
    String str = tab().stringAt(DurableClientsPrms.interestResultPolicy, "keys_values");
    if (str.equalsIgnoreCase("keys_values") || str.equalsIgnoreCase("keysValues")) {
      policy = InterestResultPolicy.KEYS_VALUES;
    } else if (str.equalsIgnoreCase("keys")) {
      policy = InterestResultPolicy.KEYS;
    } else if (str.equalsIgnoreCase("none")) {
      policy = InterestResultPolicy.NONE;
    } else {
      throw new TestException("Invalid InterestResultPolicy " + str + " configured in test");
    }
    return policy;
  }

  public static Long useClientCache;  // use new ClientCache methods vs. Cache
  
  static {
    setValues(DurableClientsPrms.class);
  }

  public static void main(String args[]) {
    dumpKeys();
  }

}
