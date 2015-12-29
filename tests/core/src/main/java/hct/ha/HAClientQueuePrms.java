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

package hct.ha;

import hydra.BasePrms;

/**
 * 
 * A class to store keys related to client queue HA.
 * 
 * @author Suyog Bhokare
 * @author Dinesh Patel
 * @author Girish Thombare
 */

public class HAClientQueuePrms extends BasePrms
{

  public static Long numPutThreads;

  public static Long numKeyRangePerThread;

  /**
   * Paremeter to put last key so that listener collects the appropriate
   * statistics.
   */
  public static Long putLastKey;

  /**
   * Parameter to precreate last_key at client. This is done to receive an
   * invalidate event at client when the feeder does that last put. At this
   * point, validations can be signalled to proceed.
   */
  public static Long precreateLastKeyAtClient;
  
  /**
   * (Vector of Strings) A list of the operations on a region entry that this
   * test is allowed to do. Can be one or more of:<br>
   * put - creates a new key/val in a region or updates it if key already
   * exists.<br>
   * invalidate - invalidate an entry in a region.<br>
   * destroy - destroy an entry in a region.<br>
   */
  public static Long entryOperations;
  
  /**
   *  Parameter used to create region name in the tests.
   */
  public static Long regionName;
  
  /**
   * Parameter used to specify number of regions in the tests.
   */
  public static Long numberOfRegions;
  
  /**
   * Parameter used to pick the region randomly to perform operations.
   */
  public static Long regionRange;
  
  

  /**
   * Parameter to get the minimum numbers of servers required alive in failover
   * tests. This is required while deciding whether a server should be killed or
   * not.
   */
  public static Long minServersRequiredAlive;
  
  public static Long maxClientsCanKill;
  
  /**
   * An integer corresponding to the entry-operation to be performed. This can
   * be from 1 to 7 corresponding to create, update, invalidate, destroy,
   * registerInterest,unregisterInterest and killing client respectively.
   */
  public static Long opCode;

  /**
   * Amount of time each {@link Feeder#feederTask} should last, in seconds.
   */
  public static Long feederTaskTimeSec;
  
  /**
   * Sets the client conflation
   */
  public static Long clientConflation;

  /**
   * Whether or not to allow ConcurrentMap operations (putIfAbsent, replace, remove)
   */
  public static Long allowConcurrentMapOps;

  /**
   * Whether or not to delay the start of message dispatcher.
   */
  public static Long delayDispatcherStart;

  /**
   * Set delay time for the start of message dispatcher.
   */
  public static Long delayDispatcherStartMilis;

  public static String getClientConflation() {
    Long key = clientConflation;
    return tasktab().stringAt(key, tab().stringAt(key, "default"));
  }

  static {
    setValues(HAClientQueuePrms.class);
  }

  public static void main(String args[])
  {
    dumpKeys();
  }
}
