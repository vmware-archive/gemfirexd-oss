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
package newWan;

import hydra.BasePrms;
import hydra.HydraConfigException;

public class WANOperationsClientPrms extends BasePrms {
  /** 
   * (boolean) true if unique key per thread are used for doEntryOperation.
   */
  public static Long useUniqueKeyPerThread; 
  
  /** (String) The number of new keys to be included in the the map argument to putAll. 
   *           This can be one of:
   *              - an int specifying a particular number
   *              - "useThreshold", which will fill the map with a random number of new 
   *                 keys such that a putAll will not exceed ParRegPrms.upperThreshold
   *                 or put more than ParRegPrms.numPutAllMaxNewKeys new keys
   *                 or put less than ParRegPrms-numPutAllMinNewKeys new keys.
   */
  public static Long numPutAllNewKeys;  

  /** (String) The maximum number of new keys to be included in the the map
   *           argument to putAll when using the "useThreshold" seting for
   *           ParRegPrms.numPutAllNewKeys.  Defaults to no set maximum.
   */
  public static Long numPutAllMaxNewKeys;

  /** (String) The minimum number of new keys to be included in the the map
   *           argument to putAll when using the "useThreshold" seting for
   *           ParRegPrms.numPutAllNewKeys.  Defaults to 1.
   */
  public static Long numPutAllMinNewKeys;

  /** (int) The number of existing keys (to be included in the the map argument to putAll. 
   *        If this many keys do not exist, then use as many as possible.
   */
  public static Long numPutAllExistingKeys; 
  
  /** (Vector of Strings) A list of the operations on a Gateway sender that this 
   *        test is allowed to do. Allowed combinations are "start", "stop", "pause" and "resume".
   */
  public static Long senderOperations;
  
  /** 
   * (int) seconds to wait for silence listener  
   * .        
   */
  public static Long waitForSilenceListenerSec;
  
  /**
   * (String)
   * ClientName of member which needs to be recycle. 
   */
  public static Long clientNamePrefixForHA;
  public static String getClientNamePrefixForHA() {
    Long key = clientNamePrefixForHA;
    return tasktab().stringAt( key, tab().stringAt( key, null ) );
  }
  
  /**
   * (boolean)
   * if true then enables killing vms. Default to false.
   */
  public static Long enableFailover;
  public static boolean enableFailover() {
    Long key = enableFailover;
    return tasktab().booleanAt( key, tab().booleanAt( key, false) );
  }
  
  /**
   * (String)
   * task termination method. Values are numOperations, numEventResolved. Default to numOperations.
   */
  public static Long taskTerminationMethod;
  public static final int NUM_OPERATIONS       = 0;
  public static final int NUM_EVENT_RESOLVED   = 1;
  public static int getTaskTerminationMethod() {
    Long key = taskTerminationMethod;
    String method = tasktab().stringAt( key, tab().stringAt( key, "") );    
    if(method.equals("numEventResolved")){
      return NUM_EVENT_RESOLVED;
    }else if (method.equals("numOperations")){
      return NUM_OPERATIONS;  
    }else{
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + method);
    }
  }
  
  /**
   * (int)
   * Task terminator count. Once this taskTerminatorCount is for taskTerminationMethod,
   * the task scheduling gets stopped.
   */
  public static Long taskTerminatorThreshold;
  public static int getTaskTerminatorThreshold() {
    Long key = taskTerminatorThreshold;
    return tasktab().intAt( key, tab().intAt( key, 1000 ) );
  }
  
  /**
   *  (int)
   *  The maximum number of keys used in the test, where <code>maxKeys</code>
   *  is the maximum key.  Required parameter.  Not intended for use
   *  with oneof or range.
   */
  public static Long maxKeys;
  public static int getMaxKeys() {
    Long key = maxKeys;
    int val = tasktab().intAt( key, tab().intAt( key, -1 ));
    int keyAllocation = getKeyAllocation();
    if ( val == -1 && (keyAllocation == OWN_KEYS_WRAP 
                      || keyAllocation == WAN_KEYS_WRAP 
                      || keyAllocation == NEXT_KEYS_WRAP) ) {      
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val 
                                      + " when keyAllocation is " + keyAllocationToString(keyAllocation));
    }    
    return val;
  }
  
  private static final String DEFAULT_KEY_ALLOCATION = "ownKeys";
  
  /**
   *  (String)
   *  The method to use when allocating keys to threads for cache operations.
   *  Options are "ownKeys", "ownKeysWrap", "wanKeysWrap", "nextKeys", "nextKeysWrap
   *  Defaults to {@link #DEFAULT_KEY_ALLOCATION}.
   */
  public static Long keyAllocation;
  public static final int OWN_KEYS              = 0; //keys are group by thr id
  public static final int OWN_KEYS_WRAP         = 1; //keys are group by thr id, wrap when maxKeys is reached
  public static final int WAN_KEYS_WRAP         = 2; //keys are group by wan id, wrap when maxKeys is reached  
  public static final int NEXT_KEYS             = 3; //Next sequential available keys
  public static final int NEXT_KEYS_WRAP        = 4; //Next sequential available keys, wrap when maxKeys is reached  
  public static int getKeyAllocation() {
    Long key = keyAllocation;
    String val = tasktab().stringAt( key, tab().stringAt( key, DEFAULT_KEY_ALLOCATION ) );
    if ( val.equalsIgnoreCase( "ownKeys" ) ) {
      return OWN_KEYS;
    } else if ( val.equalsIgnoreCase( "ownKeysWrap" ) ) {
      return OWN_KEYS_WRAP;
    } else if ( val.equalsIgnoreCase( "wanKeysWrap" ) ) {
      return WAN_KEYS_WRAP;
    } else if (val.equalsIgnoreCase("nextKeys")){
      return NEXT_KEYS;
    } else if (val.equalsIgnoreCase("nextKeysWrap")){
      return NEXT_KEYS_WRAP;
    } else {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
    }
  }
  
  public static String keyAllocationToString(int keyAllocation){
    if(keyAllocation == OWN_KEYS){
      return "ownKeys";
    }else if( keyAllocation == OWN_KEYS_WRAP){
      return "ownKeysWrap";
    }else if (keyAllocation == WAN_KEYS_WRAP){
      return "wanKeysWrap";
    }else if (keyAllocation == NEXT_KEYS){
      return "nextKeys";
    }else if (keyAllocation == NEXT_KEYS_WRAP){
      return "nextKeysWrap";
    }else{
      return null;
    }
  }
  
  static {
    setValues(WANOperationsClientPrms.class);
  }

  public static void main(String args[]) {
    dumpKeys();
  }
  
}
