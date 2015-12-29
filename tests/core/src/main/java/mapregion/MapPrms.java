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
/*
 * MapPrms.java
 *
 * Created on September 19, 2005, 8:09 PM
 */

package mapregion;

/**
 *
 * @author  prafulla
 * @since 5.0
 */
import java.util.Vector;

import hydra.BasePrms;
import hydra.HydraVector;
import hydra.TestConfig;

public class MapPrms extends BasePrms
{
  /**
   * entryOperationName is for the entry operation name, for instance put, putAll, clear, remove, etc.
   */
  public static Long entryOperationName;

  /**
   * regionOperationName is for the region operation name, for instance clear, invalidateRegion, destroyRegion, etc.
   */
  public static Long regionOperationName;

  /**
   * closeCache during GII 
   */
  
  public static Long closeCacheDuringGii;
  public static boolean getCloseCacheDuringGii (){
    Long key = closeCacheDuringGii;
    return tasktab().booleanAt( key, tab().booleanAt( key, false) );
  }

  /**
   * regionName parameter gives region name. Make sure that this parameter is same as other region name parameters given in test
   * configuration file.
   */
  public static Long regionName;

  /**
   * timeForPutOperation (in milliseconds) parameter gives the time for which thread would perform put operation on a region.
   */
  public static Long timeForPutOperation;

  /**
   * timeForPutAllOperation (in milliseconds) parameter gives the time for which thread would insert values inside a map and 
   * that map would be passed as a parameter to putAll operation on a region.
   */
  public static Long timeForPutAllOperation;

  /**
   * timeForRemoveOperation (in milliseconds) parameter gives the time for which thread would perform remove operation on a region.
   */
  public static Long timeForRemoveOperation;

  /**
   * timeForInvalidateOperation (in milliseconds) parameter gives the time for which thread would perform invalidate operation on a region.
   */
  public static Long timeForInvalidateOperation;
  
  /**
   * timeForDestroyOperation (in milliseconds) parameter gives the time for which thread would perform destroy operation on a region.
   */
  public static Long timeForDestroyOperation;
  
  /**
   * populateCache - (boolean) parameter gives whether to populate cache before performing any operation on it.
   */
  public static Long populateCache;
  
  /**
   * timeForDestroyOperation (in milliseconds) parameter gives the time for which cache should be intially populated.
   */
  public static Long timeForPopulateCache;
  
  /**
   * (String)
   * Space seperated names of regions to be created in a single VM
   */
  public static Long regionNames;

  public static String[] getRegionNames()
  {
    Long key = regionNames;
    Vector names = TestConfig.tab().vecAt(MapPrms.regionNames,
      new HydraVector());
    String[] strArr = new String[names.size()];
    for (int i = 0; i < names.size(); i++) {
      String name = (String)names.elementAt(i);
      strArr[i] = name;
    }
    return strArr;
  }
  
  /**
   * (String)
   * Region name on which operations are to be performed
   */
  public static Long regionForOps;
  
  /**
   * (int) The number of VMs to stop at a time.
   */
  public static Long numVMsToStop;  

  /**
   * This parameter gives type of object used in the test. This is same as CachePerfPrms.objectType.
   * But just not to use the cacheperf parameters this is defined here for this test.
   * Default if objects.SizedString
   */
  public static Long objectType;
  public static String getObjectType() {
    Long key = objectType;
    return tasktab().stringAt( key, tab().stringAt( key, "objects.SizedString" ) );
  }
  
  /**
   * (String)
   * Space seperated query strings.
   */
  public static Long queryStrs;

  public static String[] getQueryStrs()
  {
    Long key = queryStrs;
    Vector queries = TestConfig.tab().vecAt(MapPrms.queryStrs,
        new HydraVector());
    String[] strArr = new String[queries.size()];
    for (int i = 0; i < queries.size(); i++) {
      strArr[i] = (String)queries.elementAt(i);
    }
    return strArr;
  }
  
  /**
   * Register and execute CQs
   */
  public static Long registerAndExecuteCQs;

  public static boolean getRegisterAndExecuteCQs()
  {
    Long key = registerAndExecuteCQs;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }
  
  /**
   * (int) number of edge clients
   */
  public static Long numEdges;
  
  public static int getNumEdges()
  {
    Long key = numEdges;
    return tasktab().intAt(key, tab().intAt(key, 1));
  }
  
  /**
   * (boolean) temporary flag to set if executeWithInitialResults is to be performed
   */
  public static Long doExecuteWithInitialResultsCQ;
  public static boolean getDoExecuteWithInitialResultsCQ()
  {
    Long key = doExecuteWithInitialResultsCQ;
    return tasktab().booleanAt(key, tab().booleanAt(key, true));
  }
  
  /**
   * (long) time in millis to wait for CQ Listener events before validations.
   * Defaults to 20 seconds
   */
  
  public static Long timeToWaitForEvents;
  public static long getTimeToWaitForEvents()
  {
    Long key = timeToWaitForEvents;
    return tasktab().longAt(key, tab().longAt(key, 20000));
  }
  
  /**
   * (int) maximum number of keys / entries to be put or created into the region. 
   * Defaults to 100000
   */
  
  public static Long maxPositiveKeys;
  public static int getMaxPositiveKeys()
  {
    Long key = maxPositiveKeys;
    return tasktab().intAt(key, tab().intAt(key, 100000));
  }
  
  public static Long maxNagetiveKeys;
  public static int getMaxNagetiveKeys()
  {
    Long key = maxNagetiveKeys;
    return tasktab().intAt(key, tab().intAt(key, 100000));
  }

  /** (int) The lower size of the region that will trigger the test to choose its operations from 
   *        lowerThresholdOperations
   */ 
  public static Long lowerThreshold;
  
  /**(Vector of Strings) A list of the operations on a region entry that this
   *                     test is allowed to do when the region size falls below lowerThreshold.
   */
  public static Long lowerThresholdOperations;

  /** (int) The upper size of the  region that will trigger the test
   *         chooose its operations from the upperThresholdOperations.
   */
  public static Long upperThreshold;

  /** (Vector of Strings) A list of operations on a region that this
   *                       test is allowed to do when the region size exceeds the upperThreshold.
   */

  public static Long upperThresholdOperations;


  /** Creates a new instance of MapPrms */
  public MapPrms() {
  }

  // ================================================================================
  static {
    BasePrms.setValues(MapPrms.class);
  }// end of static block    

}// end of MapPrms
