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
package cacheLoader.hc;

//import com.gemstone.gemfire.LogWriter;

import hydra.*; 
//import hydra.blackboard.*;
//import util.*;

public class BridgeParms extends BasePrms { 


    //---------------------------------------------------------------------
    // Constants and Default Values


    //---------------------------------------------------------------------
    // Test-specific parameters


    /**
     *  Number of trim iterations done in each task.
     */
    public static Long trimIterations;
    protected static int getTrimIterations() {
      Long key = trimIterations;
      return tasktab().intAt(key, tab().intAt(key));
    }

    /**
     *  Number of work iterations done in each task.
     */
    public static Long workIterations;
    protected static int getWorkIterations() {
      Long key = workIterations;
      return tasktab().intAt(key, tab().intAt(key));
    }

    /**
     *  Name of CacheLoader for use in CacheServer.
     *  Defaults to DBCacheLoader.
     */
    public static Long serverLoaderClassname;
    protected static String getServerLoaderClassname() {
      Long key = serverLoaderClassname;
      return (tasktab().stringAt(key, tab().stringAt(key, "cacheLoader.hc.DBCacheLoader")));
    }

    /**
     *  Indicates whether detailed messages should be logged
     */
    public static Long logDetails;
    protected static boolean getLogDetails() {
      Long key = logDetails;
      return tasktab().booleanAt(key, tab().booleanAt(key, false));
    }

    /**
     *  Indicates whether values retrieved should be validated
     */
    public static Long validate;
    protected static boolean getValidate() {
      Long key = validate;
      return tasktab().booleanAt(key, tab().booleanAt(key, false));
    }

    static { 
	BasePrms.setValues(BridgeParms.class); 
    }

} 

  
