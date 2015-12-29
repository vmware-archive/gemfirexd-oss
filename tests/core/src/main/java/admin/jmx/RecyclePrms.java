
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

package admin.jmx;

import java.util.Vector;

import com.gemstone.gemfire.cache.InterestResultPolicy;
import hydra.BasePrms;
import hydra.CachePrms;
import hydra.ClientPrms;
import hydra.GatewayHubPrms;
import hydra.HydraConfigException;
import hydra.HydraInternalException;
import hydra.HydraVector;
import hydra.MasterController;
import hydra.RegionPrms;
import hydra.RemoteTestModule;
import hydra.TestConfig;
import util.TestHelper;

/**
 * Hydra parameters for Admin tests that recycle servers
 * 
 */
public class RecyclePrms 
	extends BasePrms 
{
  /** Sleep secs before recycling, default is 120 secs */
   public static Long sleepBetweenCycles;

   /** Number of Regions */
   public static Long numberOfRootRegions;
   
   /** Number of entities in region */
   public static Long numberOfEntitiesInRegion;
   
   /** Region depth */
   public static Long regionDepth;

  /**
    * (boolean(s))
    * Whether the names for regions to be created are specified in conf file or not.  Defaults to false.
    */
   public static Long isRegionNamesDefined;


  /**
   * (String)
   * Type of InterestResultPolicy to use in registerInterest call.
   * Defaults to keysValues.
   */
  public static Long interestResultPolicy;
  public static InterestResultPolicy getInterestResultPolicy() {
    Long key = interestResultPolicy;
    String val = tasktab().stringAt(key, tab().stringAt(key, "keysValues"));
    return TestHelper.getInterestResultPolicy(val);
  }

   /** Recycle mode is a kill or disconnect? */
   public static Long recycleModeIsAHardKill;

   /**
    * An entry in {@link CachePrms#names} giving the cache configuration.
    */
   public static Long edgeCacheConfig;
   
   /**
    * Returns the value of {@link #edgeCacheConfig}, or null if it is not set.
    * @throws HydraConfigException if not found in {@link CachePrms#names}.
    */
   public static String getEdgeCacheConfig() 
   {
     return getString(edgeCacheConfig);
   }

   /**
    * An entry in {@link RegionPrms#names} giving the region configuration.
    */
   public static Long edgeRegionConfig;

   /**
    * Returns the value of {@link #edgeRegionConfig}, or null if it is not set.
    * @throws HydraConfigException if not found in {@link RegionPrms#names}.
    */
   public static String getEdgeRegionConfig() 
   {
     return getString(edgeRegionConfig);
   }
   
   public static boolean isRecycleModeHardKill()
   {
     Long key = recycleModeIsAHardKill;
     return tasktab().booleanAt(key, tab().booleanAt(key, false));
   }
   
   public static boolean isRegionNamesDefined()
   {
     Long key = isRegionNamesDefined;
     return tasktab().booleanAt(key, tab().booleanAt(key, false));
   }

   /**
    * Returns the string at the given key, checking task attributes as well as
    * the regular configuration hashtable.
    */
   public static String getString(Long key) 
   {
	 String val = tasktab().stringAt(key, tab().stringAt(key, null));
     if (val == null) 
     {
       return null;
     } else 
     {
       Object description;
       if (key == edgeCacheConfig) 
       {
           description = TestConfig.getInstance().getCacheDescription(val);
       } else if (key == edgeRegionConfig) 
       {
           description = TestConfig.getInstance().getRegionDescription(val);
       } else 
       {
         throw new HydraInternalException("Unknown key: " + nameForKey(key));
       }
       
       if (description == null) 
       {
         String s = "Description for configuration name \"" + nameForKey(key)
                  + "\" not found: " + val;
         throw new HydraConfigException(s);
       }
       
       return val;
     }
   }

   /**
    * Same as {@link #generateNames(String,int,int)} but comma-separates the
    * names if asked.
    */
   public static String generateNames(String prefix, int n, int m, boolean useComma) 
   {
     String v = "";
     for ( int i = m; i < m+n; i++ ) {
       v += "\"" + prefix + i +"\"";
       if (i < m+n-1) {
         if (useComma) v += ",";
         v += " ";
       }
     }

     MasterController.log.config("RecyclePrms:generateNames["+v+"]");
     return v;
   }
     
   static 
   {
       setValues( RecyclePrms.class );
   }
}
