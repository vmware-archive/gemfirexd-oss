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

package regions.validate;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import hydra.*;

import java.util.*;

/**
*
* A client that supports cache/region definition via cache.xml
* In addition, it catches IllegalStateExceptions from CacheCreate
* and uses the CacheFactory.getAnyInstance() method to get a handle
* on the resulting cache.
*/

public class CacheXmlClient {

  static Cache myCache = null;
  static DistributedSystem ds = null;

  /**
   *  createCacheTask: general task to hook a vm to the distributed cache.
   */
  public static synchronized void createCacheWithXMLTask() {

      // Get the properties for this GF system
      String gemfireName = System.getProperty( GemFirePrms.GEMFIRE_NAME_PROPERTY );
      GemFireDescription gfd = TestConfig.getInstance().getGemFireDescription( gemfireName );
      Properties p = gfd.getDistributedSystemProperties();

      HostDescription hd = gfd.getHostDescription();

      // Add the cache.xml specification to this 
      String cacheXmlFile = TestConfig.tasktab().stringAt(CacheXmlPrms.cacheXmlFile, TestConfig.tab().stringAt(CacheXmlPrms.cacheXmlFile, null));
      p.setProperty(DistributionConfig.CACHE_XML_FILE_NAME, EnvHelper.expandEnvVars(cacheXmlFile, hd));

      ds = DistributedSystem.connect(p);
      myCache = null;

      try {
        myCache = CacheFactory.create( ds );
      } catch( CacheException e ) {
        String s = "Unable to create cache using: " + ds;
        throw new HydraRuntimeException( s, e );
      } catch (IllegalStateException is) {
        String s = "IllegalStateException!!! ";
        //throw new HydraRuntimeException( s, is );
      } finally {
        // Display the regions & attributes we created via cache.xml
        if (myCache == null) {
          myCache = CacheFactory.getAnyInstance();
          Log.getLogWriter().info("CacheFactory.getAnyInstance() returned " + myCache);
        }

        if (myCache != null) {
          Set regions = myCache.rootRegions();
          Log.getLogWriter().info("Cache contains " + regions.size() + " entries " + regions);
          for (Iterator i = regions.iterator(); i.hasNext();) {
            Region r = (Region)i.next();
            String regionName = r.getName();
            RegionAttributes attr = r.getAttributes();
            Scope scope = attr.getScope();
            Log.getLogWriter().info("Region = " + regionName + " with Scope = " + scope);
          }
       } else {
         Log.getLogWriter().info("CacheFactory create returned null, no cache");
       }
     }
  }

  public static void putDataTask() {
    Set regions = myCache.rootRegions();
    for (Iterator i = regions.iterator(); i.hasNext();) {
      Region r = (Region)i.next();
      Log.getLogWriter().info("Putting 10000 entries in " + r.getName());
      for (int count = 0; count < 10000; count ++) {
        String key = "Object_" + count;
        r.put( key, new Integer(count) );
      }
      Log.getLogWriter().info("Put 10000 entries in " + r.getName());
    }
  }

  /**
   *  closeCacheTask: general task to unhook a vm from the distributed cache.
   */
  public static synchronized void closeCacheTask() {
    if ( myCache != null ) {
      myCache.close();
    }
  }
}
