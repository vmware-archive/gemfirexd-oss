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

package admin;

import hydra.*;
import com.gemstone.gemfire.admin.SystemMembershipListener;
import com.gemstone.gemfire.admin.SystemMemberCacheListener;
/**
*
* A class used to store keys for Admin API Tests
*
*/

public class AdminPrms extends BasePrms {

    /**
     * (int)
     * adminInterface to use (admin or jmx)
     */
    public static Long adminInterface;

    public static final int ADMIN = 0;
    public static final int JMX   = 1;

    public static int getAdminInterface() {
      Long key = adminInterface;
      String val = tab().stringAt( key );
      if ( val.equalsIgnoreCase( "ADMIN" )) {
        return ADMIN;
      } else if (val.equalsIgnoreCase( "JMX" )) {
        return JMX;
      } else {
        throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val);
      }
    }

    /**
     *  (int)
     *  number of root regions to generate
     */
    public static Long numRootRegions;

    /**
     *  (int)
     *  number of subregions to generate for each region
     */
    public static Long numSubRegions;

    /**
     *  (int)
     *  depth of each region tree
     */
    public static Long regionDepth;

    /**
     *  (int) The lock time value for the cache instance.
     */
    public static Long lockTimeout;

    /**
     *  (String)
     *  Type of objects to create, as a fully qualified classname.  The class can
     *  be any supported by {@link objects.ObjectHelper}.  Defaults to
     *  {@link objects.SizedString} with the default size.
     *
     *  @see objects.ObjectHelper
     */
    public static Long objectType;
    public static String getObjectType() {
      Long key = objectType;
      return tasktab().stringAt( key, tab().stringAt( key, "objects.ArrayOfByte" ) );
    }

    /**
     *  (SystemMembershipListener)
     *  Fully qualified pathname of a SystemMembershipListener.  See interface
     *  in {@link com.gemstone.gemfire.admin.SystemMembershipListener}.
     *
     *  Default value is null (we won't install an SystemMembershipListener unless
     *  requested).
     */
    public static Long systemMembershipListener;
    public static SystemMembershipListener getSystemMembershipListener() {
    Long key = systemMembershipListener;
      String val = tasktab().stringAt( key, tab().stringAt( key, null ) );
      try {
        return (SystemMembershipListener)instantiate( key, val );
      } catch( ClassCastException e ) {
        throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val + " does not implement SystemMembershipListener", e );
      }
    }

    /**
     *  (SystemMemberCacheListener)
     *  Fully qualified pathname of a SystemMemberCacheListener.  See interface
     *  in {@link com.gemstone.gemfire.admin.SystemMemberCacheListener}.
     *
     *  Default value is null (we won't install an SystemMemberCacheListener unless
     *  requested).
     */
    public static Long systemMemberCacheListener;
    public static SystemMemberCacheListener getSystemMemberCacheListener() {
    Long key = systemMemberCacheListener;
      String val = tasktab().stringAt( key, tab().stringAt( key, null ) );
      try {
        return (SystemMemberCacheListener)instantiate( key, val );
      } catch( ClassCastException e ) {
        throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val + " does not implement SystemMemberCacheListener", e );
      }
    }

  /**
   * (boolean) 
   *
   * Does this test destroy regions?  If not, the test will fail if it
   * attemps to {@linkplain
   * com.gemstone.gemfire.admin.SystemMemberCache#getRegion administer
   * a region and finds that one does not exist.  By default, we
   * assume that regions are not destroyed.
   */
  public static Long areRegionsDestroyed;
  public static boolean areRegionsDestroyed() {
    Long key = areRegionsDestroyed;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

 /**
   * (boolean) 
   *
   * Does this test create an AdminDistributedSystem in the same VM as a
   * Distributed System.
   * By default assume that they are not in the same VM.
   */
  public static Long adminInDsVm;
  public static boolean adminInDsVm() {
    Long key = adminInDsVm;
    return tasktab().booleanAt(key, tab().booleanAt(key, false));
  }

 /**
  * (String)
  * Host on which to start CacheServer
  */
 public static Long cacheServerHost;

 /**
  * (String)
  * Remote Command for use when starting CacheServer on remote host
  */
 public static Long cacheServerRemoteCommand;
  
  /**
  * (int)
  * Max number of cache servers for tests
  */
 public static Long maxCacheServers;
 

 /* (boolean) 
  * Whether to define a CacheLoader in a remote CacheServer Region.
  * Returns boolean value of TestPrms.defineCacheLoaderRemote.
  * Defaults to false.
  */
 public static Long defineCacheLoaderRemote;
 public static boolean getDefineCacheLoaderRemote() {
   Long key = defineCacheLoaderRemote;
   return (tasktab().booleanAt(key, tab().booleanAt(key, false)));
 }

  //------------------------------------------------------------------------
  // Utility methods
  //------------------------------------------------------------------------

  private static Object instantiate( Long key, String classname ) {
    if ( classname == null ) {
      return null;
    }
    try {
      Class cls = Class.forName( classname );
      return cls.newInstance();
    } catch( Exception e ) {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key
 ) + ": cannot instantiate " + classname, e );
    }
  }

    static {
        setValues( AdminPrms.class );
    }
    public static void main( String args[] ) {
        dumpKeys();
    }
}
