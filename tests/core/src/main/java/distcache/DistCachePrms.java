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

package distcache;

//import com.gemstone.gemfire.cache.*;
import hydra.*;

/**
 *
 *  A class used to store keys for test configuration settings.
 *
 */

public class DistCachePrms extends BasePrms {

  //////////////////////////////////////////////////////////////////////////////
  // Cache vendor
  //////////////////////////////////////////////////////////////////////////////

  public static final String DEFAULT_CACHE_VENDOR = "gemfire";

  public static final int GEMFIRE    = 0;
  public static final int HASHMAP    = 1;

  /**
   * Vendor of the distributed cache.  Default is {@link #DEFAULT_CACHE_VENDOR}.
   */
  public static Long cacheVendor;
  public static int getCacheVendor() {
    Long key = cacheVendor;
    String val = tab().stringAt(key, DEFAULT_CACHE_VENDOR);
    if ( val.equalsIgnoreCase( "GEMFIRE" ) ) {
      return GEMFIRE;
    } else if ( val.equalsIgnoreCase( "HashMap" ) ) {
      return HASHMAP;
    } else {
      throw new HydraConfigException( "Illegal value for " + nameForKey( key ) + ": " + val );
    }
  }

  //////////////////////////////////////////////////////////////////////////////
  // Utility methods
  //////////////////////////////////////////////////////////////////////////////

  static {
      setValues( DistCachePrms.class );
  }

  public static void main( String args[] ) {
      dumpKeys();
  }
}
