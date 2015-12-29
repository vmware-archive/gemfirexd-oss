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

package ssl;

import com.gemstone.gemfire.SystemFailure;
import com.gemstone.gemfire.distributed.*;
import com.gemstone.gemfire.cache.*;
//import com.gemstone.gemfire.*;
import java.util.*;
import com.gemstone.gemfire.distributed.internal.*;

/** Connect to a cache as a peer with the config comming from system properties or gemfire.properties... */
public class DefaultCacheParticipant {

  static DistributedSystem ds = null;
  static Cache cache = null;
  static Region reg = null;
  
  public static void main( String[] args ) {
    try {
      
      connect();
      
      createCache();
      
      getRegion();
      
      giveMeFifty();
      
      ds.disconnect();
      System.out.println( "### client complete ###" );
      System.exit( 0 );
    } 
    catch (VirtualMachineError e) {
      SystemFailure.initiateFailure(e);
      throw e;
    }
    catch ( Throwable t ) {
      t.printStackTrace();
      System.exit( 1 );
    }
  }

  public static void connect( ) throws Exception {
    ds = DistributedSystem.connect( new Properties() );
    InternalDistributedSystem ids = (InternalDistributedSystem) ds;
    DistributionConfig dc = ids.getConfig();
    System.out.println( "DistributionConfig : " + dc );
    if ( ! ds.isConnected() ) {
      throw new Exception( "DistributedSystem is not connected." );
    } 
  }

  public static void createCache() throws Exception {
    cache = CacheFactory.create( ds );
  }

  public static void getRegion() throws Exception {
    Set roots = cache.rootRegions();
    if ( roots.isEmpty() || roots.size() != 1 ) {
      throw new Exception( "expected one root." );
    }
    Object[] rootsArray = roots.toArray();
    reg = (Region) rootsArray[0];
  }

  public static void giveMeFifty() throws Exception {
    Random rand = new Random();
    for( int i = 0; i < 50; i++ ) {
      Object key = new Integer( rand.nextInt() % 5 );
      Object value = new Integer( rand.nextInt() );
      reg.put( key, value );
      Thread.sleep( 100 );  // total ops should take 5 seconds.
    }
  }
  
}

