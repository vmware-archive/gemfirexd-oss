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
import com.gemstone.gemfire.cache.server.CacheServer;
//import com.gemstone.gemfire.*;
import java.util.*;
import java.io.*;
import com.gemstone.gemfire.distributed.internal.*;
import com.gemstone.gemfire.cache.util.*;

/** Connect to a cache as a peer with the config comming from system properties or gemfire.properties... */
public class SSLBridgeServer {

  static DistributedSystem ds = null;
  static Cache cache = null;
  static Region reg = null;

  public static void main( String[] args ) {
    try {
      
      connect();

      createCache();

      CacheServer bridge = cache.addCacheServer();
      bridge.setPort(20222);
      bridge.start();
      while( ! (new File( "stopBridgeServer.touch" )).exists() ) {
        Thread.sleep(100);
      }
      bridge.stop();
      
      ds.disconnect();
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

  public static void performGet() throws Exception {
    Set roots = cache.rootRegions();
    if ( roots.isEmpty() || roots.size() != 1 ) {
      throw new Exception( "expected one root." );
    }
    Object[] rootsArray = roots.toArray();
    reg = (Region) rootsArray[0];
  }
  
}

