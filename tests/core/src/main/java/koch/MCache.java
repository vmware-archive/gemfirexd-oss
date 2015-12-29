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

package koch;

import com.gemstone.gemfire.cache.*; 
import com.gemstone.gemfire.distributed.*; 
import hydra.*;
 
public class MCache { 
    private Cache myCache; 
    private Region myRegion; 
 
    public MCache() { 
        setUpRegion(); 
    } 
 
    /**
       Fetched the cached value with id "id".  After fetching, unload
       the value from the in-VM cache map (leaving it in shared
       memory).  We're taking this unusual step because we don't
       anticipate needing the object again soon and thus can 
       free VM memory that otherwise continue to be occupied by
       the object. 

    */ 
    public Object getItem(String id) { 
      Object theObj = null;
      try {
         theObj = myRegion.get(id); 
         if (theObj == null)  {
            Log.getLogWriter().info("Obj with key: " + id + " was null");
         }
      } catch( Exception e ) {
        throw new HydraRuntimeException( "Problem getting object", e );
      }
      try {
         if ( KochPrms.unloadAfterOperation() ) {
            myRegion.invalidate(id); 
         }
      } catch( CacheException e ) {
        throw new HydraRuntimeException( "Problem unloading object", e );
      }
      return theObj; 
    } 


    /**
       Puts the item known as id into the cache.  Since we don't 
       expect the loading VM to need this object again soon (perhaps 
       ever) in the Koch application, we immediately unload it 
       from the local VM map to save space. 
    */
    public void putItem(String id, Object obj) { 
      try {
          myRegion.put(id, obj);  
      } catch( Exception e ) {
        throw new HydraRuntimeException( "Problem replacing object", e );
      }
      try {
          if ( KochPrms.unloadAfterOperation() ) {
             myRegion.invalidate(id); 
          }
      } catch( CacheException e ) {
        throw new HydraRuntimeException( "Problem unloading object", e );
      }
    } 

    
    /**
       Creates cache regions (maps, basically) with the correct
       attributes for the job at hand.  For the Koch experiments,
       regions will be replicated, meaning that an object added to
       any one cache will automatically be distributed to all other
       caches, remote or local. 
    */
    public void setUpRegion() { 

        // Create the cache
        try { 
          DistributedSystem ds = DistributedConnectionMgr.connect();
          myCache = CacheFactory.create( ds );
        } catch( CacheException e ) {
          throw new HydraRuntimeException( "Problem creating cache", e );
        }
        // Create the root and data regions
        try {
          AttributesFactory factory = new AttributesFactory();
          factory.setScope( Scope.DISTRIBUTED_ACK );
          factory.setDataPolicy( DataPolicy.REPLICATE );
          RegionAttributes myAttributes = factory.createRegionAttributes();
          Region rootRegion = myCache.createVMRegion( "Root", myAttributes );
          myRegion = rootRegion.createSubregion( "MyObjects", myAttributes ); 
        } catch( CacheException e ) {
          throw new HydraRuntimeException( "Problem setting up regions", e );
        }
    }

    /**
     *  Closes the cache.
     */
    public void closeCache() {
      myCache.close();
      myCache = null;
      DistributedConnectionMgr.disconnect();
    }
 
    /**
     *  Task to , create a cache, put something in the cache, and bounce the VM.
     *  Intended for use in a single-threaded client VM, since it closes the
     *  cache each time around.
     */
    public static void cycleTask() { 
 
        // Create a cache
        MCache test = new MCache(); 

        // Make sure everything is working
        test.putItem("stuff", "A bunch of crud");
        Object result = test.getItem("stuff"); 

        // Close the cache
        test.closeCache();

        // Bounce the VM
        try {
          ClientVmMgr.stopAsync( "bouncing", ClientVmMgr.MEAN_KILL,
                                             ClientVmMgr.IMMEDIATE );
        } catch( ClientVmNotFoundException e ) {
          throw new HydraRuntimeException( "Unable to bounce this VM", e );
        }
    } 
} 
