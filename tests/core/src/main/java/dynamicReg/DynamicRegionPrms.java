
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

package dynamicReg;

/**
 * Hydra parameters for the DynamicRegionTest.
 * Contains parameters for configuring both the cache server and
 * the edge client VMs.
 *
 * @author jfarris
 * @since 4.3
 */
public class DynamicRegionPrms extends hydra.BasePrms {


   /*  Dynamic region test parameters  */

    /** (int) The number of region entries
     */
    public static Long maxEntries;


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


   /** (Vector of Strings) A list of the operations on a region entry that this test is allowed to do.
    *  Can be one or more of:
    *     add - add a new key/value to a region.
    *     invalidate - invalidate an entry in a region.
    *     localInvalidate - local invalidate of an entry in a region.
    *     destroy - destroy an entry in a region.
    *     localDestroy - local destroy of an entry in a region.
    *     update - update an entry in a region with a new value.
    *     read - read the value of an entry.
    */
   public static Long entryOperations;

   /** (String) how clients are to register interest
    *  Can be one of: singleKey, rangeOfKeys or allKeys
    */
   public static Long clientInterest;

   static {
       setValues( DynamicRegionPrms.class );
   }




    /** (Vector of Strings) A list of the operations on a region OR region entries
     *  that this test is allowed to do.  Can be one or more of:
     *     entry-create - create a new key/value in a region.
     *     entry-update - update an entry in a region with a new value.
     *     entry-destroy - destroy an entry in a region.
     *     entry-localDestroy - local destroy of an entry in a region.
     *     entry-inval - invalidate an entry in a region.
     *     entry-localInval - local invalidate of an entry in a region.
     *     entry-get - get the value of an entry.
     *     
     *     region-create - create a new region.
     *     region-destroy - destroy a region.
     *     region-localDestroy - locally destroy a region.
     *     region-inval - invalidate a region.
     *     region-localInval - locally invalidate a region.
     *     
     *     cache-close - close the cache.
     */
    public static Long operations;

    /** (int) The number of operations to do when DynamicRegionUtil.doOperations() is called.
     */
    public static Long numOps;


    /** 
     *  (int) 
     *  Number of entries to create (per region) initially
     */
    public static Long maxKeys;


}
