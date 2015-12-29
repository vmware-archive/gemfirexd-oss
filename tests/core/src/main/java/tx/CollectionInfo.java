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
                                                                               
package tx;
                                                                               
//import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.*;
//import com.gemstone.gemfire.internal.cache.*;
import hydra.*;
//import hydra.blackboard.*;
//import diskReg.DiskRegUtil.*;
import java.util.*;
//import java.io.*;
import util.*;
                                                                               
/**
 * A class for holding regionCollection info 
 * (Region.keySet(), Region.values(), Region.entrySet(), Region.subRegions()
 * as well as a specific Entry (retrieved via Region.getEntry() or 
 * a String with the parentRegion name).
 */
                                                                               
public class CollectionInfo {

//-------------------------------------------------------------------------
// Instance variables
//-------------------------------------------------------------------------
Set keys = null;
Collection values = null;
Set entries = null;
Region.Entry entry = null;

Iterator keySetIterator;
Iterator entrySetIterator;
Iterator valuesIterator;

String parentRegionName = null;
Set subRegions = null;

/** 
 *  Constructor
 */
  CollectionInfo(Region aRegion, Operation op) {

    if (aRegion == null) {
      throw new TestException("Unexpected null Region for operation " + op.toString());
    }

    // Entry Operations
    if (op.isEntryOperation()) {
      keys = aRegion.keySet();
      keySetIterator = keys.iterator();

      values = aRegion.values();
      valuesIterator = values.iterator();

      entries = aRegion.entrySet(false);      // no recursion
      entrySetIterator = entries.iterator();

      entry = aRegion.getEntry(op.getKey());
    } else {                                 // Region operation
      // We don't know if this is a create (and the region doesn't yet exist)
      // so just get parentRegionName from opName.getRegionName() and get
      // the region from there.  (We don't allow region operations on the root regions
      // so no need to be concerned with /root giving us a 'null'
      String regionName = op.getRegionName();
      parentRegionName = regionName.substring(0, regionName.lastIndexOf('/'));
      Region parentRegion = CacheUtil.getCache().getRegion(parentRegionName);
      subRegions = parentRegion.subregions(false);    // no recursion
    }
  }

  public String toString() {
    StringBuffer aStr = new StringBuffer();
    if (keys != null) {
      aStr.append("keySet contains " + keys.size() + " elements, Keys = " + keys + "\n");
    }
 
    if (values != null) {
      try {
         aStr.append("Values contains " + values.size() + " elements, Values = " + values + "\n");
      } catch (Exception e) {
         throw new TestException("Caught unexpected Exception " + e + " " + TestHelper.getStackTrace(e));
      }
    }
 
    if (entries != null) {
      aStr.append("entrySet contains " + entries.size() + " elements, Entries = " + entries + "\n");
    }

    if (entry != null) {
      if (entry.isDestroyed()) {
        aStr.append("Entry has been destroyed\n");
      } else {
        aStr.append("Entry = " + entry.toString() + "\n");
      } 
    }

    if (subRegions != null) {
      aStr.append("SubRegions contains " + subRegions.size() + " elements, subRegions = " + subRegions.toString() + "\n");
    }
 
    return aStr.toString();
  }
}
