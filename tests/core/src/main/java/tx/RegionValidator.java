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
import com.gemstone.gemfire.internal.cache.*;
import hydra.*;
//import hydra.blackboard.*;
//import diskReg.DiskRegUtil.*;
//import java.io.*;
import java.util.*;
import util.*;


/**
 * A class that holds information to aid in region validation
 * and supplies methods to get the expected behavior (based on 
 * operation and whether or not the tx operation should be 
 * visible in the calling thread) as well as the actual values.
 * A compare() method is also suppled to provide a check of 
 * expected data values / actual data values.
 * 
 * Each instance holds:
 * - regionName
 * - operation (for better logging when comparisons fail)
 * - regionExists (boolean)
 * - hasKeys (boolean)
 * - hasValues (boolean)
 *
 * In order to support the expected values on a per operation 
 * basis, Maps holding templates of RegionValidators are established
 * at class initialization (one for the checks required if the
 * tx should be visible by the calling thread and another if the
 * tx should NOT be visible.
 *
 * A calling application will get the expected behavior by invoking:
 * getExpected(Operation op, boolean isVisible).
 * - regionExists (whether or not the region should exist for this operation)
 * - hasKeys (whether or not the region should contain keys)
 * - hasValues (whether or not the keys in this region are expected to have values
 * Applications also invoke the getActual(Operation op) method to get the 
 * actual state from the VMs cache:
 * regionExists (getRegion)
 * hasKeys (region.keySet().size())
 * hasValues (iterate over keys and check containsValueForKey)
 *
 * Once both RegionValidator instances have been constructed, the application
 * can invoke the instance method <expected>.compare( <actual> )
 * If the expected values/state are not found, a Test Exception is thrown.
 */
public class RegionValidator {
private static Map visible = null;
private static Map notVisible = null;

static {
  visible= new HashMap();
  // note: we won't populate the region when we create it
  visible.put(Operation.REGION_CREATE, new RegionValidator(true, false, false));
  visible.put(Operation.REGION_DESTROY, new RegionValidator(false, false, false));
  visible.put(Operation.REGION_LOCAL_DESTROY, new RegionValidator(false, false, false));
  visible.put(Operation.REGION_INVAL, new RegionValidator(true, true, false));
  visible.put(Operation.REGION_LOCAL_INVAL, new RegionValidator(true, true, false));

  notVisible = new HashMap();
  notVisible.put(Operation.REGION_CREATE, new RegionValidator(false, false, false));
  notVisible.put(Operation.REGION_DESTROY, new RegionValidator(true, true, true));
  notVisible.put(Operation.REGION_LOCAL_DESTROY, new RegionValidator(true, true, true));
  notVisible.put(Operation.REGION_INVAL, new RegionValidator(true, true, true));
  notVisible.put(Operation.REGION_LOCAL_INVAL, new RegionValidator(true, true, true));
}

// private fields
private String regionName = null;
private Operation op = null;
private boolean regionExists;
private boolean hasKeys;
private boolean hasValues;

/** 
 * noArg constructor
 */
public RegionValidator(Operation op) {
  this.op = op;
}

/** Constructor to create a RegionValidator
 * used internally by this class to create templates based
 * on expected values maps.  (See static initializer above)
 *
 *  @param regionExists boolean (true => expect region to exist)
 *  @param hasKeys boolean (true => expect region.containsKey() == true)
 *  @param hasValues boolean (true => expect region.containsValueForKey() == true)
 */
public RegionValidator(boolean regionExists, boolean hasKeys, boolean hasValues) {
   this.regionExists = regionExists;
   this.hasKeys= hasKeys;
   this.hasValues = hasValues;
}

/** Constructor to create a RegionValidator
 *
 *  @param op - The Operation being validated
 *  @param regionName The name of the region involved in the operation.
 *  @param regionExists boolean (true => expect region.containsKey() == true)
 *  @param hasKeys booealn (true => expect region to contain keyset)
 *  @param hasValue boolean (true => expect keys in region to return true for .containsValueForKey())
 */
public RegionValidator(Operation op, String regionName, boolean regionExists, boolean hasKeys, boolean hasValues) {
   this.op = op;
   this.regionName = regionName;
   this.regionExists = regionExists;
   this.hasKeys = hasKeys;
   this.hasValues = hasValues;
}

public String getRegionName() {
   return this.regionName;
}

public void setRegionName(String regionName) {
   this.regionName = regionName;
}

/*
 * Note that RegionValidators don't use the isVisible flag,
 * since region operations occur immediately and are not transactional.
 */
public static RegionValidator getExpected(Operation op, boolean isVisible) {
  String regionName = op.getRegionName();
  String opName = op.getOpName();

  Log.getLogWriter().info("RegionValidator.getExpected(" + op.toString() + " isVisible = " + isVisible + ")");

  // Get Map based on whether changes will be visible to calling thread
  Map aMap = (isVisible) ? visible : notVisible;
  
  // Get template for expected values and fill in the cacheValue with 
  // old or expected value (or DONT_CARE)
  RegionValidator template = (RegionValidator)aMap.get(opName);
  RegionValidator expected = new RegionValidator(op);
  expected.setRegionName( regionName );
  expected.regionExists = template.regionExists;
  expected.hasKeys = template.hasKeys;
  expected.hasValues = template.hasValues;
  
  Log.getLogWriter().info("getExpected returns " + expected.toString());
  return expected;
}

public static RegionValidator getActual(Operation op) {
  String regionName = op.getRegionName();
  String opName = op.getOpName();

  Log.getLogWriter().info("RegionValidator.getActual(" + op.toString() + ")");
                                                                                
  // regionExists => getRegion will return null if region does not exist
  Region aRegion = CacheUtil.getCache().getRegion( regionName );
  boolean regionExists = (aRegion == null) ? false: true;
  boolean hasKeys = false;      // default to false in case regionDestroyed
  boolean hasValues = false;    // default to false in case regionDestroyed

  if (regionExists) {
    // hasKeys => if region contains keys
    Set keySet = aRegion.keySet();
    hasKeys = (keySet.size() > 0) ? true : false;

    // hasValues => if region contains keys with values
    // be careful here ... some entries may be invalidated
    int valueCount=0;
    for (Iterator it=keySet.iterator(); it.hasNext();) {
      if (aRegion.containsValueForKey(it.next())) {
        valueCount++;
      }
    }
    hasValues = (valueCount > 0) ? true : false;
  }

  RegionValidator actualValues = new RegionValidator(op, regionName, regionExists, hasKeys, hasValues);
                                                                                
  Log.getLogWriter().info("RegionValidator.getActual returning = " + actualValues.toString());
  return actualValues;
}

/**
 *  Compare two validator controls, throw an exception if not a match
 **/
public void compare(RegionValidator vc) {
  if (this.regionExists != vc.regionExists) {
    throw new TestException ("RegionValidator comparison failure: expected " + this.toString() + " but actualValues = " + vc.toString());
  }

  // For region operations, we can't really compare hasKeys as region
  // operations are not distributed, so we don't know the expected state of the 
  // region in this VM.  For example, if the tx VM (client1) does a
  // region-destroy + region-create (no keys created) + region-inval, there
  // will be no keys in the region. However, clients 2 & 3 will still have
  // their original region and keySets.
/*
  if (this.hasKeys != vc.hasKeys) {
    throw new TestException ("RegionValidator comparison failure: expected " + this.toString() + " but actualValues = " + vc.toString());
  }
*/

  if (this.hasValues != vc.hasValues) {
    // debugging info - if we aren't expecting values, but we see some, what are they?
    if (!this.hasValues) {
      Region aRegion = CacheUtil.getCache().getRegion( regionName );
      Set keySet = aRegion.keySet();
      StringBuffer aStr = new StringBuffer();
      aStr.append("Region contains unexpected key/value pairs\n");
      for (Iterator it=keySet.iterator(); it.hasNext();) {
        String key = (String)it.next();
        if (aRegion.containsValueForKey(key)) {
          Object value = diskReg.DiskRegUtil.getValueInVM(aRegion, key);
          aStr.append("    key:value = " + key + ":" + value + "\n");
        }
      }

      // use new test hook to log what's in the repeatable read set
      Set txSet = new HashSet(((LocalRegion)aRegion).testHookTXKeys());
      if (txSet.size() != 0) {
         aStr.append("For " + aRegion.getFullPath() + " tx set is " + txSet + "\n");
      }
      Log.getLogWriter().info(aStr.toString());
    }
    throw new TestException ("RegionValidator comparison failure: expected " + this.toString() + " but actualValues = " + vc.toString());
  }
}

/** Return a string description of the RegionValidator */
public String toString() {
   String aStr = "op: " + op + " regionName: " + regionName + ", regionExists: " + regionExists + ", hasKeys: " + hasKeys+ ", hasValues: " + hasValues;
   return aStr;
}

}
