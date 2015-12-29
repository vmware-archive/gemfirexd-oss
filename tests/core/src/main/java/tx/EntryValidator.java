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

import java.util.*;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.Token;

import hydra.*;
import util.*;

/**
 * A class that holds information to aid in entry validation
 * and supplies methods to get the expected behavior (based on 
 * operation and whether or not the tx operation should be 
 * visible in the calling thread) as well as the actual values.
 * A compare() method is also suppled to provide a check of 
 * expected data values / actual data values.
 * 
 * Each instance holds:
 * - regionName
 * - key
 * - operation (for better logging when comparisons fail)
 * - keyExists (boolean)
 * - hasValue (boolean)
 * - cacheValue (Object)
 *
 * In order to support the expected values on a per operation 
 * basis, Maps holding templates of EntryValidators are established
 * at class initialization (one for the checks required if the
 * tx should be visible by the calling thread and another if the
 * tx should NOT be visible.
 *
 * A calling application will get the expected behavior by invoking:
 * getExpected(Operation op, boolean isVisible).
 * - keyExists (whether or not the key should exist for this operation)
 * - hasValue (whether or not the key should contain a value for this operation)
 * - cacheValue (what value we should expect in the VM, this can be DONT_CARE,
 *   OLDVAL or NEWVAL).  The template provides these enumerated types and then
 *   uses them to fill in the appropriate value from the Operation when the 
 *   expected EntryValidator is created.
 *
 * Applications also invoke the getActual(Operation op) method to get the 
 * actual values from the VMs cache:
 * keyExists (uses regionName/key to check region.containsKey())
 * hasValue (uses regionName/key to check region.containsValueForKey())
 * cacheValue (holds the value obtained via DiskRegUtil.getValueInVM()
 *
 * Once both EntryValidator instances have been constructed, the application
 * can invoke the instance method <expected>.compare( <actual> )
 * If the expected values/state are not found, a Test Exception is thrown.
 */
public class EntryValidator {
private static Map visible = null;
private static Map notVisible = null;

// setting for cacheValue if value doesn't need to be checked
// e.g. if Tx is creating a new entry, but hasn't yet been committed.
// We don't expect to find the key or for the key to have a value
// and we don't care what that value might be ...
public static final Integer DONT_CARE = new Integer(-1);
public static final Integer OLDVAL = new Integer(0);
public static final Integer NEWVAL = new Integer(1);

static {
  visible= new HashMap();
  visible.put(Operation.ENTRY_CREATE, new EntryValidator(true, true, EntryValidator.NEWVAL));
  visible.put(Operation.ENTRY_UPDATE, new EntryValidator(true, true, EntryValidator.NEWVAL));
  visible.put(Operation.ENTRY_DESTROY, new EntryValidator(false, false, EntryValidator.NEWVAL));
  visible.put(Operation.ENTRY_LOCAL_DESTROY, new EntryValidator(false, false, EntryValidator.NEWVAL));
  visible.put(Operation.ENTRY_INVAL, new EntryValidator(true, false, EntryValidator.NEWVAL));
  visible.put(Operation.ENTRY_LOCAL_INVAL, new EntryValidator(true, false, EntryValidator.NEWVAL));
  visible.put(Operation.ENTRY_GET_NEW_KEY,  new EntryValidator(true, true, EntryValidator.NEWVAL));
  visible.put(Operation.ENTRY_GET_EXIST_KEY,  new EntryValidator(true, true, EntryValidator.NEWVAL));
  visible.put(Operation.ENTRY_GET_PREV_KEY,  new EntryValidator(true, true, EntryValidator.NEWVAL));

  notVisible = new HashMap();
  notVisible.put(Operation.ENTRY_CREATE, new EntryValidator(false, false, EntryValidator.OLDVAL));
  notVisible.put(Operation.ENTRY_UPDATE, new EntryValidator(true, true, EntryValidator.OLDVAL));
  notVisible.put(Operation.ENTRY_DESTROY, new EntryValidator(true, true, EntryValidator.OLDVAL));
  notVisible.put(Operation.ENTRY_LOCAL_DESTROY, new EntryValidator(true, true, EntryValidator.OLDVAL));
  notVisible.put(Operation.ENTRY_INVAL, new EntryValidator( true, true, EntryValidator.OLDVAL));
  notVisible.put(Operation.ENTRY_LOCAL_INVAL, new EntryValidator(true, true, EntryValidator.OLDVAL));
  notVisible.put(Operation.ENTRY_GET_NEW_KEY,  new EntryValidator(false, false, EntryValidator.OLDVAL));
  notVisible.put(Operation.ENTRY_GET_EXIST_KEY,  new EntryValidator(true, true, EntryValidator.OLDVAL));
  notVisible.put(Operation.ENTRY_GET_PREV_KEY,  new EntryValidator(true, true, EntryValidator.OLDVAL));
}

// private fields
private String regionName = null;
private String key = null;
private Operation op = null;
private boolean keyExists;
private boolean hasValue;
private Object cacheValue;

/** 
 * noArg constructor
 */
public EntryValidator(Operation op) {
  this.op = op;
}

/** Constructor to create a EntryValidator
 * used internally by this class to create templates based
 * on expected values maps.  (See static initializer above)
 *
 *  @param keyExists boolean (true => expect region.containsKey() == true)
 *  @param hasValue boolean (true => expect region.containsValueForKey() == true)
 *  @param cacheValue The expected value of this entry when retrieved from cache
 */
public EntryValidator(boolean keyExists, boolean hasValue, Object cacheValue) {
   this.keyExists = keyExists;
   this.hasValue = hasValue;
   this.cacheValue = cacheValue;
}

/** Constructor to create a EntryValidator
 *
 *  @param regionName The name of the region involved in the operation.
 *  @param key The key in the region involved in the operation.
 *  @param keyExists boolean (true => expect region.containsKey() == true)
 *  @param hasValue boolean (true => expect region.containsValueForKey() == true)
 *  @param cacheValue The expected value of this entry when retrieved from cache
 */
public EntryValidator(Operation op, String regionName, String key, boolean keyExists, boolean hasValue, Object cacheValue) {
   this.op = op;
   this.regionName = regionName;
   this.key = key;
   this.keyExists = keyExists;
   this.hasValue = hasValue;
   this.cacheValue = cacheValue;
}

public String getRegionName() {
   return this.regionName;
}

public void setRegionName(String regionName) {
   this.regionName = regionName;
}

public Object getKey() {
   return this.key;
}

public void setKey(String key) {
   this.key = key;
}

public void setKeyExists(boolean b) {
  this.keyExists = b;
}

public void setHasValue(boolean b) {
  this.hasValue = b;
}

public void setCacheValue(Object value) {
   this.cacheValue = value;
}

public Operation getOperation() {
  return this.op;
}

public void setOperation(Operation op) {
   this.op = op;
}

public static EntryValidator getExpected(Operation op, boolean isVisible) {
  String regionName = op.getRegionName();
  String key =  (String)op.getKey();
  String opName = op.getOpName();
  Object oldVal = op.getOldValue();
  Object newVal = op.getNewValue();

  Log.getLogWriter().info("EntryValidator.getExpected(" + op.toString() + " isVisible = " + isVisible + ")");

  // Get Map based on whether changes will be visible to calling thread
  Map aMap = (isVisible) ? visible : notVisible;
  
  // Get template for expected values and fill in the cacheValue with 
  // old or expected value (or DONT_CARE)
  EntryValidator template = (EntryValidator)aMap.get(opName);
  EntryValidator expected = new EntryValidator(op);
  expected.setRegionName( regionName );
  expected.setKey( key );
  expected.keyExists = template.keyExists;
  expected.hasValue = template.hasValue;
  expected.cacheValue = template.cacheValue;
  
  if (expected.cacheValue == EntryValidator.DONT_CARE) {
     // do nothing
  } else if (expected.cacheValue == EntryValidator.OLDVAL) {
     expected.setCacheValue(oldVal);
  } else if (expected.cacheValue == EntryValidator.NEWVAL) {
      expected.setCacheValue(newVal);
  } else {
      throw new TestException("EntryValidator: invalid value <" + expected.cacheValue + "> given for cacheValue");
  }

  // If expected.cacheValue is INVALID or LOCAL_INVALID, set hasValue = false
  if ((expected.cacheValue == null) ||
      (expected.cacheValue.toString().equals("INVALID")) ||
      (expected.cacheValue.toString().equals("LOCAL_INVALID"))) {
    expected.hasValue = false;
  }
  
  Log.getLogWriter().info("getExpected returns " + expected.toString());
  return expected;
}

/*
 * Get the actual values -- for a transactional thread, the underlying
 * implementation for region apis (like containsKey(), containsValueForKey()
 * and getValueInVM() will operate on transactional state.
 * For non-transactional threads, these same region apis will operate on
 * committed state.
 *
 * getActual() also verifies the operation of region collections (entries())
 * and region.getEntry() and ensures they equal what we returned from 
 * getValueInVM().
 */
public static EntryValidator getActual(Operation op) {
  String regionName = op.getRegionName();
  String key =  (String)op.getKey();
  String opName = op.getOpName();
  Object oldVal = op.getOldValue();
  Object newVal = op.getNewValue();

  Log.getLogWriter().info("EntryValidator.getActual(" + op.toString() + ")");
                                                                                
  Region aRegion = CacheUtil.getCache().getRegion( regionName );
  Object actualValue = DONT_CARE;
  boolean keyExists;
  boolean hasValue;

  // protect ourselves from destroyed region access
  if (aRegion != null) {
    keyExists = aRegion.containsKey( key );
    hasValue = aRegion.containsValueForKey( key );
                                                                                
       actualValue = null;
       Region.Entry entry = aRegion.getEntry(key);
       if (entry != null) {
          actualValue = entry.getValue();
          if (actualValue == null) {  // containsKey null => Token$Invalid
            actualValue = Token.INVALID;
          }
       } 

    // validate collection iterators in sync
    // todo@lhughes - turn back on once invalidate doesn't cause
    // problems with region collections & iteration (BUG 31894)
/*
    validateIterators(aRegion, key, actualValue);
*/

    // test really works off ValueHolder.modVal ...
    if (actualValue instanceof BaseValueHolder) {
      actualValue = ((BaseValueHolder)actualValue).modVal;
    } else {
      Log.getLogWriter().info("WARNING: actual value retrieved from cache is not a ValueHolder, is " + actualValue);
    }

    // If actualValue is INVALID or LOCAL_INVALID set hasValue = false
    if (actualValue != null) {
      if ((actualValue.toString().equals("INVALID")) ||
        (actualValue.toString().equals("LOCAL_INVALID"))) {
        hasValue = false;
      }
    }
  } else {      
    // protect ourselves from accessing a destroyed region
    keyExists = false;
    hasValue = false;
    actualValue = null;
  }
                                                                                
  EntryValidator actualValues = new EntryValidator(op, regionName, key, keyExists, hasValue, actualValue);
                                                                                
  Log.getLogWriter().info("EntryValidator.getActual returning = " + actualValues.toString());
  return actualValues;
}

/**
 *  Compare two validator controls, throw an exception if not a match
 **/
public void compare(EntryValidator vc) {

  if (this.keyExists != vc.keyExists) {
    if (!vc.keyExists && vc.getOperation().getOpName().equalsIgnoreCase(Operation.ENTRY_GET_PREV_KEY)) {
      // In this case, we don't know if the entry had been invalidated or 
      // destroyed before we tried to re-use the key (for the notVisible
      // case).  Even in the 'visible' case, we wouldn't know if another 
      // Cache has the value & returned it!  In any case, return ... we can't
      // check anything more if the key doesn't even exist -- just return.
      return;
    } 
    throw new TestException ("EntryValidator comparison failure: expected " + this.toString() + " but actualValues = " + vc.toString());
  }

  // If an entry was previously invalidated and we are now
  // doing a 'get' -- hasValue will be false.   In this case
  // we actually don't care about hasValue, so we need to 
  // skip this test (and possibly throwing an unwanted exception)
  if (!getInvalidatedEntry()) {
    if (this.hasValue != vc.hasValue) {
      throw new TestException ("EntryValidator comparison failure: expected " + this.toString() + " but actualValues = " + vc.toString());
    }
  }

  if (this.cacheValue != EntryValidator.DONT_CARE) {
    if (this.cacheValue != null) {
      if (!this.cacheValue.equals(vc.cacheValue)) {
        throw new TestException ("EntryValidator comparison failure: expected " + this.toString() + " but actualValues = " + vc.toString());
      }
    } else {  // cacheValue == null ? compare with ==?
      if (vc.cacheValue != null) {
        throw new TestException ("EntryValidator comparison failure: expected " + this.toString() + " but actualValues = " + vc.toString());
      }
    }
  }
}

private boolean getInvalidatedEntry() {
  if (this.op.getOpName().toLowerCase().startsWith("entry-get")) {
    Object oldVal = this.op.getOldValue();
    if ((oldVal != null)  && (oldVal.toString().equals("INVALID"))) {
      Log.getLogWriter().info("FOUND GET ON INVALIDATED ENTRY");
      return true;
    }
  }
  return false;
}

//private static void validateIterators(Region aRegion, Object key, Object value) {
// 
//  boolean found = false;
//  boolean entryValuesCorrect = false;
//
//  Set entries = aRegion.entries(false);
//  for (Iterator it=entries.iterator(); it.hasNext(); ) {
//    Region.Entry entry = (Region.Entry)it.next();
//    if ((entry.getKey().equals(key)) && (entry.getValue().equals(value))) {
//      found = true;
//      break;
//    }
//  }
//  // Note the key may not be found if scope = local & is a remote vm 
//  // and we've done a create entry
//  if (!found && value==null) {
//    found = true;
//  }
//
//  Region.Entry entry = aRegion.getEntry(key);
//  if (entry != null) {
//    if ((entry.getKey().equals(key)) && (entry.getValue().equals(value)) ||
//        (value == null && entry.getKey().equals(key) && entry.isDestroyed())) {
//      entryValuesCorrect = true;
//    }
//  } else {
//      if (value == null) {
//        entryValuesCorrect = true;
//      }
//  }
//  
//  if (!found || !entryValuesCorrect) {
//    throw new TestException("EntryValidator: possible iterator problem, found = " + found + " entryValueCorrect = " + entryValuesCorrect + "for key/value pair (" + key + ", " + value);
//  }
//}

/** Return a string description of the EntryValidator */
public String toString() {
   String aStr = "op: " + op + " regionName: " + regionName + " key:" + key + ", keyExists: " + keyExists + ", hasValue: " + hasValue + ", cacheValue: " + cacheValue;
   return aStr;
}

}
