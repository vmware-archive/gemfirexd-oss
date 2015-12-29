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

import util.*;
import java.util.*;
import hydra.*;

/**
 * A class that holds information about an operation on a region or entry, 
 * which indicates the current transactional state.
 */
public class Operation implements java.io.Serializable {

// available operations
public final static String ENTRY_CREATE         = "entry-create";
                           // Creates a new entry by using a never-before-used key
public final static String ENTRY_UPDATE         = "entry-update";
                           // Modifies the value associated with an existing key
public final static String ENTRY_PUTALL         = "putAll";
public final static String ENTRY_DESTROY        = "entry-destroy";
public final static String ENTRY_LOCAL_DESTROY  = "entry-localDestroy";
public final static String ENTRY_INVAL          = "entry-inval";
public final static String ENTRY_LOCAL_INVAL    = "entry-localInval";
public final static String ENTRY_GET_NEW_KEY    = "entry-getWithNewKey";
                           // Gets with a never-before-used key; may invoke a cache loader
public final static String ENTRY_GET_EXIST_KEY  = "entry-getWithExistingKey";
                           // Gets with an existing key
public final static String ENTRY_GET_PREV_KEY   = "entry-getWithPreviousKey";
                           // Gets with a key previously used in the test; may or
                           // may not get a value; may or may not invoke a cache loader
public final static String ENTRY_GET_ENTRY_EXIST_KEY     = "entry-getEntryWithExistingKey";
public final static String ENTRY_GETALL                  = "getAll";
public final static String ENTRY_CONTAINS_KEY            = "entry-containsKey";
public final static String ENTRY_CONTAINS_VALUE_FOR_KEY  = "entry-containsValueForKey";
public final static String ENTRY_CONTAINS_VALUE          = "entry-containsValue";
public final static String ENTRY_REMOVE                  = "entry-remove";

public final static String REGION_CREATE        = "region-create";
public final static String REGION_DESTROY       = "region-destroy";
public final static String REGION_LOCAL_DESTROY = "region-localDestroy";
public final static String REGION_INVAL         = "region-inval";
public final static String REGION_LOCAL_INVAL   = "region-localInval";
public final static String REGION_KEYS          = "region-keys";
public final static String REGION_KEY_SET       = "region-keySet";
public final static String REGION_VALUES        = "region-values";
public final static String REGION_ENTRIES       = "region-entries";
public final static String REGION_ENTRY_SET     = "region-entrySet";

public final static String CACHE_CLOSE          = "cache-close";

// for a list of repeatable read ops, see bugmail for 36688
public static List repeatableReadOps = Arrays.asList(new String[] {  
       ENTRY_GET_EXIST_KEY, 
       ENTRY_GET_ENTRY_EXIST_KEY, 
       ENTRY_CONTAINS_KEY, 
       ENTRY_CONTAINS_VALUE_FOR_KEY,
       ENTRY_CONTAINS_VALUE,

       ENTRY_CREATE,        // when throws EntryExistsException
       ENTRY_INVAL,         // when throws EntryNotFoundException
       ENTRY_DESTROY,       // when throws EntryNotFoundException
       ENTRY_REMOVE,        // when it returns null

       // these are repeatable reads when they are iterated or are iterated
       // implicitly as in toArray
       REGION_KEYS,
       REGION_KEY_SET,
       REGION_VALUES,
       REGION_ENTRIES,
       REGION_ENTRY_SET,
});

// for a list of repeatable read ops, see bugmail for 36688
public static List nonrepeatableReadOps = Arrays.asList(new String[] {  
       // these are NOT repeatable reads if they are not iterated
       REGION_KEYS,
       REGION_KEY_SET,
       REGION_VALUES,
       REGION_ENTRIES,
       REGION_ENTRY_SET,
});

// edgeClients have a restricted set of repeatable read ops
public static List clientRepeatableReadOps = Arrays.asList(new String[] {  
       ENTRY_GET_EXIST_KEY, 
       ENTRY_GET_ENTRY_EXIST_KEY, 
       ENTRY_CONTAINS_KEY, 
       ENTRY_CONTAINS_VALUE_FOR_KEY,
       ENTRY_CONTAINS_VALUE,

       ENTRY_CREATE,        // when throws EntryExistsException
       ENTRY_INVAL,         // when throws EntryNotFoundException
       ENTRY_DESTROY,       // when throws EntryNotFoundException
       ENTRY_REMOVE,        // when it returns null

       // these are repeatable reads when they are iterated or are iterated
       // implicitly as in toArray
       REGION_KEYS,
       REGION_KEY_SET,
       REGION_VALUES,
       REGION_ENTRIES,
       REGION_ENTRY_SET,
});

// private fields
private List opList = Arrays.asList(new String[] {  
        ENTRY_CREATE, ENTRY_UPDATE, 
        ENTRY_DESTROY, 
        ENTRY_INVAL, 
        ENTRY_GET_NEW_KEY, ENTRY_GET_EXIST_KEY, ENTRY_GET_PREV_KEY,
        ENTRY_GET_ENTRY_EXIST_KEY, ENTRY_CONTAINS_KEY, ENTRY_CONTAINS_VALUE_FOR_KEY,
        ENTRY_CONTAINS_VALUE, ENTRY_REMOVE, ENTRY_PUTALL,

        REGION_CREATE, REGION_DESTROY, REGION_LOCAL_DESTROY, REGION_INVAL, REGION_LOCAL_INVAL,
        REGION_KEYS, REGION_KEY_SET, REGION_VALUES, REGION_ENTRIES, REGION_ENTRY_SET,

        CACHE_CLOSE});

private String regionName = null;    // the region name for this operation
private Object key = null;           // the key for this operation
private String opName = null;        // which operation (listed above)
private Object oldValue = null;      // the old value before the operation (if applicable)
private Object newValue = null;      // the new value resulting from this operation (if applicable)
private boolean repeatableRead = false; // whether the op was used as a repeatable read


/** Constructor to create an operation:
 *  By default, repeatable read is false.
 *
 *  @param regionName The name of the region involved in the operation.
 *  @param key The key in the region involved in the operation.
 *  @param opName Which operation was done.
 *  @param oldValue The old value (before the operation), if applicable.
 *  @param newValue The new value (after the operation), if applicable.
 */
public Operation(String regionName, Object key, String opName, Object oldValue, Object newValue) {
   if (!opList.contains(opName))
      throw new TestException("Unknown opName " + opName);
   this.regionName = regionName;
   this.key = key;
   this.opName = opName;
   this.oldValue = oldValue;
   this.newValue = newValue;
}

// ======================================================================== 
// getter methods

/** Get the region name for the operation */
public String getRegionName() {
   return regionName;
}

/** Get the key for the operation */
public Object getKey() {
   return key;
}

/** Get the operation name */
public String getOpName() {
   return opName;
}
   
/** Get the old value */
public Object getOldValue() {
   return oldValue;
}

/** Get the new value */
public Object getNewValue() {
   return newValue;
}
   
// ======================================================================== 
// setter methods

/** Set the operation name */
public void setOpName(String opName) {
   this.opName = opName;
}
   
/** Set the old value */
public void setOldValue(Object oldValue) {
   this.oldValue = oldValue;
}
   
/** Set the new value */
public void setNewValue(Object newValue) {
   this.newValue = newValue;
}
   
/** Set the repeatable read value */
public void setRepeatableRead(boolean abool) {
   this.repeatableRead = abool;
}

// ======================================================================== 
// testing methods

/** Returns true if the operation uses the same region and key as anOp.
 *
 *  @param anOp The operation used to test its region and key.
 *
 */
public boolean usesSameRegionAndKey(Operation anOp) {
   if ((key == null) && (regionName == null)) {
      return (anOp.getRegionName() == null) && (anOp.getKey() == null);
   } else if (key == null) {
      return (regionName.equals(anOp.getRegionName())) && (anOp.getKey() == null);
   } else if (regionName == null) {
      return (anOp.getRegionName() == null) && (key.equals(anOp.getKey()));
   } else {
      return regionName.equals(anOp.getRegionName()) && key.equals(anOp.getKey());
   }
}
   
/** Returns true if the operation uses the same region as anOp.
 *
 *  @param anOp The operation used to test its region.
 *
 */
public boolean usesSameRegion(Operation anOp) {
   if (regionName == null) {
      return (anOp.getRegionName() == null);
   } else {
      return regionName.equals(anOp.getRegionName());
   }
}
   
/** Returns true if the operation is a write operation, false otherwise */
public boolean isRepeatableRead() {
   return this.repeatableRead;
}
   
/** Returns true if the operation is an operation that involves the entire
 *  region keys and/or values as a set.
 */
public boolean isRegionSetOperation() {
   return (this.opName.equals(REGION_KEYS) ||
           this.opName.equals(REGION_KEY_SET) ||
           this.opName.equals(REGION_VALUES) ||
           this.opName.equals(REGION_ENTRIES) ||
           this.opName.equals(REGION_ENTRY_SET));
}

/** Returns true if the operation is a write operation, false otherwise */
public boolean isWriteOperation() {
   return (this.isInvalidate() || this.isEntryDestroy() || this.isEntryUpdate() ||
           this.isEntryCreate() || this.isGetWithLoad()); 
}
   
/** Returns true if the operation is a get operation, false otherwise. */
public boolean isGetOperation() {
   return (this.opName.toLowerCase().indexOf("-get") > 0);
}
   
/** Returns true if the operation is a get and oldValue was null or invalid, false otherwise. */
public boolean isGetWithLoad() {
   boolean loaderInvoked = false;
   if (this.isGetOperation()) {
      loaderInvoked = (((this.getOldValue() == null) || this.isPreviouslyInvalid()) &&
                        (this.getNewValue() != null));
   }
   return loaderInvoked;
}
   
/** Returns true if the operation is a region operation, false otherwise. */
public boolean isRegionOperation() {
   return this.opName.toLowerCase().startsWith("region");
}
   
/** Returns true if the operation is an entry operation, false otherwise. */
public boolean isEntryOperation() {
   return opName.toLowerCase().startsWith("entry");
}

/** Returns true if the operation is an entry operation, false otherwise. */
public boolean isInvalidate() {
   return opName.toLowerCase().indexOf("inval") > 0;
}

/** Returns true if the operation's oldValue is any form of invalid */
public boolean isPreviouslyInvalid() {
   if (oldValue == null)
      return false;
   String oldValueStr = oldValue.toString();
   return (oldValueStr.equals("LOCAL_INVALID") || oldValueStr.equals("INVALID"));
}

/** Returns true if the operation is a local operation, false otherwise. */
public boolean isLocalOperation() {
  if (this.opName.toLowerCase().indexOf("local") > 0) {
    return true;
  }
  return false;
}

/** Returns true if the operation is a regionCreate operation, false otherwise. 
 */
public boolean isRegionCreate() {
  if (this.isRegionOperation()) {
    if (this.opName.toLowerCase().indexOf("create") > 0) {
      return true;
    }
  }
  return false;
}

/** Returns true if the operation is a regionDestroy or regionLocalDestroy
 *  operation, false otherwise. 
 */
public boolean isRegionDestroy() {
  if (this.isRegionOperation()) {
    if (this.opName.toLowerCase().indexOf("destroy") > 0) {
      return true;
    }
  }
  return false;
}

/** Returns true if the operation is a regionInval or regionLocalInval
 *  operation, false otherwise. 
 */
public boolean isRegionInvalidate() {
  if (this.isRegionOperation()) {
    if (this.opName.toLowerCase().indexOf("inval") > 0) {
      return true;
    }
  }
  return false;
}

/** Returns true if the operation is an entryInval or entryLocalInval
 *  operation, false otherwise. 
 */
public boolean isEntryUpdate() {
  if (this.isEntryOperation()) {
    if (this.opName.toLowerCase().indexOf("update") > 0) {
      return true;
    }
  }
  return false;
}

/** Returns true if the operation is an entryInval or entryLocalInval
 *  operation, false otherwise. 
 */
public boolean isEntryInvalidate() {
  if (this.isEntryOperation()) {
    if (this.opName.toLowerCase().indexOf("inval") > 0) {
      return true;
    }
  }
  return false;
}

/** Returns true if the operation is an entryCreate
 *  operation, false otherwise. 
 */
public boolean isEntryCreate() {
  if (this.isEntryOperation()) {
    if (this.opName.toLowerCase().indexOf("create") > 0) {
      return true;
    }
  }
  return false;
}

/** Returns true if the operation is an entryDestroy or entryLocalDestroy
 *  operation, false otherwise. 
 */
public boolean isEntryDestroy() {
  if (this.isEntryOperation()) {
    if (this.opName.toLowerCase().indexOf("destroy") > 0) {
      return true;
    }
  }
  return false;
}

/** Returns true if the operation is the result of two invalidates (which
 *  implies the 2nd invalidate is a noop), false otherwise.
 */
public boolean isDoubleInvalidate() {
   if (this.isInvalidate()) {
      if (oldValue == null)
         return false;
      String oldValueStr = oldValue.toString();
      if (oldValueStr.equals("LOCAL_INVALID") || oldValueStr.equals("INVALID")) {
         return true;
      }
   }
   return false;
}

/** Returns true if the operation is a any type of get (Existing, Prev, New)
 *  operation, false otherwise. 
 */
public boolean isEntryGet() {
  if (this.isEntryOperation()) {
    if (this.opName.toLowerCase().indexOf("get") > 0) {
      return true;
    }
  }
  return false;
}

/** Returns true if the argument equals the receiver, false otherwise
 *  Differs from equals() in that the original operation may have an oldValue or newValue
 *  of INVALID which must appear equal to the event's oldValue/newValue == null.
 *  This allows comparisons of operations re-generated from Events to be compared
 *  to the original operation list.
 *
 *  @param anObj an Operation re-generated from an Event
 */
public boolean equalsEventOp(Object anObj) {
   if (anObj instanceof Operation) {
      Operation op = (Operation)anObj;
      boolean result =
             ((regionName == null) ? (op.getRegionName() == null) : (regionName.equals(op.getRegionName()))) &&
             ((key == null) ? (op.getKey() == null) : (key.equals(op.getKey()))) &&
             ((opName == null) ? (op.getOpName() == null) : (opName.equals(op.getOpName()))) &&
             ((oldValue == null || oldValue.toString().equals("INVALID")) ? (op.getOldValue() == null) : (oldValue.equals(op.getOldValue()))) &&
             ((newValue == null || newValue.toString().equals("INVALID")) ? (op.getNewValue() == null) : (newValue.equals(op.getNewValue())));
      return result;
   } else {
      return false;
   }
}
   
// ======================================================================== 
// overridden methods

/** Returns true if the argument equals the receiver, false otherwise */
public boolean equals(Object anObj) {
   if (anObj instanceof Operation) {
      Operation op = (Operation)anObj;
      boolean result =
             ((regionName == null) ? (op.getRegionName() == null) : (regionName.equals(op.getRegionName()))) &&
             ((key == null) ? (op.getKey() == null) : (key.equals(op.getKey()))) &&
             ((opName == null) ? (op.getOpName() == null) : (opName.equals(op.getOpName()))) &&
             ((oldValue == null) ? (op.getOldValue() == null) : (oldValue.equals(op.getOldValue()))) &&
             ((newValue == null) ? (op.getNewValue() == null) : (newValue.equals(op.getNewValue())));
      return result;
   } else {
      return false;
   }
}
         
/** Return a string description of the operation */
public String toString() {
   String aStr = "Operation " + opName + " on " + regionName;
   if (key != null) // operations on a region will not have a key
      aStr = aStr + ":" + key;
   aStr = aStr + ", old value = " + oldValue + ", new value = " + newValue;
   aStr = aStr + " repeatableRead: " + repeatableRead;
   return aStr;
}

// ======================================================================== 
// affected-by methods

public boolean entryOpAffectedBy(Operation op) {
  // if 'this' is an entry operation and incoming op is an entry operation
  // affected => true iff region/key are identical
  if (this.isEntryOperation() && op.isEntryOperation()) {
    if ((this.regionName.equals(op.getRegionName())) && (this.key.equals(op.getKey()))) {
      Log.getLogWriter().fine("Operation.affectedBy(): entryOperations on same key " + this.toString() + " " + op.toString());
      return true;
    }
  }
  return false;
}

public boolean affectedBy(Operation op) {
  // if 'this' is an entry operation and incoming op is an entry operation
  // affected => true iff region/key are identical
  if (this.isEntryOperation() && op.isEntryOperation()) {
    if ((this.regionName.equals(op.getRegionName())) && (this.key.equals(op.getKey()))) {
      Log.getLogWriter().fine("Operation.affectedBy(): entryOperations on same key " + this.toString() + " " + op.toString());
      return true;
    }
  }

  // if 'this' is an entry operation and the incoming op, o is a region operation or
  // if 'this' is an region operation and the incoming op, o is a region operation
  // affected => true iff the incoming operation's region is the same or higher in the tree hierarchy

  if (op.isRegionOperation()) {
    if (this.regionName.startsWith(op.getRegionName())) {
      Log.getLogWriter().fine("Operation.affectedBy(): entry or region Operation affected by regionOperation " + this.toString() + " " + op.toString());
      return true;
    }
  }

  // If we had a region operation (like invalidate) and our
  // 2nd operation was an entry operation (like create or update) ...
  // we want to KEEP that subsequent entry operation but we can't
  // verify the initial region operation (because now there are keys
  // with values in the region) so we will mark is as affected and
  // it won't be collapsed into the list.
  if (this.isRegionOperation() && op.isEntryOperation()) {
    if (op.getRegionName().startsWith(this.regionName)) {
      Log.getLogWriter().fine("Operation.affectedBy: regionOperation affected by entryOperation" + this.toString() + " " + op.toString());
      return true;
    }
  }
   return false;
}

}
