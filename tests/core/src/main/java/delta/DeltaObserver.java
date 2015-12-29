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
package delta; 

import hydra.Log;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DeltaObserver {

// keep a list of keys whose values had toDelta/fromDelta called on them
protected static List toDeltaKeys = new ArrayList();
protected static List fromDeltaKeys = new ArrayList();
protected static List hasDeltaKeys = new ArrayList();
protected static Map toDeltaIdNumbers = new HashMap();
protected static Map currentReferences = new HashMap();

/** Keep a record of keys whose value had toDelta called on them.
 *  Used for validation later.
 */
public static synchronized void addToDeltaKey(Object key) {
   toDeltaKeys.add(key);
   Log.getLogWriter().info("Saved " + key + " because its value had toDelta called on it");
}

/** Keep a record of keys whose value had fromDelta called on them.
 *  Used for validation later.
 */
public static synchronized void addFromDeltaKey(Object key) {
   fromDeltaKeys.add(key);
   Log.getLogWriter().info("Saved " + key + " because its value had fromDelta called on it");
}

/** Keep a record of keys whose value had hasDelta called on them.
 *  Used for validation later.
 */
public static synchronized void addHasDeltaKey(Object key) {
   hasDeltaKeys.add(key);
   Log.getLogWriter().info("Saved " + key + " because its value had hasDelta called on it");
}

/** Given a delta object, save it's object reference to a map for later
 *  validation for cloning. The reference is saved in a map where the
 *  key is the region key for the DeltaObject, and the value is an Object[]
 *  where [0] is an  object reference obtained by the identityHashCode of
 *  the object (Integer), and [1] is a string representation of anObj.
 * @param anObj The DeltaObject whose object reference is saved
 */
public static synchronized void addReference(DeltaObject anObj) {
  Object[] anArr = new Object[] {new Integer(System.identityHashCode(anObj)), anObj.toStringFull(),
                                                           anObj.id};
  currentReferences.put(anObj.extra, anArr);
  Log.getLogWriter().info("Saved object reference for key " + anObj.extra + ", value " + anObj.toStringFull() + ", id " + anObj.id);
}

/** Null the given object from the current references.
 * 
 * @param key  The key to null. 
 */
public static synchronized void nullReference(Object key) {
  currentReferences.put(key, null);
}

/** Keep a record of toDelta id numbers received at the fromDelta calls.
 *  This is called from fromDelta to ensure that we do not call fromDelta more than once
 *  per vm with the given toDelta id number (ie the same toDelta stream).
*   This is called from fromDelta after retrieving the toDelta id number from the stream.
 *
 *  @param idNumber The id number encoded into the toDelta call, which is now being
 *                  retrieved in a fromDelta call.
 *  @param key The key corresponding to the DeltaObject for the fromDelta call containing
 *             the idNumber.
 *  @returns null if this idNumber has not been saved in this vm previously, or
 *           the key associated with the previous idNumber.
 */
public static synchronized Object addToDeltaIdNumber(long idNumber, Object key) {
   final int sizeThreshold = 50000;
   Long idNum = new Long(idNumber);
   Object previousKey = toDeltaIdNumbers.get(idNum);
   if (toDeltaIdNumbers.size() > sizeThreshold) {
      // this grows without bound in a test; reinitialize it now and then to prevent the vm from growing
      toDeltaIdNumbers = new HashMap();
   }
   toDeltaIdNumbers.put(idNum, key);
   Log.getLogWriter().info("Saved toDelta id number " + idNumber + "; previous key is " + previousKey +
       " number of saved id numbers: " + toDeltaIdNumbers.size());
   return previousKey;
}

/** Return the list of keys whose value had fromDelta called on them.
 */
public static List getFromDeltaKeys() {
   return fromDeltaKeys;
}

/** Return the list of keys whose value had toDelta called on them.
 */
public static List getToDeltaKeys() {
   return toDeltaKeys;
}

/** Return the list of keys whose value had hasDelta called on them.
 */
public static List getHasDeltaKeys() {
   return hasDeltaKeys;
}

/** Return the Map of current region references (to be used for cloning validation).
 */
public static synchronized Map getReferences() {
  return new HashMap(currentReferences);
}

/** Clear the saved lists.
 */
public static synchronized void clear() {
   Log.getLogWriter().info("Clearing DeltaObserver saved data");
   toDeltaKeys = new ArrayList();
   fromDeltaKeys = new ArrayList();
   hasDeltaKeys = new ArrayList();
   currentReferences = new HashMap();
}

}
