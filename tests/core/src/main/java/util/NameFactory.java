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
package util;

//import hydra.Log;
import java.util.*;
import com.gemstone.gemfire.cache.*;

/**
 * A factory class that creates unique names for {@link Region}s and
 * arbitrary objects.  A {@link NameBB} is used to ensure that the
 * names are unique.
 *
 * @author Lynn Gallinat
 *
 * @since 2.0
 */
public class NameFactory {

public static final String OBJECT_NAME_PREFIX = NameFactoryVersion.getObjectNamePrefix();
public static final String REGION_NAME_PREFIX = NameFactoryVersion.getRegionNamePrefix();
public static final String LISTENER_NAME_PREFIX = NameFactoryVersion.getListenerNamePrefix();

public static String getNextListenerName() {
   long counter = NameBB.getBB().incrementAndRead(NameBB.LISTENER_NAME_COUNTER);
   String name = LISTENER_NAME_PREFIX + counter;
   return name;
}

public static String getNextRegionName() {
   long counter = NameBB.getBB().incrementAndRead(NameBB.REGION_NAME_COUNTER);
   String name = REGION_NAME_PREFIX + counter;
   return name;
}

public static String getObjectNameForCounter(long counter) {
   String name = OBJECT_NAME_PREFIX + counter;
   return name;
}

public static String getNextPositiveObjectName() {
   long counter = NameBB.getBB().incrementAndRead(NameBB.POSITIVE_NAME_COUNTER);
   return getObjectNameForCounter(counter);
}

public static String getNextPositiveObjectNameInLimit(int limit) {
   long counter = NameBB.getBB().incrementAndRead(NameBB.POSITIVE_NAME_COUNTER);
   return getObjectNameForCounter((counter % limit));
}

public static String getNextNegativeObjectName() { 
   long counter = NameBB.getBB().decrementAndRead(NameBB.NEGATIVE_NAME_COUNTER);
   return getObjectNameForCounter(counter);
}

public static long getPositiveNameCounter() {
   long positiveCounter = NameBB.getBB().read(NameBB.POSITIVE_NAME_COUNTER);
   return positiveCounter;
}

public static long getNegativeNameCounter() {
   long negativeCounter = NameBB.getBB().read(NameBB.NEGATIVE_NAME_COUNTER);
   return negativeCounter;
}

public static long getTotalNameCounter() {
   long positiveCounter = NameBB.getBB().read(NameBB.POSITIVE_NAME_COUNTER);
   long negativeCounter = NameBB.getBB().read(NameBB.NEGATIVE_NAME_COUNTER);
   long numNames = positiveCounter + Math.abs(negativeCounter);
   return numNames;
}

public static String getKeySetDiscrepancies(Region aRegion, Set keySet) {
   long positiveCounter = NameBB.getBB().read(NameBB.POSITIVE_NAME_COUNTER);
   long negativeCounter = NameBB.getBB().read(NameBB.NEGATIVE_NAME_COUNTER);
   return getKeySetDiscrepancies(aRegion, keySet, (int)positiveCounter, (int)negativeCounter, new HashMap());
}

public static String getKeySetDiscrepancies(Region aRegion, Set keySet, Map ignoreMap) {
   long positiveCounter = NameBB.getBB().read(NameBB.POSITIVE_NAME_COUNTER);
   long negativeCounter = NameBB.getBB().read(NameBB.NEGATIVE_NAME_COUNTER);
   return getKeySetDiscrepancies(aRegion, keySet, (int)positiveCounter, (int)negativeCounter, ignoreMap);
}

public static String getKeySetDiscrepancies(Region aRegion, Set keySet, 
                                             int positiveCounter, int negativeCounter) {
   return getKeySetDiscrepancies(aRegion, keySet, positiveCounter, negativeCounter, new HashMap());
}

public static String getKeySetDiscrepancies(Region aRegion, Set keySet, 
                                             int positiveCounter, int negativeCounter,
                                             Map ignoreMap) {
   StringBuffer aStr = new StringBuffer();
   boolean[] foundPositiveName = new boolean[positiveCounter + 1];
   boolean[] foundNegativeName = new boolean[0 - negativeCounter + 1];
   Iterator it = keySet.iterator();
   if (!it.hasNext() && (keySet.size() != 0)) {
      aStr.append(" keySet size is " + keySet.size() + ", but keySet.iterator().hasNext() is " + it.hasNext());
   }
   while (it.hasNext()) {
      Object name = it.next();
      if (ignoreMap.get(name) != null) // ignore this name
         continue;
      int nameCounter = (new Integer(((String)name).substring(OBJECT_NAME_PREFIX.length()))).intValue();
      int arrayIndex = Math.abs(nameCounter);
      int arrayLimit = (nameCounter > 0) ? positiveCounter : Math.abs(negativeCounter);
      boolean inRange = arrayIndex <= arrayLimit;
      if (!inRange) {
         try {
            aStr.append("Name " + name + " with value " + TestHelper.toString(aRegion.get(name)) +
                 " was found in the key set of " + TestHelper.regionToString(aRegion, false) + ", but it was not expected\n");
         } catch (Exception e) {
            throw new TestException(TestHelper.getStackTrace(e));
         }
      } else {
         if (nameCounter > 0)
            foundPositiveName[arrayIndex] = true;
         else 
            foundNegativeName[arrayIndex] = true;
      }
   } 
   for (int i = 1; i <= positiveCounter; i++) {
      if (!foundPositiveName[i])
         aStr.append("Name " + getObjectNameForCounter(i) + " was expected in key set, but was not found\n");
   }
   for (int i = 1; i <= 0 - negativeCounter; i++) {
      if (!foundNegativeName[i])
         aStr.append("Name " + getObjectNameForCounter(0 - i) + " was expected in key set, but was not found\n");
   }
   return aStr.toString();
}

public static long getCounterForName(Object name) {
   if (!(name instanceof String)) {
      throw new TestException("Unable to get counter for name (expected String) " + TestHelper.toString(name));
   }
   String nameStr = new String((String)name);
   nameStr = nameStr.trim();
   if (nameStr.indexOf(OBJECT_NAME_PREFIX) >= 0) {
      return (new Long(nameStr.substring(OBJECT_NAME_PREFIX.length()))).longValue();
   } else if (nameStr.indexOf(REGION_NAME_PREFIX) >= 0) {
      return (new Long(nameStr.substring(REGION_NAME_PREFIX.length()))).longValue();
   } else {
      throw new TestException("Unable to get counter for name " + name);
   }
}

static TreeSet getOrderedKeySet(Set keySet) {
   class NameComparator implements Comparator {
      public int compare(Object o1, Object o2) {
         Long aLong1 = (new Long(((String)o1).substring(OBJECT_NAME_PREFIX.length())));
         Long aLong2 = (new Long(((String)o2).substring(OBJECT_NAME_PREFIX.length())));
         return aLong1.compareTo(aLong2);
      }
      public boolean equals(Object anObj) {
         return anObj.equals(this);
      }
   }
   TreeSet aSet = new TreeSet(new NameComparator()); 
   aSet.addAll(keySet);
   return aSet;
}

}
