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
package rebalance;

import util.NameFactory;

/** Class to implement a key to use to put into a partitioned region.
 *  Any key of this class has a limited range of hash values, to create
 *  uneven buckets ina PR.
 */
public class HashKey implements java.io.Serializable {
    
// A key from NameFactory
public Object key;

// every instance hashes to one of this many values only
// since this is public, it can be changed by a test if desired
public static int hashLimit = 3; 

/** Return a restricted hashCode.
 */
public int hashCode() {
   long counter = NameFactory.getCounterForName(key);
   int hc = (int)(counter) % hashLimit;
   return hc;
}

/** Return true if the argument is a HashKey whose key field
 *  equals this key
 */
public boolean equals(Object anObj) {
   if (anObj instanceof HashKey) {
      return key.equals(((HashKey)anObj).key);
   } else {
      return false;
   }
}

/** Return a String representation of this instance
 */
public String toString() {
   return super.toString() + " with key " + key;
}

}
