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
package parReg;

/** Class to implement a key to use to put into a partitioned region.
 *  Any key of this class always hashes to the same value, to quickly
 *  fill a parititioned region.
 */
public class FillKey implements java.io.Serializable {
    
public Object key;

/** Always return the same hashCode to make buckets fill up.
 */
public int hashCode() {
   return 1;
}

/** Return true if the argument is a FillKey whose key field
 *  equals this key
 */
public boolean equals(Object anObj) {
   if (anObj instanceof FillKey) {
      return key.equals(((FillKey)anObj).key);
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
