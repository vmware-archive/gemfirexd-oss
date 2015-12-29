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

import hydra.BasePrms;

/**
 * Configuration parameters for specifying how random data should be
 * created by {@link RandomValues}.
 *
 * @author Lynn Gallinat
 *
 * @since 1.0
 */
public class RandomValuesPrms extends BasePrms {

/** (Vector) A Vector of Strings, where each string the fully
 *  qualified class name of a class to use to create elements for
 *  collections. Strings in this vector may also include the special
 *  string "null" for null values or "this" to specify another special
 *  object, usually the collection itself to use circular references */
public static Long objectType;

public static Long keyType;
public static Long valueType;
public static Long setElementType;

/** (int) The size of random strings */
public static Long stringSize;

/** (int) For types in elementType that are collections, create
 *  elements of it with this depth.  A depth of 0 means that the
 *  collection is empty. A depth of 1 means that it contains other
 *  objects as its elements, while a depth of 2 means that those other
 *  objects contain more objects, etc. */
public static Long objectDepth;

/** (int) For types in elementType which have a constructor that take
 *  a length or size argument.  Also used to populate elements with
 *  this many objects. */
public static Long elementSize;

/** (int) The percentage (int between 0 and 100) of immutable JDK
 *  instances which will be created with border case values (ie
 *  min/max) */
public static Long borderCasePercentage;

/** (int) Limits random Integers and Longs to be between 0 and this
 *  value.  Useful for getting lots of collisions on maps. */
public static Long randomValueRange;

/** (String) A String specifying the constructor to use for creating various
 *  classes.
 *     Maps can be one of:
 *  <pre>
 *        "default"                      example: new HashMap()
 *        "initialCapacity"              example: new HashMap(initialCapacity)
 *        "initialCapacity_loadFactor"   example: new HashMap(initialCapacity, loadFactor)
 *  </pre>
 *     ArrayLists can be one of:
 *  <pre>
 *        "default"                             example: new ArrayList()
 *        "initialCapacity"                     example: new ArrayList(initialCapacity)
 *  </pre>
 *     Vectors can be one of:
 *  <pre>
 *        "default"                             example: new Vector()
 *        "initialCapacity"                     example: new Vector(initialCapacity)
 *        "initialCapacity_capacityIncrement"   example: new Vector(initialCapacity, capacityIncrement)
 *  </pre>
 *     LinkedLists can be one of:
 *  <pre>
 *        "default"                             example: new Vector()
 *  </pre>
 *     Sets can be one of:
 *  <pre>
 *        "default"                      example: new HashSet()
 *        "initialCapacity"              example: new HashSet(initialCapacity)
 *        "initialCapacity_loadFactor"   example: new HashSet(initialCapacity, loadFactor)
 *  </pre>
 */
public static Long ArrayList_constructor;
public static Long HashMap_constructor;
public static Long Hashtable_constructor;
public static Long LinkedList_constructor;
public static Long Vector_constructor;

/** (int) Used as initialCapacity argument for the constructors */
public static Long ArrayList_initialCapacity;
public static Long HashMap_initialCapacity;
public static Long Hashtable_initialCapacity;
public static Long Vector_initialCapacity;

/** (int) Used as loadFactor argument for the constructor */
public static Long HashMap_loadFactor;
public static Long Hashtable_loadFactor;

/** (int) Used as capacityIncrement argument for the constructor */
public static Long Vector_capacityIncrement;

/** (boolean) Specifies whether to create this object with a synchronized wrapper */
public static Long ArrayList_syncWrapper;
public static Long HashMap_syncWrapper;
public static Long Hashtable_syncWrapper;
public static Long LinkedList_syncWrapper;
public static Long Vector_syncWrapper;

// ================================================================================
static {
   BasePrms.setValues(RandomValuesPrms.class);
}

}
