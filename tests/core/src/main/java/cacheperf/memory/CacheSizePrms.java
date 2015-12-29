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
package cacheperf.memory;

import com.gemstone.gemfire.cache.query.IndexType;

import hydra.BasePrms;
import hydra.HydraConfigException;

public class CacheSizePrms extends BasePrms {

  public static enum KeyType { testobject, string, integer; }

  /** How many entries to create in the cache */
  public static Long numberOfEntries;
  public static int getNumberOfEntries() {
    return tasktab().intAt(numberOfEntries, tab().intAt(numberOfEntries, 50000));
  }
  
  /** How often to record the heap size */
  public static Long sampleInterval;
  public static int getSampleInterval() {
    return tasktab().intAt(sampleInterval, tab().intAt(sampleInterval, 1000));
  }
  
  /** When to start the trim interval, in number of iterations */
  public static Long trimStart;
  public static int getTrimStart() {
    return tasktab().intAt(trimStart, tab().intAt(trimStart, 10000));
  }
  
  public static Long indexType;
  public static IndexType getIndexType() {
    String result = tasktab().stringAt(indexType, tab().stringAt(indexType, "FUNCTIONAL"));
    
    return result.equalsIgnoreCase("PRIMARY_KEY") ? IndexType.PRIMARY_KEY : IndexType.FUNCTIONAL;
  }
  
  public static Long indexCardinality;
  public static int getIndexCardinality() {
    int result = tasktab().intAt(indexCardinality, tab().intAt(indexCardinality, Integer.MAX_VALUE));
    if(result == -1) {
      result = Integer.MAX_VALUE;
    }
    return result;
  }
  
  public static Long keyType;
  public static KeyType getKeyType() {
    Long key = keyType;
    String val = tasktab().stringAt(key, tab().stringAt(key, "testobject"));
    for (KeyType k : KeyType.values()) {
      if (val.equalsIgnoreCase(k.toString())) {
        return k;
      }
    }
    String s = "Illegal value for " + nameForKey(key) + ": " + val;
    throw new HydraConfigException(s);
  }
  
  public static Long numIndexedValues;
  public static int getNumIndexedValues() {
    return tasktab().intAt(numIndexedValues, tab().intAt(numIndexedValues, 5));
  }
  
  static {
    setValues( CacheSizePrms.class );
  }
  public static void main( String args[] ) {
      dumpKeys();
  }

}
