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
package com.gemstone.gemfire.internal.cache;

import java.util.*;
import junit.framework.TestCase;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.distributed.DistributedSystem;

/**
 * Confirm that bug 34583 is fixed. Cause of bug is recursion is
 * entries iterator that causes stack overflow.
 * @author darrel
 */
public class Bug34583JUnitTest extends TestCase {
  
  public Bug34583JUnitTest() {
  }
  
  public void setup() {
  }
  
  public void tearDown() {
  }
  
  
  
  public void testBunchOfInvalidEntries() throws Exception {
    Properties props = new Properties();
    DistributedSystem ds = DistributedSystem.connect(props);
    try {
      AttributesFactory factory = new AttributesFactory();
      Cache cache = null;
      cache = CacheFactory.create(ds);
     
      Region r = cache.createRegion("testRegion", factory.create());
      final int ENTRY_COUNT = 25000;
      {
        for (int i=1; i <= ENTRY_COUNT; i++) {
          r.put("key" + i, "value" + i);
        }
      }
      { // make sure iterator works while values are valid
        Collection c = r.values();
        assertEquals(ENTRY_COUNT, c.size());
        Iterator it = c.iterator();
        int count = 0;
        while (it.hasNext()) {
          it.next();
          count++;
        }
        assertEquals(ENTRY_COUNT, count);
      }
      r.localInvalidateRegion();
      // now we expect iterator to stackOverflow if bug 34583
      {
        Collection c = r.values();
        assertEquals(0, c.size());
        Iterator it = c.iterator();
        int count = 0;
        while (it.hasNext()) {
          it.next();
          count++;
        }
        assertEquals(0, count);
      }
    } finally {
      ds.disconnect();
    }
  }
}
