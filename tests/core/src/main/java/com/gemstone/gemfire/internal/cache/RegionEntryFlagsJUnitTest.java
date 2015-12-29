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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache;

import java.util.Set;

import junit.framework.TestCase;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.internal.cache.LocalRegion.NonTXEntry;

/**
 * This test verifies the flag's on-off switching for
 * boolean flags in AbstractRegionEntry.
 * Currently a byte array is used to maintain two flags.
 * 
 * @author shobhit
 *
 */
public class RegionEntryFlagsJUnitTest extends TestCase {

  /**
   * @param name
   */
  public RegionEntryFlagsJUnitTest(String name) {
    super(name);
  }

  @Override
  protected void setUp() throws Exception {
    CacheUtils.startCache();
  }

  @Override
  protected void tearDown() throws Exception {
    CacheUtils.closeCache();
  }

  public void testUpdateInProgressFlag() {
    Region region = CacheUtils.createRegion("testRegion", null,
        Scope.DISTRIBUTED_ACK);
    // Put one entry in the region.
    region.put(1, 1);
    Set entries = region.entrySet();
    assertEquals(1, entries.size());

    Region.Entry nonTxEntry = (Entry) entries.iterator().next();
    RegionEntry entry = ((NonTXEntry) nonTxEntry).getRegionEntry();
    assertFalse(entry.isUpdateInProgress());
    entry.setUpdateInProgress(true);
    assertTrue(entry.isUpdateInProgress());
    entry.setUpdateInProgress(false);
    assertFalse(entry.isUpdateInProgress());
  }

  /* ValueWasResultOfSearch flag no longer used in new TX impl
  public void testNetSearchFlag() {
    Region region = CacheUtils.createRegion("testRegion", null,
        Scope.DISTRIBUTED_ACK);
    // Put one entry in the region.
    region.put(1, 1);
    Set entries = region.entrySet();
    assertEquals(1, entries.size());

    Region.Entry nonTxEntry = (Entry) entries.iterator().next();
    RegionEntry entry = ((NonTXEntry) nonTxEntry).getRegionEntry();
    assertFalse(entry.getValueWasResultOfSearch());
    entry.setValueResultOfSearch(true);
    assertTrue(entry.getValueWasResultOfSearch());
    entry.setValueResultOfSearch(false);
    assertFalse(entry.getValueWasResultOfSearch());
  }
  */
}
