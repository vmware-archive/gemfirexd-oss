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

import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;

/**
 * Provides version-dependent support for TestHelper.
 */
public class TestHelperVersionHelper {

  /**
   * Returns a display string for region attributes getConcurrencyChecksEnabled.
   * Added for concurrencyChecksEnabled (RegionAttribute) in 7.0
   */
  public static String getConcurrencyChecksEnabled(RegionAttributes attrs) {
    return new String("   concurrencyChecksEnabled: " + attrs.getConcurrencyChecksEnabled() + "\n");
  }

  /**
   * Returns a display string for ((EntryEventImpl)event.isConcurrencyConflict()
   * Added for concurrencyChecksEnabled (RegionAttribute) in 7.0
   * EntryEventImpl gives us access to an internal method, event.isConcurrencyConflict()
   */
  public static String getConcurrencyConflict(EntryEvent event) {
    String rc = new String("");
    Region region = event.getRegion();
    if (region.getAttributes().getConcurrencyChecksEnabled()) {
      rc = new String("   event.isConcurrencyConflict(): " + ((EntryEventImpl)(event)).isConcurrencyConflict() + "\n");
    } 
    return rc;
  }
}
