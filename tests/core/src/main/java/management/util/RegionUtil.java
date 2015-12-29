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
package management.util;

import util.TestException;

import com.gemstone.gemfire.cache.Region;

public class RegionUtil {

  public static void checkRegionKeyExists(Region r, Object key) {
    if (HydraUtil.isSerialTest()) {
      if (r.containsKey(key)) {
        return;
      } else {
        throw new TestException("Region " + r.getName() + " does not contain key " + key);
      }
    }
  }

  public static void checkRegionKeyExistsValueExists(Region r, Object key, Object value) {
    if (HydraUtil.isSerialTest()) {
      if (value == null)
        throw new TestException("Expected value can not be null ");

      checkRegionKeyExists(r, key);
      Object regionValue = r.get(key);
      if (regionValue == null) {
        throw new TestException("Region " + r.getName() + " value for  " + key + " is " + regionValue
            + " but expected " + value);
      }

      if (regionValue != null && !regionValue.equals(value)) {
        throw new TestException("Region " + r.getName() + " value for  " + key + " is " + regionValue
            + " but expected " + value);
      }
    }
  }

  public static void checkRegionValueDoesNotExists(Region r, Object key) {
    if (HydraUtil.isSerialTest()) {
      if (!r.containsValueForKey(key)) {
        return;
      } else {
        throw new TestException("Region " + r.getName() + " contains value for " + key + " but was invalidated before");
      }
    }
  }

  public static void checkRegionKeyDoesNotExistsValueDoesNotExists(Region r, Object key) {
    if (HydraUtil.isSerialTest()) {
      if (r.containsKey(key)) {
        throw new TestException("Region " + r.getName() + " contains key " + key + " but was destroyed before");
      }

      if (!r.containsValueForKey(key)) {
        return;
      } else {
        throw new TestException("Region " + r.getName() + " contains value for " + key
            + " but was destroyed/invalidated before");
      }
    }
  }

}
