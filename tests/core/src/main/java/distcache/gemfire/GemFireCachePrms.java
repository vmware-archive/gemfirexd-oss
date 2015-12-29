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

package distcache.gemfire;

import com.gemstone.gemfire.cache.InterestResultPolicy;
import hydra.*;

/**
 *  A class used to store keys for test configuration settings.
 */

public class GemFireCachePrms extends BasePrms {

  //----------------------------------------------------------------------------
  // regionName 
  //----------------------------------------------------------------------------

  /**
   * Optional region name.  Overrides the name obtained from {@link
   * #regionConfig}.
   */
  public static Long regionName;
  public static String getRegionName() {
    Long key = regionName;
    return tasktab().stringAt(key, tab().stringAt(key, null));
  }

  //----------------------------------------------------------------------------
  // InterestPolicy
  //----------------------------------------------------------------------------

  /**
   *  InterestResultPolicy to use.  Valid values are "none", " keys", and
   *  "keysValues".  Not for use with oneof or range.
   *  Defaults to "keysValues".
   *  @see com.gemstone.gemfire.cache.InterestResultPolicy
   */
  public static Long interestResultPolicy;
  public static InterestResultPolicy getInterestResultPolicy() {
    Long key = interestResultPolicy;
    String val = tasktab().stringAt(key, tab().stringAt(key, "keysValues"));
    if (val.equalsIgnoreCase("none")) {
      return InterestResultPolicy.NONE;
    }
    else if (val.equalsIgnoreCase("keys")) {
      return InterestResultPolicy.KEYS;
    }
    else if (val.equalsIgnoreCase("keysValues")
          || val.equalsIgnoreCase("keys_values")) {
      return InterestResultPolicy.KEYS_VALUES;
    }
    else {
      String s = BasePrms.nameForKey(key) + " has illegal value: " + val;
      throw new HydraConfigException(s);
   }
  }

  //----------------------------------------------------------------------------
  // Utility methods
  //----------------------------------------------------------------------------

  static {
      setValues( GemFireCachePrms.class );
  }
  public static void main( String args[] ) {
      dumpKeys();
  }
}
