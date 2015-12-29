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
package com.gemstone.gemfire.internal.cache.rollingupgrade;

import com.gemstone.gemfire.internal.shared.Version;

/** 
 * RollingUpgrade dunit tests are distributed among subclasses of
 * RollingUpgradeDUnitTest to avoid spurious "hangs" being declared
 * by Hydra.  The base class tests rolling upgrade from the last
 * major revision and subclasses should test upgrading from intervening
 * minor revisions.
 */
public class RollingUpgrade_v14_DUnitTest extends RollingUpgradeDUnitTest {

  public RollingUpgrade_v14_DUnitTest(String name) {
    super(name);
  }

  private static final long serialVersionUID = 1392950401984109088L;
  
  @Override
  public void setUp() throws Exception {
    basicSetUp();

    // Windows machines should mount \\samba-bvt.gemstone.com\gcm on drive J:
    String jarRootDir = "/gcm/where";
    String osDir = "/Linux";
    String os = System.getProperty("os.name");
    if (os != null) {
        if (os.indexOf("Windows") != -1) {
            jarRootDir = "J:/where";
            osDir = "/Windows_NT";
        }
        else if (os.indexOf("SunOS") != -1) {
          osDir = "Solaris";
        }
    }
    //Add locations of previous releases of gemfire
    //For testing, other build locations for gemfire can be added
    gemfireLocations = new LocationAndOrdinal[] { new LocationAndOrdinal(
        jarRootDir + "/gemfireXD/releases/GemFireXD1.3.1-all/" + osDir
            + "/product", Version.GFXD_1302.ordinal())
    };
  }
}
