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

package hydra.gemfirexd;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.gemstone.gemfire.LogWriter;

import hydra.BasePrms;
import hydra.HydraRuntimeException;
import hydra.Log;

/**
 * Helps clients use {@link DiskStoreDescription}.
 */
public class DiskStoreHelper {

  private static LogWriter log = Log.getLogWriter();

  //----------------------------------------------------------------------------
  // DiskStore
  //----------------------------------------------------------------------------

  /**
   * Generates the DDL for creating the disk store with the given configuration
   * name from {@link DiskStorePrms#names}. This method can be invoked from
   * anywhere, including servers, peer clients, and thin clients.
   * <p>
   * It is up to the test to execute the DDL exactly once.
   *
   * @return the (possibly empty) list of DDL statements
   */
  public static synchronized List<String> getDiskStoreDDL() {
    log.info("Generating DiskStore DDL");
    List<String> ddls = new ArrayList();
    Collection<DiskStoreDescription> dsds = GfxdTestConfig.getInstance().getDiskStoreDescriptions().values();
    for (DiskStoreDescription dsd : dsds) {
      String ddl = dsd.getDDL();
      ddls.add(ddl);
    }
    log.info("Generated DiskStore DDL: " + ddls);
    return ddls;
  }

  /**
   * Generates the DDL for creating the disk store with the given configuration
   * name from {@link DiskStorePrms#names}. This method can be invoked from
   * anywhere, including servers, peer clients, and thin clients.
   * <p>
   * It is up to the test to execute the DDL exactly once.
   *
   * @return the DDL statement
   */
  public static synchronized String getDiskStoreDDL(String diskStoreConfig) {
    DiskStoreDescription dsd = GfxdTestConfig.getInstance().getDiskStoreDescription(diskStoreConfig);
    log.info("Generating DiskStore DDL for " + diskStoreConfig + " using " + dsd);
    String ddl = dsd.getDDL();
    log.info("Generated DiskStore DDL for " + diskStoreConfig + ": " + ddl);
    return ddl;
  }

  //----------------------------------------------------------------------------
  // DiskStoreDescription
  //----------------------------------------------------------------------------

  /**
   * Returns the {@link DiskStoreDescription} with the given configuration name
   * from {@link DiskStorePrms#names}.
   */
  public static DiskStoreDescription getDiskStoreDescription(String diskStoreConfig) {
    if (diskStoreConfig == null) {
      throw new IllegalArgumentException("diskStoreConfig cannot be null");
    }
    log.info("Looking up disk store config: " + diskStoreConfig);
    DiskStoreDescription dsd = GfxdTestConfig.getInstance().getDiskStoreDescription(diskStoreConfig);
    if (dsd == null) {
      String s = diskStoreConfig + " not found in " + BasePrms.nameForKey(DiskStorePrms.names);
      throw new HydraRuntimeException(s);
    }
    log.info("Looked up disk store config:\n" + dsd);
    return dsd;
  }
}
