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
 * Helps clients use {@link HDFSStoreDescription}.
 */
public class HDFSStoreHelper {

  private static LogWriter log = Log.getLogWriter();

  //----------------------------------------------------------------------------
  // HDFSStore
  //----------------------------------------------------------------------------

  /**
   * Generates the DDL for creating the HDFS store with the given configuration
   * name from {@link HDFSStorePrms#names}. This method can be invoked from
   * anywhere, including servers, peer clients, and thin clients.
   * <p>
   * It is up to the test to execute the DDL exactly once.
   *
   * @return the (possibly empty) list of DDL statements
   */
  public static synchronized List<String> getHDFSStoreDDL() {
    log.info("Generating HDFSSTORE DDL");
    List<String> ddls = new ArrayList();
    Collection<HDFSStoreDescription> hsds = GfxdTestConfig.getInstance().getHDFSStoreDescriptions().values();
    for (HDFSStoreDescription hsd : hsds) {
      String ddl = hsd.getDDL();
      ddls.add(ddl);
    }
    log.info("Generated HDFSSTORE DDL: " + ddls);
    return ddls;
  }

  /**
   * Generates the DDL for creating the HDFS store with the given configuration
   * name from {@link HDFSStorePrms#names}. This method can be invoked from
   * anywhere, including servers, peer clients, and thin clients.
   * <p>
   * It is up to the test to execute the DDL exactly once.
   *
   * @return the DDL statement
   */
  public static synchronized String getHDFSStoreDDL(String hdfsStoreConfig) {
    HDFSStoreDescription hsd = GfxdTestConfig.getInstance().getHDFSStoreDescription(hdfsStoreConfig);
    log.info("Generating HDFSSTORE DDL for " + hdfsStoreConfig + " using " + hsd);
    String ddl = hsd.getDDL();
    log.info("Generated HDFSSTORE DDL for " + hdfsStoreConfig + ": " + ddl);
    return ddl;
  }

  //----------------------------------------------------------------------------
  // HDFSStoreDescription
  //----------------------------------------------------------------------------

  /**
   * Returns the {@link HDFSStoreDescription} with the given configuration name
   * from {@link HDFSStorePrms#names}.
   */
  public static HDFSStoreDescription getHDFSStoreDescription(String hdfsStoreConfig) {
    if (hdfsStoreConfig == null) {
      throw new IllegalArgumentException("hdfsStoreConfig cannot be null");
    }
    log.info("Looking up HDFS store config: " + hdfsStoreConfig);
    HDFSStoreDescription hsd = GfxdTestConfig.getInstance().getHDFSStoreDescription(hdfsStoreConfig);
    if (hsd == null) {
      String s = hdfsStoreConfig + " not found in " + BasePrms.nameForKey(HDFSStorePrms.names);
      throw new HydraRuntimeException(s);
    }
    log.info("Looked up HDFS store config:\n" + hsd);
    return hsd;
  }
}
