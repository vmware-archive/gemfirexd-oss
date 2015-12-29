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

package hydra;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.hdfs.HDFSStore;
import com.gemstone.gemfire.cache.hdfs.HDFSStoreFactory;
import com.gemstone.gemfire.cache.hdfs.internal.HDFSStoreFactoryImpl;

/**
 * Helps clients use {@link HDFSStoreDescription}.  Methods are thread-safe.
 * HDFSStores are lazily created by region creation methods in {@link
 * RegionHelper}, so the HDFS store creation methods here need only be used
 * when <code> RegionHelper</code> is not being used to create regions.
 */
public class HDFSStoreHelper {

//------------------------------------------------------------------------------
// HDFSStore
//------------------------------------------------------------------------------

  /**
   * Creates the HDFS store using the given HDFS store configuration.  The HDFS
   * store is configured using the {@link HDFSStoreDescription} corresponding
   * to the HDFS store configuration from {@link HDFSStorePrms#names}.  The
   * name of the HDFS store is the same as its configuration name.
   * <p>
   * Lazily creates the disk store specified by {@link HDFSStorePrms
   * #diskStoreName}, using {@link DiskStoreHelper.createDiskStore(String)},
   * if necessary. If the disk store has already been created, no checks are
   * done to ensure that it is consistent with the configuration provided in
   * {@link DiskStorePrms#names}.
   *
   * @throws HydraRuntimeException if an attempt is made to reconfigure an
   *         existing HDFS store.
   */
  public static synchronized HDFSStore createHDFSStore(String hdfsStoreConfig) {
    HDFSStoreFactory factory = getHDFSStoreFactory(hdfsStoreConfig);
    return createHDFSStore(hdfsStoreConfig, factory);
  }

  /**
   * Creates the HDFS store with the given name.  The HDFS store is configured
   * using the given factory.
   * <p>
   * Lazily creates the disk store specified by {@link HDFSStorePrms
   * #diskStoreName}, using {@link DiskStoreHelper.createDiskStore(String)},
   * if necessary. If the disk store has already been created, no checks are
   * done to ensure that it is consistent with the configuration provided in
   * {@link DiskStorePrms#names}.
   *
   * @throws HydraRuntimeException if an attempt is made to reconfigure an
   *         existing HDFS store.
   */
  public static synchronized HDFSStore createHDFSStore(String hdfsStoreName,
                                       HDFSStoreFactory factory) {
    if (hdfsStoreName == null) {
      throw new IllegalArgumentException("hdfsStoreName cannot be null");
    }
    if (factory == null) {
      throw new IllegalArgumentException("factory cannot be null");
    }
    HDFSStore hdfsStore = CacheHelper.getCache().findHDFSStore(hdfsStoreName);
    if (hdfsStore == null) {
      log("Creating HDFS store named: " + hdfsStoreName + " with attributes "
         + hdfsStoreFactoryToString(hdfsStoreName, factory));
      String diskStoreConfig = factory.getHDFSEventQueueAttributes()
                                      .getDiskStoreName();
      if (diskStoreConfig != null) {
        DiskStore diskStore = CacheHelper.getCache()
                                         .findDiskStore(diskStoreConfig);
        if (diskStore == null) {
          DiskStoreHelper.createDiskStore(diskStoreConfig);
        } // else allow use of the existing store
      }
      try {
        hdfsStore = factory.create(hdfsStoreName);
      } catch (IllegalStateException e) {
        throw new HydraInternalException("Should not happen", e);
      }
      log("Created HDFS store named: " + hdfsStoreName);
    } else {
      if (!HDFSStoreDescription.equals(((HDFSStoreFactoryImpl)factory).getConfigView(), hdfsStore)) {
        // block attempts to create HDFS store with clashing attributes
        String desired = hdfsStoreFactoryToString(hdfsStoreName, factory);
        String existing = hdfsStoreToString(hdfsStore);
        String s = "HDFSStore " + hdfsStoreName
                 + " already exists with different attributes"
                 + "\n  DESIRED = " + desired + "\n  EXISTING = " + existing;
        throw new HydraRuntimeException(s);
      } // else it was already created with this configuration, which is fine
    }
    return hdfsStore;
  }

  /*
   * Returns the HDFS store with the given name, or null if no HDFS store with
   * that name exists.
   */
  public static synchronized HDFSStore getHDFSStore(String hdfsStoreName) {
    if (hdfsStoreName == null) {
      throw new IllegalArgumentException("hdfsStoreName cannot be null");
    }
    return CacheHelper.getCache().findHDFSStore(hdfsStoreName);
  }

  /**
   * Returns the given HDFS store as a string.
   */
  public static String hdfsStoreToString(HDFSStore hdfsStore) {
    if (hdfsStore == null) {
      throw new IllegalArgumentException("HDFS store cannot be null");
    }
    return HDFSStoreDescription.hdfsStoreToString(hdfsStore);
  }

//------------------------------------------------------------------------------
// HDFSStoreFactory
//------------------------------------------------------------------------------

  /**
   * Returns a HDFS store factory for the given HDFS store configuration from
   * {@link HDFSStorePrms#names}, configured using the corresponding {@link
   * HDFSStoreDescription}.
   */
  public static HDFSStoreFactory getHDFSStoreFactory(String hdfsStoreConfig) {
    // look up the HDFS store configuration
    HDFSStoreDescription hsd = getHDFSStoreDescription(hdfsStoreConfig);

    // create the HDFS store factory
    HDFSStoreFactory factory = CacheHelper.getCache().createHDFSStoreFactory();

    // configure the HDFS store factory
    log("Configuring HDFS store factory for config: " + hdfsStoreConfig);
    hsd.configure(factory);
    log("Configured HDFS store factory: " + factory);

    // return the result
    return factory;
  }

  /**
   * Returns the given named HDFS store factory as a string.
   */
  private static String hdfsStoreFactoryToString(String hdfsStoreName,
                                                 HDFSStoreFactory factory) {
    if (hdfsStoreName == null) {
      throw new IllegalArgumentException("hdfsStoreName cannot be null");
    }
    if (factory == null) {
      throw new IllegalArgumentException("factory cannot be null");
    }
    HDFSStore store = ((HDFSStoreFactoryImpl)factory).getConfigView();
    return HDFSStoreDescription.hdfsStoreToString(hdfsStoreName, store);
  }

//------------------------------------------------------------------------------
// HDFSStoreDescription
//------------------------------------------------------------------------------

  /**
   * Returns the {@link HDFSStoreDescription} with the given configuration name
   * from {@link HDFSStorePrms#names}.
   */
  public static HDFSStoreDescription getHDFSStoreDescription(
                                     String hdfsStoreConfig) {
    if (hdfsStoreConfig == null) {
      throw new IllegalArgumentException("hdfsStoreConfig cannot be null");
    }
    log("Looking up HDFS store config: " + hdfsStoreConfig);
    HDFSStoreDescription hsd = TestConfig.getInstance()
                               .getHDFSStoreDescription(hdfsStoreConfig);
    if (hsd == null) {
      String s = hdfsStoreConfig + " not found in "
               + BasePrms.nameForKey(HDFSStorePrms.names);
      throw new HydraRuntimeException(s);
    }
    log("Looked up HDFS store config:\n" + hsd);
    return hsd;
  }

//------------------------------------------------------------------------------
// Log
//------------------------------------------------------------------------------

  private static LogWriter log;
  private static synchronized void log(String s) {
    if (log == null) {
      log = Log.getLogWriter();
    }
    if (log.infoEnabled()) {
      log.info(s);
    }
  }
}
