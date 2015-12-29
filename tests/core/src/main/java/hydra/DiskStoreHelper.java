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
import com.gemstone.gemfire.cache.DiskStoreFactory;

/**
 * Helps clients use {@link DiskStoreDescription}.  Methods are thread-safe.
 * DiskStores are lazily created by region creation methods in {@link
 * RegionHelper}, so the disk store creation methods here need only be used
 * when <code> RegionHelper</code> is not being used to create regions.
 */
public class DiskStoreHelper {

//------------------------------------------------------------------------------
// DiskStore
//------------------------------------------------------------------------------

  /**
   * Creates the disk store using the given disk store configuration.  The disk
   * store is configured using the {@link DiskStoreDescription} corresponding
   * to the disk store configuration from {@link DiskStorePrms#names}.  The
   * name of the disk store is the same as its configuration name.
   *
   * @throws HydraRuntimeException if an attempt is made to reconfigure an
   *         existing disk store.
   */
  public static synchronized DiskStore createDiskStore(String diskStoreConfig) {
    DiskStoreFactory factory = getDiskStoreFactory(diskStoreConfig);
    return createDiskStore(diskStoreConfig, factory);
  }

  /**
   * Creates the disk store with the given name.  The disk store is configured
   * using the given factory.
   *
   * @throws HydraRuntimeException if an attempt is made to reconfigure an
   *         existing disk store.
   */
  public static synchronized DiskStore createDiskStore(String diskStoreName,
                                       DiskStoreFactory factory) {
    if (diskStoreName == null) {
      throw new IllegalArgumentException("diskStoreName cannot be null");
    }
    if (factory == null) {
      throw new IllegalArgumentException("factory cannot be null");
    }
    DiskStore diskStore = CacheHelper.getCache().findDiskStore(diskStoreName);
    if (diskStore == null) {
      log("Creating disk store named: " + diskStoreName + " with attributes "
         + diskStoreFactoryToString(diskStoreName, factory));
      try {
        diskStore = factory.create(diskStoreName);
      } catch (IllegalStateException e) {
        throw new HydraInternalException("Should not happen", e);
      }
      log("Created disk store named: " + diskStoreName);
    } else {
      if (!DiskStoreDescription.equals(factory, diskStore)) {
        // block attempts to create disk store with clashing attributes
        String desired = diskStoreFactoryToString(diskStoreName, factory);
        String existing = diskStoreToString(diskStore);
        String s = "DiskStore " + diskStoreName
                 + " already exists with different attributes"
                 + "\n  DESIRED = " + desired + "\n  EXISTING = " + existing;
        throw new HydraRuntimeException(s);
      } // else it was already created with this configuration, which is fine
    }
    return diskStore;
  }

  /*
   * Returns the disk store with the given name, or null if no disk store with
   * that name exists.
   */
  public static synchronized DiskStore getDiskStore(String diskStoreName) {
    if (diskStoreName == null) {
      throw new IllegalArgumentException("diskStoreName cannot be null");
    }
    return CacheHelper.getCache().findDiskStore(diskStoreName);
  }

  /**
   * Returns the given disk store as a string.
   */
  public static String diskStoreToString(DiskStore diskStore) {
    if (diskStore == null) {
      throw new IllegalArgumentException("disk store cannot be null");
    }
    return DiskStoreDescription.diskStoreToString(diskStore);
  }

//------------------------------------------------------------------------------
// DiskStoreFactory
//------------------------------------------------------------------------------

  /**
   * Returns a disk store factory for the given disk store configuration from
   * {@link DiskStorePrms#names}, configured using the corresponding {@link
   * DiskStoreDescription}.
   */
  public static DiskStoreFactory getDiskStoreFactory(String diskStoreConfig) {
    // look up the disk store configuration
    DiskStoreDescription dsd = getDiskStoreDescription(diskStoreConfig);

    // create the disk store factory
    DiskStoreFactory factory = CacheHelper.getCache().createDiskStoreFactory();

    // configure the disk store factory
    log("Configuring disk store factory for config: " + diskStoreConfig);
    dsd.configure(factory);
    log("Configured disk store factory: " + factory);

    // return the result
    return factory;
  }

  /**
   * Returns the given named disk store factory as a string.
   */
  private static String diskStoreFactoryToString(String diskStoreName,
                                                 DiskStoreFactory factory) {
    if (diskStoreName == null) {
      throw new IllegalArgumentException("diskStoreName cannot be null");
    }
    if (factory == null) {
      throw new IllegalArgumentException("factory cannot be null");
    }
    return DiskStoreDescription.diskStoreFactoryToString(diskStoreName, factory);
  }

//------------------------------------------------------------------------------
// DiskStoreDescription
//------------------------------------------------------------------------------

  /**
   * Returns the {@link DiskStoreDescription} with the given configuration name
   * from {@link DiskStorePrms#names}.
   */
  public static DiskStoreDescription getDiskStoreDescription(
                                     String diskStoreConfig) {
    if (diskStoreConfig == null) {
      throw new IllegalArgumentException("diskStoreConfig cannot be null");
    }
    log("Looking up disk store config: " + diskStoreConfig);
    DiskStoreDescription dsd = TestConfig.getInstance()
                               .getDiskStoreDescription(diskStoreConfig);
    if (dsd == null) {
      String s = diskStoreConfig + " not found in "
               + BasePrms.nameForKey(DiskStorePrms.names);
      throw new HydraRuntimeException(s);
    }
    log("Looked up disk store config:\n" + dsd);
    return dsd;
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
