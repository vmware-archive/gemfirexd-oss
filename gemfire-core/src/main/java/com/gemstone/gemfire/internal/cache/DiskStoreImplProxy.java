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

import java.io.File;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.internal.cache.persistence.DiskRecoveryStore;

public class DiskStoreImplProxy extends DiskStoreImpl {

  protected DiskStoreImplProxy(Cache cache, DiskStoreAttributes props,
      boolean ownedByRegion, InternalRegionArguments internalRegionArgs) {
    super(cache, props, ownedByRegion, internalRegionArgs);
    // TODO Auto-generated constructor stub
  }

  protected DiskStoreImplProxy(Cache cache, DiskStoreAttributes props) {
    super(cache, props);
  }

  protected DiskStoreImplProxy(Cache cache, String name,
      DiskStoreAttributes props, boolean ownedByRegion,
      InternalRegionArguments internalRegionArgs, boolean offline,
      boolean upgradeVersionOnly, boolean offlineValidating,
      boolean offlineCompacting, boolean needsOplogs) {
    super(cache, name, props, ownedByRegion, internalRegionArgs, offline,
        upgradeVersionOnly, offlineValidating, offlineCompacting, needsOplogs, true);
  }

  
  public void close() {
    super.close();
    stopAsyncFlusher();
  }
  
  public void closeLockFile() {
    super.closeLockFile();
  }
  
  public void close(boolean destroy) {
    super.close(destroy);
  }
  
  public static void closeDiskStoreFiles(DiskStoreImpl diskStore) {
    diskStore.closeLockFile();
    diskStore.close();
  }
  
  public void setForceKRFRecovery(boolean force) {
    this.FORCE_KRF_RECOVERY = force;
  }
  
  public void setDataExtractionKrfRecovery(boolean dataExtractionKrfRecovery) {
    this.dataExtractionKrfRecovery = dataExtractionKrfRecovery;
  }
  
  public void scheduleForRecovery(DiskRecoveryStore drs) {
    super.scheduleForRecovery(drs);
  }
  public static void cleanUpOffline() {
    if (offlineCache != null) {
      offlineCache.close();
      offlineCache = null;
    }
    if (offlineDS != null) {
      offlineDS.disconnect();
      offlineDS = null;
    }
  }
  
  /**
   * This just checks if the disk store is valid before trying to create a diskstoreimpl of said diskstore
   * If we attempt to create a diskstoreimpl with an invalid .if file, we end up with leaked threads
   * and no diskstore instance to close them.
   */
  public static boolean diskStoreExists(String name, File directory) {
    File f = new File(directory,
        "BACKUP" + name + DiskInitFile.IF_FILE_EXT);
    return f.exists();
  }
}
