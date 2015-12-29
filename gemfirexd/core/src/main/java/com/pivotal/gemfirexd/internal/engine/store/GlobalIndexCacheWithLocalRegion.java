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

package com.pivotal.gemfirexd.internal.engine.store;

import java.io.File;
import java.io.IOException;

import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.DiskAccessException;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.TimeoutException;
import com.gemstone.gemfire.internal.cache.CacheMap;
import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InternalRegionArguments;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.pivotal.gemfirexd.internal.engine.GfxdConstants;
import com.pivotal.gemfirexd.internal.engine.Misc;

/**
 * @author kneeraj
 * 
 */
public class GlobalIndexCacheWithLocalRegion implements CacheMap {

  private final LocalRegion region;

  private final String tableName;

  private static volatile DiskStoreImpl cacheDiskStore;

  private GlobalIndexCacheWithLocalRegion(String tableName, LocalRegion reg) {
    this.region = reg;
    this.tableName = tableName;
  }

  @SuppressWarnings("deprecation")
  public static GlobalIndexCacheWithLocalRegion createInstance(
      String qualifiedName) throws TimeoutException, RegionExistsException,
      ClassNotFoundException, IOException {

    assert qualifiedName != null: "expected the tableName to be non-null";

    // using deprecated AttributesFactory since we have to call the internal
    // createVMRegion method
    final com.gemstone.gemfire.cache.AttributesFactory<Object, Object> afact = new com.gemstone.gemfire.cache.AttributesFactory<Object, Object>();
    afact.setScope(Scope.LOCAL);
    afact.setInitialCapacity(1000);
    // we don't want versioning to interfere in DDL region ever
    afact.setConcurrencyChecksEnabled(false);
    GemFireCacheImpl cache = Misc.getGemFireCache();
    if (cacheDiskStore == null) {
      createCacheDiskStore(cache);
    }
    afact.setDiskSynchronous(true);
    afact.setDiskStoreName(GfxdConstants.GFXD_GLOBALINDEX_DISKSTORE_NAME);
    afact.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
    afact.setEvictionAttributes(EvictionAttributes.createLRUEntryAttributes(
        100000, EvictionAction.OVERFLOW_TO_DISK));

    // Set the meta-region flag for this region (this is the convention
    // for bypassing authorization checks, for example). Remaining flags
    // are the same as those set by Cache#createRegion().
    InternalRegionArguments internalRegionArgs = new InternalRegionArguments()
        .setDestroyLockFlag(true).setRecreateFlag(false)
        .setSnapshotInputStream(null).setImageTarget(null)
        .setIsUsedForMetaRegion(true).setIsUsedForPartitionedRegionAdmin(false);

    LocalRegion reg = (LocalRegion)cache.createVMRegion(qualifiedName,
        afact.create(), internalRegionArgs);
    return new GlobalIndexCacheWithLocalRegion(qualifiedName, reg);
  }

  public static void createCacheDiskStore(GemFireCacheImpl cache) {
    synchronized (GlobalIndexCacheWithLocalRegion.class) {
      if (cacheDiskStore == null) {
        String persistentDir = Misc.getMemStore().generatePersistentDirName(
            null);
        if (persistentDir == null || persistentDir.length() == 0) {
          persistentDir = "." + File.separatorChar
              + GfxdConstants.DEFAULT_GLOBALINDEX_CACHE_SUBDIR;
        }
        else {
          persistentDir += File.separatorChar
              + GfxdConstants.DEFAULT_GLOBALINDEX_CACHE_SUBDIR;
        }

        DiskStoreFactory dsf = cache.createDiskStoreFactory();
        // Set disk directories
        File[] diskDirs = new File[1];
        diskDirs[0] = new File(persistentDir);
        if (!diskDirs[0].mkdirs() && !diskDirs[0].isDirectory()) {
          throw new DiskAccessException("Could not create directory for "
              + "caching and persistence of global indexes: "
              + diskDirs[0].getAbsolutePath(), (Region<?, ?>)null);
        }
        dsf.setDiskDirs(diskDirs);
        if (DiskStoreFactory.DEFAULT_MAX_OPLOG_SIZE < 10) {
          dsf.setMaxOplogSize(DiskStoreFactory.DEFAULT_MAX_OPLOG_SIZE);
        }
        else {
          dsf.setMaxOplogSize(10);
        }
        // writes use fsync
        dsf.setSyncWrites(false);
        cacheDiskStore = (DiskStoreImpl)dsf
            .create(GfxdConstants.GFXD_GLOBALINDEX_DISKSTORE_NAME);
        cacheDiskStore.setUsedForInternalUse();
      }
    }
  }

  @Override
  public int size() {
    return this.region.size();
  }

  @Override
  public boolean isEmpty() {
    return this.region.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return this.region.containsKey(key);
  }

  @Override
  public Object get(Object key) {
    return this.region.get(key);
  }

  @Override
  public Object put(Object key, Object value) {
    return this.region.put(key, value);
  }

  @Override
  public Object remove(Object key) {
    return this.region.remove(key);
  }

  @Override
  public void clear() {
    this.region.clear();
  }

  @Override
  public void destroyCache() {
    this.region.destroyRegion();
  }
  
  public static void setCacheToNull() {
    cacheDiskStore = null;
  }
}
