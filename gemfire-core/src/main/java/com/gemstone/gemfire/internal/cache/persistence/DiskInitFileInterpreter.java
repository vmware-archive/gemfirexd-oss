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
package com.gemstone.gemfire.internal.cache.persistence;

import java.util.EnumSet;
import java.util.concurrent.ConcurrentHashMap;

import com.gemstone.gemfire.internal.cache.DiskInitFile.DiskRegionFlag;
import com.gemstone.gemfire.internal.cache.versions.RegionVersionHolder;
import com.gemstone.gemfire.internal.shared.Version;

/**
 * @author dsmith
 *
 */
public interface DiskInitFileInterpreter {

  boolean isClosing();

  String getNameForError();

  /**
   * @param id
   * @param c
   * @param ic
   */
  void cmnInstantiatorId(int id, Class c, Class ic);

  /**
   * @param id
   * @param cn
   * @param icn
   */
  void cmnInstantiatorId(int id, String cn, String icn);

  /**
   * @param dsc
   */
  void cmnDataSerializerId(Class dsc);

  /**
   * @param drId
   * @param pmid
   */
  void cmnOnlineMemberId(long drId, PersistentMemberID pmid);

  /**
   * @param drId
   * @param pmid
   */
  void cmnOfflineMemberId(long drId, PersistentMemberID pmid);

  void cmdOfflineAndEqualMemberId(long drId, PersistentMemberID pmid);
  /**
   * @param drId
   * @param pmid
   */
  void cmnRmMemberId(long drId, PersistentMemberID pmid);

  /**
   * @param drId
   * @param pmid
   */
  void cmnAddMyInitializingPMID(long drId, PersistentMemberID pmid);

  /**
   * @param drId
   */
  void cmnMarkInitialized(long drId);

  /**
   * @param drId
   * @param regName
   */
  void cmnCreateRegion(long drId, String regName);

  /**
   * @param drId
   */
  void cmnBeginDestroyRegion(long drId);

  /**
   * @param drId
   */
  void cmnEndDestroyRegion(long drId);

  /**
   * @param drId
   */
  void cmnBeginPartialDestroyRegion(long drId);

  /**
   * @param drId
   */
  void cmnEndPartialDestroyRegion(long drId);

  /**
   * @param drId
   * @param clearOplogEntryId
   */
  void cmnClearRegion(long drId, long clearOplogEntryId);
  
  /**
   * @param drId
   * @param clearRVV
   */
  void cmnClearRegion(long drId, ConcurrentHashMap<DiskStoreID, RegionVersionHolder<DiskStoreID>> clearRVV);

  public void cmnCrfCreate(long oplogId);
  public void cmnDrfCreate(long oplogId);
  public boolean cmnCrfDelete(long oplogId);
  public boolean cmnDrfDelete(long oplogId);
  
  public void cmnRegionConfig(long drId, byte lruAlgorithm, byte lruAction, int lruLimit,
                              int concurrencyLevel, int initialCapacity,
                              float loadFactor, boolean statisticsEnabled,
                              boolean isBucket, EnumSet<DiskRegionFlag> flags,
                              long uuid, String partitionName, int startingBucketId,
                              String compressorClassName, boolean enableOffHeapMemory);

  void cmnKrfCreate(long oplogId);
  
  boolean cmnPRCreate(String name, PRPersistentConfig config);
  
  boolean cmnPRDestroy(String name);

  void cmnAddCanonicalMemberId(int id, Object object);

  void cmnDiskStoreID(DiskStoreID result);

  boolean cmnRevokeDiskStoreId(PersistentMemberPattern id);
  
  void cmnGemfireVersion(Version version);
  
  public void cmnIrfCreate(long oplogId);

  public void cmnIrfDelete(long oplogId);

  public void cmnIndexCreate(String indexId);

  public void cmnIndexDelete(String indexId);
}
