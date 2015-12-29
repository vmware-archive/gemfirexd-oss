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

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.EnumSet;
import java.util.concurrent.ConcurrentHashMap;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.DiskAccessException;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.AbstractDiskRegion;
import com.gemstone.gemfire.internal.cache.CountingDataInputStream;
import com.gemstone.gemfire.internal.cache.DiskInitFile;
import com.gemstone.gemfire.internal.cache.DiskInitFile.DiskRegionFlag;
import com.gemstone.gemfire.internal.cache.DiskStoreImpl;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.ProxyBucketRegion;
import com.gemstone.gemfire.internal.cache.versions.RegionVersionHolder;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.shared.UnsupportedGFXDVersionException;
import com.gemstone.gemfire.internal.shared.Version;

/**
 * @author dsmith
 *
 */
public class DiskInitFileParser {
  private final CountingDataInputStream dis;
  private DiskInitFileInterpreter interpreter;
  
  private static final boolean DEBUG = false;

  public DiskInitFileParser(CountingDataInputStream dis, DiskInitFileInterpreter interpreter, LogWriterI18n logger) {
    this.logger = logger;
    this.dis = dis;
    if(DEBUG) {
      this.interpreter = createPrintingInterpreter(interpreter);
    } else {
      this.interpreter = interpreter;
    }
  }

  private final LogWriterI18n logger;
  private transient boolean gotEOF;
  
  private LogWriterI18n getLogger() {
    return this.logger;
  }
  
  public DiskStoreID parse() throws IOException, ClassNotFoundException {
    Version gfversion = Version.GFE_662;
    DiskStoreID result = null;
    boolean endOfFile = false;
    while (!endOfFile) {
      if (dis.atEndOfFile()) {
        endOfFile = true;
        break;
      }
      byte opCode = dis.readByte();
      if (DiskStoreImpl.TRACE_RECOVERY) {
        if (getLogger() != null)
          getLogger().info(LocalizedStrings.DEBUG,
              "TRACE_RECOVERY: DiskInitFile opcode=" + opCode);
      }
      switch (opCode) {
      case DiskInitFile.IF_EOF_ID:
        endOfFile = true;
        gotEOF = true;
        break;
      case DiskInitFile.IFREC_INSTANTIATOR_ID: {
          int id = dis.readInt();
          String cn = readClassName(dis);
          String icn = readClassName(dis);
          readEndOfRecord(dis);
          interpreter.cmnInstantiatorId(id, cn, icn);
        break;
      }
      case DiskInitFile.IFREC_DATA_SERIALIZER_ID: {
        Class<?> dsc = readClass(dis);
        readEndOfRecord(dis);
        interpreter.cmnDataSerializerId(dsc);
        break;
      }
      case DiskInitFile.IFREC_ONLINE_MEMBER_ID: {
        long drId = readDiskRegionID(dis);
        PersistentMemberID pmid = readPMID(dis, gfversion);
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          if (getLogger() != null)
            getLogger().info(
                LocalizedStrings.DEBUG,
                "TRACE_RECOVERY: IFREC_ONLINE_MEMBER_ID drId=" + drId
                    + " pmid=" + pmid);
        }
        interpreter.cmnOnlineMemberId(drId, pmid);
        break;
      }
      case DiskInitFile.IFREC_OFFLINE_MEMBER_ID: {
        long drId = readDiskRegionID(dis);
        PersistentMemberID pmid = readPMID(dis, gfversion);
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          if (getLogger() != null)
            getLogger().info(
                LocalizedStrings.DEBUG,
                "TRACE_RECOVERY: IFREC_OFFLINE_MEMBER_ID drId=" + drId
                    + " pmid=" + pmid);
        }
        interpreter.cmnOfflineMemberId(drId, pmid);
        break;
      }
      case DiskInitFile.IFREC_RM_MEMBER_ID: {
        long drId = readDiskRegionID(dis);
        PersistentMemberID pmid = readPMID(dis, gfversion);
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          if (getLogger() != null)
            getLogger().info(
                LocalizedStrings.DEBUG,
                "TRACE_RECOVERY: IFREC_RM_MEMBER_ID drId=" + drId + " pmid="
                    + pmid);
        }
        interpreter.cmnRmMemberId(drId, pmid);
        break;
      }
      case DiskInitFile.IFREC_MY_MEMBER_INITIALIZING_ID: {
        long drId = readDiskRegionID(dis);
        PersistentMemberID pmid = readPMID(dis, gfversion);
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          if (getLogger() != null)
            getLogger().info(
                LocalizedStrings.DEBUG,
                "TRACE_RECOVERY: IFREC_MY_MEMBER_INITIALIZING_ID drId=" + drId
                    + " pmid=" + pmid);
        }
        interpreter.cmnAddMyInitializingPMID(drId, pmid);
        break;
      }
      case DiskInitFile.IFREC_MY_MEMBER_INITIALIZED_ID: {
        long drId = readDiskRegionID(dis);
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          if (getLogger() != null)
            getLogger().info(LocalizedStrings.DEBUG,
                "TRACE_RECOVERY: IFREC_MY_MEMBER_INITIALIZED_ID drId=" + drId);
        }
        interpreter.cmnMarkInitialized(drId);
        break;
      }
      case DiskInitFile.IFREC_CREATE_REGION_ID: {
        long drId = readDiskRegionID(dis);
        String regName = dis.readUTF();
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          if (getLogger() != null)
            getLogger().info(
                LocalizedStrings.DEBUG,
                "TRACE_RECOVERY: IFREC_CREATE_REGION_ID drId=" + drId
                    + " name=" + regName);
        }
        interpreter.cmnCreateRegion(drId, regName);
        break;
      }
      case DiskInitFile.IFREC_BEGIN_DESTROY_REGION_ID: {
        long drId = readDiskRegionID(dis);
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          if (getLogger() != null)
            getLogger().info(LocalizedStrings.DEBUG,
                "TRACE_RECOVERY: IFREC_BEGIN_DESTROY_REGION_ID drId=" + drId);
        }
        interpreter.cmnBeginDestroyRegion(drId);
        break;
      }
      case DiskInitFile.IFREC_END_DESTROY_REGION_ID: {
        long drId = readDiskRegionID(dis);
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          if (getLogger() != null)
            getLogger().info(LocalizedStrings.DEBUG,
                "TRACE_RECOVERY: IFREC_END_DESTROY_REGION_ID drId=" + drId);
        }
        interpreter.cmnEndDestroyRegion(drId);
        break;
      }
      case DiskInitFile.IFREC_BEGIN_PARTIAL_DESTROY_REGION_ID: {
        long drId = readDiskRegionID(dis);
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          if (getLogger() != null)
            getLogger().info(
                LocalizedStrings.DEBUG,
                "TRACE_RECOVERY: IFREC_BEGIN_PARTIAL_DESTROY_REGION_ID drId="
                    + drId);
        }
        interpreter.cmnBeginPartialDestroyRegion(drId);
        break;
      }
      case DiskInitFile.IFREC_END_PARTIAL_DESTROY_REGION_ID: {
        long drId = readDiskRegionID(dis);
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          if (getLogger() != null)
            getLogger().info(
                LocalizedStrings.DEBUG,
                "TRACE_RECOVERY: IFREC_END_PARTIAL_DESTROY_REGION_ID drId="
                    + drId);
        }
        interpreter.cmnEndPartialDestroyRegion(drId);
        break;
      }
      case DiskInitFile.IFREC_CLEAR_REGION_ID: {
        long drId = readDiskRegionID(dis);
        long clearOplogEntryId = dis.readLong();
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          if (getLogger() != null)
            getLogger().info(
                LocalizedStrings.DEBUG,
                "TRACE_RECOVERY: IFREC_CLEAR_REGION_ID drId=" + drId
                    + " oplogEntryId=" + clearOplogEntryId);
        }
        interpreter.cmnClearRegion(drId, clearOplogEntryId);
        break;
      }
      case DiskInitFile.IFREC_CLEAR_REGION_WITH_RVV_ID: {
        long drId = readDiskRegionID(dis);
        int size = dis.readInt();
        ConcurrentHashMap<DiskStoreID, RegionVersionHolder<DiskStoreID>> memberToVersion 
          = new ConcurrentHashMap<DiskStoreID, RegionVersionHolder<DiskStoreID>>(size);
        for(int i = 0; i < size; i++) {
          DiskStoreID id = new DiskStoreID();
          InternalDataSerializer.invokeFromData(id, dis);
          RegionVersionHolder holder = new RegionVersionHolder(dis);
          memberToVersion.put(id, holder);
        }
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          if (getLogger() != null)
            getLogger().info(
                LocalizedStrings.DEBUG,
                "TRACE_RECOVERY: IFREC_CLEAR_REGION_WITH_RVV_ID drId=" + drId
                    + " memberToVersion=" + memberToVersion);
        }
        interpreter.cmnClearRegion(drId, memberToVersion);
        break;
      }
      case DiskInitFile.IFREC_CRF_CREATE: {
        long oplogId = dis.readLong();
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          if (getLogger() != null)
            getLogger().info(LocalizedStrings.DEBUG,
                "TRACE_RECOVERY: IFREC_CRF_CREATE oplogId=" + oplogId);
        }
        interpreter.cmnCrfCreate(oplogId);
        break;
      }
      case DiskInitFile.IFREC_DRF_CREATE: {
        long oplogId = dis.readLong();
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          if (getLogger() != null)
            getLogger().info(LocalizedStrings.DEBUG,
                "TRACE_RECOVERY: IFREC_DRF_CREATE oplogId=" + oplogId);
        }
        interpreter.cmnDrfCreate(oplogId);
      }
        break;
      case DiskInitFile.IFREC_KRF_CREATE: {
        long oplogId = dis.readLong();
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          if (getLogger() != null)
            getLogger().info(LocalizedStrings.DEBUG,
                "TRACE_RECOVERY: IFREC_KRF_CREATE oplogId=" + oplogId);
        }
        interpreter.cmnKrfCreate(oplogId);
      }
        break;
      case DiskInitFile.IFREC_IRF_CREATE: {
        long oplogId = dis.readLong();
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          if (getLogger() != null)
            getLogger().info(LocalizedStrings.DEBUG,
                "TRACE_RECOVERY: IFREC_IRF_CREATE oplogId=" + oplogId);
        }
        interpreter.cmnIrfCreate(oplogId);
        break;
      }
      case DiskInitFile.IFREC_IRF_DELETE: {
        long oplogId = dis.readLong();
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          if (getLogger() != null)
            getLogger().info(LocalizedStrings.DEBUG,
                "TRACE_RECOVERY: IFREC_IRF_DELETE oplogId=" + oplogId);
        }
        interpreter.cmnIrfDelete(oplogId);
        break;
      }
      case DiskInitFile.IFREC_INDEX_CREATE: {
        String indexId = dis.readUTF();
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          if (getLogger() != null)
            getLogger().info(LocalizedStrings.DEBUG,
                "TRACE_RECOVERY: IFREC_INDEX_CREATE indexId=" + indexId);
        }
        interpreter.cmnIndexCreate(indexId);
      }
        break;
      case DiskInitFile.IFREC_INDEX_DELETE: {
        String indexlId = dis.readUTF();
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          if (getLogger() != null)
            getLogger().info(LocalizedStrings.DEBUG,
                "TRACE_RECOVERY: IFREC_INDEX_DELETE indexlId=" + indexlId);
        }
        interpreter.cmnIndexDelete(indexlId);
      }
        break;
      case DiskInitFile.IFREC_CRF_DELETE: {
        long oplogId = dis.readLong();
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          if (getLogger() != null)
            getLogger().info(LocalizedStrings.DEBUG,
                "TRACE_RECOVERY: IFREC_CRF_DELETE oplogId=" + oplogId);
        }
        interpreter.cmnCrfDelete(oplogId);
      }
        break;
      case DiskInitFile.IFREC_DRF_DELETE: {
        long oplogId = dis.readLong();
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          if (getLogger() != null)
            getLogger().info(LocalizedStrings.DEBUG,
                "TRACE_RECOVERY: IFREC_DRF_DELETE oplogId=" + oplogId);
        }
        interpreter.cmnDrfDelete(oplogId);
      }
        break;
      case DiskInitFile.IFREC_REGION_CONFIG_ID: {
        long drId = readDiskRegionID(dis);
        byte lruAlgorithm = dis.readByte();
        byte lruAction = dis.readByte();
        int lruLimit = dis.readInt();
        int concurrencyLevel = dis.readInt();
        int initialCapacity = dis.readInt();
        float loadFactor = dis.readFloat();
        boolean statisticsEnabled = dis.readBoolean();
        boolean isBucket = dis.readBoolean();
        EnumSet<DiskRegionFlag> flags = EnumSet.noneOf(DiskRegionFlag.class);
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          if (getLogger() != null)
            getLogger().info(LocalizedStrings.DEBUG,
                "TRACE_RECOVERY: IFREC_REGION_CONFIG_ID drId=" + drId);
        }
        interpreter.cmnRegionConfig(drId, lruAlgorithm, lruAction, lruLimit,
            concurrencyLevel, initialCapacity, loadFactor,
            statisticsEnabled, isBucket, flags,
            /* no UUID in older versions */
            AbstractDiskRegion.INVALID_UUID,
            ProxyBucketRegion.NO_FIXED_PARTITION_NAME, // fixes bug 43910
            -1, null, false);
        break;
      }
      case DiskInitFile.IFREC_REGION_CONFIG_ID_66: {
        long drId = readDiskRegionID(dis);
        byte lruAlgorithm = dis.readByte();
        byte lruAction = dis.readByte();
        int lruLimit = dis.readInt();
        int concurrencyLevel = dis.readInt();
        int initialCapacity = dis.readInt();
        float loadFactor = dis.readFloat();
        boolean statisticsEnabled = dis.readBoolean();
        boolean isBucket = dis.readBoolean();
        EnumSet<DiskRegionFlag> flags = EnumSet.noneOf(DiskRegionFlag.class);
        if (GemFireCacheImpl.gfxdSystem()) {
          if (dis.readBoolean()) {
            flags.add(DiskRegionFlag.HAS_REDUNDANT_COPY);
          }
          if (dis.readBoolean()) {
            flags.add(DiskRegionFlag.DEFER_RECOVERY);
          }
        }
        String partitionName = dis.readUTF(); 
        int startingBucketId = dis.readInt();
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          getLogger().info(LocalizedStrings.DEBUG, "TRACE_RECOVERY: IFREC_REGION_CONFIG_ID drId=" + drId);
        }
        interpreter.cmnRegionConfig(drId, lruAlgorithm, lruAction, lruLimit,
                                    concurrencyLevel, initialCapacity, loadFactor,
                                    statisticsEnabled, isBucket, flags,
                                    /* no UUID in older versions */
                                    AbstractDiskRegion.INVALID_UUID,
                                    partitionName, startingBucketId, null, false);
        break;
      }
      case DiskInitFile.IFREC_REGION_CONFIG_ID_71: {
        long drId = readDiskRegionID(dis);
        byte lruAlgorithm = dis.readByte();
        byte lruAction = dis.readByte();
        int lruLimit = dis.readInt();
        int concurrencyLevel = dis.readInt();
        int initialCapacity = dis.readInt();
        float loadFactor = dis.readFloat();
        boolean statisticsEnabled = dis.readBoolean();
        boolean isBucket = dis.readBoolean();
        EnumSet<DiskRegionFlag> flags = EnumSet.noneOf(DiskRegionFlag.class);
        if (dis.readBoolean()) {
          flags.add(DiskRegionFlag.HAS_REDUNDANT_COPY);
        }
        if (dis.readBoolean()) {
          flags.add(DiskRegionFlag.DEFER_RECOVERY);
        }
        long uuid = dis.readLong();
        String partitionName = dis.readUTF(); 
        int startingBucketId = dis.readInt();
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          getLogger().info(LocalizedStrings.DEBUG,
              "TRACE_RECOVERY: IFREC_REGION_CONFIG_ID_7x drId=" + drId
                  + " uuid=" + uuid);
        }
        interpreter.cmnRegionConfig(drId, lruAlgorithm, lruAction, lruLimit,
                                    concurrencyLevel, initialCapacity, loadFactor,
                                    statisticsEnabled, isBucket, flags, uuid,
                                    partitionName, startingBucketId,
                                    null, false);
        break;
      }
      case DiskInitFile.IFREC_REGION_CONFIG_ID_75: {
        long drId = readDiskRegionID(dis);
        byte lruAlgorithm = dis.readByte();
        byte lruAction = dis.readByte();
        int lruLimit = dis.readInt();
        int concurrencyLevel = dis.readInt();
        int initialCapacity = dis.readInt();
        float loadFactor = dis.readFloat();
        boolean statisticsEnabled = dis.readBoolean();
        boolean isBucket = dis.readBoolean();
        EnumSet<DiskRegionFlag> flags = EnumSet.noneOf(DiskRegionFlag.class);
        if (dis.readBoolean()) {
          flags.add(DiskRegionFlag.HAS_REDUNDANT_COPY);
        }
        if (dis.readBoolean()) {
          flags.add(DiskRegionFlag.DEFER_RECOVERY);
        }
        long uuid = dis.readLong();
        String partitionName = dis.readUTF(); 
        int startingBucketId = dis.readInt();
        dis.readBoolean(); // griddb flag, ignored but preserve for backwards compatibility
        
        String compressorClassName = dis.readUTF();
        if ("".equals(compressorClassName)) {
          compressorClassName = null;
        }
        boolean enableOffHeapMemory = dis.readBoolean();
        if(dis.readBoolean()) {
          flags.add(DiskRegionFlag.IS_WITH_VERSIONING);
        }
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          getLogger().info(LocalizedStrings.DEBUG, "TRACE_RECOVERY: IFREC_REGION_CONFIG_ID drId=" + drId);
        }
        interpreter.cmnRegionConfig(drId, lruAlgorithm, lruAction, lruLimit,
                                    concurrencyLevel, initialCapacity, loadFactor,
                                    statisticsEnabled, isBucket, flags, uuid, partitionName,
                                    startingBucketId, compressorClassName, enableOffHeapMemory);
        break;
      }
      case DiskInitFile.IFREC_OFFLINE_AND_EQUAL_MEMBER_ID: {
        long drId = readDiskRegionID(dis);
        PersistentMemberID pmid = readPMID(dis, gfversion);
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          if (getLogger() != null)
            getLogger().info(
                LocalizedStrings.DEBUG,
                "TRACE_RECOVERY: IFREC_OFFLINE_AND_EQUAL_MEMBER_ID drId="
                    + drId + " pmid=" + pmid);
        }
        interpreter.cmdOfflineAndEqualMemberId(drId, pmid);
      }
        break;
      case DiskInitFile.IFREC_DISKSTORE_ID: {
        long leastSigBits = dis.readLong();
        long mostSigBits = dis.readLong();
        readEndOfRecord(dis);
        result = new DiskStoreID(mostSigBits, leastSigBits);
        interpreter.cmnDiskStoreID(result);
      }
        break;
      case DiskInitFile.IFREC_PR_CREATE: {
        String name = dis.readUTF();
        int numBuckets = dis.readInt();
        String colocatedWith = dis.readUTF();
        readEndOfRecord(dis);
        PRPersistentConfig config = new PRPersistentConfig(numBuckets, colocatedWith);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          if (getLogger() != null)
            getLogger().info(LocalizedStrings.DEBUG,
                "TRACE_RECOVERY: IFREC_PR_CREATE name=" + name + ", config=" + config);
        }
        interpreter.cmnPRCreate(name, config);
      }
        break;
      case DiskInitFile.IFREC_GEMFIRE_VERSION: {
        short ver = Version.readOrdinal(dis);
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          if (getLogger() != null)
            getLogger().info(LocalizedStrings.DEBUG,
                "TRACE_RECOVERY: IFREC_GEMFIRE_VERSION version=" + ver);
        }
        try {
          gfversion = Version.fromOrdinal(ver, false);
        } catch (UnsupportedGFXDVersionException e) {
          throw new DiskAccessException(LocalizedStrings
              .Oplog_UNEXPECTED_PRODUCT_VERSION_0.toLocalizedString(ver), e,
              this.interpreter.getNameForError());
        }
        interpreter.cmnGemfireVersion(gfversion);
        break;
      }
      case DiskInitFile.IFREC_PR_DESTROY: {
        String name = dis.readUTF();
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          if (getLogger() != null)
            getLogger().info(LocalizedStrings.DEBUG,
                "TRACE_RECOVERY: IFREC_PR_DESTROY name=" + name);
        }
        interpreter.cmnPRDestroy(name);
        break;
      }
      case DiskInitFile.IFREC_ADD_CANONICAL_MEMBER_ID: {
        int id = dis.readInt();
        Object object = DataSerializer.readObject(dis);
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          if (getLogger() != null)
            getLogger().info(LocalizedStrings.DEBUG,
                "TRACE_RECOVERY: IFREC_ADD_CANONICAL_MEMBER_ID id=" + id + "name=" + object);
        }
        interpreter.cmnAddCanonicalMemberId(id, object);
        break;
      }
      case DiskInitFile.IFREC_REVOKE_DISK_STORE_ID: {
        PersistentMemberPattern pattern = new PersistentMemberPattern();
        InternalDataSerializer.invokeFromData(pattern, dis);
        readEndOfRecord(dis);
        if (DiskStoreImpl.TRACE_RECOVERY) {
          if (getLogger() != null)
            getLogger().info(LocalizedStrings.DEBUG,
                "TRACE_RECOVERY: IFREC_REVOKE_DISK_STORE_ID id=" + pattern);
        }
        interpreter.cmnRevokeDiskStoreId(pattern);
      }
        break;
      default:
        throw new DiskAccessException(LocalizedStrings.DiskInitFile_UNKNOWN_OPCODE_0_FOUND.toLocalizedString(opCode), this.interpreter.getNameForError());
      }
      if (interpreter.isClosing()) {
        break;
      }
    }
    return result;
  }
  
  /**
   * Reads a class name from the given input stream, as written by writeClass,
   * and loads the class.
   * 
   * @return null if class can not be loaded; otherwise loaded Class
   */
  private static Class<?> readClass(DataInput di) throws IOException
  {
    int len = di.readInt();
    byte[] bytes = new byte[len];
    di.readFully(bytes);
    String className = new String(bytes); // use default decoder
    Class<?> result = null;
    try {
      result = InternalDataSerializer.getCachedClass(className); // see bug 41206
    }
    catch (ClassNotFoundException ignore) {
    }
    return result;
  }
  
  /**
   * Reads a class name from the given input stream.
   * 
   * @return class name 
   */
  private static String readClassName(DataInput di) throws IOException
  {
    int len = di.readInt();
    byte[] bytes = new byte[len];
    di.readFully(bytes);
    return new String(bytes); // use default decoder
  }
  
  static long readDiskRegionID(CountingDataInputStream dis) throws IOException {
    int bytesToRead = dis.readUnsignedByte();
    if (bytesToRead <= DiskStoreImpl.MAX_RESERVED_DRID
        && bytesToRead >= DiskStoreImpl.MIN_RESERVED_DRID) {
      long result = dis.readByte(); // we want to sign extend this first byte
      bytesToRead--;
      while (bytesToRead > 0) {
        result <<= 8;
        result |= dis.readUnsignedByte(); // no sign extension
        bytesToRead--;
      }
      return result;
    } else {
      return bytesToRead;
    }
  }
  
  private void readEndOfRecord(DataInput di) throws IOException {
    int b = di.readByte();
    if (b != DiskInitFile.END_OF_RECORD_ID) {
      if (b == 0) {
        // this is expected if this is the last record and we died while writing it.
        throw new EOFException("found partial last record");
      } else {
        // Our implementation currently relies on all unwritten bytes having
        // a value of 0. So throw this exception if we find one we didn't expect.
        throw new IllegalStateException("expected end of record (byte=="
                                        + DiskInitFile.END_OF_RECORD_ID
                                        + ") or zero but found " + b);
      }
    }
  }

  private PersistentMemberID readPMID(CountingDataInputStream dis,
      Version gfversion) throws IOException, ClassNotFoundException {
    int len = dis.readInt();
    byte[] buf = new byte[len];
    dis.readFully(buf);
    return bytesToPMID(buf, gfversion);
  }

  private PersistentMemberID bytesToPMID(byte[] bytes, Version gfversion)
      throws IOException, ClassNotFoundException {
    ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
    DataInputStream dis = new DataInputStream(bais);
    PersistentMemberID result = new PersistentMemberID();
    if (Version.GFE_70.compareTo(gfversion) > 0) {
      result.fromData662(dis);
    } else {
      InternalDataSerializer.invokeFromData(result, dis);
    }
    return result;
  }
  
  public static void main(String [] args) throws IOException, ClassNotFoundException {
    if(args.length != 1) {
      System.err.println("Usage: parse filename");
      System.exit(1);
    }
    dump(new File(args[0]));
  }

  public static void dump(File file) throws IOException, ClassNotFoundException {
    InputStream is = new FileInputStream(file);
    CountingDataInputStream dis = new CountingDataInputStream(is, file.length());
    
    try {
      DiskInitFileInterpreter interpreter = createPrintingInterpreter(null);
      DiskInitFileParser parser = new DiskInitFileParser(dis, interpreter, null);
      parser.parse();
    } finally {
      is.close();
    }
  }
  
  private static DiskInitFileInterpreter createPrintingInterpreter(DiskInitFileInterpreter wrapped) {
    DiskInitFileInterpreter interpreter = (DiskInitFileInterpreter) Proxy
        .newProxyInstance(DiskInitFileInterpreter.class.getClassLoader(),
            new Class[] { DiskInitFileInterpreter.class },
            new PrintingInterpreter(wrapped));
    return interpreter;
  }
  
  
  private static class PrintingInterpreter implements InvocationHandler {

    private final DiskInitFileInterpreter delegate;

    public PrintingInterpreter(DiskInitFileInterpreter wrapped) {
      this.delegate = wrapped;
    }

    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      if(method.getName().equals("isClosing")) {
        if(delegate == null) {
          return Boolean.FALSE;
        } else {
          return delegate.isClosing();
        }
      }
      Object result = null;
      if(method.getReturnType().equals(boolean.class)) {
        result = Boolean.TRUE;
      }
      
      StringBuilder out = new StringBuilder();
      out.append(method.getName()).append("(");
      for(Object arg : args) {
        out.append(arg);
        out.append(",");
      }
      out.replace(out.length() -1, out.length(), ")");
      System.out.println(out.toString());
      if(delegate == null) {
        return result;
      }
      else {
        return method.invoke(delegate, args);
      }
    }
    
  }


  public boolean gotEOF() {
    return this.gotEOF;
  }
}
