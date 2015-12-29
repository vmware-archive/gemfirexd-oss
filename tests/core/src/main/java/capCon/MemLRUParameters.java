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
package capCon;

import hydra.ClientDescription;
import hydra.HydraConfigException;
import hydra.Log;
import hydra.TestConfig;
import memscale.OffHeapHelper;
import util.CacheUtil;
import util.TestException;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.lru.LRUAlgorithm;
import com.gemstone.gemfire.internal.cache.lru.MemLRUCapacityController;
import com.gemstone.gemfire.internal.size.WellKnownClassSizer;

/** Class to define the parameters of a memLru eviction controller. Hydra parameters
 *  for shared memory size are taken into consideration. This is used primarily for
 *  tests that put fixed length keys and fixed length byte arrays into the region.
 *  With this assumption, this class can calculate the estimated entries, maximum
 *  megabytes, etc for the memory controller, and the test can then tell if
 *  eviction is occurring too early, or not occurring when expected.
 */
public class MemLRUParameters implements java.io.Serializable {

// static fields
public static int UNKNOWN = -1;                         
       // used for MemLRU parameters that have not been given a value
public static final int MEGABYTE = 1024 * 1024;  
        // num bytes in a megabyte
public static final int OFF_HEAP_OVERHEAD = 8; // number of bytes added to each value for overhead in off-heap memory

// instance fields 

// hydra params
protected int byteArraySize = UNKNOWN;              // (hydra param) size of all values in region (ByteArrays)
protected int maximumMegabytes = UNKNOWN;           // (hydra param) param to memory evictor contructor
protected long minEvictionBytes = UNKNOWN;          // sets a lower limit in the number of bytes in a region
                                                    //    that must be there for eviction to occur
protected long allowableExcessBytes = UNKNOWN;      // number of bytes by which the test will allow
                                                    //    a region to exceed the limits of the controller
protected long totalAllowableBytes;                 // the total number of allowable bytes in a region 
                                                    //    (regionByteLimit + allowableExcessBytes)
protected int keyLength;                            // length of all keys in region (Strings)
protected long regionByteLimit = UNKNOWN;           // the actual number of bytes allowed in the region 
protected int bytesPerKey = UNKNOWN;                // the number of bytes used per key
protected int perEntryOverhead = UNKNOWN;           // the overhead for each entry
protected int totalBytesPerEntry = UNKNOWN;         // the overhead for each entry

// Constructor 
// ================================================================================

/** Create an instance of MemLRUParameters.
 */
public MemLRUParameters(int keyLength, int maxMegabytes, Region aRegion) {
   init(keyLength, maxMegabytes, aRegion);
   Log.getLogWriter().info("Created MemLRUParameters " + this);
}

private void init(int keyLength, int maximumMegabytesArg, Region aRegion) {
   if (aRegion == null) {
      perEntryOverhead = UNKNOWN;
   } else {
      LRUAlgorithm algor = ((LocalRegion)aRegion).getEvictionController();
      if (algor == null) {
         perEntryOverhead = UNKNOWN;
      } else {
         MemLRUCapacityController capCon = (MemLRUCapacityController)algor;
         perEntryOverhead = capCon.getPerEntryOverhead();
      }
   }
   this.keyLength = keyLength;
   byteArraySize = TestConfig.tab().intAt(CapConPrms.byteArraySize, UNKNOWN);
   maximumMegabytes = maximumMegabytesArg;
   regionByteLimit = maximumMegabytes * MEGABYTE;
   if (keyLength != UNKNOWN) {
      bytesPerKey = WellKnownClassSizer.sizeof(new String(new char[keyLength]));
   }
   if (perEntryOverhead == UNKNOWN) {
      totalBytesPerEntry = UNKNOWN;
      byteArraySize = UNKNOWN;
   } else {
     //byteArraySize is just the length of the byte array. In the java heap
     //the byte array actually takes up additional space for the object header
     //and length.
      int byteArrayOverhead = 16; 
      totalBytesPerEntry = bytesPerKey + byteArraySize + byteArrayOverhead + perEntryOverhead;
      if (OffHeapHelper.isOffHeapMemoryConfigured()) {
        totalBytesPerEntry = byteArraySize + OFF_HEAP_OVERHEAD;
        if ((totalBytesPerEntry % 8) != 0) {
          int nextMultiple = (totalBytesPerEntry / 8) + 1;
          totalBytesPerEntry = nextMultiple * 8;
        }
      }
   }
   setAllowableExcessBytes();
   setMinEvictionBytes();
   if (allowableExcessBytes != UNKNOWN)
      totalAllowableBytes = regionByteLimit + allowableExcessBytes;
   StringBuffer aStr = new StringBuffer();
   if (maximumMegabytes == UNKNOWN)
      aStr.append("maximumMegabytes is " + maximumMegabytes + "\n");
   if (regionByteLimit == UNKNOWN)
      aStr.append("regionByteLimit is " + regionByteLimit + "\n");
   if (bytesPerKey == UNKNOWN)
      aStr.append("bytesPerKey is " + bytesPerKey + "\n");
   if (aStr.length() > 0)
      throw new TestException(aStr.toString()); 
}

// Public getter methods
// ================================================================================

/** Return the size of byte arrays stored as values in the region. 
 */
public int getByteArraySize() {
   return byteArraySize;
}

/** Return the maximum megabytes setting for the memory evictor.
 *
 *  @throws TestException if maximumMegabytes is not available
 */
public int getMaximumMegabytes() {
   if (maximumMegabytes == UNKNOWN)
      throw new TestException("maximumMegabytes " + maximumMegabytes + " is unknown");
   return maximumMegabytes;
}

/** Return the lower limit for the number of bytes that must be in a region before
 *  eviction occurs.
 *
 *  @throws TestException if minEvictionBytes is not available
 */
public long getMinEvictionBytes() {
   return minEvictionBytes;
}

/** Return the fixed keyLength settting.
 *
 *  @throws TestException if keyLength is not available
 */
public int getKeyLength() {
   if (keyLength == UNKNOWN)
      throw new TestException("keyLength " + keyLength + " is unknown");
   return keyLength;
}

/** Return the maximum number of bytes set for a region. This is the number of
 *  bytes in maximumMegabytes.
 *
 *  @throws TestException if regionByteLimit is not available
 */
public long getRegionByteLimit() {
   if (regionByteLimit == UNKNOWN)
      throw new TestException("regionByteLimit " + regionByteLimit + " is unknown");
   return regionByteLimit;
}

/** Return the number of bytes in an entry (inludes key, value and overhead)
 *
 *  @throws TestException if total bytes per entry is not available
 */
public long getTotalBytesPerEntry() {
   if (totalBytesPerEntry == UNKNOWN)
      throw new TestException("totalBytesPerEntry " + totalBytesPerEntry + " is unknown");
   return totalBytesPerEntry;
}

/** Return the number of bytes that the test should allow the region to exceed the
 *  regionByteLimit.
 *
 *  @throws TestException if allowableExcessBytes is not available
 */
public long getAllowableExcessBytes() {
   if (allowableExcessBytes == UNKNOWN)
      throw new TestException("allowableExcessBytes " + allowableExcessBytes + " is unknown");
   return allowableExcessBytes;
}

/** Return the total number of bytes allowd in the region.
 *
 *  @throws TestException totalAllowableBytes is not available
 */
public long getTotalAllowableBytes() {
   if (totalAllowableBytes == UNKNOWN)
      throw new TestException("totalAllowableBytes " + totalAllowableBytes + " is unknown");
   return totalAllowableBytes;
}

/** Return the max number of entries in a region before going over the eviction controller's limit.
 *
 *  @throws TestException if maxEntries is not available
 */
public int getMaxEntries() {
   int maxEntries = (int)(regionByteLimit / totalBytesPerEntry);
   return maxEntries;
}

// String methods
//========================================================================

/** Return a string describing all known and calculated parameters
 */
public String toString() {
   StringBuffer aStr = new StringBuffer();
   aStr.append(super.toString() + "\n");
   aStr.append("   Object overhead values:\n");
   aStr.append("      keyLength: " + keyLength + "\n");
   aStr.append("      bytesPerKey: " + bytesPerKey + "\n");
   aStr.append("      byteArraySize: " + byteArraySize + "\n");
   aStr.append("      perEntryOverhead: " + perEntryOverhead + "\n");
   aStr.append("      totalBytesPerEntry: " + totalBytesPerEntry + "\n");
   aStr.append("   Memory eviction settings:\n");
   aStr.append("      maximumMegabytes: " + maximumMegabytes + "\n");
   aStr.append("   Region limits:\n");
   aStr.append("      regionByteLimit: " + regionByteLimit + "\n");
   aStr.append("      minEvictionBytes: " + minEvictionBytes + "\n");
   aStr.append("      allowableExcessBytes: " + allowableExcessBytes + "\n");
   aStr.append("      totalAllowableBytes: " + totalAllowableBytes + "\n");
   return aStr.toString();
}

// private methods
//========================================================================
private void setAllowableExcessBytes() {
   try {
      int upperLimitDelta = TestConfig.tab().intAt(CapConPrms.upperLimitDelta);
      allowableExcessBytes = upperLimitDelta;
      Log.getLogWriter().info("   Set allowableExcessBytes to " + allowableExcessBytes + 
          "(upperLimitDelta is " + upperLimitDelta +  ", regionByteLimit is " + regionByteLimit + ")");
   } catch (HydraConfigException e) { // no parameter for upperLimitDelta
      if (totalBytesPerEntry == UNKNOWN)
         throw new TestException("Error in hydra configuration, key/value sizes are not always equal throughout test, and upperLimitDelta was not specified");
      String clientName = System.getProperty("clientName");
      ClientDescription cd = TestConfig.getInstance().getClientDescription(clientName);
      int numThreads = cd.getVmThreads(); // numThreads in this VM
      int delta = totalBytesPerEntry * (numThreads * 2); 
      if (OffHeapHelper.isOffHeapMemoryConfigured()) {
        delta = totalBytesPerEntry * ((numThreads * 3) + 1); // off-heap objects might be held in off-heap memory a little longer; old and new
                                                       // will be present for a while, then the stat sampler has to catch up when the old
                                                       // value is removed
      }
      allowableExcessBytes = delta;
      Log.getLogWriter().info("   Set allowableExcessBytes to " + allowableExcessBytes + 
          ", based on numThreads in this VM " + numThreads + " (+ some extra allowance)" +
          ",  and totalBytesPerEntry " + totalBytesPerEntry +
          ", delta is " + delta);
      return;
   }
}

private void setMinEvictionBytes() {
   try {
      int lowerLimitDelta = TestConfig.tab().intAt(CapConPrms.lowerLimitDelta);
      minEvictionBytes = regionByteLimit - lowerLimitDelta;
      Log.getLogWriter().info("   Set minEvictionBytes to " + minEvictionBytes + "(lowerLimitDelta is " +
          lowerLimitDelta + ", regionByteLimit is " + regionByteLimit + ")");
   } catch (HydraConfigException e) { // no parameter for lowerLimitDelta
      if (totalBytesPerEntry == UNKNOWN)
         throw new TestException("Error in hydra configuration, key/value sizes are not always equal throughout test, and lowerLimitDelta was not specified");
      String clientName = System.getProperty("clientName");
      ClientDescription cd = TestConfig.getInstance().getClientDescription(clientName);
      int numThreads = cd.getVmThreads(); // numThreads in this VM
      int delta = totalBytesPerEntry * numThreads;
      minEvictionBytes = regionByteLimit - delta;
      Log.getLogWriter().info("   Set minEvictionBytes to " + minEvictionBytes + 
         ", based on numThreads in this VM is " + numThreads + " and totalBytesPerEntry " + 
         totalBytesPerEntry + ", delta is " + delta);
      return;
   }
}

}
