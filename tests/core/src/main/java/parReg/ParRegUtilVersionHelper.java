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
/**
 * 
 */
package parReg;

import hydra.CacheHelper;
import hydra.Log;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.EntryDestroyedException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;

import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.PdxSerializable;

import com.gemstone.gemfire.internal.cache.BucketDump;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.versions.RegionVersionHolder;
import com.gemstone.gemfire.internal.cache.versions.RegionVersionVector;
import com.gemstone.gemfire.internal.cache.versions.VersionSource;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;

import pdx.PdxTest;
import pdx.PdxTestVersionHelper;
import util.TestException;
import util.TestHelper;
import util.BaseValueHolder;

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import java.util.Set;

/**
 * @author lynn
 *
 */
public class ParRegUtilVersionHelper {

  /** Verify that the given object is an instance of ValueHolder
   *  with expectedValue as the myValue field.
   *
   * @param key The key whose value we are checking.
   * @param expectedValue The expected myValue field of a ValueHolder in aRegion, or null
   *        if the expected value should be null.
   * @param valuetoCheck This is expected to be a ValueHolder or PdxInstance
   *        representing a ValueHolder, whose myValue field compares
   *        to expectedValue, according to comparStrategy
   * @param compareStrategy Whether the compare is equals or equivalent (for ValueHolders)
   *
   * @throws TestException if the result of a get on key does not have the expected value.
   */
  public static void verifyMyValue(Object key, Object expectedValue,
      Object valueToCheck, int compareStrategy) {
    if (valueToCheck == null) {
      if (expectedValue != null) {
         throw new TestException("For key " + key + ", expected myValue to be " + 
                   TestHelper.toString(expectedValue) + 
                   ", but it is " + TestHelper.toString(valueToCheck));
      }
   } else if ((valueToCheck instanceof BaseValueHolder) ||
              (valueToCheck instanceof PdxInstance)) {
      BaseValueHolder actualVH = PdxTest.toValueHolder(valueToCheck);
      if (actualVH.myValue == null) {
        Log.getLogWriter().info("valueToCheck is " + TestHelper.toString(valueToCheck));
        Log.getLogWriter().info("actualVH is " + TestHelper.toString(actualVH));
        throw new TestException("For key " + key + ", " + TestHelper.toString(actualVH) + " has unexpected null myValue field, expect it to be " + TestHelper.toString(expectedValue));
      }
      if (compareStrategy == ParRegUtil.EQUAL) {
         if (!actualVH.myValue.equals(expectedValue)) {
            Log.getLogWriter().info("expectedValue is " + TestHelper.toString(expectedValue));
            Log.getLogWriter().info("valueToCheck is " + TestHelper.toString(valueToCheck));
            Log.getLogWriter().info("actualVH is " + TestHelper.toString(actualVH));
            throw new TestException("For key " + key + ", expected ValueHolder.myValue to be " + 
                   TestHelper.toString(expectedValue) + 
                   ", but it is " + TestHelper.toString(actualVH));
         }
      } else if (compareStrategy == ParRegUtil.EQUIVALENT) { 
         if (actualVH.myValue.toString().equals(expectedValue.toString())) {
            Log.getLogWriter().info("valueToCheck is " + TestHelper.toString(valueToCheck));
            Log.getLogWriter().info("actualVH is " + TestHelper.toString(actualVH));
            throw new TestException("For key " + key + ", expected ValueHolder.myValue to be " + 
                      expectedValue + ", but it is " + actualVH.myValue);
         }
      }
      
      // checking for pdx values
      if (actualVH instanceof PdxSerializable) {
        String searchStr = "updated_"; // used for known keys style tests
        if (expectedValue.toString().startsWith(searchStr)) { 
          actualVH.verifyMyFields(expectedValue.toString(), Long.valueOf(expectedValue.toString().substring(searchStr.length())));
        } else {
          actualVH.verifyMyFields(Long.valueOf(expectedValue.toString()));
        }
      }
   } else {
      throw new TestException("Expected value for key " + key + " to be an instance of ValueHolder, but it is " +
         TestHelper.toString(valueToCheck));
   }
  }

  //=================================
  // 7.0 additions for RVV Versioning 
  //=================================

  /** Return the versionTag for the given Region and key.
   *
   * @param aRegion The Region 
   * @param key The key 
   *
   * @returns A String representation of the VersionTag for key in Region.
   */
  public static String getVersionTagStr(Region aRegion, Object key) {
    String versionTagStr = "";

    RegionAttributes attr = aRegion.getAttributes();
    if (attr.getConcurrencyChecksEnabled()) {
      Object version = null;
      try {
        version = ((LocalRegion)aRegion).getVersionTag(key);
      } catch (EntryDestroyedException e) {
        // If EntryDestroyedException is triggered here, the key should be a tombstone and
        // GCed inside getVersionTag(). version=null is accepted in this case
      }
      versionTagStr = ", my version=" + version;
    }
    return versionTagStr;
  }

  /** dumpBackingMap of version information (into system log)
   *
   * @param aRegion The Region to dump
   */
  public static void dumpBackingMap(Region aRegion) {
    ((LocalRegion)aRegion).dumpBackingMap();
  }

  /** Compare the contents of 2 BucketDumps. Return a string describing
   *  any discrepancies.
   *
   *  @param _dump1 A BucketDump of keys/values, ie a bucket. which includes versioning information
   *  @param _dump2 A BucketDump of keys/values, ie a bucket. which includes versioning information 
   */
  public static String compareBucketMaps(Object _dump1, Object _dump2) {
    BucketDump dump1 = (BucketDump)_dump1;
    BucketDump dump2 = (BucketDump)_dump2;
  
    StringBuilder aStr = new  StringBuilder();
  
    compareRVVs(aStr, dump1, dump2);
   
    Map<Object, Object> map1 = dump1.getValues();
    Map<Object, VersionTag> versions1 = dump1.getVersions();
    String map1LogStr = getBucketMapStr(map1);
   
    Map<Object, Object> map2 = dump2.getValues();
    Map<Object, VersionTag> versions2 = dump2.getVersions();
    String map2LogStr = getBucketMapStr(map2);

   if (map1.size() != map2.size()) {
      aStr.append("Bucket map <" + map1LogStr + "> is size " + 
           map1.size() + " and bucket map <" + map2LogStr + "> is size " + map2.size() + "\n"); 
   }
   Iterator it = map1.keySet().iterator();
   while (it.hasNext()) {
      Object key = it.next();
      VersionTag version = versions1.get(key);
      Object value = PdxTestVersionHelper.toBaseObject(map1.get(key));
      if (map2.containsKey(key)) {
         VersionTag map2Version = versions2.get(key);
         Object map2Value = PdxTestVersionHelper.toBaseObject(map2.get(key));
         try {
            ParRegUtil.verifyValue(key, value, map2Value, ParRegUtil.EQUAL);
            verifyVersionTags(version, map2Version);
         } catch (TestException e) {
           String version1Str = version == null ? "" : " version " + version;
           String version2Str = map2Version == null ? "" : " version " + map2Version;
            aStr.append("Bucket map <" +  map1LogStr +
               "> has key " + key + ", value " + TestHelper.toString(value) +
               version1Str + ", but bucket map <" + map2LogStr + "> has key " + key 
               + ", value " + TestHelper.toString(map2Value) + version2Str 
               + "; " + e.getMessage() + "\n"); 
         }
      } else {
          aStr.append("Bucket map <" + map1LogStr +
             "> contains key " + key + ", but bucket map <" + map2LogStr + 
             "> does not contain key " + key + "\n");
      }
   }

   // We have verified that every key/value in map1 is also in map2.
   // Now look for any keys in map2 that are not in map1.
   Set map1Keys = map1.keySet();
   Set map2Keys =  map2.keySet();
   map2Keys.removeAll(map1Keys);
   if (map2Keys.size() != 0) {
      aStr.append("Found extra keys in bucket map <" + map2LogStr +
         ">, that were not found in bucket map <" + map1LogStr + ">: " + map2Keys + "\n");
   }

   if (aStr.length() > 0) {
     Log.getLogWriter().info("Bucket map 1: " + map1);
     Log.getLogWriter().info("Bucket map 2: " + map2);
   }
   return aStr.toString();
}

  /** The map that is used for comparing bucket copies (bucketMap) is too big in some runs to log in 
   *  exceptions, causing OOM problems. Return a string that eliminates the contents of the map
   *  but keeps the other information such as the member that hosts this bucket map.
   * @param bucketMap A bucket map from verifyBucketCopies
   * @return A toString() version of the bucket map with entries removed.
   */
  private static String getBucketMapStr(Map<Object, Object> bucketMap) {
    String bucketMapStr = bucketMap.toString();
    StringBuilder reducedStr = new StringBuilder();
    int index = bucketMapStr.indexOf("{");
    if (index < 0) {
      return bucketMapStr;
    }
    reducedStr.append(bucketMapStr.substring(0, index));
    index = bucketMapStr.lastIndexOf("}");
    if (index < 0) {
      return bucketMapStr;
    }
    reducedStr.append(bucketMapStr.substring(index+1, bucketMapStr.length()));
    return reducedStr.toString();
  }

  private static void compareRVVs(StringBuilder aStr, BucketDump dump1, BucketDump dump2) {
    RegionVersionVector rvv1 = dump1.getRvv();
    RegionVersionVector rvv2 = dump2.getRvv();
    if(rvv1 == null) {
      if(rvv2 != null) {
        aStr.append(dump2 + " has an RVV, but " + dump1 + " does not");
      }
    }
    else {
      if(rvv2 == null) {
        aStr.append(dump1 + " has an RVV, but " + dump2 + " does not");
      }
      else {
        Map<VersionSource, RegionVersionHolder> rvv2Members = new HashMap<VersionSource, RegionVersionHolder>(rvv1.getMemberToVersion());
        Map<VersionSource, RegionVersionHolder> rvv1Members = new HashMap<VersionSource, RegionVersionHolder>(rvv1.getMemberToVersion());
        for(Map.Entry<VersionSource, RegionVersionHolder> entry : rvv1Members.entrySet()) {
          VersionSource memberId = entry.getKey();
          RegionVersionHolder versionHolder1 = entry.getValue();
          RegionVersionHolder versionHolder2 = rvv2Members.remove(memberId);
          if(versionHolder2 != null) {
            if(!versionHolder1.equals(versionHolder2)) {
              aStr.append(dump1 + " RVV does not match RVV for " + dump2 + "\n");
              aStr.append("RVV for " + dump1 + ":" + versionHolder1 + "\n");
              aStr.append("RVV for " + dump2 + ":" + versionHolder2 + "\n");
            }
  
          } else {
            // Don't fail the test if rvv1 has member that were not present in rvv2.
            // It's possible that rvv1 has an old member that rvv1 does not, and rvv1
            // has not GC'd that member from the RVV.
          }
  
        }
  
        // Don't fail the test if rvv2 has member that were not present in rvv1.
        // It's possible that rvv2 has an old member that rvv1 does not, and rvv2
        // has not GC'd that member from the RVV.
      }
    }
  }

  private static void verifyVersionTags(VersionTag version, VersionTag version2) {
    boolean equal = version == null && version2 == null || 
                    version != null & version2 != null && version.equals(version2);
    if(!equal) {
      throw new TestException("Version tag mismatch");
    }
  }
}
