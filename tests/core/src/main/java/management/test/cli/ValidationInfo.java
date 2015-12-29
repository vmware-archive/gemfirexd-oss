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
package management.test.cli;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.CustomExpiry;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.InterestPolicy;
import com.gemstone.gemfire.cache.MembershipAttributes;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.compression.Compressor;

/** Information to be used to validate MBeans. Many of the items in this class are
 *  in RegionAttributes, which does not always contain Serializable objects, so it can't be returned
 *  in a function or put on the blackboard.
 * @author lynng
 *
 */
public class ValidationInfo implements Serializable {
  
  // information about a member
  public Boolean isManager = null;
  public String member = null;

  // information about a region
  public String regionFullPath = null;
  public Integer regionSize = null;
  public Integer localEntryCount = null;
  public PartitionAttributes prAttr = null;
  public DataPolicy dataPolicy = null;
  public Boolean withPersistence = null;
  public String parentRegionName = null;
  public Integer primaryBucketCount = null;
  public Set<String> directSubregions = null;
  public Set<String> allSubregions = null;
  public List<CacheListener> cacheListeners = null;
  public CacheLoader cacheLoader = null;
  public CacheWriter cacheWriter = null;
  public Compressor compressor = null;
  public Integer concurrencyLevel = null;
  public CustomExpiry customEntryIdleTimeout = null;  
  public CustomExpiry customEntryTimeToLive = null;  
  public String diskStoreName = null;  
  public ExpirationAttributes entryIdleTimeout = null;
  public ExpirationAttributes entryTimeToLive = null;
  public String gatewayHubId = null;
  public Integer initialCapacity = null;
  public InterestPolicy interestPolicy = null;
  public Class keyConstraint = null;
  public Float loadFactor = null;
  public String poolName = null;
  public ExpirationAttributes regionIdleTimeout = null;
  public ExpirationAttributes regionTimeToLive = null;
  public Scope scope = null;
  public Class valueConstraint = null;
  public Boolean asyncConflationEnabled = null;
  public Boolean cloningEnabled = null;
  public Boolean diskSynchronous = null;
  public Boolean gatewayEnabled = null;
  public Boolean ignoreJTA = null;
  public Boolean indexMaintenanceSynchronous = null;
  public Boolean lockGrantor = null;
  public Boolean multicastEnabled = null;
  public Boolean publisher = null;
  public Boolean statisticsEnabled = null;
  public Boolean subscriptionConflationEnabled = null;
  public MembershipAttributes membershipAttr = null;
  public EvictionAttributes evictionAttr = null;
  public Integer numBucketsWithoutRedundancy = null;
  public Boolean enableOffHeapMemory;
  public Boolean hdfsWriteOnly;
  public String hdfsStoreName;

  @Override
  public String toString() {
    return "ValidationInfo [isManager=" + isManager + ", member=" + member
        + ", regionFullPath=" + regionFullPath + ", regionSize=" + regionSize
        + ", localEntryCount=" + localEntryCount + ", prAttr=" + prAttr
        + ", dataPolicy=" + dataPolicy + ", withPersistence=" + withPersistence
        + ", parentRegionName=" + parentRegionName + ", primaryBucketCount="
        + primaryBucketCount + ", directSubregions=" + directSubregions
        + ", allSubregions=" + allSubregions + ", cacheListeners="
        + cacheListeners + ", cacheLoader=" + cacheLoader + ", cacheWriter="
        + cacheWriter + ", compressor=" + compressor
        + ", concurrencyLevel=" + concurrencyLevel
        + ", customEntryIdleTimeout=" + customEntryIdleTimeout
        + ", customEntryTimeToLive=" + customEntryTimeToLive
        + ", diskStoreName=" + diskStoreName + ", entryIdleTimeout="
        + entryIdleTimeout + ", entryTimeToLive=" + entryTimeToLive
        + ", gatewayHubId=" + gatewayHubId + ", initialCapacity="
        + initialCapacity + ", interestPolicy=" + interestPolicy
        + ", keyConstraint=" + keyConstraint + ", loadFactor=" + loadFactor
        + ", poolName=" + poolName + ", regionIdleTimeout=" + regionIdleTimeout
        + ", regionTimeToLive=" + regionTimeToLive + ", scope=" + scope
        + ", valueConstraint=" + valueConstraint + ", asyncConflationEnabled="
        + asyncConflationEnabled + ", cloningEnabled=" + cloningEnabled
        + ", diskSynchronous=" + diskSynchronous + ", gatewayEnabled="
        + gatewayEnabled + ", ignoreJTA=" + ignoreJTA
        + ", indexMaintenanceSynchronous=" + indexMaintenanceSynchronous
        + ", lockGrantor=" + lockGrantor + ", multicastEnabled="
        + multicastEnabled + ", publisher=" + publisher
        + ", statisticsEnabled=" + statisticsEnabled
        + ", subscriptionConflationEnabled=" + subscriptionConflationEnabled
        + ", membershipAttr=" + membershipAttr + ", evictionAttr="
        + evictionAttr + ", numBucketsWithoutRedundancy="
        + numBucketsWithoutRedundancy + ", enableOffHeapMemory="
        + enableOffHeapMemory + ", hdfsWriteOnly="
        + hdfsWriteOnly + ", hdfsStoreName="
        + hdfsStoreName + "]";
  }
  

}
