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
package util;

import hydra.*;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.control.*;

/** Class used to define a valid combination of attributes and specifications
 *  for a cache.
 */
public class CacheDefinition extends Definition {

// cache attributes that can be defined
private Boolean copyOnRead = null;
private Integer lockTimeout = null;
private Integer lockLease = null;
private Integer searchTimeout = null;
private Float evictionHeapPercentage = null;
private Float criticalHeapPercentage = null;

// tokens for parsing
private static final String COPY_ON_READ_TOKEN = "copyOnRead";
private static final String LOCK_TIMEOUT_TOKEN = "lockTimeout";
private static final String LOCK_LEASE_TOKEN = "lockLease";
private static final String SEARCH_TIMEOUT_TOKEN = "searchTimeout";
private static final String EVICTION_HEAP_TOKEN = "evictionHeapPercentage";
private static final String CRITICAL_HEAP_TOKEN = "criticalHeapPercentage";



//================================================================================
// Constructor
private CacheDefinition() {
}

//================================================================================
// private methods

/** Initialize this instance with the cache spec given by the String.
 *
 *  @param cacheSpecStr A string containing a cache spec.
 *
 */
protected void initializeWithSpec(String cacheSpecStr) {
   tokenArr = cacheSpecStr.split(ATTR_DELIM);   
   String token = getNextToken(); // specName
   specName = token;
   token = getNextToken(); // should be a keyword token
   while (token != null) {
      if (token.equalsIgnoreCase(COPY_ON_READ_TOKEN)) {
         getTokenEquals();
         copyOnRead = getTokenBoolean();
      } else if (token.equalsIgnoreCase(LOCK_TIMEOUT_TOKEN)) {
         getTokenEquals();
         lockTimeout = getTokenInteger();
      } else if (token.equalsIgnoreCase(LOCK_LEASE_TOKEN)) {
         getTokenEquals();
         lockLease = getTokenInteger();
      } else if (token.equalsIgnoreCase(SEARCH_TIMEOUT_TOKEN)) {
         getTokenEquals();
         searchTimeout = getTokenInteger();
      } else if (token.equalsIgnoreCase(EVICTION_HEAP_TOKEN)) {
          getTokenEquals();
          evictionHeapPercentage = getTokenFloat();
       } else if (token.equalsIgnoreCase(CRITICAL_HEAP_TOKEN)) {
          getTokenEquals();
          criticalHeapPercentage = getTokenFloat();
        } else {
         throw new TestException("Unknown keyword " + token);
      }
      token = getNextToken();
   }
}

//================================================================================
// public instance methods

/** Return this instance as a string.
 */
public String toString() {
   StringBuffer aStr = new StringBuffer();
   aStr.append("CacheDefinition with name " + specName + "\n");
   aStr.append("   copyOnRead: " + copyOnRead + "\n");
   aStr.append("   lockTimeout: " + lockTimeout + "\n");
   aStr.append("   lockLease: " + lockLease + "\n");
   aStr.append("   searchTimeout: " + searchTimeout + "\n");
   aStr.append("   evictionHeapPercentage: " + evictionHeapPercentage + "\n");
   aStr.append("   criticalHeapPercentage: " + criticalHeapPercentage + "\n");
   return aStr.toString();
}

/** Create a cache using this instance of CacheDefinition. If a cache 
 *  has already been created, then throw an error.
 *
 *  @param xmlFile The name of an xmlFile to use when the cache is created.
 *         This will cause the creation of any regions and/or entries.
 *
 *  @return The instance of Cache.
 */
public Cache createCache(String xmlFile) {
   Cache theCache = CacheUtil.getCache();
   if (theCache != null)
      throw new TestException("Cache has already been created");
   theCache = CacheUtil.createCache(xmlFile);
   if (copyOnRead != null)
      theCache.setCopyOnRead(copyOnRead.booleanValue());
   if (lockTimeout != null)
      theCache.setLockTimeout(lockTimeout.intValue());
   if (lockLease != null)
      theCache.setLockLease(lockLease.intValue());
   if (searchTimeout != null)
      theCache.setSearchTimeout(searchTimeout.intValue());
   if (evictionHeapPercentage != null) {
	ResourceManager rm = theCache.getResourceManager();
	rm.setEvictionHeapPercentage(evictionHeapPercentage);
   }
   if (criticalHeapPercentage != null) {
	ResourceManager rm = theCache.getResourceManager();
	rm.setCriticalHeapPercentage(criticalHeapPercentage);
   }
   Log.getLogWriter().info("Finished creating " + theCache + " with " + this);
   return theCache;
}

/** Create a cache using this instance of CacheDefinition. If a cache 
 *  has already been created, then throw an error.
 *
 *  @return The instance of Cache.
 */
public Cache createCache() {
   return createCache(null);
}

//================================================================================
// public static methods

/** Creates a cache definition with the specified specName. 
 *
 *  @param hydraSpecParam The name of the hydra parameter containing the spec string.
 *  @param specName The name of the attribute specification, defined in hydraSpecParam.
 *
 *  @return A CacheDefinition as described above.
 *
 *  @throws TestException if there is no spec named <code>specName</code>
 */
public static CacheDefinition createCacheDefinition(Long hydraSpecParam, String specName) {
   CacheDefinition cacheDef = new CacheDefinition();
   cacheDef.getDefinition(hydraSpecParam, specName);
   return cacheDef;
}

//================================================================================
// getter methods

public Boolean getCopyOnRead() {
   return copyOnRead;
}

public Integer getLockTimeout() {
   return lockTimeout;
}

public Integer getLockLease() {
   return lockLease;
}

public Integer getSearchTimeout() {
   return searchTimeout;
}

public Float getEvictionHeapPercentage() {
  return this.evictionHeapPercentage;
}

public Float getCriticalHeapPercentage() {
  return this.criticalHeapPercentage;
}

//================================================================================
// setter methods (in case anybody wants to override these settings)

public void setCopyOnRead(boolean copyOnReadArg) {
   copyOnRead = new Boolean(copyOnReadArg);
}

public void setLockTimeout(int lockTimeoutArg) {
   lockTimeout = new Integer(lockTimeoutArg);
}

public void setLockLease(int lockLeaseArg) {
   lockLease = new Integer(lockLeaseArg);
}

public void setSearchTimeout(int searchTimeoutArg) {
   searchTimeout = new Integer(searchTimeoutArg);
}

public void setEvictionHeapPercentage(float pct) {
  this.evictionHeapPercentage = pct;
}
public void setCriticalHeapPercentage(float pct) {
  this.criticalHeapPercentage = pct;
}


}
