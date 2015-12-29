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
import java.io.File;
import java.io.ByteArrayInputStream;
import java.util.*;

import memscale.OffHeapHelper;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.*;


/** Class used to define a valid combination of attributes and specifications
 *  for a region. Since some combinations are illegal, this gives a test
 *  an easier way to consider all valid combinations.
 *
 *  @see RegionDefPrms
 */
public class RegionDefinition extends Definition {

  /** The system property that is set to the name of the parent
      directory of the GFX disk directories.  See bug 31753. */
  public static final String GFX_DISK_DIR_PARENT_PROP = "gfx.disk.dir.parent";

// region attributes that can be defined
private Scope scope = null;
private MirrorType mirroring = null;
private DataPolicy dataPolicy = null;
private Boolean concurrencyChecksEnabled = null;
private SubscriptionAttributes subscriptionAttributes = null;
private Integer concurrencyLevel = null;
private Integer entryIdleTimeoutSec = null;
private ExpirationAction entryIdleTimeoutAction = null;
private Integer entryTTLSec = null;
private ExpirationAction entryTTLAction = null;
private Integer regionIdleTimeoutSec = null;
private ExpirationAction regionIdleTimeoutAction = null;
private Integer regionTTLSec = null;
private ExpirationAction regionTTLAction = null;
private Class keyConstraint = null;
private Class valueConstraint = null;
private Float loadFactor = null;
private Boolean statisticsEnabled = null;
private String enableOffHeapMemory = null;
private Boolean persistBackup = null;
private Integer evictionLimit = null;
private Boolean asyncConflation = null;
private Boolean isSynchronous = null;
private Long timeInterval = null;
private Long bytesThreshold = null;
private Integer numDiskDirs = null;
private String evictionAction = null;
private List diskDirList = null;
private String eviction = null;
private List cacheListeners = null;
private String cacheLoader = null;
private String cacheWriter = null;
private String objectSizerClass = null;
private Boolean indexMaintenanceSynchronous = null;
private List requiredRoles = null;
private LossAction lossAction = null;
private ResumptionAction resumptionAction = null;
private Boolean parReg = null;
private String parReg_cacheWriter = null;
private String parReg_partitionResolver = null;
private String parReg_colocatedWith = null;
private Integer parReg_entryIdleTimeoutSec = null;
private ExpirationAction parReg_entryIdleTimeoutAction = null;
private Integer parReg_entryTTLSec = null;
private ExpirationAction parReg_entryTTLAction = null;
private Properties parReg_localProperties = null;
private Properties parReg_globalProperties = null;
// private Properties parReg_partitionResolver_properties = null;
private Integer parReg_redundantCopies = null;
private Boolean multicastEnabled = null;
private Boolean enableWAN = null;
private Boolean rollOplogs = null;
private Integer maxOplogSize = null;

// other information about a region that is not an attribute
private String regionName = null;
private ArrayList subRegions = new ArrayList();

// other variables used internally
public static final String REGION_DEF_KEY = "OneRegionPerTest";
public static final String EVICTION_LRU_STR = "LRU"; 
public static final String EVICTION_MEM_LRU_STR = "MemLRU"; 
public static final String EVICTION_HEAP_STR = "Heap"; 
public static final String NONE = "none"; 
// whether to use the muilticast setting for the distributed system 
// in the region defn
private Boolean useDsMulticastSetting = null;


// tokens for parsing
private static final String REGION_NAME_TOKEN = "regionName";
private static final String SCOPE_TOKEN = "scope";
private static final String MIRROR_TOKEN = "mirroring";
private static final String DATA_POLICY_TOKEN = "dataPolicy";
private static final String INTEREST_POLICY_TOKEN = "interestPolicy";
private static final String CONCURRENCY_LEVEL_TOKEN = "concurrencyLevel";
private static final String ENTRY_IDLE_TIMEOUT_SEC_TOKEN = "entryIdleTimeoutSec";
private static final String ENTRY_IDLE_TIMEOUT_ACTION_TOKEN = "entryIdleTimeoutAction";
private static final String ENTRY_TTL_SEC_TOKEN = "entryTTLSec";
private static final String ENTRY_TTL_ACTION_TOKEN = "entryTTLAction";
private static final String REGION_IDLE_TIMEOUT_SEC_TOKEN = "regionIdleTimeoutSec";
private static final String REGION_IDLE_TIMEOUT_ACTION_TOKEN = "regionIdleTimeoutAction";
private static final String REGION_TTL_SEC_TOKEN = "regionTTLSec";
private static final String REGION_TTL_ACTION_TOKEN = "regionTTLAction";
private static final String KEY_CONSTRAINT_TOKEN = "keyConstraint";
private static final String VALUE_CONSTRAINT_TOKEN = "valueConstraint";
private static final String LOAD_FACTOR_TOKEN = "loadFactor";
private static final String STATISTICS_ENABLED_TOKEN = "statisticsEnabled";
private static final String ENABLE_OFF_HEAP_MEMORY = "enableOffHeapMemory";
private static final String PERSIST_BACKUP_TOKEN = "persistBackup";
private static final String EVICTION_LIMIT_TOKEN = "evictionLimit";
private static final String ASYNC_CONFLATION_TOKEN = "asyncConflation";
private static final String IS_SYNCHRONOUS_TOKEN = "isSynchronous";
private static final String TIME_INTERVAL_TOKEN = "timeInterval";
private static final String BYTES_THRESHOLD_TOKEN = "bytesThreshold";
private static final String NUM_DISK_FILES_TOKEN = "numDiskDirs";
private static final String EVICTION_ACTION_TOKEN = "evictionAction";
private static final String DISK_DIR_LIST_TOKEN = "diskDirList";
private static final String EVICTION_TOKEN = "eviction";
private static final String CACHE_LISTENERS_TOKEN = "cacheListeners";
private static final String CACHE_LOADER_TOKEN = "cacheLoader";
private static final String CACHE_WRITER_TOKEN = "cacheWriter";
private static final String OBJECT_SIZER_CLASS_TOKEN = "objectSizerClass";
private static final String INDEX_MAINTENANCE_SYNCHRONOUS_TOKEN = "indexMaintenanceSynchronous";
private static final String REQUIRED_ROLES_TOKEN = "requiredRoles";
private static final String LOSS_ACTION_TOKEN = "lossAction";
private static final String RESUMPTION_ACTION_TOKEN = "resumptionAction";
private static final String MULTICAST_ENABLED_TOKEN = "multicastEnabled";
private static final String USE_DS_MULTICAST_SETTING_TOKEN = "useDsMulticastSetting";
private static final String ENABLE_WAN_TOKEN = "enableWAN";
private static final String CONCURRENCY_CHECKS_ENABLED_TOKEN = "concurrencyChecksEnabled";

// partitionedRegion tokens
private static final String PARREG_TOKEN = "PartitionedRegion";
private static final String PARREG_CACHE_WRITER_TOKEN = "ParReg_cacheWriter";
private static final String PARREG_ROUTING_RESOLVER_TOKEN = "ParReg_routingResolver";
private static final String PARREG_COLOCATED_WITH_TOKEN = "ParReg_colocatedWith";
private static final String PARREG_ENTRY_IDLE_TIMEOUT_SEC_TOKEN = "ParReg_entryIdleTimeoutSec";
private static final String PARREG_ENTRY_IDLE_TIMEOUT_ACTION_TOKEN = "ParReg_entryIdleTimeoutAction";
private static final String PARREG_ENTRY_TTL_SEC_TOKEN = "ParReg_entryTTLSec";
private static final String PARREG_ENTRY_TTL_ACTION_TOKEN = "ParReg_entryTTLAction";
private static final String PARREG_LOCAL_PROPERTIES = "ParReg_localProperties";
private static final String PARREG_GLOBAL_PROPERTIES = "ParReg_globalProperties";
private static final String PARREG_ROUTING_RESOLVER_PROPERTIES = "ParReg_routingResolver_properties";
private static final String PARREG_REDUNDANT_COPIES = "ParReg_redundantCopies";

///added by Prafulla for changes done in disk region implementation 
private static final String ROLL_OPLOGS_TOKEN = "rollOplogs";
private static final String MAX_OPLOG_SIZE_TOKEN = "maxOplogSize";

// Legal values for RegionDefPrms.regionDefUsage
public static final String USAGE_ANY = "useAnyRegionSpec";
public static final String USAGE_ONE = "useOneRegionSpec";
public static final String USAGE_NONE = "none";
public static final String USAGE_FIXED_SEQUENCE = "useFixedSequence";

//================================================================================
// Constructor
private RegionDefinition() {
}

//================================================================================
// private methods

/** Get the desired cacheLoader instance
 */
private CacheLoader getCacheLoaderInstance(String className) {
   try {
      return (CacheLoader)(TestHelper.createInstance(Class.forName(className)));
   } catch (ClassNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

/** Get the desired cacheWriter instance
 */
private CacheWriter getCacheWriterInstance(String className) {
   try {
      return (CacheWriter)(TestHelper.createInstance(Class.forName(className)));
   } catch (ClassNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

/** Get the desired PartitionResolver instance
 */
private PartitionResolver getPartitionResolverInstance(String className) {
   try {
      return (PartitionResolver)(TestHelper.createInstance(Class.forName(className)));
   } catch (ClassNotFoundException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
}

/**
 * Read Hydra params and fill in this instance of
 * <code>RegionDefinition</code>.  
 */
private void getRegionDefinition() {
   String regionSpecName = null;
   Long key = util.RegionDefPrms.VMRegionSpecName;
   Object tmp = TestConfig.tasktab().get( key, TestConfig.tab().get(key) );
   if (tmp instanceof OneOf) {
      OneOf oneof = (OneOf)tmp;
      regionSpecName = (String)(oneof.next(RegDefRand));
   } else {
      regionSpecName = (String)tmp;
   }
   getDefinition(RegionDefPrms.regionSpecs, regionSpecName);
}

/**
 * Create a new RegionDefinition with ReliabilityPrms specified
 *
 * @return RegionDefinition - regionDefinition based on ReliabilityPrms
 */
public static RegionDefinition createReliabilityDefinition() {
   RegionDefinition relDef = null;
   Long key = RegionDefPrms.reliabilitySpecName;
   String specName = TestConfig.tasktab().stringAt(key, TestConfig.tab().stringAt(key, null));
   if (specName != null) {
      Log.getLogWriter().info("Using membership attributes from spec: " + specName);
      relDef = new RegionDefinition();
      relDef.getDefinition(ReliabilityPrms.reliabilitySpecs, specName);
   }
   return relDef;
}

/** merge the ReliabiltyAttributes given into this RegionDefinition instance
 *
 *  @param reliabilityDef - RegionDefinion build using ReliabailityPrms
 */
public void mergeReliabilityDefinition(RegionDefinition reliabilityDef) {
   // Typically, apps don't have reliabilityAttrs to add in
   if (reliabilityDef == null) {
     return;
   }

   this.requiredRoles = reliabilityDef.getRequiredRoles();
   this.lossAction = reliabilityDef.getLossAction();
   this.resumptionAction = reliabilityDef.getResumptionAction();
}

/** Initialize this instance with the region spec given by the String.
 *
 *  @param regionSpecStr A string containing a region spec.
 *
 */
protected void initializeWithSpec(String regionSpecStr) {
   tokenArr = regionSpecStr.split(ATTR_DELIM);   
   String token = getNextToken(); // specName
   specName = token;
   token = getNextToken(); // should be a keyword token
   while (token != null) {
      if (token.equalsIgnoreCase(REGION_NAME_TOKEN)) {
         getTokenEquals();
         regionName = getTokenString();
      } else if (token.equalsIgnoreCase(SCOPE_TOKEN)) {
         getTokenEquals();
         scope = getTokenScope();
      } else if (token.equalsIgnoreCase(MIRROR_TOKEN)) {
         getTokenEquals();
         mirroring = getTokenMirroring();
      } else if (token.equalsIgnoreCase(DATA_POLICY_TOKEN)) {
         getTokenEquals();
         dataPolicy = getTokenDataPolicy();
      } else if (token.equalsIgnoreCase(CONCURRENCY_CHECKS_ENABLED_TOKEN)) {
         getTokenEquals();
         concurrencyChecksEnabled = getTokenBoolean();
      } else if (token.equalsIgnoreCase(INTEREST_POLICY_TOKEN)) {
         getTokenEquals();
         subscriptionAttributes = new SubscriptionAttributes(getTokenInterestPolicy());
      } else if (token.equalsIgnoreCase(CONCURRENCY_LEVEL_TOKEN)) {
         getTokenEquals();
         concurrencyLevel = getTokenInteger();
      } else if (token.equalsIgnoreCase(ENTRY_IDLE_TIMEOUT_SEC_TOKEN)) {
         getTokenEquals();
         entryIdleTimeoutSec = getTokenInteger();
      } else if (token.equalsIgnoreCase(ENTRY_IDLE_TIMEOUT_ACTION_TOKEN)) {
         getTokenEquals();
         entryIdleTimeoutAction = getTokenAction();
      } else if (token.equalsIgnoreCase(ENTRY_TTL_SEC_TOKEN)) {
         getTokenEquals();
         entryTTLSec = getTokenInteger();
      } else if (token.equalsIgnoreCase(ENTRY_TTL_ACTION_TOKEN)) {
         getTokenEquals();
         entryTTLAction = getTokenAction();
      } else if (token.equalsIgnoreCase(REGION_IDLE_TIMEOUT_SEC_TOKEN)) {
         getTokenEquals();
         regionIdleTimeoutSec = getTokenInteger();
      } else if (token.equalsIgnoreCase(REGION_IDLE_TIMEOUT_ACTION_TOKEN)) {
         getTokenEquals();
         regionIdleTimeoutAction = getTokenAction();
      } else if (token.equalsIgnoreCase(REGION_TTL_SEC_TOKEN)) {
         getTokenEquals();
         regionTTLSec = getTokenInteger();
      } else if (token.equalsIgnoreCase(REGION_TTL_ACTION_TOKEN)) {
         getTokenEquals();
         regionTTLAction = getTokenAction();
      } else if (token.equalsIgnoreCase(KEY_CONSTRAINT_TOKEN)) {
         getTokenEquals();
         keyConstraint = getTokenClass();
      } else if (token.equalsIgnoreCase(VALUE_CONSTRAINT_TOKEN)) {
         getTokenEquals();
         valueConstraint = getTokenClass();   
      } else if (token.equalsIgnoreCase(LOAD_FACTOR_TOKEN)) {
         getTokenEquals();
         loadFactor = getTokenFloat();
      } else if (token.equalsIgnoreCase(STATISTICS_ENABLED_TOKEN)) {
         getTokenEquals();
         statisticsEnabled = getTokenBoolean();
      } else if (token.equalsIgnoreCase(ENABLE_OFF_HEAP_MEMORY)) {
         getTokenEquals();
         enableOffHeapMemory = getTokenString();
      } else if (token.equalsIgnoreCase(PERSIST_BACKUP_TOKEN)) {
         getTokenEquals();
         persistBackup = getTokenBoolean();
      } else if (token.equalsIgnoreCase(EVICTION_LIMIT_TOKEN)) {
         getTokenEquals();
         evictionLimit = getTokenInteger();
      } else if (token.equalsIgnoreCase(ASYNC_CONFLATION_TOKEN)) {
         getTokenEquals();
         asyncConflation = getTokenBoolean();
      } else if (token.equalsIgnoreCase(IS_SYNCHRONOUS_TOKEN)) {
         getTokenEquals();
         isSynchronous = getTokenBoolean();
      } else if (token.equalsIgnoreCase(TIME_INTERVAL_TOKEN)) {
         getTokenEquals();
         timeInterval = getTokenLong();
      } else if (token.equalsIgnoreCase(BYTES_THRESHOLD_TOKEN)) {
         getTokenEquals();
         bytesThreshold = getTokenLong();
      } else if (token.equalsIgnoreCase(NUM_DISK_FILES_TOKEN)) {
         getTokenEquals();
         numDiskDirs = getTokenInteger();
      } else if (token.equalsIgnoreCase(EVICTION_ACTION_TOKEN)) {
         getTokenEquals();
         evictionAction = getTokenString();
      } else if (token.equalsIgnoreCase(DISK_DIR_LIST_TOKEN)) {
         getTokenEquals();
         diskDirList = getTokenList();
      } else if (token.equalsIgnoreCase(EVICTION_TOKEN)) {
         getTokenEquals();
         eviction = getTokenString();
      } else if (token.equalsIgnoreCase(CACHE_LISTENERS_TOKEN)) {
         getTokenEquals();
         cacheListeners = getTokenList();
      } else if (token.equalsIgnoreCase(CACHE_LOADER_TOKEN)) {
         getTokenEquals();
         cacheLoader = getTokenString();
      } else if (token.equalsIgnoreCase(CACHE_WRITER_TOKEN)) {
         getTokenEquals();
         cacheWriter = getTokenString();
      } else if (token.equalsIgnoreCase(OBJECT_SIZER_CLASS_TOKEN)) {
         getTokenEquals();
         objectSizerClass = getTokenString();
      } else if (token.equalsIgnoreCase(INDEX_MAINTENANCE_SYNCHRONOUS_TOKEN)) {
         getTokenEquals();
         indexMaintenanceSynchronous = getTokenBoolean();   
      } else if (token.equalsIgnoreCase(REQUIRED_ROLES_TOKEN)) {
         getTokenEquals();
         requiredRoles = getTokenList();
      } else if (token.equalsIgnoreCase(LOSS_ACTION_TOKEN)) {
         getTokenEquals();
         lossAction = getTokenLossAction();
      } else if (token.equalsIgnoreCase(RESUMPTION_ACTION_TOKEN)) {
         getTokenEquals();
         resumptionAction = getTokenResumptionAction();
      } else if (token.equalsIgnoreCase(PARREG_TOKEN)) {
         getTokenEquals();
         parReg = getTokenBoolean();   
      } else if (token.equalsIgnoreCase(PARREG_CACHE_WRITER_TOKEN)) {
         getTokenEquals();
         parReg_cacheWriter = getTokenString();
      } else if (token.equalsIgnoreCase(PARREG_ROUTING_RESOLVER_TOKEN)) {
         getTokenEquals();
         parReg_partitionResolver = getTokenString();
      } else if (token.equalsIgnoreCase(PARREG_COLOCATED_WITH_TOKEN)) {
         getTokenEquals();
         parReg_colocatedWith = getTokenString();
      } else if (token.equalsIgnoreCase(PARREG_ENTRY_IDLE_TIMEOUT_SEC_TOKEN)) {
         getTokenEquals();
         parReg_entryIdleTimeoutSec = getTokenInteger();
      } else if (token.equalsIgnoreCase(PARREG_ENTRY_IDLE_TIMEOUT_ACTION_TOKEN)) {
         getTokenEquals();
         parReg_entryIdleTimeoutAction = getTokenAction();
      } else if (token.equalsIgnoreCase(PARREG_ENTRY_TTL_SEC_TOKEN)) {
         getTokenEquals();
         parReg_entryTTLSec = getTokenInteger();
      } else if (token.equalsIgnoreCase(PARREG_ENTRY_TTL_ACTION_TOKEN)) {
         getTokenEquals();
         parReg_entryTTLAction = getTokenAction();
      } else if (token.equalsIgnoreCase(PARREG_LOCAL_PROPERTIES)) {
         getTokenEquals();
         parReg_localProperties = getTokenProperties();
      } else if (token.equalsIgnoreCase(PARREG_GLOBAL_PROPERTIES)) {
         getTokenEquals();
         parReg_globalProperties = getTokenProperties();
//      } else if (token.equalsIgnoreCase(PARREG_ROUTING_RESOLVER_PROPERTIES)) {
//         getTokenEquals();
//         parReg_partitionResolver_properties = getTokenProperties();
      } else if (token.equalsIgnoreCase(PARREG_REDUNDANT_COPIES)) {
         getTokenEquals();
         parReg_redundantCopies = getTokenInteger();
      } else if (token.equalsIgnoreCase(MULTICAST_ENABLED_TOKEN)) {
         getTokenEquals();
         multicastEnabled = getTokenBoolean();
      } else if (token.equalsIgnoreCase(USE_DS_MULTICAST_SETTING_TOKEN)) {
         getTokenEquals();
         useDsMulticastSetting = getTokenBoolean();    
      } else if (token.equalsIgnoreCase(ENABLE_WAN_TOKEN)) {
         getTokenEquals();
         enableWAN = getTokenBoolean();
      } else if (token.equalsIgnoreCase(ROLL_OPLOGS_TOKEN)) {
         getTokenEquals();
         rollOplogs = getTokenBoolean(); 
      } else if (token.equalsIgnoreCase(MAX_OPLOG_SIZE_TOKEN)) {
         getTokenEquals();
         maxOplogSize = getTokenInteger(); 
      } else {
         throw new TestException("Unknown keyword " + token);
      }
      token = getNextToken();
   }
   // set region multicast attr (depends on value of useDsMulticast setting)
   // get mcast setting for distributed system
   String gemfireName =
       System.getProperty(GemFirePrms.GEMFIRE_NAME_PROPERTY);
   GemFireDescription gfd =
       TestConfig.getInstance().getGemFireDescription( gemfireName );
   Boolean mcastSettingForDs = gfd.getEnableMcast();
  
   if ( useDsMulticastSetting == null ) {
     if (multicastEnabled == null) {
       // region def doesn't specify what to do for multicast
       // so use setting for dist. system 
       useDsMulticastSetting = new Boolean(true);
     } else {
       // region def does specify multicast setting for region
       useDsMulticastSetting = new Boolean(false);
     }
   } 
   if (useDsMulticastSetting.booleanValue()) {
      multicastEnabled = mcastSettingForDs;
   } else if (multicastEnabled == null) {
     throw new TestException("Region def must specify multicast setting for region or use multicast setting for DS");
   }
}

/** Return a Properties object from the next token or list of tokens */
private Properties getTokenProperties() {
   Properties props = new Properties();
   StringBuffer propStr = new StringBuffer();
   do {
      String token = getNextToken();
      if (token == null) break;
      if (token.equals(SPEC_TERMINATOR)) break;
      propStr.append(token);
      propStr.append(getTokenEquals());
      token = getNextToken();
      if (token.endsWith(SPEC_TERMINATOR)) {
         token = token.substring(0, token.length() - 1);
         propStr.append(token);
         break;
      } else {
         propStr.append(token);
         propStr.append("\n");
      }
   } while (true);
   try {
      props.load(new ByteArrayInputStream(propStr.toString().getBytes()));
   } catch (java.io.IOException ex) {
      throw new TestException(TestHelper.getStackTrace(ex));
   }
   Log.getLogWriter().fine("Properties is " + props);
   return props;
}

/** Return a Scope from the next token or list of tokens */
private Scope getTokenScope() {
   String token = getTokenFromList();
   if (token.equalsIgnoreCase("null"))
      return null;
   Scope scope = null;
   try {
      scope = TestHelper.getScope(token);
   } catch (TestException e) {
      throw new TestException(tokensToString() + " " + TestHelper.getStackTrace(e));
   }
   Log.getLogWriter().fine("Chose scope " + scope);
   return scope;
}

/** Return a MirrorType from the next token or list of tokens */
private MirrorType getTokenMirroring() {
   String token = getTokenFromList();
   if (token.equalsIgnoreCase("null"))
      return null;
   MirrorType mt = null;
   try {
      mt = TestHelper.getMirrorType(token);
   } catch (TestException e) {
      throw new TestException(tokensToString() + " " + TestHelper.getStackTrace(e));
   }
   Log.getLogWriter().fine("Chose mirroring " + mt);
   return mt;
}

/** Return a DataPolicy from the next token or list of tokens */
private DataPolicy getTokenDataPolicy() {
   String token = getTokenFromList();
   if (token.equalsIgnoreCase("null"))
      return null;
   DataPolicy db = null;
   try {
      db = TestHelper.getDataPolicy(token);
   } catch (TestException e) {
      throw new TestException(tokensToString() + " " + TestHelper.getStackTrace(e));
   }
   Log.getLogWriter().fine("Chose dataPolicy " + db);
   return db;
}

private DataPolicy getTokenConcurrencyChecksEnabled() {
   String token = getTokenFromList();
   if (token.equalsIgnoreCase("null"))
      return null;
   DataPolicy db = null;
   try {
      db = TestHelper.getDataPolicy(token);
   } catch (TestException e) {
      throw new TestException(tokensToString() + " " + TestHelper.getStackTrace(e));
   }
   Log.getLogWriter().fine("Chose dataPolicy " + db);
   return db;
}

/** Return a DataPolicy from the next token or list of tokens */
private InterestPolicy getTokenInterestPolicy() {
   String token = getTokenFromList();
   InterestPolicy ip = null;
   try {
      ip = TestHelper.getInterestPolicy(token);
   } catch (TestException e) {
      throw new TestException(tokensToString() + " " + TestHelper.getStackTrace(e));
   }
   Log.getLogWriter().fine("Chose interestPolicy " + ip);
   return ip;
}

/** Return an ExpirationAction (or null) from the next token or list of tokens */
private ExpirationAction getTokenAction() {
   String token = getTokenFromList();
   if (token.equalsIgnoreCase("null"))
      return null;
   Log.getLogWriter().fine("Chose exp action " + token);
   return TestHelper.getExpirationAction(token);
}

private ExpirationAttributes getExpirationAttributes(Integer time, ExpirationAction action, String expirationType) {
   ExpirationAttributes expAttr = null;
   if (time == null) {
      if (action != null)
         throw new TestException("Unable to create region attributes: expiration action is " + action + ", but no time was specified for " + expirationType);
   } else {
      if (action == null)
         expAttr = new ExpirationAttributes(time.intValue());
      else
         expAttr = new ExpirationAttributes(time.intValue(), action);
   }
   return expAttr;
}

/**
 * get lossAction from ReliabilityAttrsSpec
 */
private LossAction getTokenLossAction() {
   String token = getTokenFromList();
   if (token.equalsIgnoreCase("null"))
      return null;
   LossAction lossAction = null;
   try {
      lossAction = TestHelper.getLossAction(token);
   } catch (TestException e) {
      throw new TestException(tokensToString() + " " + TestHelper.getStackTrace(e));
   }
   Log.getLogWriter().fine("Chose lossAction " + lossAction);
   return lossAction;
}

/**
 * get resumptionAction from ReliabilityAttrsSpec
 */
private ResumptionAction getTokenResumptionAction() {
   String token = getTokenFromList();
   if (token.equalsIgnoreCase("null"))
      return null;
   ResumptionAction action = null;
   try {
      action = TestHelper.getResumptionAction(token);
   } catch (TestException e) {
      throw new TestException(tokensToString() + " " + TestHelper.getStackTrace(e));
   }
   Log.getLogWriter().fine("Chose resumptionAction " + action);
   return action;
}

/**
 * get the MembershipAttributes for this RegionDefinition
 */
private MembershipAttributes getMembershipAttributes() {
   MembershipAttributes  reliabilityAttrs = null;
   if (requiredRoles != null) {
     String[] rolesArr = new String[requiredRoles.size()];
     int i=0;
     for (Iterator it = requiredRoles.iterator(); it.hasNext(); i++) {
        rolesArr[i] = (String)it.next();
     }
     reliabilityAttrs = new MembershipAttributes(rolesArr, lossAction, resumptionAction);
   }
   return reliabilityAttrs;
}

/** Return EvictionAction for this RegionDefinition. If the eviction
 *  action is not set, it will return the default eviction action.
 */
private EvictionAction getEvictionActionOrDefault() {
   EvictionAction action = EvictionAction.DEFAULT_EVICTION_ACTION;
   if (evictionAction != null) {
      if (evictionAction.equalsIgnoreCase("localDestroy"))
         action = EvictionAction.LOCAL_DESTROY;
      else if (evictionAction.equalsIgnoreCase("overflowToDisk"))
         action = EvictionAction.OVERFLOW_TO_DISK;
      else
         throw new TestException("Unknown eviction action " + evictionAction);
   }
   return action;
}

private void deriveDiskDirList() {
   // If no list specified, generate filename list
   if (diskDirList == null) {
      diskDirList = new ArrayList();
      if (numDiskDirs == null) { // no disk dirs to derive
         return;
      }
      int numDirs = numDiskDirs.intValue();
      String gemfireName = System.getProperty(GemFirePrms.GEMFIRE_NAME_PROPERTY);
      if ( gemfireName == null ) {
        throw new HydraConfigException("No gemfire name has been specified");
      }
      String currDir = System.getProperty("user.dir");
      for (int i = 0; i < numDirs; i++) {
         String fname = currDir + "/DiskRegionForVmID_" + RemoteTestModule.getMyVmid() + "_dirNum_" + (i+1);
         diskDirList.add(fname);
      }
   }
}

//================================================================================
// public instance methods

/** Return this instance as a string.
 */
public String toString() {
   StringBuffer aStr = new StringBuffer();
   aStr.append("RegionDefinition with name " + specName + "\n");
   aStr.append("   scope: " + scope + "\n");
   aStr.append("   mirroring: " + mirroring + "\n");
   aStr.append("   dataPolicy: " + dataPolicy + "\n");
   aStr.append("   concurrencyChecksEnabled: " + concurrencyChecksEnabled + "\n");
   aStr.append("   subscription: " + subscriptionAttributes + "\n");
   aStr.append("   concurrencyLevel: " + concurrencyLevel + "\n");
   aStr.append("   entryIdleTimeoutSec: " + entryIdleTimeoutSec + "\n");
   aStr.append("   entryIdleTimeoutAction: " + entryIdleTimeoutAction + "\n");
   aStr.append("   entryTTLSec: " + entryTTLSec + "\n");
   aStr.append("   entryTTLAction: " + entryTTLAction + "\n");
   aStr.append("   regionIdleTimeoutSec: " + regionIdleTimeoutSec + "\n");
   aStr.append("   regionIdleTimeoutAction: " + regionIdleTimeoutAction + "\n");
   aStr.append("   regionTTLSec: " + regionTTLSec + "\n");
   aStr.append("   regionTTLAction: " + regionTTLAction + "\n");
   if (keyConstraint == null)
      aStr.append("   keyConstraint: " + keyConstraint + "\n");
   else
      aStr.append("   keyConstraint: " + keyConstraint.getName() + "\n");
   if (valueConstraint == null)
      aStr.append("   valueConstraint: " + valueConstraint + "\n");
   else
      aStr.append("   valueConstraint: " + valueConstraint.getName() + "\n");
   aStr.append("   loadFactor: " + loadFactor + "\n");
   aStr.append("   statisticsEnabled: " + statisticsEnabled + "\n");
   aStr.append("   enableOffHeapMemory: " + enableOffHeapMemory + "\n");
   aStr.append("   persistBackup: " + persistBackup + "\n");
   aStr.append("   isSynchronous: " + isSynchronous + "\n");
   aStr.append("   eviction: " + eviction + "\n");
   aStr.append("     evictionLimit: " + evictionLimit + "\n");
   aStr.append("     evictionAction: " + evictionAction + "\n");
   aStr.append("     objectSizerClass: " + objectSizerClass + "\n");
   aStr.append("   asyncConflation: " + asyncConflation + "\n");
   aStr.append("   timeInterval: " + timeInterval + "\n");
   aStr.append("   bytesThreshold: " + bytesThreshold + "\n");
   aStr.append("   cacheLoader: " + cacheLoader + "\n");
   aStr.append("   cacheWriter: " + cacheWriter + "\n");
   aStr.append("   numDiskDirs: " + numDiskDirs + "\n");
   aStr.append("   indexMaintenanceSynchronous: " + indexMaintenanceSynchronous + "\n");
   aStr.append("   requiredRoles: " + requiredRoles + "\n");
   aStr.append("   lossAction: " + lossAction + "\n");
   aStr.append("   resumptionAction: " + resumptionAction + "\n");
   aStr.append("   parReg: " + parReg + "\n");
   aStr.append("     parReg_cacheWriter: " + parReg_cacheWriter + "\n");
   aStr.append("     parReg_entryIdleTimeoutSec: " + parReg_entryIdleTimeoutSec + "\n");
   aStr.append("     parReg_entryIdleTimeoutAction: " + parReg_entryIdleTimeoutAction + "\n");
   aStr.append("     parReg_entryTTLSec: " + parReg_entryIdleTimeoutSec + "\n");
   aStr.append("     parReg_entryTTLAction: " + parReg_entryIdleTimeoutAction + "\n");
   aStr.append("     parReg_localProperties: " + parReg_localProperties + "\n");
   aStr.append("     parReg_globalProperties: " + parReg_globalProperties + "\n");
   aStr.append("     parReg_redundantCopies: " + parReg_redundantCopies + "\n");
   aStr.append("     parReg_colocatedWith: " + parReg_colocatedWith + "\n");
   aStr.append("     parReg_routingResolver: " + parReg_partitionResolver + "\n");
//   aStr.append("        parReg_routingResolver_properties: " + parReg_partitionResolver_properties + "\n");
   aStr.append("   multicastEnabled: " + multicastEnabled + "\n");
   if (cacheListeners == null) {
      aStr.append("   cacheListeners: " + cacheListeners + "\n");
   } else if (cacheListeners.size() == 0) {
      throw new TestException("cacheListeners list is size 0");
   } else {
      aStr.append("   cacheListeners: " + cacheListeners.get(0) + "\n");
      for (int i = 1; i < cacheListeners.size(); i++) {
         aStr.append("                  " + cacheListeners.get(i) + "\n");
      }
   }
   aStr.append("   enableWAN: " + enableWAN + "\n");
aStr.append("   rollOplogs: " + rollOplogs + "\n");
   aStr.append("   maxOplogSize: " + maxOplogSize + "\n");
   if (diskDirList != null) {
      aStr.append("   diskDirList: ");
      for (int i = 0; i < diskDirList.size(); i++) {
        aStr.append((String)diskDirList.get(i) + " " );
      }
      aStr.append("\n");
   }
   return aStr.toString();
}

/** Create and return region attributes based on the current values of 
 *  this RegionDefinition.  Has the side effect of deriving the diskDirList
 *  if it is null.
 *
 *  @return RegionAttributes The region attributes instance with the 
 *          region attributes set as specified in this RegionDefinition.
 */
public RegionAttributes getRegionAttributes() {
   return getRegionAttributes(null, null, null);
}

/** Create and return region attributes based on the current values of 
 *  this RegionDefinition.
 *
 *  @param cacheListenerArg The desired CacheListern, or null if none.
 *  @param cacheLoaderArg The desired CacheLoader, or null if none.
 *  @param cacheWriterArg The desired CacheWriter, or null if none.
 *
 *  @return RegionAttributes The region attributes instance with the 
 *          region attributes set as specified in this RegionDefinition.
 */
public RegionAttributes getRegionAttributes(CacheListener cacheListenerArg,
                                            CacheLoader cacheLoaderArg,
                                            CacheWriter cacheWriterArg) {
   AttributesFactory factory = new AttributesFactory();

   if (scope != null)
      factory.setScope(scope);
   if (mirroring != null)
      factory.setMirrorType(mirroring);
   if (dataPolicy != null) 
      factory.setDataPolicy( dataPolicy );
   if (concurrencyChecksEnabled != null) 
      factory.setConcurrencyChecksEnabled(concurrencyChecksEnabled.booleanValue());
   if (subscriptionAttributes != null) 
      factory.setSubscriptionAttributes( subscriptionAttributes );

   // cacheListeners
   if ((cacheListeners == null) && (cacheListenerArg == null)) { // both are null
      // it is ok for both to be null
   } else { // one or both are specified
      if ((cacheListeners != null) && (cacheListenerArg != null)) { // both are specified
         throw new TestException("A cache listener was specified in the region definition as " +
               cacheListeners + ", but it was also specified as an argument to create region " +
               "attributes " + cacheListenerArg);
      } else { // only one or the other is specified 
         if (cacheListeners != null) { // set from the instance
            for (int i = 0; i < cacheListeners.size(); i++) {
               factory.addCacheListener((CacheListener)(TestHelper.createInstance((String)(cacheListeners.get(i)))));
            }
         } else { // set from the argument
            factory.setCacheListener(cacheListenerArg);
         }
      }
   }

   // eviction 
   if (eviction != null) {
      EvictionAttributes evAttr = getEvictionAttributes();
      factory.setEvictionAttributes(evAttr);
   }

   // cacheLoader
   if ((cacheLoader == null) && (cacheLoaderArg == null)) { // both are null
      // it is ok for both to be null
   } else { // one or both are specified
      if ((cacheLoader != null) && (cacheLoaderArg != null)) { // both are specified
         throw new TestException("A cache Loader was specified in the region definition as " +
               cacheLoader + ", but it was also specified as an argument to create region " +
               "attributes " + cacheLoaderArg);
      } else { // only one or the other is specified 
         if (cacheLoader != null) { // set from the instance
            factory.setCacheLoader(getCacheLoaderInstance(cacheLoader)); 
         } else { // set from the argument
            factory.setCacheLoader(cacheLoaderArg);
         }
      }
   }

   // cacheWriter
   if ((cacheWriter == null) && (cacheWriterArg == null)) { // both are null
      // it is ok for both to be null
   } else { // one or both are specified
      if ((cacheWriter != null) && (cacheWriterArg != null)) { // both are specified
         throw new TestException("A cache Writer was specified in the region definition as " +
               cacheWriter + ", but it was also specified as an argument to create region " +
               "attributes " + cacheWriterArg);
      } else { // only one or the other is specified 
         if (cacheWriter != null) { // set from the instance
            factory.setCacheWriter(getCacheWriterInstance(cacheWriter)); 
         } else { // set from the argument
            factory.setCacheWriter(cacheWriterArg);
         }
      }
   }

   if (concurrencyLevel != null)
      factory.setConcurrencyLevel(concurrencyLevel.intValue());
   if (keyConstraint != null)
      factory.setKeyConstraint(keyConstraint);
    if (valueConstraint != null)
      factory.setValueConstraint(valueConstraint);
   if (loadFactor != null)
      factory.setLoadFactor(loadFactor.floatValue());
   if (statisticsEnabled != null)
      factory.setStatisticsEnabled(statisticsEnabled.booleanValue());
   if (enableOffHeapMemory != null) {
      if (enableOffHeapMemory.equalsIgnoreCase("ifOffHeapMemoryConfigured")) {
        if (OffHeapHelper.isOffHeapMemoryConfigured()) {
          factory.setEnableOffHeapMemory(true);
        }
      }
   }
   if (indexMaintenanceSynchronous != null)
      factory.setIndexMaintenanceSynchronous(indexMaintenanceSynchronous.booleanValue());
   if (enableWAN != null)
      factory.setEnableWAN(enableWAN.booleanValue());
   if (persistBackup != null)
      factory.setPersistBackup(persistBackup.booleanValue());
   if (asyncConflation != null)
      factory.setEnableAsyncConflation(asyncConflation.booleanValue());
   if (isSynchronous != null) {
      factory.setDiskWriteAttributes(getDiskWriteAttributes());
   }
   if (multicastEnabled != null) {
      factory.setMulticastEnabled(multicastEnabled.booleanValue());
   }
   if (numDiskDirs != null) {
      factory.setDiskDirs(getDiskDirFileArr());
   }

   // region expirations
   ExpirationAttributes expAttr = getRegionTTL();
   if (expAttr != null)
      factory.setRegionTimeToLive(expAttr);
   expAttr = getRegionIdleTimeout();
   if (expAttr != null)
      factory.setRegionIdleTimeout(expAttr);

   // entry expirations
   expAttr = getEntryTTL();
   if (expAttr != null)
      factory.setEntryTimeToLive(expAttr);
   expAttr = getEntryIdleTimeout();
   if (expAttr != null)
      factory.setEntryIdleTimeout(expAttr);

   // reliabilityAttibutes
   MembershipAttributes reliabilityAttrs = getMembershipAttributes();
   if (reliabilityAttrs != null) {
      factory.setMembershipAttributes(reliabilityAttrs);
   }

   // partitioned regions
   PartitionAttributesFactory parFac = null;
   if (parReg != null) {
      if (parReg.booleanValue()) {
         parFac = new PartitionAttributesFactory();
         if (parReg_colocatedWith != null)
            parFac.setColocatedWith(parReg_colocatedWith);
         if (parReg_partitionResolver != null) {
            PartitionResolver instance = getPartitionResolverInstance(parReg_partitionResolver);
//            instance.init(parReg_partitionResolver_properties);
            parFac.setPartitionResolver(instance);
         }
//         if (parReg_cacheWriter != null)
//            parFac.setCacheWriter(getCacheWriterInstance(parReg_cacheWriter));
//         expAttr = getParRegEntryTTL();
//         if (expAttr != null)
//            parFac.setEntryTimeToLive(expAttr);
//         expAttr = getParRegEntryIdleTimeout();
//         if (expAttr != null)
//            parFac.setEntryIdleTimeout(expAttr);
         if (parReg_localProperties != null)
            parFac.setLocalProperties(parReg_localProperties);
         if (parReg_globalProperties != null)
            parFac.setGlobalProperties(parReg_globalProperties);
         if (parReg_redundantCopies != null)
            parFac.setRedundantCopies(parReg_redundantCopies.intValue());
         PartitionAttributes parAttr = parFac.create();
         factory.setPartitionAttributes(parAttr);
      } else { // partitioned region is false
         if ((parReg_cacheWriter != null) ||
             (parReg_colocatedWith != null) ||
             (parReg_partitionResolver != null) ||
//             (parReg_partitionResolver_properties != null) ||
             (parReg_entryIdleTimeoutSec != null) ||
             (parReg_entryIdleTimeoutAction != null) ||
             (parReg_entryTTLSec != null) ||
             (parReg_entryTTLAction != null) ||
             (parReg_localProperties != null) ||
             (parReg_globalProperties != null) ||
             (parReg_redundantCopies != null)){
             throw new TestException("Some partitioned region attributes are set, but " +
                   "\"partitionedRegion = false\" in your RegionDefinition");
          }
      }
   } else { // parReg is null
      if ((parReg_cacheWriter != null) ||
          (parReg_colocatedWith != null) ||
          (parReg_partitionResolver != null) ||
//          (parReg_partitionResolver_properties != null) ||
          (parReg_entryIdleTimeoutSec != null) ||
          (parReg_entryIdleTimeoutAction != null) ||
          (parReg_entryTTLSec != null) ||
          (parReg_entryTTLAction != null) ||
          (parReg_localProperties != null) ||
          (parReg_globalProperties != null) ||
          (parReg_redundantCopies != null)) {
          throw new TestException("Some partitioned region attributes are set; you must also " +
                "set \"partitionedRegion=true\" in your RegionDefinition, but it is missing");
       }
   }

   // create the attributes 
   RegionAttributes attr = factory.createRegionAttributes();
   return attr;
}

/** Create a root region in the given cache.
 *
 *  @param aCache - Specifies the cache to create the root region in.
 *  @param regionName - The name of the new region.
 *  @param listener - The CacheListener to install in the region, or null.
 *  @param loader - The CacheLoader to install in the region, or null.
 *  @param writer - The CacheWriter to install in the region, or null.
 *
 *  @return The new root region.
 */
public Region createRootRegion(Cache aCache,
                               String name,
                               CacheListener listener,
                               CacheLoader loader,
                               CacheWriter writer) {
   String nameToUse = null;
   if ((regionName == null) && (name == null)) { // both are null
      // not ok for both to be null
      throw new TestException("A region name was not specified either in the region definition or as an argument to this call");
   } else { // one or both are specified
      if ((regionName != null) && (name != null) && (!regionName.equals(name))) { // both are specified and they don't match
         throw new TestException("A region name was specified in the region definition as " +
               regionName + ", but it was also specified as an argument to create region " +
               name);
      } 
      // only one or the other is specified or both was specified and they are equal; we are happy
      if (regionName != null)
         nameToUse = regionName;
      else
         nameToUse = name;
   }

   RegionAttributes attr = this.getRegionAttributes(listener, loader, writer);
   Region aRegion = null;
   try {
      Log.getLogWriter().info("Creating VM region " + nameToUse + " with " + TestHelper.regionAttributesToString(attr));
      aRegion = aCache.createVMRegion(nameToUse, attr); 
   } catch (RegionExistsException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   } catch (TimeoutException e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }
   Log.getLogWriter().info("Finished creating " + TestHelper.regionToString(aRegion, true));
   return aRegion;
}

/**
 *  Returns a HashMap of CollectionAttributes {@link CollectionAttributes} 
 *  for use with GFXCollectionManagementService.createCollection(String name, HashMap env)
 *  {@link GFXCollectionManagementService#createCollection}.
 *  
 *  @return A HashMap as described above.
 */




//================================================================================
// public static methods

/**
 *  Creates a randomly chosen region definition based on {@link
 *  RegionDefPrms#regionSpecs} {@link RegionDefPrms#regionSpecName}
 *  {@link RegionDefPrms#regionDefUsage} {@link RegionDefPrms#reliabilitySpecName}
 *
 *  @return A <Code>RegionDefinition</code> as described above.
 */
public synchronized static RegionDefinition createRegionDefinition() {
   Log.getLogWriter().info("In RegionDefinition.createRegionDefinition...");
   String regionDefUsage = TestConfig.tab().stringAt(RegionDefPrms.regionDefUsage);
   if (regionDefUsage.equalsIgnoreCase(USAGE_NONE)) {
      return null;
   } else if ((regionDefUsage.equalsIgnoreCase(USAGE_ANY)) ||
              (regionDefUsage.equalsIgnoreCase(USAGE_FIXED_SEQUENCE))) {
      // For both of these usage patterns, just get a random region definition.
      // In the case of fixed-sequence usage, a random number generator has already
      // been initialized (via the region definition init task InitTask_initialize)
      // with a particular random seed so that each VM will used the same sequence
      // of random numbers. For the "any" usage pattern, a random number generator
      // is used that is randomly seeded.
      if (RegDefRand == null)
         throw new TestException("You must include an INITTASK for RegionDefinition.InitTask_initialize");

      RegionDefinition regDef = new RegionDefinition();
      regDef.getRegionDefinition();
      RegionDefinition reliabilityDef = createReliabilityDefinition();
      regDef.mergeReliabilityDefinition( reliabilityDef );

      if (regionDefUsage.equalsIgnoreCase(USAGE_ANY))
         Log.getLogWriter().info("RegionDefinition.createRegionDefinition: Getting any region definition: " + regDef);
      else
         Log.getLogWriter().info("RegionDefinition.createRegionDefinition: Getting a fixed-sequence region definition: " + regDef);
      // Get any defined MembershipAttributes as part of this regionDefinition
      return regDef;
   } else if (regionDefUsage.equalsIgnoreCase(USAGE_ONE)) {
      RegionDefinition regDef = (RegionDefinition)RegionDefBB.getBB().getSharedMap().get(REGION_DEF_KEY);
      RegionDefinition reliabilityDef = createReliabilityDefinition();
      regDef.mergeReliabilityDefinition( reliabilityDef );
      Log.getLogWriter().info("RegionDefinition.createRegionDefinition: Getting singleton region definition " + regDef);
      if (regDef == null)
         throw new TestException("key " + REGION_DEF_KEY + " in RegionDefBB has null value; you must include an INITTASK for RegionDefinition.InitTask_initialize");
      // Get any defined MembershipAttributes as part of this regionDefinition
      return regDef;
   } else {
      throw new TestException("Unknown value for RegionDefPrms.regionDefUsage " + regionDefUsage);
   }

}

/** Creates a region definition with the specified specName. Since
 *  this names a particular specification, {@link
 *  RegionDefPrms#regionDefUsage} is ignored
 *
 *  @param hydraSpecParam The name of the hydra parameter containing the spec string.
 *  @param specName The name of the attribute specification, defined in hydraSpecParam.
 *
 *  @return A RegionDefinition as described above.
 *
 *  @throws TestException
 *          If there is no spec named <code>specName</code>
 */
public static RegionDefinition createRegionDefinition(Long hydraSpecParam, String specName) {
   RegionDefinition regDef = new RegionDefinition();
   regDef.getDefinition(hydraSpecParam, specName);
   return regDef;
}

/** Returns the possible specNames available for a hydra parameter that lists
 *  attributes specs.
 *
 *  @param hydraSpecParam A hydra parameter containing one or more attributes specs.
 *
 *  @return A Set of Strings, which are names of specs available for the given
 *          hydra parameter.
 */
public static Set getSpecNames(Long hydraSpecParam) {
   RegionDefinition regDef = new RegionDefinition();
   regDef.initializeSpecMap(hydraSpecParam);
   return regDef.specMap.keySet();
}

/** 
 * An INIT task to be used by all client threads for a test run.  If
 * the test should {@linkplain RegionDefPrms#regionDefUsage only use}
 * one region configuration, then that configuration is created and
 * placed in the {@linkplain RegionDefBB blackboard}.
 */
public static void InitTask_initialize() {
   HydraTask_initialize();
}

/** A task that can be run either as a start task or init task
 */
public static void HydraTask_initialize() {
   String regionDefUsage = TestConfig.tab().stringAt(RegionDefPrms.regionDefUsage);
   if (regionDefUsage.equalsIgnoreCase(USAGE_NONE)) {
      // no initialization necessary
      return;
   } else if (regionDefUsage.equalsIgnoreCase(USAGE_ONE)) {
      // put RegionDef in BB dict with this key; last thread to do
      // this sets the one RegionDef
      RegionDefinition regDef = new RegionDefinition();
      regDef.getRegionDefinition();
      RegionDefBB.getBB().getSharedMap().put(REGION_DEF_KEY, regDef);
   } else if (regionDefUsage.equalsIgnoreCase(USAGE_FIXED_SEQUENCE)) {
      long seed = TestConfig.tab().longAt(hydra.Prms.randomSeed);
      Log.getLogWriter().info("Initializing region definition random seed to " + seed);
      RegDefRand = new GsRandom(TestConfig.tab().longAt(hydra.Prms.randomSeed));
   } else if (regionDefUsage.equalsIgnoreCase(USAGE_ANY)) {
      // no initialization necessary
      return;
   } else {
      throw new TestException("Unknown value for RegionDefPrms.regionDefUsage " + regionDefUsage);
   }
}

//================================================================================
// getter methods

public String getRegionName() {
   return regionName;
}

public Scope getScope() {
   return scope;
}

public MirrorType getMirroring() {
   return mirroring;
}

public DataPolicy getDataPolicy() {
   return dataPolicy;
}

public Boolean getConcurrencyChecksEnabled() {
   return concurrencyChecksEnabled;
}

public SubscriptionAttributes getSubscriptionAttributes() {
   return subscriptionAttributes;
}

public List getRequiredRoles() {
   return requiredRoles;
}

public LossAction getLossAction() {
   return lossAction;
}

public ResumptionAction getResumptionAction() {
   return resumptionAction;
}

public Integer getConcurrencyLevel() {
   return concurrencyLevel;
}

public Integer getEntryIdleTimeoutSec() {
   return entryIdleTimeoutSec;
}

public ExpirationAction getEntryIdleTimeoutAction() {
   return entryIdleTimeoutAction;
}

public Integer getEntryTTLSec() {
   return entryTTLSec;
}

public ExpirationAction getEntryTTLAction() {
   return entryTTLAction;
}

public Integer getRegionIdleTimeoutSec() {
   return regionIdleTimeoutSec;
}

public ExpirationAction getRegionIdleTimeoutAction() {
   return regionIdleTimeoutAction;
}

public Integer getRegionTTLSec() {
   return regionTTLSec;
}

public ExpirationAction getRegionTTLAction() {
   return regionTTLAction;
}

public Class getKeyConstraint() {
   return keyConstraint;
}

public Class getValueConstraint() {
   return valueConstraint;
}

public Float getLoadFactor() {
   return loadFactor;
}

public Boolean getStatisticsEnabled() {
   return statisticsEnabled;
}

public String getEnableOffHeapMemory() {
   return enableOffHeapMemory;
}

public Boolean getPersistBackup() {
   return persistBackup;
}

public Integer getEvictionLimit() {
   return evictionLimit;
}

public Boolean getAsyncConflation() {
   return asyncConflation;
}

public Boolean getIsSynchronous() {
   return isSynchronous;
}

public Boolean getEnableWAN() {
   return enableWAN;
}

public Long getTimeInterval() {
   return timeInterval;
}

public Long getBytesThreshold() {
   return bytesThreshold;
}

public Integer getNumDiskDirs() {
   return numDiskDirs;
}

public String getEvictionAction() {
   return evictionAction;
}

public List getDiskDirList() {
   return diskDirList;
}

public String getEviction() {
   return eviction;
}

public List getCacheListeners() {
   return cacheListeners;
}

public String getCacheLoader() {
   return cacheLoader;
}

public String getCacheWriter() {
   return cacheWriter;
}

public String getObjectSizerClass() {
   return objectSizerClass;
}

public Boolean getIndexMaintenanceSynchronous() {
   return indexMaintenanceSynchronous;
}

/** Returns true if this region definition specifies region with multicastEnabled.
 *
 *  @return true if this region definition specifies region with multicastEnabled. 
 *  
 *  @since 5.0
 *  @author Jean Farris
 */
public Boolean getMulticastEnabled() {
   return multicastEnabled;
}
/** Returns true if region definition should specify region with same
 *  multicastEnabled setting as distributed system.
 *
 *  @return true if region definition should specify region with same
 *  multicastEnabled setting as distributed system.
 *  @since 5.0
 *  @author Jean Farris
 */
public Boolean getUseDsMulticastSetting() {
   return useDsMulticastSetting;
}



public ArrayList getSubregions() {
   return subRegions;
}

////added by Prafulla

public Boolean getRollOplogs (){
	return rollOplogs;
}

public Integer getMaxOplogSize (){
	return maxOplogSize;
}

protected DiskWriteAttributes getDiskWriteAttributes() {
   DiskWriteAttributesFactory factory = new DiskWriteAttributesFactory();
   if (isSynchronous != null) {
      factory.setSynchronous(isSynchronous.booleanValue());
   }
   if (timeInterval != null) {
      factory.setTimeInterval(timeInterval.longValue());
   }
   if (bytesThreshold != null) {
      factory.setBytesThreshold(bytesThreshold.longValue());
   }
   if (rollOplogs != null) {
      factory.setRollOplogs(rollOplogs.booleanValue());
   }
   if (maxOplogSize != null) {
      factory.setMaxOplogSize(maxOplogSize.intValue());
   }
   return factory.create();
}

public File[] getDiskDirFileArr() {
   if (diskDirList == null)
      deriveDiskDirList(); 
   if (diskDirList != null) {
      // add the dirs verbatim (do not prepend the gemfire system dir)
      File[] fileArr = new File[diskDirList.size()];
      for (int i = 0; i < diskDirList.size(); i++) {
         File aDir = new File((String)diskDirList.get(i));
         fileArr[i] = aDir;
         aDir.mkdir(); // returns false if already exists
      }
      return fileArr;
   } 
   return null;
}

public ExpirationAttributes getRegionTTL() {
   ExpirationAttributes expAttr = getExpirationAttributes(regionTTLSec, regionTTLAction, "regionTTL");
   return expAttr;
}

public ExpirationAttributes getRegionIdleTimeout() {
   ExpirationAttributes expAttr = getExpirationAttributes(regionIdleTimeoutSec, regionIdleTimeoutAction, "regionIdleTimeout");
   return expAttr;
}

public ExpirationAttributes getEntryTTL() {
   ExpirationAttributes expAttr = getExpirationAttributes(entryTTLSec, entryTTLAction, "entryTTL");
   return expAttr;
}

public ExpirationAttributes getEntryIdleTimeout() {
   ExpirationAttributes expAttr = getExpirationAttributes(entryIdleTimeoutSec, entryIdleTimeoutAction, "entryIdleTimeout");
   return expAttr;
}

public Boolean getPartitionedRegion() {
   return parReg;
}

public String getParRegCacheWriter() {
   return parReg_cacheWriter;
}

public String getParRegPartitionResolver() {
   return parReg_partitionResolver;
}

public String getColocatedWith() {
   return parReg_colocatedWith;
}

public Integer getParRegEntryIdleTimeoutSec() {
   return parReg_entryIdleTimeoutSec;
}

public ExpirationAction getParRegEntryIdleTimeoutAction() {
   return parReg_entryIdleTimeoutAction;
}

public ExpirationAttributes getParRegEntryTTL() {
   ExpirationAttributes expAttr = getExpirationAttributes(parReg_entryTTLSec, parReg_entryTTLAction, "parReg_entryTTL");
   return expAttr;
}

public ExpirationAttributes getParRegEntryIdleTimeout() {
   ExpirationAttributes expAttr = getExpirationAttributes(parReg_entryIdleTimeoutSec, parReg_entryIdleTimeoutAction, "parReg_entryIdleTimeout");
   return expAttr;
}

public Integer getParRegEntryTTLSec() {
   return parReg_entryTTLSec;
}

public Properties getParRegLocalProperties() {
   return parReg_localProperties;
}

public Properties getParRegGlobalProperties() {
   return parReg_globalProperties;
}

//public Properties getParRegPartitionResolverProperties() {
//   return parReg_partitionResolver_properties;
//}

public Integer getParRegRedundantCopies() {
   return parReg_redundantCopies;
}


/** Return EvictionAttributes for this RegionDefinition. If none are set, this
 *  will return null.
 */
public EvictionAttributes getEvictionAttributes() {
   EvictionAttributes evAttr = null;
   boolean evictionLimitIsSet = evictionLimit != null;
   boolean evictionActionIsSet = evictionAction != null;
   boolean objectSizerIsSet = objectSizerClass != null;
   if (eviction.equalsIgnoreCase(EVICTION_LRU_STR)) { // set an LRU evictor
      if (evictionLimitIsSet && evictionActionIsSet) { // both are set
         evAttr = EvictionAttributes.createLRUEntryAttributes(
                  evictionLimit.intValue(), getEvictionActionOrDefault());
      } else if (!evictionLimitIsSet && !evictionActionIsSet) { // neither are set
         evAttr = EvictionAttributes.createLRUEntryAttributes();
      } else if (evictionLimitIsSet && !evictionActionIsSet) { // only evictionLimit is Set
         evAttr = EvictionAttributes.createLRUEntryAttributes(evictionLimit.intValue());
      } else {
         throw new TestException("For eviction " + eviction + ", evictionLimit is " + evictionLimit +
                   ", evictionAction is " + evictionAction + ", but valid settings are either to set " +
                   "(1) both evictionLimit and evictionAction, (2) neither, or (3) evictionLimit only");
      }
   } else if (eviction.equalsIgnoreCase(EVICTION_MEM_LRU_STR)) { // set a MemLRU evictor
      if (evictionLimitIsSet && evictionActionIsSet && objectSizerIsSet) { // all 3 are set
         evAttr = EvictionAttributes.createLRUMemoryAttributes(
                     evictionLimit.intValue(), 
                     (ObjectSizer)(TestHelper.createInstance(objectSizerClass)),
                     getEvictionActionOrDefault());
      } else if (evictionLimitIsSet && evictionActionIsSet && !objectSizerIsSet) { // limit/action set, sizer not
         evAttr = EvictionAttributes.createLRUMemoryAttributes(
                     evictionLimit.intValue(), 
                     null,
                     getEvictionActionOrDefault());
      } else if (evictionLimitIsSet && objectSizerIsSet && !evictionActionIsSet) { // limit/sizer set, action not
         evAttr = EvictionAttributes.createLRUMemoryAttributes(
                     evictionLimit.intValue(), 
                     (ObjectSizer)(TestHelper.createInstance(objectSizerClass)));
      } else if (evictionLimitIsSet && !objectSizerIsSet && !evictionActionIsSet) { // only limit is set
         evAttr = EvictionAttributes.createLRUMemoryAttributes(evictionLimit.intValue());
      } else if (!evictionLimitIsSet && !objectSizerIsSet && !evictionActionIsSet) { // none is set
         evAttr = EvictionAttributes.createLRUMemoryAttributes();
      } else {
         throw new TestException("For eviction " + eviction + ", evictionLimit is " + evictionLimit +
                   ", evictionAction is " + evictionAction + ", objectSizerClass is " + objectSizerClass +
                   ", but valid settings are either to set (1) all 3, (2) none, (3) evictionLimit only, " +
                   "or (4) evictionLimit and objectSizerClass");
      }
   } else if (eviction.equalsIgnoreCase(EVICTION_HEAP_STR)) { // set a heap evictor
      if(evictionLimitIsSet) {
        throw new TestException("With heap lru, evictionLimit cannot be set. You must use evictionHeapPercentage on cache definition.");
      }
      if (objectSizerIsSet && evictionActionIsSet) { // two are set
         evAttr = EvictionAttributes.createLRUHeapAttributes(
               (ObjectSizer)(TestHelper.createInstance(objectSizerClass)),
               getEvictionActionOrDefault());
      } else if (!evictionActionIsSet && !evictionActionIsSet) { // none is set
         evAttr = EvictionAttributes.createLRUHeapAttributes();
      } else if (objectSizerIsSet && !evictionActionIsSet) { // none is set
         evAttr = EvictionAttributes.createLRUHeapAttributes((ObjectSizer)(TestHelper.createInstance(objectSizerClass)));
      } else if(!objectSizerIsSet && evictionActionIsSet) {
         evAttr = EvictionAttributes.createLRUHeapAttributes(null,getEvictionActionOrDefault());
      } else {
         throw new TestException("For eviction " + eviction +
                   ", evictionAction is " + evictionAction + ", objectSizer is " + objectSizerClass +
                   ", but valid settings are either to set (1) both , (2) neither, or (3) objectSizerClass only, or (4) evictionAction only");
      }
   } else {
      throw new TestException("Unknown eviction " + eviction);
   }
   return evAttr;
}

//================================================================================
// setter methods (in case anybody wants to override these settings)

public void setRegionName(String regionNameArg) {
   regionName = regionNameArg;
}

public void setScope(Scope scopeArg) {
   scope = scopeArg;
}

public void setMirroring(MirrorType mirroringArg) {
   mirroring = mirroringArg;
}

public void setDataPolicy(DataPolicy dataPolicyArg) {
   dataPolicy = dataPolicyArg;
}
public void setSubscriptionAttributes(SubscriptionAttributes subscriptionAttributesArg) {
   subscriptionAttributes = subscriptionAttributesArg;
}

public void setConcurrencyChecksEnabled(boolean concurrencyChecksEnabledArg) {
   concurrencyChecksEnabled = new Boolean(concurrencyChecksEnabledArg);
}
public void setConcurrencyChecksEnabled(Boolean concurrencyChecksEnabledArg) {
   concurrencyChecksEnabled = concurrencyChecksEnabledArg;
}

public void setRequiredRoles(List rolesArg) {
   requiredRoles = rolesArg;
}

public void setLossAction(LossAction actionArg) {
   lossAction = actionArg;
}

public void setResumptionAction(ResumptionAction actionArg) {
   resumptionAction = actionArg;
}

public void setConcurrencyLevel(int concurrencyLevelArg) {
   concurrencyLevel = new Integer(concurrencyLevelArg);
}
public void setConcurrencyLevel(Integer concurrencyLevelArg) {
   concurrencyLevel = concurrencyLevelArg;
}

public void setEntryIdleTimeoutSec(int entryIdleTimeoutSecArg) {
   entryIdleTimeoutSec = new Integer(entryIdleTimeoutSecArg);
}
public void setEntryIdleTimeoutSec(Integer entryIdleTimeoutSecArg) {
   entryIdleTimeoutSec = entryIdleTimeoutSecArg;
}

public void setEntryIdleTimeoutAction(ExpirationAction expActionArg) {
   entryIdleTimeoutAction = expActionArg;
}

public void setEntryTTLSec(int entryTTLSecArg) {
   entryTTLSec = new Integer(entryTTLSecArg);
}
public void setEntryTTLSec(Integer entryTTLSecArg) {
   entryTTLSec = entryTTLSecArg;
}

public void setEntryTTLAction(ExpirationAction expActionArg) {
   entryTTLAction = expActionArg;
}

public void setRegionIdleTimeoutSec(int regionIdleTimeoutSecArg) {
   regionIdleTimeoutSec = new Integer(regionIdleTimeoutSecArg);
}
public void setRegionIdleTimeoutSec(Integer regionIdleTimeoutSecArg) {
   regionIdleTimeoutSec = regionIdleTimeoutSecArg;
}

public void setRegionIdleTimeoutAction(ExpirationAction expActionArg) {
   regionIdleTimeoutAction = expActionArg;
}

public void setRegionTTLSec(int regionTTLSecArg) {
   regionTTLSec = new Integer(regionTTLSecArg);
}
public void setRegionTTLSec(Integer regionTTLSecArg) {
   regionTTLSec = regionTTLSecArg;
}

public void setRegionTTLAction(ExpirationAction expActionArg) {
   regionTTLAction = expActionArg;
}

public void setKeyConstraint(Class keyConstraintArg) {
   keyConstraint = keyConstraintArg;
}

public void setLoadFactor(float loadFactorArg) {
   loadFactor = new Float(loadFactorArg);
}
public void setLoadFactor(Float loadFactorArg) {
   loadFactor = loadFactorArg;
}

public void setStatisticsEnabled(boolean statisticsEnabledArg) {
   statisticsEnabled = new Boolean(statisticsEnabledArg);
}
public void setStatisticsEnabled(Boolean statisticsEnabledArg) {
   statisticsEnabled = statisticsEnabledArg;
}

public void setEnableOffHeapMemory(String enableOffHeapMemoryArg) {
   enableOffHeapMemory = enableOffHeapMemoryArg;
}

public void setPersistBackup(boolean persistBackupArg) {
   persistBackup = new Boolean(persistBackupArg);
}
public void setPersistBackup(Boolean persistBackupArg) {
   persistBackup = persistBackupArg;
}

public void setAsyncConflation(boolean asyncConflationArg) {
   asyncConflation = new Boolean(asyncConflationArg);
}

public void setAsyncConflation(Boolean asyncConflationArg) {
   asyncConflation = asyncConflationArg;
}

public void setEvictionLimit(int evictionLimitArg) {
   evictionLimit = new Integer(evictionLimitArg);
}
public void setEvictionLimit(Integer evictionLimitArg) {
   evictionLimit = evictionLimitArg;
}

public void setIsSynchronous(boolean isSynchronousArg) {
   isSynchronous = new Boolean(isSynchronousArg);
}
public void setIsSynchronous(Boolean isSynchronousArg) {
   isSynchronous = isSynchronousArg;
}

public void setTimeInterval(long timeIntervalArg) {
   timeInterval = new Long(timeIntervalArg);
}
public void setTimeInterval(Long timeIntervalArg) {
   timeInterval = timeIntervalArg;
}

public void setBytesThreshold(long bytesThresholdArg) {
   bytesThreshold = new Long(bytesThresholdArg);
}
public void setBytesThreshold(Long bytesThresholdArg) {
   bytesThreshold = bytesThresholdArg;
}

public void setNumDiskDirs(int numDiskDirsArg) {
   numDiskDirs = new Integer(numDiskDirsArg);
}
public void setNumDiskDirs(Integer numDiskDirsArg) {
   numDiskDirs = numDiskDirsArg;
}

public void setEvictionAction(String evictionActionArg) {
   evictionAction = evictionActionArg;
}

public void setDiskDirList(List aList) {
   diskDirList = aList;
}

public void setEviction(String evictionArg) {
   eviction = evictionArg;
}

public void setCacheListener(String listenerClassName) {
   cacheListeners = new ArrayList();
   cacheListeners.add(listenerClassName);
}

public void setCacheListeners(List cacheListenerArg) {
   cacheListeners = cacheListenerArg;
}

public void addCacheListener(String cacheListenerClassName) {
   if (cacheListeners == null)
      cacheListeners = new ArrayList();
   cacheListeners.add(cacheListenerClassName);
}

public void setCacheLoader(String cacheLoaderArg) {
   cacheLoader = cacheLoaderArg;
}

public void setCacheWriter(String cacheWriterArg) {
   cacheWriter = cacheWriterArg;
}

public void setObjectSizerClass(String sizerClass) {
   objectSizerClass = sizerClass;
}


/** Sets whether region definition should specify region with multicastEnabled.
 *
 *  @since 5.0
 *  @author Jean Farris
 */
public void setUseDsMulticastSetting(Boolean useDsMulticastSettingArg) {
   useDsMulticastSetting = useDsMulticastSettingArg;
}
/** Sets whether region definition should have same multicastEnabled
 *  value as distributed system (default is true).
 *
 *  @since 5.0
 *  @author Jean Farris
 */
public void setMulticastEnabled(Boolean multicastEnabledArg) {
   multicastEnabled = multicastEnabledArg;
}

///added by Prafulla

public void setRollOplogs (Boolean rollOplogsArg) {
	rollOplogs = rollOplogsArg;
}

public void setRollOplogs (boolean rollOplogsArg) {
	rollOplogs = new Boolean (rollOplogsArg);
}

public void setMaxOplogSize (Integer maxOplogSizeArg) {
	maxOplogSize = maxOplogSizeArg;
}

public void setMaxOplogSize (int maxOplogSizeArg) {
	maxOplogSize = new Integer(maxOplogSizeArg);
}

/** Set the subregion list for this region definition.
 * 
 *  @param regDefList The list of all subregions of this region.
 */
public void setSubRegion(ArrayList regDefList) {
   if (regDefList == null)
      throw new TestException("subregion list cannot be null");
   subRegions = regDefList;
}

/** Add a subregion to this region's definition. Used for generating
 *  a region hierarchy in a cacheXml file.
 * 
 *  @param regDef The definition of a subregion of this region.
 */
public void addSubRegion(RegionDefinition regDef) {
   if (regDef == null)
      throw new TestException("Cannot add a null subregion");
   subRegions.add(regDef);
}

public void setPartionedRegion(boolean aBool) {
   parReg = new Boolean(aBool);
}

public void setParRegCacheWriter(String cacheWriterArg) {
   parReg_cacheWriter = cacheWriterArg;
}

public void setParRegRoutingResolver(String routingResolverArg) {
   parReg_partitionResolver = routingResolverArg;
}

public void setParRegColocatedWith(String colocatedArg) {
   parReg_colocatedWith = colocatedArg;
}

public void setParRegEntryIdleTimeoutSec(int entryIdleTimeoutSecArg) {
   parReg_entryIdleTimeoutSec = new Integer(entryIdleTimeoutSecArg);
}
public void setParRegEntryIdleTimeoutSec(Integer entryIdleTimeoutSecArg) {
   parReg_entryIdleTimeoutSec = entryIdleTimeoutSecArg;
}
public void setParRegEntryIdleTimeoutAction(ExpirationAction expActionArg) {
   parReg_entryIdleTimeoutAction = expActionArg;
}

public void setParRegEntryTTLSec(int entryTTLSecArg) {
   parReg_entryTTLSec = new Integer(entryTTLSecArg);
}
public void setParRegEntryTTLSec(Integer entryTTLSecArg) {
   parReg_entryTTLSec = entryTTLSecArg;
}
public void setParRegEntryTTLAction(ExpirationAction expActionArg) {
   parReg_entryTTLAction = expActionArg;
}

public void setParRegLocalProperties(Properties propArg) {
   parReg_localProperties = propArg;
}

public void setParRegGlobalProperties(Properties propArg) {
   parReg_globalProperties = propArg;
}

//public void setParRegRoutingResolverProperties(Properties propArg) {
//   parReg_partitionResolver_properties = propArg;
//}

public void setParRegRedundantCopies(int intArg) {
   parReg_redundantCopies = new Integer(intArg);
}

/** Set all partitioned region attributes in regDef to the
  * current settings in this instance of RegionDefinition.
  *
  * @param regDef The region defintion to have its partitioned region
  *        attributes set.
  */
public void setParRegAttributes(RegionDefinition regDef) {
   regDef.parReg = this.parReg;
   regDef.parReg_cacheWriter = this.parReg_cacheWriter;
   regDef.parReg_colocatedWith = this.parReg_colocatedWith;
   regDef.parReg_partitionResolver = this.parReg_partitionResolver;
//   regDef.parReg_partitionResolver_properties = this.parReg_partitionResolver_properties;
   regDef.parReg_entryIdleTimeoutSec = this.parReg_entryIdleTimeoutSec;
   regDef.parReg_entryIdleTimeoutAction = this.parReg_entryIdleTimeoutAction;
   regDef.parReg_entryTTLSec = this.parReg_entryTTLSec;
   regDef.parReg_entryTTLAction = this.parReg_entryTTLAction;
   regDef.parReg_localProperties = this.parReg_localProperties;
   regDef.parReg_globalProperties = this.parReg_globalProperties;
   regDef.parReg_redundantCopies = this.parReg_redundantCopies;
}

}
