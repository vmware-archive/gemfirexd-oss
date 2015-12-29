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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.CancelException;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DiskStore;
import com.gemstone.gemfire.cache.DiskStoreFactory;
import com.gemstone.gemfire.cache.DynamicRegionFactory;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.client.Pool;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.client.ClientRegionFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import com.gemstone.gemfire.internal.cache.xmlcache.ClientCacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.FunctionServiceCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.ResourceManagerCreation;

/**
 * Helps clients use {@link ClientCacheDescription}.  Methods are thread-safe.
 */
public class ClientCacheHelper {

  /** Name of the client cache description used to create the current cache */
  protected static String TheCacheConfig;

  /** Maps of cache, region, and pool descriptions used to generate XML */
  private static Map XmlCacheConfigs = new HashMap();
  private static Map XmlRegionConfigs = new HashMap();
  private static Map XmlFunctionConfigs = new HashMap();


//------------------------------------------------------------------------------
// ClientCache
//------------------------------------------------------------------------------

  /**
   * Creates a client cache for the given client cache configuration from
   * {@link ClientCachePrms#names}, configured using the corresponding {@link
   * ClientCacheDescription}.  Returns the existing client cache if it was
   * previously created and is still open.
   * <p>
   * The product connects the cache to a loner distributed system, using
   * properties from the {@link GemFireDescription} corresponding to the
   * system property {@link GemFirePrms#GEMFIRE_NAME_PROPERTY}.  Note that
   * this system is not noted in or checked by {@link DistributedSystemHelper}.
   * <p>
   * Hydra creates the default disk store named "DEFAULT" using the {@link
   * DiskStoreDescription} corresponding to {@link ClientCachePrms
   * #defaultDiskStoreName}, if specified.  THe disk store is created using
   * {@link DiskStoreHelper.createDiskStore(String)}.
   * <p>
   * The product creates the default pool, configured using the {@link
   * PoolDescription} corresponding to {@link ClientCachePrms#defaultPoolName},
   * if specified.  Note that this pool is not noted in or checked by {@link
   * PoolHelper}.  If no pool name is given, the product sets the default pool
   * to the existing pool, if exactly one exists, or to null, if more than one
   * pool exists, or to a pool configured with default settings, if no pool
   * exists.
   * <p>
   * The default pool, if any, is only used by regions created through a region
   * shortcut that do not specify some other pool.  If the default pool is null,
   * then regions created through a region shortcut must specify a pool name.
   *
   * @throws HydraRuntimeException if an attempt is made to reconfigure an
   *         existing client cache or distributed system.
   */
  public static synchronized ClientCache createCache(String cacheConfig) {
    if (cacheConfig == null) {
      throw new IllegalArgumentException("cacheConfig cannot be null");
    }
    ClientCache cache = getCache();
    if (cache == null) {

      // look up the cache and distributed system configuration
      ClientCacheDescription ccd = getClientCacheDescription(cacheConfig);
      Properties p = DistributedSystemHelper.getDistributedSystemProperties(null);

      // configure the client cache factory
      ClientCacheFactory factory = new ClientCacheFactory(p);
      log("Configuring client cache factory from config: " + cacheConfig);
      ccd.configure(factory);

      // create the cache
      log("Creating client cache");
      cache = factory.create();
      log("Created client cache: " + cacheToString(cache));

      // create the default disk store, if specified
      DiskStoreDescription dsd = ccd.getDefaultDiskStoreDescription();
      if (dsd != null) {
        String defaultDiskStoreName = dsd.getName();
        DiskStoreFactory diskfactory =
                         DiskStoreHelper.getDiskStoreFactory(dsd.getName());
        DiskStoreHelper.createDiskStore(
                        DiskStoreFactory.DEFAULT_DISK_STORE_NAME, diskfactory);
      }

      // the product creates the default pool

      // save the cache config for future reference
      TheCacheConfig = cacheConfig;

    } else if (TheCacheConfig == null) {
      // block attempt to create cache in multiple ways
      String s = "Cache was already created without ClientCacheHelper using"
               + " an unknown, and possibly different, configuration";
      throw new HydraRuntimeException(s);

    } else if (!TheCacheConfig.equals(cacheConfig)) {
      // block attempt to recreate cache with clashing configuration
      String s = "Cache already exists using cache configuration "
               + TheCacheConfig + ", cannot also use " + cacheConfig;
      throw new HydraRuntimeException(s);

    } // else it was already created with this configuration, which is fine

    return cache;
  }

  /**
   * Creates a client cache using the given XML configuration file.  The
   * distributed system is automatically connected by the product, if needed.
   * Returns the existing cache if it was previously created and is still open.
   *
   * @throws HydraRuntimeException if an attempt is made to reconfigure an
   *         existing cache.
   */
  public static ClientCache createCacheFromXml(String cacheXmlFile) {
    if (cacheXmlFile == null) {
      throw new IllegalArgumentException("cacheXmlFile cannot be null");
    }
    String fn = FileUtil.absoluteFilenameFor(
                EnvHelper.expandEnvVars(cacheXmlFile));
    ClientCache cache = getCache();
    if (cache == null) {
      log("Creating cache from XML: " + fn);
      try {
        Properties p = DistributedSystemHelper.getDistributedSystemProperties(null);
        ClientCacheFactory ccf = new ClientCacheFactory(p);
        cache = ccf.set(DistributionConfig.CACHE_XML_FILE_NAME, fn).create();
      } catch (IllegalStateException e) {
        String s = "Unable to create cache using: " + fn;
        throw new HydraRuntimeException(s, e);
      }
      log("Created cache from XML");

      // save the cache config for future reference
      TheCacheConfig = fn;

    } else if (TheCacheConfig == null) {
      // block attempt to create cache in multiple ways
      String s = "Cache was already created without ClientCacheHelper using"
               + " an unknown, and possibly different, configuration";
      throw new HydraRuntimeException(s);

    } else if (!TheCacheConfig.equals(fn)) {
      // block attempt to recreate cache with clashing configuration
      String s = "Cache already exists using client cache configuration "
               + TheCacheConfig + ", cannot also use " + fn;
      throw new HydraRuntimeException(s);

    } // else it was already created with this configuration, which is fine

    return cache;
  }

  /**
   * Generates XML for the given client cache and region configurations from 
   * {@link ClientCachePrms#names} and {@link ClientRegionPrms#names}, using 
   * the corresponding {@link ClientCacheDescription} and {@link
   * ClientRegionDescription}.  Writes the resulting XML to the specified file.
   * <p>
   * Includes the default pool and disk store from {@link ClientCachePrms
   * #defaultPoolName} and {@link ClientCachePrms#defaultDiskStoreName}, if
   * specified, as well as any pool and disk store specified in the region
   * configuration from {@link ClientRegionPrms#poolName} and {@link
   * ClientRegionPrms#diskStoreName}, plus any disk store specified in {@link
   * ClientCachePrms#pdxDiskStoreName}.
   * <p>
   * IMPORTANT: This method should only be invoked from the JVM that will use
   * the result.  Otherwise, runtime-generated artifacts (e.g., ports, disk
   * directories) might not work as required.
   * <p>
   * IMPORTANT: This method is not synchronized across multiple JVMs.  It is
   * up to the user to avoid writing to the same filename from multiple JVMs.
   *
   * @throws HydraRuntimeException if an attempt is made to rewrite an existing
   *         cache XML file with different content.
   */
  public static synchronized void generateCacheXmlFile(String cacheConfig,
                                  String regionConfig, String cacheXmlFile) {
    generateCacheXmlFile(cacheConfig,
                         null /* dynamicRegionConfig */,
                         regionConfig,
                         null /* regionNames */,
                         null /* functions */,
                         cacheXmlFile);
  }

  /**
   * Generates XML for the given client cache and region configurations from 
   * {@link ClientCachePrms#names} and {@link ClientRegionPrms#names}, using 
   * the corresponding {@link ClientCacheDescription} and {@link
   * ClientRegionDescription}.  Generates a region for each name given.
   * Writes the resulting XML to the specified file.
   * <p>
   * Includes the default pool and disk store from {@link ClientCachePrms
   * #defaultPoolName} and {@link ClientCachePrms#defaultDiskStoreName}, if
   * specified, as well as any pool and disk store specified in the region
   * configuration from {@link ClientRegionPrms#poolName} and {@link
   * ClientRegionPrms#diskStoreName}, plus any disk store specified in {@link
   * ClientCachePrms#pdxDiskStoreName}.
   * <p>
   * IMPORTANT: This method should only be invoked from the JVM that will use
   * the result.  Otherwise, runtime-generated artifacts (e.g., ports, disk
   * directories) might not work as required.
   * <p>
   * IMPORTANT: This method is not synchronized across multiple JVMs.  It is
   * up to the user to avoid writing to the same filename from multiple JVMs.
   *
   * @throws HydraRuntimeException if an attempt is made to rewrite an existing
   *         cache XML file with different content.
   */
  public static synchronized void generateCacheXmlFile(
                String cacheConfig, String regionConfig, List regionNames,
                String cacheXmlFile) {
    generateCacheXmlFile(cacheConfig,
                         null /* dynamicRegionConfig */,
                         regionConfig,
                         regionNames /* regionNames */,
                         null /* functions */,
                         cacheXmlFile);
  }

  /**
   * Generates XML for the given client cache and region configurations from 
   * {@link ClientCachePrms#names} and {@link ClientRegionPrms#names}, using 
   * the corresponding {@link ClientCacheDescription} and {@link
   * ClientRegionDescription}.  Generates a region for each name given.
   * Includes the dynamic region and functions, if given.  Writes the
   * resulting XML to the specified file.
   * <p>
   * Includes the default pool and disk store from {@link ClientCachePrms
   * #defaultPoolName} and {@link ClientCachePrms#defaultDiskStoreName}, if
   * specified, as well as any pool and disk store specified in the region
   * configuration from {@link ClientRegionPrms#poolName} and {@link
   * ClientRegionPrms#diskStoreName}, plus any disk store specified in {@link
   * ClientCachePrms#pdxDiskStoreName}.
   * <p>
   * IMPORTANT: This method should only be invoked from the JVM that will use
   * the result.  Otherwise, runtime-generated artifacts (e.g., ports, disk
   * directories) might not work as required.
   * <p>
   * IMPORTANT: This method is not synchronized across multiple JVMs.  It is
   * up to the user to avoid writing to the same filename from multiple JVMs.
   *
   * @throws HydraRuntimeException if an attempt is made to rewrite an existing
   *         cache XML file with different content.
   */
  public static synchronized void generateCacheXmlFile(
                String cacheConfig,
                DynamicRegionFactory.Config dynamicRegionConfig, 
                String regionConfig, List regionNames,
                List functions, String cacheXmlFile) {
    if (cacheConfig == null) {
      throw new IllegalArgumentException("cacheConfig cannot be null");
    }
    if (cacheXmlFile == null) {
      throw new IllegalArgumentException("cacheXmlFile cannot be null");
    }
    String fn = FileUtil.absoluteFilenameFor(
                EnvHelper.expandEnvVars(cacheXmlFile));
    if (!FileUtil.exists(fn)) {
      log("Generating XML file: " + fn + " from client cache config: "
         + cacheConfig + " and region config " + regionConfig);

      // first connect, if necessary
      DistributedSystem ds = DistributedSystemHelper.getDistributedSystem();
      if (ds == null) {
         DistributedSystemHelper.connectWithXml(fn);
      }

      ClientCacheDescription ccd = getClientCacheDescription(cacheConfig);
      ClientCache dummyCache = generateDummyCache(cacheConfig,
                                       ccd, dynamicRegionConfig, fn);
      generateDummyFunctions(dummyCache, functions, fn);
      {
        PoolDescription pd = ccd.getDefaultPoolDescription();
        if (pd != null) { // add client cache default pool
          generateDummyPool(dummyCache, pd, fn);
        }
      }
      {
        DiskStoreDescription dsd = ccd.getDefaultDiskStoreDescription();
        if (dsd != null) { // add client cache default disk store
          generateDummyDiskStore(dummyCache,
                       DiskStoreFactory.DEFAULT_DISK_STORE_NAME, dsd, fn);
        }
      }
      {
        DiskStoreDescription dsd = ccd.getPdxDiskStoreDescription();
        if (dsd != null) { // add client cache pdx disk store name
          generateDummyDiskStore(dummyCache, dsd.getName(), dsd, fn);
        }
      }

      if (regionConfig != null) {
        ClientRegionDescription rd =
              ClientRegionHelper.getClientRegionDescription(regionConfig);
        {
          PoolDescription pd = rd.getPoolDescription();
          if (pd != null) { // add client region pool
            generateDummyPool(dummyCache, pd, fn);
          }
        }
        {
          DiskStoreDescription dsd = rd.getDiskStoreDescription();
          if (dsd != null) { // add client region disk store
            generateDummyDiskStore(dummyCache, dsd.getName(), dsd, fn);
          }
        }
        if (regionNames != null && regionNames.size() > 0) {
          for (Iterator i = regionNames.iterator(); i.hasNext();) {
            String regionName = (String)i.next();
            generateDummyRegion(dummyCache, regionConfig, rd, regionName, fn);
          }
        } else {
          generateDummyRegion(dummyCache, regionConfig, rd, null, fn);
        }
      }

      // generate the xml file
      generateCacheXmlFile(fn, dummyCache);

    } else {
      if (XmlCacheConfigs.get(fn) == null) {
        // block attempt to create file in multiple ways
        String s = "Cache XML file was already created without ClientCacheHelper"
                 + " or from a different JVM using"
                 + " an unknown, and possibly different, configuration: " + fn;
        throw new HydraRuntimeException(s);

      } else {
        // block attempt to recreate file with clashing configuration
        String cc = (String)XmlCacheConfigs.get(fn);
        if (!cc.equals(cacheConfig)) {
          String s = "Cache XML file already exists using cache configuration "
                   + cc + ", cannot also use " + cacheConfig + ": " + fn;
          throw new HydraRuntimeException(s);
        }
        String rc = (String)XmlRegionConfigs.get(fn);
        if ((rc == null && regionConfig != null)
         || (rc != null && regionConfig == null)
	 || (rc != null && regionConfig != null && !rc.equals(regionConfig))) {
          String s = "Cache XML file already exists using region configuration "
                   + rc + ", cannot also use " + regionConfig + ": " + fn;
          throw new HydraRuntimeException(s);
        }
        List fc = (List)XmlFunctionConfigs.get(fn);
        List functionNames = classnamesFor(functions);
        if ((fc == null && functionNames != null)
         || (fc != null && functionNames == null)
	 || (fc != null && functionNames != null
                        && !(fc.containsAll(functionNames)
                             && functionNames.containsAll(fc)))) {
          String s = "Cache XML file already exists using functions "
                   + fc + ", cannot also use " + functions + ": " + fn;
          throw new HydraRuntimeException(s);
        }
        // else it was already created with this configuration, which is fine
        return;
      }
    }
  }

  /**
   * Generates a dummy cache from the given client cache description.
   */
  private static ClientCacheCreation generateDummyCache(
                 String cacheConfig, ClientCacheDescription ccd, 
                 DynamicRegionFactory.Config dynamicRegionConfig, String fn) {
    if (dynamicRegionConfig != null) {
      // must open the DynamicRegionFactory before creating the cache
      log("Opening the configured DynamicRegionFactory");
      DynamicRegionFactory.get().open(dynamicRegionConfig);
    }
    // create and configure the dummy cache
    log("Adding dummy cache from: " + ccd);
    ClientCacheCreation dummyCache = new ClientCacheCreation();
    ResourceManagerCreation dummyResourceManager = new ResourceManagerCreation();
    dummyCache.setResourceManagerCreation(dummyResourceManager);
    ccd.configureDummy(dummyCache);

    // add the dynamic region factory configuration, if any
    if (dynamicRegionConfig != null) {
      dummyCache.setDynamicRegionFactoryConfig(dynamicRegionConfig);
    }
    log("Added dummy cache: " + ccd.cacheToString(dummyCache));

    // save the cache config for future reference
    XmlCacheConfigs.put(fn, cacheConfig);

    return dummyCache;
  }

  /**
   * Generates a dummy pool from the given cache and pool description.
   */
  private static void generateDummyPool(ClientCache dummyCache,
                                        PoolDescription pd, String fn) {
    // create and configure the dummy pool
    PoolFactory dummyFactory =
      ((ClientCacheCreation)dummyCache).createPoolFactory();
    pd.configure(dummyFactory);
    String poolName = pd.getName();
    Pool dummyPool = null;
    try {
      dummyPool = dummyFactory.create(poolName);
    } catch (IllegalStateException e) {
      String msg = "A pool named \"" + poolName + "\" already exists.";
      throw new HydraRuntimeException(msg, e);
    }
    log("Added dummy pool: " + PoolHelper.poolToString(dummyPool));
  }

  /**
   * Generates a dummy disk store from the given cache and disk store
   * description.
   */
  private static void generateDummyDiskStore(ClientCache dummyCache,
                 String diskStoreName, DiskStoreDescription dsd, String fn) {
    // create and configure the dummy disk store
    DiskStoreFactory dummyFactory =
      ((ClientCacheCreation)dummyCache).createDiskStoreFactory();
    dsd.configure(dummyFactory);
    DiskStore dummyDiskStore = null;
    try {
      dummyDiskStore = dummyFactory.create(diskStoreName);
    } catch (IllegalStateException e) {
      String msg = "A disk store named \"" + diskStoreName
                 + "\" already exists.";
      throw new HydraRuntimeException(msg, e);
    }
    log("Added dummy disk store: "
        + DiskStoreHelper.diskStoreToString(dummyDiskStore));
  }

  /**
   * Generates a dummy region from the given cache, region configuration, and
   * region name.
   */
  private static void generateDummyRegion(ClientCache dummyCache,
                      String regionConfig, ClientRegionDescription rd,
                      String regionName, String fn) {
    if (rd != null) {
      // create and configure the dummy region
      String rName = regionName;
      if (rName == null) {
        rName = rd.getRegionName();
      }
      ClientRegionShortcut shortcut = rd.getClientRegionShortcut();
      log("Adding dummy region named: " + rName
         + " from region config " + regionConfig
         + " using client region shortcut: " + shortcut.toString());
      try {
        Region region = ((ClientCacheCreation)dummyCache).createRegion(rName,
                                                          shortcut.toString());
        RegionAttributesCreation ratts = new RegionAttributesCreation();
        // don't set refid in the attributes, it is already set in the region
        ((RegionCreation)region).setAttributes(ratts, false);
        rd.configure(ratts, false);
        log("Added dummy region named: " + rName + " with attributes: "
          + RegionDescription.regionAttributesToString(region.getAttributes()));
      } catch (RegionExistsException e) {
        throw new HydraInternalException("Should not happen", e);
      }

      // save the region config for future reference
      XmlRegionConfigs.put(fn, regionConfig);
    }
  }

  /**
   * Generates dummy functions from the given cache and function list.
   */
  private static void generateDummyFunctions(ClientCache dummyCache,
                                             List functions, String fn) {
    if (functions != null) {
      // create and configure the dummy functions
      log("Adding dummy functions: " + functions);
      FunctionServiceCreation fsc = new FunctionServiceCreation();
      for (Iterator i = functions.iterator(); i.hasNext();) {
        Function function = (Function)i.next();
        fsc.registerFunction(function);
      }
      //fsc.create(); // not needed, functions are registered during createCacheWithXml
      ((ClientCacheCreation)dummyCache).setFunctionServiceCreation(fsc);
      log("Added dummy functions: " + fsc.getFunctions());

      // save the functions for future reference
      XmlFunctionConfigs.put(fn, classnamesFor(functions));
    }
  }
  private static List classnamesFor(List objs) {
    List classnames = null;
    if (objs != null) {
      classnames = new ArrayList();
      for (Iterator i = objs.iterator(); i.hasNext();) {
        Object obj = i.next();
        classnames.add(obj.getClass().getName());
      }
    }
    return classnames;
  }

  /**
   * Generates a cache XML file with the given filename.  Overwrites any
   * existing file with the same name.  This method is not thread safe.
   */
  private static void generateCacheXmlFile(String fn, ClientCache dummyCache) {
    log("Generating XML file: " + fn);
    PrintWriter pw = null;
    try {
      pw = new PrintWriter(new FileWriter(new File(fn)));
    } catch (IOException e) {
      String s = "Unable to open file: " + fn;
      throw new HydraRuntimeException(s, e);
    }
    CacheXmlGenerator.generate(dummyCache, pw);
    log("Generated XML file: " + fn);
  }


  /**
   * Returns the client cache if it exists and is open, or null if no cache exists.
   */
  public static synchronized ClientCache getCache() {
    ClientCache c = null;
    try {
      c = ClientCacheFactory.getAnyInstance();
    } catch (CancelException ce) {
      // no instances, just return null
    }
    return c;
  }

  /**
   * Closes the cache if it exists and is open.
   */
  public static synchronized void closeCache() {
    ClientCache cache = getCache();
    if (cache != null) {
      log("Closing cache: " + cacheToString(cache));
      cache.close();
      log("Closed cache");
      TheCacheConfig = null; // so the next create can have a different config
    }
  }

  /**
   * Returns the given cache as a string.
   */
  public static String cacheToString(ClientCache cache) {
    return ClientCacheDescription.cacheToString(cache);
  }

//------------------------------------------------------------------------------
// ClientCacheDescription
//------------------------------------------------------------------------------

  /**
   * Returns the {@link ClientCacheDescription} with the given configuration name
   * from {@link ClientCachePrms#names}.
   */
  public static ClientCacheDescription getClientCacheDescription(String cacheConfig) {
    if (cacheConfig == null) {
      throw new IllegalArgumentException("cacheConfig cannot be null");
    }
    log("Looking up cache config: " + cacheConfig);
    ClientCacheDescription ccd = TestConfig.getInstance()
                                    .getClientCacheDescription(cacheConfig);
    if (ccd == null) {
      String s = cacheConfig + " not found in "
               + BasePrms.nameForKey(ClientCachePrms.names);
      throw new HydraRuntimeException(s);
    }
    log("Looked up client cache config:\n" + ccd);
    return ccd;
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
