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
package container.app.phonedirectory.dao.provider;

import java.io.InputStream;
import java.util.Collection;
import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Region;

import container.app.lang.Configurable;
import container.app.lang.Destroyable;
import container.app.lang.Initable;
import container.app.phonedirectory.dao.AbstractPhoneDirectoryDao;
import container.app.phonedirectory.domain.Person;
import container.app.phonedirectory.domain.PhoneDirectoryEntry;
import container.app.util.Assert;
import container.app.util.IOUtils;
import container.app.util.StringUtils;

public class CachingPhoneDirectoryDao extends AbstractPhoneDirectoryDao implements Initable, Configurable<Properties>, Destroyable {

  protected static final boolean DEFAULT_LAZY_INITIALIZE = false;
  
  private static Properties props;

  public static final String DEFAULT_GEMFIRE_CACHE_XML_FILE = "/container/app/phonedirectory/dao/provider/phoneDirectoryCache.xml";
  public static final String DEFAULT_GEMFIRE_CONFIGURATION_FILE = "/container/app/phonedirectory/dao/provider/gemfire.properties";
  public static final String DEFAULT_GEMFIRE_LOG_LEVEL = "warning";
  public static final String DEFAULT_GEMFIRE_MCAST_PORT = "56123";
  public static final String PHONE_DIRECTORY_CACHE_NAME = "CachingPhoneDirectoryDao";
  public static final String PHONE_DIRECTORY_REGION_PATH = "/phoneDirectory";

  private boolean configured = false;
  private boolean destroyed = false;
  private boolean initialized = false;

  private Cache phoneDirectoryCache;

  private Region<String, PhoneDirectoryEntry> phoneDirectoryRegion;

  public static InputStream getCacheConfiguration() {
    return IOUtils.open(CachingPhoneDirectoryDao.class.getResource(DEFAULT_GEMFIRE_CACHE_XML_FILE));
  }

  public static synchronized Properties getGemFireConfiguration() {
    if (props == null) {
      props = new Properties();

      try {
        props.load(CachingPhoneDirectoryDao.class.getResourceAsStream(DEFAULT_GEMFIRE_CONFIGURATION_FILE));
      }
      catch (Exception e) {
        props.setProperty("log-level", DEFAULT_GEMFIRE_LOG_LEVEL);
        props.setProperty("mcast-port", DEFAULT_GEMFIRE_MCAST_PORT);
      }
    }

    return props;
  }

  public CachingPhoneDirectoryDao() {
    this(DEFAULT_LAZY_INITIALIZE);
  }

  public CachingPhoneDirectoryDao(final boolean lazyInitialize) {
    if (!lazyInitialize) {
      init();
    }
  }

  public Properties getConfiguration() {
    return getGemFireConfiguration();
  }

  public synchronized boolean isConfigured() {
    return configured;
  }

  public synchronized boolean isDestroyed() {
    return destroyed;
  }

  public synchronized boolean isInitialized() {
    return initialized;
  }

  public synchronized void configure(final Properties configuration) {
    getGemFireConfiguration().putAll(configuration);
    this.configured = true;
  }

  public synchronized void destroy() {
    Assert.state(isInitialized(), "The Phone Directory DAO ({0}) is not currently initialized!", getClass().getName());
    this.phoneDirectoryCache.close();
    this.phoneDirectoryCache = null;
    this.phoneDirectoryRegion = null;
    this.configured = false;
    this.destroyed = true;
    this.initialized = false;
  }

  public synchronized void init() {
    Assert.state(!isInitialized(), "The Phone Directory DAO ({0}) has been already been initialized!", getClass().getName());
    // TODO the cache is a perfect candidate for configuration and dependency injection using Spring baby!
    // TODO use the Spring GemFire Integration API
    this.phoneDirectoryCache = new CacheFactory(getGemFireConfiguration())
      .set("name", PHONE_DIRECTORY_CACHE_NAME)
      .create();
    this.phoneDirectoryCache.loadCacheXml(getCacheConfiguration());
    this.phoneDirectoryRegion = phoneDirectoryCache.getRegion(PHONE_DIRECTORY_REGION_PATH);
    this.configured = false;
    this.destroyed = false;
    this.initialized = true;
  }

  protected Cache getCache() {
    Assert.state(isInitialized(), "The Phone Directory DAO ({0}) is not initialized!", getClass().getName());
    return phoneDirectoryCache;
  }

  protected String getKey(final PhoneDirectoryEntry entry) {
    Assert.notNull(entry, "The Phone Directory Entry used to construct a Region key cannot be null!");
    return getKey(entry.getPerson());
  }

  protected String getKey(final Person person) {
    Assert.notNull(person, "The Person used to construct a Region key cannot be null!");

    final String personsName = StringUtils.toLowerCase(person.getFullName());
    Assert.hasValue(personsName, "The Person's full name must be specified!");

    return personsName;
  }

  protected Region<String, PhoneDirectoryEntry> getRegion() {
    Assert.state(isInitialized(), "The Phone Directory DAO ({0}) is not initialized!", getClass().getName());
    return phoneDirectoryRegion;
  }

  @Override
  public PhoneDirectoryEntry find(final Person person) {
    return getRegion().get(getKey(person));
  }

  @Override
  public Collection<PhoneDirectoryEntry> loadAll() {
    return getRegion().values();
  }

  @Override
  public PhoneDirectoryEntry remove(final Person person) {
    return getRegion().destroy(getKey(person));
  }

  @Override
  public PhoneDirectoryEntry save(final PhoneDirectoryEntry entry) {
    getRegion().put(getKey(entry), entry);
    return entry;
  }

  @Override
  public String toString() {
    return "Caching Phone Directory DAO";
  }

}
