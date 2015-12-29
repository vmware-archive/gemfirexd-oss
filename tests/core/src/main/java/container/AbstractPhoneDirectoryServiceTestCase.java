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
package container;

import java.rmi.Naming;

import junit.extensions.TestSetup;
import junit.framework.Assert;
import junit.framework.Test;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;

import container.app.phonedirectory.dao.provider.CachingPhoneDirectoryDao;
import container.app.phonedirectory.domain.Address;
import container.app.phonedirectory.domain.Person;
import container.app.phonedirectory.domain.PhoneDirectoryEntry;
import container.app.phonedirectory.domain.PhoneNumber;
import container.app.phonedirectory.remoting.rmi.PhoneDirectoryServiceExporter;
import container.app.phonedirectory.service.PhoneDirectoryService;
import container.app.util.ComposableFilter;
import container.app.util.DefaultFilter;
import container.app.util.Filter;
import container.app.util.Sys;

public abstract class AbstractPhoneDirectoryServiceTestCase extends ContainerTestCase {

  protected static final String CACHE_XML_FILE = "/container/phoneDirectoryCache.xml";
  protected static final String PHONE_DIRECTORY_CACHE_NAME = "PhoneDirectoryServiceTest";
  protected static final String PHONE_DIRECTORY_REGION_PATH = CachingPhoneDirectoryDao.PHONE_DIRECTORY_REGION_PATH;

  private static PhoneDirectoryService service;

  private static Region<String, PhoneDirectoryEntry> region;

  protected static Filter<Object> getSystemPropertyFilter() {
    final SystemPropertyFilter filter = new SystemPropertyFilter();
    // Note, use 'null' in place of 'filter' to show all System properties.
    return ComposableFilter.composeAnd(filter, new DefaultFilter<Object>(true));
  }

  public AbstractPhoneDirectoryServiceTestCase(final String testName) {
    super(testName);
  }

  protected static PhoneDirectoryService getPhoneDirectoryService() {
    container.app.util.Assert.state(service != null, "The Phone Directory Service was not properly initialized!");
    return service;
  }

  protected static Region<String, PhoneDirectoryEntry> getRegion() {
    container.app.util.Assert.state(region != null, "The ({0}) Region reference was not properly initialized!", 
        PHONE_DIRECTORY_REGION_PATH);
    return region;
  }
  
  protected static void assertEquals(final PhoneDirectoryEntry expected, final PhoneDirectoryEntry actual) {
    Assert.assertEquals(expected.getPerson(), actual.getPerson());
    Assert.assertEquals(expected.getAddress(), actual.getAddress());
    Assert.assertEquals(expected.getPhoneNumber(), actual.getPhoneNumber());
  }

  protected Address createAddress(final String street1, final String city, final String state, final String zipCode) {
    return new Address(street1, city, state, zipCode);
  }

  protected Person createPerson(final String firstName, final String lastName) {
    return new Person(firstName, lastName);
  }

  protected PhoneNumber createPhoneNumber(final String areaCode, final String prefix, final String suffix) {
    return new PhoneNumber(areaCode, prefix, suffix);
  }

  protected PhoneDirectoryEntry createPhoneDirectoryEntry(final Person person, final Address address, final PhoneNumber phoneNumber) {
    return new PhoneDirectoryEntry(person, address, phoneNumber);
  }

  protected static abstract class AbstractPhoneDirectoryServiceTestSetup extends TestSetup {

    public AbstractPhoneDirectoryServiceTestSetup(final Test test) {
      super(test);
    }

    protected String getRmiRegistryPort() {
      return System.getProperty("gemfire.container-tests.rmi-registry.port",
        String.valueOf(PhoneDirectoryServiceExporter.DEFAULT_REGISTRY_PORT));
    }

    protected String getServiceUrl() {
      final StringBuilder buffer = new StringBuilder("//");
      buffer.append(PhoneDirectoryServiceExporter.DEFAULT_REGISTRY_HOST);
      buffer.append(":");
      buffer.append(getRmiRegistryPort());
      buffer.append("/");
      buffer.append(PhoneDirectoryServiceExporter.SERVICE_NAME);
      return buffer.toString();
    }

    @Override
    protected void setUp() throws Exception {
      super.setUp();
      showSystemState(getSystemPropertyFilter());
      final String serviceUrl = getServiceUrl();
      Sys.out("Phone Directory Service URL ({0})", serviceUrl);
      service = (PhoneDirectoryService) Naming.lookup(serviceUrl);
    }
    
    @Override
    protected void tearDown() throws Exception {
      service = null;
    }
  }

  protected static class PhoneDirectoryServiceCacheTestSetup extends AbstractPhoneDirectoryServiceTestSetup {

    private Cache cache;

    public PhoneDirectoryServiceCacheTestSetup(final Test test) {
      super(test);
    }

    @Override
    protected void setUp() throws Exception {
      super.setUp();
      cache = createCache(PHONE_DIRECTORY_CACHE_NAME, CACHE_XML_FILE, null);
      region = cache.getRegion(PHONE_DIRECTORY_REGION_PATH);
    }

    @Override
    protected void tearDown() throws Exception {
      super.tearDown();
      closeCache(cache);
    }
  }

  @SuppressWarnings("unused")
  protected static class PhoneDirectoryServiceClientCacheTestSetup extends AbstractPhoneDirectoryServiceTestSetup {

    private ClientCache clientCache;

    public PhoneDirectoryServiceClientCacheTestSetup(final Test test) {
      super(test);
    }

    @Override
    protected void setUp() throws Exception {
      super.setUp();
      clientCache = createClientCache();
      region = clientCache.<String, PhoneDirectoryEntry>createClientRegionFactory(ClientRegionShortcut.PROXY)
        .create(PHONE_DIRECTORY_REGION_PATH.substring(1));
    }
    
    @Override
    protected void tearDown() throws Exception {
      super.tearDown();
      closeClientCache(clientCache);
    }
  }

}
