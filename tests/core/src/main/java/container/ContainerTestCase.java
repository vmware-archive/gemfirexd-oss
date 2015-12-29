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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import junit.framework.TestCase;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.LocalLogWriter;
import com.gemstone.gemfire.internal.LogWriterImpl;

import container.app.util.Assert;
import container.app.util.Filter;
import container.app.util.StringUtils;
import container.app.util.Sys;

/**
 * TODO: dunit.VM might be useful for these tests... including:
 * {@link dunit.VM#invoke(java.util.concurrent.Callable)} and 
 * {@link dunit.VM#invokeAsync(java.util.concurrent.Callable)} and all the
 * override flavors.
 * </p>
 * @author Kirk Lund
 * @author John Blum
 * @since 6.6
 */
@SuppressWarnings("unused")
public class ContainerTestCase extends TestCase {

  private static final LogWriter logWriter = new LocalLogWriter(LogWriterImpl.INFO_LEVEL);

  private static final String DEFAULT_GEMFIRE_MCAST_PORT = "56123";

  public ContainerTestCase(String name) {
    super(name);
  }

  protected static com.gemstone.gemfire.LogWriter getLogWriter() {
    return logWriter;
  }

  protected static String getRandomAvailableMCastPortString() {
    return String.valueOf(AvailablePort.getRandomAvailablePort(AvailablePort.JGROUPS));
  }

  protected static void closeCache(final Cache cache) {
    cache.close();
  }
  
  protected static void closeClientCache(final ClientCache cache) {
    cache.close(true);
  }

  protected static Cache createCache(final String cacheName, final String cacheXmlFile, final Properties gemfireProperties) {
    return new CacheFactory(gemfireProperties)
      .set(DistributionConfig.NAME_NAME, cacheName)
      .set("cache-xml-file", cacheXmlFile)
      .create();
  }

  protected static ClientCache createClientCache() {
    return new ClientCacheFactory()
      .addPoolServer("localhost", Integer.parseInt(DEFAULT_GEMFIRE_MCAST_PORT))
      .setPoolSubscriptionEnabled(true)
      .setPoolSubscriptionRedundancy(1)
      .create();
  }

  protected static void showGemFireState(final Filter<Object> propertyFilter) {
    throw new UnsupportedOperationException(StringUtils.NOT_IMPLEMENTED);
  }

  protected static void showSystemState(final Filter<Object> systemPropertyFilter) {
    Assert.notNull(systemPropertyFilter, "The object used to filter the System properties cannot be null!");

    for (final Object property : System.getProperties().keySet()) {
      if (systemPropertyFilter.accept(property)) {
        Sys.out("{0} = {1}", property, System.getProperty(property.toString()));
      }
    }
  }

  protected void sleep(long millis) { // TODO: copy pause from dunit?
    try {
      Thread.sleep(millis);
    } 
    catch (InterruptedException ex) {
      getLogWriter().info("Unexpected interrupt during sleep");
    }
  }

  protected static class SystemPropertyFilter implements Filter<Object> {

    private static final List<String> systemProperties = new ArrayList<String>();

    public SystemPropertyFilter() {
    }

    public boolean accept(final Object obj) {
      return systemProperties.contains(obj);
    }

    public boolean add(final String systemProperty) {
      return (StringUtils.hasValue(systemProperty) && systemProperties.add(systemProperty));
    }
    
    public boolean remove(final String systemProperty) {
      return systemProperties.remove(systemProperty);
    }
  }

}
