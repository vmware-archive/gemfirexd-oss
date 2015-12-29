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
/*
 * Created on Mar 22, 2005
 */
package com.gemstone.gemfire.internal.datasource;

import java.sql.Connection;
import java.util.*;
import javax.naming.Context;
//import javax.sql.PooledConnection;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import junit.framework.TestCase;

/**
 * @author mitulb
 * 
 * To change the template for this generated type comment go to Window -
 * Preferences - Java - Code Generation - Code and Comments
 */
public class CleanUpTest extends TestCase {

  private static Properties props = null;
  private static DistributedSystem ds1 = null;
  private static Cache cache = null;
  static {
    try {
      props = new Properties();
      //      props.setProperty("mcast-port","33405");
      String path = System.getProperty("JTAXMLFILE");
      props.setProperty("cache-xml-file", path);
      ds1 = DistributedSystem.connect(props);
      cache = CacheFactory.create(ds1);
    }
    catch (Exception e) {
      fail("Exception occured in creating ds and cache due to " + e);
      e.printStackTrace();
    }
  }

  public CleanUpTest(String name) {
    super(name);
  }

  public void setup() {
  }

  public void teardown() {
  }

  public void testGetSimpleDataSource() throws Exception {
    try {
      Context ctx = cache.getJNDIContext();
      GemFireBasicDataSource ds = (GemFireBasicDataSource) ctx
          .lookup("java:/SimpleDataSource");
      Connection conn = ds.getConnection();
      if (conn == null)
          fail("DataSourceFactoryTest-testGetSimpleDataSource() Error in creating the GemFireBasicDataSource");
    }
    catch (Exception e) {
      fail("Exception occured in testGetSimpleDataSource due to " + e);
      e.printStackTrace();
    }
  }

  /*
   * public void testExpiration() { try { Context ctx = cache.getJNDIContext();
   * GemFireConnPooledDataSource ds = (GemFireConnPooledDataSource) ctx
   * .lookup("java:/PooledDataSource"); GemFireConnectionPoolManager provider =
   * (GemFireConnectionPoolManager) ds .getConnectionProvider();
   * ConnectionPoolCacheImpl poolCache = (ConnectionPoolCacheImpl) provider
   * .getConnectionPoolCache(); PooledConnection conn =
   * poolCache.getPooledConnectionFromPool();
   * poolCache.returnPooledConnectionToPool(conn);
   * Thread.sleep(poolCache.expirationTime * 2); if
   * (!(poolCache.availableCache.isEmpty())) { fail("Clean-up on expiration not
   * done"); } } catch (Exception e) { e.printStackTrace(); } }
   */
  public void testBlockingTimeOut() {
    try {
      Context ctx = cache.getJNDIContext();
      GemFireConnPooledDataSource ds = (GemFireConnPooledDataSource) ctx
          .lookup("java:/PooledDataSource");
      GemFireConnectionPoolManager provider = (GemFireConnectionPoolManager) ds
          .getConnectionProvider();
      ConnectionPoolCacheImpl poolCache = (ConnectionPoolCacheImpl) provider
          .getConnectionPoolCache();
      poolCache.getPooledConnectionFromPool();
      Thread.sleep(40000);
      if (!(poolCache.activeCache.isEmpty())) {
        fail("Clean-up on expiration not done");
      }
    }
    catch (Exception e) {
      fail("Exception occured in testBlockingTimeOut due to " + e);
      e.printStackTrace();
    }
  }

  public void testCleanUp() {
    cache.close();
    ds1.disconnect();
  }
}