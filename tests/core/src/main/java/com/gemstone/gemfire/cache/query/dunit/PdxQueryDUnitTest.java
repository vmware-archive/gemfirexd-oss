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
package com.gemstone.gemfire.cache.query.dunit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.CqAttributes;
import com.gemstone.gemfire.cache.query.CqAttributesFactory;
import com.gemstone.gemfire.cache.query.CqEvent;
import com.gemstone.gemfire.cache.query.CqListener;
import com.gemstone.gemfire.cache.query.CqQuery;
import com.gemstone.gemfire.cache.query.IndexType;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.cq.dunit.CqQueryTestListener;
import com.gemstone.gemfire.cache.query.data.PortfolioPdx;
import com.gemstone.gemfire.cache.query.data.PositionPdx;
import com.gemstone.gemfire.cache.query.internal.Undefined;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.BridgeServer;
import com.gemstone.gemfire.cache30.BridgeTestCase;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.compression.SnappyCompressor;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.cache.CachedDeserializableFactory;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.pdx.FieldType;
import com.gemstone.gemfire.pdx.PdxInstance;
import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxSerializable;
import com.gemstone.gemfire.pdx.PdxWriter;
import com.gemstone.gemfire.pdx.VersionClassLoader;
import com.gemstone.gemfire.pdx.internal.ClientTypeRegistration;
import com.gemstone.gemfire.pdx.internal.PdxType;
import com.gemstone.gemfire.pdx.internal.PeerTypeRegistration;
import com.gemstone.gemfire.pdx.internal.TypeRegistration;

import dunit.DistributedTestCase;
import dunit.Host;
import dunit.SerializableCallable;
import dunit.SerializableRunnable;
import dunit.VM;

/**
 * Tests Querying on Pdx types.
 *
 * @author agingade
 * @since 6.6
 */
public class PdxQueryDUnitTest extends CacheTestCase {

  /** The port on which the bridge server was started in this VM */
  private static int bridgeServerPort;
  
  protected static final Compressor compressor = SnappyCompressor.getDefaultInstance();
  
  final String rootRegionName = "root";

  private final String regionName = "PdxTest";
  private final String regionName2 = "PdxTest2";
  
  private final String regName = "/" + rootRegionName + "/" + regionName;
  private final String regName2 = "/" + rootRegionName + "/" + regionName2;

  // Used with compiled queries.
  private final String[] queryString = new String[] {
    "SELECT DISTINCT id FROM " + regName, // 0
    "SELECT * FROM " + regName, // 1
    "SELECT ticker FROM " + regName, // 2
    "SELECT * FROM " + regName + " WHERE id > 5", // 3
    "SELECT p FROM " + regName + " p, p.idTickers idTickers WHERE p.ticker = 'vmware'", // 4
  };

  public PdxQueryDUnitTest(String name) {
    super(name);
  }

  ////////  Test Methods
  /*
  public void setUp() throws Exception {
    super.setUp();
    
    // avoid IllegalStateException from HandShake by connecting all vms tor
    // system before creating connection pools
    getSystem();
    invokeInEveryVM(new SerializableRunnable("getSystem") {
      public void run() {
        getSystem();
      }
    });    
  }
  */

  public void setUp() throws Exception {
    super.setUp();

    // Reset the testObject numinstance for the next test.
    TestObject.numInstance = 0;
    PortfolioPdx.numInstance = 0;
    PositionPdx.numInstance = 0;
    TestObject2.numInstance = 0;
    // In all VM.
    resetTestObjectInstanceCount();    
  }

  public void tearDown2() throws Exception {
    disconnectAllFromDS(); // tests all expect to create a new ds
    // Reset the testObject numinstance for the next test.
    TestObject.numInstance = 0;
    PortfolioPdx.numInstance = 0;
    PositionPdx.numInstance = 0;
    TestObject2.numInstance = 0;
    // In all VM.
    resetTestObjectInstanceCount();    
  }

  private void resetTestObjectInstanceCount() {
    invokeInEveryVM(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        TestObject.numInstance = 0;
        PortfolioPdx.numInstance = 0;
        PositionPdx.numInstance = 0;
        TestObject2.numInstance = 0;
      }
    });
  }
  
  public void createPool(VM vm, String poolName, String server, int port, boolean subscriptionEnabled) {
    createPool(vm, poolName, new String[]{server}, new int[]{port}, subscriptionEnabled);  
  }

  public void createPool(VM vm, String poolName, String server, int port) {
    createPool(vm, poolName, new String[]{server}, new int[]{port}, false);  
  }

  public void createPool(VM vm, final String poolName, final String[] servers, final int[] ports,
      final boolean subscriptionEnabled) {
    createPool(vm, poolName, servers, ports, subscriptionEnabled, 0);    
  }

  public void createPool(VM vm, final String poolName, final String[] servers, final int[] ports,
      final boolean subscriptionEnabled, final int redundancy) {
    vm.invoke(new CacheSerializableRunnable("createPool :" + poolName) {
      public void run2() throws CacheException {
        // Create Cache.
        Properties props = new Properties();
        props.setProperty("mcast-port", "0");
        props.setProperty("locators", "");
        getSystem(props );
        getCache();        
        PoolFactory cpf = PoolManager.createFactory();
        cpf.setSubscriptionEnabled(subscriptionEnabled);
        cpf.setSubscriptionRedundancy(redundancy);
        for (int i=0; i < servers.length; i++){
          getLogWriter().info("### Adding to Pool. ### Server : " + servers[i] + " Port : " + ports[i]);
          cpf.addServer(servers[i], ports[i]);
        }
        cpf.create(poolName);
      }
    });   
  }


  /**
   * Tests client-server query on PdxInstance. The client receives projected value.
   */
  public void testServerQuery() throws CacheException {

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    final int numberOfEntries = 5;

    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new TestObject(i, "vmware"));
          //region.put("key-"+i, new TestObject(i, "vmware"));
        }
      }
    });


    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries, TestObject.numInstance);
        }
        else {
          assertEquals(0, TestObject.numInstance);
        }

        // Execute query with different type of Results. 
        QueryService qs = getCache().getQueryService();
        Query query = null;
        SelectResults sr = null;
        for (int i=0; i < queryString.length; i++) {
          try {
            query = qs.newQuery(queryString[i]);
            sr = (SelectResults)query.execute();
          } catch (Exception ex) {
            fail("Failed to execute query, " + ex.getMessage());
          }

          for (Object o: sr.asSet()) {
            if (i == 0 && !(o instanceof Integer)) {
              fail("Expected type Integer, not found in result set. Found type :" + o.getClass());
            } else if (i == 1 && !(o instanceof TestObject)) {
              fail("Expected type TestObject, not found in result set. Found type :" + o.getClass());
            } else if (i == 2 && !(o instanceof String)) {
              fail("Expected type String, not found in result set. Found type :" + o.getClass());
            } 
          }
        }
        // Created for TestObject ResultSet .
        assertEquals(numberOfEntries, TestObject.numInstance);        
      }
    });

    this.closeClient(vm1);
    this.closeClient(vm0);
  }


  /**
   * Tests client-server query on PdxInstance. The client receives projected value.
   */
  public void testClientServerQueryWithProjections() throws CacheException {

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;

    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        class PdxObject extends TestObject {
          PdxObject() {}
          
          PdxObject (int id, String ticker) {
            super(id, ticker);
          }
        };
       for (int i=0; i<numberOfEntries; i++) {
         if (CachedDeserializableFactory.preferObject()) {
           region.put("key-"+i, new TestObject(i, "vmware"));
         }
         else {
           region.put("key-"+i, new PdxObject(i, "vmware"));
         }
        }
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        System.out.println("##### Region size is: " + region.size());
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries, TestObject.numInstance);
        }
        else {
          assertEquals(0, TestObject.numInstance);
        }
        //for (int i=0; i<numberOfEntries; i++) {
        //  region.put("key-"+i, new TestObject(i, "vmware"));
        //}
      }
    });

    // Create client region
    final int port0 = vm0.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");
    final int port1 = vm1.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");

    final String host0 = getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueriesWithParamsPool"; 
    createPool(vm2, poolName, new String[]{host0}, new int[]{port0}, true);
    createPool(vm3, poolName, new String[]{host0}, new int[]{port1}, true);

    // Execute client queries
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        SelectResults results = null;
        Comparator comparator = null;
        Object[] resultsArray = null;
        QueryService qService = null;
        try {
          qService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }    
        try {
          getLogWriter().info("### Executing Query :" + queryString[0]);
          Query query = qService.newQuery(queryString[0]);
          results = (SelectResults)query.execute();
        } catch (Exception e) {
          fail("Failed executing " + queryString[0], e);
        }
        assertEquals(numberOfEntries, results.size());
      }
    };

    //vm2.invoke(executeQueries);
    vm3.invoke(executeQueries);
    
    // Check for TestObject instances on Server2.
    // It should be 0
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries, TestObject.numInstance);
        }
        else {
          assertEquals(0, TestObject.numInstance);
        }
      }
    });
    
    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }
  
  /**
   * Tests client-server query on compressed PdxInstance. The client receives uncompressed value.
   */
  public void testClientServerQueryWithCompression() throws CacheException {
    final String randomString = "asddfjkhaskkfdjhzjc0943509328kvnhfjkldsg09q3485ibjafdp9q8y43p9u7hgavpiuaha48uy9afliasdnuaiuqa498qa4"
        + "asddfjkhaskkfdjhzjc0943509328kvnhfjkldsg09q3485ibjafdp9q8y43p9u7hgavpiuaha48uy9afliasdnuaiuqa498qa4"
        + "asddfjkhaskkfdjhzjc0943509328kvnhfjkldsg09q3485ibjafdp9q8y43p9u7hgavpiuaha48uy9afliasdnuaiuqa498qa4"
        + "asddfjkhaskkfdjhzjc0943509328kvnhfjkldsg09q3485ibjafdp9q8y43p9u7hgavpiuaha48uy9afliasdnuaiuqa498qa4";

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;

    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false, false, false, compressor);
        Region region = getRootRegion().getSubregion(regionName);
        assert (region.getAttributes().getCompressor() != null);
        for (int i = 0; i < numberOfEntries; i++) {
          region.put("key-" + i, new TestObject(i, randomString));
        }
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        System.out.println("##### Region size is: " + region.size());
        assertEquals(0, TestObject.numInstance);
      }
    });

    // Create client region
    final int port0 = vm0.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");
    final int port1 = vm1.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");

    final String host0 = getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueriesWithParamsPool";
    createPool(vm2, poolName, new String[] { host0 }, new int[] { port0 }, true);
    createPool(vm3, poolName, new String[] { host0 }, new int[] { port1 }, true);

    // Execute client queries
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      @SuppressWarnings("unchecked")
      public void run2() throws CacheException {
        SelectResults<String> results = null;
        QueryService qService = null;
        try {
          qService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }
        try {
          getLogWriter().info("### Executing Query :" + queryString[2]);
          Query query = qService.newQuery(queryString[2]);
          results = (SelectResults<String>) query.execute();
        } catch (Exception e) {
          fail("Failed executing " + queryString[2], e);
        }
        assertEquals(numberOfEntries, results.size());
        for (String result : results) {
          assertEquals(randomString, result);
        }
      }
    };

    // vm2.invoke(executeQueries);
    vm3.invoke(executeQueries);

    // Check for TestObject instances on Server2.
    // It should be 0
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(0, TestObject.numInstance);
      }
    });

    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }

  /**
   * Tests client-server query on PdxInstance. The client receives projected value.
   */
  public void testVersionedClass() throws CacheException {

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;

    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        try {
          VersionClassLoader.initClassLoader(1);
          String className = "com.gemstone.gemfire.cache.query.data.PdxTestObject";
          for (int i=0; i<numberOfEntries; i++) {
            Object obj =VersionClassLoader.getVersionedInstance(className, new Object[] { new Integer(i), "vmware"});
            region.put("key-"+i, obj);
            //region.put("key-"+i, new TestObject(i, "vmware"));
          } 
        } catch (Exception ex) {
          fail("Failed to load the class.", ex);
        }
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        if (CachedDeserializableFactory.preferObject()) {
          getCache(new CacheFactory().setPdxReadSerialized(true));
        }
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        System.out.println("##### Region size is: " + region.size());
        assertEquals(0, TestObject.numInstance);
        
      }
    });

    // Create client region
    final int port0 = vm0.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");
    final int port1 = vm1.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");

    final String host0 = getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueriesWithParamsPool"; 
    createPool(vm2, poolName, new String[]{host0}, new int[]{port0}, true);
    createPool(vm3, poolName, new String[]{host0}, new int[]{port1}, true);

    // Execute client queries
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        SelectResults results = null;
        Comparator comparator = null;
        Object[] resultsArray = null;
        QueryService qService = null;
        try {
          qService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }    
        try {
          getLogWriter().info("### Executing Query :" + queryString[0]);
          Query query = qService.newQuery(queryString[0]);
          results = (SelectResults)query.execute();
        } catch (Exception e) {
          fail("Failed executing " + queryString[0], e);
        }
        
        assertEquals(numberOfEntries, results.size());
      }
    };

    //vm2.invoke(executeQueries);
    vm3.invoke(executeQueries);
    
    // Check for TestObject instances on Server2.
    // It should be 0
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(0, TestObject.numInstance);
      }
    });
    
    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }
  
  /**
   * Tests client-server query on PdxInstance.
   */
  public void testClientServerQuery() throws CacheException {
    
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;
    
    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new TestObject(i, "vmware"));
        }
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
      }
    });


    // Client pool.
    final int port0 = vm0.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");
    final int port1 = vm1.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");

    final String host0 = getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueryPool"; 
    createPool(vm2, poolName, new String[]{host0}, new int[]{port0}, true);
    createPool(vm3, poolName, new String[]{host0}, new int[]{port1}, true);

    // Create client region
    vm3.invoke(new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {
        QueryService localQueryService = null;
        
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        BridgeTestCase.configureConnectionPool(factory, host0, port1,-1, true, -1, -1, null);
        Region region = createRegion(regionName, rootRegionName,  factory.create());
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new TestObject(i, "vmware"));
        }
        
        //Execute query locally.
        try {
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }          
        for (int i=0; i < 3; i++){
          try {
            Query query = localQueryService.newQuery(queryString[i]);
            SelectResults results = (SelectResults)query.execute();
            assertEquals(numberOfEntries, results.size());
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
        }
      }
    });
    
    // Execute client queries
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        SelectResults results = null;
        Comparator comparator = null;
        Object[] resultsArray = null;
        QueryService remoteQueryService = null;
        QueryService localQueryService = null;
        SelectResults[][] rs = new SelectResults[1][2];
        try {
          remoteQueryService = (PoolManager.find(poolName)).getQueryService();
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }          
        
        for (int i=1; i < 3; i++){
          try {
            getLogWriter().info("### Executing Query on server:" + queryString[i]);
            Query query = remoteQueryService.newQuery(queryString[i]);
            rs[0][0] = (SelectResults)query.execute();
            assertEquals(numberOfEntries, rs[0][0].size());
            
            getLogWriter().info("### Executing Query locally:" + queryString[i]);
            query = localQueryService.newQuery(queryString[i]);
            rs[0][1] = (SelectResults)query.execute();
            assertEquals(numberOfEntries, rs[0][1].size());
            
            // Compare local and remote query results.
            if (!CacheUtils.compareResultsOfWithAndWithoutIndex(rs))
            {
               fail("Local and Remote Query Results are not matching for query :" + queryString[i]);  
            }
            
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
       
        }
        assertEquals(2 * numberOfEntries, TestObject.numInstance);
      }
    };
    
    vm3.invoke(executeQueries);

    // Check for TestObject instances on Server2.
    // It should be 0
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(2 * numberOfEntries, TestObject.numInstance);
        }
        else {
          assertEquals(0, TestObject.numInstance);
        }
      }
    });
    
    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }

  /**
   * Tests client-server query on PdxInstance.
   */
  public void testClientServerQueryWithRangeIndex() throws CacheException {

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;
    final CacheTestCase test = this;
    
    final String[] qs = new String[] { 
        "SELECT * FROM " + regName + " p WHERE p.ID > 0", 
        "SELECT p FROM " + regName + " p WHERE p.ID > 0",
        "SELECT * FROM " + regName + " p WHERE p.ID = 1",
        "SELECT * FROM " + regName + " p WHERE p.ID < 10",
        "SELECT * FROM " + regName + " p WHERE p.ID != 10",
        "SELECT * FROM " + regName + " p, p.positions.values pos WHERE p.ID > 0",
        "SELECT * FROM " + regName + " p, p.positions.values pos WHERE p.ID = 10",
        "SELECT p, pos FROM " + regName + " p, p.positions.values pos WHERE p.ID > 0",
        "SELECT p, pos FROM " + regName + " p, p.positions.values pos WHERE p.ID = 10",
        "SELECT pos FROM " + regName + " p, p.positions.values pos WHERE p.ID > 0", 
        "SELECT p, pos FROM " + regName + " p, p.positions.values pos WHERE pos.secId != 'XXX'", 
        "SELECT pos FROM " + regName + " p, p.positions.values pos WHERE pos.secId != 'XXX'",
        "SELECT pos FROM " + regName + " p, p.positions.values pos WHERE pos.secId = 'SUN'",
        "SELECT p, pos FROM " + regName + " p, p.positions.values pos WHERE pos.secId = 'SUN'",
        "SELECT p, pos FROM " + regName + " p, p.positions.values pos WHERE pos.secId = 'DELL'",
        "SELECT * FROM " + regName + " p, p.positions.values pos WHERE pos.secId = 'SUN'",
        "SELECT * FROM " + regName + " p, p.positions.values pos WHERE pos.secId = 'DELL'",
        "SELECT p, p.position1 FROM " + regName + " p where p.position1.secId != 'XXX'",
        "SELECT p, p.position1 FROM " + regName + " p where p.position1.secId = 'SUN'",
        "SELECT p.position1 FROM " + regName + " p WHERE p.ID > 0",
        "SELECT * FROM " + regName + " p WHERE p.status = 'active'",
        "SELECT p FROM " + regName + " p WHERE p.status != 'active'",
      };
    
    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false, false, true, null); // Async index
        Region region = getRootRegion().getSubregion(regionName); 
        // Create Range index.
        QueryService qs = getCache().getQueryService();
        try {
          qs.createIndex("idIndex", "p.ID", regName + " p");
          qs.createIndex("statusIndex", "p.status", regName + " p");
          qs.createIndex("secIdIndex", "pos.secId", regName  + " p, p.positions.values pos");
          qs.createIndex("pSecIdIdIndex", "p.position1.secId", regName  + " p");
        } catch (Exception ex) {
          fail("Failed to create index." + ex.getMessage());
        }
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false, false, true, null); // Async index
        Region region = getRootRegion().getSubregion(regionName);
      }
    });

    // Client pool.
    final int port0 = vm0.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");
    final int port1 = vm1.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");

    final String host0 = getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueryPool"; 
    createPool(vm2, poolName, new String[]{host0}, new int[]{port0}, true);
    createPool(vm3, poolName, new String[]{host0}, new int[]{port1}, true);

    // Create client region
    vm3.invoke(new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {
        QueryService localQueryService = null;
        
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        BridgeTestCase.configureConnectionPool(factory, host0, port1,-1, true, -1, -1, null);
        Region region = createRegion(regionName, rootRegionName,  factory.create());
        int j = 0;
        for (int i=0; i< 100; i++) {
          region.put("key-"+i, new PortfolioPdx(j, j++));
          // To add duplicate:
          if (i % 24 == 0) {
            j = 0; // reset
          }
        }
      }
    });
    
    
    // Execute query and make sure there is no PdxInstance in the results.
    // bug# 44448
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
       // Execute query locally.
        QueryService queryService = getCache().getQueryService();
        for (int i=0; i < qs.length; i++){
          try {
            Query query = queryService.newQuery(qs[i]);
            SelectResults results = (SelectResults)query.execute();
            for(Object o : results.asList()) {
              if (o instanceof Struct) { 
                Object[] values = ((Struct)o).getFieldValues();  
                for (int c = 0; c < values.length; c++) { 
                  if (values[c] instanceof PdxInstance) {
                    fail("Found unexpected PdxInstance in the query results. At struct field [" + 
                        c  + "] query :" + qs[i] + " Object is: " + values[c]);
                  }
                } 
              } else {
              if (o instanceof PdxInstance) {
                fail("Found unexpected PdxInstance in the query results. " + qs[i]);
              }
              }
            }
          } catch (Exception e) {
            fail("Failed executing " + qs[i], e);
          }
       
        }
      }
    });
    
    // Re-execute query to fetch PdxInstance in the results.
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        // Execute query locally.
        GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
        cache.setReadSerialized(true);
        try {
          QueryService queryService = getCache().getQueryService();
          for (int i=0; i < qs.length; i++){
            try {
              Query query = queryService.newQuery(qs[i]);
              SelectResults results = (SelectResults)query.execute();
              for(Object o : results.asList()) {
                if (o instanceof Struct) { 
                  Object[] values = ((Struct)o).getFieldValues();  
                  for (int c = 0; c < values.length; c++) { 
                    if (!(values[c] instanceof PdxInstance)) {
                      fail("Didn't found expected PdxInstance in the query results. At struct field [" + 
                          c  + "] query :" + qs[i] + " Object is: " + values[c]);
                    }
                  } 
                } else {
                  if (!(o instanceof PdxInstance)) {
                    fail("Didn't found expected PdxInstance in the query results. " + qs[i] + 
                        " Object is: " + o );
                  }
                }
              }
            } catch (Exception e) {
              fail("Failed executing " + qs[i], e);
            }
          }
        } finally {
          cache.setReadSerialized(false);
        }
      }
    });
    
    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }

  /**
   * Tests client-server query on PdxInstance.
   */
  public void testClientServerCountQuery() throws CacheException {

    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;
    final String queryStr = "SELECT COUNT(*) FROM " + regName
        + " WHERE id >= 0";

    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        for (int i = 0; i < numberOfEntries; i++) {
          region.put("key-" + i, new TestObject(i, "vmware"));
        }
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
      }
    });

    // Client pool.
    final int port0 = vm0.invokeInt(PdxQueryDUnitTest.class,
        "getCacheServerPort");
    final int port1 = vm1.invokeInt(PdxQueryDUnitTest.class,
        "getCacheServerPort");

    final String host0 = getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueryPool";
    createPool(vm2, poolName, new String[] { host0 }, new int[] { port0 }, true);
    createPool(vm3, poolName, new String[] { host0 }, new int[] { port1 }, true);

    // Create client region
    vm3.invoke(new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {
        QueryService localQueryService = null;

        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        BridgeTestCase.configureConnectionPool(factory, host0, port1, -1, true,
            -1, -1, null);
        Region region = createRegion(regionName, rootRegionName,
            factory.create());
        for (int i = 0; i < numberOfEntries; i++) {
          region.put("key-" + i, new TestObject(i, "vmware"));
        }

        // Execute query locally.
        try {
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }
        try {
          Query query = localQueryService.newQuery(queryStr);
          SelectResults results = (SelectResults) query.execute();
          assertEquals(numberOfEntries, ((Integer)results.asList().get(0)).intValue());
        } catch (Exception e) {
          fail("Failed executing " + queryStr, e);
        }

      }
    });

    // Execute client queries
    SerializableRunnable executeQueries = new CacheSerializableRunnable(
        "Execute queries") {
      public void run2() throws CacheException {
        SelectResults results = null;
        Comparator comparator = null;
        Object[] resultsArray = null;
        QueryService remoteQueryService = null;
        QueryService localQueryService = null;
        SelectResults[][] rs = new SelectResults[1][2];
        try {
          remoteQueryService = (PoolManager.find(poolName)).getQueryService();
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }

        try {
          getLogWriter().info("### Executing Query on server:" + queryStr);
          Query query = remoteQueryService.newQuery(queryStr);
          rs[0][0] = (SelectResults) query.execute();
          assertEquals(numberOfEntries,
              ((Integer) rs[0][0].asList().get(0)).intValue());
          
          getLogWriter().info("### Executing Query locally:" + queryStr);
          query = localQueryService.newQuery(queryStr);
          rs[0][1] = (SelectResults) query.execute();
          assertEquals(numberOfEntries,
              ((Integer) rs[0][1].asList().get(0)).intValue());

          // Compare local and remote query results.
          if (!CacheUtils.compareResultsOfWithAndWithoutIndex(rs)) {
            fail("Local and Remote Query Results are not matching for query :"
                + queryStr);
          }

        } catch (Exception e) {
          fail("Failed executing " + queryStr, e);
        }

        assertEquals(numberOfEntries, TestObject.numInstance);
      }
    };

    vm3.invoke(executeQueries);

    // Check for TestObject instances on Server2.
    // It should be 0
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(0, TestObject.numInstance);
      }
    });

    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }
  
  /**
   * Tests client-server query on PdxInstance.
   */
  public void testVersionedClientServerQuery() throws CacheException {
    
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;
    
    final String[] queryStr = new String[] {
        "SELECT DISTINCT ID FROM " + regName, // 0
        "SELECT * FROM " + regName, // 1
        "SELECT pkid FROM " + regName, // 2
        "SELECT * FROM " + regName + " WHERE ID > 5", // 3
        "SELECT p FROM " + regName + " p, p.positions pos WHERE p.pkid != 'vmware'", // 4
        "SELECT entry.value FROM " + this.regName + ".entries entry WHERE entry.value.ID > 0",
        "SELECT entry.value FROM  " + this.regName + ".entries entry WHERE entry.key = 'key-1'",
        "SELECT e.value FROM " + this.regName + ".entrySet e where  e.value.pkid >= '0'",
        "SELECT * FROM " + this.regName + ".values p WHERE p.pkid in SET('1', '2','3')",
        "SELECT * FROM " + this.regName + " pf where pf.position1.secId > '2'",
        "SELECT * FROM " + this.regName + " p where p.position3[1].portfolioId = 2",
        "SELECT * FROM " + this.regName + " p, p.positions.values AS pos WHERE pos.secId != '1'",
        "SELECT key, positions FROM " + this.regName + ".entrySet, value.positions.values " +
            "positions WHERE positions.mktValue >= 25.00",
        "SELECT * FROM " + this.regName + " portfolio1, " + this.regName + " portfolio2 WHERE " +
            "portfolio1.status = portfolio2.status",
        "SELECT portfolio1.ID, portfolio2.status FROM " + this.regName + " portfolio1, " +
            this.regName + " portfolio2  WHERE portfolio1.status = portfolio2.status",
         "SELECT * FROM " + this.regName + " portfolio1, portfolio1.positions.values positions1, " +
            this.regName + " portfolio2,  portfolio2.positions.values positions2 WHERE " +
            "positions1.secId = positions1.secId ",
        "SELECT * FROM " + this.regName + " portfolio, portfolio.positions.values positions WHERE " +
            "portfolio.Pk IN SET ('1', '2') AND positions.secId = '1'",
        "SELECT DISTINCT pf1, pf2 FROM " + this.regName + "  pf1, pf1.collectionHolderMap.values coll1," +
            " pf1.positions.values posit1, " + this.regName + "  pf2, pf2.collectionHolderMap.values " +
            " coll2, pf2.positions.values posit2 WHERE pf1.ID = pf2.ID",
      };
    
    
    
    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        if (CachedDeserializableFactory.preferObject()) {
          getCache(new CacheFactory().setPdxReadSerialized(true));
        }
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        if (CachedDeserializableFactory.preferObject()) {
          getCache(new CacheFactory().setPdxReadSerialized(true));
        }
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
      }
    });


    // Client pool.
    final int port0 = vm0.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");
    final int port1 = vm1.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");

    final String host0 = getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueryPool"; 
    createPool(vm2, poolName, new String[]{host0}, new int[]{port0}, true);
    createPool(vm3, poolName, new String[]{host0}, new int[]{port1}, true);

    // Create client region
    vm3.invoke(new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {
        
        // Load client/server region.
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        BridgeTestCase.configureConnectionPool(factory, host0, port1,-1, true, -1, -1, null);
        Region region = createRegion(regionName, rootRegionName,  factory.create());
        
        try {
          VersionClassLoader.initClassLoader(1);
          // Load TestObject
          String pPdxVersion1ClassName = "com.gemstone.gemfire.cache.query.data.PortfolioPdxVersion1";
          Object obj;
          for (int i=0; i<numberOfEntries; i++) {
            obj = VersionClassLoader.getVersionedInstance(pPdxVersion1ClassName, new Object[] { new Integer(i), new Integer(i)});
            region.put("key-" + i, obj);
            //region.put("key-"+i, new TestObject(i, "vmware"));
          } 
        } catch (Exception ex) {
          fail("Failed to load the class.");
        }
          
        // Execute query:
        Object[] resultsArray = null;
        QueryService remoteQueryService = null;
        QueryService localQueryService = null;
        SelectResults[][] rs = new SelectResults[1][2];
        
        try {
          remoteQueryService = (PoolManager.find(poolName)).getQueryService();
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }          

        for (int i=0; i < queryStr.length; i++) {
          try {    
            getLogWriter().info("### Executing Query on server:" + queryStr[i]);
            Query query = remoteQueryService.newQuery(queryStr[i]);
            rs[0][0] = (SelectResults)query.execute();
            //printResults (rs[0][0], " ### Remote Query Results : ####");
            getLogWriter().info("### Executing Query locally:" + queryStr[i]);
            query = localQueryService.newQuery(queryStr[i]);
            rs[0][1] = (SelectResults)query.execute();
            getLogWriter().info("### Remote Query rs size: " + (rs[0][0]).size() + 
                "Local Query rs size: " + (rs[0][1]).size());
            //printResults (rs[0][1], " ### Local Query Results : ####");
            // Compare local and remote query results.
            if (!CacheUtils.compareResultsOfWithAndWithoutIndex(rs))
            {
              fail("Local and Remote Query Results are not matching for query :" + queryStr[i]);  
            }
          } catch (Exception e) {
            fail("Failed executing " + queryStr[i], e);
          }

        }
        
      }
    });
    
    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        String pPdxVersion1ClassName = "com.gemstone.gemfire.cache.query.data.PortfolioPdxVersion1";
        try {
          Object obj = VersionClassLoader.getVersionedInstance(pPdxVersion1ClassName, 
              new Object[] { new Integer(0), new Integer(0)});
          fail("Should have thrown class not found exception.");
        } catch (Exception ex) {
          // Expected.
        }
        }
    });

    
    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }
  
  /**
   * Tests query on with PR.
   */
  public void testQueryOnPR() throws CacheException {
    
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 100;
    
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(true, true);
      }
    });

    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(true, false);       
      }
    });

    vm2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(true, false);
      }
    });

    // Load region.
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new TestObject(i, "vmware"));
        }
      }
    });

    // Client pool.
    final int port0 = vm0.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");
    final int port1 = vm1.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");

    final String host0 = getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueryPool"; 
    createPool(vm3, poolName, new String[]{host0}, new int[]{port1}, true);

    // Execute client queries
    vm3.invoke(new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        SelectResults results = null;
        Comparator comparator = null;
        Object[] resultsArray = null;
        QueryService remoteQueryService = null;
        QueryService localQueryService = null;
        
        try {
          remoteQueryService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }          
        
        for (int i=1; i < 3; i++){
          try {
            getLogWriter().info("### Executing Query on server:" + queryString[i]);
            Query query = remoteQueryService.newQuery(queryString[i]);
            SelectResults rs = (SelectResults)query.execute();
            assertEquals(numberOfEntries, rs.size());            
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
        }
        assertEquals(numberOfEntries, TestObject.numInstance);
      }
    });
    
    // Check for TestObject instances.
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        /*
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries, TestObject.numInstance);
        }
        else {
          assertEquals(numberOfEntries/2, TestObject.numInstance);
        }
        */
        assertEquals(numberOfEntries, TestObject.numInstance);
      }
    });

    // Check for TestObject instances.
    // It should be 0
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        /*
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries, TestObject.numInstance);
        }
        else {
          assertEquals(numberOfEntries/4, TestObject.numInstance);
        }
        */
        assertEquals(0, TestObject.numInstance);
      }
    });

    // Check for TestObject instances.
    // It should be 0
    vm2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        /*
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries, TestObject.numInstance);
        }
        else {
          assertEquals(numberOfEntries/4, TestObject.numInstance);
        }
        */
        assertEquals(0, TestObject.numInstance);
      }
    });

    // Execute Query on Server2.
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        QueryService qs = getCache().getQueryService();
        Query query = null;
        SelectResults sr = null;
        for (int i=0; i < queryString.length; i++) {
          try {
            query = qs.newQuery(queryString[i]);
            sr = (SelectResults)query.execute();
          } catch (Exception ex) {
            fail("Failed to execute query, " + ex.getMessage());
          }
          
          for (Object o: sr.asSet()) {
            if (i == 0 && !(o instanceof Integer)) {
              fail("Expected type Integer, not found in result set. Found type :" + o.getClass());
            } else if (i == 1 && !(o instanceof TestObject)) {
              fail("Expected type TestObject, not found in result set. Found type :" + o.getClass());
            } else if (i == 2 && !(o instanceof String)) {
              fail("Expected type String, not found in result set. Found type :" + o.getClass());
            } 
          }
        }
        if (TestObject.numInstance <= 0) {
          fail("Expected TestObject instance to be >= 0.");
        }
      }
    });
    
    // Check for TestObject instances.
    // It should be 0
    vm2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        /*
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries, TestObject.numInstance);
        }
        else {
          assertEquals(numberOfEntries/4, TestObject.numInstance);
        }
        */
        assertEquals(0, TestObject.numInstance);
      }
    });

    
    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }

  /**
   * Tests query on with PR.
   */
  public void testLocalPRQuery() throws CacheException {
    
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 100;
    
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(true, true);
      }
    });

    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        if (CachedDeserializableFactory.preferObject()) {
          getCache(new CacheFactory().setPdxReadSerialized(true));
        }
        configAndStartBridgeServer(true, false);       
      }
    });

    vm2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        if (CachedDeserializableFactory.preferObject()) {
          getCache(new CacheFactory().setPdxReadSerialized(true));
        }
        configAndStartBridgeServer(true, false);
      }
    });

    vm3.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        if (CachedDeserializableFactory.preferObject()) {
          getCache(new CacheFactory().setPdxReadSerialized(true));
        }
        configAndStartBridgeServer(true, false);
      }
    });

    
    // Load region using class loader and execute query on the same thread.
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        try {
          VersionClassLoader.initClassLoader(1);
          // Load TestObject
          String pPdxVersion1ClassName = "com.gemstone.gemfire.cache.query.data.PortfolioPdxVersion1";
          Object obj;
          for (int i=0; i<numberOfEntries; i++) {
            obj = VersionClassLoader.getVersionedInstance(pPdxVersion1ClassName, new Object[] { new Integer(i), new Integer(i)});
            region.put("key-" + i, obj);
            //region.put("key-"+i, new TestObject(i, "vmware"));
          } 
        } catch (Exception ex) {
          fail("Failed to load the class.");
        }
        
        SelectResults results = null;
        QueryService localQueryService = null;
        
        try {
          localQueryService = region.getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }          
        
        for (int i=1; i < 3; i++){
          try {
            getLogWriter().info("### Executing Query on server:" + queryString[i]);
            Query query = localQueryService.newQuery(queryString[i]);
            SelectResults rs = (SelectResults)query.execute();
            assertEquals(numberOfEntries, rs.size());            
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
        }
      }
    });
    
    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }

  /**
   * Tests query on with PR.
   */
  public void testPdxReadSerializedForPRQuery() throws CacheException {
    
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 100;
    
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(true, true);
      }
    });

    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(true, false);       
      }
    });

    vm2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(true, false);
      }
    });

    vm3.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(true, false);
      }
    });

    
    // Load region using class loader and execute query on the same thread.
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        try {
          VersionClassLoader.initClassLoader(1);
          // Load TestObject
          String pPdxVersion1ClassName = "com.gemstone.gemfire.cache.query.data.PortfolioPdxVersion1";
          Object obj;
          for (int i=0; i<numberOfEntries; i++) {
            obj = VersionClassLoader.getVersionedInstance(pPdxVersion1ClassName, new Object[] { new Integer(i), new Integer(i)});
            region.put("key-" + i, obj);
            //region.put("key-"+i, new TestObject(i, "vmware"));
          } 
        } catch (Exception ex) {
          fail("Failed to load the class.");
        }
        
        SelectResults results = null;
        QueryService localQueryService = null;
        
        try {
          localQueryService = region.getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }          
        
        for (int i=1; i < 3; i++){
          try {
            getLogWriter().info("### Executing Query on server:" + queryString[i]);
            Query query = localQueryService.newQuery(queryString[i]);
            SelectResults rs = (SelectResults)query.execute();
            assertEquals(numberOfEntries, rs.size());            
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
        }
      }
    });
    
    final String[] qs = new String[] {
        "SELECT * FROM " + regName,
        "SELECT * FROM " + regName + " WHERE ID > 5",
        "SELECT p FROM " + regName + " p, p.positions.values pos WHERE p.ID > 2 or pos.secId = 'vmware'",
    };

    // Execute query on node without class and with pdxReadSerialized.
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        GemFireCacheImpl c = (GemFireCacheImpl)region.getCache();
        try {
          // Set read serialized.
          c.setReadSerialized(true);

          SelectResults results = null;
          QueryService localQueryService = null;

          try {
            localQueryService = region.getCache().getQueryService();
          } catch (Exception e) {
            fail("Failed to get QueryService.", e);
          }          

          // This should not throw class not found exception.
          for (int i=1; i < qs.length; i++){
            try {
              getLogWriter().info("### Executing Query on server:" + qs[i]);
              Query query = localQueryService.newQuery(qs[i]);
              SelectResults rs = (SelectResults)query.execute();
              for (Object o: rs.asSet()) {
                if (!(o instanceof PdxInstance)) {
                  fail("Expected type PdxInstance, not found in result set. Found type :" + o.getClass());
                }
              }     
            } catch (Exception e) {
              fail("Failed executing " + qs[i], e);
            }
          }
        } finally {
          c.setReadSerialized(false);
        }
      }
    });
    
    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }

  /**
   * Tests client-server query on PdxInstance.
   */
  public void testCq() throws CacheException {
    
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;
    final int queryLimit = 6;  // where id > 5 (0-5)
    
    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new TestObject(i, "vmware"));
        }
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
      }
    });

    // Client pool.
    final int port0 = vm0.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");
    final int port1 = vm1.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");

    final String host0 = getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testCqPool"; 
    createPool(vm2, poolName, new String[]{host0}, new int[]{port0}, true);
    createPool(vm3, poolName, new String[]{host0}, new int[]{port1}, true);
    final String cqName = "testCq";
    
    // Execute CQ
    SerializableRunnable executeCq = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        getLogWriter().info("### Create CQ. ###" + cqName);
        // Get CQ Service.
        QueryService qService = null;
        try {
          qService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception cqe) {
          fail("Failed to getCQService.", cqe);
        }
        // Create CQ Attributes.
        CqAttributesFactory cqf = new CqAttributesFactory();
        CqListener[] cqListeners = {new CqQueryTestListener(getLogWriter())};
        ((CqQueryTestListener)cqListeners[0]).cqName = cqName;

        cqf.initCqListeners(cqListeners);
        CqAttributes cqa = cqf.create();

        // Create CQ.
        try {
          CqQuery cq = qService.newCq(cqName, queryString[3], cqa);
          SelectResults sr = cq.executeWithInitialResults();
          for (Object o: sr.asSet()) {
            Struct s = (Struct)o;
            Object value = s.get("value");
            if (!(value instanceof TestObject)) {
              fail("Expected type TestObject, not found in result set. Found type :" + o.getClass());
            } 
          }
        } catch (Exception ex){
          AssertionError err = new AssertionError("Failed to create CQ " + cqName + " . ");
          err.initCause(ex);
          getLogWriter().info("QueryService is :" + qService, err);
          throw err;
        }
      }
    };

    vm2.invoke(executeCq);
    vm3.invoke(executeCq);

    // Check for TestObject instances on Server2.
    // It should be 0
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries, TestObject.numInstance);
        }
        else {
          assertEquals(0, TestObject.numInstance);
        }
      }
    });
    
    // update
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        for (int i=0; i<numberOfEntries * 2; i++) {
          region.put("key-"+i, new TestObject(i, "vmware"));
        }
        // Check for TestObject instances.        
        assertEquals(numberOfEntries * 3, TestObject.numInstance);
      }
    });

    // Check for TestObject instances on Server2.
    // It should be 0
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries * 3, TestObject.numInstance);
        }
        else {
          assertEquals(0, TestObject.numInstance);
        }
      }
    });

    SerializableRunnable validateCq = new CacheSerializableRunnable("Validate CQs") {
      public void run2() throws CacheException {
        getLogWriter().info("### Validating CQ. ### " + cqName);
        // Get CQ Service.
        QueryService cqService = null;
        try {          
          cqService = getCache().getQueryService();
        } catch (Exception cqe) {
          fail("Failed to getCQService.", cqe);
        }
        
        CqQuery cQuery = cqService.getCq(cqName);
        if (cQuery == null) {
          fail("Failed to get CqQuery for CQ : " + cqName);
        }
        
        CqAttributes cqAttr = cQuery.getCqAttributes();
        CqListener cqListeners[] = cqAttr.getCqListeners();
        final CqQueryTestListener listener = (CqQueryTestListener) cqListeners[0];
        
        //Wait for the events to show up on the client.
        waitForCriterion(new WaitCriterion() {
          
          public boolean done() {
            return listener.getTotalEventCount() >= (numberOfEntries * 2 - queryLimit);
          }
          
          public String description() {
            return null;
          }
        }, 30000, 100, false);
        
        listener.printInfo(false);
    
        // Check for event type.
        Object[] cqEvents = listener.getEvents();
        for (Object o: cqEvents) {
          CqEvent cqEvent = (CqEvent)o;
          Object value = cqEvent.getNewValue();
          if (!(value instanceof TestObject)) {
            fail("Expected type TestObject, not found in result set. Found type :" + o.getClass());
          } 
        }
        
        // Check for totalEvents count.
        assertEquals("Total Event Count mismatch", (numberOfEntries * 2 - queryLimit), listener.getTotalEventCount());
                
        // Check for create count.
        assertEquals("Create Event mismatch", numberOfEntries, listener.getCreateEventCount());
        
        // Check for update count.
        assertEquals("Update Event mismatch", numberOfEntries  - queryLimit, listener.getUpdateEventCount());      
      }
    };
    
    vm2.invoke(validateCq);
    vm3.invoke(validateCq);
    
    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }

  /**
   * Tests index on PdxInstance.
   */
  public void testIndex() throws CacheException {
    
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;
    
    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        for (int i=0; i<numberOfEntries; i++) {
          if (i % 2 == 0) {
            region.put("key-"+i, new TestObject(i, "vmware"));
          } else {
            region.put("key-"+i, new TestObject(i, "vmware" + i));
          }
        }
        
        try {
          QueryService qs = getCache().getQueryService();
          qs.createIndex("idIndex", IndexType.FUNCTIONAL,"id", regName);
          qs.createIndex("tickerIndex", IndexType.FUNCTIONAL,"p.ticker", regName + " p");
          qs.createIndex("tickerIdTickerMapIndex", IndexType.FUNCTIONAL,"p.ticker", regName + " p, p.idTickers idTickers");
        } catch (Exception ex) {
          fail("Unable to create index. " + ex.getMessage());
        }
        assertEquals(numberOfEntries, TestObject.numInstance);
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);

        try {
          QueryService qs = getCache().getQueryService();
          qs.createIndex("idIndex", IndexType.FUNCTIONAL,"id", regName);
          qs.createIndex("tickerIndex", IndexType.FUNCTIONAL,"p.ticker", regName + " p");
          qs.createIndex("tickerIdTickerMapIndex", IndexType.FUNCTIONAL,"p.ticker", regName + " p, p.idTickers idTickers");
        } catch (Exception ex) {
          fail("Unable to create index. " + ex.getMessage());
        }
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries, TestObject.numInstance);
        }
        else {
          assertEquals(0, TestObject.numInstance);
        }
      }
    });

    // Client pool.
    final int port0 = vm0.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");
    final int port1 = vm1.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");

    final String host0 = getServerHostName(vm0.getHost());
    
//    vm0.invoke(new SerializableRunnable("set log writer") {
//      public void run() {
//        TestObject.log = getLogWriter();
//      }
//      });

    // Create client pool.
    final String poolName = "testClientServerQueryPool"; 
    createPool(vm2, poolName, new String[]{host0}, new int[]{port0}, true);
    createPool(vm3, poolName, new String[]{host0}, new int[]{port1}, true);

    // Create client region
    SerializableRunnable createClientRegions = new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {
        QueryService localQueryService = null;
        
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        BridgeTestCase.configureConnectionPool(factory, host0, port1,-1, true, -1, -1, null);
        Region region = createRegion(regionName, rootRegionName,  factory.create());
        for (int i=0; i<numberOfEntries * 2; i++) {
          if (i % 2 == 0) {
            region.put("key-"+i, new TestObject(i, "vmware"));
          } else {
            region.put("key-"+i, new TestObject(i, "vmware" + i));
          }
        }        
      }
    };
      
    vm2.invoke(createClientRegions);
    vm3.invoke(createClientRegions);
    
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
//        TestObject.log = null;
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries * 5, TestObject.numInstance);
        }
        else {
          assertEquals(numberOfEntries, TestObject.numInstance);
        }
      }
    });
    
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries * 5, TestObject.numInstance);
        }
        else {
          assertEquals(0, TestObject.numInstance);
        }
      }
    });

    
    // Execute client queries
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        SelectResults results = null;
        Comparator comparator = null;
        Object[] resultsArray = null;
        QueryService remoteQueryService = null;
        QueryService localQueryService = null;
        SelectResults[][] rs = new SelectResults[1][2];
        try {
          remoteQueryService = (PoolManager.find(poolName)).getQueryService();
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }          
        
        for (int i=0; i < 3; i++){
          try {
            getLogWriter().info("### Executing Query on server:" + queryString[i]);
            Query query = remoteQueryService.newQuery(queryString[i]);
            rs[0][0] = (SelectResults)query.execute();
            assertEquals(numberOfEntries * 2, rs[0][0].size());
            
            getLogWriter().info("### Executing Query locally:" + queryString[i]);
            query = localQueryService.newQuery(queryString[i]);
            rs[0][1] = (SelectResults)query.execute();
            assertEquals(numberOfEntries * 2, rs[0][1].size());
            
            // Compare local and remote query results.
            if (!CacheUtils.compareResultsOfWithAndWithoutIndex(rs))
            {
               fail("Local and Remote Query Results are not matching for query :" + queryString[i]);  
            }
            
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
       
        }
        assertEquals(4 * numberOfEntries, TestObject.numInstance);
        
        for (int i=3; i < queryString.length; i++){
          try {
            getLogWriter().info("### Executing Query on server:" + queryString[i]);
            Query query = remoteQueryService.newQuery(queryString[i]);
            rs[0][0] = (SelectResults)query.execute();
            
            getLogWriter().info("### Executing Query locally:" + queryString[i]);
            query = localQueryService.newQuery(queryString[i]);
            rs[0][1] = (SelectResults)query.execute();
            
            // Compare local and remote query results.
            if (!CacheUtils.compareResultsOfWithAndWithoutIndex(rs))
            {
               fail("Local and Remote Query Results are not matching for query :" + queryString[i]);  
            }
            
          } catch (Exception e) {
            fail("Failed executing " + queryString[i], e);
          }
       
        }
        
      }
    };

    vm2.invoke(executeQueries);
    vm3.invoke(executeQueries);

    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries * 5, TestObject.numInstance);
        }
        else {
          assertEquals(numberOfEntries, TestObject.numInstance);
        }
      }
    });
    
    // Check for TestObject instances on Server2.
    // It should be 0
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries * 5, TestObject.numInstance);
        }
        else {
          assertEquals(0, TestObject.numInstance);
        }
      }
    });
    
    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }
  
  /**
   * Tests client-server query with region iterators.
   */
  public void testRegionIterators() throws CacheException {
    
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;
    
    final String[] queries = new String[] {
      "SELECT entry.value FROM " + this.regName + ".entries entry WHERE entry.value.id > 0",
      "SELECT entry.value FROM  " + this.regName + ".entries entry WHERE entry.key = 'key-1'",
      "SELECT e.value FROM " + this.regName + ".entrySet e where  e.value.id >= 0",
      "SELECT * FROM " + this.regName + ".values p WHERE p.ticker = 'vmware'",
    };
    
    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new TestObject(i, "vmware"));
        }
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
      }
    });


    // Client pool.
    final int port0 = vm0.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");
    final int port1 = vm1.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");

    final String host0 = getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueryPool"; 
    createPool(vm2, poolName, new String[]{host0}, new int[]{port0}, true);
    createPool(vm3, poolName, new String[]{host0}, new int[]{port1}, true);

    // Create client region
    vm3.invoke(new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {
        QueryService localQueryService = null;
        
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        BridgeTestCase.configureConnectionPool(factory, host0, port1,-1, true, -1, -1, null);
        Region region = createRegion(regionName, rootRegionName,  factory.create());
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new TestObject(i, "vmware"));
        }        
      }
    });
    
    // Execute client queries
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        SelectResults results = null;
        Comparator comparator = null;
        Object[] resultsArray = null;
        QueryService remoteQueryService = null;
        QueryService localQueryService = null;
        SelectResults[][] rs = new SelectResults[1][2];
        try {
          remoteQueryService = (PoolManager.find(poolName)).getQueryService();
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }          
        
        for (int i=0; i < queries.length; i++){
          try {
            getLogWriter().info("### Executing Query on server:" + queries[i]);
            Query query = remoteQueryService.newQuery(queries[i]);
            rs[0][0] = (SelectResults)query.execute();
            
            getLogWriter().info("### Executing Query locally:" + queries[i]);
            query = localQueryService.newQuery(queries[i]);
            rs[0][1] = (SelectResults)query.execute();
            
            // Compare local and remote query results.
            if (!CacheUtils.compareResultsOfWithAndWithoutIndex(rs))
            {
               fail("Local and Remote Query Results are not matching for query :" + queries[i]);  
            }
            
          } catch (Exception e) {
            fail("Failed executing " + queries[i], e);
          }
       
        }
      }
    };

    //vm2.invoke(executeQueries);
    vm3.invoke(executeQueries);

    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries * 2, TestObject.numInstance);
        }
        else {
          assertEquals(0, TestObject.numInstance);
        }
      }
    });

    
    // Create index
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        QueryService qs = getCache().getQueryService();
        try {
          qs.createIndex("idIndex", IndexType.FUNCTIONAL, "entry.value.id", regName + ".entries entry");
          qs.createIndex("tickerIndex", IndexType.FUNCTIONAL, "p.ticker", regName + ".values p");
        } catch (Exception ex) {
          fail("Unable to create index. " + ex.getMessage());
        }
      }
    });

    vm3.invoke(executeQueries);

    // Check for TestObject instances.
    // It should be 0
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries * 2, TestObject.numInstance);
        }
        else {
          assertEquals(0, TestObject.numInstance);
        }
      }
    });
    
    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }

  /**
   * Tests client-server query with nested and collection of Pdx.
   */
  // Bug 47888
  /*public void testNestedAndCollectionPdx() throws CacheException {
    
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 50;
    
    final String[] queries = new String[] {
      "SELECT * FROM " + this.regName + " pf where pf.position1.secId > '2'",
      "SELECT * FROM " + this.regName + " p where p.position3[1].portfolioId = 2",
      "SELECT * FROM " + this.regName + " p, p.positions.values AS pos WHERE pos.secId != '1'",
      "SELECT key, positions FROM " + this.regName + ".entrySet, value.positions.values " + 
          "positions WHERE positions.mktValue >= 25.00",
      "SELECT * FROM " + this.regName + " portfolio1, " + this.regName2 + " portfolio2 WHERE " + 
          "portfolio1.status = portfolio2.status",
      "SELECT portfolio1.ID, portfolio2.status FROM " + this.regName + " portfolio1, " + 
          this.regName + " portfolio2  WHERE portfolio1.status = portfolio2.status",
       "SELECT * FROM " + this.regName + " portfolio1, portfolio1.positions.values positions1, " + 
          this.regName + " portfolio2,  portfolio2.positions.values positions2 WHERE " + 
          "positions1.secId = positions1.secId ",
      "SELECT * FROM " + this.regName + " portfolio, portfolio.positions.values positions WHERE " + 
          "portfolio.Pk IN SET ('1', '2') AND positions.secId = '1'",
      "SELECT DISTINCT * FROM " + this.regName + "  pf1, pf1.collectionHolderMap.values coll1," +
          " pf1.positions.values posit1, " + this.regName2 + "  pf2, pf2.collectionHolderMap.values " + 
          " coll2, pf2.positions.values posit2 WHERE posit1.secId='IBM' AND posit2.secId='IBM'",
    };
    
    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region1 = getRootRegion().getSubregion(regionName);
        Region region2 = getRootRegion().getSubregion(regionName2);
        
        for (int i=0; i<numberOfEntries; i++) {
          region1.put("key-"+i, new PortfolioPdx(i, i));
          region2.put("key-"+i, new PortfolioPdx(i, i));
        }
        // 6.5 positions per portfolio
        assertEquals(numberOfEntries * 13, PositionPdx.numInstance);
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
        Region region2 = getRootRegion().getSubregion(regionName2);
      }
    });


    // Client pool.
    final int port0 = vm0.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");
    final int port1 = vm1.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");

    final String host0 = getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueryPool"; 
    createPool(vm2, poolName, new String[]{host0}, new int[]{port0}, true);
    createPool(vm3, poolName, new String[]{host0}, new int[]{port1}, true);

    // Create client region
    vm3.invoke(new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {
        QueryService localQueryService = null;
        
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        BridgeTestCase.configureConnectionPool(factory, host0, port1,-1, true, -1, -1, null);
        Region region1 = createRegion(regionName, rootRegionName,  factory.create());
        Region region2 = createRegion(regionName2, rootRegionName,  factory.create());
        
        for (int i=0; i<numberOfEntries; i++) {
          region1.put("key-"+i, new PortfolioPdx(i, i));
          region2.put("key-"+i, new PortfolioPdx(i, i));
        }        
        // 6.5 positions per portfolio
        assertEquals(numberOfEntries * 13, PositionPdx.numInstance);
      }
    });
    
    // Execute client queries
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        SelectResults results = null;
        Comparator comparator = null;
        Object[] resultsArray = null;
        QueryService remoteQueryService = null;
        QueryService localQueryService = null;
        SelectResults[][] rs = new SelectResults[1][2];
        try {
          remoteQueryService = (PoolManager.find(poolName)).getQueryService();
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }          
        
        for (int i=0; i < queries.length; i++){
          try {
            getLogWriter().info("### Executing Query on server:" + queries[i]);
            Query query = remoteQueryService.newQuery(queries[i]);
            rs[0][0] = (SelectResults)query.execute();
            
            getLogWriter().info("### Executing Query locally:" + queries[i]);
            query = localQueryService.newQuery(queries[i]);
            rs[0][1] = (SelectResults)query.execute();
            
            // Compare local and remote query results.
            if (!CacheUtils.compareResultsOfWithAndWithoutIndex(rs))
            {
               fail("Local and Remote Query Results are not matching for query :" + queries[i]);  
            }
            
          } catch (Exception e) {
            fail("Failed executing " + queries[i], e);
          }
       
        }
      }
    };

    //vm2.invoke(executeQueries);
    vm3.invoke(executeQueries);

    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries * 4, PortfolioPdx.numInstance);
          // 6.5 positions per portfolio; but only 5.5 in deserialization
          // since the PortfolioPdx.positions map uses the same key twice
          assertEquals(numberOfEntries * 22, PositionPdx.numInstance);
        }
        else {
          assertEquals(0, PortfolioPdx.numInstance);
          assertEquals(0, PositionPdx.numInstance);
        }
      }
    });

    
    // Create index
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        QueryService qs = getCache().getQueryService();
        try {
          qs.createIndex("pkIndex", IndexType.FUNCTIONAL, "portfolio.Pk", regName + " portfolio");
          qs.createIndex("secIdIndex", IndexType.FUNCTIONAL, "pos.secId", regName + 
              " p, p.positions.values AS pos");
          qs.createIndex("tickerIndex", IndexType.FUNCTIONAL, "pf.position1.secId", regName + " pf");
          qs.createIndex("secIdIndexPf1",IndexType.FUNCTIONAL,"pos11.secId", regName + 
              " pf1, pf1.collectionHolderMap.values coll1, pf1.positions.values pos11");
          qs.createIndex("secIdIndexPf2",IndexType.FUNCTIONAL,"pos22.secId", regName2 + 
              " pf2, pf2.collectionHolderMap.values coll2, pf2.positions.values pos22");
        } catch (Exception ex) {
          fail("Unable to create index. " + ex.getMessage());
        }
      }
    });

    vm3.invoke(executeQueries);

    // index is created on portfolio.Pk field which does not exists in
    // PorfolioPdx object
    // but there is a method getPk(), so for #44436, the objects are
    // deserialized to get the value in vm1
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(numberOfEntries, PortfolioPdx.numInstance);
        assertEquals(325, PositionPdx.numInstance); // 50 PorforlioPdx objects
                                                    // create (50*3)+50+50+50+25
                                                    // = 325 PositionPdx objects
                                                    // when deserialized    
      }
    });
    
    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }*/

  /**
   * Tests client-server query with nested and collection of Pdx.
   */
  // Bug 47888
  /*public void testNestedAndCollectionPdxWithPR() throws CacheException {
    
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 50;
    
    final String[] queries = new String[] {
      "SELECT * FROM " + this.regName + " pf where pf.position1.secId > '2'",
      "SELECT * FROM " + this.regName + " p where p.position3[1].portfolioId = 2",
      "SELECT * FROM " + this.regName + " p, p.positions.values AS pos WHERE pos.secId != '1'",
      "SELECT key, positions FROM " + this.regName + ".entrySet, value.positions.values " + 
          "positions WHERE positions.mktValue >= 25.00",
      "SELECT * FROM " + this.regName + " portfolio1, " + this.regName2 + " portfolio2 WHERE " + 
          "portfolio1.status = portfolio2.status",
      "SELECT portfolio1.ID, portfolio2.status FROM " + this.regName + " portfolio1, " + 
          this.regName + " portfolio2  WHERE portfolio1.status = portfolio2.status",
       "SELECT * FROM " + this.regName + " portfolio1, portfolio1.positions.values positions1, " + 
          this.regName + " portfolio2,  portfolio2.positions.values positions2 WHERE " + 
          "positions1.secId = positions1.secId ",
      "SELECT * FROM " + this.regName + " portfolio, portfolio.positions.values positions WHERE " + 
          "portfolio.Pk IN SET ('1', '2') AND positions.secId = '1'",
      "SELECT DISTINCT * FROM " + this.regName + "  pf1, pf1.collectionHolderMap.values coll1," +
          " pf1.positions.values posit1, " + this.regName2 + "  pf2, pf2.collectionHolderMap.values " + 
          " coll2, pf2.positions.values posit2 WHERE posit1.secId='IBM' AND posit2.secId='IBM'",
    };
    
    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false, true);
        Region region1 = getRootRegion().getSubregion(regionName);
        Region region2 = getRootRegion().getSubregion(regionName2);        
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false, true);
        Region region = getRootRegion().getSubregion(regionName);
        Region region2 = getRootRegion().getSubregion(regionName2);
      }
    });

    // Start server2
    vm2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false, true);
        Region region = getRootRegion().getSubregion(regionName);
        Region region2 = getRootRegion().getSubregion(regionName2);
      }
    });

    // Client pool.
    final int port0 = vm0.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");
    final int port1 = vm1.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");

    final String host0 = getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueryPool"; 
    createPool(vm2, poolName, new String[]{host0}, new int[]{port0}, true);
    createPool(vm3, poolName, new String[]{host0}, new int[]{port1}, true);

    // Create client region
    vm3.invoke(new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {
        QueryService localQueryService = null;
        
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        BridgeTestCase.configureConnectionPool(factory, host0, port1,-1, true, -1, -1, null);
        Region region1 = createRegion(regionName, rootRegionName,  factory.create());
        Region region2 = createRegion(regionName2, rootRegionName,  factory.create());
        
        for (int i=0; i<numberOfEntries; i++) {
          region1.put("key-"+i, new PortfolioPdx(i, i));
          region2.put("key-"+i, new PortfolioPdx(i, i));
        }        
      }
    });
    
    // Execute client queries
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        SelectResults results = null;
        Comparator comparator = null;
        Object[] resultsArray = null;
        QueryService remoteQueryService = null;
        QueryService localQueryService = null;
        SelectResults[][] rs = new SelectResults[1][2];
        try {
          remoteQueryService = (PoolManager.find(poolName)).getQueryService();
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }          
        
        for (int i=0; i < queries.length; i++){
          try {
            getLogWriter().info("### Executing Query on server:" + queries[i]);
            Query query = remoteQueryService.newQuery(queries[i]);
            rs[0][0] = (SelectResults)query.execute();
            
            getLogWriter().info("### Executing Query locally:" + queries[i]);
            query = localQueryService.newQuery(queries[i]);
            rs[0][1] = (SelectResults)query.execute();
            
            // Compare local and remote query results.
            if (!CacheUtils.compareResultsOfWithAndWithoutIndex(rs))
            {
               fail("Local and Remote Query Results are not matching for query :" + queries[i]);  
            }
            
          } catch (Exception e) {
            fail("Failed executing " + queries[i], e);
          }
       
        }
      }
    };

    //vm2.invoke(executeQueries);
    vm3.invoke(executeQueries);

    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries * 2, PortfolioPdx.numInstance);
          // 6.5 positions per portfolio; but only 5.5 in deserialization
          // since the PortfolioPdx.positions map uses the same key twice
          assertEquals(numberOfEntries * 11, PositionPdx.numInstance);
        }
        else {
          assertEquals(0, PortfolioPdx.numInstance);
          assertEquals(0, PositionPdx.numInstance);
        }
      }
    });

    
    // Create index
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        QueryService qs = getCache().getQueryService();
        try {
          qs.createIndex("pkIndex", IndexType.FUNCTIONAL, "portfolio.Pk", regName + " portfolio");
          qs.createIndex("idIndex", IndexType.FUNCTIONAL, "pos.secId", regName + 
              " p, p.positions.values AS pos");
          qs.createIndex("tickerIndex", IndexType.FUNCTIONAL, "pf.position1.secId", regName + " pf");
          qs.createIndex("secIdIndexPf1",IndexType.FUNCTIONAL,"pos11.secId", regName + 
              " pf1, pf1.collectionHolderMap.values coll1, pf1.positions.values pos11");
          qs.createIndex("secIdIndexPf2",IndexType.FUNCTIONAL,"pos22.secId", regName2 + 
              " pf2, pf2.collectionHolderMap.values coll2, pf2.positions.values pos22");
        } catch (Exception ex) {
          fail("Unable to create index. " + ex.getMessage());
        }
      }
    });

    vm3.invoke(executeQueries);

    // Check for TestObject instances.
    // It should be 0
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries * 2, PortfolioPdx.numInstance);
          // 6.5 positions per portfolio; but only 5.5 in deserialization
          // since the PortfolioPdx.positions map uses the same key twice
          assertEquals(numberOfEntries * 11, PositionPdx.numInstance);
        }
        else {
          assertEquals(0, PortfolioPdx.numInstance);
          assertEquals(0, PositionPdx.numInstance);
        }
      }
    });
    
    // index is created on portfolio.Pk field which does not exists in
    // PorfolioPdx object
    // but there is a method getPk(), so for #44436, the objects are
    // deserialized to get the value in vm1
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(numberOfEntries, PortfolioPdx.numInstance);
        // 50 PorforlioPdx objects create (50*3)+50+50+50+25 = 325 PositionPdx
        // objects when deserialized
        assertEquals(325, PositionPdx.numInstance);     
      }
    });
    
    vm2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        assertEquals(0, PortfolioPdx.numInstance);
        assertEquals(0, PositionPdx.numInstance);
      }
    });
    
    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }*/
  
  
  /**
   * Tests client-server query on PdxInstance.
   */
  // Bug 47888
  /*public void testCqAndInterestRegistrations() throws CacheException {
    
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;
    final int queryLimit = 6;  // where id > 5 (0-5)
    
    final String[] queries = new String[] {
        "SELECT * FROM " + regName + " p WHERE p.ticker = 'vmware'",
        "SELECT * FROM " + regName + " WHERE id > 5", 
      };
    
    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false, true);
        Region region = getRootRegion().getSubregion(regionName);
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false, true);
        Region region = getRootRegion().getSubregion(regionName);
      }
    });

    // Client pool.
    final int port0 = vm0.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");
    final int port1 = vm1.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");

    final String host0 = getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testCqPool"; 
    
    createPool(vm2, poolName, new String[]{host0, host0}, new int[]{port0, port1}, true);
    createPool(vm3, poolName, new String[]{host0, host0}, new int[]{port1, port0}, true);
    
    final String cqName = "testCq";

    vm3.invoke(new CacheSerializableRunnable("init region") {
      public void run2() throws CacheException {
        QueryService localQueryService = null;

        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        BridgeTestCase.configureConnectionPool(factory, host0, port1,-1, true, -1, -1, null);
        Region region = createRegion(regionName, rootRegionName,  factory.create());

        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new TestObject(i, "vmware"));
        }   
      }
    });

    vm2.invoke(new CacheSerializableRunnable("init region") {
      public void run2() throws CacheException {
        QueryService localQueryService = null;

        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        BridgeTestCase.configureConnectionPool(factory, host0, port0,-1, true, -1, -1, null);
        Region region = createRegion(regionName, rootRegionName,  factory.create());
      }
    });
    
    SerializableRunnable subscribe = new CacheSerializableRunnable("subscribe") {
      public void run2() throws CacheException {
        
        // Register interest
        Region region = getRootRegion().getSubregion(regionName);
        List list = new ArrayList();
        for (int i = 1; i <= numberOfEntries * 3; i++) {
          if (i % 4 == 0) {
            list.add("key-"+i);
          }
        }
        region.registerInterest(list);
        
        getLogWriter().info("### Create CQ. ###" + cqName);
        // Get CQ Service.
        QueryService qService = null;
        try {
          qService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception cqe) {
          fail("Failed to getCQService.", cqe);
        }
        // Create CQ Attributes.
        for (int i=0; i < queries.length; i++) {
          CqAttributesFactory cqf = new CqAttributesFactory();
          CqListener[] cqListeners = {new CqQueryTestListener(getLogWriter())};
          ((CqQueryTestListener)cqListeners[0]).cqName = (cqName + i);

          cqf.initCqListeners(cqListeners);
          CqAttributes cqa = cqf.create();

          // Create CQ.
          try {
            CqQuery cq = qService.newCq(cqName + i, queries[i], cqa);
            SelectResults sr = cq.executeWithInitialResults();
            for (Object o: sr.asSet()) {
              Struct s = (Struct)o;
              Object value = s.get("value");
              if (!(value instanceof TestObject)) {
                fail("Expected type TestObject, not found in result set. Found type :" + o.getClass());
              } 
            }
          } catch (Exception ex){
            AssertionError err = new AssertionError("Failed to create CQ " + cqName + " . ");
            err.initCause(ex);
            getLogWriter().info("QueryService is :" + qService, err);
            throw err;
          }
        }
      }
    };

    vm2.invoke(subscribe);
    vm3.invoke(subscribe);

    // Check for TestObject instances on Server2.
    // It should be 0
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries, TestObject.numInstance);
        }
        else {
          assertEquals(0, TestObject.numInstance);
        }
      }
    });
    
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        // Check for TestObject instances.
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries, TestObject.numInstance);
        }
        else {
          assertEquals(0, TestObject.numInstance);
        }
      }
    });

    vm3.invoke(new CacheSerializableRunnable("Update") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        
        for (int i=0; i<numberOfEntries * 2; i++) {
          region.put("key-"+i, new TestObject(i, "vmware"));
        }        
      }
    });
    
    // Validate CQs.
    for (int i=0; i < queries.length; i++) {
      int expectedEvent = 0;
      int updateEvents = 0;

      if (i != 0) {
        expectedEvent = numberOfEntries * 2 - queryLimit;
        updateEvents = numberOfEntries - queryLimit;
      } else {
        expectedEvent = numberOfEntries * 2;
        updateEvents = numberOfEntries;
      }

      validateCq (vm2, cqName + i, expectedEvent, numberOfEntries, updateEvents);
      validateCq (vm3, cqName + i, expectedEvent, numberOfEntries, updateEvents);
    }
    

    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        // Check for TestObject instances.        
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries * 3, TestObject.numInstance);
        }
        else {
          assertEquals(0, TestObject.numInstance);
        }
      }
    });
    
    // Check for TestObject instances on Server2.
    // It should be 0
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries * 3, TestObject.numInstance);
        }
        else {
          assertEquals(0, TestObject.numInstance);
        }
      }
    });
    
    
    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }*/
  
  /**
   * Tests client-server query on PdxInstance.
   */
  //  Bug 47888
  /*public void testCqAndInterestRegistrationsWithFailOver() throws CacheException {
    
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;
    final int queryLimit = 6;  // where id > 5 (0-5)
    
    final String[] queries = new String[] {
        "SELECT * FROM " + regName + " p WHERE p.ticker = 'vmware'",
        "SELECT * FROM " + regName + " WHERE id > 5", 
      };
    
    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false, true);
        Region region = getRootRegion().getSubregion(regionName);
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false, true);
        Region region = getRootRegion().getSubregion(regionName);
      }
    });

    // Start server3
    vm2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer(false, true);
        Region region = getRootRegion().getSubregion(regionName);
      }
    });
    
    // Client pool.
    final int port0 = vm0.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");
    final int port1 = vm1.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");
    final int port2 = vm2.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");
    
    final String host0 = getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testCqPool";     
    createPool(vm3, poolName, new String[]{host0, host0, host0}, new int[]{port1, port0, port2}, true, 1);
    
    final String cqName = "testCq";

    vm3.invoke(new CacheSerializableRunnable("init region") {
      public void run2() throws CacheException {
        QueryService localQueryService = null;

        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        BridgeTestCase.configureConnectionPool(factory, host0, port1,-1, true, -1, -1, null);
        Region region = createRegion(regionName, rootRegionName,  factory.create());

        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new TestObject(i, "vmware"));
        }   
      }
    });

    SerializableRunnable subscribe = new CacheSerializableRunnable("subscribe") {
      public void run2() throws CacheException {
        
        // Register interest
        Region region = getRootRegion().getSubregion(regionName);
        List list = new ArrayList();
        for (int i = 1; i <= numberOfEntries * 3; i++) {
          if (i % 4 == 0) {
            list.add("key-"+i);
          }
        }
        region.registerInterest(list);
        
        getLogWriter().info("### Create CQ. ###" + cqName);
        // Get CQ Service.
        QueryService qService = null;
        try {
          qService = (PoolManager.find(poolName)).getQueryService();
        } catch (Exception cqe) {
          fail("Failed to getCQService.", cqe);
        }
        // Create CQ Attributes.
        for (int i=0; i < queries.length; i++) {
          CqAttributesFactory cqf = new CqAttributesFactory();
          CqListener[] cqListeners = {new CqQueryTestListener(getLogWriter())};
          ((CqQueryTestListener)cqListeners[0]).cqName = (cqName + i);

          cqf.initCqListeners(cqListeners);
          CqAttributes cqa = cqf.create();

          // Create CQ.
          try {
            CqQuery cq = qService.newCq(cqName + i, queries[i], cqa);
            SelectResults sr = cq.executeWithInitialResults();
            for (Object o: sr.asSet()) {
              Struct s = (Struct)o;
              Object value = s.get("value");
              if (!(value instanceof TestObject)) {
                fail("Expected type TestObject, not found in result set. Found type :" + o.getClass());
              } 
            }
          } catch (Exception ex){
            AssertionError err = new AssertionError("Failed to create CQ " + cqName + " . ");
            err.initCause(ex);
            getLogWriter().info("QueryService is :" + qService, err);
            throw err;
          }
        }
      }
    };

    vm3.invoke(subscribe);

    // Check for TestObject instances on Server2.
    // It should be 0
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries, TestObject.numInstance);
        }
        else {
          assertEquals(0, TestObject.numInstance);
        }
      }
    });
    
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        // Check for TestObject instances.        
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries, TestObject.numInstance);
        }
        else {
          assertEquals(0, TestObject.numInstance);
        }
      }
    });

    // update
    vm3.invoke(new CacheSerializableRunnable("Update") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        
        for (int i=0; i<numberOfEntries * 2; i++) {
          region.put("key-"+i, new TestObject(i, "vmware"));
        }        
      }
    });
    
    // Validate CQs.
    for (int i=0; i < queries.length; i++) {
      int expectedEvent = 0;
      int updateEvents = 0;

      if (i != 0) {
        expectedEvent = (numberOfEntries * 2) - queryLimit;
        updateEvents = numberOfEntries - queryLimit;
      } else {
        expectedEvent = numberOfEntries * 2;
        updateEvents = numberOfEntries;
      }

      validateCq (vm3, cqName + i, expectedEvent, numberOfEntries, updateEvents);
    }
    

    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        // Check for TestObject instances.        
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries * 3, TestObject.numInstance);
        }
        else {
          assertEquals(0, TestObject.numInstance);
        }
      }
    });

    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries * 3, TestObject.numInstance);
        }
        else {
          assertEquals(0, TestObject.numInstance);
        }
      }
    });
    

    // Update
    vm3.invokeAsync(new CacheSerializableRunnable("Update") {
      public void run2() throws CacheException {
        Region region = getRootRegion().getSubregion(regionName);
        
        for (int i=0; i<numberOfEntries * 2; i++) {
          region.put("key-"+i, new TestObject(i, "vmware"));
        }        
      }
    });
    
    // Kill server
    this.closeClient(vm0);
    
    // validate cq
    for (int i=0; i < queries.length; i++) {
      int expectedEvent = 0;
      int updateEvents = 0;

      if (i != 0) {
        expectedEvent = (numberOfEntries * 4) - (queryLimit * 2); // Double the previous time
        updateEvents = (numberOfEntries * 3) - (queryLimit * 2); 
      } else {
        expectedEvent = numberOfEntries * 4;
        updateEvents = numberOfEntries * 3;
      }

      validateCq (vm3, cqName + i, expectedEvent, numberOfEntries, updateEvents);
    }
    
    this.closeClient(vm1);
    
    // Check for TestObject instances on Server3.
    // It should be 0
    vm2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries * 5, TestObject.numInstance);
        }
        else {
          assertEquals(0, TestObject.numInstance);
        }
      }
    });
    
    
    this.closeClient(vm2);
    this.closeClient(vm3);
    
  }*/
  public void validateCq (VM vm, final String cqName, final int expectedEvents, final int createEvents, final int updateEvents) {
    vm.invoke(new CacheSerializableRunnable("Validate CQs") {
      public void run2() throws CacheException {
        getLogWriter().info("### Validating CQ. ### " + cqName);
        // Get CQ Service.
        QueryService cqService = null;
          try {          
            cqService = getCache().getQueryService();
          } catch (Exception cqe) {
            fail("Failed to getCQService.", cqe);
          }

          CqQuery cQuery = cqService.getCq(cqName);
          if (cQuery == null) {
            fail("Failed to get CqQuery for CQ : " + cqName);
          }

          CqAttributes cqAttr = cQuery.getCqAttributes();
          CqListener cqListeners[] = cqAttr.getCqListeners();
          CqQueryTestListener listener = (CqQueryTestListener) cqListeners[0];
          listener.printInfo(false);

          // Check for event type.
          Object[] cqEvents = listener.getEvents();
          for (Object o: cqEvents) {
            CqEvent cqEvent = (CqEvent)o;
            Object value = cqEvent.getNewValue();
            if (!(value instanceof TestObject)) {
              fail("Expected type TestObject, not found in result set. Found type :" + o.getClass());
            } 
          }

          // Check for totalEvents count.
          if (listener.getTotalEventCount() != expectedEvents) {
            listener.waitForTotalEvents(expectedEvents);
          }
          
          assertEquals("Total Event Count mismatch", (expectedEvents), listener.getTotalEventCount());

          // Check for create count.
          assertEquals("Create Event mismatch", createEvents, listener.getCreateEventCount());

          // Check for update count.
          assertEquals("Update Event mismatch", updateEvents, listener.getUpdateEventCount());      
        }
    });
  }
  
  /**
   * Tests identity of Pdx.
   */
  // Bug 47888
  /*public void testPdxIdentity() throws CacheException {
    
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;
    final String queryStr = "SELECT DISTINCT * FROM " + this.regName + " pf where pf.ID > 2 and pf.ID < 10";
    // Start server1
    vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
      }
    });

    // Start server2
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        configAndStartBridgeServer();
        Region region = getRootRegion().getSubregion(regionName);
      }
    });


    // Client pool.
    final int port0 = vm0.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");
    final int port1 = vm1.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");

    final String host0 = getServerHostName(vm0.getHost());

    // Create client pool.
    final String poolName = "testClientServerQueryPool"; 
    createPool(vm2, poolName, new String[]{host0}, new int[]{port0}, true);
    createPool(vm3, poolName, new String[]{host0}, new int[]{port1}, true);

    final int dupIndex = 2;
    
    // Create client region
    vm3.invoke(new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {
        QueryService localQueryService = null;
        
        AttributesFactory factory = new AttributesFactory();
        factory.setScope(Scope.LOCAL);
        BridgeTestCase.configureConnectionPool(factory, host0, port1,-1, true, -1, -1, null);
        Region region = createRegion(regionName, rootRegionName,  factory.create());
        int j=0;
        for (int i=0; i<numberOfEntries * 2; i++) {
          // insert duplicate values.
          if (i%dupIndex == 0) {
            j++;
          }
          region.put("key-"+i, new PortfolioPdx(j));
        }
      }
    });
    
    // Execute client queries
    SerializableRunnable executeQueries = new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        SelectResults results = null;
        Comparator comparator = null;
        Object[] resultsArray = null;
        QueryService remoteQueryService = null;
        QueryService localQueryService = null;
        SelectResults[][] rs = new SelectResults[1][2];
        try {
          remoteQueryService = (PoolManager.find(poolName)).getQueryService();
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }          

        int expectedResultSize = 7;
        
        try {
          getLogWriter().info("### Executing Query on server:" + queryStr);
          Query query = remoteQueryService.newQuery(queryStr);
          rs[0][0] = (SelectResults)query.execute();
          assertEquals(expectedResultSize, rs[0][0].size());

          getLogWriter().info("### Executing Query locally:" + queryStr);
          query = localQueryService.newQuery(queryStr);
          rs[0][1] = (SelectResults)query.execute();
          assertEquals(expectedResultSize, rs[0][1].size());
          getLogWriter().info("### Remote Query rs size: " + (rs[0][0]).size() + 
              "Local Query rs size: " + (rs[0][1]).size());
          
          // Compare local and remote query results.
          if (!CacheUtils.compareResultsOfWithAndWithoutIndex(rs))
          {
             fail("Local and Remote Query Results are not matching for query :" + queryStr);  
          }
        } catch (Exception e) {
          fail("Failed executing " + queryStr, e);
        }
      }
    };

    //vm2.invoke(executeQueries);
    vm3.invoke(executeQueries);

    // Check for TestObject instances on Server2.
    // It should be 0
    vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
      public void run2() throws CacheException {
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries * 2, PortfolioPdx.numInstance);
        }
        else {
          assertEquals(0, PortfolioPdx.numInstance);
        }
      }
    });
    
    this.closeClient(vm2);
    this.closeClient(vm3);
    this.closeClient(vm1);
    this.closeClient(vm0);
  }*/

  /**
   * Tests function calls in the query.
   */
  // Bug 47888
  /*public void testFunctionCalls() throws CacheException {
  final Host host = Host.getHost(0);
  VM vm0 = host.getVM(0);
  VM vm1 = host.getVM(1);
  VM vm2 = host.getVM(2);
  VM vm3 = host.getVM(3);
  final int numberOfEntries = 10;
  final String[] queryStr = new String[] {
      "SELECT * FROM " + this.regName + " pf where pf.getIdValue() > 0", // 0
      "SELECT * FROM " + this.regName + " pf where pf.test.getId() > 0", //1
      "SELECT * FROM " + this.regName + " pf, pf.positions.values pos where " + 
      "pos.getSecId() != 'VMWARE'", //2
      "SELECT * FROM " + this.regName + " pf, pf.positions.values pos where " + 
      "pf.getIdValue() > 0 and pos.getSecId() != 'VMWARE'", //3
      "SELECT * FROM " + this.regName + " pf, pf.getPositions('test').values pos where " + 
      "pos.getSecId() != 'VMWARE'", //4
      "SELECT * FROM " + this.regName + " pf, pf.getPositions('test').values pos where " + 
      "pf.id > 0 and pos.getSecId() != 'IBM'", //5
      "SELECT * FROM " + this.regName + " pf, pf.getPositions('test').values pos where " + 
      "pf.getIdValue() > 0 and pos.secId != 'IBM'", //6
  };
  
  final int numPositionsPerTestObject = 2;
  
  final int[] numberOfTestObjectForAllQueries = new int[]{
      numberOfEntries, // Query 0
      0, // Query 1
      0, // Query 2
      numberOfEntries, // Query 3
      numberOfEntries, // Query 4
      numberOfEntries, // Query 5
      numberOfEntries,  // Query 6
  };

  final int[] numberOfTestObject2ForAllQueries = new int[]{
      numberOfEntries, // Query 0
      numberOfEntries, // Query 1
      0, // Query 2
      numberOfEntries, // Query 3
      numberOfEntries, // Query 4
      numberOfEntries, // Query 5
      numberOfEntries,  // Query 6
  };
  
  final int[] numberOfPositionPdxForAllQueries = new int[]{
      (numberOfEntries * numPositionsPerTestObject), // Query 0
      0, // Query 1
      (numberOfEntries * numPositionsPerTestObject), //2 
      (numberOfEntries * numPositionsPerTestObject * 2), //3
      (numberOfEntries * numPositionsPerTestObject), //4
      (numberOfEntries * numPositionsPerTestObject), //5
      (numberOfEntries * numPositionsPerTestObject), //6
  };

  
  // Start server1
  vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
    public void run2() throws CacheException {
      configAndStartBridgeServer();
      Region region = getRootRegion().getSubregion(regionName);
    }
  });

  // Start server2
  vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
    public void run2() throws CacheException {
      configAndStartBridgeServer();
      Region region = getRootRegion().getSubregion(regionName);
    }
  });


  // Client pool.
  final int port0 = vm0.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");
  final int port1 = vm1.invokeInt(PdxQueryDUnitTest.class, "getCacheServerPort");

  final String host0 = getServerHostName(vm0.getHost());

  // Create client pool.
  final String poolName = "testClientServerQueryPool"; 
  createPool(vm2, poolName, new String[]{host0}, new int[]{port0}, true);
  createPool(vm3, poolName, new String[]{host0}, new int[]{port1}, true);

  final int dupIndex = 2;
  
  // Create client region
  vm3.invoke(new CacheSerializableRunnable("Create region") {
    public void run2() throws CacheException {
      QueryService localQueryService = null;
      
      AttributesFactory factory = new AttributesFactory();
      factory.setScope(Scope.LOCAL);
      BridgeTestCase.configureConnectionPool(factory, host0, port1,-1, true, -1, -1, null);
      Region region = createRegion(regionName, rootRegionName,  factory.create());
      int j=0;
      for (int i=0; i < numberOfEntries; i++) {
        // insert duplicate values.
        if (i%dupIndex == 0) {
          j++;
        }
        region.put("key-"+i, new TestObject(j, "vmware", numPositionsPerTestObject));
      }
    }
  });
  

  //for (int i=0; i < queryStr.length; i++) {
  for (int i=0; i < queryStr.length; i++) {
    final int testObjectCnt = numberOfTestObjectForAllQueries[i];
    final int positionObjectCnt = numberOfPositionPdxForAllQueries[i];
    final int testObjCnt = numberOfTestObject2ForAllQueries[i];
    
    executeClientQueries(vm3, poolName, queryStr[i]);
    // Check for TestObject instances on Server2.

    vm1.invoke(new CacheSerializableRunnable("validate") {
      public void run2() throws CacheException {
        if (CachedDeserializableFactory.preferObject()) {
          assertEquals(numberOfEntries, TestObject.numInstance);
          assertEquals(numberOfEntries * numPositionsPerTestObject,
              PositionPdx.numInstance);
          assertEquals(numberOfEntries, TestObject2.numInstance);
        }
        else {
          assertEquals(testObjectCnt, TestObject.numInstance);
          assertEquals(positionObjectCnt, PositionPdx.numInstance);
          assertEquals(testObjCnt, TestObject2.numInstance);

          // Reset the instances
          TestObject.numInstance = 0;
          PositionPdx.numInstance = 0;
          TestObject2.numInstance = 0;
        }
      }
    });
  }

  this.closeClient(vm2);
  this.closeClient(vm3);
  this.closeClient(vm1);
  this.closeClient(vm0);
  }*/
  
  public void executeClientQueries(VM vm, final String poolName, final String queryStr){
    vm.invoke(new CacheSerializableRunnable("Execute queries") {
      public void run2() throws CacheException {
        SelectResults results = null;
        Comparator comparator = null;
        Object[] resultsArray = null;
        QueryService remoteQueryService = null;
        QueryService localQueryService = null;
        SelectResults[][] rs = new SelectResults[1][2];
        
        try {
          remoteQueryService = (PoolManager.find(poolName)).getQueryService();
          localQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }          

        try {
          getLogWriter().info("### Executing Query on server:" + queryStr);
          Query query = remoteQueryService.newQuery(queryStr);
          rs[0][0] = (SelectResults)query.execute();
          //printResults (rs[0][0], " ### Remote Query Results : ####");
          getLogWriter().info("### Executing Query locally:" + queryStr);
          query = localQueryService.newQuery(queryStr);
          rs[0][1] = (SelectResults)query.execute();
          getLogWriter().info("### Remote Query rs size: " + (rs[0][0]).size() + 
              "Local Query rs size: " + (rs[0][1]).size());
          //printResults (rs[0][1], " ### Local Query Results : ####");
          // Compare local and remote query results.
          if (!CacheUtils.compareResultsOfWithAndWithoutIndex(rs))
          {
             fail("Local and Remote Query Results are not matching for query :" + queryStr);  
          }
        } catch (Exception e) {
          fail("Failed executing " + queryStr, e);
        }
      }
    });
  }

  public void printResults(SelectResults results, String message){
    Object r;
    Struct s;
    LogWriterI18n logger = GemFireCacheImpl.getInstance().getLoggerI18n();
    logger.fine(message);
    int row = 0;
    for (Iterator iter = results.iterator(); iter.hasNext(); ) {
      r = iter.next();
      row++;
      if (r instanceof Struct) {
         s = (Struct)r;
         String[] fieldNames = ((Struct)r).getStructType().getFieldNames();
         for (int i=0; i < fieldNames.length; i++){
            logger.fine("### Row " + row  + "\n" + "Field: " +
                fieldNames[i] + " > " + s.get(fieldNames[i]).toString());
         }
      } else {
        logger.fine("#### Row " + row + "\n" + r);
      }
    }
  }

  protected void configAndStartBridgeServer() {
    configAndStartBridgeServer(false, false, false, null); 
  }
  
  protected void configAndStartBridgeServer(boolean isPr, boolean isAccessor) {
    configAndStartBridgeServer(false, false, false, null); 
  }
  
  protected void configAndStartBridgeServer(boolean isPr, boolean isAccessor, boolean asyncIndex, Compressor compressor) {
    AttributesFactory factory = new AttributesFactory();
    if (isPr) {
      PartitionAttributesFactory paf = new PartitionAttributesFactory();
      //factory.setDataPolicy(DataPolicy.PARTITION);
      if (isAccessor){
        paf.setLocalMaxMemory(0);
      }
      PartitionAttributes prAttr = paf.setTotalNumBuckets(20).setRedundantCopies(0).create();
      factory.setPartitionAttributes(prAttr);
    } else {
      factory.setScope(Scope.DISTRIBUTED_ACK);
      factory.setDataPolicy(DataPolicy.REPLICATE);      
    }
    if (asyncIndex) {
      factory.setIndexMaintenanceSynchronous(!asyncIndex);
    }
    if (compressor != null) {
      factory.setCompressor(compressor);
    }
    
    createRegion(this.regionName, this.rootRegionName, factory.create());
    createRegion(this.regionName2, this.rootRegionName, factory.create());
    
    try {
      startBridgeServer(0, false);
    } catch (Exception ex) {
      fail("While starting CacheServer", ex);
    }
  }

  
  protected void executeCompiledQueries(String poolName, Object[][] params) {
    SelectResults results = null;
    Comparator comparator = null;
    Object[] resultsArray = null;
    QueryService qService = null;

    try {
      qService = (PoolManager.find(poolName)).getQueryService();
    } catch (Exception e) {
      fail("Failed to get QueryService.", e);
    }          

    for (int i=0; i < queryString.length; i++){
      try {
        getLogWriter().info("### Executing Query :" + queryString[i]);
        Query query = qService.newQuery(queryString[i]);
        results = (SelectResults)query.execute(params[i]);
      } catch (Exception e) {
        fail("Failed executing " + queryString[i], e);
      }
    }        
  }
  /**
   * Starts a bridge server on the given port, using the given
   * deserializeValues and notifyBySubscription to serve up the
   * given region.
   */
  protected void startBridgeServer(int port, boolean notifyBySubscription)
  throws IOException {

    Cache cache = getCache();
    BridgeServer bridge = cache.addBridgeServer();
    bridge.setPort(port);
    bridge.setNotifyBySubscription(notifyBySubscription);
    bridge.start();
    bridgeServerPort = bridge.getPort();
  }

  /**
   * Stops the bridge server that serves up the given cache.
   */
  protected void stopBridgeServer(Cache cache) {
    BridgeServer bridge =
      (BridgeServer) cache.getBridgeServers().iterator().next();
    bridge.stop();
    assertFalse(bridge.isRunning());
  }

  /* Close Client */
  public void closeClient(VM client) {
    SerializableRunnable closeCache =
      new CacheSerializableRunnable("Close Client") {
      public void run2() throws CacheException {
        getLogWriter().info("### Close Client. ###");
        try {
          closeCache();
          disconnectFromDS();
        } catch (Exception ex) {
          getLogWriter().info("### Failed to get close client. ###");
        }
      }
    };
    
    client.invoke(closeCache);
  }
  
  private static int getCacheServerPort() {
    return bridgeServerPort;
  }
  
  public static class TestObject2 implements PdxSerializable {
    public int _id;
    public static int numInstance = 0;
    
    public TestObject2(){
      numInstance++;
    }
    
    public TestObject2(int id){
      this._id = id;
      numInstance++;
    }
    
    public int getId() {
      return this._id;
    }
    
    public void toData(PdxWriter out) {
      out.writeInt("id", this._id);
    }
    
    public void fromData(PdxReader in) {
      this._id = in.readInt("id");
    }
    
    @Override
    public boolean equals(Object o){
      getLogWriter().info("In TestObject2.equals() this: " + this + " other :" + o);
      GemFireCacheImpl.getInstance().getLoggerI18n().fine("In TestObject2.equals() this: " + this + " other :" + o);
      TestObject2 other = (TestObject2)o;
      if (_id == other._id) {
        return true;
      } else {
        getLogWriter().info("NOT EQUALS");  
        return false;
      }
    }
    
    @Override
    public int hashCode(){
      GemFireCacheImpl.getInstance().getLoggerI18n().fine("In TestObject2.hashCode() : " + this._id);
      return this._id;
    }
  }
  
  public static class TestObject implements PdxSerializable {
    public static LogWriter log;
    protected String _ticker;
    protected int _price;
    public int id;
    public int important;
    public int selection;
    public int select;
    public static int numInstance = 0;
    public Map idTickers = new HashMap();
    public HashMap positions = new HashMap();
    public TestObject2 test;
    
    public TestObject() {
      if (log != null) {
        log.info("TestObject ctor stack trace", new Exception());
      }
      numInstance++;
      //GemFireCacheImpl.getInstance().getLoggerI18n().fine(new Exception("DEBUG"));
    }

    public TestObject(int id, String ticker) {
      if (log != null) {
        log.info("TestObject ctor stack trace", new Exception());
      }
      this.id = id;
      this._ticker = ticker;
      this._price = id;
      this.important = id;
      this.selection =id;
      this.select =id;
      //GemFireCacheImpl.getInstance().getLoggerI18n().fine(new Exception("DEBUG"));
      numInstance++;
      idTickers.put(id + "", ticker);
      this.test = new TestObject2(id);
    }

    public TestObject(int id, String ticker, int numPositions) {
      this(id, ticker);
      for (int i=0; i < numPositions; i++) {
        positions.put(id + i, new PositionPdx(ticker + ":" +  id + ":" + i , (id + 100)));
      }
    }
    
    public int getIdValue() {
      return this.id;
    }

    public String getTicker() {
      return this._ticker;
    }

    public int getPriceValue() {
      return this._price;
    }

    public HashMap getPositions(String id) {
      return this.positions;  
    }

    public String getStatus(){
      return (id % 2 == 0) ? "active" : "inactive";
    }

    public void toData(PdxWriter out)
    {
      //System.out.println("Is serializing in WAN: " + GatewayEventImpl.isSerializingValue());
      out.writeInt("id", this.id);
      out.writeString("ticker", this._ticker);
      out.writeInt("price", this._price);
      out.writeObject("idTickers", this.idTickers);
      out.writeObject("positions", this.positions);
      out.writeObject("test", this.test);
    }

    public void fromData(PdxReader in)
    {
      //System.out.println("Is deserializing in WAN: " + GatewayEventImpl.isDeserializingValue());
      this.id = in.readInt("id");
      this._ticker = in.readString("ticker");
      this._price = in.readInt("price");
      this.idTickers = (Map)in.readObject("idTickers");
      this.positions = (HashMap)in.readObject("positions");
      this.test = (TestObject2)in.readObject("test");
    }

    public String toString() {
      StringBuffer buffer = new StringBuffer();
      buffer
      .append("TestObject [")
      .append("id=")
      .append(this.id)
      .append("; ticker=")
      .append(this._ticker)
      .append("; price=")
      .append(this._price)
      .append("]");
      return buffer.toString();
    }

    @Override
    public boolean equals(Object o){
//      getLogWriter().info("In TestObject.equals() this: " + this + " other :" + o);
//      GemFireCacheImpl.getInstance().getLoggerI18n().fine("In TestObject.equals() this: " + this + " other :" + o);
      TestObject other = (TestObject)o;
      if ((id == other.id) && (_ticker.equals(other._ticker))) {
        return true;
      } else {
//        getLogWriter().info("NOT EQUALS");  
        return false;
      }
    }
    
    @Override
    public int hashCode(){
      GemFireCacheImpl.getInstance().getLoggerI18n().fine("In TestObject.hashCode() : " + this.id);
      return this.id;
    }

  }

  /** The port on which the bridge server was started in this VM */
  private static int mcastPort;

  /**
   * This test creates 3 cache servers with a PR and one client which puts
   * some PDX values in PR and runs a query. This was failing randomely in
   * a POC. 
   * 
   * This test is for Bug #44457.
   * @author shobhit
   *
   */
  // Bug 47888
  /*public void testPutAllWithIndexes() {
    final String name = "testRegion";
    final Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
    final int numberOfEntries = 10000;

    for (int i = 0; i < 20; i++) {
      // Start server
      vm0.invoke(new CacheSerializableRunnable("Create Bridge Server") {
        public void run2() throws CacheException {
          Properties config = new Properties();
          mcastPort = AvailablePort
              .getRandomAvailablePort(AvailablePort.JGROUPS);
          config.setProperty("mcast-port", String.valueOf(mcastPort));
          Cache cache = new CacheFactory(config).create();
          AttributesFactory factory = new AttributesFactory();
          PartitionAttributesFactory prfactory = new PartitionAttributesFactory();
          prfactory.setRedundantCopies(0);
          factory.setPartitionAttributes(prfactory.create());
          cache.createRegionFactory(factory.create()).create(name);
          try {
            startCacheServer(0, false);
          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }
          // Create Index on empty region
          try {
            cache.getQueryService().createIndex("myFuncIndex", "intId",
                "/" + name);
          } catch (Exception e) {
            fail("index creation failed", e);
          }
        }
      });

      final int mcastport = vm0.invokeInt(
          PdxQueryDUnitTest.class, "getMcastPort");

      // Start server
      vm1.invoke(new CacheSerializableRunnable("Create Bridge Server") {
        public void run2() throws CacheException {
          Properties config = new Properties();
          config.setProperty("mcast-port", String.valueOf(mcastport));
          Cache cache = new CacheFactory(config).create();
          AttributesFactory factory = new AttributesFactory();
          PartitionAttributesFactory prfactory = new PartitionAttributesFactory();
          prfactory.setRedundantCopies(0);
          factory.setPartitionAttributes(prfactory.create());
          cache.createRegionFactory(factory.create()).create(name);
          try {
            startCacheServer(0, false);
          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }
        }
      });

      // Start server
      vm2.invoke(new CacheSerializableRunnable("Create Bridge Server") {
        public void run2() throws CacheException {
          Properties config = new Properties();
          config.setProperty("mcast-port", String.valueOf(mcastport));
          Cache cache = new CacheFactory(config).create();
          AttributesFactory factory = new AttributesFactory();
          PartitionAttributesFactory prfactory = new PartitionAttributesFactory();
          prfactory.setRedundantCopies(0);
          factory.setPartitionAttributes(prfactory.create());
          cache.createRegionFactory(factory.create()).create(name);
          try {
            startCacheServer(0, false);
          } catch (Exception ex) {
            fail("While starting CacheServer", ex);
          }
        }
      });

      // Create client region
      final int port = vm0.invokeInt(PdxQueryDUnitTest.class,
          "getCacheServerPort");
      final String host0 = getServerHostName(vm2.getHost());
      vm3.invoke(new CacheSerializableRunnable("Create region") {
        public void run2() throws CacheException {
          Properties config = new Properties();
          config.setProperty("mcast-port", "0");
          ClientCache cache = new ClientCacheFactory(config)
              .addPoolServer(host0, port).setPoolPRSingleHopEnabled(true)
              .setPoolSubscriptionEnabled(true).create();
          AttributesFactory factory = new AttributesFactory();
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(
              name);
        }
      });

      vm3.invoke(new CacheSerializableRunnable("putAll() test") {

        @Override
        public void run2() throws CacheException {
          try {
            ClientCache cache = new ClientCacheFactory().create();
            Region region = cache.getRegion(name);
            QueryService queryService = cache.getQueryService();
            String k;
            for (int x = 0; x < 285; x++) {
              k = Integer.valueOf(x).toString();
              PortfolioPdx v = new PortfolioPdx(x, x);
              region.put(k, v);
            }
            Query q = queryService.newQuery("SELECT DISTINCT * from /" + name
                + " WHERE ID = 2");
            SelectResults qResult = (SelectResults) q.execute();
            for (Object o : qResult.asList()) {
              System.out.println("o = " + o);
            }
          } catch (Exception e) {
            fail("Querying failed: ", e);
          }
        }
      });

      invokeInEveryVM(DistributedTestCase.class, "disconnectFromDS");
    }
  }*/

  /**
   * for #44436
   * In PeerTypeRegistration when a PdxType is updated, a local map of class => PdxTypes is populated.
   * This map is used to search a field for a class in different versions (PdxTypes)
   * This test verifies that the map is being updated by the cachelistener 
   * 
   * @throws CacheException
   */
  // Bug 47888
  /*public void testLocalMapInPeerTypePdxRegistry() throws CacheException {
    
    final Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;
    final String name = "/" + regionName;
    final String[] qs = { "select * from " + name + " where pdxStatus = 'active'",
        "select pdxStatus from " + name + " where id > 4" };
 
    // Start server1
    final int port1 = (Integer) vm0.invoke(new SerializableCallable(
        "Create Server1") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.REPLICATE)
            .create(regionName);
       
        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });

    // Start server2
    final int port2 = (Integer) vm1.invoke(new SerializableCallable(
        "Create Server2") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.REPLICATE)
            .create(regionName);
       
        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });
     
    // client1 loads version 1 objects on server1
    vm2.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(getServerHostName(vm0.getHost()), port1);
        ClientCache cache = getClientCache(cf);
        Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY)
           .create(regionName);
        
        // Load version 1 objects 
        VersionClassLoader.initClassLoader(1);
        String pPdxVersion1ClassName = "parReg.query.PdxVersionedNewPortfolio";
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, VersionClassLoader.getVersionedInstance(pPdxVersion1ClassName,  new Object[] {"version1_obj-"+i, i}));
        }
        
        return null;
      }
    });

    // client 2 loads version 2 objects on server2
    vm3.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(getServerHostName(vm1.getHost()), port2);
        ClientCache cache = getClientCache(cf);
        Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY)
           .create(regionName);
        String pPdxVersion1ClassName = "parReg.query.PdxVersionedNewPortfolio";
        // Load version 2 objects
        VersionClassLoader.initClassLoader(2);
        for (int i=numberOfEntries; i<numberOfEntries * 2; i++) {
          region.put("key-"+i, VersionClassLoader.getVersionedInstance(pPdxVersion1ClassName,  new Object[] {"version2_obj-"+i, i}));
        }
        return null;
      }
    });
    
    // on server 1 verify local map in PeerTypeRegistration has fields
    vm0.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        TypeRegistration registration = GemFireCacheImpl.getForPdx("PDX registry is unavailable because the Cache has been closed.")
            .getPdxRegistry().getTypeRegistration();
        assertTrue(registration instanceof PeerTypeRegistration);
        Map<String, Set<PdxType>> m = ((PeerTypeRegistration)registration).getClassToType();
        assertEquals(1, m.size());
        assertEquals("parReg.query.PdxVersionedNewPortfolio", m.keySet().iterator().next());
        assertEquals(2,  m.values().iterator().next().size());
        for(PdxType p :  m.values().iterator().next()){
          assertEquals("parReg.query.PdxVersionedNewPortfolio", p.getClassName());
        }
        return null;
      }
    });
    
    // on server 2 verify local map in PeerTypeRegistration has fields
    vm1.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        TypeRegistration registration = GemFireCacheImpl.getForPdx("PDX registry is unavailable because the Cache has been closed.")
            .getPdxRegistry().getTypeRegistration();
        assertTrue(registration instanceof PeerTypeRegistration);
        Map<String, Set<PdxType>> m = ((PeerTypeRegistration)registration).getClassToType();
        assertEquals(1, m.size());
        assertEquals("parReg.query.PdxVersionedNewPortfolio", m.keySet().iterator().next());
        assertEquals(2,  m.values().iterator().next().size());
        for(PdxType p :  m.values().iterator().next()){
          assertEquals("parReg.query.PdxVersionedNewPortfolio", p.getClassName());
        }
        return null;
      }
    });

    invokeInEveryVM(DistributedTestCase.class, "disconnectFromDS");
  }*/
  
  /**
   * for #44436
   * Test to query a field that is not present in the Pdx object
   * but has a get method
   * @throws CacheException
   */
  // Bug 47888
  /*public void testPdxInstanceWithMethodButNoField() throws CacheException {
    
    final Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;
    final String name = "/" + regionName;
    final String[] qs = { "select * from " + name + " where status = 'active'",
        "select status from " + name + " where id >= 5" };
 
    // Start server1
    final int port1 = (Integer) vm0.invoke(new SerializableCallable(
        "Create Server1") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.PARTITION)
            .create(regionName);
       
        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });

    // Start server2
    final int port2 = (Integer) vm1.invoke(new SerializableCallable(
        "Create Server2") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.PARTITION)
            .create(regionName);
       
        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });
    
    // Start server3
    final int port3 = (Integer) vm2.invoke(new SerializableCallable(
        "Create Server3") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.PARTITION)
            .create(regionName);

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });
    
    // create client
    vm3.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(getServerHostName(vm0.getHost()), port1);
        cf.addPoolServer(getServerHostName(vm1.getHost()), port2);
        cf.addPoolServer(getServerHostName(vm2.getHost()), port3);
        ClientCache cache = getClientCache(cf);
        Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY)
           .create(regionName);
        
       for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, new TestObject(i, "vmware"));
        }
       return null;
      }
    });
    
    vm3.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        QueryService remoteQueryService = null;
        //Execute query remotely
        try {
          remoteQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }          
       
        for (int i=0; i < qs.length; i++){
          try {
            SelectResults sr = (SelectResults)remoteQueryService.newQuery(qs[i]).execute();
            assertEquals(5, sr.size());
            } catch (Exception e) {
            fail("Failed executing " + qs[i], e);
          }
        }
         return null;
      }
    });
    
    // create index
    vm0.invoke(new SerializableCallable("Query") {
      @Override
      public Object call() throws Exception {
        QueryService qs = null;
        try {
          qs = getCache().getQueryService();
          qs.createIndex("status", "status", name);
        } catch (Exception e) {
         fail("Exception getting query service ", e);
        }
        
        return null;
      }
    });
    
    // create client
    vm3.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
     
        QueryService remoteQueryService = null;
        //Execute query remotely
        try {
          remoteQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }          
       
        for (int i=0; i < qs.length; i++){
          try {
            SelectResults sr = (SelectResults)remoteQueryService.newQuery(qs[i]).execute();
            assertEquals(5, sr.size());
          } catch (Exception e) {
            fail("Failed executing " + qs[i], e);
          }
        }
         return null;
      }
    });
    invokeInEveryVM(DistributedTestCase.class, "disconnectFromDS");
  }*/

  
  /**
   * for #44436
   * Test to query a field that is not present in the Pdx object
   * but is present in some other version of the pdx instance
   * @throws CacheException
   */
  // Bug 47888
  /*public void testPdxInstanceFieldInOtherVersion() throws CacheException {
    
    final Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;
    final String name = "/" + regionName;
    final String[] qs = { "select pdxStatus from " + name + " where pdxStatus = 'active'",
        "select pdxStatus from " + name + " where id > 8 and id < 14" };
 
    // Start server1
    final int port1 = (Integer) vm0.invoke(new SerializableCallable(
        "Create Server1") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.REPLICATE)
            .create(regionName);
       
        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });

    // Start server2
    final int port2 = (Integer) vm1.invoke(new SerializableCallable(
        "Create Server2") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.REPLICATE)
            .create(regionName);
       
        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });
    
    // client1 loads version 1 objects on server1
    vm2.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(getServerHostName(vm0.getHost()), port1);
        ClientCache cache = getClientCache(cf);
        Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY)
           .create(regionName);
        
        // Load version 1 objects 
        VersionClassLoader.initClassLoader(1);
        String pPdxVersion1ClassName = "parReg.query.PdxVersionedNewPortfolio";
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, VersionClassLoader.getVersionedInstance(pPdxVersion1ClassName,  new Object[] {"version1_obj-"+i, i}));
        }
        
        return null;
      }
    });

    // client 2 loads version 2 objects on server2
    vm3.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(getServerHostName(vm1.getHost()), port2);
        ClientCache cache = getClientCache(cf);
        Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY)
           .create(regionName);
        String pPdxVersion1ClassName = "parReg.query.PdxVersionedNewPortfolio";
        // Load version 2 objects
        VersionClassLoader.initClassLoader(2);
        for (int i=numberOfEntries; i<numberOfEntries * 2; i++) {
          region.put("key-"+i, VersionClassLoader.getVersionedInstance(pPdxVersion1ClassName,  new Object[] {"version2_obj-"+i, i}));
        }
        return null;
      }
    });

    // query remotely from client 1 with version 1 in classpath 
    vm2.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        QueryService remoteQueryService = null;
        //Execute query remotely
        try {
          remoteQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }          
       
        for (int i=0; i < qs.length; i++){
          try {
            SelectResults sr = (SelectResults)remoteQueryService.newQuery(qs[i]).execute();
            assertEquals(5, sr.size());
            if(i == 1){
              for(Object o : sr){
                if(o == null){
                } else if (o instanceof String){
                } else{
                  fail("Result should be either null or String and not " + o.getClass());
                }
              }
            }
           } catch (Exception e) {
            fail("Failed executing " + qs[i], e);
          }
        }
         return null;
      }
    });
    
    // query remotely from client 2 with version 2 in classpath 
    vm3.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        QueryService remoteQueryService = null;
        //Execute query remotely
        try {
          remoteQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }          
       
        for (int i=0; i < qs.length; i++){
          try {
            SelectResults sr = (SelectResults)remoteQueryService.newQuery(qs[i]).execute();
            assertEquals(5, sr.size());
            if(i == 1){
              for(Object o : sr){
                if(o == null){
                } else if (o instanceof String){
                } else{
                  fail("Result should be either null or String and not " + o.getClass());
                }
              }
            }
           } catch (Exception e) {
            fail("Failed executing " + qs[i], e);
          }
        }
         return null;
      }
    });
    
    // query locally on server
    vm0.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
        cache.setReadSerialized(true);
        QueryService queryService = null;
        try {
          queryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }          
       
        for (int i=0; i < qs.length; i++){
          try {
            SelectResults sr = (SelectResults)queryService.newQuery(qs[i]).execute();
            assertEquals(5, sr.size());
            if(i == 1){
              for(Object o : sr){
                if(o == null){
                } else if (o instanceof String){
                } else{
                  fail("Result should be either null or String and not " + o.getClass());
                }
              }
            }
           } catch (Exception e) {
            fail("Failed executing " + qs[i], e);
          }
        }
         return null;
      }
    });
    
    // create index
    vm0.invoke(new SerializableCallable("Query") {
      @Override
      public Object call() throws Exception {
        QueryService qs = null;
        try {
          qs = getCache().getQueryService();
          qs.createIndex("status", "status", name);
        } catch (Exception e) {
         fail("Exception getting query service ", e);
        }
        
        return null;
      }
    });
    
 // query from client 1 with version 1 in classpath
    vm2.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
     
        QueryService remoteQueryService = null;
        //Execute query remotely
        try {
          remoteQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }          
       
        for (int i=0; i < qs.length; i++){
          try {
            SelectResults sr = (SelectResults)remoteQueryService.newQuery(qs[i]).execute();
            assertEquals(5, sr.size());
          } catch (Exception e) {
            fail("Failed executing " + qs[i], e);
          }
        }
         return null;
      }
    });
    
    invokeInEveryVM(DistributedTestCase.class, "disconnectFromDS");
  }*/
  
  /**
   * for #44436
   * 2 servers(replicated) and 2 clients.
   * client2 puts version1 and version2 objects on server1
   * client1 had registered interest to server2, hence gets the pdx objects for both versions
   * Test local query on client1
   * Test if client1 fetched pdxtypes from server
   * @throws CacheException
   */
  // Bug 47888
  /*public void testClientForFieldInOtherVersion() throws CacheException {
    
    final Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;
    final String name = "/" + regionName;
    final String[] qs = { "select pdxStatus from " + name + " where pdxStatus = 'active'",
        "select pdxStatus from " + name + " where id > 8 and id < 14" };
 
    // Start server1
    final int port1 = (Integer) vm0.invoke(new SerializableCallable(
        "Create Server1") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.REPLICATE)
            .create(regionName);
       
        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });

    // Start server2
    final int port2 = (Integer) vm1.invoke(new SerializableCallable(
        "Create Server2") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.REPLICATE)
            .create(regionName);
       
        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });
    
    // client 1 registers interest for server2
    vm2.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.setPoolSubscriptionEnabled(true);
        cf.addPoolServer(getServerHostName(vm1.getHost()), port2);
        ClientCache cache = getClientCache(cf);
        Region region = cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
           .create(regionName);
        region.registerInterest("ALL_KEYS");
        return null;
      }
    });

    // client2 loads both version objects on server1
    vm3.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(getServerHostName(vm0.getHost()), port1);
        ClientCache cache = getClientCache(cf);
        Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY)
           .create(regionName);
        
        String pPdxVersion1ClassName = "parReg.query.PdxVersionedNewPortfolio";
        // Load version 1 objects
        VersionClassLoader.initClassLoader(1);
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, VersionClassLoader.getVersionedInstance(pPdxVersion1ClassName,  new Object[] {"version1_obj-"+i, i}));
        }
        
        //Load version 2 objects
        VersionClassLoader.initClassLoader(2);
        for (int i=numberOfEntries; i<numberOfEntries*2; i++) {
          region.put("key-"+i, VersionClassLoader.getVersionedInstance(pPdxVersion1ClassName,  new Object[] {"version1_obj-"+i, i}));
        }
        
        return null;
      }
    });
    
    // query locally on client 1 which has registered interest
    vm2.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
        cache.setReadSerialized(true);
        QueryService localQueryService = null;
        //Execute query remotely
        try {
          localQueryService = ((ClientCache)getCache()).getLocalQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }          
       
        for (int i=0; i < qs.length; i++){
          try {
            SelectResults sr = (SelectResults)localQueryService.newQuery(qs[i]).execute();
            assertEquals(5, sr.size());
            if(i == 1){
              for(Object o : sr){
                if(o == null){
                } else if (o instanceof String){
                } else{
                  fail("Result should be either null or String and not " + o.getClass());
                }
              }
            }
           } catch (Exception e) {
            fail("Failed executing " + qs[i], e);
          }
        }
        //check if the types registered on server are fetched by the client
        TypeRegistration registration = GemFireCacheImpl.getForPdx("PDX registry is unavailable because the Cache has been closed.")
            .getPdxRegistry().getTypeRegistration();
        assertTrue(registration instanceof ClientTypeRegistration);
        Map<Integer, PdxType> m = ((ClientTypeRegistration)registration).types();
        assertEquals(2, m.size());
        for(PdxType type: m.values()){
          assertEquals("parReg.query.PdxVersionedNewPortfolio", type.getClassName());
        }
         return null;
      }
    });
  
    invokeInEveryVM(DistributedTestCase.class, "disconnectFromDS");
  }*/
  
  /**
   * for #44436
   * Test to query a field that is not present in the Pdx object
   * Also the implicit method is absent in the class
   * @throws CacheException
   */
  public void testPdxInstanceNoFieldNoMethod() throws CacheException {
    
    final Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;
    final String name = "/" + regionName;
    final String[] qs = { "select * from " + name + " where pdxStatus = 'active'",
        "select pdxStatus from " + name + " where id > 4" };
 
    // Start server1
    final int port1 = (Integer) vm0.invoke(new SerializableCallable(
        "Create Server1") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.REPLICATE)
            .create(regionName);
       
        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });
    
    // create client and load only version 1 objects with no pdxStatus field
    vm3.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(getServerHostName(vm0.getHost()), port1);
        ClientCache cache = getClientCache(cf);
        Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY)
           .create(regionName);
        
        // Load version 1 objects 
        VersionClassLoader.initClassLoader(1);
        String pPdxVersion1ClassName = "parReg.query.PdxVersionedNewPortfolio";
        for (int i=0; i<numberOfEntries; i++) {
          region.put("key-"+i, VersionClassLoader.getVersionedInstance(pPdxVersion1ClassName,  new Object[] {"version1_obj-"+i, i}));
        }
        return null;
      }
    });
    
    //Version1 class loader
    vm3.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        // Load version 1 classloader 
        VersionClassLoader.initClassLoader(1);
        QueryService remoteQueryService = null;
        //Execute query remotely
        try {
          remoteQueryService = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }          
       
        for (int i=0; i < qs.length; i++){
          try {
            SelectResults sr = (SelectResults)remoteQueryService.newQuery(qs[i]).execute();
            if(i == 1){
              assertEquals(5, sr.size());
              for(Object o : sr){
                if (!(o instanceof Undefined)){
                  fail("Result should be Undefined and not " + o.getClass());
                }
              }
            } else{
              assertEquals(0, sr.size());
            }
           } catch (Exception e) {
            fail("Failed executing " + qs[i], e);
          }
        }
        return null;
      }
    });
    
    invokeInEveryVM(DistributedTestCase.class, "disconnectFromDS");
  }
  
  /**
   * Test query execution when default values of {@link FieldType} are used.
   * This happens when one version of Pdx object does not have a field but other
   * version has.
   * 
   * @throws Exception
   */
  public void testDefaultValuesInPdxFieldTypes() throws Exception{
    final Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    final int numberOfEntries = 10;
    final String name = "/" + regionName;
    final String query = "select stringField, booleanField, charField, shortField, intField, longField, floatField, doubleField from " + name;
 
    // Start server1
    final int port1 = (Integer) vm0.invoke(new SerializableCallable(
        "Create Server1") {
      @Override
      public Object call() throws Exception {
        Region r1 = getCache().createRegionFactory(RegionShortcut.REPLICATE)
            .create(regionName);
        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailablePortForDUnitSite();
        server.setPort(port);
        server.start();
        return port;
      }
    });

    // client loads version1 and version2 objects on server
    vm1.invoke(new SerializableCallable("Create client") {
      @Override
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(getServerHostName(vm0.getHost()), port1);
        ClientCache cache = getClientCache(cf);
        Region region = cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
           .create(regionName);
        
        String pPdxVersion1ClassName = "objects.PdxVersionedFieldType";
        // Load version 1 objects
        VersionClassLoader.initClassLoader(1);
        for (int i=0; i<numberOfEntries; i++) {
          Object v1 = VersionClassLoader.getVersionedInstance(pPdxVersion1ClassName,  new Object[] {i});
          getLogWriter().info("Putting object: " + v1);
          region.put("key-"+i, v1);
        }
        
        //Load version 2 objects
        VersionClassLoader.initClassLoader(2);
        for (int i=numberOfEntries; i<numberOfEntries*2; i++) {
          Object v2 = VersionClassLoader.getVersionedInstance(pPdxVersion1ClassName,  new Object[] {i});
          getLogWriter().info("Putting object: " + v2);
          region.put("key-"+i, v2);
        }
        
        return null;
      }
    });
    
    // query locally on server, create index, verify results with and without index
    vm0.invoke(new SerializableCallable("Create index") {
      @Override
      public Object call() throws Exception {
        GemFireCacheImpl cache = (GemFireCacheImpl)getCache();
        cache.setReadSerialized(true);
        
        QueryService qs = null;
        SelectResults [][]sr = new SelectResults[1][2];
        // Execute query locally
        try {
          qs = getCache().getQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }

        try {
          sr[0][0] = (SelectResults) qs.newQuery(query)
              .execute();
          assertEquals(20, sr[0][0].size());
 
        } catch (Exception e) {
          fail("Failed executing " + qs, e);
        }
        // create index
        try {
          qs.createIndex("stringIndex", "stringField", name);
          qs.createIndex("boolanIndex", "booleanField", name);
          qs.createIndex("shortIndex", "shortField", name);
          qs.createIndex("charIndex", "charField", name);
          qs.createIndex("intIndex", "intField", name);
          qs.createIndex("longIndex", "longField", name);
          qs.createIndex("floatIndex", "floatField", name);
          qs.createIndex("doubleIndex", "doubleField", name);
        }
        catch (Exception e) {
          fail("Exception creating index ", e);
         }

        // query after index creation
        try {
          sr[0][1] = (SelectResults) qs.newQuery(query)
              .execute();
          assertEquals(20, sr[0][1].size());
 
        } catch (Exception e) {
          fail("Failed executing " + qs, e);
        }
        
        CacheUtils.compareResultsOfWithAndWithoutIndex(sr);
        return null;
      }
    });
  
    // Update index
    vm1.invoke(new SerializableCallable("update index") {
      @Override
      public Object call() throws Exception {
       
        Region region = getCache().getRegion(regionName);
        
        String pPdxVersion1ClassName = "objects.PdxVersionedFieldType";
        // Load version 1 objects
        VersionClassLoader.initClassLoader(1);
        for (int i=numberOfEntries; i<numberOfEntries*2; i++) {
          Object v1 = VersionClassLoader.getVersionedInstance(pPdxVersion1ClassName,  new Object[] {i});
          getLogWriter().info("Putting object: " + v1);
          region.put("key-"+i, v1);
        }
        
        //Load version 2 objects
        VersionClassLoader.initClassLoader(2);
        for (int i=0; i<numberOfEntries; i++) {
          Object v2 = VersionClassLoader.getVersionedInstance(pPdxVersion1ClassName,  new Object[] {i});
          getLogWriter().info("Putting object: " + v2);
          region.put("key-"+i, v2);
        }
        return null;
      }
    });
    
    //query remotely from client
    vm1.invoke(new SerializableCallable("query") {
      @Override
      public Object call() throws Exception {
        QueryService remoteQueryService = null;
        QueryService localQueryService = null;
        SelectResults [][]sr = new SelectResults[1][2];
        // Execute query locally
        try {
          remoteQueryService = getCache().getQueryService();
          localQueryService = ((ClientCache)getCache()).getLocalQueryService();
        } catch (Exception e) {
          fail("Failed to get QueryService.", e);
        }
        try {
          sr[0][0] = (SelectResults) remoteQueryService.newQuery(query)
              .execute();
          assertEquals(20, sr[0][0].size());
          sr[0][1] = (SelectResults) localQueryService.newQuery(query)
              .execute();
          assertEquals(20, sr[0][1].size());
        } catch (Exception e) {
          fail("Failed executing query " +  e);
        }
        
        CacheUtils.compareResultsOfWithAndWithoutIndex(sr);
        
        return null;
      }
    });
   
    invokeInEveryVM(DistributedTestCase.class, "disconnectFromDS");
  }
  
  /**
   * Starts a bridge server on the given port, using the given
   * deserializeValues and notifyBySubscription to serve up the
   * given region.
   */
  protected void startCacheServer(int port, boolean notifyBySubscription)
  throws IOException {

    Cache cache = CacheFactory.getAnyInstance();
    BridgeServer bridge = cache.addBridgeServer();
    bridge.setPort(port);
    bridge.setNotifyBySubscription(notifyBySubscription);
    bridge.start();
    bridgeServerPort = bridge.getPort();
  }

  private static int getMcastPort() {
    return mcastPort;
  }
}

