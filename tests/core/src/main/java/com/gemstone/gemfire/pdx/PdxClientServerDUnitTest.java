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
package com.gemstone.gemfire.pdx;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.Properties;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.client.PoolFactory;
import com.gemstone.gemfire.cache.client.PoolManager;
import com.gemstone.gemfire.cache.query.internal.DefaultQuery;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.GatewayHub;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import com.gemstone.gemfire.internal.cache.CachedDeserializableFactory;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.shared.Version;

import dunit.Host;
import dunit.SerializableCallable;
import dunit.VM;

/**
 * @author dsmith
 *
 */
public class PdxClientServerDUnitTest extends CacheTestCase {

  public PdxClientServerDUnitTest(String name) {
    super(name);
  }

  public void testSimplePut() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);


    createServerRegion(vm0);
    int port = createServerAccessor(vm3);
    createClientRegion(vm1, port);
    createClientRegion(vm2, port);
    
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion("testSimplePdx");
        r.put(1, new SimpleClass(57, (byte) 3));
        return null;
      }
    });
    final SerializableCallable checkValue = new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion("testSimplePdx");
        assertEquals(new SimpleClass(57, (byte) 3), r.get(1));
        return null;
      }
    };
    vm2.invoke(checkValue);
    vm0.invoke(checkValue);
    vm1.invoke(checkValue);

  }
  
  /**
   * Test of bug 47338 - what happens to the client type
   * registry if the server is restarted.
   * 
   * This test is disabled until the bug is fixed.
   */
  public void testNonPersistentServerRestart() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    int port = createServerRegion(vm0);
    createClientRegion(vm1, port);
    
    //Define a PDX type with 2 fields that will be cached on the client
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion("testSimplePdx");
        r.put(1, new PdxType2(1,1));
        r.get(1);
        return null;
      }
    });
    
    closeCache(vm0);
    createServerRegion(vm0, port);
    createClientRegion(vm2, port);
    
    //Now defined a PDX type with only 1 field. This should
    //reuse the same type id because the server was restarted.
    vm2.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion("testSimplePdx");
        r.put(3, new PdxType1(3));
        r.get(3);
        return null;
      }
    });
    
    //See what happens when vm1 tries to read the type.
    //If it cached the type id it will have problems.
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion("testSimplePdx");
        try {
          r.get(4);
        } catch(Exception expected) {
          //The client may not have noticed the server go away and come
          //back. Let's trigger the exception so the client will retry.
        }
        PdxType1 results = (PdxType1) r.get(3);
        assertEquals(3, results.int1);
        return null;
      }
    });
  }
  
  /**
   * Test that we through an exception if one of the servers has persistent
   * regions but not a persistent registry.
   */
  public void testServersWithPersistence() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);



    createServerRegionWithPersistence(vm0, false);
    int port = createServerAccessor(vm1);
    createClientRegion(vm2, port);
    createClientRegion(vm3, port);
    
    SerializableCallable createValue = new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion("testSimplePdx");
        try {
          r.put(1, new SimpleClass(57, (byte) 3));
          fail("should have received an exception");
        } catch(PdxInitializationException expected) {
          //do nothing
        }
        return null;
      }
    };
  }

  private void closeCache(VM vm) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        closeCache();
        return null;
      }
    });
  }
  
  public void testPutThreadLocalConnections() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);


    int port = createServerRegion(vm0);
    createClientRegion(vm1, port, true);
    createClientRegion(vm2, port, true);
    
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion("testSimplePdx");
        r.put(1, new SimpleClass(57, (byte) 3));
        r.put(2, new SimpleClass2(57, (byte) 3));
        return null;
      }
    });
    final SerializableCallable checkValue = new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion("testSimplePdx");
        assertEquals(new SimpleClass(57, (byte) 3), r.get(1));
        assertEquals(new SimpleClass2(57, (byte) 3), r.get(2));
        return null;
      }
    };
    vm2.invoke(checkValue);
    
    vm0.invoke(checkValue);
    vm1.invoke(checkValue);

  }
  
  public void testSimplePdxInstancePut() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);


    int port = createServerRegion(vm0);
    createClientRegion(vm1, port);
    createClientRegion(vm2, port);
    
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion("testSimplePdx");
        r.put(1, new SimpleClass(57, (byte) 3));
        return null;
      }
    });
    final SerializableCallable checkValue = new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion("testSimplePdx");
        DefaultQuery.setPdxReadSerialized(true);
        final GemFireCacheImpl gfCache = GemFireCacheImpl.getInstance();
        final boolean expectPdxInstance = gfCache.isClient()
            || !CachedDeserializableFactory.preferObject();
        try {
          Object v = r.get(1);
          if (expectPdxInstance) {
            if (!(v instanceof PdxInstance)) {
              fail("expected v " + v.getClass() + " to be a PdxInstance");
            }
            PdxInstance piv = (PdxInstance)v;
            assertEquals(new SimpleClass(57, (byte)3), piv.getObject());
            Object v2 = r.get(1);
            if (v == v2) {
              fail("expected v and v2 to have a different identity");
            }
            assertEquals(v, v2);
          }
          else {
            assertEquals(new SimpleClass(57, (byte)3), v);
            Object v2 = r.get(1);
            if (v != v2) {
              fail("expected v and v2 to have the same identity");
            }
          }
        } finally {
          DefaultQuery.setPdxReadSerialized(false);
        }
        return null;
      }
    };
    vm2.invoke(checkValue);
    
    vm0.invoke(checkValue);

  }
  /**
   * Test to make sure that types are 
   * sent to all pools, even if they are 
   * in multiple distributed systems.
   * @throws Exception 
   */
  public void testMultipleServerDSes() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);


    final int port1 = createLonerServerRegion(vm0, "region1", "1");
    final int port2 = createLonerServerRegion(vm1, "region2", "2");
    
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty("mcast-port", "0");
        props.setProperty("locators", "");
        getSystem(props);
        Cache cache = getCache();
        PoolFactory pf = PoolManager.createFactory();
        pf.addServer(getServerHostName(vm0.getHost()), port1);
        pf.create("pool1");
        
        pf = PoolManager.createFactory();
        pf.addServer(getServerHostName(vm0.getHost()), port2);
        pf.create("pool2");
        
        AttributesFactory af = new AttributesFactory();
        af.setPoolName("pool1");
        cache.createRegion("region1", af.create());
        
        af = new AttributesFactory();
        af.setPoolName("pool2");
        cache.createRegion("region2", af.create());
        return null;
      }
    };
    vm2.invoke(createRegion);
    vm3.invoke(createRegion);
    createRegion.call();
    
    //Serialize an object and put it in both regions, sending
    //the event to each pool
    vm2.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        HeapDataOutputStream bytes = new HeapDataOutputStream(Version.CURRENT);
        Region r1 = getRootRegion("region1");
        r1.put(1, new SimpleClass(57, (byte) 3));
        Region r2 = getRootRegion("region2");
        r2.put(1, new SimpleClass(57, (byte) 3));
        return null;
      }
    });

    //Make sure we get deserialize the value in a different client
    vm3.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion("region1");
        assertEquals(new SimpleClass(57, (byte) 3), r.get(1));
        return null;
      }
    });
    
    //Make sure we can get the entry in the current member
    Region r = getRootRegion("region2");
    assertEquals(new SimpleClass(57, (byte) 3), r.get(1));
  }
  
  public void testUserSerializesObject() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);


    int port = createServerRegion(vm0);
    createClientRegion(vm1, port);
    createClientRegion(vm2, port);
    
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        HeapDataOutputStream out = new HeapDataOutputStream(Version.CURRENT);
        DataSerializer.writeObject(new SimpleClass(57, (byte) 3), out);
        byte[] bytes = out.toByteArray();
        Region r = getRootRegion("testSimplePdx");
        r.put(1, bytes);
        return null;
      }
    });
    
    SerializableCallable checkValue = new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion("testSimplePdx");
        byte[] bytes = (byte[]) r.get(1);
        DataInputStream bis = new DataInputStream(new ByteArrayInputStream(bytes));
        Object result = DataSerializer.readObject(bis);
        assertEquals(new SimpleClass(57, (byte) 3), result);
        return null;
      }
    };
    vm2.invoke(checkValue);
    
    vm0.invoke(checkValue);
  }
  
  /**
   * Test that we still use the client
   * type registry, even if pool is created late.
   */
  public void testLatePoolCreation() {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);


    final int port = createServerRegion(vm0);
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty("mcast-port", "0");
        props.setProperty("locators", "");
        getSystem(props);
        Cache cache = getCache();
        PoolFactory pf = PoolManager.createFactory();
        pf.addServer(getServerHostName(vm0.getHost()), port);
        pf.create("pool");
        
        AttributesFactory af = new AttributesFactory();
        af.setPoolName("pool");
        cache.createRegion("testSimplePdx", af.create());
        return null;
      }
    };
    vm1.invoke(createRegion);
    vm2.invoke(createRegion);
    
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion("testSimplePdx");
        r.put(1, new SimpleClass(57, (byte) 3));
        return null;
      }
    });
    final SerializableCallable checkValue = new SerializableCallable() {
      public Object call() throws Exception {
        Region r = getRootRegion("testSimplePdx");
        assertEquals(new SimpleClass(57, (byte) 3), r.get(1));
        return null;
      }
    };
    vm2.invoke(checkValue);
    
    vm0.invoke(checkValue);
  }
  
  /**
   * Test that we throw an exception if someone
   * tries to create a pool after we were forced to use a peer
   * type registry.
   */
  public void testExceptionWithPoolAfterTypeRegistryCreation() {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);


    final int port = createServerRegion(vm0);
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty("mcast-port", "0");
        props.setProperty("locators", "");
        getSystem(props);
        Cache cache = getCache();
        HeapDataOutputStream out = new HeapDataOutputStream(Version.CURRENT);
        DataSerializer.writeObject(new SimpleClass(57, (byte) 3), out);
        
        PoolFactory pf = PoolManager.createFactory();
        pf.addServer(getServerHostName(vm0.getHost()), port);
        try {
          pf.create("pool");
          fail("should have received an exception");
        } catch(PdxInitializationException expected) {
          //do nothing
        }
        return null;
      }
    };
    
    vm1.invoke(createRegion);
  }
  
  /**
   * Test that a client can have a gateway, if PDX
   * is not in fact used.
   * @throws Exception
   */
  public void testClientWithGateway() throws Exception {
    {
      Host host = Host.getHost(0);
      final VM vm0 = host.getVM(0);
      VM vm1 = host.getVM(1);


      final int port = createServerRegion(vm0);
      
      vm1.invoke(new SerializableCallable() {
        public Object call() throws Exception {
          disconnectFromDS();
          Properties props = new Properties();
          props.setProperty("mcast-port", "0");
          props.setProperty("locators", "");
          getSystem(props);
          Cache cache = getCache();
          PoolFactory pf = PoolManager.createFactory();
          pf.addServer(getServerHostName(vm0.getHost()), port);
          pf.create("pool");
          int hubPort= AvailablePortHelper.getRandomAvailableTCPPort();
          GatewayHub hub = cache.addGatewayHub("hub", hubPort);
          hub.start();
          
          
          
          try {
            HeapDataOutputStream out = new HeapDataOutputStream(Version.CURRENT);
            DataSerializer.writeObject(new SimpleClass(57, (byte) 3), out);
//            hub.addGateway("gateway");
            fail("should have received an exception");
          } catch(PdxInitializationException expected) {
            //do nothing
          }
          return null;
        }
      });
      
      vm1.invoke(new SerializableCallable() {
        public Object call() throws Exception {
          disconnectFromDS();
          Properties props = new Properties();
          props.setProperty("mcast-port", "0");
          props.setProperty("locators", "");
          getSystem(props);
          Cache cache = getCache();
          PoolFactory pf = PoolManager.createFactory();
          pf.addServer(getServerHostName(vm0.getHost()), port);
          pf.create("pool");
          
          
          HeapDataOutputStream out = new HeapDataOutputStream(Version.CURRENT);
          DataSerializer.writeObject(new SimpleClass(57, (byte) 3), out);
          
          try {
            int hubPort= AvailablePortHelper.getRandomAvailableTCPPort();
            GatewayHub hub = cache.addGatewayHub("hub", hubPort);
//            hub.addGateway("gateway");
            hub.start();
            fail("should have received an exception");
          } catch(PdxInitializationException expected) {
            //do nothing
          }
          return null;
        }
      });
      
    }
  }

  private int createServerRegion(VM vm) {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.REPLICATE);
        createRootRegion("testSimplePdx", af.create());
        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(port);
        server.start();
        return port;
      }
    };

    return (Integer) vm.invoke(createRegion);
  }

  private int createServerRegion(VM vm, final int port) {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.REPLICATE);
        createRootRegion("testSimplePdx", af.create());

        CacheServer server = getCache().addCacheServer();
        server.setPort(port);
        server.start();
        return port;
      }
    };

    return (Integer) vm.invoke(createRegion);
  }
  
  private int createServerRegionWithPersistence(VM vm,
      final boolean persistentPdxRegistry) {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        CacheFactory cf = new CacheFactory();
        if(persistentPdxRegistry) {
          cf.setPdxPersistent(true)
          .setPdxDiskStore("store");
        }
//      
        Cache cache = getCache(cf);
        cache.createDiskStoreFactory()
          .setDiskDirs(getDiskDirs())
          .create("store");
        
        AttributesFactory af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        af.setDiskStoreName("store");
        createRootRegion("testSimplePdx", af.create());

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(port);
        server.start();
        return port;
      }
    };

    return (Integer) vm.invoke(createRegion);
  }
  
  private int createServerAccessor(VM vm) {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.EMPTY);
        createRootRegion("testSimplePdx", af.create());

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(port);
        server.start();
        return port;
      }
    };

    return (Integer) vm.invoke(createRegion);
  }
  
  private int createLonerServerRegion(VM vm, final String regionName, final String dsId) {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty("locators", "");
        props.setProperty(DistributionConfig.DISTRIBUTED_SYSTEM_ID_NAME, dsId);
        getSystem(props);
        AttributesFactory af = new AttributesFactory();
        af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.REPLICATE);
        createRootRegion(regionName, af.create());

        CacheServer server = getCache().addCacheServer();
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        server.setPort(port);
        server.start();
        return port;
      }
    };

    return (Integer) vm.invoke(createRegion);
  }
  
  private void createClientRegion(final VM vm, final int port) {
    createClientRegion(vm, port, false);
  }

  private void createClientRegion(final VM vm, final int port, 
      final boolean threadLocalConnections) {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        ClientCacheFactory cf = new ClientCacheFactory();
        cf.addPoolServer(getServerHostName(vm.getHost()), port);
        cf.setPoolThreadLocalConnections(threadLocalConnections);
        ClientCache cache = getClientCache(cf);
        cache.createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
        .create("testSimplePdx");
        return null;
      }
    };
    vm.invoke(createRegion);
  }
  
  public static class PdxType1 implements PdxSerializable {
    int int1;
    
    public PdxType1() {
      
    }
    
    public PdxType1(int int1) {
      this.int1 = int1;
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeInt("int1", int1);
      
    }

    @Override
    public void fromData(PdxReader reader) {
      int1 = reader.readInt("int1");
      
    }
  }
  
  public static class PdxType2 implements PdxSerializable {
    int int1;
    int int2;
    
    
    public PdxType2() {
      
    }

    public PdxType2(int int1, int int2) {
      super();
      this.int1 = int1;
      this.int2 = int2;
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeInt("int1", int1);
      writer.writeInt("int2", int2);
    }

    @Override
    public void fromData(PdxReader reader) {
      int2 = reader.readInt("int2");
      int2 = reader.readInt("int2");
    }
  }
}
