

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
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.HeapDataOutputStream;
import dunit.Host;
import dunit.SerializableCallable;
import dunit.VM;
import com.gemstone.gemfire.pdx.internal.json.PdxToJSON;
/**
 * @author hiteshk
 *
 */
public class JSONPdxClientServerDUnitTest extends CacheTestCase {

  public JSONPdxClientServerDUnitTest(String name) {
    super(name);
  }

  public void testSimplePut() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);


    createServerRegion(vm0);
    int port = createServerRegion(vm3);
    createClientRegion(vm1, port);
    createClientRegion(vm2, port);
    
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        JSONAllStringTest();
        return null;
      }
    });
   
    vm2.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        JSONAllByteArrayTest();
        return null;
      }
    });    
  }
  
  //this is for unquote fielnames in json string
  public void testSimplePut2() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);


    createServerRegion(vm0);
    int port = createServerRegion(vm3);
    createClientRegion(vm1, port);
    createClientRegion(vm2, port);
    
    vm1.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        JSONUnQuoteFields();
        return null;
      }
    });
     
  }
  
  public void JSONUnQuoteFields() {
    System.setProperty("pdxToJson.unqouteFieldNames", "true");
    PdxToJSON.PDXTOJJSON_UNQUOTEFIELDNAMES = true;
    String JTESTS = System.getProperty("JTESTS");
    String jsonStringsDir = JTESTS + File.separator + "com" + File.separator + "gemstone"  + File.separator + 
                               "gemfire" + File.separator + "pdx"  + File.separator + "jsonStrings" + File.separator + "unquoteJsonStrings";
    JSONAllStringTest(jsonStringsDir);
  }  
  
  public void JSONAllStringTest() {
    String JTESTS = System.getProperty("JTESTS");
    String jsonStringsDir = JTESTS + File.separator + "com" + File.separator + "gemstone"  + File.separator + 
                               "gemfire" + File.separator + "pdx"  + File.separator + "jsonStrings";
    JSONAllStringTest(jsonStringsDir);
  }
  
  public void JSONAllStringTest(String dirname) {
    
      
    JSONData[] allJsons = loadAllJSON(dirname);
    int i = 0;
    for(JSONData jsonData : allJsons)
    {
      if(jsonData != null) {
        i++;
        VerifyJSONString(jsonData);
      }
    }
    Assert.assertTrue(i >= 1, "Number of files should be more than 10 : " + i);
  }
  
  public void JSONAllByteArrayTest() {
    String JTESTS = System.getProperty("JTESTS");
    String jsonStringsDir = JTESTS + File.separator  + "com" + File.separator + "gemstone"  + File.separator + 
                               "gemfire" + File.separator + "pdx"  + File.separator + "jsonStrings";
      
    JSONData[] allJsons = loadAllJSON(jsonStringsDir);
    int i = 0;
    for(JSONData jsonData : allJsons)
    {
      if(jsonData != null) {
        i++;
        VerifyJSONByteArray(jsonData);
      }
    }
    Assert.assertTrue(i > 10, "Number of files should be more than 10");
  }
  
  static class JSONData {
    String jsonFileName;
    byte[] jsonByteArray;
    
    public JSONData(String fn, byte[] js) {
      jsonFileName = fn;
      jsonByteArray = js;
    }
    public String getFileName () {
      return jsonFileName;
    }
    public String getJsonString() {
      return new String(jsonByteArray);
    }
    public byte[] getJsonByteArray(){
      return jsonByteArray;
    }
  }
  
  
  public  void VerifyJSONString(JSONData jd) {
    Region r = getRootRegion("testSimplePdx");
    
    PdxInstance pdx = JSONFormatter.fromJSON(jd.getJsonString());
    
    r.put(1, pdx);
    
    pdx = (PdxInstance)r.get(1);
    
    String getJsonString = JSONFormatter.toJSON(pdx);
    
    String o1 = jsonParse(jd.getJsonString());
    String o2 = jsonParse(getJsonString);
    junit.framework.Assert.assertEquals("Json Strings are not equal " + jd.getFileName() + " " +  Boolean.getBoolean("pdxToJson.unqouteFieldNames"), o1, o2);
  
    PdxInstance pdx2 = JSONFormatter.fromJSON(getJsonString);
    
    junit.framework.Assert.assertEquals("Pdx are not equal; json filename " + jd.getFileName(), pdx, pdx2);    
  }  
  
  protected final static int INT_TAB = '\t';
  protected final static int INT_LF = '\n';
  protected final static int INT_CR = '\r';
  protected final static int INT_SPACE = 0x0020;
  
  public String jsonParse(String jsonSting) {
        
   byte[] ba = jsonSting.getBytes();
   byte [] withoutspace = new byte[ba.length];
  
      int i = 0;
      int j = 0;
      for(i =0; i< ba.length; i++) {
        int cbyte = ba[i];
        
        if(cbyte == INT_TAB || cbyte == INT_LF || cbyte == INT_CR || cbyte == INT_SPACE)
          continue;
        withoutspace[j++] = ba[i];
      }
      
      return new String(withoutspace, 0, j);
    
  }
  public  void VerifyJSONByteArray(JSONData jd) {
    Region r = getRootRegion("testSimplePdx");
    
    PdxInstance pdx = JSONFormatter.fromJSON(jd.getJsonByteArray());
    
    r.put(1, pdx);
    
    pdx = (PdxInstance)r.get(1);
    
    byte[] jsonByteArray = JSONFormatter.toJSONByteArray(pdx);
    
    byte[] o1 = jsonParse(jd.getJsonByteArray());
    byte[] o2 = jsonParse(jsonByteArray);
    
   // junit.framework.Assert.assertEquals("Pdx byte aray are not equal after fetching from cache " + jd.getFileName(), o1, o2); 
   compareByteArray(o1, o2);
    
    PdxInstance pdx2 = JSONFormatter.fromJSON(jsonByteArray);
    boolean pdxequals = pdx.equals(pdx2);
    
    junit.framework.Assert.assertEquals("Pdx are not equal for byte array ; json filename " + jd.getFileName(), pdx, pdx2 );    
  }  
  public void compareByteArray(byte[] b1, byte[] b2) {
    if(b1.length != b2.length) 
      throw new IllegalStateException("Json byte array length are not equal " + b1.length + " ; " + b2.length);
    
    for(int i =0; i< b1.length; i++) {
      if(b1[i] != b2[i])
        throw new IllegalStateException("Json byte arrays are not equal ");
    }
  } 
  public byte[] jsonParse(byte[] jsonBA) {
    
    byte[] ba = jsonBA;
    byte [] withoutspace = new byte[ba.length];
  
    int i = 0;
    int j = 0;
    for(i =0; i< ba.length; i++) {
      int cbyte = ba[i];
      
      if(cbyte == INT_TAB || cbyte == INT_LF || cbyte == INT_CR || cbyte == INT_SPACE)
        continue;
      withoutspace[j++] = ba[i];
    }
      
     byte[] retBA = new byte[j];
     
     for(i =0 ; i< j ; i++) {
       retBA[i] = withoutspace[i];
     }
      
      return retBA;
    
  }
  public static JSONData[] loadAllJSON(String jsondir) {
    File dir = new File(jsondir);
    
    JSONData [] JSONDatas = new JSONData[dir.list().length];
    int i = 0;
    for(String jsonFileName : dir.list()) {
      
      if(!jsonFileName.contains(".txt"))
        continue;
      try {
        byte[] ba = getBytesFromFile(dir.getAbsolutePath() +  File.separator + jsonFileName);
        JSONDatas[i++] = new JSONData(jsonFileName, ba);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }       
    return JSONDatas;
  }
  
  public static byte[] getBytesFromFile(String fileName) throws IOException {
    File file = new File(fileName);
    
    java.io.InputStream is = new FileInputStream(file);

     // Get the size of the file
     long length = file.length();

     // Create the byte array to hold the data
     byte[] bytes = new byte[(int)length];

     // Read in the bytes
     int offset = 0;
     int numRead = 0;
     while (offset < bytes.length
            && (numRead=is.read(bytes, offset, bytes.length-offset)) >= 0) {
         offset += numRead;
     }

     // Ensure all the bytes have been read in
     if (offset < bytes.length) {
         throw new IOException("Could not completely read file "+file.getName());
     }
    
     is.close();
     return bytes;
  }
  
  private void closeCache(VM vm) {
    vm.invoke(new SerializableCallable() {
      public Object call() throws Exception {
        closeCache();
        return null;
      }
    });
  }
  
  
  private int createServerRegion(VM vm) {
    SerializableCallable createRegion = new SerializableCallable() {
      public Object call() throws Exception {
        AttributesFactory af = new AttributesFactory();
        //af.setScope(Scope.DISTRIBUTED_ACK);
        af.setDataPolicy(DataPolicy.PARTITION);
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
        //af.setScope(Scope.DISTRIBUTED_ACK);
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
        cache.createClientRegionFactory(ClientRegionShortcut.PROXY)
        .create("testSimplePdx");
        return null;
      }
    };
    vm.invoke(createRegion);
  }
}


