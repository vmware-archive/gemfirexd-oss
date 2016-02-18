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
import java.util.Properties;
import junit.framework.TestCase;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.internal.jndi.JNDIInvoker;
//import com.gemstone.gemfire.internal.jta.CacheUtils;
import javax.transaction.TransactionManager;

/**
 * @author Nand Kishor
 * 
 * This test check the graceful removal of all the resource
 * (DataSources , TransactionManager and UserTransaction and 
 * its associated thread) before we reconnect to the distributed 
 * syatem.
 */

public class RestartTest extends TestCase {

  private static Properties props = null;
  private static DistributedSystem ds1 = null;
  private static Cache cache = null;
  public RestartTest(String name) {
    super(name);
  }

  public void setup() {

  }

  public void teardown() {
  }

  public void testCleanUp() {
	TransactionManager tm1 = null;
	TransactionManager tm2 = null;
    try{
    props = new Properties();
//    props.setProperty("mcast-port","33405");
    String path = System.getProperty("JTAXMLFILE");
    props.setProperty("cache-xml-file",path);

    ds1 = DistributedSystem.connect(props);
    cache = CacheFactory.create(ds1);
    tm1 = JNDIInvoker.getTransactionManager();
    cache.close();
    ds1.disconnect();

    ds1 = DistributedSystem.connect(props);
    cache = CacheFactory.create(ds1);
    tm2 = JNDIInvoker.getTransactionManager();
    assertNotSame("TransactionManager are same in before restart and after restart",tm1,tm2);
    
    ds1.disconnect();
  }catch(Exception e){
    fail("Failed in restarting the distributed system");
}
  }
}

