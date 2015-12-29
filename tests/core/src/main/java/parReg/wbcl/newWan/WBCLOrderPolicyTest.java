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
package parReg.wbcl.newWan;

import hydra.AsyncEventQueueHelper;
import hydra.ConfigPrms;
import hydra.Log;
import hydra.RegionHelper;
import hydra.TestConfig;
import orderPolicy.OrderPolicyTest;
import util.RandomValues;
import util.TestHelper;
import wan.CacheClientPrms;
import wan.WANBlackboard;

import com.gemstone.gemfire.cache.Region;

public class WBCLOrderPolicyTest extends OrderPolicyTest {  
  
  /**
   * create DataStore region  
   */
  public static synchronized void createDSRegionTask(){
        ((WBCLOrderPolicyTest)testInstance).createRegion();    
  }
  
  public Region createRegion(){
    String regionConfig = ConfigPrms.getRegionConfig();
    return RegionHelper.createRegion(regionConfig);
  }
  
  protected void createAsyncEventListener(){
    String asyncEventConfig = ConfigPrms.getAsyncEventQueueConfig();
    if(asyncEventConfig != null){
      AsyncEventQueueHelper.createAndStartAsyncEventQueue(asyncEventConfig);        
    }
  }
  
  /**
   * wait for silence 
   */
 public static void waitForSilenceTask(){
   MyAsyncEventListener.waitForSilence(30, 2000);
 }
 
 //============================================================================
 // INITTASKS (overrides)
 //============================================================================

/** 
 *  Cache initialization for peer gateways
 */
 public static synchronized void initPeerForOrderPolicyTest() {
   if (testInstance == null) {
     String cacheConfig = TestConfig.tasktab().stringAt(CacheClientPrms.cacheConfig, TestConfig.tab().stringAt(CacheClientPrms.cacheConfig, null));
     
     testInstance = new WBCLOrderPolicyTest();
     Log.getLogWriter().info("testInstance = " + testInstance);
     ((WBCLOrderPolicyTest)testInstance).createCache(cacheConfig);     
     ((WBCLOrderPolicyTest)testInstance).createGatewaySender();     
     ((WBCLOrderPolicyTest)testInstance).createRegion();
     ((WBCLOrderPolicyTest)testInstance).createAsyncEventListener();
     ((WBCLOrderPolicyTest)testInstance).createGatewayHub();

     // single randomValues for this VM
     randomValues = new RandomValues();
     ((WBCLOrderPolicyTest)testInstance).initTaskTime();
   } 
 }
 
  /**
   * Check that no Listener Exceptions were posted to the BB
   */
  public static void checkForEventErrors() {
    TestHelper.checkForEventError(WANBlackboard.getInstance());
  }  
}