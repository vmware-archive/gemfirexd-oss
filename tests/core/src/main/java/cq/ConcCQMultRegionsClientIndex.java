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
package cq;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionDestroyedException;

import parReg.query.Position;

import util.NameFactory;
import util.TestException;
import util.TestHelper;
import hydra.Log;
import hydra.RegionHelper;
import hydra.TestConfig;

import java.util.Properties;

import mapregion.MapBB;

/**
 * @author eshu
 *
 */
public class ConcCQMultRegionsClientIndex extends ConcCQMultRegionsClient {
  public static ConcCQMultRegionsClientIndex concCQMultRegionsClientIndex;
  protected static ConcCQBB concCqBB = ConcCQBB.getBB();
  
  public synchronized static void HydraTask_initialize() {
    if (concCQMultRegionsClient == null) {
      concCQMultRegionsClientIndex = new ConcCQMultRegionsClientIndex();
      ConcCQTest.testInstance = new ConcCQTest();
      ConcCQAndOpsTest.concCQAndOpsTest = new ConcCQAndOpsTest();
      //ConcCQAndOpsIndexTest.concCQAndOpsIndexTest = new ConcCQAndOpsIndexTest();
      concCQMultRegionsClient = new ConcCQMultRegionsClient(); 
      verifyCount = TestConfig.tab().intAt(CQUtilPrms.edgeThreads);
      concCQMultRegionsClientIndex.initialize();
    }   
  }
  
  public static void HydraTask_performEntryOperationsWithVerification() {
    concCQMultRegionsClientIndex.performEntryOperationsWithVerification();
    
    //  release any BridgeWriter/Loader connections per task invocation
    releaseConnections();
  }
  
  public static void feedData()
  {
    concCQMultRegionsClientIndex.loadData();
    
    //  release any BridgeWriter/Loader connections per task invocation
    releaseConnections();
  }
  
  
  protected static String updateKey = "Object_0";

  /**
   * to put qualifed object so that cq listener event can be varified.
   * @param regionName 
   */
  protected void putQualifiedObject(String regionName) {
    long putQualified = concCqBB.getSharedCounters().incrementAndRead(ConcCQBB.OpsForQualifiedCount);
    if (putQualified == 1) {            //synchronzied to allow only one thread performs qualified event at a time
      Region region = RegionHelper.getRegion(regionName);
      String name = NameFactory.getNextPositiveObjectName();
      Object anObj = getQualifiedObjectToAdd(name);
      try {
        Object oldValue = region.put(updateKey, anObj); 
        Log.getLogWriter().info("put this key " + updateKey + " and value " + anObj.toString());
        
        if (((Position) anObj).getMktValue() >50 ) {
          updateBBForPuts(regionName, updateKey, anObj);
        } //for all updates of new mktValue is greater than 50.0
        else {
          if (oldValue != null && ((Position)oldValue).getMktValue() > 50) {
            updateBBForPutsFewerResults(regionName, updateKey); 
          } //update to old mktValue greater than 50.00
        }
      } catch (RegionDestroyedException e) {
        handleRegionDestroyedException(region, e);
      } catch (Exception e) {
        throw new TestException(TestHelper.getStackTrace(e));
      }   
      concCqBB.getSharedCounters().zero(ConcCQBB.OpsForQualifiedCount); //to allow another operation
    }
  }
  
  /**
   * To update MapBB for NUM_PUT and ConcCQBB for snapshots if the put object qualifies
   * for the query criteria
   * @param regionName
   * @param name
   * @param anObj
   */
  protected void updateBBForPutsFewerResults(String regionName, String name) {
    MapBB.getBB().getSharedCounters().increment(MapBB.NUM_PUT);   
    
    String key = regionName + "_" + name;
    ConcCQBB.getBB().getSharedMap().remove(key);
    
  }
  
  /**
   * to get the qualifed object to put
   * @param name - used to determine secId
   * @return a Position which will be qualied for cq event
   */
  protected Object getQualifiedObjectToAdd(String name) {       
    int counter = (int) NameFactory.getCounterForName(name);
    Properties props = new Properties();
    
    Double qty = new Double(99.0);  
    Double mktValue = new Double(rnd.nextDouble() * maxPrice);
    Integer secId = new Integer(counter);
    
    props.setProperty("qty", qty.toString());
    props.setProperty("secId", secId.toString());
    props.setProperty("mktValue", mktValue.toString());
    
    Position aPosition = new Position();
    aPosition.init(props);
            
    return aPosition;
  }
  
  protected void putObject(String regionName) {
    int random = rnd.nextInt(10);
    if (random == 0) {
      putQualifiedObject(regionName);
      return;
    }
    Region region = RegionHelper.getRegion(regionName);
    String name = NameFactory.getNextPositiveObjectName();
    Object anObj = getObjectToAdd(name);
    try {
      region.put(name, anObj);
      Log.getLogWriter().info("put this key " + name + " and value " + anObj.toString());
   } catch (RegionDestroyedException e) {
      handleRegionDestroyedException(region, e);
   } catch (Exception e) {
      throw new TestException(TestHelper.getStackTrace(e));
   }     
 }


}
