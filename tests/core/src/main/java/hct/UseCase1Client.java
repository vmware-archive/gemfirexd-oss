
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

package hct;

import java.util.*;
import java.rmi.*;
import java.io.*;
import hydra.*;
import util.TestException;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.util.*;

/**
 * UseCase1 CBB POC reproduction
 *
 * @author lhughes
 * @since 4.3
 */
public class UseCase1Client {

  public static final String REGION_NAME="/root/day0";

  /** The by test clients (edge VMs).  */
  static Region edgeRegion;

  /**
   * Initializes the test region in the cache server VM according to
   * the "server" params in useCase1Server.xml.
   */
public static void initServerCache() {

   // create subdirectories to create disk dirs in
   String bridgeName = System.getProperty(ClientPrms.CLIENT_NAME_PROPERTY);
   for (int i = 0; i <= 7; i++) {
      File aDir = new File(bridgeName + "_diskDir_day" + i);
      aDir.mkdir();
   }

   // create cache from xml
   String cacheXmlFile = "$JTESTS/hct/" + bridgeName + ".xml";
   CacheHelper.createCacheFromXml(cacheXmlFile);

   displayRegions();
}

  /** display regions w/attributes */
  protected static void displayRegions() {
    Set rootRegions = CacheHelper.getCache().rootRegions();
    for (Iterator rit=rootRegions.iterator(); rit.hasNext();) {
      Region parentRegion = (Region)rit.next();
      Log.getLogWriter().info(RegionHelper.regionAttributesToString(parentRegion.getAttributes()));
      Set subRegions = parentRegion.subregions(true);
      for (Iterator sit=subRegions.iterator(); sit.hasNext();) {
        Region subregion = (Region)sit.next();
        Log.getLogWriter().info(RegionHelper.regionAttributesToString(subregion.getAttributes()));
      }
    }
  }

  /**
   * A Hydra INIT task that initialize the test region in an edge
   * client according to the "edge" params of useCase1Client.xml
   */
public synchronized static void initEdgeCache() throws Exception {

  // create cache using configuration "cache"
  Cache cache = CacheHelper.createCache("cache");

  // create root region using configuration "edgeRoot"
  Region rootRegion = RegionHelper.createRegion("root", "edgeRoot");

  // create subregions using configuration "edge"
  synchronized (UseCase1Client.class) {
    if (rootRegion.subregions(true).size() == 0) {
      RegionAttributes ratts = RegionHelper.getRegionAttributes("edge");
      if(ratts.getPoolName() != null) {
        PoolHelper.createPool(ratts.getPoolName());
      }
      Region day0Region = rootRegion.createSubregion("day0", ratts);
      edgeRegion = day0Region;

      // do some gets immediately
      for (int i=0;i<10;i++) {
        day0Region.get(new Long(i));
        Log.getLogWriter().info("Retrieved entry " + i + " from Region " + day0Region.getName());
      }
      displayRegions();  
    }
  }
}

  /**
   * A Hydra INIT task that initialize the test region in the feeder VM
   * client according to the definition in useCase1Feeder.xml
   *
   */
public synchronized static void initFeederCache() throws Exception {

  // create cache using configuration "cache"
  Cache cache = CacheHelper.createCache("cache");

  // create root region using configuration "feedRoot"
  Region rootRegion = RegionHelper.createRegion("root", "feedRoot");

  // create subregions using configuration "feed"
  synchronized (UseCase1Client.class) {
    if (rootRegion.subregions(true).size() == 0) {
      RegionAttributes ratts = RegionHelper.getRegionAttributes("feed");
      for (int i = 0; i < REGION_NAMES.length; i++) {
        // todo@lhughes -- figure out why UseCase1 CBB uses distributed-ack here
        // this works fine for feeder, but has a negative impact on the 
        // clients performing invoke/fetch later
        // todo@lhughes - use this with bridgeClient feeder (has bridgeLoader)
        // todo@lhughes - use this with p2p feeder
        rootRegion.createSubregion(REGION_NAMES[i], ratts);
      }
      displayRegions();  
    }
  }
}

  public static void stopServers() throws ClientVmNotFoundException {
    MasterController.sleepForMs( 30000 );

    // kill all but 1 bridge server, do netstats after each kill
    List endpoints = BridgeHelper.getEndpoints();
    for (int i = 0; i < endpoints.size() - 1; i++) {
      BridgeHelper.Endpoint endpoint = (BridgeHelper.Endpoint)endpoints.get(i);
      ClientVmInfo target = new ClientVmInfo(endpoint);
      ClientVmMgr.stop("Stopping cache server",
                        ClientVmMgr.NICE_EXIT, ClientVmMgr.ON_DEMAND, target);
      MasterController.sleepForMs( 30000 );
      MasterController.sleepForMs( 30000 );
    }
  }

  /**
   * UseCase1 CBBs GFService.invoke
   */
static int readMin_ = 50;
static int readMax_ = 250;
static int sleepTime_ = 1000;

public static void invoke() throws RemoteException {
   Integer numEntries = (Integer)UseCase1BB.getInstance().getSharedMap().get(REGION_NAME);
   int entryCount_ = numEntries.intValue();
   Log.getLogWriter().info("Retrieved numEntries from BB " + entryCount_);

   int totalGets = 0;
   Random rand = new Random();
   long getsTime = 0L;
   int getCount = readMin_ + rand.nextInt(readMax_ - readMin_);

   // long startTime = System.nanoTime();
   long startTime = System.currentTimeMillis() * 1000000;
   for(int i=0; i<getCount; i++) {
     //perform a get on a random key
     Integer randomKey = new Integer(rand.nextInt(entryCount_));
     fetch(randomKey.intValue());
     Log.getLogWriter().fine("fetched " + randomKey.intValue());
     ++totalGets;
   }

   long endTime = System.currentTimeMillis() * 1000000;
   // long endTime = System.nanoTime();
   getsTime += (endTime - startTime);
   long sleeptime= 0;
   try {
     long s = System.currentTimeMillis() * 1000000;
     Thread.sleep(sleepTime_);
     long e = System.currentTimeMillis() * 1000000;
     sleeptime = (e - s);
   } catch (java.lang.Exception e) {
   }
   Log.getLogWriter().fine(Long.toString(getsTime) + ":" + Integer.toString(totalGets) + ":sleeptime=" + Long.toString(sleeptime) + ":sleepTime_=" + Integer.toString(sleepTime_) );
   Log.getLogWriter().info("totalGets = " + totalGets);
   ClientHelper.release(edgeRegion);
}

public static void fetch(int randomKey) throws RemoteException {
  try {
    //Thread.sleep(100);
    //how many entries did the feeder create?
    // int entryCount = ((Integer)_region.get("NUM_ENTRIES")).intValue();
    //Integer randomKey = new Integer(rand.nextInt(entryCount));
   Region _region = CacheHelper.getCache().getRegion(REGION_NAME);
    _region.get(new Long(randomKey));
  } catch (java.lang.Exception e) {
    throw new TestException("exception in region.get(): ", e);
  }
}
                                                                                   
  /**
   *  UseCase1's feeder task
   */
public static int MAX_DATA_ALLOWED = 250 * 1024 * 1024;    //250MB
public static final String[] REGION_NAMES={"day7",
                                           "day6",
                                           "day5",
                                           "day4",
                                           "day3",
                                           "day2",
                                           "day1",
                                           "day0"};
public static Region[] _regions = new Region[8];
public static Random rand = null;

public synchronized static void runFeeder() throws Exception {
  rand = new Random();

  Cache myCache = CacheHelper.getCache();
  Region parentRegion = myCache.getRegion("root");
  Set subRegions = parentRegion.subregions(true);
  int i=0;
  for (Iterator iter=subRegions.iterator(); iter.hasNext(); i++) {
    Region aRegion = (Region)iter.next();
    _regions[i] = myCache.getRegion(aRegion.getFullPath());
    Log.getLogWriter().info("Retrieved region: " + _regions[i].getFullPath() + " (scope=" + _regions[i].getAttributes().getScope() + ")");
  }

  // default is 2K bytes
  int averageSize = TestConfig.tab().intAt(HctPrms.averageSize, 2048);
                                                                                   
  for(i=0; i<REGION_NAMES.length; i++) {
    int totalData = 0;
    int currentKey = 0;
    byte[] value;
    Log.getLogWriter().info("Populating " + _regions[i].getName() + " with byte arrays of size within 20% of " + averageSize);
    while (totalData < MAX_DATA_ALLOWED) {
      value = new byte[randomSize(averageSize, 20)];
      _regions[i].put(new Integer(currentKey), value);
      totalData += value.length;
      currentKey++;
    }
    _regions[i].put("NUM_ENTRIES", new Integer(currentKey));
    Log.getLogWriter().info("Created " + currentKey + " entries in " + _regions[i].getName() + " with " + totalData + " bytes.");
    UseCase1BB.getInstance().getSharedMap().put(_regions[i].getFullPath(), new Integer(currentKey));
  }
  UseCase1BB.getInstance().print();
}

public static int randomSize(int average, int rangePercent) {
                                                                                   
  int range = 2 * average * rangePercent / 100;
  int lowerBound = average * (100 - rangePercent) / 100;
                                                                                   
  return lowerBound + rand.nextInt(range);
}

}
