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

package admin.jmx;

import hydra.BridgeHelper;
import hydra.CacheHelper;
import hydra.ClientVmMgr;
import hydra.ClientVmNotFoundException;
import hydra.ConfigHashtable;
import hydra.ConfigPrms;
import hydra.DistributedSystemHelper;
import hydra.GatewayHubHelper;
import hydra.GsRandom;
import hydra.HydraRuntimeException;
import hydra.Log;
import hydra.RegionHelper;
import hydra.RegionPrms;
import hydra.TestConfig;
import hydra.blackboard.SharedMap;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;
import java.util.Vector;

import util.NameFactory;
import util.TestException;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.admin.GemFireMemberStatus;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.InterestResultPolicy;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.RegionExistsException;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.cache.util.CacheListenerAdapter;
import com.gemstone.gemfire.cache.util.GatewayHub;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.cache.GatewayHubStatus;
import com.gemstone.gemfire.internal.cache.HARegion;
import com.gemstone.gemfire.internal.cache.tier.InternalBridgeMembership;

/**
 * Pick and Chose from various tests to get a JMX test setup together
 * All config details picked up from WAN tests
 */
public class RecycleSystem 
{
	//============================================================================
	// TASKS
	//============================================================================
	public static void doTask() 
	{
		if (CacheHelper.getCache()!=null)
		{
			DistributedSystem disSys = CacheHelper.getCache().getDistributedSystem();
			Cache cache = CacheHelper.getCache();
			
			GemFireMemberStatus status = new GemFireMemberStatus(cache);
			
			logger.info("RecycleSystem:doTask:: Status from the existing system " + status.toString());
		} else 
		{
			logger.info("RecycleSystem:doTask:: No Cache Available");
		}	
		int sleepSecs = conftab.intAt(RecyclePrms.sleepBetweenCycles, 120);
	    try { Thread.sleep(sleepSecs*1000); } catch(Exception e) { }
	}	
	
	public static void updateKeys()
	{
		if (DistributedSystemHelper.getDistributedSystem() == null
        || CacheHelper.getCache() == null) {
      return;
    }
		Cache cache = CacheFactory.getInstance(DistributedSystemHelper.getDistributedSystem());
		Object [] regionList = cache.rootRegions().toArray();
		logger.info("RecycleSystem:updateKeys:: cache" + cache);
		int numRegs = regionList.length;
		
			for(int i =0 ; i < numRegs; i++)
			{
				Region reg = (Region)regionList[i];
                                if (!(reg instanceof HARegion) ) {
                                   Set keys = reg.keySet();
				   logger.info("RecycleSystem:updateKeys:: number of keys in  "+ reg.getFullPath() + " is  " + keys.size());
				   for (Iterator iter=keys.iterator(); iter.hasNext() ;) {
				     Object key = iter.next();
				     reg.put(key, key);
				     logger.info("RecycleSystem:updateKeys:: updated value for" + key);
				   }
                                }
			}
		
		GemFireMemberStatus status = new GemFireMemberStatus(cache);
			
		logger.info("RecycleSystem:updateKeys:: Status from the existing system " + status.toString());
		
		logger.info("RecycleSystem:updateKeys::" + InternalBridgeMembership.getClientQueueSizes());
		
		int sleepSecs = restart();
	    try { Thread.sleep(sleepSecs*1000); } catch(Exception e) { }
	}
        
        public static void getKeys()
        {
                Cache cache = CacheFactory.getInstance(DistributedSystemHelper.getDistributedSystem());
                Object [] regionList = cache.rootRegions().toArray();
                logger.info("RecycleSystem:getKeys:: cache" + cache);
                int numRegs = regionList.length;
                
                        for(int i =0 ; i < numRegs; i++)
                        {
                                Region reg = (Region)regionList[i];
                                if (!(reg instanceof HARegion) ) {
                                   Set keys = reg.keySet();
                                   logger.info("RecycleSystem:getKeys:: number of keys in  "+ reg.getFullPath() + " is  " + keys.size());
                                   for (Iterator iter=keys.iterator(); iter.hasNext() ;) {
                                     Object key = iter.next();
                                     Object value = reg.get(key);
                                     logger.info("RecycleSystem:getKeys:: value for" + key+" is "+value);
                                   }
                                }
                        }
                         
                int sleepSecs = restart();
            try { Thread.sleep(sleepSecs*1000); } catch(Exception e) { }
        }

        public static void invokeCacheMiss() {
          Cache cache = CacheFactory.getInstance(DistributedSystemHelper.getDistributedSystem());
          Object[] regionList = cache.rootRegions().toArray();
          int numRegs = regionList.length;

          for (int i = 0; i < numRegs; i++) {
            Region reg = (Region)regionList[i];
            if (!(reg instanceof HARegion)) {
              int keysize = reg.keySet().size();
              for (int j = 0; j < keysize; j++)
                reg.get("notexist" + j);
            }
          }
        }  
        
        


	protected static void hardStop()
	{    
		boolean hardStop = RecyclePrms.isRecycleModeHardKill();
		if (hardStop)
		{
		    try {
		    	ClientVmMgr.stop("killing myself", ClientVmMgr.MEAN_KILL, ClientVmMgr.IMMEDIATE);
		    	return;
		    } catch (ClientVmNotFoundException ex) {
		    	throw new HydraRuntimeException("I should always be able to kill myself", ex);
		    }
		}		
	}
	
	
	public static void stopEdgeClient()
	{
		stopCacheEntities();
		hardStop();
	}
	
	/**
	 * Stop the existing Cache entities
	 */
	public static void stopCacheEntities()
	{
		if (CacheHelper.getCache()!=null)
		{
			DistributedSystem disSys = CacheHelper.getCache().getDistributedSystem();

			RecycleSysBlackboard.getInstance().getSharedMap().remove(disSys.getMemberId());
			
			GatewayHubHelper.stopGatewayHub();
			BridgeHelper.stopBridgeServer();
			CacheHelper.closeCache();
			
			disSys.disconnect();
			absoluteRegionCount = 0;

			logger.info("RecycleSystem:stopCacheEntities:: Disconnected from the existing system");
		} else 
		{
			logger.info("RecycleSystem:stopCacheEntities:: Wasn't even connected to any system");
		}
	}
	
	public static void stopGateways()
	{
		if (CacheHelper.getCache()!=null && GatewayHubHelper.getGatewayHub()!=null)
		{
			GatewayHubHelper.getGatewayHub().stopGateways();
			//GatewayHubHelper.getGatewayHub().removeGateway(arg0)
			logger.info("RecycleSystem:stopGateways:: Stopped all Gateways in the Hub");
		} else 
		{
			logger.info("RecycleSystem:stopGateways:: There was no Cache or Hub");
		}
	}

	public static void stopGatewayHub()
	{
		if (CacheHelper.getCache()!=null && GatewayHubHelper.getGatewayHub()!=null)
		{
			DistributedSystem disSys = CacheHelper.getCache().getDistributedSystem();
			Cache cache = CacheHelper.getCache();
			GemFireMemberStatus status = new GemFireMemberStatus(cache);
			
			GatewayHubHelper.getGatewayHub().stopGateways();
			GatewayHubHelper.stopGatewayHub();
			//GatewayHubHelper.getGatewayHub().removeGateway(arg0)

			// Increment Shared Counter for Gateway
			if (status.getIsPrimaryGatewayHub())
			{
				long num = RecycleSysBlackboard.getInstance().getSharedCounters().
								decrementAndRead(RecycleSysBlackboard.numOfPrimaryGateways);
				
				logger.info("RecycleSystem:stopGateways:: Stopped all Gateways & the Prim Hub." +
							"Current total=" + num);
			} else if (status.getIsSecondaryGatewayHub())
			{
				long num = RecycleSysBlackboard.getInstance().getSharedCounters().
								decrementAndRead(RecycleSysBlackboard.numOfSecondaryGateways);
					
				logger.info("RecycleSystem:stopGateways:: Stopped all Gateways & the Seco Hub." +
							"Current total=" + num);
			} else 
			{
				long num = RecycleSysBlackboard.getInstance().getSharedCounters().
								decrementAndRead(RecycleSysBlackboard.numOfGateways);
					
				logger.info("RecycleSystem:stopGateways:: Stopped all Gateways & the Hub." +
							"Current total=" + num);
			} 
				
		} else 
		{
			logger.info("RecycleSystem:stopGateways:: There was no Cache or Hub");
		}
	}
	
	/**
	 * Initializes a peer cache based on the {@link ConfigPrms}.
	 */
	public static void startPeerCache() 
	{
		startCache();
	}

	/**
	 * Initializes a server cache based on the {@link ConfigPrms}.
	 */
	public static void startServerCache() 
	{
		String bridgeConfig 	= ConfigPrms.getBridgeConfig();
		//String gatewayHubConfig = ConfigPrms.getGatewayHubConfig();		

		startCache();
		
		CacheServer bridgeServer = BridgeHelper.startBridgeServer(bridgeConfig);
		InternalDistributedSystem ds = (InternalDistributedSystem)CacheHelper.getCache().getDistributedSystem();
		DistributionConfig config = ds.getConfig();
		logger.info("RecycleSystem::startServerCache bindAddress = "+config.getBindAddress());
		Cache cache = CacheHelper.getCache();
		
		GemFireMemberStatus status = new GemFireMemberStatus(cache);
		RecycleSysBlackboard.getInstance().getSharedMap().
        put(status.getMemberId(), status);
		//GatewayHubHelper.createGatewayHub(gatewayHubConfig);
		//GatewayHubHelper.addGateways(gatewayConfig);
		//GatewayHubHelper.startGatewayHub();
	}
	
	/**
	 * Initializes a Gateway Hub cache based on the {@link ConfigPrms}.
	 */
	public static void createGatewayCache() 
	{
		String bridgeConfig 	= ConfigPrms.getBridgeConfig();
		String gatewayHubConfig = ConfigPrms.getGatewayHubConfig();		
		String gatewayConfig 	= ConfigPrms.getGatewayConfig();

		startCache();
		
		BridgeHelper.startBridgeServer(bridgeConfig);
		GatewayHub hub =GatewayHubHelper.createGatewayHub(ConfigPrms.getGatewayHubConfig());
		logger.info("RecycleSystem::createGatewayCache::setpForDS = " + 
					CacheHelper.getCache().getDistributedSystem().getName());

	}
	
	public static void createGateways()
	{
		if (GatewayHubHelper.getGatewayHub()!=null)
		{
			GatewayHubHelper.addGateways(ConfigPrms.getGatewayConfig());
			startGatewayHub();			
		} else {
			logger.info("RecycleSystem::createGateways::No Gateway Added " + 
					    "No Gateway Hub identified");
		}
	}

	/**
	 * Start a a Gateway Hub earlier created
	 */
	public static void startGateways() 
	{
		try {
			GatewayHubHelper.getGatewayHub().startGateways();
		} catch (IOException ioEx) {
                  String err = "Problem starting gateways";
                  throw new HydraRuntimeException(err, ioEx);
		}
	}
	
	/**
	 * Start a a Gateway Hub earlier created
	 */
	public static void startGatewayHub() 
	{
			GatewayHubHelper.startGatewayHub();
               try {
                        GatewayHubHelper.getGatewayHub().startGateways();
               } catch (IOException ioEx) {
                  String err = "Problem starting gateways";
                  throw new HydraRuntimeException(err, ioEx);
               }

	                GemFireMemberStatus status = new GemFireMemberStatus(
	                    CacheHelper.getCache());

	                RecycleSysBlackboard.getInstance().getSharedMap().
	                      put(status.getMemberId(), status);			
	}
	
	/**
	 * Initializes a edge cache based on the {@link ConfigPrms}.
	 */
	public static void startEdgeClient() 
	{
		String cacheConfig 	= RecyclePrms.getEdgeCacheConfig();
		String regionConfig = RecyclePrms.getEdgeRegionConfig();
//
//		RecycleSystem system  = new RecycleSystem();
//		CacheLoader loader    = system.new EdgeLoader();
//		CacheWriter writer    = system.new EdgeWriter();
//		CacheListener listener= system.new EdgeCacheListener();

		Cache cache = CacheHelper.createCache(cacheConfig);
		int numRegs = conftab.intAt(RecyclePrms.numberOfRootRegions, 0);
    if (RecyclePrms.isRegionNamesDefined())
      createUserDefinedRegion(cache);
    else
      createRegion(regionConfig, cache, numRegs, false); // Should be false for a partition:
    
		GemFireMemberStatus status = new GemFireMemberStatus(cache);
		int loaders = cache.getCacheServers().size();
				
		RecycleSysBlackboard.getInstance().getSharedMap().
        put(status.getMemberId(), status);
		
		logger.info("RecycleSystem::startEdgeClient:: statusIsClient:serversSize " + 
					status.getIsClient() + ":" + loaders);
			
		// Reset back the MCast for use
	}
	
	public static void restartPeerCache() 
	{
	    int sleepSecs = restart();
	    try { Thread.sleep(sleepSecs*1000); } catch(Exception e) { }
		
	    // Stop
		stopCacheEntities();
		hardStop();
		// Let us re-start
		startPeerCache();
	}

	/**
	 * Stop the existing Cache entities and then re-init
	 */
	public static void restartServerCache()
	{
	    int sleepSecs = restart();
	    try { Thread.sleep(sleepSecs*1000); } catch(Exception e) { logger.warning(e.getMessage()); }
		
	    // Stop
		stopCacheEntities();
		hardStop();
		
		// Let us re-start
		startServerCache();
	}

	/**
	 * Stop the existing Cache entities and then re-init
	 */
	public static void restartGatewayCache()
	{
	    // Add Gateways
	    if (executionCount == 0) 
	    {
			GatewayHubHelper.addGateways(ConfigPrms.getGatewayConfig());
			GatewayHubHelper.startGatewayHub();	    
	    }
	    
	    int sleepSecs = restart();
	    try { Thread.sleep(sleepSecs*1000); } catch(Exception e) { logger.warning(e.getMessage()); }
	    
	    // Stop Gateways only
		stopGateways();
		
		// Let us re-start
		startGateways();

		DistributedSystem disSys = CacheHelper.getCache().getDistributedSystem();
		Cache cache = CacheHelper.getCache();
		GemFireMemberStatus status = new GemFireMemberStatus(cache);
		logger.info("RecycleSystem::restartgatewayCache:Status = " + ((GatewayHubStatus)(status.getGatewayHubStatus())).getGatewayStatuses()[0].getEndpointStatuses().length);
		logger.info("RecycleSystem::restartgatewayCache:Status = " + status.toString());
	}
	
	public static void printGatewayStatus()
	{
		DistributedSystem disSys = CacheHelper.getCache().getDistributedSystem();
		Cache cache = CacheHelper.getCache();
		GemFireMemberStatus status = new GemFireMemberStatus(cache);
		logger.info("RecycleSystem::restartgatewayCache:Status = " + status.toString());
		int sleepSecs = restart();
	    try { Thread.sleep(sleepSecs*1000); } catch(Exception e) { logger.warning(e.getMessage()); }
	}
	
	/**
	 * Stop the existing Cache entities and then re-init
	 */
	public static void restartGatewayHubs()
	{
	    int sleepSecs = restart();
		try { Thread.sleep(sleepSecs*1000); } catch(Exception e) { logger.warning(e.getMessage()); }
		
		// Let us re-start
		startGatewayHub();

	    sleepSecs = restart();
	    try { Thread.sleep(sleepSecs*1000); } catch(Exception e) { logger.warning(e.getMessage()); }

	    DistributedSystem disSys = CacheHelper.getCache().getDistributedSystem();
		Cache cache = CacheHelper.getCache();
		GemFireMemberStatus status = new GemFireMemberStatus(cache);

		// Increment Shared Counter for Gateway
		if (status.getIsPrimaryGatewayHub())
		{
			long num = RecycleSysBlackboard.getInstance().getSharedCounters().
							incrementAndRead(RecycleSysBlackboard.numOfPrimaryGateways);
			
			logger.info("RecycleSystem:startGatewayHub:: Started all Gateways & the Prim Hub." +
						"Current total=" + num);
		} else if (status.getIsSecondaryGatewayHub())
		{
			long num = RecycleSysBlackboard.getInstance().getSharedCounters().
							incrementAndRead(RecycleSysBlackboard.numOfSecondaryGateways);
				
			logger.info("RecycleSystem:startGatewayHub:: Started all Gateways & the Seco Hub." +
						"Current total=" + num);
		} else 
		{
			long num = RecycleSysBlackboard.getInstance().getSharedCounters().
							incrementAndRead(RecycleSysBlackboard.numOfGateways);
				
			logger.info("RecycleSystem:startGatewayHub:: Started all Gateways & the Hub." +
						"Current total=" + num);
		} 

		// Stop Gateways only
		stopGatewayHub();
		hardStop();
		
		logger.info("RecycleSystem::restartGatewayHubs:Status = " + status.toString());
	}
	
	public static void restartEdgeClient() 
	{
	    int sleepSecs = restart();
	    try { Thread.sleep(sleepSecs*1000); } catch(Exception e) { }
		
	    // Stop
		stopCacheEntities();
		hardStop();

		// Let us re-start
	    startEdgeClient();
	}
	
	public static void printResults()
	{
		logger.info("RecycleSystem:printResults");
		
		RecycleSysBlackboard.getInstance().printSharedCounters();
	}

	
	private static int restart() 
	{
	    int sleepSecs = conftab.intAt(RecyclePrms.sleepBetweenCycles, 120);    
	    if (executionCount == 0) {
        	    sleepSecs = 1;
        	    executionCount++;
	    }
	    
	    return sleepSecs;
	}

	protected static void startCache()
	{
		String cacheConfig	= ConfigPrms.getCacheConfig();
		String regionConfig	= ConfigPrms.getRegionConfig();

		Cache cache = CacheHelper.createCache(cacheConfig);
		
		int numRegs = conftab.intAt(RecyclePrms.numberOfRootRegions, 0);
		

    if (RecyclePrms.isRegionNamesDefined())
      createUserDefinedRegion(cache);
    else
      createRegion(regionConfig, cache, numRegs, false);

		GemFireMemberStatus status = new GemFireMemberStatus(CacheHelper.getCache());

                RecycleSysBlackboard.getInstance().getSharedMap().
                      put(status.getMemberId(), status);
	}
	
	public static void destroyAndCreateRegions()
	{
		int sleepSecs = restart();
	    try { Thread.sleep(sleepSecs*1000); } catch(Exception e) { }
	    
		String regionConfig	= ConfigPrms.getRegionConfig();
		Cache cache = CacheFactory.getInstance(DistributedSystemHelper.getDistributedSystem());
		
		int numRegs = conftab.intAt(RecyclePrms.numberOfRootRegions, 0);
		
		logger.info("\n\tRecycleSystem:destroyAndCreateRegions:: No of root region of "+
					cache +" before destroying  " + cache.rootRegions().size() + ", no of root region desired : " + numRegs);
			int rootRegNo = cache.rootRegions().size();
			
			if(numRegs < rootRegNo)
			{
				destroyRegion(cache.rootRegions(), rootRegNo - numRegs);
				logger.info("\n\tRecycleSystem:destroyAndCreateRegions:: No of root region of "
							+ cache +" after destroying  " + cache.rootRegions().size());
			}else if(numRegs > rootRegNo)
			{
				createRegion(regionConfig, cache, numRegs - rootRegNo, false);
				logger.info("\n\t RecycleSystem:destroyAndCreateRegions:: No of root region of "
						+ cache +" after creating new region  " + cache.rootRegions().size());
			}
	}
		
	protected static void destroyRegion(Set regions, int noOfRegToBeDestroyed)
	{ 
		Object [] regionList = regions.toArray();
		int numRegs = regionList.length;
		Random generator = new Random();
		
		if(numRegs < 1)
			return;
		
		int temp = 0 ;
		for(int i =0 ; i < noOfRegToBeDestroyed; i++) {
		  temp = generator.nextInt(numRegs);
     	  Region region = (Region)regionList[temp];
		  if(region == null) {
			i--;
			continue;
		  }
     	  
     	  Set keySet = region.keySet();
				
		  // printing region data before destroying
		  Log.getLogWriter().info("Data in  " + region.toString() + ": -----" );
		  for(Iterator iter = keySet.iterator(); iter.hasNext();) {
			Object key = iter.next();
			Log.getLogWriter().info("Key :  " + key + "/ value:" + region.get(key));
		  }
		  
		  region.destroyRegion();
		  regionList[temp] = null;
		}
	  
	  logger.info("RecycleSystem:destroyRegion:: destroyed region :" + regionList[temp]);
	}

  /**
   * Creates user defined regions based on the region attributes specified.
   * 
   */
  
  protected static void createUserDefinedRegion(Cache cache) {
    Vector regionNames = TestConfig.tab().vecAt(RegionPrms.names, null);
    logger
        .info("RecycleSystem:createRegion:: Number of more Region to be created "
            + regionNames.size());
    for (int i = 0; i < regionNames.size(); i++) {
      String regionDescriptName = (String) (regionNames.get(i));

      Region aRegion = null;

      String regionName = RegionHelper.getRegionDescription(regionDescriptName)
          .getRegionName();
      try {
        aRegion = cache.createRegion(regionName, RegionHelper
            .getRegionAttributes(regionDescriptName));

        Log.getLogWriter().info(
            "Created  region " + regionName + " with region descript name "
                + regionDescriptName);
        populateRegion(aRegion);
      } catch (RegionExistsException e) {
        // region already exists; ok
        Log.getLogWriter().info("Using existing  region " + regionName);
        aRegion = e.getRegion();
        if (aRegion == null) {
          throw new TestException(
              "RegionExistsException.getRegion returned null");
        }
      }
    }
  }
		
	protected static void createRegion(String regionConfig, Cache cache, int noOfRegToBeCreated, boolean registerInterest)
	{	
		logger.info("RecycleSystem:createRegion:: Number of more Region to be created " + noOfRegToBeCreated);
		String regInfo = "\n\t";
		
		for (int i=0; i < noOfRegToBeCreated; i++)
		{
			AttributesFactory factory = RegionHelper.getAttributesFactory(regionConfig);
			RegionAttributes attributes = RegionHelper.getRegionAttributes(factory);
			regInfo = "\n\t";
			
			String name = REGION_NAME+"_"+ absoluteRegionCount;
			absoluteRegionCount = absoluteRegionCount +1;
			
			Region reg = RegionHelper.createRegion(name, attributes);
			if(registerInterest)
				registerInterest(reg);
			populateRegion(reg);
			regInfo = regInfo + reg.getFullPath()+"\n\t";
			int j=0;
			int subRegs = conftab.intAt(RecyclePrms.regionDepth, 0);
			while (j < subRegs) {
					reg = reg.createSubregion(reg.getName() + "_" + j,
							attributes);
					if(registerInterest)
						registerInterest(reg);
					populateRegion(reg);
					regInfo = regInfo + reg.getFullPath() + "\n\t";
				j++;
			}
		}
		logger.info("RecycleSystem:createRegion:: Region Created " + regInfo);
	}
	

	/**
	 * Registers interest in all keys using the client interest policy.
	 */
	private static void registerInterest(Region region) 
	{
		InterestResultPolicy interestResultPolicy = RecyclePrms
				.getInterestResultPolicy();
		region.registerInterest("ALL_KEYS", interestResultPolicy);
		Log
				.getLogWriter()
				.info(
						"Initialized region "
								+ region
								+ "\nRegistered interest in ALL_KEYS with InterestResultPolicy = "
								+ interestResultPolicy);
	}
	
	
	private static void populateRegion(Region region){
	  boolean isPartitionRegion = false;
      if(region.getAttributes().getDataPolicy().equals(DataPolicy.PARTITION))
        isPartitionRegion = true;
          
      int noOfEntity = conftab.intAt(RecyclePrms.numberOfEntitiesInRegion, 0);
	  Random generator = new Random();
	  for(int i = 0; i< noOfEntity ; i++)
	  {
	    Object key = String.valueOf(i);
        Object value = new Integer(generator.nextInt(200));
	    if(isPartitionRegion)
          key = NameFactory.getNextPositiveObjectName();
          region.put(key,value);
          Log.getLogWriter().info("Putting key :" + key + " in region :" + region.getFullPath());
	  }
	  
	  Log.getLogWriter().info("region info : " + region.toString() + " data size:" + region.entrySet().size());
	}
	
	public static void setSystemReady(){
	  SharedMap map = RecycleSysBlackboard.getInstance().getSharedMap();
	  map.put(RecycleSysBlackboard.SYSTEM_READY, "true");
       }

	////////////////////////////////////////////////////////////////////
	protected static final String REGION_NAME = "GlobalVillage";
	protected static int absoluteRegionCount;
	
	static ConfigHashtable conftab = TestConfig.tab();
	static LogWriter logger = Log.getLogWriter();
	static GsRandom rand = new GsRandom();
	
	static int executionCount = 0;
	
	public class EdgeCacheListener
		extends CacheListenerAdapter
	{
		public EdgeCacheListener()
		{
			;
		}
		
		public void afterRegionCreate(RegionEvent event) 
		{
			logger.info("Executing EdgeCacheListener:afterRegionCreate::Notification Received");
		}
	}
	
	public class EdgeWriter
		implements CacheWriter
	{

		public void beforeCreate(EntryEvent arg0) 
			throws CacheWriterException 
		{
			logger.info("EdgeWriter::beforeCreate");
		}

		public void beforeDestroy(EntryEvent arg0) 
			throws CacheWriterException 
		{
			logger.info("EdgeWriter::beforeDestroy");
		}

		public void beforeRegionClear(RegionEvent arg0) 
			throws CacheWriterException 
		{
			logger.info("EdgeWriter::beforeRegionClear");
		}

		public void beforeRegionDestroy(RegionEvent arg0) 
			throws CacheWriterException 
		{
			logger.info("EdgeWriter::beforeRegionDestroy");
		}

		public void beforeUpdate(EntryEvent arg0) 
			throws CacheWriterException 
		{
			logger.info("EdgeWriter::beforeUpdate");
		}

		public void close() 
		{
			logger.info("EdgeWriter::close");
		}
	}
}
