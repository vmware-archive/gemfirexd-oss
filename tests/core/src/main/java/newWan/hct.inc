hydra.BridgePrms-names = bridge;
hydra.BridgePrms-maxThreads = ONEOF default 16 FOENO; 

hydra.PoolPrms-names    = brloader     ;
hydra.PoolPrms-contactAlgorithm = hydra.EdgeHelper getRandomEndpointsInWanHctSite;
hydra.PoolPrms-subscriptionEnabled = true;
hydra.PoolPrms-subscriptionRedundancy = -1;
hydra.PoolPrms-readTimeout = 30000;

hydra.CachePrms-names = bridgeCache edgeCache;

hydra.RegionPrms-names          = bridgeRegion	clientRegion;
hydra.RegionPrms-poolName		= none        	brloader;
hydra.RegionPrms-dataPolicy     = replicate   	default;
hydra.RegionPrms-scope          = dack        	local;
hydra.RegionPrms-cacheListeners	 = util.SilenceListener;

hydra.ConfigPrms-cacheConfig	= bridgeCache;
hydra.ConfigPrms-regionConfig	= bridgeRegion;
hydra.ConfigPrms-bridgeConfig	= bridge;

newWan.WANTestPrms-clientCacheConfig            = edgeCache;
newWan.WANTestPrms-senderQueueRegionListener 	= none;

hydra.GemFirePrms-conserveSockets = false;

hydra.GemFirePrms-remoteDistributedSystems =
     fcn "newWan.WANTest.generateRingRemoteDSName
          (\"bridgeds_\", ${wanSites}, ${locatorHostsPerSite})" ncf         
     ,
     none
     ; 
     
THREADGROUP locator
  totalThreads = fcn ${wanSites} * ${locatorHostsPerSite}
               * ${locatorVMsPerHost} * ${locatorThreadsPerVM} ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
           (\"locator\", ${wanSites}, ${locatorHostsPerSite}, false, true)" ncf;

THREADGROUP bridge
  totalThreads = fcn ${wanSites} * ${bridgeHostsPerSite}
               * ${bridgeVMsPerHost} * ${bridgeThreadsPerVM} ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
           (\"bridge\", ${wanSites}, ${bridgeHostsPerSite}, false, true)" ncf;

hydra.VmPrms-extraVMArgs =
  fcn "hydra.TestConfigFcns.duplicate
      (\"-Dnone\",
          ${locatorHostsPerSite} * ${wanSites}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
      (\"-Xms${serverMem} -Xmx${serverMem}\",
          ${bridgeHostsPerSite} * ${wanSites}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
      (\"-Xms${clientMem} -Xmx${clientMem}\",
          ${edgeHostsPerSite} * ${wanSites}, true)" ncf;
