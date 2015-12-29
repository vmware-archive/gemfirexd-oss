hydra.BridgePrms-names = bridge;
hydra.BridgePrms-maxThreads = ONEOF default 16 FOENO; // selector control

hydra.PoolPrms-names            = brloader;
hydra.PoolPrms-contactAlgorithm = hydra.EdgeHelper getRandomEndpointsInWanHctSite;
hydra.PoolPrms-subscriptionEnabled = true;
hydra.PoolPrms-readTimeout = 20000;

hydra.CachePrms-names = bridge edge;

hydra.RegionPrms-names         = bridgeRegion  clientRegion;
hydra.RegionPrms-poolName      = none        brloader;
hydra.RegionPrms-dataPolicy    = replicate   default;
hydra.RegionPrms-scope         = dack        local;

hydra.ConfigPrms-cacheConfig = bridge;
hydra.ConfigPrms-regionConfig = bridgeRegion;
hydra.ConfigPrms-bridgeConfig	= bridge;
newWan.WANTestPrms-clientCacheConfig = edge;

hydra.ConfigPrms-gatewaySenderConfig 		= sender;
hydra.GatewaySenderPrms-names 				= sender;
hydra.GatewaySenderPrms-parallel 			= false;
hydra.GatewaySenderPrms-remoteDistributedSystemsAlgorithm =  hydra.GatewaySenderHelper getRemoteDistributedSystems;
hydra.GatewaySenderPrms-batchSize           = ONEOF 100 200 500 FOENO;
hydra.GatewaySenderPrms-batchTimeInterval   = ONEOF 500 1000 2000 1500 2000 FOENO;
hydra.GatewaySenderPrms-maximumQueueMemory  = ONEOF 50 100 150 FOENO;
hydra.GatewaySenderPrms-socketBufferSize    = default;
hydra.GatewaySenderPrms-manualStart		= false;

hydra.GatewaySenderPrms-diskStoreName 	= disk;
hydra.DiskStorePrms-names 				= disk;

hydra.ConfigPrms-gatewayReceiverConfig 			= receiver;
hydra.GatewayReceiverPrms-names 				= receiver;
hydra.GatewayReceiverPrms-maximumTimeBetweenPings = RANGE 15000 60000 EGNAR;
hydra.GatewayReceiverPrms-socketBufferSize 		= default;

hydra.RegionPrms-gatewaySenderNames = sender;

hydra.GemFirePrms-remoteDistributedSystems =
     fcn "newWan.WANTest.generateRingRemoteDSName
          (\"valid_bridgeds_\", ${validWanSites}, ${locatorHostsPerSite})" 
     ncf
     invalid_bridgeds_${invalidWanSites} 
     ,
     fcn "newWan.WANTest.generateRingRemoteDSName
          (\"invalid_bridgeds_\", ${invalidWanSites}, ${locatorHostsPerSite})" 
     ncf
     valid_bridgeds_${validWanSites} 
     ,
     none
     ; 
    
THREADGROUP validlocator
  totalThreads = fcn ${validWanSites} * ${locatorHostsPerSite}
               * ${locatorVMsPerHost} * ${locatorThreadsPerVM} ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
           (\"valid_locator\", ${validWanSites}, ${locatorHostsPerSite}, false, true)" ncf;
           
THREADGROUP invalidlocator
  totalThreads = fcn ${invalidWanSites} * ${locatorHostsPerSite}
               * ${locatorVMsPerHost} * ${locatorThreadsPerVM} ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
           (\"invalid_locator\", ${invalidWanSites}, ${locatorHostsPerSite}, false, true)" ncf;

THREADGROUP validpeer
  totalThreads = fcn ${validWanSites} * ${bridgeHostsPerSite}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
                 (\"validbridge\", ${validWanSites}, ${bridgeHostsPerSite}, false, true)" ncf;
                 
THREADGROUP invalidpeer
  totalThreads = fcn ${invalidWanSites} * ${bridgeHostsPerSite}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
                 (\"invalidbridge\", ${invalidWanSites}, ${bridgeHostsPerSite}, false, true)" ncf;

hydra.VmPrms-extraVMArgs =
  fcn "hydra.TestConfigFcns.duplicate
      (\"-Dnone\",
          ${locatorHostsPerSite} * ${validWanSites}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
      (\"-Dnone\",
          ${locatorHostsPerSite} * ${invalidWanSites}, true)" ncf
  ,
    fcn "hydra.TestConfigFcns.duplicate
      (\"-Xms${peerMem} -Xmx${peerMem}\",
          ${bridgeHostsPerSite} * ${validWanSites}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
      (\"-Xms${peerMem} -Xmx${peerMem}\",
          ${bridgeHostsPerSite} * ${invalidWanSites}, true)" ncf
  ;
