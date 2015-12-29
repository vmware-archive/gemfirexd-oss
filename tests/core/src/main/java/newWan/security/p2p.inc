hydra.CachePrms-names = peer;

hydra.RegionPrms-names         = peer;
hydra.RegionPrms-dataPolicy    = replicate;
hydra.RegionPrms-scope         = dack;

hydra.ConfigPrms-cacheConfig = peer;
hydra.ConfigPrms-regionConfig = peer;

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
hydra.DiskStorePrms-queueSize 			= ONEOF 1 5 10 20 FOENO;
hydra.DiskStorePrms-timeInterval 		= oneof 1 10 50 500 1000 2000 foeno;

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
                 (\"valid_bridge\", ${validWanSites}, ${bridgeHostsPerSite}, false, true)" ncf;
                 
THREADGROUP invalidpeer
  totalThreads = fcn ${invalidWanSites} * ${bridgeHostsPerSite}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
                 (\"invalid_bridge\", ${invalidWanSites}, ${bridgeHostsPerSite}, false, true)" ncf;

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
