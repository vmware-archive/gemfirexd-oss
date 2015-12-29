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

hydra.ConfigPrms-gatewayReceiverConfig 			= receiver;
hydra.GatewayReceiverPrms-names 				= receiver;
hydra.GatewayReceiverPrms-maximumTimeBetweenPings = RANGE 15000 60000 EGNAR;
hydra.GatewayReceiverPrms-socketBufferSize 		= default;

hydra.RegionPrms-gatewaySenderNames = sender;

hydra.GemFirePrms-remoteDistributedSystems =
     fcn "newWan.WANTest.generateRingRemoteDSName
          (\"writer_bridgeds_\", ${writerWanSites}, ${locatorHostsPerSite})" 
     ncf
     reader_bridgeds_${readerWanSites} 
     ,
     fcn "newWan.WANTest.generateRingRemoteDSName
          (\"reader_bridgeds_\", ${readerWanSites}, ${locatorHostsPerSite})" 
     ncf
     writer_bridgeds_${writerWanSites} 
     ,
     none
     ; 
        
THREADGROUP writerlocator
  totalThreads = fcn ${writerWanSites} * ${locatorHostsPerSite}
               * ${locatorVMsPerHost} * ${locatorThreadsPerVM} ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
           (\"writer_locator\", ${writerWanSites}, ${locatorHostsPerSite}, false, true)" ncf;

THREADGROUP readerlocator
  totalThreads = fcn ${readerWanSites} * ${locatorHostsPerSite}
               * ${locatorVMsPerHost} * ${locatorThreadsPerVM} ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
           (\"reader_locator\", ${readerWanSites}, ${locatorHostsPerSite}, false, true)" ncf;
           
THREADGROUP writerpeer
  totalThreads = fcn ${writerWanSites} * ${bridgeHostsPerSite}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
                 (\"writer_bridge\", ${writerWanSites}, ${bridgeHostsPerSite}, false, true)" ncf;

THREADGROUP readerpeer
  totalThreads = fcn ${readerWanSites} * ${bridgeHostsPerSite}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
                 (\"reader_bridge\", ${readerWanSites}, ${bridgeHostsPerSite}, false, true)" ncf;
                 
        

hydra.VmPrms-extraVMArgs =
  fcn "hydra.TestConfigFcns.duplicate
      (\"-Dnone\",
          ${locatorHostsPerSite} * ${writerWanSites}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
      (\"-Dnone\",
          ${locatorHostsPerSite} * ${readerWanSites}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
      (\"-Xms${peerMem} -Xmx${peerMem}\",
          ${bridgeHostsPerSite} * ${writerWanSites}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
      (\"-Xms${peerMem} -Xmx${peerMem}\",
          ${bridgeHostsPerSite} * ${readerWanSites}, true)" ncf
  ;
