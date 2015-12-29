hydra.CachePrms-names = peer;

hydra.RegionPrms-names         = peer;
hydra.RegionPrms-enableGateway = true;
hydra.RegionPrms-dataPolicy    = replicate;
hydra.RegionPrms-scope         = dack;

wan.CacheClientPrms-cacheConfig = peer;
wan.CacheClientPrms-regionConfig = peer;

wan.CacheClientPrms-secToWaitForQueue = 1200;

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
