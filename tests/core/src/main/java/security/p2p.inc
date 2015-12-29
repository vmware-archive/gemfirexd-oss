hydra.CachePrms-names = peer;

hydra.RegionPrms-names         = peer;
hydra.RegionPrms-enableGateway = true;
hydra.RegionPrms-dataPolicy    = replicate;
hydra.RegionPrms-scope         = dack;

wan.CacheClientPrms-cacheConfig = peer;
wan.CacheClientPrms-regionConfig = peer;

wan.CacheClientPrms-secToWaitForQueue = 1200;

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
