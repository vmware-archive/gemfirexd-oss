hydra.CachePrms-names = peer;

hydra.RegionPrms-names         = peer;
hydra.RegionPrms-enableGateway = true;
hydra.RegionPrms-dataPolicy    = replicate;
hydra.RegionPrms-scope         = dack;

wan.CacheClientPrms-cacheConfig = peer;
wan.CacheClientPrms-regionConfig = peer;

wan.CacheClientPrms-secToWaitForQueue = 1200;
wan.CacheClientPrms-numPutAllEntries = 5;   // max per perAll

THREADGROUP locator
  totalThreads = fcn ${wanSites} * ${locatorHostsPerSite}
               * ${locatorVMsPerHost} * ${locatorThreadsPerVM} ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
           (\"locator\", ${wanSites}, ${locatorHostsPerSite}, false, true)" ncf;

THREADGROUP peer
  totalThreads = fcn ${wanSites} * ${peerHostsPerSite}
               * ${peerVMsPerHost} * ${peerThreadsPerVM} ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
                 (\"peer\", ${wanSites}, ${peerHostsPerSite}, false, true)" ncf;

hydra.VmPrms-extraVMArgs =
  fcn "hydra.TestConfigFcns.duplicate
      (\"-Dnone\",
          ${locatorHostsPerSite} * ${wanSites}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
      (\"-Xms${peerMem} -Xmx${peerMem}\",
          ${peerHostsPerSite} * ${wanSites}, true)" ncf
  ;
