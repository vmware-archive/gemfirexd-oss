hydra.BridgePrms-names = bridge;

hydra.BridgePrms-maxThreads = ONEOF default 16 FOENO; // selector control

hydra.PoolPrms-names    = brloader;
hydra.PoolPrms-contactAlgorithm = hydra.EdgeHelper
                                   getRandomEndpointsInWanHctSite;
hydra.PoolPrms-subscriptionEnabled = true;
hydra.PoolPrms-readTimeout = 20000;

hydra.CachePrms-names = bridge edge;

hydra.RegionPrms-names         = bridge      edge;
hydra.RegionPrms-poolName   = none        brloader;
hydra.RegionPrms-enableGateway = true        false;
hydra.RegionPrms-dataPolicy    = replicate   default;
hydra.RegionPrms-scope         = dack        local;

wan.CacheServerPrms-cacheConfig = bridge;
wan.CacheServerPrms-regionConfig = bridge;
wan.CacheServerPrms-bridgeConfig = bridge;

wan.CacheClientPrms-cacheConfig = edge;
wan.CacheClientPrms-regionConfig = edge;

wan.CacheClientPrms-secToWaitForQueue = 2400;

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
