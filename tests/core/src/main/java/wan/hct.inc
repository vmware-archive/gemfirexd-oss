hydra.BridgePrms-names = bridge;

hydra.BridgePrms-maxThreads = ONEOF default 16 FOENO; // selector control

hydra.PoolPrms-names    = brloader     ;
hydra.PoolPrms-contactAlgorithm = hydra.EdgeHelper
                                   getRandomEndpointsInWanHctSite;
hydra.PoolPrms-subscriptionEnabled = true;
hydra.PoolPrms-readTimeout = 30000;

hydra.CachePrms-names = bridge edge;

hydra.RegionPrms-names         = bridge      edge;
hydra.RegionPrms-poolName   = none        brloader;
hydra.RegionPrms-enableGateway = true        false;
hydra.RegionPrms-dataPolicy    = replicate   default;
hydra.RegionPrms-scope         = dack        local;
hydra.RegionPrms-cacheListeners = util.SummaryLogListener;

wan.CacheServerPrms-cacheConfig = bridge;
wan.CacheServerPrms-regionConfig = bridge;
wan.CacheServerPrms-bridgeConfig = bridge;

wan.CacheClientPrms-cacheConfig = edge;
wan.CacheClientPrms-regionConfig = edge;

wan.CacheClientPrms-secToWaitForQueue = 2400;
wan.CacheClientPrms-numPutAllEntries = 5;   // max per perAll

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
