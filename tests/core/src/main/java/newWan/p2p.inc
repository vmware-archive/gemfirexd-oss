hydra.CachePrms-names = peer;

hydra.RegionPrms-names         = peer;
hydra.RegionPrms-regionName		= peerRegion;
hydra.RegionPrms-dataPolicy    = replicate;
hydra.RegionPrms-scope         = dack;
hydra.RegionPrms-cacheListeners	 = util.SilenceListener;

hydra.ConfigPrms-cacheConfig = peer;
hydra.ConfigPrms-regionConfig = peer;

newWan.WANTestPrms-senderQueueRegionListener = none;

hydra.GemFirePrms-conserveSockets = false;

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
      (\"-Dnone\", ${locatorHostsPerSite} * ${wanSites}, true)" ncf  ,
  fcn "hydra.TestConfigFcns.duplicate
      (\"-Xms${peerMem} -Xmx${peerMem}\", ${peerHostsPerSite} * ${wanSites}, true)" ncf;
