include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/performance.inc;
include $JTESTS/hydraconfig/topology_wan_p2p_2_locator.inc;
include $JTESTS/cacheperf/comparisons/newWan/wanSerial.inc;

hydra.Prms-totalTaskTimeSec = 259200;  // this test is workload based
hydra.Prms-maxResultWaitSec = 600;

//------------------------------------------------------------------------------
// Threadgroups
//------------------------------------------------------------------------------
THREADGROUP datastore
  totalThreads = fcn ${wanSites} * ${${A}HostsPerSite}
                   * ${${A}VMsPerHost} * ${${A}ThreadsPerVM} ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
              (\"${A}\", ${wanSites}, ${${A}HostsPerSite}, false, true)"
                 ncf;

THREADGROUP edge
  totalThreads = fcn ${wanSites} * ${${B}HostsPerSite}
                   * ${${B}VMsPerHost} * ${${B}ThreadsPerVM} ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
              (\"${B}\", ${wanSites}, ${${B}HostsPerSite}, false, true)"
                 ncf;

THREADGROUP locator
  totalThreads = fcn ${wanSites} * ${locatorHostsPerSite}
                                 * ${locatorVMsPerHost} * ${locatorThreadsPerVM} ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
                        (\"locator\", ${wanSites}, ${locatorHostsPerSite}, false, true)" ncf;
                        
//------------------------------------------------------------------------------
// Tasks
//------------------------------------------------------------------------------

INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = createAndStartLocatorTask
          threadGroups = locator
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openStatisticsTask
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = createGatewaySenderIdsTask  
          hydra.ConfigPrms-gatewaySenderConfig = sender        
          threadgroups = datastore 
          ;                    
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          hydra.ConfigPrms-cacheConfig = datastore
          hydra.ConfigPrms-regionConfig = datastore
          hydra.ConfigPrms-bridgeConfig = bridge
          threadgroups = datastore
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          hydra.ConfigPrms-cacheConfig = edge
          hydra.ConfigPrms-regionConfig = edge
          threadGroups = edge   
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = initGatewaySenderTask
          hydra.ConfigPrms-gatewaySenderConfig = sender
          threadgroups = datastore
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = initGatewayReceiverTask
          hydra.ConfigPrms-gatewayReceiverConfig = receiver
          threadgroups = datastore
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = createDataTask
          batch
          cacheperf.CachePerfPrms-keyAllocation = ownKeys
          cacheperf.CachePerfPrms-warmupTerminatorMethod = none               
          cacheperf.CachePerfPrms-taskTerminatorMethod = terminateOnMaxKey
          cacheperf.CachePerfPrms-taskTerminatorFrequency = 1 iterations
          threadgroups = edge
          ;
TASK      taskClass = cacheperf.comparisons.newWan.NewWanPerfClient taskMethod = putDataGWSenderTask
          cacheperf.CachePerfPrms-keyAllocation = ownKey
          cacheperf.CachePerfPrms-isMainWorkload = true
          threadGroups = edge
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeCacheTask
          threadGroups = edge, datastore
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeStatisticsTask
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = reportTrimIntervalsTask
          ;

//------------------------------------------------------------------------------
cacheperf.CachePerfPrms-batchTerminatorMethod     = terminateOnBatchSeconds;
cacheperf.CachePerfPrms-batchSeconds              = 360;
cacheperf.CachePerfPrms-warmupTerminatorMethod    = terminateOnTrimSeconds;
cacheperf.CachePerfPrms-warmupTerminatorFrequency = 10 seconds;
cacheperf.CachePerfPrms-trimSeconds               = ${trimSeconds};
cacheperf.CachePerfPrms-taskTerminatorMethod      = terminateOnTotalSeconds;
cacheperf.CachePerfPrms-taskTerminatorFrequency   = 10 seconds;
cacheperf.CachePerfPrms-workSeconds               = 240;


//------------------------------------------------------------------------------
// Cache and region
//------------------------------------------------------------------------------

hydra.CachePrms-names = datastore   edge;

hydra.RegionPrms-names          = datastore   edge;
hydra.RegionPrms-poolName       = none        brloader;
hydra.RegionPrms-dataPolicy     = replicate   default;
hydra.RegionPrms-scope          = default     local;
hydra.RegionPrms-gatewaySenderNames = sender  none;
hydra.RegionPrms-cacheListeners = cacheperf.comparisons.newWan.WanLatencyListener, none;

hydra.PoolPrms-names                  = brloader;
hydra.PoolPrms-contactAlgorithm       = hydra.EdgeHelper getRandomEndpointsInWanHctSite;
hydra.PoolPrms-subscriptionEnabled    = true;

hydra.BridgePrms-names = bridge;

hydra.GemFirePrms-remoteDistributedSystems =
     fcn "newWan.WANTest.generateAllRemoteDSNameLists (\"ds_\", ${wanSites}, ${locatorHostsPerSite})" ncf         
     ,
     none
     ; 
hydra.GemFirePrms-distributedSystem  =
  fcn "hydra.TestConfigFcns.generateNamesRepeatedly (\"ds_\", ${wanSites}, ${locatorHostsPerSite}, false)" ncf
  fcn "hydra.TestConfigFcns.generateNamesRepeatedly (\"ds_\", ${wanSites}, ${dataHostsPerSite}, false)"  ncf
  fcn "hydra.TestConfigFcns.duplicate   (\"loner\", ${wanSites} * ${edgeHostsPerSite})" ncf
  ;
  
//------------------------------------------------------------------------------
// Data
//------------------------------------------------------------------------------
cacheperf.CachePerfPrms-maxKeys         = fcn ${wanSites} * ${edgeHostsPerSite} * ${edgeVMsPerHost} * ${edgeThreadsPerVM} ncf;

cacheperf.comparisons.newWan.NewWanPerfPrms-inputPutsPerSec           = ${putsPerSec};
                                                                                            
//------------------------------------------------------------------------------
// Hostname mapping
//------------------------------------------------------------------------------     
hydra.HostPrms-names =
  masterhost
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locatorhost\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${A}host\", ${wanSites}, ${${A}HostsPerSite}, false)"
  ncf
  ;
  
hydra.GemFirePrms-hostNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locatorhost\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${A}host\", ${wanSites}, ${${A}HostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locatorhost\", ${wanSites}, ${${B}HostsPerSite}, false)"
  ncf
  ;
  
hydra.VmPrms-hostNames =
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locatorhost\", ${wanSites}, ${locatorHostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"${A}host\", ${wanSites}, ${${A}HostsPerSite}, false)"
  ncf
  fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
      (\"locatorhost\", ${wanSites}, ${${B}HostsPerSite}, false)"
  ncf
  ;
  

   
//------------------------------------------------------------------------------
// Correct for clock skew to get accurate latencies.
//------------------------------------------------------------------------------
hydra.timeserver.TimeServerPrms-clockSkewUpdateFrequencyMs = 1000;
hydra.timeserver.TimeServerPrms-clockSkewMaxLatencyMs      = 25;
    
//------------------------------------------------------------------------------
// Performance
//------------------------------------------------------------------------------     

hydra.VmPrms-extraVMArgs =
  fcn "hydra.TestConfigFcns.duplicate
      (\"-Xms256m -Xmx256m\", ${locatorHostsPerSite} * ${wanSites}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
      (\"-Xms${dataMem} -Xmx${dataMem}\", ${dataHostsPerSite} * ${wanSites}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
      (\"-Xms256m -Xmx256m\", ${edgeHostsPerSite} * ${wanSites}, true)" ncf
  ;
          
hydra.VmPrms-extraVMArgsSUN = -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC;

perffmwk.PerfReportPrms-statisticsSpecification = $JTESTS/cacheperf/comparisons/newWan/newWan.spec;
