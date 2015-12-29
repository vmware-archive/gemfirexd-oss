include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/performance.inc;
include $JTESTS/hydraconfig/topology_wan_p2p_2_locator.inc;
include $JTESTS/cacheperf/comparisons/newWan/wanSerial.inc;

hydra.Prms-totalTaskTimeSec = 259200;  // this test is workload based
hydra.Prms-maxResultWaitSec = 600;

hydra.Prms-testDescription += "Accessor put replicated data on datahost having serial gateway sender.";

//------------------------------------------------------------------------------
// Threadgroups
//------------------------------------------------------------------------------

THREADGROUP accessor
  totalThreads = fcn ${wanSites} * ${accessorHostsPerSite}
                                 * ${accessorVMsPerHost} * ${accessorThreadsPerVM} ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
                        (\"accessor\", ${wanSites}, ${accessorHostsPerSite}, false, true)" ncf;
                        
THREADGROUP datastore
  totalThreads = fcn ${wanSites} * ${dataHostsPerSite}
                                 * ${dataVMsPerHost} * ${dataThreadsPerVM} ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateDoubleSuffixedNames
                        (\"data\", ${wanSites}, ${dataHostsPerSite}, false, true)" ncf;
                        
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
          threadgroups = datastore, accessor
          ;                    
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          hydra.ConfigPrms-cacheConfig = datastore
          hydra.ConfigPrms-regionConfig = datastore
          threadgroups = datastore
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          hydra.ConfigPrms-cacheConfig = accessor
          hydra.ConfigPrms-regionConfig = accessor
          threadgroups = accessor
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
          threadgroups = accessor
          ;
TASK      taskClass = cacheperf.comparisons.newWan.NewWanPerfClient taskMethod = putDataGWSenderTask
          cacheperf.CachePerfPrms-keyAllocation = ownKey
          cacheperf.CachePerfPrms-isMainWorkload = true
          threadGroups = accessor
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeCacheTask
          threadGroups = accessor, datastore
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

hydra.CachePrms-names = datastore accessor;

hydra.RegionPrms-names          = datastore       accessor;
hydra.RegionPrms-cacheListeners = cacheperf.comparisons.newWan.WanLatencyListener, none;
hydra.RegionPrms-dataPolicy     = replicate       empty;
hydra.RegionPrms-gatewaySenderNames = sender;
hydra.RegionPrms-scope           = default;

hydra.GemFirePrms-remoteDistributedSystems =
     fcn "newWan.WANTest.generateAllRemoteDSNameLists (\"ds_\", ${wanSites}, ${locatorHostsPerSite})" ncf         
     ,
     none
     ; 

//------------------------------------------------------------------------------
// Data
//------------------------------------------------------------------------------
cacheperf.CachePerfPrms-maxKeys         = fcn ${wanSites} * ${accessorHostsPerSite} * ${accessorVMsPerHost} * ${accessorThreadsPerVM} ncf;

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
      (\"-Xms256m -Xmx256m\", ${locatorHostsPerSite} * ${wanSites}, true)" ncf  ,
  fcn "hydra.TestConfigFcns.duplicate
      (\"-Xms${dataMem} -Xmx${dataMem}\", ${dataHostsPerSite} * ${wanSites}, true)" ncf  ,
  fcn "hydra.TestConfigFcns.duplicate
      (\"-Xms256m -Xmx256m\", ${accessorHostsPerSite} * ${wanSites}, true)" ncf;        

hydra.VmPrms-extraVMArgsSUN = -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC;

perffmwk.PerfReportPrms-statisticsSpecification = $JTESTS/cacheperf/comparisons/newWan/newWan.spec;
