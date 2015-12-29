include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/performance.inc;
include $JTESTS/hydraconfig/topology_p2p_1_locator.inc;

hydra.Prms-totalTaskTimeSec = 259200;  // workload-based
hydra.Prms-maxResultWaitSec = 600;

//------------------------------------------------------------------------------
// Threadgroups
//------------------------------------------------------------------------------

THREADGROUP locator
  totalThreads = fcn
                 ${locatorHosts} * ${locatorVMsPerHost} * ${locatorThreadsPerVM}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames
                      (\"locator\", ${locatorHosts}, true)"
                 ncf;
THREADGROUP peer
    totalThreads = fcn
                   ${peerHosts} * ${peerVMsPerHost} * ${peerThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"peer\", ${peerHosts}, true)"
                   ncf;
//------------------------------------------------------------------------------
// Tasks
//------------------------------------------------------------------------------

INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = createAndStartLocatorTask    
          threadGroups = locator
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openStatisticsTask
          threadGroups = peer
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          hydra.ConfigPrms-cacheConfig = cache
          hydra.ConfigPrms-regionConfig = region
          threadGroups = peer
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = assignBucketsTask
          threadGroups = peer
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeCacheTask
          threadGroups = peer
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeStatisticsTask
          threadGroups = peer
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = reportTrimIntervalsTask
          threadGroups = peer
          ;

//------------------------------------------------------------------------------

cacheperf.CachePerfPrms-batchTerminatorMethod     = terminateOnBatchSeconds;
cacheperf.CachePerfPrms-batchSeconds              = 300;
cacheperf.CachePerfPrms-warmupTerminatorMethod    = terminateOnTrimSeconds;
cacheperf.CachePerfPrms-warmupTerminatorFrequency = 10 seconds;
cacheperf.CachePerfPrms-trimSeconds               = 60;
cacheperf.CachePerfPrms-taskTerminatorMethod      = terminateOnTotalSeconds;
cacheperf.CachePerfPrms-taskTerminatorFrequency   = 10 seconds;
cacheperf.CachePerfPrms-workSeconds               = 180;

//------------------------------------------------------------------------------
// Cache and region
//------------------------------------------------------------------------------

hydra.CachePrms-names = cache;
hydra.RegionPrms-names = region;
hydra.RegionPrms-scope = dack;
hydra.RegionPrms-dataPolicy = replicate;

//------------------------------------------------------------------------------
// Data
//------------------------------------------------------------------------------

cacheperf.CachePerfPrms-maxKeys    = fcn ${maxKeys} * ${peerHosts} * ${peerVMsPerHost} ncf;
cacheperf.CachePerfPrms-keyType    = java.lang.Long;
cacheperf.CachePerfPrms-objectType = objects.ArrayOfByte;
objects.ArrayOfBytePrms-size       = ${objectSize};

//------------------------------------------------------------------------------
// Versioning (allows running current tests against old builds)
//------------------------------------------------------------------------------

hydra.ClientPrms-versionNames = version;

hydra.VersionPrms-names   = version;
hydra.VersionPrms-version = ${version};

//------------------------------------------------------------------------------
// Performance
//------------------------------------------------------------------------------

hydra.VmPrms-extraVMArgs =
  fcn "hydra.TestConfigFcns.duplicateString(\"-Dnone\", ${locatorHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicateString(\"-Xms${heapMB}m -Xmx${heapMB}m -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC -XX:CMSInitiatingOccupancyFraction=33\", ${peerHosts}, true)" ncf
  ;

hydra.GemFirePrms-conserveSockets = ${conserveSockets};

perffmwk.PerfReportPrms-statisticsSpecification = $JTESTS/cacheperf/gemfire/specs/creates.spec;
