include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/performance.inc;
include $JTESTS/hydraconfig/topology_p2p_2_locator.inc;

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
THREADGROUP data
    totalThreads = fcn
                   ${dataHosts} * ${dataVMsPerHost} * ${dataThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"data\", ${dataHosts}, true)"
                   ncf;
THREADGROUP feed
    totalThreads = fcn
                   ${feedHosts} * ${feedVMsPerHost} * ${feedThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"feed\", ${feedHosts}, true)"
                   ncf;

//------------------------------------------------------------------------------
// Tasks
//------------------------------------------------------------------------------

INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = createAndStartLocatorTask    
          threadGroups = locator
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openStatisticsTask
          threadGroups = data, feed
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          hydra.ConfigPrms-cacheConfig = data
          hydra.ConfigPrms-regionConfig = data
          threadGroups = data
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          hydra.ConfigPrms-cacheConfig = feed
          hydra.ConfigPrms-regionConfig = feed
          threadGroups = feed
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = assignBucketsTask
          threadGroups = data
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeCacheTask
          threadGroups = data, feed
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeStatisticsTask
          threadGroups = data, feed
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = reportTrimIntervalsTask
          threadGroups = data, feed
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

hydra.CachePrms-names               = data      feed;

hydra.RegionPrms-names              = data      feed;
hydra.RegionPrms-dataPolicy         = replicate empty;
hydra.RegionPrms-scope              = dack      dack;

//------------------------------------------------------------------------------
// Data
//------------------------------------------------------------------------------

cacheperf.CachePerfPrms-maxKeys    = fcn ${maxKeys} * ${dataHosts} * ${dataVMsPerHost} ncf;
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
  fcn "hydra.TestConfigFcns.duplicateString(\"-Xms${heapMB}m -Xmx${heapMB}m -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC -XX:CMSInitiatingOccupancyFraction=33\", ${dataHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicateString(\"-Xms250m -Xmx250m\", ${feedHosts}, true)" ncf
  ;

hydra.GemFirePrms-conserveSockets = ${conserveSockets};

perffmwk.PerfReportPrms-statisticsSpecification = $JTESTS/cacheperf/gemfire/specs/creates.spec;
