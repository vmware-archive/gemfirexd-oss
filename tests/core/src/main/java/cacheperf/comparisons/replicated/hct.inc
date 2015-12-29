include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/performance.inc;
include $JTESTS/hydraconfig/topology_hct_locator.inc;

THREADGROUP locator
  totalThreads = fcn
                 ${locatorHosts} * ${locatorVMsPerHost} * ${locatorThreadsPerVM}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames
                      (\"locator\", ${locatorHosts}, true)"
                 ncf;
THREADGROUP bridge
  totalThreads = fcn
                 ${bridgeHosts} * ${bridgeVMsPerHost} * ${bridgeThreadsPerVM}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames
                      (\"bridge\", ${bridgeHosts}, true)"
                 ncf;
THREADGROUP edge
  totalThreads = fcn
                 ${edgeHosts} * ${edgeVMsPerHost} * ${edgeThreadsPerVM}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames
                      (\"edge\", ${edgeHosts}, true)"
                 ncf;

hydra.Prms-totalTaskTimeSec = 259200;  // workload-based
hydra.Prms-maxResultWaitSec = 600;

//------------------------------------------------------------------------------
// Tasks
//------------------------------------------------------------------------------

INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = createAndStartLocatorTask    
          threadGroups = locator
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openStatisticsTask
          threadGroups = bridge, edge
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          hydra.ConfigPrms-cacheConfig = bridge
          hydra.ConfigPrms-regionConfig = bridge
          hydra.ConfigPrms-bridgeConfig = bridge
          threadGroups = bridge
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          hydra.ConfigPrms-cacheConfig = edge
          hydra.ConfigPrms-regionConfig = edge
          threadGroups = edge
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = assignBucketsTask
          threadGroups = bridge
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeCacheTask
          threadGroups = bridge
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeCacheTask
          threadGroups = edge
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeStatisticsTask
          threadGroups = bridge, edge
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = reportTrimIntervalsTask
          threadGroups = bridge, edge
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

hydra.CachePrms-names = bridge edge;

hydra.RegionPrms-names         = bridge    edge;
hydra.RegionPrms-dataPolicy    = replicate empty;
hydra.RegionPrms-poolName      = none      pool;
hydra.RegionPrms-scope         = dack      local;

hydra.PoolPrms-names                  = pool;
hydra.PoolPrms-freeConnectionTimeout  = 10000000;
hydra.PoolPrms-readTimeout            = 10000000;
hydra.PoolPrms-subscriptionEnabled    = false;
hydra.PoolPrms-threadLocalConnections = true;

hydra.BridgePrms-names                = bridge;

//------------------------------------------------------------------------------
// Data
//------------------------------------------------------------------------------

cacheperf.CachePerfPrms-maxKeys    = fcn ${maxKeys} * ${bridgeHosts} * ${bridgeVMsPerHost} ncf;
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
  fcn "hydra.TestConfigFcns.duplicateString(\"-Xms${heapMB}m -Xmx${heapMB}m -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC -XX:CMSInitiatingOccupancyFraction=33\", ${bridgeHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicateString(\"-Xms250m -Xmx250m\", ${edgeHosts}, true)" ncf
  ;

hydra.GemFirePrms-conserveSockets = ${conserveSockets};

perffmwk.PerfReportPrms-statisticsSpecification = $JTESTS/cacheperf/gemfire/specs/creates.spec;
