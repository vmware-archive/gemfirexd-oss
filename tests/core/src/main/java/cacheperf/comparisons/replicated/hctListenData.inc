include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/performance.inc;
include $JTESTS/hydraconfig/topology_3_locator.inc;

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
THREADGROUP listen
  totalThreads = fcn
                 ${listenHosts} * ${listenVMsPerHost} * ${listenThreadsPerVM}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames
                      (\"listen\", ${listenHosts}, true)"
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

//------------------------------------------------------------------------------
// Tasks
//------------------------------------------------------------------------------

INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = createAndStartLocatorTask    
          threadGroups = locator
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openStatisticsTask
          threadGroups = listen, bridge, edge
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          hydra.ConfigPrms-cacheConfig = bridge
          hydra.ConfigPrms-regionConfig = bridge
          hydra.ConfigPrms-bridgeConfig = bridge
          threadGroups = bridge
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          hydra.ConfigPrms-cacheConfig = listen
          hydra.ConfigPrms-regionConfig = listen
          hydra.ConfigPrms-bridgeConfig = listen
          threadGroups = listen
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = assignBucketsTask
          threadGroups = bridge
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          distcache.gemfire.GemFireCachePrms-interestResultPolicy = none
          hydra.ConfigPrms-cacheConfig = edge
          hydra.ConfigPrms-regionConfig = edge
          threadGroups = edge
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeCacheTask
          threadGroups = listen
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeCacheTask
          threadGroups = bridge
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeCacheTask
          threadGroups = edge
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeStatisticsTask
          threadGroups = listen, bridge, edge
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = reportTrimIntervalsTask
          threadGroups = listen, bridge, edge
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
// Distributed system
//------------------------------------------------------------------------------

hydra.GemFirePrms-distributedSystem =
  fcn "hydra.TestConfigFcns.duplicate(\"ds\",    ${locatorHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"ds\",    ${listenHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"ds\",    ${bridgeHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"loner\", ${edgeHosts})" ncf;

//------------------------------------------------------------------------------
// Cache and region
//------------------------------------------------------------------------------

hydra.CachePrms-names = listen bridge edge;

hydra.RegionPrms-names          = listen    bridge    edge;
hydra.RegionPrms-cacheListeners = cacheperf.gemfire.LatencyListener, none, none;
hydra.RegionPrms-dataPolicy     = empty     replicate empty;
hydra.RegionPrms-interestPolicy = all       default   default;
hydra.RegionPrms-poolName       = none      none      pool;
hydra.RegionPrms-scope          = dack      dack      local;

hydra.PoolPrms-names                  = pool;
hydra.PoolPrms-freeConnectionTimeout  = 10000000;
hydra.PoolPrms-readTimeout            = 10000000;
hydra.PoolPrms-threadLocalConnections = true;

hydra.BridgePrms-names = listen bridge;

//------------------------------------------------------------------------------
// Data
//------------------------------------------------------------------------------

cacheperf.CachePerfPrms-maxKeys    = fcn ${maxKeys} * ${bridgeHosts} * ${bridgeVMsPerHost} ncf;
cacheperf.CachePerfPrms-keyType    = java.lang.Long;
cacheperf.CachePerfPrms-objectType = objects.ArrayOfByte;
objects.ArrayOfBytePrms-size       = ${objectSize};
objects.ArrayOfBytePrms-encodeTimestamp = true;

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
  fcn "hydra.TestConfigFcns.duplicateString(\"-Xms250m -Xmx250m\", ${listenHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicateString(\"-Xms${heapMB}m -Xmx${heapMB}m -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC -XX:CMSInitiatingOccupancyFraction=33\", ${bridgeHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicateString(\"-Xms250m -Xmx250m\", ${edgeHosts}, true)" ncf
  ;

hydra.GemFirePrms-conserveSockets = ${conserveSockets};

perffmwk.PerfReportPrms-statisticsSpecification = $JTESTS/cacheperf/gemfire/specs/createupdateEvents.spec;
