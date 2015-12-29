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
THREADGROUP feed
  totalThreads = fcn
                 ${feedHosts} * ${feedVMsPerHost} * ${feedThreadsPerVM}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames
                      (\"feed\", ${feedHosts}, true)"
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
          threadGroups = feed, bridge, edge
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          hydra.ConfigPrms-cacheConfig = feed
          hydra.ConfigPrms-regionConfig = feed
          threadGroups = feed
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          hydra.ConfigPrms-cacheConfig = bridge
          hydra.ConfigPrms-regionConfig = bridge
          hydra.ConfigPrms-bridgeConfig = bridge
          threadGroups = bridge
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          distcache.gemfire.GemFireCachePrms-interestResultPolicy = none
          hydra.ConfigPrms-cacheConfig = edge
          hydra.ConfigPrms-regionConfig = edge
          threadGroups = edge
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = assignBucketsTask
          threadGroups = bridge
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = registerInterestRegexAllTask
          threadGroups = edge
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeCacheTask
          threadGroups = feed
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeCacheTask
          threadGroups = bridge
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeCacheTask
          threadGroups = edge
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeStatisticsTask
          threadGroups = feed, bridge, edge
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = reportTrimIntervalsTask
          threadGroups = feed, bridge, edge
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
  fcn "hydra.TestConfigFcns.duplicate(\"ds\",    ${feedHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"ds\",    ${bridgeHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"loner\", ${edgeHosts})" ncf;

//------------------------------------------------------------------------------
// Cache and region
//------------------------------------------------------------------------------

hydra.CachePrms-names = feed bridge edge;

hydra.RegionPrms-names          = feed      bridge    edge;
hydra.RegionPrms-cacheListeners = none, none, cacheperf.gemfire.LatencyListener;
hydra.RegionPrms-dataPolicy     = empty     replicate empty;
hydra.RegionPrms-enableSubscriptionConflation = false;
hydra.RegionPrms-interestPolicy = default   default   all;
hydra.RegionPrms-poolName       = none      none      pool;
hydra.RegionPrms-scope          = dack      dack      local;

hydra.PoolPrms-names                  = pool;
hydra.PoolPrms-freeConnectionTimeout  = 10000000;
hydra.PoolPrms-subscriptionEnabled           = true;
hydra.PoolPrms-readTimeout            = 10000000;
hydra.PoolPrms-threadLocalConnections = true;

hydra.BridgePrms-names = bridge;

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
  fcn "hydra.TestConfigFcns.duplicateString(\"-Xms250m -Xmx250m\", ${feedHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicateString(\"-Xms${heapMB}m -Xmx${heapMB}m -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC -XX:CMSInitiatingOccupancyFraction=33\", ${bridgeHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicateString(\"-Xms250m -Xmx250m\", ${edgeHosts}, true)" ncf
  ;

hydra.GemFirePrms-conserveSockets = ${conserveSockets};

perffmwk.PerfReportPrms-statisticsSpecification = $JTESTS/cacheperf/gemfire/specs/createupdateEvents.spec;
