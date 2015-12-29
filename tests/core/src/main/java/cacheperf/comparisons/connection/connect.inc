include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/performance.inc;
include $JTESTS/hydraconfig/topology_3_locator.inc;
include $JTESTS/smoketest/perf/statmon.inc;

cacheperf.comparisons.connection.ConnectionPrms-bounceVm = ${bounceVm};
cacheperf.comparisons.connection.ConnectionPrms-createCacheAndRegion = ${createCacheAndRegion};
cacheperf.comparisons.connection.ConnectionPrms-deleteSystemFiles = ${deleteSystemFiles};
cacheperf.comparisons.connection.ConnectionPrms-logTimes = ${logTimes};
cacheperf.comparisons.connection.ConnectionPrms-useCacheXml = ${useCacheXml};

hydra.GemFirePrms-logLevel = ${logLevel};
hydra.SecurityPrms-logLevel = ${logLevel};

cacheperf.CachePerfPrms-sleepMs = ${sleepMs};

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
THREADGROUP archiver
  totalThreads = fcn
                 ${archiverHosts} * ${archiverVMsPerHost} * ${archiverThreadsPerVM}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames
                      (\"archiver\", ${archiverHosts}, true)"
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
          threadgroups = archiver
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          hydra.ConfigPrms-cacheConfig = bridge
          hydra.ConfigPrms-regionConfig = bridge
          hydra.ConfigPrms-bridgeConfig = bridge
          threadGroups = bridge
          ;
TASK      taskClass = cacheperf.CachePerfClient taskMethod = statArchiverTask
          perffmwk.HistogramStatsPrms-enable = false // stats are sampled
          threadGroups = archiver
          ;
TASK      taskClass = cacheperf.comparisons.connection.ConnectionClient
          taskMethod = cycleConnectionOnceTask
          hydra.ConfigPrms-cacheConfig = edge
          hydra.ConfigPrms-regionConfig = edge
          hydra.ConfigPrms-poolConfig = pool
          perffmwk.HistogramStatsPrms-enable = false // uses statarchiver
          threadGroups = edge
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeCacheTask
          threadGroups = bridge
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeStatisticsTask
          threadgroups = archiver
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = reportTrimIntervalsTask
          threadgroups = archiver
          ;

//------------------------------------------------------------------------------
// Termination
//------------------------------------------------------------------------------

hydra.Prms-totalTaskTimeSec = fcn ${trimSeconds} + ${workSeconds} + 10 ncf;
hydra.Prms-maxResultWaitSec = fcn ${trimSeconds} + ${workSeconds} + 10 ncf;

// used by statarchiver
cacheperf.CachePerfPrms-batchTerminatorMethod     = terminateOnBatchSeconds;
cacheperf.CachePerfPrms-batchSeconds              = fcn ${trimSeconds} + ${workSeconds} + 10 ncf;
cacheperf.CachePerfPrms-warmupTerminatorMethod    = terminateOnTrimSeconds;
cacheperf.CachePerfPrms-warmupTerminatorFrequency = 10 seconds;
cacheperf.CachePerfPrms-trimSeconds               = ${trimSeconds};
cacheperf.CachePerfPrms-taskTerminatorMethod      = terminateOnTotalSeconds;
cacheperf.CachePerfPrms-taskTerminatorFrequency   = 10 seconds;
cacheperf.CachePerfPrms-workSeconds               = ${workSeconds};

//------------------------------------------------------------------------------
// Clients: ensure only 1 thread per edge
//------------------------------------------------------------------------------

hydra.VmPrms-type =
  fcn "hydra.TestConfigFcns.duplicate(\"server\", ${locatorHosts}, true)" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"server\", ${archiverHosts}, true)" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"server\", ${bridgeHosts}, true)" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"${vmType}\", ${edgeHosts}, true)" ncf
  ;

hydra.ClientPrms-vmThreads =
  fcn "hydra.TestConfigFcns.duplicate
                            (\"${locatorThreadsPerVM}\", ${locatorHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate
                            (\"${archiverThreadsPerVM}\", ${archiverHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate
                            (\"${bridgeThreadsPerVM}\", ${bridgeHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate
                            (\"1\", ${edgeHosts})" ncf
  ;

//------------------------------------------------------------------------------
// Distributed system
//------------------------------------------------------------------------------

hydra.GemFirePrms-distributedSystem =
  fcn "hydra.TestConfigFcns.duplicate (\"ds\",    ${locatorHosts})"  ncf
  fcn "hydra.TestConfigFcns.duplicate (\"none\",  ${archiverHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate (\"ds\",    ${bridgeHosts})"   ncf
  fcn "hydra.TestConfigFcns.duplicate (\"loner\", ${edgeHosts})"     ncf
  ;

//------------------------------------------------------------------------------
// Cache and region
//------------------------------------------------------------------------------

hydra.CachePrms-names = bridge edge;

hydra.RegionPrms-names         = bridge     edge;
hydra.RegionPrms-scope         = default    local;
hydra.RegionPrms-dataPolicy    = partition  empty;
hydra.RegionPrms-partitionName = partition  none;
hydra.RegionPrms-poolName      = none       pool;

hydra.PartitionPrms-names = partition;

hydra.BridgePrms-names = bridge;

hydra.PoolPrms-names = pool;
hydra.PoolPrms-contactNum = 1;
hydra.PoolPrms-minConnections = 0;
hydra.PoolPrms-subscriptionEnabled = false;
hydra.PoolPrms-threadLocalConnections = false;

//------------------------------------------------------------------------------
// Data
//------------------------------------------------------------------------------

cacheperf.CachePerfPrms-maxKeys = 1; // not used

//------------------------------------------------------------------------------
// Performance
//------------------------------------------------------------------------------

perffmwk.PerfReportPrms-statisticsSpecification = $JTESTS/cacheperf/comparisons/connection/connect.spec;
