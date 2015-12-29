include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/performance.inc;
include $JTESTS/hydraconfig/topology_hct.inc;
include $JTESTS/smoketest/perf/statmon.inc;

hydra.CachePrms-names = datastore query;
hydra.CachePrms-pdxReadSerialized = true;
hydra.RegionPrms-names         = datastore   query;

hydra.RegionPrms-poolName      = none        pool;
hydra.RegionPrms-scope         = default     local;

hydra.BridgePrms-names = datastore;

hydra.PoolPrms-names = pool;
hydra.PoolPrms-contactAlgorithm = hydra.EdgeHelper getRoundRobinEndpoints;
hydra.PoolPrms-contactNum = 1;
hydra.PoolPrms-subscriptionEnabled = true;
hydra.PoolPrms-threadLocalConnections = true;

hydra.VmPrms-extraVMArgs = "-Xms1024m -Xmx1024m";
hydra.VmPrms-extraVMArgsSUN = -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC;
hydra.VmPrms-extraClassPaths = $GEMFIRE/lib/antlr.jar;

INITTASK  taskClass = cacheperf.gemfire.query.QueryPerfClient taskMethod = openStatisticsTask
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          hydra.ConfigPrms-cacheConfig = datastore
          hydra.ConfigPrms-regionConfig = datastore
          hydra.ConfigPrms-bridgeConfig = datastore
          threadgroups = datastore
          ;
INITTASK  taskClass = cacheperf.gemfire.query.QueryPerfClient taskMethod = HydraTask_attachQueryObserver
          hydra.ConfigPrms-cacheConfig = datastore
          hydra.ConfigPrms-regionConfig = datastore
          hydra.ConfigPrms-bridgeConfig = datastore
          threadgroups = datastore
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          hydra.ConfigPrms-cacheConfig = query
          hydra.ConfigPrms-regionConfig = query
          threadgroups = query
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = createDataTask
          batch
          cacheperf.CachePerfPrms-keyAllocation = ownKeys
          cacheperf.CachePerfPrms-warmupTerminatorMethod = none
          cacheperf.CachePerfPrms-taskTerminatorMethod = terminateOnMaxKey
          cacheperf.CachePerfPrms-taskTerminatorFrequency = 1 iterations
          threadgroups = datastore
          ;

CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeCacheTask
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeStatisticsTask
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = reportTrimIntervalsTask
          ;

THREADGROUP datastore
    totalThreads = fcn ${bridgeHosts} * ${bridgeVMsPerHost}
                                      * ${bridgeThreadsPerVM} ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"bridge\", ${bridgeHosts}, true)" ncf;
THREADGROUP query
    totalThreads = fcn ${edgeHosts} * ${edgeVMsPerHost}
                                    * ${edgeThreadsPerVM} ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"edge\", ${edgeHosts}, true)" ncf;

distcache.gemfire.GemFireCachePrms-regionName = ${regionName};
cacheperf.CachePerfPrms-objectType = objects.PRPortfolio;

hydra.RegionPrms-valueConstraint = objects.PRPortfolio;
cacheperf.CachePerfPrms-maxKeys = 10000;

cacheperf.CachePerfPrms-batchTerminatorMethod     = terminateOnBatchSeconds;
cacheperf.CachePerfPrms-batchSeconds              = 360;
cacheperf.CachePerfPrms-warmupTerminatorMethod    = terminateOnTrimSeconds;
cacheperf.CachePerfPrms-warmupTerminatorFrequency = 10 seconds;
cacheperf.CachePerfPrms-trimSeconds               = 60;
cacheperf.CachePerfPrms-taskTerminatorMethod      = terminateOnTotalSeconds;
cacheperf.CachePerfPrms-taskTerminatorFrequency   = 10 seconds;
cacheperf.CachePerfPrms-workSeconds               = 1800;
hydra.PoolPrms-readTimeout            = 10000000;
hydra.Prms-totalTaskTimeSec = 259200;  // this test is workload based
hydra.Prms-maxResultWaitSec = 1800;
hydra.log.LogPrms-file_logLevel          = info;
perffmwk.PerfReportPrms-statisticsSpecification = $JTESTS/smoketest/query/query001.spec;
