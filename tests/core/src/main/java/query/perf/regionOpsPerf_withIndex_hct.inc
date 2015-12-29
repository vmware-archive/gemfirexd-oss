include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/performance.inc;
include $JTESTS/hydraconfig/topology_hct.inc;
include $JTESTS/smoketest/perf/statmon.inc;

hydra.CachePrms-names = datastore regionOps;

hydra.RegionPrms-names         = datastore   regionOps;

hydra.RegionPrms-poolName      = none        pool;
hydra.RegionPrms-scope         = default     local;

hydra.BridgePrms-names = datastore;

hydra.PoolPrms-names = pool;
hydra.PoolPrms-contactAlgorithm = hydra.EdgeHelper getRoundRobinEndpoints;
hydra.PoolPrms-contactNum = 1;
hydra.PoolPrms-subscriptionEnabled = true;
hydra.PoolPrms-threadLocalConnections = true;

hydra.VmPrms-extraVMArgs = "-Xms256m -Xmx256m";
hydra.VmPrms-extraVMArgsSUN = -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC;
hydra.VmPrms-extraClassPaths = $GEMFIRE/lib/antlr.jar;

INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openStatisticsTask
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          hydra.ConfigPrms-cacheConfig = datastore
          hydra.ConfigPrms-regionConfig = datastore
          hydra.ConfigPrms-bridgeConfig = datastore
          threadgroups = datastore
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          hydra.ConfigPrms-cacheConfig = regionOps
          hydra.ConfigPrms-regionConfig = regionOps
          threadgroups = regionOps
          ;
INITTASK  taskClass = cacheperf.gemfire.query.QueryPerfClient taskMethod = indexDataTask
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
THREADGROUP regionOps
    totalThreads = fcn ${edgeHosts} * ${edgeVMsPerHost}
                                    * ${edgeThreadsPerVM} ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"edge\", ${edgeHosts}, true)" ncf;

distcache.gemfire.GemFireCachePrms-regionName = ${regionName};
cacheperf.CachePerfPrms-objectType = objects.PRPortfolio;

hydra.RegionPrms-valueConstraint = objects.PRPortfolio;
cacheperf.CachePerfPrms-maxKeys = 200000;

cacheperf.CachePerfPrms-batchTerminatorMethod     = terminateOnBatchSeconds;
cacheperf.CachePerfPrms-batchSeconds              = 360;
cacheperf.CachePerfPrms-warmupTerminatorMethod    = terminateOnTrimSeconds;
cacheperf.CachePerfPrms-warmupTerminatorFrequency = 10 seconds;
cacheperf.CachePerfPrms-trimSeconds               = 60;
cacheperf.CachePerfPrms-taskTerminatorMethod      = terminateOnTotalSeconds;
cacheperf.CachePerfPrms-taskTerminatorFrequency   = 10 seconds;
cacheperf.CachePerfPrms-workSeconds               = 180;

hydra.Prms-totalTaskTimeSec = 259200;  // this test is workload based
hydra.Prms-maxResultWaitSec = 600;
hydra.log.LogPrms-file_logLevel          = info;

cacheperf.CachePerfPrms-queryIndex =
        "
        ID
        "
        "
        status
        ";
cacheperf.CachePerfPrms-queryFromClause =
        "
        /${regionName} pf, pf.positions.values pos
        "
        "/${regionName} pf";