include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/performance.inc;
include $JTESTS/hydraconfig/topology_p2p_1.inc;
include $JTESTS/smoketest/perf/statmon.inc;

hydra.CachePrms-names = peer;
hydra.CachePrms-pdxReadSerialized = true;

hydra.RegionPrms-names         = peer;

hydra.VmPrms-extraVMArgs = "-Xms1024m -Xmx1024m";
hydra.VmPrms-extraVMArgsSUN = -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC;
hydra.VmPrms-extraClassPaths = $GEMFIRE/lib/antlr.jar;

INITTASK  taskClass = cacheperf.gemfire.query.QueryPerfClient taskMethod = openStatisticsTask
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          hydra.ConfigPrms-cacheConfig = peer
          hydra.ConfigPrms-regionConfig = peer
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          hydra.ConfigPrms-cacheConfig = peer
          hydra.ConfigPrms-regionConfig = peer
          ;
INITTASK  taskClass = cacheperf.gemfire.query.QueryPerfClient taskMethod = HydraTask_attachQueryObserver
          hydra.ConfigPrms-cacheConfig = peer
          hydra.ConfigPrms-regionConfig = peer
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = createDataTask
          batch
          cacheperf.CachePerfPrms-keyAllocation = ownKeys
          cacheperf.CachePerfPrms-warmupTerminatorMethod = none
          cacheperf.CachePerfPrms-taskTerminatorMethod = terminateOnMaxKey
          cacheperf.CachePerfPrms-taskTerminatorFrequency = 1 iterations
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeCacheTask
          ;
CLOSETASK taskClass = cacheperf.gemfire.query.QueryPerfClient taskMethod = closeStatisticsTask
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = reportTrimIntervalsTask
          ;

distcache.gemfire.GemFireCachePrms-regionName = ${regionName};

cacheperf.CachePerfPrms-maxKeys = 10000;
cacheperf.CachePerfPrms-objectType = objects.PRPortfolio;

hydra.RegionPrms-valueConstraint = objects.PRPortfolio;

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
