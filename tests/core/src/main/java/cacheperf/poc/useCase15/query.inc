include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/performance.inc;
include $JTESTS/hydraconfig/topology_hct_locator.inc;
include $JTESTS/smoketest/perf/statmon.inc;

hydra.Prms-testDescription = "Empty edges query indexed entries in partitioned datastore.  Entries are Serializable objects.";


hydra.CachePrms-names          = datastore   query;

hydra.RegionPrms-names         = datastore   query;
hydra.RegionPrms-dataPolicy    = partitioned empty;
hydra.RegionPrms-partitionName = datastore   none;
hydra.RegionPrms-poolName      = none        pool;
hydra.RegionPrms-scope         = default     local;

hydra.BridgePrms-names = datastore;

hydra.PoolPrms-names = edge;
hydra.PoolPrms-subscriptionEnabled = true;
hydra.PoolPrms-threadLocalConnections = true;

hydra.PartitionPrms-names          = datastore;
hydra.PartitionPrms-localMaxMemory = default;
//hydra.PartitionPrms-totalNumBuckets = 499;

hydra.VmPrms-extraVMArgs +=
  fcn "hydra.TestConfigFcns.duplicate
       (\"-Xms128m -Xmx128m\", ${locatorHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
       (\"-Xms1600m -Xmx1600m\", ${bridgeHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
       (\"-Xms256m -Xmx256m\", ${edgeHosts}, true)" ncf
  ;
hydra.VmPrms-extraVMArgs += -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC;
hydra.VmPrms-extraClassPaths = $GEMFIRE/lib/antlr.jar;

INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = createAndStartLocatorTask
          threadgroups = locator
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openStatisticsTask
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          hydra.ConfigPrms-cacheConfig = datastore
          hydra.ConfigPrms-regionConfig = datastore
          hydra.ConfigPrms-bridgeConfig = datastore
          threadgroups = datastore, indexer
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
          threadgroups = datastore, indexer
          ;
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = indexDataTask
          threadgroups = indexer
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeCacheTask
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeStatisticsTask
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = reportTrimIntervalsTask
          ;

THREADGROUP locator
    totalThreads = fcn ${locatorHosts} * ${locatorVMsPerHost}
                                       * ${locatorThreadsPerVM} ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"locator\", ${locatorHosts}, true)" ncf;
THREADGROUP indexer
    totalThreads = 1
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"bridge\", ${bridgeHosts}, true)" ncf;
THREADGROUP datastore
    totalThreads = fcn ${bridgeHosts} * ${bridgeVMsPerHost}
                                      * ${bridgeThreadsPerVM} - 1 ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"bridge\", ${bridgeHosts}, true)" ncf;
THREADGROUP query
    totalThreads = fcn ${edgeHosts} * ${edgeVMsPerHost}
                                    * ${edgeThreadsPerVM} ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"edge\", ${edgeHosts}, true)" ncf;

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

perffmwk.PerfReportPrms-statisticsSpecification = $JTESTS/cacheperf/poc/useCase15/query.spec;
