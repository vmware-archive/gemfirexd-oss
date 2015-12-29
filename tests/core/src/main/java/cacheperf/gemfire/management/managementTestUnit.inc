//This file defines the main workload for management related perf/scale tests

hydra.GemFirePrms-conserveSockets = ${conserveSockets};

hydra.CachePrms-names          = bridge    edge;

hydra.RegionPrms-names         = bridge    edge;
hydra.RegionPrms-dataPolicy    = partition empty;
hydra.RegionPrms-partitionName = bridge    none;
hydra.RegionPrms-poolName      = none      pool;
hydra.RegionPrms-scope         = default   local;

hydra.PartitionPrms-names = bridge;
hydra.PartitionPrms-redundantCopies = ${redundantCopies};
hydra.PartitionPrms-totalNumBuckets = fcn 10 * ${bridgeHosts} * ${bridgeVMsPerHost} ncf;

hydra.BridgePrms-names = bridge;

hydra.PoolPrms-names                  = pool;
hydra.PoolPrms-contactNum             = 1;
hydra.PoolPrms-readTimeout            = 10000000;
hydra.PoolPrms-subscriptionEnabled    = false;
hydra.PoolPrms-threadLocalConnections = true;

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

INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = createAndStartLocatorTask
          threadGroups = locator
          ;          
          
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openStatisticsTask
          threadGroups = bridge
          ;
          
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = openCacheTask
          hydra.ConfigPrms-cacheConfig = bridge
          hydra.ConfigPrms-regionConfig = bridge
          hydra.ConfigPrms-bridgeConfig = bridge
          threadGroups = bridge
          ;
          
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = assignBucketsTask
          threadGroups = bridge
          ;
          
INITTASK  taskClass = cacheperf.CachePerfClient taskMethod = createDataTask
          batch
          cacheperf.CachePerfPrms-keyAllocation = ownKeysChunked
          cacheperf.CachePerfPrms-warmupTerminatorMethod = none
          cacheperf.CachePerfPrms-taskTerminatorMethod = terminateOnMaxKey
          cacheperf.CachePerfPrms-taskTerminatorFrequency = 1 iterations
          threadGroups = bridge
          ;
          
TASK      taskClass = cacheperf.CachePerfClient taskMethod = mixPutGetDataTask
          cacheperf.CachePerfPrms-keyAllocation = ownKeysRandomWrap
          threadGroups = bridge
          ;
          
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeCacheTask
          threadGroups = bridge
          ;
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = closeStatisticsTask
          threadGroups = bridge
          ;
          
CLOSETASK taskClass = cacheperf.CachePerfClient taskMethod = reportTrimIntervalsTask
          threadGroups = bridge
          ;

hydra.Prms-totalTaskTimeSec = 259200;  // this test is workload based
hydra.Prms-maxResultWaitSec = 600;

cacheperf.CachePerfPrms-keyType    = java.lang.Long;
cacheperf.CachePerfPrms-objectType = objects.PSTObject;
cacheperf.CachePerfPrms-maxKeys    = ${maxKeys};
objects.PSTObjectPrms-size         = ${dataSize};

cacheperf.CachePerfPrms-putPercentage = 20;

cacheperf.CachePerfPrms-batchTerminatorMethod     = terminateOnBatchSeconds;
cacheperf.CachePerfPrms-batchSeconds              = 180;
cacheperf.CachePerfPrms-warmupTerminatorMethod    = terminateOnTrimSeconds;
cacheperf.CachePerfPrms-warmupTerminatorFrequency = 5 seconds;
cacheperf.CachePerfPrms-trimSeconds               = 180;
cacheperf.CachePerfPrms-taskTerminatorMethod      = terminateOnTotalSeconds;
cacheperf.CachePerfPrms-taskTerminatorFrequency   = 15 seconds;
cacheperf.CachePerfPrms-workSeconds               = ${workSeconds};

hydra.VmPrms-extraVMArgs =
   fcn "hydra.TestConfigFcns.duplicate
        (\"-Dnone\", ${locatorHosts}, true)" ncf
   ,
   fcn "hydra.TestConfigFcns.duplicate
        (\"-Xms${bridgeVMSizeMB}m -Xmx${bridgeVMSizeMB}m\", ${bridgeHosts}, true)" ncf
   ;

hydra.VmPrms-extraVMArgs +=
  -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC;

hydra.VmPrms-extraVMArgs += "-XX:CMSInitiatingOccupancyFraction=33";
hydra.VmPrms-extraVMArgs += "-Dgemfire.disableManagement=${disableMgmtService}";
hydra.Prms-manageLocatorAgents=false;

perffmwk.PerfReportPrms-statisticsSpecification = $JTESTS/cacheperf/gemfire/management/mgmtSpec.spec;
