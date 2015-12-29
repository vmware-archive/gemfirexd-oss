include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/gemfirexd/topology_2_locator.inc;
include $JTESTS/hydraconfig/gemfirexd/performance.inc;

//hydra.Prms-clientShutdownHook = cacheperf.comparisons.gemfirexd.useCase5.poc.UseCase5Client
//                                dumpBucketsHook;

hydra.Prms-totalTaskTimeSec = 3600000; // time-based workload
hydra.Prms-maxResultWaitSec = 1800;

hydra.VmPrms-extraVMArgs =
  fcn "hydra.TestConfigFcns.duplicate
       (\"-Xms256m -Xmx256m\", ${locatorHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
       (\"-Xms${clientHeapMB}m -Xmx${clientHeapMB}m\",
                                    ${clientHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
       (\"-Xms${serverHeapMB}m -Xmx${serverHeapMB}m\",
                                   ${serverHosts}, true)" ncf
  ;
hydra.VmPrms-extraVMArgs +=
  fcn "hydra.TestConfigFcns.duplicate
       (\"-XX:+DisableExplicitGC\", ${locatorHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
       (\"-XX:+DisableExplicitGC\", ${clientHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicateString
       (\"-XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC\", ${serverHosts}, true)" ncf
  ;

hydra.gemfirexd.FabricServerPrms-conserveSockets = false;
hydra.gemfirexd.FabricServerPrms-distributedSystem = ds;

THREADGROUP locator
  totalThreads = fcn ${locatorHosts} * ${locatorVMsPerHost} * ${locatorThreadsPerVM}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames (\"locator\", ${locatorHosts}, true)"
                 ncf;
THREADGROUP client
  totalThreads = fcn ${clientHosts} * ${clientVMsPerHost} * ${clientThreadsPerVM}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"client\", ${clientHosts}, true)"
                 ncf;
THREADGROUP server
  totalThreads = fcn ${serverHosts} * ${serverVMsPerHost} * ${serverThreadsPerVM}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"server\", ${serverHosts}, true)"
                 ncf;

INITTASK  taskClass = cacheperf.comparisons.gemfirexd.useCase5.poc.UseCase5Client
          taskMethod = openStatisticsTask
          threadGroups = locator, server, client
          ;
INITTASK  taskClass = cacheperf.comparisons.gemfirexd.useCase5.poc.UseCase5Client
          taskMethod = executeDDLTask
          threadGroups = server
          ;
INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = createBucketsTask
          threadGroups = server
          ;
//INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
//          taskMethod = rebalanceBucketsTask
//          threadGroups = server
//          ;
INITTASK  taskClass = cacheperf.comparisons.gemfirexd.useCase5.poc.UseCase5Client
          taskMethod = printBucketListTask
          threadGroups = server
          ;
INITTASK  taskClass = cacheperf.comparisons.gemfirexd.useCase5.poc.UseCase5Client
          taskMethod = setPrimaryBucketListTask
          threadGroups = server
          ;
INITTASK  taskClass = cacheperf.comparisons.gemfirexd.useCase5.poc.UseCase5Client
          taskMethod = setupTerminalsTask
          threadGroups = client
          ;
//INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
//          taskMethod = checkBucketsTask
//          threadGroups = server
//          ;
//INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
//          taskMethod = checkDataLoadTask
//          threadGroups = server
//          ;
INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = closeConnectionTask
          threadGroups = server
          ;
INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = configureDebuggingTask
          threadGroups = server
          ;
TASK      taskClass = cacheperf.comparisons.gemfirexd.useCase5.poc.UseCase5Client
          taskMethod = sellCashBetsTask
          threadGroups = client
          ;
CLOSETASK taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = reportTrimIntervalsTask
          threadGroups = client, server
          ;
CLOSETASK taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = dumpQueryPlansTask
          threadGroups = server
          ;
CLOSETASK taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = checkDataLoadTask
          threadGroups = server
          ;
CLOSETASK taskClass = cacheperf.comparisons.gemfirexd.useCase5.poc.UseCase5Client
          taskMethod = closeStatisticsTask
          threadGroups = client
          ;
CLOSETASK taskClass = cacheperf.comparisons.gemfirexd.useCase5.poc.UseCase5Client
          taskMethod = closeStatisticsTask
          threadGroups = server
          ;

cacheperf.CachePerfPrms-keyAllocation = ownKeys;
cacheperf.CachePerfPrms-batchSeconds = 300;
cacheperf.CachePerfPrms-batchTerminatorMethod = terminateOnBatchSeconds;
cacheperf.CachePerfPrms-warmupTerminatorMethod = terminateOnTrimIterations;
cacheperf.CachePerfPrms-warmupTerminatorFrequency = 1 iterations;
cacheperf.CachePerfPrms-trimIterations = 0;
cacheperf.CachePerfPrms-taskTerminatorMethod = terminateOnMaxKey;
cacheperf.CachePerfPrms-taskTerminatorFrequency = 1 iterations;

/*
cacheperf.CachePerfPrms-trimSeconds               = ${trimSeconds};
cacheperf.CachePerfPrms-warmupTerminatorMethod    = terminateOnTrimSeconds;
cacheperf.CachePerfPrms-warmupTerminatorFrequency = 10 seconds;
cacheperf.CachePerfPrms-workSeconds               = ${workSeconds};
cacheperf.CachePerfPrms-taskTerminatorMethod      = terminateOnTotalSeconds;
cacheperf.CachePerfPrms-taskTerminatorFrequency   = 10 seconds;
*/

cacheperf.comparisons.gemfirexd.QueryPerfPrms-txIsolation = ${txIsolation};

objects.query.QueryPrms-api = ${api};
objects.query.QueryPrms-logQueries = ${logQueries};

cacheperf.comparisons.gemfirexd.useCase5.poc.UseCase5Prms-ddlFile  = ${ddlFile};
cacheperf.comparisons.gemfirexd.useCase5.poc.UseCase5Prms-numTerminals  = ${numTerminals};
cacheperf.comparisons.gemfirexd.useCase5.poc.UseCase5Prms-timeStmts = ${timeStmts};

perffmwk.PerfReportPrms-statisticsSpecification = $JTESTS/cacheperf/comparisons/gemfirexd/useCase5/poc/specs/useCase5.spec;
