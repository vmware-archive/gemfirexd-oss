include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/topology_2_locator.inc;
include $JTESTS/hydraconfig/performance.inc;

hydra.Prms-testDescription = "YCSB.";

hydra.Prms-totalTaskTimeSec = 3600000; // terminator-based workload
hydra.Prms-maxResultWaitSec = 1800;

//------------------------------------------------------------------------------
// Threadgroups
//------------------------------------------------------------------------------

THREADGROUP locator
  totalThreads = fcn ${locatorHosts} * ${locatorVMsPerHost}
                                     * ${locatorThreadsPerVM} ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"locator\",
                        ${locatorHosts}, true)" ncf;
THREADGROUP client
  totalThreads = fcn ${clientHosts} * ${clientVMsPerHost}
                                    * ${clientThreadsPerVM} ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"client\",
                        ${clientHosts}, true)" ncf;
THREADGROUP server
  totalThreads = fcn ${serverHosts} * ${serverVMsPerHost}
                                    * ${serverThreadsPerVM} ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"server\",
                        ${serverHosts}, true)" ncf;

//------------------------------------------------------------------------------
// JVM
//------------------------------------------------------------------------------

hydra.VmPrms-extraClassPaths +=
  fcn "hydra.TestConfigFcns.duplicate
       (\"none\",
                ${locatorHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
       (\"none\",
                ${clientHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicateString
       (\"$GEMFIRE/lib/griddb-dependencies.jar\",
                ${serverHosts}, true)" ncf
  ;
hydra.VmPrms-extraVMArgs =
  fcn "hydra.TestConfigFcns.duplicate
       (\"-Xms256m -Xmx256m\",
                ${locatorHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
       (\"-Xms${clientHeap} -Xmx${clientHeap}\",
                ${clientHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
       (\"-Xms${serverHeap} -Xmx${serverHeap}\",
                ${serverHosts}, true)" ncf
  ;
hydra.VmPrms-extraVMArgs += "-XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC";

//------------------------------------------------------------------------------
// GFXD
//------------------------------------------------------------------------------

hydra.GemFirePrms-conserveSockets = false;
hydra.GemFirePrms-enforceUniqueHost = true;

hydra.ConfigPrms-cacheConfig = cache;
hydra.CachePrms-names = cache;

hydra.ConfigPrms-regionConfig = client;
hydra.RegionPrms-names = server client;

//------------------------------------------------------------------------------
// TASKS
//------------------------------------------------------------------------------

INITTASK  taskClass = gfxdperf.ycsb.gf.GFClient
          taskMethod = createLocatorTask
          threadGroups = locator
          ;
INITTASK  taskClass = gfxdperf.ycsb.gf.GFClient
          taskMethod = startLocatorTask
          threadGroups = locator
          ;
INITTASK  taskClass = gfxdperf.ycsb.gf.GFClient
          taskMethod = startServerTask
          threadGroups = server
          hydra.ConfigPrms-regionConfig = server
          ;
INITTASK  taskClass = gfxdperf.ycsb.gf.GFClient
          taskMethod = initDBTask
          threadGroups = client
          ;
INITTASK  taskClass = gfxdperf.ycsb.gf.GFClient
          taskMethod = initWorkloadTask
          threadGroups = client
          ;
INITTASK  taskClass = gfxdperf.ycsb.gf.GFClient
          taskMethod = createBucketsTask
          threadGroups = client
          ;
INITTASK  taskClass = gfxdperf.ycsb.gf.GFClient
          taskMethod = checkBucketLoadBalanceTask
          threadGroups = server
          ;
INITTASK  taskClass = gfxdperf.ycsb.gf.GFClient
          taskMethod = loadDataTask
          threadGroups = client
          batch
          gfxdperf.PerfPrms-terminatorName = IterationTerminator
          gfxdperf.terminators.IterationTerminatorPrms-batchSeconds = ${batchSeconds}
          gfxdperf.terminators.IterationTerminatorPrms-warmupIterations = ${warmupIterations}
          gfxdperf.terminators.IterationTerminatorPrms-workIterations = fcn ${recordCount} / (${clientHosts} * ${clientVMsPerHost} * ${clientThreadsPerVM}) ncf
          gfxdperf.ycsb.YCSBPrms-trimInterval = load
          ;
INITTASK  taskClass = gfxdperf.ycsb.gf.GFClient
          taskMethod = checkDataLoadBalanceTask
          threadGroups = server
          ;
TASK      taskClass = gfxdperf.ycsb.gf.GFClient
          taskMethod = doWorkloadTask
          threadGroups = client
          gfxdperf.PerfPrms-terminatorName = TimeTerminator
          gfxdperf.terminators.TimeTerminatorPrms-batchSeconds = ${batchSeconds}
          gfxdperf.terminators.TimeTerminatorPrms-warmupSeconds = ${warmupSeconds}
          gfxdperf.terminators.TimeTerminatorPrms-workSeconds = ${workSeconds}
          gfxdperf.ycsb.YCSBPrms-trimInterval = workload
          ;
CLOSETASK taskClass = gfxdperf.ycsb.gf.GFClient
          taskMethod = cleanupDBTask
          threadGroups = client
          ;
CLOSETASK taskClass = gfxdperf.ycsb.gf.GFClient
          taskMethod = stopServerTask
          threadGroups = server
          ;

//------------------------------------------------------------------------------
// Application parameters
//------------------------------------------------------------------------------

gfxdperf.ycsb.core.workloads.CoreWorkloadPrms-fieldCount = ${fieldCount};
gfxdperf.ycsb.core.workloads.CoreWorkloadPrms-recordCount = ${recordCount};

gfxdperf.ycsb.gf.GFPrms-bucketCount =
  fcn ${bucketsPerServer} * ${serverHosts} * ${serverVMsPerHost} ncf;
gfxdperf.ycsb.gf.GFPrms-partitionRedundancy = 1;
