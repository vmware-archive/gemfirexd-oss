include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/gemfirexd/topology_2_locator.inc;
include $JTESTS/hydraconfig/gemfirexd/performance.inc;

hydra.Prms-testDescription = "TPC-H.";

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

hydra.VmPrms-extraClassPaths += $GEMFIRE/lib/jna-3.5.1.jar;
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

//------------------------------------------------------------------------------
// GFXD
//------------------------------------------------------------------------------

hydra.gemfirexd.FabricServerPrms-conserveSockets = false;
hydra.gemfirexd.FabricServerPrms-distributedSystem = ds;
hydra.gemfirexd.FabricServerPrms-enforceUniqueHost = true;
hydra.gemfirexd.FabricServerPrms-lockMemory = true;

//------------------------------------------------------------------------------
// TASKS
//------------------------------------------------------------------------------

INITTASK  taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = createLocatorTask
          threadGroups = locator
          ;
INITTASK  taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = startLocatorTask
          threadGroups = locator
          ;
INITTASK  taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = startFabricServerTask
          threadGroups = server
          ;
INITTASK  taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = initDBTask
          threadGroups = client
          ;
INITTASK  taskClass = gfxdperf.PerfClient
          taskMethod = openStatisticsTask
          threadGroups = client, server
          ;
/*
INITTASK  taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = generateDDLTask
          threadGroups = server
          ;
INITTASK  taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = executeDDLTask
          threadGroups = client
          ;
INITTASK  taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = createBucketsTask
          threadGroups = client
          ;
INITTASK  taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = checkBucketLoadBalanceTask
          threadGroups = server
          ;
INITTASK  taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = loadDataTask
          threadGroups = client
          batch
          gfxdperf.PerfPrms-terminatorName = IterationTerminator
          gfxdperf.terminators.IterationTerminatorPrms-batchSeconds = ${batchSeconds}
          gfxdperf.terminators.IterationTerminatorPrms-warmupIterations = ${warmupIterations}
          gfxdperf.terminators.IterationTerminatorPrms-workIterations = fcn ${recordCount} / (${clientHosts} * ${clientVMsPerHost} * ${clientThreadsPerVM}) ncf
          gfxdperf.tpch.TPCHPrms-trimInterval = load
          ;
INITTASK  taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = checkDataLoadBalanceTask
          threadGroups = server
          ;
*/
TASK      taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = doWorkloadTask
          threadGroups = client
          gfxdperf.PerfPrms-terminatorName = TimeTerminator
          gfxdperf.terminators.TimeTerminatorPrms-batchSeconds = ${batchSeconds}
          gfxdperf.terminators.TimeTerminatorPrms-warmupSeconds = ${warmupSeconds}
          gfxdperf.terminators.TimeTerminatorPrms-workSeconds = ${workSeconds}
          gfxdperf.tpch.TPCHPrms-trimInterval = workload
          gfxdperf.tpch.gfxd.GFXDPrms-queryPlanFrequency = ${queryPlanFrequency}
          ;
/*
CLOSETASK taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = dumpQueryPlansTask
          threadGroups = server
          ;
*/
CLOSETASK taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = cleanupDBTask
          threadGroups = client
          ;
CLOSETASK taskClass = gfxdperf.tpch.gfxd.GFXDClient
          taskMethod = shutDownAllTask
          threadGroups = locator
          ;

// make sure we stop the servers no matter what
hydra.Prms-clientShutdownHook = gfxdperf.tpch.gfxd.GFXDClient shutDownHook;

//------------------------------------------------------------------------------
// Application parameters
//------------------------------------------------------------------------------

gfxdperf.tpch.gfxd.GFXDPrms-bucketCount =
  fcn ${bucketsPerServer} * ${serverHosts} * ${serverVMsPerHost} ncf;
gfxdperf.tpch.gfxd.GFXDPrms-partitionRedundancy = 1;
