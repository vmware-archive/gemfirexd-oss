include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/gemfirexd/topology_2.inc;
include $JTESTS/hydraconfig/gemfirexd/performance.inc;

hydra.Prms-testDescription = "TPC-H.";

hydra.Prms-totalTaskTimeSec = 3600000; // terminator-based workload
hydra.Prms-maxResultWaitSec = 1800;

//------------------------------------------------------------------------------
// Threadgroups
//------------------------------------------------------------------------------

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
       (\"-Xms${clientHeap} -Xmx${clientHeap}\",
                ${clientHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
       (\"-Xms${serverHeap} -Xmx${serverHeap}\",
                ${serverHosts}, true)" ncf
  ;

//------------------------------------------------------------------------------
// TASKS
//------------------------------------------------------------------------------

INITTASK  taskClass = gfxdperf.tpch.oracle.OracleClient
          taskMethod = startLonerTask
          threadGroups = server
          ;
INITTASK  taskClass = gfxdperf.tpch.oracle.OracleClient
          taskMethod = initDBTask
          threadGroups = client
          ;
INITTASK  taskClass = gfxdperf.tpch.oracle.OracleClient
          taskMethod = dropIndexes
          threadGroups = client
          ;
INITTASK  taskClass = gfxdperf.tpch.oracle.OracleClient
          taskMethod = listIndexes
          threadGroups = client
          ;
INITTASK  taskClass = gfxdperf.PerfClient
          taskMethod = openStatisticsTask
          threadGroups = client, server
          ;
TASK      taskClass = gfxdperf.tpch.oracle.OracleClient
          taskMethod = doWorkloadTask
          threadGroups = client
          gfxdperf.PerfPrms-terminatorName = TimeTerminator
          gfxdperf.terminators.TimeTerminatorPrms-batchSeconds = ${batchSeconds}
          gfxdperf.terminators.TimeTerminatorPrms-warmupSeconds = ${warmupSeconds}
          gfxdperf.terminators.TimeTerminatorPrms-workSeconds = ${workSeconds}
          gfxdperf.tpch.TPCHPrms-trimInterval = workload
          ;
CLOSETASK taskClass = gfxdperf.tpch.oracle.OracleClient
          taskMethod = cleanupDBTask
          threadGroups = client
          ;
