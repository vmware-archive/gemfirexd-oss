include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/gemfirexd/topology_2_locator.inc;
include $JTESTS/hydraconfig/gemfirexd/performance.inc;

hydra.Prms-testDescription = "YCSB with HDFS.";

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
hydra.VmPrms-extraClassPaths +=
  fcn "hydra.TestConfigFcns.duplicate
       (\"none\",
                ${locatorHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
       (\"none\",
                ${clientHosts}, true)" ncf
  ,
  fcn "hydra.HadoopPrms.getServerJars
       (\"$HADOOP_DIST\",
                ${serverHosts})" ncf
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
hydra.VmPrms-extraVMArgs +=
  fcn "hydra.TestConfigFcns.duplicate
       (\"-XX:+DisableExplicitGC\",
                ${locatorHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
       (\"-XX:+DisableExplicitGC\",
                ${clientHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicateString
       (\"-XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC\",
                ${serverHosts}, true)" ncf
  ;

//------------------------------------------------------------------------------
// GFXD
//------------------------------------------------------------------------------

hydra.gemfirexd.FabricServerPrms-conserveSockets = false;
hydra.gemfirexd.FabricServerPrms-distributedSystem = ds;
hydra.gemfirexd.FabricServerPrms-enforceUniqueHost = true;
hydra.gemfirexd.FabricServerPrms-lockMemory = true;
hydra.gemfirexd.FabricServerPrms-persistDD = true true; // to get sys-disk-dir

include $JTESTS/gfxdperf/ycsb/gfxd/continuous/persist.inc;

hydra.gemfirexd.GfxdConfigPrms-hdfsStoreConfig = store;
hydra.gemfirexd.HDFSStorePrms-names = store;
hydra.gemfirexd.HDFSStorePrms-batchSize = 32; // product default is 32 MB
hydra.gemfirexd.HDFSStorePrms-clientConfigFile = $PWD/gfxd-client-config.xml;
hydra.gemfirexd.HDFSStorePrms-diskStoreName = ycsb;
hydra.gemfirexd.HDFSStorePrms-hadoopName = hadoop;
hydra.gemfirexd.HDFSStorePrms-maxQueueMemory = ${hdfsQueueMB};
hydra.gemfirexd.HDFSStorePrms-minorCompact = true;

hydra.ConfigPrms-hadoopConfig = hadoop;
hydra.HadoopPrms-names = hadoop;
hydra.HadoopPrms-replication = 2;

//------------------------------------------------------------------------------
// TASKS
//------------------------------------------------------------------------------

INITTASK  taskClass = gfxdperf.ycsb.gfxd.GFXDClient
          taskMethod = configureHadoopTask
          threadGroups = locator
          ;
INITTASK  taskClass = gfxdperf.ycsb.gfxd.GFXDClient
          taskMethod = startHDFSClusterTask
          threadGroups = locator
          ;
INITTASK  taskClass = gfxdperf.ycsb.gfxd.GFXDClient
          taskMethod = createLocatorTask
          threadGroups = locator
          ;
INITTASK  taskClass = gfxdperf.ycsb.gfxd.GFXDClient
          taskMethod = startLocatorTask
          threadGroups = locator
          ;
INITTASK  taskClass = gfxdperf.ycsb.gfxd.GFXDClient
          taskMethod = startFabricServerTask
          threadGroups = server
          ;
INITTASK  taskClass = gfxdperf.ycsb.gfxd.GFXDClient
          taskMethod = initDBTask
          threadGroups = client
          ;
INITTASK  taskClass = gfxdperf.ycsb.gfxd.GFXDClient
          taskMethod = initWorkloadTask
          threadGroups = client
          ;
INITTASK  taskClass = gfxdperf.ycsb.gfxd.GFXDClient
          taskMethod = generateHDFSClientConfigTask
          threadGroups = server
          ;
INITTASK  taskClass = gfxdperf.ycsb.gfxd.GFXDClient
          taskMethod = generateDDLTask
          threadGroups = server
          ;
INITTASK  taskClass = gfxdperf.ycsb.gfxd.GFXDClient
          taskMethod = executeDDLTask
          threadGroups = client
          ;
INITTASK  taskClass = gfxdperf.ycsb.gfxd.GFXDClient
          taskMethod = createBucketsTask
          threadGroups = client
          ;
INITTASK  taskClass = gfxdperf.ycsb.gfxd.GFXDClient
          taskMethod = checkBucketLoadBalanceTask
          threadGroups = server
          ;
INITTASK  taskClass = gfxdperf.ycsb.gfxd.GFXDClient
          taskMethod = loadDataTask
          threadGroups = client
          batch
          gfxdperf.PerfPrms-terminatorName = IterationTerminator
          gfxdperf.terminators.IterationTerminatorPrms-batchSeconds = ${batchSeconds}
          gfxdperf.terminators.IterationTerminatorPrms-warmupIterations = ${warmupIterations}
          gfxdperf.terminators.IterationTerminatorPrms-workIterations = fcn ${recordCount} / (${clientHosts} * ${clientVMsPerHost} * ${clientThreadsPerVM}) ncf
          gfxdperf.ycsb.YCSBPrms-trimInterval = load
          gfxdperf.ycsb.gfxd.GFXDPrms-hdfsFlushQueues = true
          gfxdperf.ycsb.gfxd.GFXDPrms-hdfsForceCompaction = true
          ;
INITTASK  taskClass = gfxdperf.ycsb.gfxd.GFXDClient
          taskMethod = checkDataLoadBalanceTask
          threadGroups = server
          ;
TASK      taskClass = gfxdperf.ycsb.gfxd.GFXDClient
          taskMethod = doWorkloadTask
          threadGroups = client
          gfxdperf.PerfPrms-terminatorName = TimeTerminator
          gfxdperf.terminators.TimeTerminatorPrms-batchSeconds = ${batchSeconds}
          gfxdperf.terminators.TimeTerminatorPrms-warmupSeconds = ${warmupSeconds}
          gfxdperf.terminators.TimeTerminatorPrms-workSeconds = ${workSeconds}
          gfxdperf.ycsb.YCSBPrms-trimInterval = workload
          gfxdperf.ycsb.gfxd.GFXDPrms-hdfsFlushQueues = true
          gfxdperf.ycsb.gfxd.GFXDPrms-hdfsForceCompaction = false
          gfxdperf.ycsb.gfxd.GFXDPrms-queryPlanFrequency = ${queryPlanFrequency}
          ;
CLOSETASK taskClass = gfxdperf.ycsb.gfxd.GFXDClient
          taskMethod = dumpQueryPlansTask
          threadGroups = server
          ;
CLOSETASK taskClass = gfxdperf.ycsb.gfxd.GFXDClient
          taskMethod = cleanupDBTask
          threadGroups = client
          ;
CLOSETASK taskClass = gfxdperf.ycsb.gfxd.GFXDClient
          taskMethod = stopFabricServerTask
          threadGroups = server
          ;
CLOSETASK taskClass = gfxdperf.ycsb.gfxd.GFXDClient
          taskMethod = stopHDFSClusterTask
          threadGroups = locator
          ;

//------------------------------------------------------------------------------
// Application parameters
//------------------------------------------------------------------------------

gfxdperf.ycsb.core.workloads.CoreWorkloadPrms-fieldCount = ${fieldCount};
gfxdperf.ycsb.core.workloads.CoreWorkloadPrms-recordCount = ${recordCount};

gfxdperf.ycsb.gfxd.GFXDPrms-bucketCount =
  fcn ${bucketsPerServer} * ${serverHosts} * ${serverVMsPerHost} ncf;
gfxdperf.ycsb.gfxd.GFXDPrms-partitionRedundancy = 1;
gfxdperf.ycsb.gfxd.GFXDPrms-useHDFSWriteOnly = false;
