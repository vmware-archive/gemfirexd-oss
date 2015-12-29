include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/gemfirexd/topology_1_locator.inc;

hydra.gemfirexd.FabricServerPrms-conserveSockets = false;
hydra.gemfirexd.FabricServerPrms-distributedSystem = ds;
hydra.gemfirexd.FabricServerPrms-hostData = false true;
hydra.gemfirexd.FabricServerPrms-persistDD = true true;
hydra.gemfirexd.FabricServerPrms-serverGroups = none, none;
hydra.gemfirexd.FabricServerPrms-tableDefaultPartitioned = false;

hydra.VmPrms-extraVMArgs =
  fcn "hydra.TestConfigFcns.duplicate
       (\"-Xms256m -Xmx256m\", ${locatorHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
       (\"-Xms${peerHeapMB}m -Xmx${peerHeapMB}m\",
                                    ${peerHosts}, true)" ncf 
  ;
  
hydra.VmPrms-extraVMArgs +=
  fcn "hydra.TestConfigFcns.duplicate
       (\"-XX:+DisableExplicitGC\", ${locatorHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
       (\"-XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC\", ${peerHosts}, true)" ncf
  ;

hydra.VmPrms-extraClassPaths        += $GEMFIRE/../product-gfxd/lib/gemfirexd.jar;
hydra.VmPrms-extraClassPaths        += $GEMFIRE/../product-gfxd/lib/gemfirexd-tools.jar;
hydra.VmPrms-extraClassPaths        += $GEMFIRE/../product-gfxd/lib/jline-1.0.jar;

THREADGROUP locator
  totalThreads = fcn ${locatorHosts} * ${locatorVMsPerHost} * ${locatorThreadsPerVM}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames (\"locator\", ${locatorHosts}, true)"
                 ncf;
THREADGROUP peerclient
  totalThreads = fcn ${peerHosts} * ${peerVMsPerHost} * ${peerThreadsPerVM} -1 
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"peer\", ${peerHosts}, true)"
                 ncf;
THREADGROUP ddlThread
  totalThreads = 1
  clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"peer\", ${peerHosts}, true)"
                 ncf;
                 
INITTASK  taskClass = sql.tpch.TPCHTest
          taskMethod = HydraTask_initializeParameters
          threadGroups = ddlThread
	      ;

INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = createLocatorTask
          threadGroups = locator
          ;
INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = startLocatorTask
          threadGroups = locator
          ;
INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = startFabricServerTask
          threadGroups = peerclient, ddlThread
          ;
INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = connectPeerClientTask
          threadGroups = peerclient, ddlThread
          ;
          
INITTASK  taskClass = sql.tpch.TPCHTest
          taskMethod = openStatisticsTask
          threadGroups = ddlThread
          ;

cacheperf.CachePerfPrms-batchSeconds              = 900;
cacheperf.CachePerfPrms-batchTerminatorMethod     = terminateOnBatchSeconds;
cacheperf.CachePerfPrms-trimSeconds               = ${trimSeconds};
cacheperf.CachePerfPrms-warmupTerminatorMethod    = terminateOnTrimSeconds;
cacheperf.CachePerfPrms-warmupTerminatorFrequency = 10 seconds;
cacheperf.CachePerfPrms-workSeconds               = ${workSeconds};
cacheperf.CachePerfPrms-taskTerminatorMethod      = terminateOnTotalSeconds;
cacheperf.CachePerfPrms-taskTerminatorFrequency   = 10 seconds;

cacheperf.CachePerfPrms-maxKeys = 1; // not used

cacheperf.comparisons.gemfirexd.QueryPerfPrms-txIsolation = ${txIsolation};

objects.query.QueryPrms-api = ${api};
objects.query.QueryPrms-logQueries = ${logQueries};
