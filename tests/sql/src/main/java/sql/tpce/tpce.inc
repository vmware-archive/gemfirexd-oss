include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/gemfirexd/topology_3_locator.inc;

hydra.gemfirexd.FabricServerPrms-conserveSockets = false;
hydra.gemfirexd.FabricServerPrms-distributedSystem = ds;
hydra.gemfirexd.FabricServerPrms-hostData = false false true true;
hydra.gemfirexd.FabricServerPrms-persistDD = true false true true;
hydra.gemfirexd.FabricServerPrms-serverGroups = none, none, CustomerGroup, MarketGroup;

hydra.VmPrms-extraVMArgs =
  fcn "hydra.TestConfigFcns.duplicate
       (\"-Xms256m -Xmx256m\", ${locatorHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
       (\"-Xms${peerHeapMB}m -Xmx${peerHeapMB}m\",
                                    ${peerHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
       (\"-Xms${customerserverHeapMB}m -Xmx${customerserverHeapMB}m\",
                                   ${customerserverHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
       (\"-Xms${marketserverHeapMB}m -Xmx${marketserverHeapMB}m\",
                                   ${marketserverHosts}, true)" ncf
  ;
hydra.VmPrms-extraVMArgs +=
  fcn "hydra.TestConfigFcns.duplicate
       (\"-XX:+DisableExplicitGC\", ${locatorHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
       (\"-XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC\", ${peerHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicateString
       (\"-XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC -XX:CMSInitiatingOccupancyFraction=50\", ${customerserverHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicateString
       (\"-XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC -XX:CMSInitiatingOccupancyFraction=50\", ${marketserverHosts}, true)" ncf
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
THREADGROUP customerserver
  totalThreads = fcn ${customerserverHosts} * ${customerserverVMsPerHost} * ${customerserverThreadsPerVM}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"customerserver\", ${customerserverHosts}, true)"
                 ncf;
THREADGROUP marketserver
  totalThreads = fcn ${marketserverHosts} * ${marketserverVMsPerHost} * ${marketserverThreadsPerVM}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"marketserver\", ${marketserverHosts}, true)"
                 ncf;
                 
INITTASK  taskClass = sql.tpce.TPCETest
          taskMethod = HydraTask_initializeParameters
          threadGroups = peerclient
	      ;
/*         
INITTASK  taskClass = cacheperf.comparisons.gemfirexd.tpcc.TPCETest
          taskMethod = readBlackboardTask
          threadGroups = peerclient
          ;
*/
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
          threadGroups = customerserver, marketserver, peerclient, ddlThread
          ;
INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = connectPeerClientTask
          threadGroups = customerserver, marketserver, peerclient, ddlThread
          ;
          
INITTASK  taskClass = sql.tpce.TPCETest
          taskMethod = openStatisticsTask
          threadGroups = locator, customerserver, marketserver, peerclient, ddlThread
          ;

cacheperf.CachePerfPrms-batchSeconds              = 300;
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