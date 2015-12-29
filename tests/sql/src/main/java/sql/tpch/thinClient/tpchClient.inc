include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/gemfirexd/topology_client_2_locator.inc;

hydra.gemfirexd.FabricServerPrms-conserveSockets = false;
hydra.gemfirexd.FabricServerPrms-distributedSystem = ds;
hydra.gemfirexd.FabricServerPrms-hostData = false true true;
hydra.gemfirexd.FabricServerPrms-persistDD = true;
hydra.gemfirexd.FabricServerPrms-serverGroups = none, CustomerGroup, MarketGroup;

hydra.VmPrms-extraVMArgs =
  fcn "hydra.TestConfigFcns.duplicate
       (\"-Xms256m -Xmx256m\", ${locatorHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
       (\"-Xms${${A}serverHeapMB}m -Xmx${${A}serverHeapMB}m\",
                                   ${${A}serverHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
       (\"-Xms${${B}serverHeapMB}m -Xmx${${B}serverHeapMB}m\",
                                   ${${B}serverHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
       (\"-Xms${clientHeapMB}m -Xmx${clientHeapMB}m\",
                                    ${clientHosts}, true)" ncf
  ;
hydra.VmPrms-extraVMArgs +=
  fcn "hydra.TestConfigFcns.duplicate
       (\"-XX:+DisableExplicitGC\", ${locatorHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicate
       (\"-XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC\", ${clientHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicateString
       (\"-XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC -XX:CMSInitiatingOccupancyFraction=50\", ${${A}serverHosts}, true)" ncf
  ,
  fcn "hydra.TestConfigFcns.duplicateString
       (\"-XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC -XX:CMSInitiatingOccupancyFraction=50\", ${${B}serverHosts}, true)" ncf
  ;

//hydra.VmPrms-extraClassPaths        += $GEMFIRE/../product-gfxd/lib/gemfirexd.jar;
hydra.VmPrms-extraClassPaths        += $GEMFIRE/../product-gfxd/lib/gemfirexdtools.jar;
hydra.VmPrms-extraClassPaths        += $GEMFIRE/../product-gfxd/lib/jline-1.0.jar;
//hydra.VmPrms-extraClassPaths        += $GEMFIRE/../product-gfxd/lib/gemfirexdclient.jar;

THREADGROUP locator
  totalThreads = fcn ${locatorHosts} * ${locatorVMsPerHost} * ${locatorThreadsPerVM}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames (\"locator\", ${locatorHosts}, true)"
                 ncf;
THREADGROUP client
  totalThreads = fcn ${clientHosts} * ${clientVMsPerHost} * ${clientThreadsPerVM} -1 
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"client\", ${clientHosts}, true)"
                 ncf;
THREADGROUP ddlThread
  totalThreads = 1
  clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"client\", ${clientHosts}, true)"
                 ncf;
THREADGROUP customerserver
  totalThreads = fcn ${${A}serverHosts} * ${${A}serverVMsPerHost} * ${${A}serverThreadsPerVM}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${A}server\", ${${A}serverHosts}, true)"
                 ncf;
THREADGROUP marketserver
  totalThreads = fcn ${${B}serverHosts} * ${${B}serverVMsPerHost} * ${${B}serverThreadsPerVM}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${B}server\", ${${B}serverHosts}, true)"
                 ncf;
                 
INITTASK  taskClass = sql.tpch.TPCHTest
          taskMethod = HydraTask_initializeParameters
          threadGroups = client
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
          threadGroups = customerserver, marketserver
          ;
INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = startNetworkServerTask
          threadGroups = customerserver, marketserver
          ;
INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = connectPeerClientTask
          threadGroups = customerserver, marketserver
          ;

INITTASK  taskClass = cacheperf.comparisons.gemfirexd.QueryPerfClient
          taskMethod = connectThinClientTask          
          threadGroups = client, ddlThread
          ;
INITTASK  taskClass = sql.tpch.TPCHTest
          taskMethod = openStatisticsTask
          threadGroups = locator, customerserver, marketserver, client, ddlThread
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

// GIVE LONER SAME LOGICAL NAME AS CLIENT AS SEEN IN STATSPECS
hydra.gemfirexd.LonerPrms-names = client;
hydra.gemfirexd.LonerPrms-clientNames =
  fcn "hydra.TestConfigFcns.generateNames(\"client\", ${clientHosts})" ncf
  ;
  
hydra.gemfirexd.NetworkServerPrms-names = network;

hydra.gemfirexd.SqlfConfigPrms-networkServerConfig = network;

objects.query.QueryPrms-api = ${api};
objects.query.QueryPrms-logQueries = ${logQueries};
