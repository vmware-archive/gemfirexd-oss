include $JTESTS/hydraconfig/topology_hct.inc;

//STARTTASK taskClass   = cacheLoader.hc.Util      taskMethod  = reloadTableTask
//          clientNames = edge1
//          ;

INITTASK  taskClass = cacheLoader.hc.BridgeClient taskMethod = openStatisticsTask
          threadGroups = edge
          ;

INITTASK  taskClass   = cacheLoader.hc.BridgeClient  taskMethod = initBridgeCacheTask
          threadGroups = bridge
          ;

INITTASK  taskClass   = cacheLoader.hc.BridgeClient  taskMethod  = initEdgeCacheTask
          threadGroups = edge
          ;

TASK      taskClass   = cacheLoader.hc.BridgeClient  taskMethod  = readTask
          threadGroups = edge
          ;

CLOSETASK taskClass   = cacheLoader.hc.BridgeClient  taskMethod  = closeCacheTask
          threadGroups = edge
          ;

CLOSETASK taskClass = cacheLoader.hc.BridgeClient taskMethod = closeStatisticsTask
          threadGroups = edge
          ;

THREADGROUP bridge
  totalThreads = fcn
                 ${bridgeHosts} * ${bridgeVMsPerHost} * ${bridgeThreadsPerVM}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames
                      (\"bridge\", ${bridgeHosts}, true)"
                 ncf;
THREADGROUP edge
  totalThreads = fcn
                 ${edgeHosts} * ${edgeVMsPerHost} * ${edgeThreadsPerVM}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames
                      (\"edge\", ${edgeHosts}, true)"
                 ncf;

hydra.Prms-doInitTasksSequentially = true;

hydra.BridgePrms-names  = bridge;
hydra.BridgePrms-maxThreads = ONEOF default 16 FOENO; // selector control


hydra.PoolPrms-names    = brloader;
hydra.PoolPrms-threadLocalConnections = ONEOF true false FOENO;

hydra.CachePrms-names        = bridge edge;

hydra.RegionPrms-names       = bridge edge;
hydra.RegionPrms-dataPolicy  = normal normal;
hydra.RegionPrms-scope       = oneof local noack ack global foeno // bridges
                               local;                             // edges
hydra.RegionPrms-poolName = none      // bridges (see below)
                               brloader; // edges

// server loader configured and added to region in BridgeClient.initBridgeCache
cacheLoader.hc.BridgeParms-serverLoaderClassname = "cacheLoader.hc.DBCacheLoader";
cacheLoader.hc.BridgeParms-validate = ${validate};

//hydra.VmPrms-extraVMArgs   = fcn "hydra.TestConfigFcns.duplicate
//    (\"-XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC -Xmx512m -Xms512m\", ${bridgeHosts}, true)"
//  ncf
//  ,
//  fcn "hydra.TestConfigFcns.duplicate
//    (\"-XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+DisableExplicitGC -Xmx512m -Xms512m\", ${edgeHosts}, true)"
//  ncf;
