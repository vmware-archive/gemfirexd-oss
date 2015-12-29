
INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_hct.inc;
hydra.Prms-totalTaskTimeSec = 600;
hydra.Prms-maxResultWaitSec = 600;

hydra.VmPrms-extraClassPaths = fcn "hydra.TestConfigFcns.duplicate
                                  (\"$GEMFIRE/lib/antlr.jar\", ${bridgeHosts}, true)"
                             ncf
                             ,
                             fcn "hydra.TestConfigFcns.duplicate
                                  (\"$GEMFIRE/lib/antlr.jar\", ${edgeHosts}, true)"
                             ncf;

hydra.CachePrms-names           = bridge                   edge;
hydra.RegionPrms-names          = bridge                   edge;
hydra.RegionPrms-poolName    = none                     brloader;
hydra.RegionPrms-scope          = ack                      local;

hydra.BridgePrms-names          = bridge;


hydra.PoolPrms-names            = brloader;
hydra.PoolPrms-minConnections = 5;
hydra.PoolPrms-subscriptionEnabled = true;
hydra.PoolPrms-threadLocalConnections = true;

THREADGROUP bridge
  totalThreads = fcn ${bridgeHosts} * ${bridgeVMsPerHost} ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames
                      (\"bridge\", ${bridgeHosts}, true)" ncf;

THREADGROUP edgeInit 
  totalThreads = fcn "${edgeHosts} * ${edgeVMsPerHost}" ncf
  totalVms     = fcn "${edgeHosts} * ${edgeVMsPerHost}" ncf;

INITTASK    taskClass   = query.remote.RemoteQueryTest    taskMethod  = initServerRegion_WithReplicate
            hydra.ConfigPrms-cacheConfig  = bridge
            hydra.ConfigPrms-regionConfig = bridge
            hydra.ConfigPrms-bridgeConfig = bridge
            runMode = always
            threadGroups = bridge;

INITTASK    taskClass   = query.remote.RemoteQueryTest    taskMethod  = initClientRegion
            hydra.ConfigPrms-cacheConfig  = edge
            hydra.ConfigPrms-regionConfig = edge
            runMode = always
            threadGroups = edgeInit;

INITTASK   taskClass  =  query.remote.RemoteQueryTest taskMethod = initQuery
           threadGroups	 = edgeInit;
