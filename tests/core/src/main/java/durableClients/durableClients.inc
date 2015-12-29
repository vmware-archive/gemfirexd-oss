INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_3.inc;
INCLUDE $JTESTS/hct/hctparams.inc;
hydra.Prms-totalTaskTimeSec = 300;
hydra.Prms-maxResultWaitSec = 600;
hydra.Prms-manageLocatorAgents = true;

hydra.Prms-testDescription = "Tests get operations on the edge(s) of a hierarchical cache. ";

// make bridges and feeds be peers and make edges be loners
hydra.GemFirePrms-distributedSystem =
  fcn "hydra.TestConfigFcns.duplicate(\"ds\",    ${bridgeHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"ds\",    ${feedHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"loner\", ${edgeHosts})" ncf;

hydra.GemFirePrms-conserveSockets = true;

hydra.BridgePrms-names = bridge;
hydra.BridgePrms-maxThreads = ONEOF default 16 FOENO; // selector control

hydra.PoolPrms-names                       = brloader;
hydra.PoolPrms-minConnections        = 10;
hydra.PoolPrms-subscriptionEnabled = true;
hydra.PoolPrms-threadLocalConnections         = true;
hydra.PoolPrms-subscriptionAckInterval=10000;

hydra.CachePrms-messageSyncInterval = 25;

hydra.CachePrms-names        = bridge     feed       edge;

hydra.RegionPrms-names       = bridge     feed       edge;
hydra.RegionPrms-poolName = none       none       brloader;
hydra.RegionPrms-dataPolicy  = replicate  replicate  default;
hydra.RegionPrms-scope       = dack;

hydra.GemFirePrms-enableDurableClient = false false true;


// override generic topology to make just one single-threaded VM of each type
hydra.ClientPrms-vmQuantities = 1;
hydra.ClientPrms-vmThreads    = 1;

durableClients.DurableClientsPrms-numPutThreads = ${hc.nPutThreads};
durableClients.DurableClientsPrms-numKeyRangePerThread = ${hc.nPutKeyRange};
durableClients.DurableClientsPrms-regionName= Region;

THREADGROUP bridge
    totalThreads = ${bridgeHosts}
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"bridge\", ${bridgeHosts}, true)" ncf;
THREADGROUP feed
    totalThreads = ${feedHosts}
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"feed\", ${feedHosts}, true)" ncf;

THREADGROUP edge
    totalThreads = fcn "${edgeHosts} - 1" ncf
    totalVms = fcn "${edgeHosts} - 1" ncf;

THREADGROUP reference
    totalThreads = 1
    ;  
                        
INITTASK   taskClass   = durableClients.DurableClientsTest taskMethod  = initCacheServer
           runMode = always
           hydra.ConfigPrms-cacheConfig  = bridge
           hydra.ConfigPrms-regionConfig = bridge
           hydra.ConfigPrms-bridgeConfig = bridge
           threadGroups = bridge;
                        
