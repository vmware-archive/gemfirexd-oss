INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hct/hctparams.inc;
hydra.Prms-totalTaskTimeSec = 600;
hydra.Prms-maxResultWaitSec = 400;

hydra.Prms-testDescription = "Tests get operations on the edge(s) of different version. ";
  
hydra.GemFirePrms-conserveSockets = true;

hydra.BridgePrms-names = bridge;
hydra.BridgePrms-maxThreads = ONEOF default 16 FOENO; // selector control

hydra.PoolPrms-names                       = brloader;
hydra.PoolPrms-minConnections        = 10;
hydra.PoolPrms-subscriptionEnabled = true;
hydra.PoolPrms-threadLocalConnections         = true;
hydra.PoolPrms-subscriptionAckInterval=10000;

hydra.CachePrms-messageSyncInterval = 25;
hydra.RegionPrms-scope       = dack;

// override generic topology to make just one single-threaded VM of each type
hydra.ClientPrms-vmQuantities = 1;
hydra.ClientPrms-vmThreads    = 1;

delta.DeltaPropagationPrms-numPutThreads = ${nPutThreads};
delta.DeltaPropagationPrms-numKeyRangePerThread = ${nPutKeyRange};

THREADGROUP bridge
    totalThreads = ${bridgeHosts}
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"bridge\", ${bridgeHosts}, true)" ncf;
THREADGROUP feed
    totalThreads = ${feedHosts}
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"feed\", ${feedHosts}, true)" ncf;

THREADGROUP edge
    totalThreads = ${edgeHosts}
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"edge\", ${edgeHosts}, true)" ncf;

STARTTASK   taskClass  =  delta.DeltaPropagationBB taskMethod = initialize
            clientNames = edge1;

// Setup cache and regions for bridge server caches
INITTASK   taskClass   = delta.DeltaPropagation taskMethod  = initCacheServer
           runMode = always
           hydra.ConfigPrms-cacheConfig  = bridge
           hydra.ConfigPrms-regionConfig = bridge
           hydra.ConfigPrms-bridgeConfig = bridge
           threadGroups = bridge;
           