INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_5.inc;
INCLUDE $JTESTS/hct/hctparams.inc;
hydra.Prms-totalTaskTimeSec = 300;
hydra.Prms-maxResultWaitSec = 600;

hydra.Prms-testDescription = "Tests get operations on the edge(s) of a hierarchical cache. ";

// make bridges and feeds be peers and make edges be loners
hydra.GemFirePrms-distributedSystem =
  fcn "hydra.TestConfigFcns.duplicate(\"ds\",    ${bridgeHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"ds\",    ${feedHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"loner\", ${defaultEdgeHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"loner\", ${conflationEdgeHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"loner\", ${noConflationEdgeHosts})" ncf;
  

  
hydra.GemFirePrms-enableDurableClient  =
  fcn "hydra.TestConfigFcns.duplicate(\"false\",    ${bridgeHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"false\",    ${feedHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"true\", ${defaultEdgeHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"true\", ${conflationEdgeHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"true\", ${noConflationEdgeHosts})" ncf;  
  

hydra.GemFirePrms-conserveSockets = true;

hydra.BridgePrms-names = bridge;
hydra.BridgePrms-maxThreads = ONEOF default 16 FOENO; // selector control

hydra.PoolPrms-names                       = brloader;
hydra.PoolPrms-minConnections        = 10;
hydra.PoolPrms-subscriptionEnabled = true;
hydra.PoolPrms-threadLocalConnections         = true;
hydra.PoolPrms-subscriptionAckInterval 		   = 10000;			

hydra.CachePrms-messageSyncInterval = 25;

hydra.CachePrms-names        = bridge     feed       edge	conflationedge	noconflationedge;

hydra.RegionPrms-names       = bridge     feed       edge   conflationedge 	noconflationedge;
hydra.RegionPrms-poolName = none       none       brloader 	brloader 	brloader;
hydra.RegionPrms-dataPolicy  = replicate  replicate  default	default		default;
hydra.RegionPrms-scope       = dack;

// If this has to be tested with durable clients, use the following configuration
// hydra.GemFirePrms-enableDurableClient = false false true	true	true;

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
                       
THREADGROUP conflationedge
    totalThreads = ${conflationEdgeHosts}
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"conflationEdge\", ${conflationEdgeHosts}, true)" ncf;   
                        
THREADGROUP defaultedge
    totalThreads = ${defaultEdgeHosts}
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"defaultEdge\", ${defaultEdgeHosts}, true)" ncf;
                        
THREADGROUP noconflationedge
    totalThreads = fcn "${noConflationEdgeHosts} - 1" ncf
    totalVms = fcn "${noConflationEdgeHosts} - 1" ncf;     
    
THREADGROUP reference
    totalThreads = 1
    ;                                                                     

STARTTASK   taskClass  =  hct.ha.HAClientQueueBB taskMethod = initialize
            clientNames = defaultEdge1;

// Setup cache and regions for bridge server caches
INITTASK   taskClass   = durableClients.DurableClientsTest taskMethod  = initCacheServer
           runMode = always
           hydra.ConfigPrms-cacheConfig  = bridge
           hydra.ConfigPrms-regionConfig = bridge
           hydra.ConfigPrms-bridgeConfig = bridge
           threadGroups = bridge;
