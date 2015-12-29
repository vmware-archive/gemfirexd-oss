INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_3.inc;
hydra.Prms-totalTaskTimeSec = 600;
hydra.Prms-maxResultWaitSec = 600;

hydra.VmPrms-extraClassPaths = fcn "hydra.TestConfigFcns.duplicate
                                  (\"$GEMFIRE/lib/antlr.jar\", ${bridgeHosts}, true)"
                             ncf
                             ,
                             fcn "hydra.TestConfigFcns.duplicate
                                  (\"$GEMFIRE/lib/antlr.jar\", ${edgeHosts}, true)"
                             ncf
                             ,
                             fcn "hydra.TestConfigFcns.duplicate
                                  (\"$GEMFIRE/lib/antlr.jar\", ${feedHosts}, true)"
                             ncf
                             ;
                             
// make bridges and feeds be peers and make edges be loners
hydra.GemFirePrms-distributedSystem =
  fcn "hydra.TestConfigFcns.duplicate(\"ds\",    ${bridgeHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"loner\", ${edgeHosts})" ncf
  fcn "hydra.TestConfigFcns.duplicate(\"ds\", ${feedHosts})" ncf ;
  
hydra.CachePrms-names           	= bridge       		edge        feed;
hydra.RegionPrms-names          	= bridge       		edge        feed;
hydra.RegionPrms-poolName    	= none         		brloader    none;
hydra.RegionPrms-scope         		= ack          		local       local;

hydra.RegionPrms-diskStoreName      = disk none none;
hydra.RegionPrms-evictionAttributes = lruEntryCount 1000 overflowToDisk ,default, 	default   ;

hydra.DiskStorePrms-names = disk;
hydra.RegionPrms-diskSynchronous = true;

hydra.BridgePrms-names          = bridge;


hydra.PoolPrms-names            = brloader;
hydra.PoolPrms-minConnections = 5;
hydra.PoolPrms-subscriptionEnabled = true;
hydra.PoolPrms-threadLocalConnections = true;

THREADGROUP bridge
    totalThreads = ${bridgeHosts}
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                       (\"bridge\", ${bridgeHosts}, true)" ncf;                      
                       
THREADGROUP edge
    totalThreads = ${edgeHosts}
    totalVms     = ${edgeHosts} 
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"edge\", ${edgeHosts}, true)" ncf;
    
THREADGROUP feed
    totalThreads = ${feedHosts}
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"feed\", ${feedHosts}, true)" ncf;
                        
INITTASK    taskClass   = query.remote.RemoteQueryTest    taskMethod  = initServerRegion_WithReplicate
            hydra.ConfigPrms-cacheConfig  = bridge
            hydra.ConfigPrms-regionConfig = bridge
            hydra.ConfigPrms-bridgeConfig = bridge
            runMode = always
            threadGroups = bridge;
            
INITTASK    taskClass   = query.remote.RemoteQueryTest    taskMethod  = initClientRegionWithoutInterest
            hydra.ConfigPrms-cacheConfig  = edge
            hydra.ConfigPrms-regionConfig = edge
            runMode = always
            threadGroups = edge;
                 
INITTASK    taskClass   = query.remote.RemoteQueryTest    taskMethod  = initClientRegionWithoutInterest
            hydra.ConfigPrms-cacheConfig  = feed
            hydra.ConfigPrms-regionConfig = feed
            runMode = always
            threadGroups = feed;         
