hydra.Prms-testDescription = "
Concurrently create and destroy partitioned regions with concurrent execution of rebalance.  Final rebalance CLOSETASK also verifies balance improved.
";

INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_p2p_2.inc;

INITTASK  taskClass = parReg.RebalanceParRegCreateDestroy taskMethod = HydraTask_initialize
          threadGroups = peer;

TASK      taskClass = parReg.RebalanceParRegCreateDestroy taskMethod = HydraTask_createDestroy
          threadGroups = peer;

THREADGROUP peer
    totalThreads = fcn
                   ${peerHosts} * ${peerVMsPerHost} * ${peerThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"peer\", ${peerHosts}, true)"
                   ncf;  

// VM to control the CacheServer stop/start
THREADGROUP rebalance
    totalThreads = fcn
                   ${rebalanceHosts} * ${rebalanceVMsPerHost} * ${rebalanceThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"rebalance\", ${rebalanceHosts}, true)"
                   ncf;

hydra.Prms-totalTaskTimeSec = 1200;
hydra.Prms-maxResultWaitSec = 900;
hydra.Prms-clientShutdownHook = parReg.ParRegUtil dumpAllPartitionedRegions;

hydra.GemFirePrms-conserveSockets = ONEOF true false FOENO;

util.TestHelperPrms-minTaskGranularitySec = 30;

hydra.CachePrms-names           = cache1;
hydra.CachePrms-searchTimeout   = 600;

hydra.RegionPrms-names          =
      region1 region2 region3 region4 region5 region6 region7 region8 region9 region10;
hydra.RegionPrms-regionName     =
      clientRegion1 clientRegion2 clientRegion3 clientRegion4 clientRegion5
      clientRegion6 clientRegion7 clientRegion8 clientRegion9 clientRegion10;
hydra.RegionPrms-dataPolicy     = partition;
hydra.RegionPrms-partitionName  =
      pr1 pr2 pr3 pr4 pr5 pr6 pr7 pr8 pr9 pr10;

hydra.PartitionPrms-names           =
      pr1 pr2 pr3 pr4 pr5 pr6 pr7 pr8 pr9 pr10;
hydra.PartitionPrms-redundantCopies =
      ONEOF 0 1 FOENO ONEOF 0 1 FOENO ONEOF 0 1 FOENO ONEOF 0 1 FOENO ONEOF 0 1 FOENO
      ONEOF 0 1 FOENO ONEOF 0 1 FOENO ONEOF 0 1 FOENO ONEOF 0 1 FOENO ONEOF 0 1 FOENO;
// rely on rebalance (don't do redundancy recover on startup)
hydra.PartitionPrms-startupRecoveryDelay = -1;





