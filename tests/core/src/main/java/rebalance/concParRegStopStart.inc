INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;

INITTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_initialize
             hydra.ConfigPrms-cacheConfig = cache1
             hydra.ConfigPrms-regionConfig = clientRegion
             threadGroups = peer
             runMode = always
             ;

INITTASK     taskClass   = util.StopStartVMs  taskMethod = StopStart_initTask
             threadGroups = peer
             ;

TASK         taskClass   = parReg.ParRegTest  taskMethod = HydraTask_doEntryOpsDataLoss
             threadGroups = peer
             ;

TASK         taskClass   = rebalance.RebalanceTest taskMethod = HydraTask_stopStartNonAdminVMs
             threadGroups = peer
             maxThreads = 1
             ;

CLOSETASK    taskClass   = parReg.ParRegTest  taskMethod = HydraTask_logLocalSize
             threadGroups = peer
             ;

THREADGROUP peer
    totalThreads = fcn
                   ${${A}Hosts} * ${${A}VMsPerHost} * ${${A}ThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"${A}\", ${${A}Hosts}, true)"
                   ncf;

hydra.GemFirePrms-stopSystemsAfterTest = true;

hydra.Prms-totalTaskTimeSec = 1500;  // this is workload based
hydra.Prms-maxResultWaitSec = 1200;
hydra.Prms-serialExecution = false;
hydra.Prms-clientShutdownHook = parReg.ParRegUtil dumpAllPartitionedRegions;

INCLUDE $JTESTS/util/randomValues.inc;
util.RandomValuesPrms-objectType = byte[];
util.RandomValuesPrms-elementSize = 1000;
util.ValueHolderPrms-useExtraObject = true;
hydra.GemFirePrms-conserveSockets = ONEOF true false FOENO;

hydra.CachePrms-names           = cacheServerConfig cache1;
hydra.CachePrms-searchTimeout   = 600;

hydra.RegionPrms-names          = cacheServerRegion clientRegion;
hydra.RegionPrms-regionName     = partitionedRegion;
hydra.RegionPrms-cacheLoader    = none parReg.ParRegLoader;
hydra.RegionPrms-dataPolicy     = partition;
hydra.RegionPrms-partitionName  = csPR              peerPR;

hydra.PartitionPrms-names           = csPR          peerPR;
hydra.PartitionPrms-redundantCopies = ${redundantCopies};
hydra.PartitionPrms-localMaxMemory = 0              default;
// rely on rebalance (don't do recover on member departure/join)
hydra.PartitionPrms-startupRecoveryDelay = -1;

util.TestHelperPrms-minTaskGranularitySec = 45;

// lynn - local destroy not supported for partitioned regions for Congo
// lynn - local invalidate not supported for partitioned regions for Congo
// The following 3 parameters specify random operations.
// The test will try to hold the size of the region to regionSizeThreshold.
// If the region grows larger in size than regionSizeThreshold, then the
// operations will be chosen from thresholdOperations, otherwise the operations
// will be chosen from entryOperations
parReg.ParRegPrms-entryOperations = ONEOF add add getNew getNew update invalidate get destroy FOENO; 
parReg.ParRegPrms-upperThreshold = 500;
parReg.ParRegPrms-upperThresholdOperations = ONEOF destroy FOENO;
parReg.ParRegPrms-lowerThreshold = 10;
parReg.ParRegPrms-lowerThresholdOperations = ONEOF add getNew FOENO;

util.CachePrms-useDeclarativeXmlFile = ONEOF true false FOENO;
util.StopStartPrms-numVMsToStop = RANGE 1 ${numVMsToStop} EGNAR;
util.StopStartPrms-stopModes = ONEOF NICE_EXIT MEAN_KILL MEAN_EXIT FOENO;
