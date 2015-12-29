INITTASK     taskClass   = util.StopStartVMs  taskMethod = StopStart_initTask
             runMode = once
             threadGroups = accessorThreads, dataStoreThreads;

INITTASK     taskClass   = util.PRObserver  taskMethod = initialize
             runMode = once
             threadGroups = dataStoreThreads;

INITTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_HA_initializeDataStore
             runMode = once
             threadGroups = dataStoreThreads;

INITTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_HA_initializeAccessor
             runMode = once
             threadGroups = accessorThreads;

INITTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_HA_reinitializeAccessor
             runMode = dynamic
             threadGroups = accessorThreads;

INITTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_HA_reinitializeDataStore
             runMode = dynamic
             threadGroups = dataStoreThreads;

INITTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_waitForMyStartupRecovery
             threadGroups = dataStoreThreads;

TASK         taskClass   = parReg.ParRegTest  taskMethod = HydraTask_doRROpsAndVerify
             threadGroups = dataStoreThreads, accessorThreads;

CLOSETASK   taskClass   = parReg.ParRegTest  taskMethod = HydraTask_prepareForValidation;

CLOSETASK    taskClass   = parReg.ParRegUtil  taskMethod = HydraTask_rebalance
             threadGroups = dataStoreThreads;

CLOSETASK   taskClass   = parReg.ParRegTest  taskMethod = HydraTask_validatePR;

hydra.GemFirePrms-stopSystemsAfterTest = true;

hydra.Prms-totalTaskTimeSec = 1800;
hydra.Prms-maxResultWaitSec = 600;
hydra.Prms-serialExecution = true;
hydra.Prms-roundRobin = true;
hydra.Prms-clientShutdownHook = parReg.ParRegUtil dumpAllPartitionedRegions;

INCLUDE $JTESTS/util/randomValues.inc;
util.RandomValuesPrms-objectType = byte[];
util.RandomValuesPrms-elementSize = 1000;
util.ValueHolderPrms-useExtraObject = true;
hydra.GemFirePrms-conserveSockets = ONEOF true false FOENO;

parReg.ParRegPrms-numOpsPerTask = 100;

// The following 3 parameters specify random operations.
// The test will try to hold the size of the region to regionSizeThreshold.
// If the region grows larger in size than regionSizeThreshold, then the
// operations will be chosen from thresholdOperations, otherwise the operations
// will be chosen from entryOperations
// lynn - localDestroy/localInvalidate not supported for partitioned regions (for now)
parReg.ParRegPrms-entryOperations = ONEOF add putAll putIfAbsentAsCreate getNew getNew update invalidate get destroy 
                                          putIfAbsent remove replaceNoInval replaceOldNoInval FOENO; 
parReg.ParRegPrms-numPutAllNewKeys = useThreshold;
parReg.ParRegPrms-numPutAllExistingKeys = RANGE 0 50 EGNAR;

parReg.ParRegPrms-upperThreshold = 500;
parReg.ParRegPrms-upperThresholdOperations = ONEOF destroy remove FOENO;
parReg.ParRegPrms-lowerThreshold = 0;
parReg.ParRegPrms-lowerThresholdOperations = ONEOF add putAll getNew putIfAbsentAsCreate FOENO;
parReg.ParRegPrms-highAvailability = true;
parReg.ParRegPrms-numVMsToStop = ${numVMsToStop};
parReg.ParRegPrms-localMaxMemory = RANGE 1 10 EGNAR;
util.StopStartPrms-stopModes = ONEOF NICE_EXIT MEAN_KILL MEAN_EXIT NICE_KILL FOENO;
util.CachePrms-useDeclarativeXmlFile = ONEOF true false FOENO;
