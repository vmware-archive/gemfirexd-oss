INITTASK     taskClass   = util.StopStartVMs  taskMethod = StopStart_initTask;

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
             threadGroups = accessorThreads
             runMode = dynamic;

INITTASK     taskClass   = parReg.ParRegTest  taskMethod = HydraTask_HA_reinitializeDataStore
             threadGroups = dataStoreThreads
             runMode = dynamic;

INITTASK     taskClass   =  parReg.ParRegTest  taskMethod = HydraTask_waitForMyStartupRecovery
             threadGroups = dataStoreThreads;

// maxThreads of 1 means only one thread will be running the HAController at a time
TASK         taskClass   = parReg.ParRegTest  taskMethod = HydraTask_HAController
             threadGroups = dataStoreThreads, accessorThreads
             maxThreads = 1;

TASK         taskClass   = parReg.ParRegTest  taskMethod = HydraTask_HADoEntryOps
             threadGroups = dataStoreThreads, accessorThreads;

CLOSETASK    taskClass   = parReg.ParRegUtil  taskMethod = HydraTask_rebalance
             threadGroups = dataStoreThreads;

hydra.GemFirePrms-stopSystemsAfterTest = true;

hydra.Prms-totalTaskTimeSec = 31536000;  // don't let hydra terminate based on time
hydra.Prms-maxResultWaitSec = 1020;  // 17 minutes to avoid timeouts on slow machines
hydra.Prms-serialExecution = false;
hydra.Prms-clientShutdownHook = parReg.ParRegUtil dumpAllPartitionedRegions;

// this is the granularity for pausing
util.TestHelperPrms-minTaskGranularitySec = 10;

INCLUDE $JTESTS/util/randomValues.inc;
util.RandomValuesPrms-objectType = byte[];
util.RandomValuesPrms-elementSize = 1000;
util.ValueHolderPrms-useExtraObject = true;
hydra.GemFirePrms-conserveSockets = ONEOF true false FOENO;

hydra.CachePrms-names           = cache1;
hydra.CachePrms-searchTimeout   = 600;

// The following 3 parameters specify random operations.
// The test will try to hold the size of the region to regionSizeThreshold.
// If the region grows larger in size than regionSizeThreshold, then the
// operations will be chosen from thresholdOperations, otherwise the operations
// will be chosen from entryOperations
// lynn - localDestroy/localInvalidate not supported for partitioned regions (for now)
parReg.ParRegPrms-entryOperations = ONEOF add putAll putIfAbsent remove replaceNoInval replaceOldNoInval getNew getNew update invalidate get destroy FOENO; 
parReg.ParRegPrms-upperThreshold = 500;
parReg.ParRegPrms-upperThresholdOperations = ONEOF destroy remove FOENO;
parReg.ParRegPrms-lowerThreshold = 0;
parReg.ParRegPrms-lowerThresholdOperations = ONEOF add getNew putIfAbsentAsCreate FOENO;
parReg.ParRegPrms-numPutAllNewKeys = 1;
parReg.ParRegPrms-numPutAllExistingKeys = RANGE 1 100 EGNAR;

parReg.ParRegPrms-secondsToRun = 1800; // this is the time for the whole run
parReg.ParRegPrms-numVMsToStop = ${numVMsToStop};
parReg.ParRegPrms-localMaxMemory = RANGE 1 10 EGNAR;
util.StopStartPrms-stopModes = ONEOF NICE_EXIT MEAN_KILL MEAN_EXIT NICE_KILL FOENO;
parReg.ParRegPrms-highAvailability = true;
util.CachePrms-useDeclarativeXmlFile = true;
