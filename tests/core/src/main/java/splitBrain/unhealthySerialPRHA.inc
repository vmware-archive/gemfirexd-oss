INITTASK     taskClass   = util.StopStartVMs  taskMethod = StopStart_initTask
             threadGroups = dataStoreThreads, accessorThreads, adminThread;

INITTASK     taskClass   = util.PRObserver  taskMethod = initialize
             runMode = once
             threadGroups = dataStoreThreads;

INITTASK     taskClass   = splitBrain.SBParRegTest  taskMethod = HydraTask_HA_initializeDataStore
             runMode = once
             threadGroups = dataStoreThreads;

INITTASK     taskClass   = splitBrain.SBParRegTest  taskMethod = HydraTask_HA_initializeAccessor
             runMode = once
             threadGroups = accessorThreads;

INITTASK     taskClass   = splitBrain.SBParRegTest  taskMethod = HydraTask_HA_reinitializeAccessor
             runMode = dynamic
             threadGroups = accessorThreads;

INITTASK     taskClass   = splitBrain.SBParRegTest  taskMethod = HydraTask_HA_reinitializeDataStore
             runMode = dynamic
             threadGroups = dataStoreThreads;

INITTASK     taskClass     = util.AdminHelper  taskMethod = HydraTask_initializeAdminDS
             threadGroups = adminThread;

INITTASK     taskClass   = splitBrain.SBParRegTest  taskMethod = HydraTask_waitForStartupRecovery
             threadGroups = dataStoreThreads;

TASK         taskClass   = parReg.ParRegTest  taskMethod = HydraTask_doRROpsAndVerify
             threadGroups = dataStoreThreads, accessorThreads;

hydra.GemFirePrms-stopSystemsAfterTest = true;
hydra.GemFirePrms-conserveSockets = ONEOF true false FOENO;

hydra.Prms-totalTaskTimeSec = 600;
hydra.Prms-maxResultWaitSec = 600;
hydra.Prms-serialExecution = true;
hydra.Prms-roundRobin = true;

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
parReg.ParRegPrms-entryOperations = ONEOF add add getNew getNew update invalidate get destroy FOENO; 
parReg.ParRegPrms-upperThreshold = 500;
parReg.ParRegPrms-upperThresholdOperations = ONEOF destroy FOENO;
parReg.ParRegPrms-lowerThreshold = 0;
parReg.ParRegPrms-lowerThresholdOperations = ONEOF add getNew FOENO;
parReg.ParRegPrms-highAvailability = true;
parReg.ParRegPrms-numVMsToStop = 1;
parReg.ParRegPrms-localMaxMemory = RANGE 1 10 EGNAR;
parReg.ParRegPrms-stopModes = ONEOF NICE_EXIT MEAN_KILL MEAN_EXIT FOENO;
util.CachePrms-useDeclarativeXmlFile = false;

hydra.GemFirePrms-ackSevereAlertThreshold = 10;
hydra.GemFirePrms-ackWaitThreshold = 15;
hydra.GemFirePrms-enableNetworkPartitionDetection = ONEOF true false FOENO;
hydra.GemFirePrms-disableAutoReconnect = true;

