STARTTASK    taskClass     = parReg.KnownKeysTest  taskMethod = StartTask_initialize;

INITTASK     taskClass   = util.StopStartVMs  taskMethod = StopStart_initTask
             runMode = once
             threadGroups = accessorVMThreads, dataStoreVMThreads;

INITTASK     taskClass     = parReg.KnownKeysTest  taskMethod = HydraTask_HA_dataStoreInitialize
             threadGroups = dataStoreVMThreads
             runMode = always;

INITTASK     taskClass     = parReg.KnownKeysTest  taskMethod = HydraTask_HA_accessorInitialize
             threadGroups = accessorVMThreads;

INITTASK     taskClass   =  parReg.KnownKeysTest  taskMethod = HydraTask_waitForStartupRecovery
             threadGroups = dataStoreVMThreads;

INITTASK     taskClass     = parReg.KnownKeysTest  taskMethod = HydraTask_loadRegion
             threadGroups = accessorVMThreads
             batch;

INITTASK     taskClass     = parReg.KnownKeysTest  taskMethod  = HydraTask_verifyPRMetaData
             threadGroups = verifyThreads;

INITTASK     taskClass     = parReg.KnownKeysTest  taskMethod = HydraTask_verifyRegionSize
             threadGroups = verifyThreads
             batch;
 
TASK         taskClass     = parReg.KnownKeysTest  taskMethod = HydraTask_doOps
             threadGroups = accessorVMThreads;
 
TASK         taskClass     = parReg.KnownKeysTest  taskMethod = HydraTask_stopStartVms
             threadGroups = controllerThread;

CLOSETASK    taskClass   = parReg.KnownKeysTest  taskMethod = HydraTask_logLocalSize
             threadGroups = dataStoreVMThreads;

CLOSETASK    taskClass     = parReg.ParRegUtil  taskMethod  = HydraTask_rebalance
             threadGroups = dataStoreVMThreads;

CLOSETASK    taskClass     = parReg.KnownKeysTest  taskMethod  = HydraTask_verifyPrimaries
             threadGroups = verifyThreads;

CLOSETASK    taskClass     = parReg.KnownKeysTest  taskMethod  = HydraTask_verifyPRMetaData
             threadGroups = verifyThreads;

CLOSETASK    taskClass     = parReg.KnownKeysTest  taskMethod  = HydraTask_verifyBucketCopiesBatched
             threadGroups = verifyThreads
             batch;

CLOSETASK    taskClass     = parReg.KnownKeysTest  taskMethod  = HydraTask_verifyRegionContents
             threadGroups = verifyThreads
             batch;

hydra.Prms-totalTaskTimeSec = 14400; // test is workload based
hydra.Prms-maxResultWaitSec = 600;
hydra.Prms-clientShutdownHook = parReg.ParRegUtil dumpAllPartitionedRegions;

util.TestHelperPrms-minTaskGranularitySec = 60;

getInitialImage.InitImagePrms-numKeys = ${numKeys};
// numNewKeys is 10% of the total number of keys
getInitialImage.InitImagePrms-numNewKeys = fcn "${numKeys} * 0.1" ncf;
getInitialImage.InitImagePrms-useCacheLoader=false;

INCLUDE $JTESTS/util/randomValues.inc;
util.RandomValuesPrms-objectType = byte[];
util.RandomValuesPrms-elementSize = ${byteArraySize};
util.ValueHolderPrms-useExtraObject = true;

hydra.GemFirePrms-conserveSockets = ONEOF true false FOENO;

hydra.CachePrms-names           = cache1;
hydra.CachePrms-searchTimeout   = 600;

parReg.ParRegPrms-numVMsToStop = RANGE 1 ${numVMsToStop} EGNAR;
util.StopStartPrms-stopModes = ONEOF MEAN_EXIT MEAN_KILL NICE_EXIT NICE_KILL FOENO;
parReg.ParRegPrms-highAvailability = true;
