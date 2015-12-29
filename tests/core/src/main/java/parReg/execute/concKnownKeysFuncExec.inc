STARTTASK    taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod = StartTask_initialize;

INITTASK     taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod = HydraTask_HA_dataStoreInitialize
             threadGroups = dataStoreVMThreads
             runMode = always;

INITTASK     taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod = HydraTask_HA_accessorInitialize
             threadGroups = accessorVMThreads;
             
INITTASK     taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod = HydraTask_initRegisterFunction
             threadGroups = dataStoreVMThreads,accessorVMThreads
             runMode = always;             

INITTASK     taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod = HydraTask_loadRegion
             threadGroups = accessorVMThreads
             batch;

INITTASK     taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod  = HydraTask_verifyPRMetaData
             threadGroups = verifyThreads;

INITTASK     taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod = HydraTask_verifyRegionSize
             threadGroups = verifyThreads
             batch;
                          
INITTASK         taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod = HydraTask_waitForEventsReceival
             threadGroups = accessorVMThreads;             
 
TASK         taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod = HydraTask_doFunctionExecution
             threadGroups = accessorVMThreads;

CLOSETASK    taskClass   = parReg.execute.ExecutionAndColocationTest  taskMethod = HydraTask_logLocalSize
             threadGroups = dataStoreVMThreads;

CLOSETASK    taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod  = HydraTask_verifyPrimaries
             threadGroups = verifyThreads;

CLOSETASK    taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod  = HydraTask_verifyPRMetaData
             threadGroups = verifyThreads;

CLOSETASK    taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod  = HydraTask_verifyBucketCopiesBatched
             threadGroups = verifyThreads
             batch;

CLOSETASK    taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod  = HydraTask_verifyRegionContents
             threadGroups = verifyThreads
             batch;
             
CLOSETASK    taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod  = HydraTask_putKeySetInBB
             threadGroups = verifyThreads
			 ;   
			 
CLOSETASK    taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod = HydraTask_waitForEventsReceival
             threadGroups = accessorVMThreads;			           
                                   
CLOSETASK    taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod  = HydraTask_executeFunctionAllKeys
             threadGroups = accessorVMThreads
			 ;   
			 
CLOSETASK    taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod  = HydraTask_executeFunctionPrimaryBuckets
             threadGroups = accessorVMThreads
			 ;			           

hydra.Prms-totalTaskTimeSec = 14400; // test is workload based
hydra.Prms-maxResultWaitSec = 600;

hydra.Prms-clientShutdownHook = parReg.execute.ExecutionAndColocationTest shutdownHook;

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
