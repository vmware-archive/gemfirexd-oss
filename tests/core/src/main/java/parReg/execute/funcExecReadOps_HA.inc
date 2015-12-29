STARTTASK    taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod = StartTask_initialize;

INITTASK     taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod = HydraTask_HA_dataStoreInitialize
             threadGroups = dataStoreVMThreads
             runMode = always;

INITTASK     taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod = HydraTask_HA_accessorInitialize
             threadGroups = accessorVMThreads;
                        
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
             
INITTASK    taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod  = HydraTask_putKeySetInBB
            threadGroups = verifyThreads
			 ;   
			 
TASK    taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod  = HydraTask_executeFunctionAllKeysHA
             threadGroups = accessorVMThreads
			 ; 			                   
 
//Adding task for the fail over

TASK         taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod = HydraTask_stopStartVms
             threadGroups = controllerThread
             startInterval = 5;             

hydra.Prms-totalTaskTimeSec = 600;
hydra.Prms-maxResultWaitSec = 900;

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

parReg.ParRegPrms-numVMsToStop = ${numVMsToStop};
util.StopStartPrms-stopModes = ONEOF MEAN_EXIT MEAN_KILL NICE_EXIT NICE_KILL FOENO;
parReg.ParRegPrms-highAvailability = true;
