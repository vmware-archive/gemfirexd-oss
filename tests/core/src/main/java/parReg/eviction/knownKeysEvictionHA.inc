hydra.Prms-testDescription = "
This test creates a partitioned region, then does various entry operations on a 
known key range, while VMs are going up and down in a configuration that should 
result in no data loss. Values are checked for correctness in the close task.
";

INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_p2p_2.inc;

hydra.VmPrms-extraVMArgs   = fcn "hydra.TestConfigFcns.duplicate
                                  (\"-Xmx128m \", ${${A}Hosts}, true)"
                             ncf
                             ,
                             fcn "hydra.TestConfigFcns.duplicate
                                  (\"-Xms512m -Xmx512m \", ${${B}Hosts}, true)"
                             ncf;


// this test uses tasks from getInitialImage tests to load the region,
// do operations and verify the region contents 

// one verify thread per accessor and datastore vm to verify each vm's view
THREADGROUP verifyThreads 
            totalThreads = fcn "(${${A}Hosts} * ${${A}VMsPerHost}) +
                                (${${B}Hosts} * ${${B}VMsPerHost})" ncf
            totalVMs     = fcn "(${${A}Hosts} * ${${A}VMsPerHost}) +
                                (${${B}Hosts} * ${${B}VMsPerHost})" ncf;

// accessorThreads are all threads in the accessor VMs minus 1 thread for the controller
// thread, minus one thread per accessor VM for the verifyThreads
THREADGROUP accessorVMThreads 
            totalThreads = fcn (${${A}Hosts} * ${${A}VMsPerHost} * ${${A}ThreadsPerVM}) - 1
                               - (${${A}Hosts} * ${${A}VMsPerHost}) ncf  
            totalVMs     = fcn ${${A}Hosts} * ${${A}VMsPerHost} ncf
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${A}\", 
                                ${${A}Hosts} * ${${A}VMsPerHost}, true)" ncf;

// dataStoreThreads are all threads in the dataStore VMs minus 
// the one thread per dataStore VM for the verifyThreads
THREADGROUP dataStoreVMThreads 
            totalThreads = fcn (${${B}Hosts} * ${${B}VMsPerHost} * ${${B}ThreadsPerVM}) 
                               - (${${B}Hosts} * ${${B}VMsPerHost}) ncf  
            totalVMs     = fcn ${${B}Hosts} * ${${B}VMsPerHost} ncf
            clientNames  = fcn "hydra.TestConfigFcns.generateNames(\"${B}\", 
                                ${${B}Hosts} * ${${B}VMsPerHost}, true)" ncf;

// one thread from an accessor VM (it's the only thread not mapped to a thread group at this point)
THREADGROUP controllerThread totalThreads = 1 totalVMs = 1;

// to avoid bug 34430, scope must be ack
hydra.Prms-useFixedRandomInMaster= true;
hydra.RegionPrms-names          = accessorRegion       dataStoreRegion;
hydra.RegionPrms-regionName     = partitionedRegion    partitionedRegion;
hydra.RegionPrms-dataPolicy     = partition;
hydra.RegionPrms-partitionName  = accessorPR           dataStorePR;

hydra.PartitionPrms-names           = accessorPR           dataStorePR;
hydra.PartitionPrms-localMaxMemory  = 0                    default;
hydra.PartitionPrms-redundantCopies = ${redundantCopies}   ${redundantCopies};

STARTTASK    taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod = StartTask_initialize;

INITTASK     taskClass   = util.StopStartVMs  taskMethod = StopStart_initTask
             runMode = once
             threadGroups = accessorVMThreads, dataStoreVMThreads;

INITTASK     taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod = HydraTask_HA_dataStoreInitialize
             threadGroups = dataStoreVMThreads
             runMode = always;

INITTASK     taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod = HydraTask_HA_accessorInitialize
             threadGroups = accessorVMThreads;

INITTASK     taskClass   =  parReg.execute.ExecutionAndColocationTest  taskMethod = HydraTask_waitForStartupRecovery
             threadGroups = dataStoreVMThreads;

INITTASK     taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod = HydraTask_loadRegion
             threadGroups = accessorVMThreads
             batch;

INITTASK     taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod  = HydraTask_verifyPRMetaData
             threadGroups = verifyThreads;

INITTASK     taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod = HydraTask_verifyRegionSize
             threadGroups = verifyThreads
             batch;
 
TASK         taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod = HydraTask_doOps
             threadGroups = accessorVMThreads;
 
TASK         taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod = HydraTask_stopStartVms
             threadGroups = controllerThread;

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

CLOSETASK    taskClass     = parReg.ParRegUtil  taskMethod  = HydraTask_rebalance
             threadGroups = dataStoreVMThreads;

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

