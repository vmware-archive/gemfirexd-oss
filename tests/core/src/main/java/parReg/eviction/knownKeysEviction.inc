hydra.Prms-testDescription = "
This test creates a partitioned region, then does various entry operations on a known key
range. Values are checked for correctness in the close task.
";

INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_p2p.inc;

// need one verify thread per vm to verify each vm's view
THREADGROUP verifyThreads totalThreads = fcn "${peerVMsPerHost} * ${peerHosts}" ncf
                          totalVMs = fcn "${peerVMsPerHost} * ${peerHosts}" ncf;

// this test uses tasks from getInitialImage tests to load the region,
// do operations and verify the region contents 
STARTTASK    taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod = StartTask_initialize;

INITTASK     taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod = HydraTask_initialize;

INITTASK     taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod = HydraTask_loadRegion
             threadGroups = verifyThreads, default
             batch;

INITTASK     taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod  = HydraTask_verifyPRMetaData
             threadGroups = verifyThreads;

INITTASK     taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod = HydraTask_verifyRegionSize
             threadGroups = verifyThreads
             batch;
 
TASK         taskClass     = parReg.execute.ExecutionAndColocationTest  taskMethod = HydraTask_doOps
             threadGroups = verifyThreads, default;
 
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

CLOSETASK    taskClass     = parReg.ParRegUtil  taskMethod  = HydraTask_rebalance;

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

// to avoid bug 34430, scope must be ack 
hydra.RegionPrms-names          = clientRegion;
hydra.RegionPrms-regionName     = partitionedRegion;
hydra.RegionPrms-dataPolicy     = partition;
hydra.RegionPrms-partitionName  = pr;

hydra.PartitionPrms-names       = pr;

