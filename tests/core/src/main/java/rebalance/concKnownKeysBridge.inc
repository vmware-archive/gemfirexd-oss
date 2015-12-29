
// this test uses tasks from getInitialImage tests to load the region,
// do operations and verify the region contents 
STARTTASK    taskClass     = parReg.KnownKeysTest  taskMethod = StartTask_initialize;

/**
 * Starts the locator and connects to admin-only distributed systems.
 */
INITTASK     taskClass = rebalance.RebalanceUtil taskMethod = createLocatorTask
             threadGroups = locator;

INITTASK     taskClass = rebalance.RebalanceUtil taskMethod = startAndConnectLocatorTask
             threadGroups = locator;

INITTASK     taskClass     = parReg.KnownKeysTest  taskMethod = HydraTask_dataStoreInitialize
             threadGroups = oneThreadPerBridgeVM;

// Non-WAN tests will simply return without doing anything from this task
INITTASK     taskClass     = parReg.KnownKeysTest  taskMethod = HydraTask_startGatewayHub
             threadGroups = oneThreadPerBridgeVM;

INITTASK     taskClass     = parReg.KnownKeysTest  taskMethod = HydraTask_accessorInitialize
             threadGroups = oneThreadPerEdgeVM;

INITTASK     taskClass     = parReg.KnownKeysTest  taskMethod = HydraTask_loadRegion
             threadGroups = edgeThreads, oneThreadPerEdgeVM 
             batch;

INITTASK     taskClass     = parReg.KnownKeysTest  taskMethod = HydraTask_waitForSilence
             threadGroups  = oneThreadPerEdgeVM ;

INITTASK     taskClass     = parReg.KnownKeysTest  taskMethod  = HydraTask_verifyPRMetaData
             threadGroups = oneThreadPerBridgeVM;

INITTASK     taskClass     = parReg.KnownKeysTest  taskMethod = HydraTask_verifyRegionSize
             threadGroups = oneThreadPerBridgeVM
             batch;

TASK         taskClass     = parReg.KnownKeysTest  taskMethod = HydraTask_doOps
             threadGroups = bridgeThreads, oneThreadPerBridgeVM, edgeThreads, oneThreadPerEdgeVM, newCapacityThreads;

CLOSETASK    taskClass     = parReg.KnownKeysTest  taskMethod = HydraTask_waitForSilence
             threadGroups  = oneThreadPerEdgeVM ;

CLOSETASK    taskClass   = parReg.KnownKeysTest  taskMethod = HydraTask_logLocalSize
             threadGroups = oneThreadPerBridgeVM;

CLOSETASK    taskClass     = parReg.KnownKeysTest  taskMethod  = HydraTask_verifyPrimaries
             threadGroups = oneThreadPerBridgeVM;

CLOSETASK    taskClass     = parReg.KnownKeysTest  taskMethod  = HydraTask_verifyPRMetaData
             threadGroups = oneThreadPerBridgeVM;

CLOSETASK    taskClass     = parReg.KnownKeysTest  taskMethod  = HydraTask_verifyBucketCopiesBatched
             threadGroups = oneThreadPerBridgeVM 
             batch;

CLOSETASK    taskClass     = parReg.KnownKeysTest  taskMethod  = HydraTask_verifyRegionContents
             threadGroups = oneThreadPerBridgeVM, oneThreadPerEdgeVM 
             batch;

hydra.Prms-totalTaskTimeSec = 14400; // test is workload based
hydra.Prms-maxResultWaitSec = 900;
hydra.Prms-clientShutdownHook = parReg.ParRegUtil dumpAllPartitionedRegions;

util.TestHelperPrms-minTaskGranularitySec = 60;

getInitialImage.InitImagePrms-numKeys = ${numKeys};
// numNewKeys is 10% of the total number of keys
getInitialImage.InitImagePrms-numNewKeys = fcn "${numKeys} * 0.1" ncf;
getInitialImage.InitImagePrms-useCacheLoader=false;

INCLUDE $JTESTS/util/randomValues.inc;
util.RandomValuesPrms-objectType = byte[];
util.RandomValuesPrms-elementSize = ONEOF 50 100 500 1000 2500 5000 FOENO;
util.ValueHolderPrms-useExtraObject = true;

hydra.GemFirePrms-conserveSockets = ONEOF true false FOENO;

hydra.CachePrms-names           = cache1;
hydra.CachePrms-searchTimeout   = 600;

hydra.RegionPrms-poolName           = edgeDescript           none;

// define the edge clients
hydra.PoolPrms-names                  = edgeDescript;
hydra.PoolPrms-minConnections         = 2;
hydra.PoolPrms-subscriptionEnabled           = true;
hydra.PoolPrms-threadLocalConnections = false;
hydra.PoolPrms-readTimeout            = 3600000; // hydra will hang first
hydra.PoolPrms-subscriptionRedundancy   = 1;
hydra.PoolPrms-pingInterval           = 400;
hydra.PoolPrms-loadConditioningInterval     = 5000;

// define the bridge servers
hydra.BridgePrms-names                = bridge;


