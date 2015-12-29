hydra.Prms-testDescription = "
This test creates random CQs, loads a set of keys, does known operations to 
key intervals, then validates the CQs and initialResultSet using a partitioned region.
During the TASK phase, rebalance is also run concurrently.  Data sizes vary in order
to create imbalance during doOps().
";

INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_hct_2_locator.inc;

hydra.VmPrms-extraClassPaths =
   fcn "hydra.TestConfigFcns.duplicate(\"$GEMFIRE/lib/antlr.jar\", ${initialbridgeHosts} + ${newCapacitybridgeHosts})" ncf;

THREADGROUP locator
    totalThreads = fcn ${locatorHosts} * ${locatorVMsPerHost}
                                       * ${locatorThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"locator\", ${locatorHosts}, true)"
                   ncf;

// bridgeServer are divided into two subgroups
// A = newCapacityVM(s), B = original dataStore BridgeServers
THREADGROUP newCapacityThreads
  totalThreads = fcn
                 ${newCapacitybridgeHosts} * ${newCapacitybridgeVMsPerHost} * ${newCapacitybridgeThreadsPerVM} - (${newCapacitybridgeHosts} * ${newCapacitybridgeVMsPerHost})
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames
                      (\"newCapacitybridge\", ${newCapacitybridgeHosts}, true)"
                 ncf;

THREADGROUP oneThreadPerNewCapacityBridge
  totalThreads = fcn
                 ${newCapacitybridgeHosts} * ${newCapacitybridgeVMsPerHost}
                 ncf
  totalVMs = fcn ${newCapacitybridgeHosts} * ${newCapacitybridgeVMsPerHost} ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames
                      (\"newCapacitybridge\", ${newCapacitybridgeHosts}, true)"
                 ncf;

THREADGROUP bridgeThreads
  totalThreads = fcn
                 ${initialbridgeHosts} * ${initialbridgeVMsPerHost} * ${initialbridgeThreadsPerVM} -
                 (${initialbridgeHosts} * ${initialbridgeVMsPerHost})
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames
                      (\"initialbridge\", ${initialbridgeHosts}, true)"
                 ncf;

THREADGROUP oneThreadPerBridgeVM
  totalThreads = fcn
                 ${initialbridgeHosts} * ${initialbridgeVMsPerHost}
                 ncf
  totalVMs = fcn ${initialbridgeHosts} * ${initialbridgeVMsPerHost} ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames
                      (\"initialbridge\", ${initialbridgeHosts}, true)"
                 ncf;

// edge clients
THREADGROUP edgeThreads
  totalThreads = fcn
                 ${edgeHosts} * ${edgeVMsPerHost} * ${edgeThreadsPerVM} -
                 (${edgeHosts} * ${edgeVMsPerHost})
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames
                      (\"edge\", ${edgeHosts}, true)"
                 ncf;         

THREADGROUP oneThreadPerEdgeVM
  totalThreads = fcn
                 ${edgeHosts} * ${edgeVMsPerHost}
                 ncf
  totalVMs = fcn ${edgeHosts} * ${edgeVMsPerHost} ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames
                      (\"edge\", ${edgeHosts}, true)"
                 ncf;         

STARTTASK    taskClass     = cq.CQKnownKeysTest  taskMethod = StartTask_initialize;

/**
 * Starts the locator and connects to admin-only distributed systems.
 */
INITTASK taskClass = rebalance.RebalanceUtil taskMethod = createLocatorTask
         threadGroups = locator;

INITTASK taskClass = rebalance.RebalanceUtil taskMethod = startAndConnectLocatorTask
         threadGroups = locator;

INITTASK     taskClass   = cq.CQKnownKeysTest  taskMethod = HydraTask_initializeBridgeServer
             threadGroups = oneThreadPerBridgeVM;

INITTASK     taskClass   = cq.CQKnownKeysTest  taskMethod = HydraTask_initializeClient
             threadGroups = oneThreadPerEdgeVM;

INITTASK     taskClass     = cq.CQKnownKeysTest  taskMethod = HydraTask_loadRegion
             threadGroups = bridgeThreads, edgeThreads, oneThreadPerBridgeVM, oneThreadPerEdgeVM
             batch;

INITTASK     taskClass     = cq.CQKnownKeysTest  taskMethod  = HydraTask_waitForSilence
             threadGroups = oneThreadPerEdgeVM;

INITTASK     taskClass     = cq.CQKnownKeysTest  taskMethod  = HydraTask_verifyQueries
             threadGroups = oneThreadPerEdgeVM
             batch;

INITTASK     taskClass   = cq.CQKnownKeysTest  taskMethod = HydraTask_initializeBridgeServer
             threadGroups = oneThreadPerNewCapacityBridge;

TASK         taskClass     = cq.CQKnownKeysTest  taskMethod = HydraTask_doOps
             threadGroups = bridgeThreads, edgeThreads, oneThreadPerBridgeVM, oneThreadPerEdgeVM, newCapacityThreads;

CLOSETASK    taskClass     = cq.CQKnownKeysTest  taskMethod  = HydraTask_waitForSilence
             threadGroups = oneThreadPerEdgeVM;

CLOSETASK    taskClass     = cq.CQKnownKeysTest  taskMethod  = HydraTask_verifyRegionContents
             threadGroups = oneThreadPerBridgeVM, oneThreadPerEdgeVM
             batch;

CLOSETASK    taskClass     = cq.CQKnownKeysTest  taskMethod  = HydraTask_verifyQueries
             threadGroups = oneThreadPerEdgeVM
             batch;

hydra.Prms-totalTaskTimeSec = 14400; // test is workload based
hydra.Prms-maxResultWaitSec = 900;
hydra.Prms-manageLocatorAgents = false;   // disable Master managed locators

util.TestHelperPrms-minTaskGranularitySec = 60;

INCLUDE $JTESTS/util/randomValues.inc;
util.RandomValuesPrms-objectType = byte[];
// vary dataSizes to create imbalance during doOps
util.RandomValuesPrms-elementSize = ONEOF 50 500 5000 10000 20000 FOENO;
util.RandomValuesPrms-borderCasePercentage = 0;

cq.CQUtilPrms-numKeys = ${numKeys};
cq.CQUtilPrms-numNewKeys = fcn "${numKeys} * 0.1" ncf; // numNewKeys is 10% of the total number of keys
cq.CQUtilPrms-numQueriesPerClientVM = 10; 
cq.CQUtilPrms-QueryServicePoolName = qservice;
cq.CQUtilPrms-QueryServiceUsingPool = ONEOF true false FOENO;

hydra.GemFirePrms-conserveSockets = ONEOF true false FOENO;

hydra.CachePrms-names           = cache1;
hydra.CachePrms-searchTimeout   = 600;

hydra.Prms-useFixedRandomInMaster = false;
hydra.RegionPrms-names          = clientRegion            serverRegion;
hydra.RegionPrms-regionName     = testRegion              testRegion;
hydra.RegionPrms-scope          = local                   default;
hydra.RegionPrms-dataPolicy     = normal                  partition;
hydra.RegionPrms-poolName    = edgeDescript            none;
hydra.RegionPrms-cacheListeners = util.SilenceListener,   hct.BridgeEventListener;
hydra.RegionPrms-partitionName  = none                    serverPR;
hydra.PartitionPrms-names           = serverPR;
hydra.PartitionPrms-localMaxMemory  = default;
hydra.PartitionPrms-redundantCopies = 1;
// depend on rebalancing (no recovery)
hydra.PartitionPrms-startupRecoveryDelay = -1;

// define the edge clients
hydra.PoolPrms-names                       = edgeDescript qservice;
hydra.PoolPrms-minConnections        = 2;
hydra.PoolPrms-subscriptionEnabled = true;
hydra.PoolPrms-threadLocalConnections         = true;
hydra.PoolPrms-readTimeout                 = 60000;
hydra.PoolPrms-subscriptionRedundancy             = RANGE 0
                                                   fcn ${initialbridgeHosts} * ${initialbridgeVMsPerHost} ncf
                                             EGNAR;

// define the bridge servers
hydra.BridgePrms-names                = bridge;


rebalance.RebalancePrms-resourceObserver = rebalance.RebalanceResourceObserver;





