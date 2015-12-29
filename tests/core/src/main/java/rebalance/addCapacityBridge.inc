hydra.Prms-testRequirement = "Verify that capacity can be added to a PR via rebalancing";
hydra.Prms-testDescription = "
Fill a PR to capacity, then add a new vm and rebalance. Verify that there is now more
capacity available.
";

INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_4.inc;
hydra.GemFirePrms-distributedSystem =
                              fcn "hydra.TestConfigFcns.duplicate
                                   (\"bridgeds\", ${adminHosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.duplicate
                                   (\"loner\", ${edgeHosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.duplicate
                                   (\"bridgeds\", ${bridgeHosts})"
                              ncf
                              fcn "hydra.TestConfigFcns.duplicate
                                   (\"bridgeds\", ${extraBridgeHosts})"
                              ncf;


THREADGROUP adminThreads
    totalThreads = fcn
                   ${${A}Hosts} * ${${A}VMsPerHost} * ${${A}ThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"${A}\", ${${A}Hosts}, true)"
                   ncf;
THREADGROUP edgeThreads
    totalThreads = fcn
                   ${${B}Hosts} * ${${B}VMsPerHost} * ${${B}ThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"${B}\", ${${B}Hosts}, true)"
                   ncf;
THREADGROUP bridgeThreads
    totalThreads = fcn
                   ${${C}Hosts} * ${${C}VMsPerHost} * ${${C}ThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"${C}\", ${${C}Hosts}, true)"
                   ncf;
THREADGROUP extraBridgeThreads
    totalThreads = fcn
                   ${${D}Hosts} * ${${D}VMsPerHost} * ${${D}ThreadsPerVM}
                   ncf
    clientNames  = fcn "hydra.TestConfigFcns.generateNames
                        (\"${D}\", ${${D}Hosts}, true)"
                   ncf;

INITTASK     taskClass   = rebalance.CapacityTest  taskMethod = HydraTask_initEdge
             threadGroups = edgeThreads;

INITTASK     taskClass   = rebalance.CapacityTest  taskMethod = HydraTask_initBridge
             threadGroups = bridgeThreads;

INITTASK     taskClass     = util.AdminHelper  taskMethod = HydraTask_initializeAdminDS
             threadGroups = adminThreads;

// Bring a new vm into the ds one at a time; this is a serial execution test
// with round robin
TASK         taskClass   = rebalance.CapacityTest  taskMethod = HydraTask_addCapacity
             threadGroups = extraBridgeThreads
             maxTimesToRun = fcn ${extraBridgeHosts} * ${extraBridgeVMsPerHost} ncf;


hydra.Prms-totalTaskTimeSec = 1200;
hydra.Prms-maxResultWaitSec = 600;
hydra.Prms-serialExecution = true;
hydra.Prms-roundRobin = true;

INCLUDE $JTESTS/util/randomValues.inc;
util.RandomValuesPrms-objectType = byte[];
util.RandomValuesPrms-elementSize = 1000;
util.ValueHolderPrms-useExtraObject = true;
hydra.GemFirePrms-conserveSockets = ONEOF true false FOENO;

hydra.CachePrms-names           = cache1;
hydra.CachePrms-searchTimeout   = 600;

util.TestHelperPrms-minTaskGranularitySec = 30;

hydra.RegionPrms-names          = accessorRegion     dataStoreRegion;
hydra.RegionPrms-regionName     = partitionedRegion  partitionedRegion;
hydra.RegionPrms-scope          = local              default;
hydra.RegionPrms-dataPolicy     = default            partition;
hydra.RegionPrms-partitionName  = none               pr;
hydra.RegionPrms-poolName       = edgeDescript       none;


hydra.PartitionPrms-names           = pr;
hydra.PartitionPrms-redundantCopies = 3;
hydra.PartitionPrms-localMaxMemory = 5;


util.CachePrms-useDeclarativeXmlFile = ONEOF true false FOENO;
util.AdminHelperPrms-alertListener = rebalance.CapacityAdminListener;

// define the edge clients
hydra.PoolPrms-names                  = edgeDescript;
hydra.PoolPrms-minConnections         = 2;
hydra.PoolPrms-subscriptionEnabled    = true;
hydra.PoolPrms-threadLocalConnections = false;
hydra.PoolPrms-readTimeout            = 3600000; // hydra will hang first
hydra.PoolPrms-pingInterval           = 400;
hydra.PoolPrms-loadConditioningInterval     = 5000;

// define the bridge servers
hydra.BridgePrms-names                = bridge;


