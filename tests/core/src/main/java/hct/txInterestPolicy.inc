INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_hct.inc;

hydra.Prms-testTopology = "Hierarchical cache topology divided into two named subgroups, bridge and edge.  Subgroup bridge has ${bridgeHosts} hosts, ${bridgeVMsPerHost} VMs per host, and 1 thread per VM.  Subgroup edge has ${edgeHosts} hosts, ${edgeVMsPerHost} VMs per host, with one VM containing ${numOpThreads} opThreads and the rest only 1 thread.";

// overrides the topology with respect to the number of threads per VM
hydra.ClientPrms-vmThreads =
  fcn "hydra.TestConfigFcns.duplicate(\"1\", ${bridgeHosts})" ncf // bridges
  ${numOpThreads} // edge1
  1;              // edge2...edgeN

// Servers need antlr jar for query support
hydra.VmPrms-extraClassPaths =
   fcn "hydra.TestConfigFcns.duplicate(\"$GEMFIRE/lib/antlr.jar\", ${bridgeHosts}, true)" ncf;

THREADGROUP bridge
  totalThreads = fcn ${bridgeHosts} * ${bridgeVMsPerHost} ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames
                      (\"bridge\", ${bridgeHosts}, true)" ncf;
THREADGROUP opThreads
  totalThreads = ${numOpThreads}  totalVMs = 1  clientNames = edge1;

hydra.CachePrms-names           = bridge                   edge;
hydra.RegionPrms-names          = bridge                   edge;
hydra.RegionPrms-scope          = ack                      local;
hydra.RegionPrms-dataPolicy     = replicate                default;
hydra.RegionPrms-cacheListeners = hct.BridgeEventListener, util.SilenceListener;
hydra.RegionPrms-poolName    = none                     brloader;

hydra.BridgePrms-names          = bridge;

hydra.BridgePrms-maxThreads = ONEOF default 16 FOENO; // selector control

hydra.PoolPrms-names            = brloader;
hydra.PoolPrms-minConnections = 2;
hydra.PoolPrms-subscriptionEnabled = true;
hydra.PoolPrms-threadLocalConnections = true;
hydra.PoolPrms-readTimeout = 60000;
hydra.PoolPrms-subscriptionRedundancy = 0;

STARTTASK    taskClass   = hct.TXInterestPolicyTest taskMethod = StartTask_initialize
             clientNames = edge1;

INITTASK     taskClass   = hct.InterestPolicyTest  taskMethod = initBridgeServer
             hydra.ConfigPrms-cacheConfig  = bridge
             hydra.ConfigPrms-regionConfig = bridge
             hydra.ConfigPrms-bridgeConfig = bridge
             threadGroups = bridge
             runMode = always ;

INITTASK     taskClass   = hct.InterestPolicyTest  taskMethod = HydraTask_initialize
             hydra.ConfigPrms-cacheConfig  = edge
             hydra.ConfigPrms-regionConfig = edge
             threadGroups = opThreads, default
             runMode = always ;

INITTASK     taskClass   = hct.InterestPolicyTest taskMethod = HydraTask_loadRegion
             threadGroups = opThreads;

CLOSETASK   taskClass   = cq.CQUtil    taskMethod = HydraTask_verifyCQListenerInvoked
            threadGroups = default;

hydra.Prms-totalTaskTimeSec = 300;
hydra.Prms-maxResultWaitSec = 600;
hydra.Prms-errorOnFailedGFStartup = false;
hydra.GemFirePrms-conserveSockets = true;

INCLUDE $JTESTS/util/randomValues.inc;
util.RandomValuesPrms-borderCasePercentage = 0;
util.RandomValuesPrms-objectType = java.lang.Integer;
util.RandomValuesPrms-valueType = java.lang.Integer;
util.ValueHolderPrms-useExtraObject = false;

hct.HctPrms-blackboardType = "RMI";
hct.HctPrms-blackboardName = "HctBlackboard";

// this is the number of keys divided among the key intervals
getInitialImage.InitImagePrms-numKeys = 10000;
getInitialImage.InitImagePrms-numNewKeys = 200;
getInitialImage.InitImagePrms-useCacheLoader = false; // we must use a bridge loader
getInitialImage.InitImagePrms-useTransactions = true;

hct.HctPrms-resultPolicy = keys keysValues none;
