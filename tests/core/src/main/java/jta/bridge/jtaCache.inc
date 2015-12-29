hydra.Prms-testRequirement = 
"Test entry put/get/update events on cache regions in 
muti-threaded and transactional environment";

hydra.Prms-testDescription = "This test concurrently executes 
put/get/update operation on cache regions in multi-threded transactional environment";

INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/topology_hct.inc;

THREADGROUP bridge
  totalThreads = fcn
                 ${bridgeHosts} * ${bridgeVMsPerHost} * ${bridgeThreadsPerVM}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames
                      (\"bridge\", ${bridgeHosts}, true)"
                 ncf;
THREADGROUP edge
  totalThreads = fcn
                 ${edgeHosts} * ${edgeVMsPerHost} * ${edgeThreadsPerVM}
                 ncf
  clientNames  = fcn "hydra.TestConfigFcns.generateNames
                      (\"edge\", ${edgeHosts}, true)"
                 ncf;

// create bridge, initialize region, start server
INITTASK     taskClass  = jta.JtaCacheTestUtil taskMethod = HydraTask_initializeBridgeServer
             hydra.ConfigPrms-bridgeConfig   = bridge
             hydra.ConfigPrms-cacheConfig    = bridge
             hydra.ConfigPrms-regionConfig   = bridge
             threadGroups = bridge;

//shutdown bridgeServer gracefully
CLOSETASK    taskClass   = hydra.BridgeHelper taskMethod  = stopBridgeServer
             threadGroups = bridge;

hydra.VmPrms-extraClassPaths        =   
$GEMFIRE/lib/gemfire.jar $JTESTS/lib/derby.jar,$GEMFIRE/lib/gemfire.jar $JTESTS/lib/derby.jar,$GEMFIRE/lib/gemfire.jar $JTESTS/lib/derby.jar; 

hydra.GemFirePrms-stopSystemsAfterTest = true;

hydra.Prms-totalTaskTimeSec = 1200;
hydra.Prms-maxResultWaitSec = 1200;
hydra.Prms-haltIfBadResult = true;
hydra.Prms-serialExecution = false;
hydra.Prms-alwaysDoEndTasks = false;
hydra.Prms-maxClientShutdownWaitSec = 360;
hydra.Prms-finalClientSleepSec = 60;
hydra.Prms-maxEndTaskResultWaitSec = 1800;

// Logging params
hydra.log.LogPrms-file_logging = true;

jta.JtaPrms-numberOfRandomRegions=0;
jta.JtaPrms-entryOperations = ONEOF put update get FOENO; 

jta.JtaPrms-cacheXmlFile = $JTESTS/jta/cachejta.xml;

// define the bridge servers
hydra.BridgePrms-names                = bridge;

hydra.CachePrms-names               = edge          bridge;
hydra.RegionPrms-names              = edge          bridge;
hydra.RegionPrms-regionName         = jtaRegion     jtaRegion;
hydra.RegionPrms-scope              = local         ack;
hydra.RegionPrms-poolName           = edgeDescript  none;
hydra.RegionPrms-dataPolicy = ONEOF normal empty FOENO replicate;

hydra.RegionPrms-cacheListeners = tx.LogListener;
hydra.RegionPrms-cacheWriter    = tx.LogWriter;

jta.JtaPrms-txListener = tx.TxLogListener;
jta.JtaPrms-txWriter   = tx.TxLogWriter;

// define the edge clients
hydra.PoolPrms-names                  = edgeDescript;
hydra.PoolPrms-minConnections         = 2;
hydra.PoolPrms-subscriptionEnabled    = true;
hydra.PoolPrms-threadLocalConnections = false;
hydra.PoolPrms-readTimeout            = 3600000; // hydra will hang first
hydra.PoolPrms-subscriptionRedundancy = 0;
hydra.PoolPrms-pingInterval           = 400;
hydra.PoolPrms-loadConditioningInterval = 5000;



