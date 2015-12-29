hydra.Prms-testRequirement = "Use JTA transactions (with CacheWriter) to coordinate transaction with outside database";
hydra.Prms-testDescription = "This test mirrors the gemfireCacheCallback tests, but uses the JTA (synchronization) to maintain consistency between the GemFire cache/region and an outside database";

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

STARTTASK    taskClass   = jta.JtaCacheCallbackHydraTest taskMethod = createDatabase
             clientNames = edge1
             ;

INITTASK     taskClass   = util.StopStartVMs  taskMethod = StopStart_initTask
             threadGroups = bridge
             ;

// create bridge, initialize region, start server
INITTASK     taskClass  = jta.JtaCacheCallbackHydraTest taskMethod = HydraTask_initializeBridgeServer
             hydra.ConfigPrms-bridgeConfig   = bridge
             hydra.ConfigPrms-cacheConfig    = bridge
             hydra.ConfigPrms-regionConfig   = bridge
             runMode = always
             threadGroups = bridge;

INITTASK     taskClass   = jta.JtaCacheCallbackHydraTest taskMethod = initTask
             hydra.ConfigPrms-cacheConfig    = edge
             hydra.ConfigPrms-regionConfig   = edge
             threadGroups = edge
             ;

INITTASK     taskClass   = jta.JtaCacheCallbackHydraTest taskMethod = loadCache
             sequential
             threadGroups = edge
             ;

TASK         taskClass   = jta.JtaCacheCallbackHydraTest taskMethod  = testTask
             threadGroups = edge
             ;

TASK         taskClass   = jta.JtaCacheCallbackHydraTest taskMethod = HydraTask_stopStartServerVM
             threadGroups = edge 
             maxThreads = 1 
             ;

//Note that we cannot validate consistency as GemFire is not enlisted as a JTA resource
//CLOSETASK    taskClass   = jta.JtaCacheCallbackHydraTest taskMethod  = closeTask
             //threadGroups = edge
             //;

//shutdown bridgeServer gracefully
CLOSETASK    taskClass   = hydra.BridgeHelper taskMethod  = stopBridgeServer
             threadGroups = bridge;

ENDTASK      taskClass   = jta.JtaCacheCallbackHydraTest taskMethod = dumpDatabase
             clientNames = edge1
             ;

util.StopStartPrms-numVMsToStop = 1;
util.StopStartPrms-stopModes = ONEOF NICE_EXIT MEAN_KILL MEAN_EXIT FOENO;

hydra.VmPrms-extraClassPaths        =   $GEMFIRE/lib/gemfire.jar $JTESTS/lib/derby.jar; 

hydra.GemFirePrms-stopSystemsAfterTest = true;

hydra.Prms-totalTaskTimeSec = 300;
hydra.Prms-maxResultWaitSec = 120;
hydra.Prms-haltIfBadResult = true;
hydra.Prms-alwaysDoEndTasks = true;
hydra.Prms-maxClientShutdownWaitSec = 360;
hydra.Prms-finalClientSleepSec = 60;
hydra.Prms-maxEndTaskResultWaitSec = 300;

// Logging params
hydra.log.LogPrms-file_logging = true;

jta.JtaPrms-cacheXmlFile = $JTESTS/jta/cachejta.xml;
jta.JtaPrms-useGemFireTransactionManager = false;
jta.JtaPrms-numberOfRandomRegions = 0;

// define the bridge servers
hydra.BridgePrms-names                = bridge;

hydra.CachePrms-names               = edge            bridge;

// region parameters for 'employee' subregion
hydra.RegionPrms-names              = edge            bridge     bridgeSubregion;
hydra.RegionPrms-scope              = local           ack;
hydra.RegionPrms-poolName           = edgeDescript    none;
hydra.RegionPrms-dataPolicy         = normal          replicate;
hydra.RegionPrms-cacheLoader        = jta.DBLoader;
hydra.RegionPrms-cacheWriter        = tx.LogWriter;
hydra.RegionPrms-cacheListeners     = tx.LogListener;

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

