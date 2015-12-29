//------------------------------------------------------------------------------
// INCLUDE FILES
//------------------------------------------------------------------------------

include $JTESTS/hydraconfig/hydraparams1.inc;
include $JTESTS/hydraconfig/topology_wan_hct_locator.inc;
include $JTESTS/newWan/wan.inc;
include $JTESTS/newWan/hct.inc;

//------------------------------------------------------------------------------
// TEST Configuration
//------------------------------------------------------------------------------
INCLUDE $JTESTS/util/randomValues.inc;
util.RandomValuesPrms-objectType = byte[];
util.RandomValuesPrms-elementSize = 100;
util.ValueHolderPrms-useExtraObject = true;

util.TestHelperPrms-minTaskGranularitySec = 10;
util.OperationsClientPrms-numOpsPerTask = 100;
util.OperationsClientPrms-entryOperations = ONEOF putAll putIfAbsent replace update getNew get destroy remove FOENO;
util.OperationsClientPrms-upperThreshold = 30;
util.OperationsClientPrms-upperThresholdOperations = ONEOF remove destroy FOENO;
util.OperationsClientPrms-lowerThreshold = 10;
util.OperationsClientPrms-lowerThresholdOperations = ONEOF putIfAbsent update putAll FOENO;

newWan.WANOperationsClientPrms-numPutAllNewKeys          = 2;
newWan.WANOperationsClientPrms-numPutAllExistingKeys     = 1;
newWan.WANOperationsClientPrms-waitForSilenceListenerSec = 80;
newWan.WANOperationsClientPrms-useUniqueKeyPerThread     = false;

util.StopStartPrms-stopModes = ONEOF NICE_EXIT MEAN_KILL MEAN_EXIT NICE_KILL FOENO;
util.StopStartPrms-numVMsToStop = RANGE 1 fcn (${bridgeHostsPerSite} * ${bridgeVMsPerHost}) - 1 ncf EGNAR;
newWan.WANOperationsClientPrms-clientNamePrefixForHA     = bridge;

hydra.RegionPrms-cacheListeners  = util.SilenceListener;
newWan.WANTestPrms-senderQueueRegionListener    = none;
//------------------------------------------------------------------------------
// INITIAL WORK (configure locators, Cache, GatewaySender, GatewayReceiver)
//------------------------------------------------------------------------------
// initialize for stop/start
INITTASK taskClass  = util.StopStartVMs  taskMethod = StopStart_initTask;

/** Configures the locators. */
INITTASK taskClass = versioning.newWan.WanConflictResolverTest taskMethod = createLocatorTask
         threadGroups = locator;
INITTASK taskClass = versioning.newWan.WanConflictResolverTest taskMethod = startAndConnectLocatorTask
         threadGroups = locator;

/** Configures the server cache. */
INITTASK taskClass = versioning.newWan.WanConflictResolverTest taskMethod = HydraTask_initServerTask
         threadGroups = bridge
         runMode = always;

/** Configures the edge client cache. */
INITTASK taskClass = versioning.newWan.WanConflictResolverTest taskMethod = HydraTask_initEdgeTask
         threadGroups = edge;
         
//------------------------------------------------------------------------------
// CLOSETASK 
//------------------------------------------------------------------------------
CLOSETASK taskClass = newWan.WANBlackboard taskMethod = printBlackboard
          threadGroups = edge;

CLOSETASK taskClass = versioning.newWan.WanConflictResolverTest taskMethod = closeEdgeClientCacheTask
          threadGroups = edge;
                              
CLOSETASK taskClass = versioning.newWan.WanConflictResolverTest taskMethod = HydraTask_stopWANComponents
         threadGroups = bridge;           
