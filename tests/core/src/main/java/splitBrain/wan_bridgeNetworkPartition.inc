// All wan bridge networkPartition tests share this include.  It contains the tasks to execute
// and the region configuation.
// The individual confs establish the correct vm configurations across the two hosts and 
// overlay threadgroups appropriately for the configurations described in the SplitBrainTestSpec
// Section 10.3.5 (1-4)
//
// WAN Site #1 is spread across two hosts, divided into wan1Lose and wan1Survive clients
// edgeClients execute on host3
// locator naming scheme: locator_<wanSite(1 or 2)>_<host(1 or 2)>
//

STARTTASK    taskClass   = splitBrain.SplitBrainBB taskMethod = HydraTask_initialize clientNames = wan1Lose;

//------------------------------------------------------------------------------
// INITIALIZATION WORK (configure locators)
//------------------------------------------------------------------------------

/**
 * Starts the locators and connects them to admin-only distributed systems.
 * Setup WAN Site #1 carefully as this is the site that will experience the n/w split
 * Start coordinator first, then other locators
 * Then start the LeadMember
 */
INITTASK taskClass = splitBrain.SBUtil taskMethod = createLocatorTask
         threadGroups = coordinator;

INITTASK taskClass = splitBrain.SBUtil taskMethod = startAndConnectLocatorTask
         threadGroups = coordinator;

INITTASK taskClass = splitBrain.SBUtil taskMethod = createLocatorTask
         threadGroups = otherLocators;

INITTASK taskClass = splitBrain.SBUtil taskMethod = startAndConnectLocatorTask
         threadGroups = otherLocators;

INITTASK taskClass   = splitBrain.NetworkPartitionTest taskMethod = HydraTask_initializeBridgeServer
         hydra.ConfigPrms-cacheConfig  = bridge
         hydra.ConfigPrms-regionConfig = bridge
         hydra.ConfigPrms-bridgeConfig = bridge
         hydra.ConfigPrms-diskStoreConfig = disk
         threadGroups = leadMember;

INITTASK taskClass   = splitBrain.NetworkPartitionTest taskMethod = HydraTask_initializeBridgeServer
         hydra.ConfigPrms-cacheConfig  = bridge
         hydra.ConfigPrms-regionConfig = bridge
         hydra.ConfigPrms-bridgeConfig = bridge
         hydra.ConfigPrms-diskStoreConfig = disk
         threadGroups = otherServers;

INITTASK taskClass = splitBrain.NetworkPartitionTest taskMethod = startGatewayHubTask
         threadGroups = leadMember;

INITTASK taskClass = splitBrain.NetworkPartitionTest taskMethod = startGatewayHubTask
         threadGroups = otherServers;

INITTASK taskClass   = util.AdminHelper taskMethod = HydraTask_initializeAdminDS
         threadGroups = adminDS;

INITTASK taskClass   = splitBrain.NetworkPartitionTest taskMethod  = HydraTask_initializeBridgeClient
         hydra.ConfigPrms-cacheConfig  = edge
         hydra.ConfigPrms-regionConfig = edge
         threadGroups = edgeClients;

//------------------------------------------------------------------------------
// PRIMARY WORK (cache operations)
//------------------------------------------------------------------------------

TASK     taskClass = splitBrain.NetworkPartitionTest taskMethod = HydraTask_doEntryOperations
         threadGroups = leadMember, otherServers, edgeClients;

TASK     taskClass = splitBrain.SBUtil taskMethod = dropConnection
         maxTimesToRun = 1
         startInterval = 60
         threadGroups = coordinator;

CLOSETASK taskClass = splitBrain.NetworkPartitionTest taskMethod = HydraCloseTask_checkForRegionDestroyedForcedDisconnects
          threadGroups = leadMember, otherServers;

CLOSETASK   taskClass = splitBrain.NetworkPartitionTest taskMethod = closeCacheAndDisconnectFromDS
            threadGroups = leadMember, otherServers;

ENDTASK  taskClass = splitBrain.NetworkPartitionTest taskMethod = HydraEndTask_verifyLosingPartition
         clientNames = wan1Survive;

ENDTASK  taskClass = splitBrain.SBUtil taskMethod = restoreConnection
         clientNames = wan1Survive;

// util.AdminHelper configuration
util.AdminHelperPrms-systemMembershipListener = splitBrain.SBSystemMembershipListener;

// Settings for splitBrain behavior
splitBrain.SplitBrainPrms-losingPartition = host1;
hydra.GemFirePrms-enableNetworkPartitionDetection = true;
hydra.GemFirePrms-disableAutoReconnect = true;
hydra.Prms-manageLocatorAgents = false; // turn off master-managed locators

INCLUDE $JTESTS/util/randomValues.inc;
util.RandomValuesPrms-objectType = byte[];
util.RandomValuesPrms-elementSize = 500;
util.ValueHolderPrms-useExtraObject = true;

util.TestHelperPrms-minTaskGranularitySec = 60;
util.OperationsClientPrms-entryOperations = ONEOF add add getNew getNew update invalidate get destroy putIfAbsent putIfAbsent replace remove FOENO;
util.OperationsClientPrms-upperThreshold = 500;
util.OperationsClientPrms-upperThresholdOperations = ONEOF destroy remove FOENO;
util.OperationsClientPrms-lowerThreshold = 10;
util.OperationsClientPrms-lowerThresholdOperations = ONEOF add getNew putIfAbsent FOENO;

hydra.GemFirePrms-conserveSockets = true;
hydra.GemFirePrms-stopSystemsAfterTest = true;
hydra.Prms-alwaysDoEndTasks = true;

hydra.Prms-totalTaskTimeSec = 300;
hydra.Prms-maxResultWaitSec = 180;
hydra.Prms-haltIfBadResult = true;
hydra.Prms-serialExecution = false;
hydra.Prms-maxClientShutdownWaitSec = 360;
hydra.Prms-finalClientSleepSec = 60;
hydra.Prms-maxEndTaskResultWaitSec = 1800;

// region, bridge and edge definitions 
hydra.CachePrms-names           = bridge                     edge;
hydra.RegionPrms-names          = bridge                     edge;
hydra.RegionPrms-regionName     = TestRegion;
hydra.RegionPrms-enableGateway  = true                       false;
hydra.RegionPrms-scope          = ack                        local;
hydra.RegionPrms-dataPolicy     = oneof replicate persistent_replicate foeno default;
hydra.RegionPrms-poolName       = none                       brclient;
hydra.RegionPrms-cacheListeners = splitBrain.SBListener;

hydra.BridgePrms-names          = bridge;

hydra.BridgePrms-maxThreads = ONEOF default 16 FOENO; // selector control

// Pool Config
hydra.PoolPrms-names                  = brclient;
hydra.PoolPrms-readTimeout            = 60000;
hydra.PoolPrms-threadLocalConnections = true;  // see Bug 37157
hydra.PoolPrms-minConnections         = 2;
hydra.PoolPrms-subscriptionEnabled    = true;
hydra.PoolPrms-subscriptionRedundancy = -1;
hydra.PoolPrms-contactAlgorithm       = hydra.PoolHelper getRandomContactsInWanHctSite;
hydra.PoolPrms-contactAlgorithm       = hydra.PoolHelper getRandomContactsInWanHctSite;

// Used only when a persistent data policy is selected
hydra.DiskStorePrms-names = disk;
hydra.DiskStorePrms-queueSize = 0;
hydra.DiskStorePrms-timeInterval = 1000;

// Gateway specific Prms
hydra.ConfigPrms-gatewayHubConfig = hub;
hydra.GatewayHubPrms-names = hub;

hydra.ConfigPrms-gatewayConfig = gateway;
hydra.GatewayPrms-names = gateway;
hydra.GatewayPrms-maximumQueueMemory = 100;
hydra.GatewayPrms-batchTimeInterval = 10;
hydra.GatewayPrms-diskStoreName = disk;

hydra.DiskStorePrms-names = disk;
