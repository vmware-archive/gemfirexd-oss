// Use this include file to create a p2p configuration as per SplitBrainTestSpec 10.3.5.1
// New with quorum partitioning, we will still see host2 as the surviving side
// host1 has a weight of 23 (2 servers, 1 locator)
// host2 has a weight of 28 (2 servers (1 is the lead member), 1 locator)
// Host1: 2 client VMs, each with 2 threads and 1 locator
//        (client VMs, no locators)
//        AdminDS - to monitor SystemMembershipListener/AlertListener events
// Host2: 2 client VMs, each with 2 threads and 1 locator 
//        (leadMember, client VMs, Coordinator, eligibleCoordinators)
//        AdminDS - to monitor SystemMembershipListener/AlertListener events
// This includes the INITTASKS to place the Coordinator and leadMembers as expected,
// as well as the drop/restore connection tasks
INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/gemfireprms.inc;

hydra.HostPrms-names          = host1      host2;

hydra.VmPrms-names            = vm1        vm2        vm3        vm4        vm5       vm6;
hydra.VmPrms-hostNames        = host1      host1      host2      host1      host2     host2;

hydra.GemFirePrms-names       = gemfire1   gemfire2   gemfire3   gemfire4   gemfire5  gemfire6;
hydra.GemFirePrms-hostNames   = host1      host1      host2      host1      host2     host2;

hydra.ClientPrms-names        = client1    admin1     client2    locator1   admin2   otherLocator;
hydra.ClientPrms-vmNames      = vm1        vm2        vm3        vm4        vm5      vm6;
hydra.ClientPrms-gemfireNames = gemfire1   gemfire2   gemfire3   gemfire4   gemfire5 gemfire6;

// Host1 has 2 client VMs, each with 2 threads and 1 locator
// Host2 has 2 client VMs, each with 2 threads and 1 locator with 1 thread
hydra.ClientPrms-vmQuantities = 2          1          2          1          1        1;
hydra.ClientPrms-vmThreads    = 2          1          2          1          1        1;

STARTTASK    taskClass   = splitBrain.SplitBrainBB taskMethod = HydraTask_initialize clientNames = client1;

//------------------------------------------------------------------------------
// INITIALIZATION WORK (configure locators)
//------------------------------------------------------------------------------

/**
 * Starts the locators and connects them to admin-only distributed systems.
 * Start coordinator first, then other locators
 * Then start the LeadMember, followed by other clients
 */
INITTASK taskClass = splitBrain.SBUtil taskMethod = createLocatorTask
         threadGroups = coordinator;

INITTASK taskClass = splitBrain.SBUtil taskMethod = startAndConnectLocatorTask
         threadGroups = coordinator;

INITTASK taskClass = splitBrain.SBUtil taskMethod = createLocatorTask
         threadGroups = otherLocators;

INITTASK taskClass = splitBrain.SBUtil taskMethod = startAndConnectLocatorTask
         threadGroups = otherLocators;

INITTASK taskClass   = splitBrain.NetworkPartitionTest taskMethod  = HydraTask_initialize
         hydra.ConfigPrms-diskStoreConfig = disk
         threadGroups = leadMember;

INITTASK taskClass   = splitBrain.NetworkPartitionTest taskMethod  = HydraTask_initialize
         hydra.ConfigPrms-diskStoreConfig = disk
         threadGroups = client;

INITTASK taskClass   = util.AdminHelper taskMethod = HydraTask_initializeAdminDS
         threadGroups = adminDS;

INITTASK taskClass   = splitBrain.NetworkPartitionTest taskMethod = HydraTask_addAdminFailureListener
         threadGroups = adminDS;
         
INITTASK taskClass   = splitBrain.NetworkPartitionTest taskMethod = HydraTask_addExpectForcedDisconnectClient
         threadGroups = client;

TASK     taskClass = splitBrain.SBUtil taskMethod = dropConnection
         maxTimesToRun = 1
         startInterval = 60
         threadGroups = coordinator;

CLOSETASK taskClass = splitBrain.NetworkPartitionTest taskMethod = HydraTask_createSnapshot
          threadGroups = leadMember, client;

CLOSETASK taskClass = splitBrain.NetworkPartitionTest taskMethod = HydraEndTask_verifyLosingPartition
          threadGroups = client;

CLOSETASK taskClass = splitBrain.SBUtil taskMethod = restoreConnection
          threadGroups = client;

CLOSETASK taskClass = splitBrain.NetworkPartitionTest taskMethod = HydraEndTask_waitForReconnect
          threadGroups = client;

CLOSETASK taskClass = util.AdminHelper taskMethod = HydraEndTask_waitForReconnect
          threadGroups = adminDS;

CLOSETASK taskClass = splitBrain.NetworkPartitionTest taskMethod = HydraEndTask_verifyReconnect
          threadGroups = client;

CLOSETASK taskClass = splitBrain.NetworkPartitionTest taskMethod = HydraTask_verifyAdminFailures
          threadGroups = adminDS;
          
CLOSETASK taskClass = splitBrain.NetworkPartitionTest taskMethod = HydraTask_verifyRegion
          threadGroups = leadMember, client;

CLOSETASK taskClass = splitBrain.NetworkPartitionTest taskMethod = closeCacheAndDisconnectFromDS
          threadGroups = leadMember, client;

// Separate out coordinator, must be started first
THREADGROUP coordinator totalThreads = 1 totalVMs = 1 clientNames = locator1;
THREADGROUP otherLocators totalThreads = 1 totalVMs = 1 clientNames = otherLocator;

// Separate out leadMember, must be started first
THREADGROUP leadMember totalThreads = 1 totalVMs = 1 clientNames = client2;
THREADGROUP client  totalThreads = 4 totalVMs = 2 clientNames = client1
                    totalThreads = 3 totalVMs = 2 clientNames = client2;

THREADGROUP adminDS totalThreads = 1 totalVMs = 1 clientNames = admin1
                    totalThreads = 1 totalVMs = 1 clientNames = admin2;

//THREADGROUP reconnectClients totalThreads = 2 totalVMs = 2 clientNames = client1;
//THREADGROUP reconnectAdmin totalThreads = 1 totalVMs = 1 clientNames = admin1;


// util.AdminHelper configuration
util.AdminHelperPrms-systemMembershipListener = splitBrain.SBSystemMembershipListener;
util.AdminHelperPrms-alertListener = splitBrain.SBAlertListener;

// Settings for splitBrain behavior with auto reconnect
splitBrain.SplitBrainPrms-losingPartition = host1;
hydra.GemFirePrms-enableNetworkPartitionDetection = true;
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

hydra.ConfigPrms-cacheConfig    = myCache;
hydra.ConfigPrms-regionConfig   = myRegion;
hydra.CachePrms-names           = myCache;
hydra.RegionPrms-names          = myRegion;
hydra.RegionPrms-regionName     = TestRegion;
hydra.RegionPrms-dataPolicy     = replicate;
hydra.RegionPrms-scope          = ONEOF global distributedAck FOENO;
hydra.RegionPrms-cacheListeners = splitBrain.SBListener;

// Used only when a persistent data policy is selected
hydra.DiskStorePrms-names = disk;
hydra.DiskStorePrms-queueSize = 0;
hydra.DiskStorePrms-timeInterval = 1000;
hydra.RegionPrms-diskSynchronous = true;

