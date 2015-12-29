// Use this include file to create a p2p configuration as per SplitBrainTestSpec 10.3.5.2
// New with quorum partitioning, we will still see host2 as the surviving side
// host1 has a weight of 20 (2 servers)
// host2 has a weight of 28 (2 servers (1 is the lead member), 1 locator)
// Host1: 2 client VMs, each with 2 threads 
//        (client VMs, no locators)
//        Admin - to monitor SystemMembershipListener and AlertListener events
// Host2: 2 client VMs, each with 2 threads and 1 locator 
//        (leadMember, client VMs and Coordinator)
//        Admin - to monitor SystemMembershipListener and AlertListener events
// This includes the INITTASKS to place the Coordinator and leadMembers as expected,
// as well as the drop/restore connection tasks
INCLUDE $JTESTS/hydraconfig/hydraparams1.inc;
INCLUDE $JTESTS/hydraconfig/gemfireprms.inc;

hydra.HostPrms-names          = host1      host2;

hydra.VmPrms-names            = vm1        vm2        vm3        vm4       vm5;
hydra.VmPrms-hostNames        = host1      host1      host2      host2     host2;

hydra.GemFirePrms-names       = gemfire1   gemfire2   gemfire3   gemfire4  gemfire5;
hydra.GemFirePrms-hostNames   = host1      host1      host2      host2     host2;

hydra.ClientPrms-names        = client1    admin1     client2    locator2  admin2;
hydra.ClientPrms-vmNames      = vm1        vm2        vm3        vm4       vm5;
hydra.ClientPrms-gemfireNames = gemfire1   gemfire2   gemfire3   gemfire4  gemfire5;

// Host1 contains 2 client VMs, each with 2 threads
// Host2 contains 2 client Vms, each with 2 threads + a single locator (coordinator)
hydra.ClientPrms-vmQuantities = 2          1          2          1          1;
hydra.ClientPrms-vmThreads    = 2          1          2          1          1;

STARTTASK    taskClass   = splitBrain.SplitBrainBB taskMethod = HydraTask_initialize clientNames = client1;

//------------------------------------------------------------------------------
// INITIALIZATION WORK (configure locators)
//------------------------------------------------------------------------------

/**
 * Starts the locators and connects them to admin-only distributed systems.
 */
INITTASK taskClass = splitBrain.SBUtil taskMethod = createLocatorTask
         threadGroups = coordinator;

INITTASK taskClass = splitBrain.SBUtil taskMethod = startAndConnectLocatorTask
         threadGroups = coordinator;

INITTASK taskClass   = splitBrain.NetworkPartitionTest taskMethod  = HydraTask_initialize
         hydra.ConfigPrms-diskStoreConfig = disk
         threadGroups = leadMember;

INITTASK taskClass   = splitBrain.NetworkPartitionTest taskMethod  = HydraTask_initialize
         hydra.ConfigPrms-diskStoreConfig = disk
         threadGroups = client;

INITTASK taskClass   = util.AdminHelper taskMethod = HydraTask_initializeAdminDS
         threadGroups = adminDS;

TASK     taskClass = splitBrain.SBUtil taskMethod = dropConnection
         maxTimesToRun = 1
         startInterval = 60
         threadGroups = coordinator;

CLOSETASK taskClass = splitBrain.NetworkPartitionTest taskMethod = HydraTask_createSnapshot
          threadGroups = leadMember, client;

CLOSETASK   taskClass = splitBrain.NetworkPartitionTest taskMethod = closeCacheAndDisconnectFromDS
            threadGroups = leadMember, client;

CLOSETASK  taskClass = splitBrain.NetworkPartitionTest taskMethod = HydraEndTask_verifyLosingPartition
           threadGroups = client;

CLOSETASK  taskClass = splitBrain.SBUtil taskMethod = restoreConnection
           threadGroups = client;

// Locators - we have to separate the coordinator so it can be started first
THREADGROUP coordinator totalThreads = 1 totalVMs = 1 clientNames = locator2;

// LeadMember - we have to separate out the leadMember so it can be in the same
// subgroup as the Coordinator
THREADGROUP leadMember totalThreads = 1 totalVMs = 1 clientNames = client2;
THREADGROUP client  totalThreads = 4 totalVMs = 2 clientNames = client1
                    totalThreads = 3 totalVMs = 2 clientNames = client2;

THREADGROUP adminDS totalThreads = 1 totalVMs = 1 clientNames = admin1
                    totalThreads = 1 totalVMs = 1 clientNames = admin2;

// util.AdminHelper configuration
util.AdminHelperPrms-systemMembershipListener = splitBrain.SBSystemMembershipListener;
util.AdminHelperPrms-alertListener = splitBrain.SBAlertListener;

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

hydra.ConfigPrms-cacheConfig    = myCache;
hydra.ConfigPrms-regionConfig   = myRegion;
hydra.CachePrms-names           = myCache;
hydra.RegionPrms-names          = myRegion;
hydra.RegionPrms-regionName     = TestRegion;
hydra.RegionPrms-dataPolicy     = ONEOF replicate persistent_replicate FOENO;
hydra.RegionPrms-scope          = ONEOF global distributedAck FOENO;
hydra.RegionPrms-cacheListeners = splitBrain.SBListener;

// Used only when a persistent data policy is selected
hydra.DiskStorePrms-names = disk;
hydra.DiskStorePrms-queueSize = 0;
hydra.DiskStorePrms-timeInterval = 1000;
hydra.RegionPrms-diskSynchronous = true;

